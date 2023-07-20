// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.http;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.http.rest.WarehouseAction;
import com.starrocks.load.loadv2.BrokerLoadJob;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.metric.WarehouseMetricMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.warehouse.LocalWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseInfo;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class WarehouseActionTest extends StarRocksHttpTestCase {
    @BeforeClass
    public static void beforeClass() {
        // Mock CatalogIdGenerator::getNextId, which is used when creating a new load job and assigned a job id.
        AtomicLong nextId = new AtomicLong(0L);
        new MockUp<CatalogIdGenerator>() {
            @Mock
            public synchronized long getNextId() {
                return nextId.getAndIncrement();
            }
        };
    }

    @Before
    public void before() {
        GlobalStateMgr.getCurrentState().initDefaultWarehouse();
    }

    @Test
    public void testGetWarehousesEmpty() throws IOException {
        Map<String, WarehouseInfo> infos = fetchWarehouseInfos();
        assertThat(infos.values()).containsOnlyOnce(new WarehouseInfo(WarehouseManager.DEFAULT_WAREHOUSE_NAME, 0L));
    }

    @Test
    public void testGetWarehousesWithSingleFE() throws IOException {
        WarehouseInfo expectedInfo = new WarehouseInfo(WarehouseManager.DEFAULT_WAREHOUSE_NAME, 0L);
        expectedInfo.increaseNumUnfinishedQueryJobs(1L);
        expectedInfo.increaseNumUnfinishedBackupJobs(2L);
        expectedInfo.increaseNumUnfinishedRestoreJobs(3L);

        WarehouseMetricMgr.increaseUnfinishedQueries(expectedInfo.getWarehouse(),
                expectedInfo.getNumUnfinishedQueryJobs());
        WarehouseMetricMgr.increaseUnfinishedBackupJobs(expectedInfo.getWarehouse(),
                expectedInfo.getNumUnfinishedBackupJobs());
        WarehouseMetricMgr.increaseUnfinishedRestoreJobs(expectedInfo.getWarehouse(),
                expectedInfo.getNumUnfinishedRestoreJobs());

        Map<String, WarehouseInfo> infos = fetchWarehouseInfos();

        assertThat(infos.values()).containsOnlyOnce(expectedInfo);
    }

    @Test
    public void testGetWarehouses() throws IOException, MetaNotFoundException {
        long startTimestampMs = System.currentTimeMillis();

        List<Warehouse> whs = ImmutableList.of(
                new LocalWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME),
                new LocalWarehouse(1, "wh-1"));

        LoadMgr loadMgr = mockLoadMgr();
        addBrokerLoadJobs(loadMgr, whs.get(0).getFullName(), 2, 4);
        addBrokerLoadJobs(loadMgr, whs.get(1).getFullName(), 20, 40);

        long afterTimestampMs = System.currentTimeMillis();

        Map<String, WarehouseInfo> whToInfo = fetchWarehouseInfos();
        Assert.assertEquals(2, whToInfo.size());
        WarehouseInfo info0 = whToInfo.get(WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        Assert.assertEquals(4, info0.getNumUnfinishedLoadJobs());
        assertThat(info0.getLastFinishedJobTimestampMs())
                .isGreaterThanOrEqualTo(startTimestampMs)
                .isLessThanOrEqualTo(afterTimestampMs);

        mockWarehouses(whs);
    }

    private class JobInfo {
        private long numQueryJobs = 0;
        private long numdBackupJobs = 0;
        private long numRestoreJobs = 0;

        private long numBrokerLoadJobs = 0;
        private long numInsertLoadJobs = 0;
        private long numSparkLoadJobs = 0;
        private long numRoutineLoadJobs = 0;
        private long numStreamLoadJobs = 0;
    }

    private class JobExecutingInfo {

    }

    private void prepareJobExecutingInfo(long numUnfinishedQueryJobs,) {

        WarehouseInfo expectedInfo = new WarehouseInfo(WarehouseManager.DEFAULT_WAREHOUSE_NAME, 0L);
        expectedInfo.increaseNumUnfinishedQueryJobs(1L);
        expectedInfo.increaseNumUnfinishedBackupJobs(2L);
        expectedInfo.increaseNumUnfinishedRestoreJobs(3L);
    }

    private LoadMgr mockLoadMgr() {
        LoadMgr loadMgr = new LoadMgr(null);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public LoadMgr getLoadMgr() {
                return loadMgr;
            }
        };
        return loadMgr;
    }

    private BrokerLoadJob genBrokerLoadJob(String wh, int index) throws MetaNotFoundException {
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.setCurrentWarehouse(wh);

        return new BrokerLoadJob(TEST_DB_ID, "broker-load-" + index, null, null, context);
    }

    private void addBrokerLoadJobs(LoadMgr loadMgr, String wh, int numFinishedJobs, int numUnfinishedJobs)
            throws MetaNotFoundException {
        for (int i = 0; i < numFinishedJobs; i++) {
            BrokerLoadJob job = genBrokerLoadJob(wh, i);
            job.updateState(JobState.FINISHED);
            loadMgr.replayCreateLoadJob(job);
        }

        for (int i = 0; i < numUnfinishedJobs; i++) {
            BrokerLoadJob job = genBrokerLoadJob(wh, i + numFinishedJobs);
            loadMgr.replayCreateLoadJob(job);
        }
    }

    private void mockWarehouses(List<Warehouse> warehouses) {
        new MockUp<WarehouseManager>() {
            @Mock
            Collection<Warehouse> getWarehouses() {
                return warehouses;
            }
        };
    }

    private Map<String, WarehouseInfo> fetchWarehouseInfos() throws IOException {

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + WarehouseAction.URI)
                .build();

        Response response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.isSuccessful());

        Assert.assertNotNull(response.body());
        Gson gson = new Gson();
        return Arrays.stream(gson.fromJson(response.body().charStream(), WarehouseInfo[].class))
                .collect(Collectors.toMap(
                        WarehouseInfo::getWarehouse,
                        Function.identity()
                ));
    }
}
