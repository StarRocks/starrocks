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
import com.google.common.collect.Maps;
import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.GenericPool;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.http.rest.ActionStatus;
import com.starrocks.http.rest.RestSuccessBaseResult;
import com.starrocks.http.rest.WarehouseAction;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.loadv2.BrokerLoadJob;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.metric.WarehouseMetricMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TGetWarehousesRequest;
import com.starrocks.thrift.TGetWarehousesResponse;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.MockGenericPool;
import com.starrocks.warehouse.LocalWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseInfo;
import com.starrocks.warehouse.WarehouseInfosBuilder;
import io.netty.handler.codec.http.HttpResponseStatus;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.ImmutableMap;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class WarehouseActionTest extends StarRocksHttpTestCase {
    private static final AtomicLong NEXT_INDEX = new AtomicLong(0L);
    private LoadMgr loadMgr;
    private RoutineLoadMgr routineLoadMgr;
    private StreamLoadMgr streamLoadMgr;

    private GenericPool<FrontendService.Client> prevFrontendClientPool;

    @Before
    public void before() {
        GlobalStateMgr.getCurrentState().initDefaultWarehouse();
        mockLoadMgr();

        // Mock CatalogIdGenerator::getNextId, which is used when creating a new load job and assigned a job id.
        new MockUp<CatalogIdGenerator>() {
            @Mock
            public synchronized long getNextId() {
                return NEXT_INDEX.getAndIncrement();
            }
        };

        // Mock logCreateXxxLoadJob to make we able to add LoadJobs to the load managers.
        new MockUp<EditLog>() {
            @Mock
            public void logCreateLoadJob(com.starrocks.load.loadv2.LoadJob loadJob) {
                // Do nothing.
            }

            @Mock
            public void logCreateRoutineLoadJob(RoutineLoadJob routineLoadJob) {
                // Do nothing.
            }
        };

        prevFrontendClientPool = ClientPool.frontendPool;
        ClientPool.frontendPool = new MockGenericPool("mock-pool") {
            @Override
            public TServiceClient borrowObject(TNetworkAddress address) throws Exception {
                return new FrontendService.Client(null);
            }
        };
    }

    @After
    public void after() {
        ClientPool.frontendPool = prevFrontendClientPool;
    }

    @Test
    public void testGetWarehousesEmpty() throws IOException {
        Map<String, WarehouseInfo> infos = fetchWarehouseInfos();
        assertThat(infos.values()).containsOnlyOnce(new WarehouseInfo(WarehouseManager.DEFAULT_WAREHOUSE_NAME, 0L));
    }

    @Test
    public void testGetWarehousesWithException() throws IOException {
        new MockUp<WarehouseInfosBuilder>() {
            @Mock
            public WarehouseInfosBuilder makeBuilderFromMetricAndMgrs() {
                throw new RuntimeException("mock-runtime-exception");
            }
        };

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + WarehouseAction.URI)
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            Assert.assertFalse(response.isSuccessful());
            Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.code());

            Assert.assertNotNull(response.body());
            RestSuccessBaseResult<WarehouseAction.Result> res =
                    RestSuccessBaseResult.fromJson(response.body().charStream(), WarehouseAction.Result.class);
            Assert.assertEquals(ActionStatus.FAILED, res.status);
            assertThat(res.msg).contains("mock-runtime-exception");
        }
    }

    @Test
    public void testGetWarehouses() throws IOException, UserException, InterruptedException {
        List<Warehouse> whs = ImmutableList.of(
                new LocalWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME),
                new LocalWarehouse(1, "wh-1"));

        Map<Warehouse, JobExecutingInfo> whToJobInfo = ImmutableMap.of(
                whs.get(0), new JobExecutingInfo(
                        new JobInfo(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L),
                        new JobInfo(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L)
                ),
                whs.get(1), new JobExecutingInfo(
                        new JobInfo(11L, 21L, 31L, 41L, 51L, 61L, 71L, 81L),
                        new JobInfo(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
                )
        );

        long startTimestampMs = System.currentTimeMillis();
        Map<String, WarehouseInfo> expectedWhToWhInfo = prepareWarehouseJobExecutingInfo(whToJobInfo);
        long afterTimestampMs = System.currentTimeMillis();

        // Add statistics insert jobs.
        Thread.sleep(10L); // Make the finished timestamps of statistic jobs are different from the above jobs.
        addInsertLoadJobs(whs.get(0).getFullName(), 13, false, true);
        addInsertLoadJobs(whs.get(1).getFullName(), 12, true, true);

        // ------------------------------------------------------------------------------------
        // Case 1. The warehouse#1 is not in the warehouse manager.
        // ------------------------------------------------------------------------------------
        Map<String, WarehouseInfo> whToWhInfo = fetchWarehouseInfos();
        Assert.assertEquals(2, whToWhInfo.size());

        // Check the finished timestamp, it occurs in prepareWarehouseJobExecutingInfo,
        // which should be between [startTimestampMs, afterTimestampMs].
        whToWhInfo.forEach((wh, whInfo) -> {
            assertThat(whInfo.getLastFinishedJobTimestampMs())
                    .isGreaterThanOrEqualTo(startTimestampMs)
                    .isLessThanOrEqualTo(afterTimestampMs);
            // Record the timestamp to expected.
            expectedWhToWhInfo.get(wh).updateLastFinishedJobTimeMs(whInfo.getLastFinishedJobTimestampMs());
        });
        // The warehouse#1 hasn't been in the warehouse manager, so WarehouseInfo.id is ABSENT_ID.
        assertThat(whToWhInfo.get(whs.get(1).getFullName()).getId()).isEqualTo(WarehouseInfo.ABSENT_ID);
        // Set id of warehouse#1 to ABSENT_ID temporarily, to compare the whole info below.
        expectedWhToWhInfo.get(whs.get(1).getFullName()).setId(WarehouseInfo.ABSENT_ID);
        assertThat(whToWhInfo).containsExactlyInAnyOrderEntriesOf(expectedWhToWhInfo);
        // Reset id of warehouse#1.
        expectedWhToWhInfo.get(whs.get(1).getFullName()).setId(1);

        // ------------------------------------------------------------------------------------
        // Case 2. All the warehouses are in the warehouse manager.
        // ------------------------------------------------------------------------------------
        mockWarehouses(whs);
        whToWhInfo = fetchWarehouseInfos();
        assertThat(whToWhInfo).containsExactlyInAnyOrderEntriesOf(expectedWhToWhInfo);

        // ------------------------------------------------------------------------------------
        // Case 3. After removing the expired finished jobs from load managers,
        // the last finished timestamp should not be different.
        // ------------------------------------------------------------------------------------
        int prevLabelKeepMaxSecond = Config.label_keep_max_second;
        int prevLabelKeepMaxNum = Config.label_keep_max_num;
        try {
            Config.label_keep_max_second = 1;
            Config.label_keep_max_num = 0;
            Thread.sleep(Config.label_keep_max_second); // Make the jobs expired.

            loadMgr.removeOldLoadJob();
            routineLoadMgr.cleanOldRoutineLoadJobs();
            streamLoadMgr.cleanOldStreamLoadTasks(true);
            streamLoadMgr.cleanSyncStreamLoadTasks();

            whToWhInfo = fetchWarehouseInfos();
            assertThat(whToWhInfo).containsExactlyInAnyOrderEntriesOf(expectedWhToWhInfo);

        } finally {
            Config.label_keep_max_num = prevLabelKeepMaxNum;
            Config.label_keep_max_second = prevLabelKeepMaxSecond;
        }
    }

    @Test
    public void testGetWarehousesWithMultipleFEs() throws IOException {
        List<Frontend> frontends = ImmutableList.of(
                new Frontend(FrontendNodeType.FOLLOWER, "FE-1", "127.0.0.1", 9010),
                new Frontend(FrontendNodeType.FOLLOWER, "FE-2", "127.0.0.2", 9010),
                new Frontend(FrontendNodeType.FOLLOWER, "FE-3", "127.0.0.3", 9010),
                new Frontend(FrontendNodeType.FOLLOWER, "FE-4", "127.0.0.4", 9010),
                new Frontend(FrontendNodeType.FOLLOWER, "FE-5", "127.0.0.5", 9010),
                new Frontend(FrontendNodeType.FOLLOWER, "FE-6", "127.0.0.6", 9010),
                new Frontend(FrontendNodeType.FOLLOWER, "FE-7", "127.0.0.7", 9010),
                new Frontend(FrontendNodeType.FOLLOWER, "FE-8", "127.0.0.8", 9010)
        );

        List<List<WarehouseInfo>> whInfosPerFE = ImmutableList.of(
                // FE-2
                ImmutableList.of(
                        new WarehouseInfo(WarehouseManager.DEFAULT_WAREHOUSE_NAME,
                                WarehouseManager.DEFAULT_WAREHOUSE_ID, 1L, 2L, 3L, 4L, 5L),
                        new WarehouseInfo("wh-1", 1, 6, 7, 8, 9, 4L)
                ),
                // FE-3
                ImmutableList.of(
                        new WarehouseInfo(WarehouseManager.DEFAULT_WAREHOUSE_NAME,
                                WarehouseManager.DEFAULT_WAREHOUSE_ID, 10L, 12L, 13L, 14L, 15L),
                        new WarehouseInfo("wh-2", 2, 16, 17, 18, 19, 14L)
                )
        );
        Map<String, WarehouseInfo> expectedWhToInfos = Maps.newHashMap();
        whInfosPerFE.stream().flatMap(Collection::stream).forEach(newInfo ->
                expectedWhToInfos.compute(newInfo.getWarehouse(), (wh, prevInfo) -> {
                    WarehouseInfo info = prevInfo;
                    if (prevInfo == null) {
                        info = new WarehouseInfo(newInfo.getWarehouse(), newInfo.getId());
                    }
                    info.increaseNumUnfinishedQueryJobs(newInfo.getNumUnfinishedQueryJobs());
                    info.increaseNumUnfinishedLoadJobs(newInfo.getNumUnfinishedLoadJobs());
                    info.increaseNumUnfinishedBackupJobs(newInfo.getNumUnfinishedBackupJobs());
                    info.increaseNumUnfinishedRestoreJobs(newInfo.getNumUnfinishedRestoreJobs());
                    info.updateLastFinishedJobTimeMs(newInfo.getLastFinishedJobTimestampMs());

                    return info;
                }));

        new MockUp<NodeMgr>() {
            @Mock
            public List<Frontend> getFrontends(FrontendNodeType nodeType) {
                return frontends;
            }

            @Mock
            public Pair<String, Integer> getSelfNode() {
                return new Pair<>("127.0.0.1", 9010);
            }
        };

        AtomicInteger getTimes = new AtomicInteger(0);
        new MockUp<FrontendService.Client>() {
            @Mock
            public TGetWarehousesResponse getWarehouses(TGetWarehousesRequest request) throws TException {
                int times = getTimes.getAndIncrement();
                if (times < whInfosPerFE.size()) {
                    TGetWarehousesResponse res = new TGetWarehousesResponse();
                    res.setStatus(new TStatus(TStatusCode.OK));
                    res.setWarehouse_infos(
                            whInfosPerFE.get(times).stream().map(WarehouseInfo::toThrift).collect(Collectors.toList()));
                    return res;
                } else if (times < whInfosPerFE.size() + 1) { // Internal Error.
                    TGetWarehousesResponse res = new TGetWarehousesResponse();
                    res.setStatus(new TStatus(TStatusCode.INTERNAL_ERROR));
                    return res;
                } else if (times < whInfosPerFE.size() + 2) { // OK without warehouse_infos.
                    TGetWarehousesResponse res = new TGetWarehousesResponse();
                    res.setStatus(new TStatus(TStatusCode.OK));
                    return res;
                } else { // Throw exception.
                    throw new TException("mock exception");
                }
            }
        };

        Map<String, WarehouseInfo> infos = fetchWarehouseInfos();
        assertThat(infos).containsExactlyInAnyOrderEntriesOf(expectedWhToInfos);
    }

    private void mockWarehouses(List<Warehouse> warehouses) {
        new MockUp<WarehouseManager>() {
            @Mock
            Collection<Warehouse> getWarehouses() {
                return warehouses;
            }
        };
    }

    /**
     * Reset all the load managers to clear information of jobs.
     */
    private void mockLoadMgr() {
        loadMgr = new LoadMgr(null);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public LoadMgr getLoadMgr() {
                return loadMgr;
            }
        };

        routineLoadMgr = new RoutineLoadMgr();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public RoutineLoadMgr getRoutineLoadMgr() {
                return routineLoadMgr;
            }
        };

        streamLoadMgr = new StreamLoadMgr();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StreamLoadMgr getStreamLoadMgr() {
                return streamLoadMgr;
            }
        };
    }

    private static class JobInfo {
        private final long numQueryJobs;
        private final long numBackupJobs;
        private final long numRestoreJobs;

        private final long numBrokerLoadJobs;
        private final long numInsertLoadJobs;
        private final long numSparkLoadJobs;
        private final long numRoutineLoadJobs;
        private final long numStreamLoadJobs;

        public JobInfo(long numQueryJobs, long numBackupJobs, long numRestoreJobs, long numBrokerLoadJobs,
                       long numInsertLoadJobs, long numSparkLoadJobs, long numRoutineLoadJobs, long numStreamLoadJobs) {
            this.numQueryJobs = numQueryJobs;
            this.numBackupJobs = numBackupJobs;
            this.numRestoreJobs = numRestoreJobs;
            this.numBrokerLoadJobs = numBrokerLoadJobs;
            this.numInsertLoadJobs = numInsertLoadJobs;
            this.numSparkLoadJobs = numSparkLoadJobs;
            this.numRoutineLoadJobs = numRoutineLoadJobs;
            this.numStreamLoadJobs = numStreamLoadJobs;
        }

        private long getNumLoadJobsSupportWh() {
            return numBrokerLoadJobs + numInsertLoadJobs;
        }

        private long getNumLoadJobsNonSupportWh() {
            return numSparkLoadJobs + numRoutineLoadJobs + numStreamLoadJobs;
        }
    }

    private static class JobExecutingInfo {
        private final JobInfo unfinishedInfo;
        private final JobInfo finishedInfo;

        public JobExecutingInfo(JobInfo unfinishedInfo, JobInfo finishedInfo) {
            this.unfinishedInfo = unfinishedInfo;
            this.finishedInfo = finishedInfo;
        }
    }

    private Map<String, WarehouseInfo> prepareWarehouseJobExecutingInfo(Map<Warehouse, JobExecutingInfo> whToInfo)
            throws UserException {
        Map<String, WarehouseInfo> whToWhInfo = whToInfo.keySet().stream().collect(Collectors.toMap(
                Warehouse::getFullName,
                WarehouseInfo::fromWarehouse
        ));
        WarehouseInfo defaultWhInfo = whToWhInfo.get(WarehouseManager.DEFAULT_WAREHOUSE_NAME);

        for (Map.Entry<Warehouse, JobExecutingInfo> whAndJobInfo : whToInfo.entrySet()) {
            Warehouse wh = whAndJobInfo.getKey();
            JobExecutingInfo jobInfo = whAndJobInfo.getValue();

            WarehouseInfo whInfo = whToWhInfo.get(wh.getFullName());

            prepareJobInfo(whInfo, defaultWhInfo, wh, jobInfo.finishedInfo, true);
            prepareJobInfo(whInfo, defaultWhInfo, wh, jobInfo.unfinishedInfo, false);
        }

        return whToWhInfo;
    }

    private void prepareJobInfo(WarehouseInfo whInfo, WarehouseInfo defaultWhInfo, Warehouse wh, JobInfo jobInfo,
                                boolean isFinished)
            throws UserException {
        long deltaFactor = isFinished ? -1L : 1L;

        // Add job info to whInfo.
        whInfo.increaseNumUnfinishedQueryJobs(jobInfo.numQueryJobs * deltaFactor);
        whInfo.increaseNumUnfinishedBackupJobs(jobInfo.numBackupJobs * deltaFactor);
        whInfo.increaseNumUnfinishedRestoreJobs(jobInfo.numRestoreJobs * deltaFactor);
        if (!isFinished) {
            whInfo.increaseNumUnfinishedLoadJobs(jobInfo.getNumLoadJobsSupportWh());
            defaultWhInfo.increaseNumUnfinishedLoadJobs(jobInfo.getNumLoadJobsNonSupportWh());
        }

        // Add job info to metric and manager.
        WarehouseMetricMgr.increaseUnfinishedQueries(wh.getFullName(), jobInfo.numQueryJobs * deltaFactor);
        WarehouseMetricMgr.increaseUnfinishedBackupJobs(wh.getFullName(), jobInfo.numBackupJobs * deltaFactor);
        WarehouseMetricMgr.increaseUnfinishedRestoreJobs(wh.getFullName(), jobInfo.numRestoreJobs * deltaFactor);
        addLoadJobs(wh.getFullName(), jobInfo.numBrokerLoadJobs, isFinished, this::genBrokerLoadJob);
        addLoadJobs(wh.getFullName(), jobInfo.numSparkLoadJobs, isFinished, this::genSparkLoadJob);
        addInsertLoadJobs(wh.getFullName(), jobInfo.numInsertLoadJobs, isFinished, false);
        addRoutineJobs(wh.getFullName(), jobInfo.numRoutineLoadJobs, isFinished);
        addStreamJobs(wh.getFullName(), jobInfo.numStreamLoadJobs, isFinished);
    }

    private BrokerLoadJob genBrokerLoadJob(String wh) throws MetaNotFoundException {
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.setCurrentWarehouse(wh);
        return new BrokerLoadJob(TEST_DB_ID, "broker-load-" + NEXT_INDEX.getAndIncrement(), null, null, context);
    }

    private SparkLoadJob genSparkLoadJob(String wh) throws MetaNotFoundException {
        return new SparkLoadJob(TEST_DB_ID, "spark-load-" + NEXT_INDEX.getAndIncrement(), null, null);
    }

    private RoutineLoadJob genRoutineLoadJob(String wh) {
        return new KafkaRoutineLoadJob(NEXT_INDEX.getAndIncrement(), "routine-load-" + NEXT_INDEX.getAndIncrement(),
                TEST_DB_ID, TEST_TABLE_ID, "brokerList", "topic");
    }

    private StreamLoadTask genStreamLoadJob(String wh) {
        Database db = GlobalStateMgr.getCurrentState().getDb(TEST_DB_ID);
        Table table = db.getTable(TEST_TABLE_ID);
        return new StreamLoadTask(NEXT_INDEX.getAndIncrement(),
                db,
                (OlapTable) table,
                "stream-load-" + NEXT_INDEX.getAndIncrement(),
                0L, System.currentTimeMillis(),
                false);
    }

    @FunctionalInterface
    interface FunctionWithException<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    private void addLoadJobs(String wh, long numJobs, boolean isFinished,
                             FunctionWithException<String, LoadJob, MetaNotFoundException> jobSupplier)
            throws MetaNotFoundException {
        for (int i = 0; i < numJobs; i++) {
            LoadJob job = jobSupplier.apply(wh);
            if (isFinished) {
                job.updateState(JobState.FINISHED);
            }
            loadMgr.replayCreateLoadJob(job);
        }
    }

    private void addInsertLoadJobs(String wh, long numJobs, boolean isFinished, boolean isStatisticsJob)
            throws UserException {

        for (int i = 0; i < numJobs; i++) {
            long jobId = loadMgr.registerLoadJob(
                    "insert-load-" + NEXT_INDEX.getAndIncrement(),
                    DB_NAME,
                    TEST_TABLE_ID,
                    EtlJobType.INSERT,
                    System.currentTimeMillis(),
                    0L,
                    TLoadJobType.INSERT_QUERY,
                    0L,
                    wh,
                    isStatisticsJob);
            if (isFinished) {
                loadMgr.getLoadJob(jobId).updateState(JobState.FINISHED);
            }
        }
    }

    private void addRoutineJobs(String wh, long numJobs, boolean isFinished)
            throws UserException {
        for (int i = 0; i < numJobs; i++) {
            RoutineLoadJob job = genRoutineLoadJob(wh);
            if (isFinished) {
                job.updateState(RoutineLoadJob.JobState.STOPPED, null, true);
            }
            routineLoadMgr.addRoutineLoadJob(job, DB_NAME);
        }
    }

    private void addStreamJobs(String wh, long numJobs, boolean isFinished)
            throws UserException {
        for (int i = 0; i < numJobs; i++) {
            StreamLoadTask job = genStreamLoadJob(wh);
            if (isFinished) {
                job.cancelAfterRestart();
            }
            streamLoadMgr.addLoadTask(job);
        }
    }

    private Map<String, WarehouseInfo> fetchWarehouseInfos() throws IOException {

        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + WarehouseAction.URI)
                .build();

        try (Response response = networkClient.newCall(request).execute()) {
            Assert.assertTrue(response.isSuccessful());

            Assert.assertNotNull(response.body());
            RestSuccessBaseResult<WarehouseAction.Result> res =
                    RestSuccessBaseResult.fromJson(response.body().charStream(), WarehouseAction.Result.class);
            Assert.assertEquals(ActionStatus.OK, res.status);

            return res.getResult().getWarehouses().stream()
                    .collect(Collectors.toMap(
                            WarehouseInfo::getWarehouse,
                            Function.identity()
                    ));
        }
    }
}
