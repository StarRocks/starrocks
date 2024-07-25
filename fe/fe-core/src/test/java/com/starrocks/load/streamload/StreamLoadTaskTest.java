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

package com.starrocks.load.streamload;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.common.ErrorCode.ERR_NO_PARTITIONS_HAVE_DATA_LOAD;

public class StreamLoadTaskTest {

    @Mocked
    private DefaultCoordinator coord;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private WarehouseManager warehouseManager;

    @Mocked
    private Warehouse warehouse;

    private StreamLoadTask streamLoadTask;

    @Before
    public void setUp() {
        long id = 123L;
        String label = "label_abc";
        long timeoutMs = 10000L;
        long createTimeMs = System.currentTimeMillis();
        boolean isRoutineLoad = false;
        long warehouseId = 0L;
        streamLoadTask =
                new StreamLoadTask(id, new Database(), new OlapTable(), label, timeoutMs, createTimeMs, isRoutineLoad,
                        warehouseId);
    }

    @Test
    public void testAfterCommitted() throws UserException {
        streamLoadTask.setCoordinator(coord);
        new Expectations() {
            {
                coord.isProfileAlreadyReported();
                result = false;
            }
        };
        TUniqueId labelId = new TUniqueId(2, 3);
        streamLoadTask.setTUniqueId(labelId);
        QeProcessorImpl.INSTANCE.registerQuery(streamLoadTask.getTUniqueId(), coord);
        Assert.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;
        streamLoadTask.afterCommitted(txnState, txnOperated);
        Assert.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
    }

    @Test
    public void testAfterAborted() throws UserException {
        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;

        TUniqueId labelId = new TUniqueId(2, 3);
        streamLoadTask.setTUniqueId(labelId);
        QeProcessorImpl.INSTANCE.registerQuery(streamLoadTask.getTUniqueId(), coord);
        Assert.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        streamLoadTask.afterAborted(txnState, txnOperated, "");
        Assert.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
    }

    @Test
    public void testNoPartitionsHaveDataLoad() {
        Map<String, String> loadCounters = Maps.newHashMap();
        loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, "0");
        loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "0");
        loadCounters.put(LoadJob.UNSELECTED_ROWS, "0");
        loadCounters.put(LoadJob.LOADED_BYTES, "0");

        streamLoadTask.setCoordinator(coord);
        new Expectations() {
            {
                coord.join(anyInt);
                result = true;
                coord.getLoadCounters();
                returns(null, loadCounters);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(UserException.class, ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg(),
                () -> Deencapsulation.invoke(streamLoadTask, "unprotectedWaitCoordFinish"));
        ExceptionChecker.expectThrowsWithMsg(UserException.class, ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg(),
                () -> Deencapsulation.invoke(streamLoadTask, "unprotectedWaitCoordFinish"));
    }

    @Test
    public void testGetShowInfo() {
        List<String> showInfo = streamLoadTask.getShowInfo();
        Assert.assertNotNull(showInfo);
        // warehouse
        Assert.assertEquals("", showInfo.get(showInfo.size() - 1));
        List<String> showBriefInfo = streamLoadTask.getShowBriefInfo();
        Assert.assertNotNull(showBriefInfo);
        // warehouse
        Assert.assertEquals("", showBriefInfo.get(showInfo.size() - 1));
        TStreamLoadInfo tStreamLoadInfo = streamLoadTask.toThrift();
        Assert.assertNotNull(tStreamLoadInfo);
        // warehouse
        Assert.assertEquals("", tStreamLoadInfo.getWarehouse());

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouse(anyLong);
                minTimes = 0;
                result = warehouse;

                warehouse.getName();
                minTimes = 0;
                result = "test_warehouse";
            }
        };

        showInfo = streamLoadTask.getShowInfo();
        Assert.assertNotNull(showInfo);
        // warehouse
        Assert.assertEquals("test_warehouse", showInfo.get(showInfo.size() - 1));
        showBriefInfo = streamLoadTask.getShowBriefInfo();
        Assert.assertNotNull(showBriefInfo);
        // warehouse
        Assert.assertEquals("test_warehouse", showBriefInfo.get(showInfo.size() - 1));
        tStreamLoadInfo = streamLoadTask.toThrift();
        Assert.assertNotNull(tStreamLoadInfo);
        // warehouse
        Assert.assertEquals("test_warehouse", tStreamLoadInfo.getWarehouse());
    }
}
