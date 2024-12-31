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

package com.starrocks.load.batchwrite;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.LoadException;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.streamload.StreamLoadHttpHeader;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LoadExecutorTest extends BatchWriteTestBase {

    private String label;
    private TUniqueId loadId;
    StreamLoadKvParams kvParams;
    private StreamLoadInfo streamLoadInfo;
    private TestLoadExecuteCallback loadExecuteCallback;

    @Mocked
    private Coordinator coordinator;
    private TestCoordinatorFactor coordinatorFactory;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME_1)
                .useDatabase(DB_NAME_1)
                .withTable(new MTable(TABLE_NAME_1_1, Arrays.asList("c0 INT", "c1 STRING")));
        DATABASE_1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME_1);
        TABLE_1_1 = (OlapTable) DATABASE_1.getTable(TABLE_NAME_1_1);
    }

    @Before
    public void setup() throws Exception {
        label = "batch_write_" + DebugUtil.printId(UUIDUtil.toTUniqueId(UUID.randomUUID()));
        loadId = UUIDUtil.toTUniqueId(UUID.randomUUID());

        Map<String, String> map = new HashMap<>();
        map.put(StreamLoadHttpHeader.HTTP_FORMAT, "json");
        map.put(StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE, "true");
        map.put(StreamLoadHttpHeader.HTTP_BATCH_WRITE_ASYNC, "true");
        kvParams = new StreamLoadKvParams(map);
        streamLoadInfo = StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), kvParams);
        loadExecuteCallback = new TestLoadExecuteCallback();
        coordinatorFactory = new TestCoordinatorFactor(coordinator);
    }

    @Test
    public void testLoadSuccess() {
        LoadExecutor executor = new LoadExecutor(
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback
            );

        new Expectations() {
            {
                coordinator.join((anyInt));
                result = true;
                coordinator.getExecStatus();
                result = new Status();
                coordinator.getCommitInfos();
                result = buildCommitInfos();
            }
        };

        executor.run();
        assertNull(executor.getFailure());
        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        TransactionStatus txnStatus =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .getLabelStatus(DATABASE_1.getId(), label).getStatus();
        assertEquals(TransactionStatus.VISIBLE, txnStatus);
    }

    @Test
    public void testPlanExecuteFail() {
        LoadExecutor executor = new LoadExecutor(
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback
        );

        Status status = new Status(TStatusCode.INTERNAL_ERROR, "artificial failure");
        new Expectations() {
            {
                coordinator.join((anyInt));
                result = true;
                coordinator.getExecStatus();
                result = status;
            }
        };

        Exception expectException = new LoadException(
                String.format("Failed to execute load, status code: %s, error message: %s",
                        status.getErrorCodeString(), status.getErrorMsg()));
        testLoadFailBase(executor, expectException, TransactionStatus.ABORTED);
    }

    @Test
    public void testPlanExecuteTimeout() {
        LoadExecutor executor = new LoadExecutor(
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback
        );

        new Expectations() {
            {
                coordinator.join((anyInt));
                result = false;
            }
        };

        Exception expectException = new LoadException("Timeout to execute load");
        testLoadFailBase(executor, expectException, TransactionStatus.ABORTED);
    }

    @Test
    public void testTableDoesNotExist() {
        String fakeTableName = TABLE_NAME_1_1 + "_fake";
        LoadExecutor executor = new LoadExecutor(
                new TableId(DB_NAME_1, fakeTableName),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback
        );

        Exception expectException = new LoadException(
                String.format("Table [%s.%s] does not exist", DB_NAME_1, fakeTableName));
        testLoadFailBase(executor, expectException, TransactionStatus.UNKNOWN);
    }

    @Test
    public void testMaxFilterRatio() {
        LoadExecutor executor = new LoadExecutor(
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback
        );

        Map<String, String> counters = new HashMap<>();
        counters.put(LoadEtlTask.DPP_NORMAL_ALL, "100");
        counters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "100");
        String trackingUrl = "test_tracking_url";
        new Expectations() {
            {
                coordinator.join((anyInt));
                result = true;
                coordinator.getExecStatus();
                result = new Status();
                coordinator.getCommitInfos();
                result = buildCommitInfos();
                coordinator.getLoadCounters();
                result = counters;
                coordinator.getTrackingUrl();
                result = trackingUrl;
            }
        };

        Exception expectException = new LoadException(
                "There is data quality issue, please check the tracking url for details." +
                        " Max filter ratio: 0.0. The tracking url: " + trackingUrl);
        testLoadFailBase(executor, expectException, TransactionStatus.ABORTED);
    }

    private void testLoadFailBase(
            LoadExecutor executor, Exception expectedException, TransactionStatus expectedTxnStatus) {
        executor.run();
        Throwable throwable = executor.getFailure();
        assertNotNull(throwable);
        assertSame(expectedException.getClass(), throwable.getClass());
        assertTrue(throwable.getMessage().contains(expectedException.getMessage()));
        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        TransactionStatus txnStatus =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .getLabelStatus(DATABASE_1.getId(), label).getStatus();
        assertEquals(expectedTxnStatus, txnStatus);
    }

    @Test
    public void testIsActive() {
        LoadExecutor executor = new LoadExecutor(
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback
        );

        assertTrue(executor.isActive());

        new Expectations() {
            {
                coordinator.join((anyInt));
                result = true;
                coordinator.getExecStatus();
                result = new Status(TStatusCode.INTERNAL_ERROR, "artificial failure");
            }
        };
        executor.run();
        assertFalse(executor.isActive());
    }

    @Test
    public void testContainCoordinatorBackend() {
        LoadExecutor executor = new LoadExecutor(
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback
        );
        assertFalse(executor.containCoordinatorBackend(10001L));
        assertTrue(executor.containCoordinatorBackend(10002L));
    }

    private static class TestLoadExecuteCallback implements LoadExecuteCallback {

        private final List<String> finishedLoads = new ArrayList<>();

        public List<String> getFinishedLoads() {
            return finishedLoads;
        }

        @Override
        public void finishLoad(String label) {
            finishedLoads.add(label);
        }
    }
    
    private static class TestCoordinatorFactor implements Coordinator.Factory {

        private final Coordinator coordinator;
        
        public TestCoordinatorFactor(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public Coordinator createStreamLoadScheduler(LoadPlanner loadPlanner) {
            return coordinator;
        }
        
        @Override
        public Coordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                List<ScanNode> scanNodes, TDescriptorTable descTable) {
            return coordinator;
        }

        @Override
        public Coordinator createInsertScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                 List<ScanNode> scanNodes, TDescriptorTable descTable) {
            return coordinator;
        }

        @Override
        public Coordinator createBrokerLoadScheduler(LoadPlanner loadPlanner) {
            return coordinator;
        }


        @Override
        public Coordinator createSyncStreamLoadScheduler(StreamLoadPlanner planner, TNetworkAddress address) {
            return coordinator;
        }

        @Override
        public Coordinator createNonPipelineBrokerLoadScheduler(Long jobId, TUniqueId queryId,
                                                                DescriptorTable descTable, List<PlanFragment> fragments,
                                                                List<ScanNode> scanNodes, String timezone,
                                                                long startTime, Map<String, String> sessionVariables,
                                                                ConnectContext context, long execMemLimit,
                                                                long warehouseId) {
            return coordinator;
        }

        @Override
        public Coordinator createBrokerExportScheduler(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                                       List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                                       String timezone, long startTime,
                                                       Map<String, String> sessionVariables, long execMemLimit,
                                                       long warehouseId) {
            return coordinator;
        }

        @Override
        public Coordinator createRefreshDictionaryCacheScheduler(ConnectContext context, TUniqueId queryId,
                                                                 DescriptorTable descTable,
                                                                 List<PlanFragment> fragments,
                                                                 List<ScanNode> scanNodes) {
            return coordinator;
        }
    }
}
