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

import com.google.common.collect.Lists;
import com.starrocks.backup.CatalogMocker;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.ActionStatus;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.load.LoadJobVisibility;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TGetLoadsParams;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TTrackingLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnStateCallbackFactory;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamLoadManagerTest {

    private static final Logger LOG = LogManager.getLogger(StreamLoadManagerTest.class);

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private EditLog editLog;

    private SystemInfoService systemInfoService;
    private GlobalTransactionMgr globalTransactionMgr;
    private Database db;
    private NodeMgr nodeMgr;

    @BeforeEach
    public void setUp() {
        globalTransactionMgr = new GlobalTransactionMgr(globalStateMgr);
        FeConstants.runningUnitTest = true;

        try {
            db = CatalogMocker.mockDb();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assertions.fail();
        }

        new MockUp<EditLog>() {
            @Mock
            public void logSaveTransactionId(long transactionId) {
            }

            @Mock
            public void logInsertTransactionState(TransactionState transactionState) {
            }

            @Mock
            public void logCreateStreamLoadJob(StreamLoadTask streamLoadTask,
                                               WALApplier walApplier) {
                walApplier.apply(streamLoadTask);
            }

            @Mock
            public void logCreateMultiStmtStreamLoadJob(
                    StreamLoadMultiStmtTask streamLoadTask, WALApplier walApplier) {
                walApplier.apply(streamLoadTask);
            }
        };

        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(anyString);
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getTable(anyString, anyString);
                minTimes = 0;
                result = db.getTable(CatalogMocker.TEST_TBL_ID);

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new MockUp<Database>() {
            @Mock
            public long getDataQuota() {
                return 100;
            }
        };

        globalTransactionMgr.addDatabaseTransactionMgr(db.getId());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
                minTimes = 0;
                result = globalTransactionMgr;

                nodeMgr = new NodeMgr();
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                GlobalStateMgr.getCurrentState().getNextId();
                minTimes = 0;
                result = 1001L;
            }
        };

        new Expectations(nodeMgr) {
            {
                systemInfoService = new SystemInfoService();
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        new Expectations(systemInfoService) {
            {
                systemInfoService.getBackendIds(true);
                minTimes = 0;
                result = Lists.newArrayList();
            }
        };
    }

    @Test
    public void testBeginStreamLoadTask() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                "test_db", "test_tbl", "label1", "", "", 100000, 1, 0, resp);

        Map<String, StreamLoadTask> idToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "idToStreamLoadTask");
        Assertions.assertEquals(1, idToStreamLoadTask.size());
        StreamLoadTask task = idToStreamLoadTask.values().iterator().next();
        Assertions.assertEquals("label1", task.getLabel());
        Assertions.assertEquals("test_db", task.getDBName());
        Assertions.assertEquals(20000, task.getDBId());
        Assertions.assertEquals("test_tbl", task.getTableName());
    }

    @Test
    public void testChannelIdEqualChannelNum() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                "test_db", "test_tbl", "label1", "", "", 100000, 1, 1, resp);
        Map<String, StreamLoadTask> idToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "idToStreamLoadTask");
        Assertions.assertEquals(1, idToStreamLoadTask.size());
        StreamLoadTask task = idToStreamLoadTask.values().iterator().next();
        Assertions.assertEquals("CANCELLED", task.getStateName());
    }

    @Test
    public void testGetTaskByName() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                "test_db", "test_tbl", "label1", "", "", 100000, 5, 0, resp);
        List<AbstractStreamLoadTask> tasks = streamLoadManager.getTaskByName("label1");
        Assertions.assertEquals(1, tasks.size());
        Assertions.assertEquals("label1", tasks.get(0).getLabel());
    }

    @Test
    public void testGetTaskByNameWithNullLabelName() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                "test_db", "test_tbl", "label1", "", "", 100000, 5, 0, resp);
        streamLoadManager.beginLoadTaskFromFrontend(
                "test_db", "test_tbl", "label2", "", "", 100000, 5, 0, resp);
        List<AbstractStreamLoadTask> tasks = streamLoadManager.getTaskByName(null);
        Assertions.assertEquals(2, tasks.size());
    }

    @Test
    public void testGetTaskByIdWhenMatched() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                "test_db", "test_tbl", "label1", "", "", 100000, 5, 0, resp);
        AbstractStreamLoadTask task = streamLoadManager.getTaskById(1001L);
        Assertions.assertNotNull(task);
        Assertions.assertEquals("label1", task.getLabel());
    }

    @Test
    public void testGetTaskByIdWhenNotMatched() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                "test_db", "test_tbl", "label1", "", "", 100000, 5, 0, resp);
        AbstractStreamLoadTask task = streamLoadManager.getTaskById(1002L);
        Assertions.assertNull(task);
    }

    @Test
    public void testMultiStatementTrackingSqlFindsOnlyItsSubTask(
            @Mocked LoadMgr loadMgr, @Mocked RoutineLoadMgr routineLoadMgr) throws Exception {
        // ConnectContext is mocked here, so no caller could be authorized; which jobs a caller may see is covered
        // by LoadJobVisibilityTest, and here every job is shown in full.
        new MockUp<LoadJobVisibility>() {
            @Mock
            public LoadJobVisibility.Access accessOf(ConnectContext context, LoadJob job) {
                return LoadJobVisibility.Access.FULL;
            }

            @Mock
            public List<LoadJobVisibility.VisibleTask> visibleTasks(ConnectContext context,
                                                                     AbstractStreamLoadTask task) {
                return LoadJobVisibility.allTasksInFull(task);
            }
        };
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        StreamLoadMultiStmtTask parent = new StreamLoadMultiStmtTask(
                1001L, db, "multi_tracking", "", "", 100000L,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Map<String, AbstractStreamLoadTask> tasks =
                Deencapsulation.getField(streamLoadManager, "idToStreamLoadTask");
        tasks.put(parent.getLabel(), parent);

        Map<String, StreamLoadTask> children = Deencapsulation.getField(parent, "taskMaps");
        for (long childId : List.of(1002L, 1003L)) {
            OlapTable table = new OlapTable();
            table.setName("tbl_" + childId);
            StreamLoadTask child = new StreamLoadTask(childId, db, table, parent.getLabel(), "", "",
                    100000L, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
            Deencapsulation.setField(child, "trackingUrl",
                    "http://127.0.0.1:8040/api/_load_error_log?file=error_log_" + childId);
            Deencapsulation.setField(child, "state", StreamLoadTask.State.CANCELLED);
            children.put(table.getName(), child);
        }
        new Expectations() {
            {
                globalStateMgr.getStreamLoadMgr();
                result = streamLoadManager;
                globalStateMgr.getLoadMgr();
                result = loadMgr;
                globalStateMgr.getRoutineLoadMgr();
                result = routineLoadMgr;
                // These IDs belong only to stream-load children. Do not let cascading mocks
                // fabricate a broker or routine-load job that short-circuits getTrackingLoads.
                loadMgr.getLoadJob(anyLong);
                result = null;
                routineLoadMgr.getJob(anyLong);
                result = null;
            }
        };

        Assertions.assertSame(parent, streamLoadManager.getTaskById(parent.getId()));
        Assertions.assertNull(streamLoadManager.getTaskById(9999L));
        FrontendServiceImpl service = new FrontendServiceImpl(null);
        for (StreamLoadTask child : parent.getTasks()) {
            TLoadInfo displayed = child.toThrift().get(0);
            String trackingSql = displayed.getTracking_sql();
            long jobId = Long.parseLong(trackingSql.substring(trackingSql.indexOf("job_id=") + 7));
            TGetLoadsParams request = new TGetLoadsParams().setJob_id(jobId);
            Assertions.assertSame(child, streamLoadManager.getTaskById(jobId));

            // Exercise the real FE endpoints behind the advertised tracking SQL and load views.
            List<TTrackingLoadInfo> tracking = service.getTrackingLoads(request).getTrackingLoads();
            Assertions.assertEquals(1, tracking.size());
            Assertions.assertEquals(child.getId(), tracking.get(0).getJob_id());
            Assertions.assertEquals(List.of(displayed.getUrl()), tracking.get(0).getUrls());
            List<TStreamLoadInfo> streamLoads = service.getStreamLoads(request).getLoads();
            Assertions.assertEquals(1, streamLoads.size());
            Assertions.assertEquals(child.getId(), streamLoads.get(0).getId());
            List<TLoadInfo> loads = service.getLoads(request).getLoads();
            Assertions.assertEquals(1, loads.size());
            Assertions.assertEquals(child.getId(), loads.get(0).getJob_id());
            Assertions.assertEquals(1, service.getLoads(new TGetLoadsParams().setJob_id(jobId)
                    .setDb(db.getFullName())).getLoads().size());
            Assertions.assertTrue(service.getLoads(new TGetLoadsParams().setJob_id(jobId)
                    .setTable_name("another_table")).getLoads().isEmpty());
            Assertions.assertTrue(service.getLoads(new TGetLoadsParams().setJob_id(jobId)
                    .setUser("another_user")).getLoads().isEmpty());
        }
    }

    @Test
    public void testStreamLoadTaskAfterCommit() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromBackend(
                "test_db", "test_tbl", "label2", null, "", "", 100000, resp,
                false, WarehouseManager.DEFAULT_RESOURCE, 10001);

        Map<String, StreamLoadTask> idToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "idToStreamLoadTask");
        Assertions.assertEquals(1, idToStreamLoadTask.size());
        StreamLoadTask task = idToStreamLoadTask.get("label2");

        TransactionState state = new TransactionState();
        task.afterCommitted(state);
        Assertions.assertNotEquals(-1, task.commitTimeMs());
        Assertions.assertTrue(task.isUnreversibleState());
        Assertions.assertFalse(task.isFinalState());

        streamLoadManager.cleanSyncStreamLoadTasks();
        Assertions.assertEquals(1, streamLoadManager.getStreamLoadTaskCount());
    }

    // ---- Cover lines 149-153: beginMultiStatementLoadTask label exists in txn history ----
    @Test
    public void testBeginMultiStmtLoadTaskLabelAlreadyExists() throws StarRocksException {
        String label = "label_exists_in_txn";
        TransactionState existingTxn = new TransactionState();
        existingTxn.setTransactionStatus(TransactionStatus.VISIBLE);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getLabelTransactionState(long dbId, String lbl) {
                if (label.equals(lbl)) {
                    return existingTxn;
                }
                return null;
            }
        };

        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginMultiStatementLoadTask(
                CatalogMocker.TEST_DB_NAME, label, "", "127.0.0.1", 100000L, resp,
                WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(ActionStatus.LABEL_ALREADY_EXISTS, resp.status);
    }

    // ---- Cover line 406: tryPrepareLoadTaskTxn ----
    @Test
    public void testTryPrepareLoadTaskTxn() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                "test_db", "test_tbl", "prep_label", "", "", 100000, 1, 0, resp);

        TransactionResult prepResp = new TransactionResult();
        streamLoadManager.tryPrepareLoadTaskTxn("prep_label", 5000, prepResp);
        Assertions.assertTrue(prepResp.stateOK());
    }

    @Test
    public void testTryPrepareLoadTaskTxnNotExist() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                "test_db", "test_tbl", "some_label", "", "", 100000, 1, 0, resp);
        Assertions.assertThrows(StarRocksException.class, () -> {
            streamLoadManager.tryPrepareLoadTaskTxn("not_exist", 5000, new TransactionResult());
        });
    }

    // ---- Cover lines 511-514: cleanOldStreamLoadTasks with aborting MultiStmtTasks ----
    @Test
    public void testCleanOldStreamLoadTasksWithAbortingMultiStmt() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult beginResp = new TransactionResult();
        streamLoadManager.beginMultiStatementLoadTask(
                CatalogMocker.TEST_DB_NAME, "aborting_label", "", "127.0.0.1",
                100000L, beginResp, WarehouseManager.DEFAULT_RESOURCE);
        AbstractStreamLoadTask task = streamLoadManager.getTaskByLabel("aborting_label");
        Deencapsulation.setField(task, "state",
                StreamLoadMultiStmtTask.State.ABORTING);

        streamLoadManager.cleanOldStreamLoadTasks(false);
        Assertions.assertNotNull(streamLoadManager.getTaskByLabel("aborting_label"));
    }

    @Test
    public void testTheOldestFinishedSyncTasksAreEvictedFirst() {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        Config.stream_load_task_keep_max_num = 5;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr();
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            for (int i = 0; i < 8; i++) {
                StreamLoadTask task = streamLoadManager.createLoadTaskWithoutLock(testDb, table, "rl_" + i, "", "",
                        100000, true, WarehouseManager.DEFAULT_RESOURCE);
                task.setTxnId(1000 + i);
                streamLoadManager.addLoadTask(task, false);
                // every task but rl_1 finishes
                if (i != 1) {
                    Deencapsulation.setField(task, "state", StreamLoadTask.State.FINISHED);
                    Deencapsulation.setField(task, "endTimeMs", System.currentTimeMillis());
                }
            }
            // each added task evicted the oldest finished one, and the running one stays
            Assertions.assertEquals(5, streamLoadManager.getStreamLoadTaskCount());
            for (String label : List.of("rl_1", "rl_4", "rl_5", "rl_6", "rl_7")) {
                Assertions.assertNotNull(streamLoadManager.getTaskByLabel(label), label);
            }
            Assertions.assertNull(streamLoadManager.getSyncSteamLoadTaskByTxnId(1000));
            Assertions.assertNotNull(streamLoadManager.getSyncSteamLoadTaskByTxnId(1001));
        } finally {
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testASyncTaskAddedAtTheLimitKeepsItsPlaceInTheEvictionOrder() {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        Config.stream_load_task_keep_max_num = 3;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr();
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            // rl_3 and rl_4 are added at the limit while every task runs, so there is nothing to evict
            List<StreamLoadTask> tasks = Lists.newArrayList();
            for (int i = 0; i < 5; i++) {
                tasks.add(addSyncTask(streamLoadManager, testDb, table, i));
            }
            Assertions.assertEquals(tasks, Lists.newArrayList(streamLoadManager.syncStreamLoadTaskQueue));

            // once they finish, the next tasks evict the oldest finished ones first, rl_3 included
            tasks.forEach(StreamLoadManagerTest::finishTask);
            finishTask(addSyncTask(streamLoadManager, testDb, table, 5));
            addSyncTask(streamLoadManager, testDb, table, 6);
            Assertions.assertEquals(3, streamLoadManager.getStreamLoadTaskCount());
            Assertions.assertNull(streamLoadManager.getTaskByLabel("rl_3"));
            for (String label : List.of("rl_4", "rl_5", "rl_6")) {
                Assertions.assertNotNull(streamLoadManager.getTaskByLabel(label), label);
            }
        } finally {
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testConcurrentSyncTasksKeepTheNewestHistoryAfterTheLimitIsLowered() throws Exception {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        CountDownLatch waitingForLock = new CountDownLatch(2);
        CountDownLatch takeLock = new CountDownLatch(1);
        AtomicBoolean coordinateLock = new AtomicBoolean(false);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Config.stream_load_task_keep_max_num = 5;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr() {
                private final ThreadLocal<Boolean> coordinated = ThreadLocal.withInitial(() -> false);

                @Override
                protected void writeLock() {
                    if (coordinateLock.get() && !coordinated.get()) {
                        coordinated.set(true);
                        waitingForLock.countDown();
                        try {
                            if (!takeLock.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting to take the manager lock");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                    super.writeLock();
                }
            };
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            for (int i = 0; i < 5; i++) {
                finishTask(addSyncTask(streamLoadManager, testDb, table, i));
            }
            Config.stream_load_task_keep_max_num = 3;
            coordinateLock.set(true);
            // Both registrations reach their first write-lock attempt before either one can clean up.
            Future<?> first = executor.submit(() -> addSyncTask(streamLoadManager, testDb, table, 5));
            Future<?> second = executor.submit(() -> addSyncTask(streamLoadManager, testDb, table, 6));
            Assertions.assertTrue(waitingForLock.await(10, TimeUnit.SECONDS));
            takeLock.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);

            Assertions.assertEquals(3, streamLoadManager.getStreamLoadTaskCount());
            for (String label : List.of("rl_4", "rl_5", "rl_6")) {
                Assertions.assertNotNull(streamLoadManager.getTaskByLabel(label), label);
            }
            Assertions.assertNull(streamLoadManager.getSyncSteamLoadTaskByTxnId(1003));
            Assertions.assertNotNull(streamLoadManager.getSyncSteamLoadTaskByTxnId(1004));
        } finally {
            takeLock.countDown();
            executor.shutdownNow();
            boolean terminated;
            try {
                terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
            } finally {
                Config.stream_load_task_keep_max_num = savedKeepMaxNum;
            }
            Assertions.assertTrue(terminated);
        }
    }

    @Test
    public void testRegistrationLeavesNonFinalMultiStatementCleanupToThePeriodicSweep() {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        Config.stream_load_task_keep_max_num = 5;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr();
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            StreamLoadMultiStmtTask running = streamLoadManager.createMultiStatementLoadTask(
                    testDb, "running", "", "", 100000, WarehouseManager.DEFAULT_RESOURCE);
            StreamLoadMultiStmtTask aborting = streamLoadManager.createMultiStatementLoadTask(
                    testDb, "aborting", "", "", 100000, WarehouseManager.DEFAULT_RESOURCE);
            Deencapsulation.setField(aborting, "state", StreamLoadMultiStmtTask.State.ABORTING);
            streamLoadManager.addLoadTask(running, false);
            streamLoadManager.addLoadTask(aborting, false);

            AtomicInteger timeoutChecks = new AtomicInteger();
            AtomicInteger abortRetries = new AtomicInteger();
            new MockUp<StreamLoadMultiStmtTask>() {
                @Mock
                public boolean checkNeedRemove(long currentMs, boolean isForce) {
                    timeoutChecks.incrementAndGet();
                    return false;
                }

                @Mock
                public boolean retryAbortIfNeeded(long currentMs) {
                    ReentrantReadWriteLock managerLock = Deencapsulation.getField(streamLoadManager, "lock");
                    Assertions.assertFalse(managerLock.isWriteLockedByCurrentThread());
                    abortRetries.incrementAndGet();
                    return false;
                }
            };

            Config.stream_load_task_keep_max_num = 2;
            addSyncTask(streamLoadManager, testDb, table, 0);
            Assertions.assertEquals(0, timeoutChecks.get());
            Assertions.assertEquals(0, abortRetries.get());
            Assertions.assertNotNull(streamLoadManager.getTaskByLabel("running"));
            Assertions.assertNotNull(streamLoadManager.getTaskByLabel("aborting"));

            streamLoadManager.cleanOldStreamLoadTasks(false);
            Assertions.assertEquals(1, timeoutChecks.get());
            Assertions.assertEquals(1, abortRetries.get());
        } finally {
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testRepeatedBackendBeginFailuresKeepOnlyTheLatestSyncTask() throws StarRocksException {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        boolean savedDisableLoadJob = Config.disable_load_job;
        Config.stream_load_task_keep_max_num = 2;
        try {
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            // The GlobalStateMgr of this class is @Mocked, so a real begin fails on its FailoverGroupMgr, a Thread that
            // JMockit cannot mock, before it checks the label. Stand in for the label check: the first begin of a label
            // gets a transaction that stays in PREPARE, and every later begin of it fails on the used label. A begin
            // rejected before the label check (disable_load_job) runs the real code.
            Set<String> preparedLabels = new HashSet<>();
            new MockUp<GlobalTransactionMgr>() {
                @Mock
                public long beginTransaction(Invocation invocation, long dbId, List<Long> tableIdList, String label,
                                             TUniqueId requestId, TransactionState.TxnCoordinator coordinator,
                                             TransactionState.LoadJobSourceType sourceType, long listenerId,
                                             long timeoutSecond, ComputeResource computeResource)
                        throws LabelAlreadyUsedException {
                    if (Config.disable_load_job) {
                        Long txnId = invocation.proceed();
                        return txnId;
                    }
                    if (!preparedLabels.add(label)) {
                        throw new LabelAlreadyUsedException(label, TransactionStatus.PREPARE);
                    }
                    return 5000L;
                }
            };
            for (boolean disabled : List.of(false, true)) {
                Config.disable_load_job = false;
                StreamLoadMgr streamLoadManager = new StreamLoadMgr();
                String label = disabled ? "disabled_begin" : "used_begin";
                if (!disabled) {
                    TransactionResult first = new TransactionResult();
                    streamLoadManager.beginLoadTaskFromBackend(
                            CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, label, new TUniqueId(1, 0),
                            "", "", 100000, first, false, WarehouseManager.DEFAULT_RESOURCE, 10001);
                    Assertions.assertTrue(first.stateOK());
                }
                StreamLoadTask history = addSyncTask(streamLoadManager, testDb, table, 0);
                finishTask(history);
                Config.disable_load_job = disabled;
                // The successful transaction stays in PREPARE. Each retry represents a new backend request,
                // so its distinct request ID must fail on label reuse rather than succeed as a duplicate request.
                for (int retry = 0; retry < 5; retry++) {
                    AbstractStreamLoadTask previous = streamLoadManager.getTaskByLabel(label);
                    TransactionResult response = new TransactionResult();
                    streamLoadManager.beginLoadTaskFromBackend(
                            CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, label,
                            new TUniqueId(disabled ? 2 : 1, retry + 1), "", "", 100000, response, false,
                            WarehouseManager.DEFAULT_RESOURCE, 10001);
                    Assertions.assertEquals(disabled ? ActionStatus.FAILED : ActionStatus.LABEL_ALREADY_EXISTS,
                            response.status);
                    StreamLoadTask latest = (StreamLoadTask) streamLoadManager.getTaskByLabel(label);
                    Assertions.assertNotSame(previous, latest);
                    Assertions.assertEquals(StreamLoadTask.State.CANCELLED, latest.getState());
                    Assertions.assertEquals(2, streamLoadManager.getStreamLoadTaskCount());
                    Assertions.assertEquals(List.of(history, latest),
                            Lists.newArrayList(streamLoadManager.syncStreamLoadTaskQueue));
                }
            }
        } finally {
            Config.disable_load_job = savedDisableLoadJob;
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testReplacingASyncTaskAtTheLimitKeepsTheOtherHistory() {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        Config.stream_load_task_keep_max_num = 2;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr();
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            addSyncTask(streamLoadManager, testDb, table, 0);
            StreamLoadTask history = addSyncTask(streamLoadManager, testDb, table, 1);
            finishTask(history);
            StreamLoadTask replacement = streamLoadManager.createLoadTaskWithoutLock(testDb, table, "rl_0", "", "",
                    100000, true, WarehouseManager.DEFAULT_RESOURCE);
            replacement.setTxnId(2000);
            streamLoadManager.addLoadTask(replacement, false);
            streamLoadManager.addLoadTask(replacement, false);

            Assertions.assertEquals(2, streamLoadManager.getStreamLoadTaskCount());
            Assertions.assertSame(replacement, streamLoadManager.getTaskByLabel("rl_0"));
            Assertions.assertSame(replacement, streamLoadManager.getSyncSteamLoadTaskByTxnId(2000));
            Assertions.assertEquals(List.of(history, replacement),
                    Lists.newArrayList(streamLoadManager.syncStreamLoadTaskQueue));

            StreamLoadTask next = addSyncTask(streamLoadManager, testDb, table, 2);
            Assertions.assertNull(streamLoadManager.getTaskByLabel("rl_1"));
            Assertions.assertEquals(List.of(replacement, next),
                    Lists.newArrayList(streamLoadManager.syncStreamLoadTaskQueue));
        } finally {
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testReplacingSyncAndNonSyncTasksKeepsOnlyTheCurrentSyncTaskQueued() {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        addSyncTask(streamLoadManager, testDb, table, 0);
        StreamLoadTask other = addSyncTask(streamLoadManager, testDb, table, 1);
        StreamLoadMultiStmtTask nonSync = streamLoadManager.createMultiStatementLoadTask(
                testDb, "rl_0", "", "", 100000, WarehouseManager.DEFAULT_RESOURCE);
        streamLoadManager.addLoadTask(nonSync, false);
        streamLoadManager.addLoadTask(nonSync, false);
        Assertions.assertEquals(List.of(other), Lists.newArrayList(streamLoadManager.syncStreamLoadTaskQueue));

        StreamLoadTask sync = streamLoadManager.createLoadTaskWithoutLock(testDb, table, "rl_0", "", "",
                100000, true, WarehouseManager.DEFAULT_RESOURCE);
        sync.setTxnId(3000);
        streamLoadManager.addLoadTask(sync, false);
        Assertions.assertEquals(List.of(other, sync), Lists.newArrayList(streamLoadManager.syncStreamLoadTaskQueue));
        sync.setIsSyncStreamLoad(false);
        streamLoadManager.addLoadTask(sync, false);
        Assertions.assertEquals(List.of(other), Lists.newArrayList(streamLoadManager.syncStreamLoadTaskQueue));
        sync.setIsSyncStreamLoad(true);
        streamLoadManager.addLoadTask(sync, false);
        Assertions.assertEquals(List.of(other, sync), Lists.newArrayList(streamLoadManager.syncStreamLoadTaskQueue));
        Assertions.assertEquals(2, streamLoadManager.getStreamLoadTaskCount());
    }

    private static StreamLoadTask addSyncTask(StreamLoadMgr streamLoadManager, Database db, OlapTable table, int i) {
        StreamLoadTask task = streamLoadManager.createLoadTaskWithoutLock(db, table, "rl_" + i, "", "", 100000, true,
                WarehouseManager.DEFAULT_RESOURCE);
        task.setTxnId(1000 + i);
        streamLoadManager.addLoadTask(task, false);
        return task;
    }

    private static void finishTask(StreamLoadTask task) {
        Deencapsulation.setField(task, "state", StreamLoadTask.State.FINISHED);
        Deencapsulation.setField(task, "endTimeMs", System.currentTimeMillis());
    }

    // ---- Cover lines 538-541: cleanSyncStreamLoadTasks actually removes tasks ----
    @Test
    public void testCleanSyncStreamLoadTasksRemovesFinished() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromBackend(
                CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME,
                "sync_label", null, "", "", 100000, resp, false,
                WarehouseManager.DEFAULT_RESOURCE, 10001);

        StreamLoadTask task = (StreamLoadTask) streamLoadManager.getTaskByLabel(
                "sync_label");
        task.setIsSyncStreamLoad(true);
        Deencapsulation.setField(task, "state", StreamLoadTask.State.FINISHED);
        Deencapsulation.setField(task, "endTimeMs",
                System.currentTimeMillis() - 10000);

        streamLoadManager.cleanSyncStreamLoadTasks();
        Assertions.assertNull(streamLoadManager.getTaskByLabel("sync_label"));
    }

    // ---- Regression: a multi-statement parent task must unregister its txn-state callback when it
    // is cleaned up. The explicit transaction carries no callback id, so the parent's
    // afterCommitted/afterVisible/afterAborted are never dispatched and never remove the callback;
    // without removal in unprotectedRemoveTaskFromDb every multi-statement stream load leaks one
    // entry (and the sub-task shells it references) in TxnStateCallbackFactory forever. ----
    @Test
    public void testMultiStmtTaskRemovesTxnCallbackOnCleanup() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        TxnStateCallbackFactory callbackFactory = globalTransactionMgr.getCallbackFactory();

        TransactionResult beginResp = new TransactionResult();
        streamLoadManager.beginMultiStatementLoadTask(
                CatalogMocker.TEST_DB_NAME, "leak_label", "", "127.0.0.1",
                100000L, beginResp, WarehouseManager.DEFAULT_RESOURCE);

        AbstractStreamLoadTask task = streamLoadManager.getTaskByLabel("leak_label");
        Assertions.assertNotNull(task);
        long taskId = task.getId();
        // The parent task is registered as a txn-state callback when it is created.
        Assertions.assertNotNull(callbackFactory.getCallback(taskId));

        // Drive the task to a final state so cleanup is allowed to remove it.
        Deencapsulation.setField(task, "state", StreamLoadMultiStmtTask.State.COMMITED);
        Deencapsulation.setField(task, "endTimeMs",
                System.currentTimeMillis() - (Config.stream_load_task_keep_max_second * 1000L + 10000));

        streamLoadManager.cleanOldStreamLoadTasks(true);

        // The task is removed from the manager...
        Assertions.assertNull(streamLoadManager.getTaskByLabel("leak_label"));
        // ...and crucially its txn-state callback must be unregistered too (this is the leak fix).
        Assertions.assertNull(callbackFactory.getCallback(taskId));
    }

}
