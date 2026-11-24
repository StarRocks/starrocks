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
        task.afterCommitted(state, true);
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
    public void testEvictedTasksStayListedUntilLoadsHistoryHasThem() {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        Config.stream_load_task_keep_max_num = 2;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr();
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            long syncedBeforeMs = System.currentTimeMillis() - 61_000L;
            streamLoadManager.setLoadsHistorySyncedBeforeMs(syncedBeforeMs);
            finishTask(addSyncTask(streamLoadManager, testDb, table, 0), syncedBeforeMs - 1000);
            StreamLoadTask unsynced = addSyncTask(streamLoadManager, testDb, table, 1);
            finishTask(unsynced, syncedBeforeMs + 1000);
            // The next two tasks evict rl_0, which loads_history has, and rl_1, which it does not have yet.
            addSyncTask(streamLoadManager, testDb, table, 2);
            addSyncTask(streamLoadManager, testDb, table, 3);
            Assertions.assertEquals(2, streamLoadManager.getStreamLoadTaskCount());
            Assertions.assertNull(streamLoadManager.getTaskByLabel("rl_1"));
            Assertions.assertEquals(List.of(unsynced), streamLoadManager.getTasksPendingLoadsHistory(null));
            Assertions.assertEquals(List.of(unsynced), streamLoadManager.getTasksPendingLoadsHistory("rl_1"));
            Assertions.assertEquals(List.of(), streamLoadManager.getTasksPendingLoadsHistory("rl_0"));
            // The syncer sees its finish time, so it syncs again.
            Assertions.assertEquals(syncedBeforeMs + 1000, streamLoadManager.getLatestFinishTime());

            // A sync that has not passed its finish time keeps it, one that has drops it.
            streamLoadManager.setLoadsHistorySyncedBeforeMs(syncedBeforeMs + 1000);
            Assertions.assertEquals(List.of(unsynced), streamLoadManager.getTasksPendingLoadsHistory(null));
            streamLoadManager.setLoadsHistorySyncedBeforeMs(syncedBeforeMs + 1001);
            Assertions.assertEquals(List.of(), streamLoadManager.getTasksPendingLoadsHistory(null));
        } finally {
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testCleanedTasksStayListedUntilLoadsHistoryHasThem() {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        long syncedBeforeMs = System.currentTimeMillis() - 61_000L;
        streamLoadManager.setLoadsHistorySyncedBeforeMs(syncedBeforeMs);
        finishTask(addSyncTask(streamLoadManager, testDb, table, 0), syncedBeforeMs - 1000);
        StreamLoadTask unsynced = addSyncTask(streamLoadManager, testDb, table, 1);
        finishTask(unsynced, syncedBeforeMs + 1000);
        StreamLoadTask running = addSyncTask(streamLoadManager, testDb, table, 2);

        streamLoadManager.cleanOldStreamLoadTasks(true);
        Assertions.assertEquals(List.of(running), streamLoadManager.getAllTasks());
        Assertions.assertEquals(List.of(unsynced), streamLoadManager.getTasksPendingLoadsHistory(null));
    }

    @Test
    public void testARetriedLabelKeepsTheReplacedTaskListedUntilLoadsHistoryHasIt() throws StarRocksException {
        // A real begin fails on the @Mocked GlobalStateMgr, see testRepeatedBackendBeginFailuresKeepOnlyTheLatestSyncTask.
        // The first begin of a label gets a transaction, every later begin of it fails on the used label.
        Set<String> beganLabels = new HashSet<>();
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
                                         TransactionState.TxnCoordinator coordinator,
                                         TransactionState.LoadJobSourceType sourceType, long listenerId,
                                         long timeoutSecond, ComputeResource computeResource)
                    throws LabelAlreadyUsedException {
                if (!beganLabels.add(label)) {
                    throw new LabelAlreadyUsedException(label, TransactionStatus.PREPARE);
                }
                return 5000L;
            }
        };
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        long syncedBeforeMs = System.currentTimeMillis() - 61_000L;
        streamLoadManager.setLoadsHistorySyncedBeforeMs(syncedBeforeMs);
        TransactionResult first = new TransactionResult();
        streamLoadManager.beginLoadTaskFromBackend(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, "retried",
                new TUniqueId(1, 0), "", "", 100000, first, false, WarehouseManager.DEFAULT_RESOURCE, 10001);
        Assertions.assertTrue(first.stateOK());
        StreamLoadTask original = (StreamLoadTask) streamLoadManager.getTaskByLabel("retried");

        // Two retries while the original still runs, as after a client timeout.
        for (int retry = 0; retry < 2; retry++) {
            TransactionResult response = new TransactionResult();
            streamLoadManager.beginLoadTaskFromBackend(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME,
                    "retried", new TUniqueId(1, retry + 1), "", "", 100000, response, false,
                    WarehouseManager.DEFAULT_RESOURCE, 10001);
            Assertions.assertEquals(ActionStatus.LABEL_ALREADY_EXISTS, response.status);
        }
        // The label leads to the last retry. The original stays listed, and its load still finds it by txn id.
        // The first retry ended without a transaction or a finish time, so loads_history never gets it.
        Assertions.assertNotSame(original, streamLoadManager.getTaskByLabel("retried"));
        Assertions.assertEquals(List.of(original), streamLoadManager.getTasksPendingLoadsHistory("retried"));
        Assertions.assertSame(original, streamLoadManager.getSyncSteamLoadTaskByTxnId(5000L));

        finishTask(original, System.currentTimeMillis());
        Assertions.assertEquals(original.endTimeMs(), streamLoadManager.getLatestFinishTime());
        streamLoadManager.setLoadsHistorySyncedBeforeMs(syncedBeforeMs);
        Assertions.assertEquals(List.of(original), streamLoadManager.getTasksPendingLoadsHistory(null));
        streamLoadManager.setLoadsHistorySyncedBeforeMs(original.endTimeMs() + 1);
        Assertions.assertEquals(List.of(), streamLoadManager.getTasksPendingLoadsHistory(null));
        Assertions.assertNull(streamLoadManager.getSyncSteamLoadTaskByTxnId(5000L));
    }

    @Test
    public void testTasksKeptForLoadsHistoryAreBoundedByKeepMaxNum() {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        Config.stream_load_task_keep_max_num = 2;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr();
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            // no sync has succeeded yet
            streamLoadManager.setLoadsHistorySyncedBeforeMs(0L);
            List<StreamLoadTask> tasks = Lists.newArrayList();
            for (int i = 0; i < 6; i++) {
                StreamLoadTask task = addSyncTask(streamLoadManager, testDb, table, i);
                finishTask(task);
                tasks.add(task);
            }
            // rl_3 found the manager over the limit and evicted rl_0 to rl_2, which overflowed the kept tasks
            Assertions.assertEquals(3, streamLoadManager.getStreamLoadTaskCount());
            List<AbstractStreamLoadTask> kept = streamLoadManager.getTasksPendingLoadsHistory(null);
            Assertions.assertEquals(2, kept.size());
            Assertions.assertTrue(tasks.subList(0, 3).containsAll(kept));
        } finally {
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testNothingIsKeptWhileThisFeDoesNotSyncLoadsHistory() {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        Config.stream_load_task_keep_max_num = 1;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr();
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            // a follower: LoadsHistorySyncer runs on the leader only. rl_2 evicts rl_0 and rl_1.
            finishTask(addSyncTask(streamLoadManager, testDb, table, 0));
            finishTask(addSyncTask(streamLoadManager, testDb, table, 1));
            StreamLoadTask leaderTask = addSyncTask(streamLoadManager, testDb, table, 2);
            Assertions.assertNull(streamLoadManager.getTaskByLabel("rl_0"));
            Assertions.assertEquals(List.of(), streamLoadManager.getTasksPendingLoadsHistory(null));

            // a leader keeps its tasks until it stops syncing
            streamLoadManager.setLoadsHistorySyncedBeforeMs(0L);
            finishTask(leaderTask);
            addSyncTask(streamLoadManager, testDb, table, 3);
            addSyncTask(streamLoadManager, testDb, table, 4);
            Assertions.assertEquals(List.of(leaderTask), streamLoadManager.getTasksPendingLoadsHistory(null));
            streamLoadManager.setLoadsHistorySyncedBeforeMs(Long.MAX_VALUE);
            Assertions.assertEquals(List.of(), streamLoadManager.getTasksPendingLoadsHistory(null));
        } finally {
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testTasksKeptForLoadsHistoryAreFoundById() {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        Config.stream_load_task_keep_max_num = 1;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr();
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            streamLoadManager.setLoadsHistorySyncedBeforeMs(0L);
            StreamLoadTask kept = addSyncTask(streamLoadManager, testDb, table, 0);
            Deencapsulation.setField(kept, "id", 7001L);
            finishTask(kept);
            StreamLoadTask registered = addSyncTask(streamLoadManager, testDb, table, 1);
            Deencapsulation.setField(registered, "id", 7002L);
            // the manager is over the limit, so this evicts the finished rl_0
            addSyncTask(streamLoadManager, testDb, table, 2);

            Assertions.assertEquals(List.of(kept), streamLoadManager.getTasksPendingLoadsHistory(null));
            Assertions.assertSame(kept, streamLoadManager.getTaskById(7001L));
            Assertions.assertSame(registered, streamLoadManager.getTaskById(7002L));
            Assertions.assertNull(streamLoadManager.getTaskById(7003L));
        } finally {
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testARunningReplacedTaskIsTheLastKeptTaskToBeDropped() {
        int savedKeepMaxNum = Config.stream_load_task_keep_max_num;
        Config.stream_load_task_keep_max_num = 1;
        try {
            StreamLoadMgr streamLoadManager = new StreamLoadMgr();
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
            streamLoadManager.setLoadsHistorySyncedBeforeMs(0L);
            StreamLoadTask running = addSyncTask(streamLoadManager, testDb, table, 0);
            StreamLoadTask retry = streamLoadManager.createLoadTaskWithoutLock(testDb, table, "rl_0", "", "",
                    100000, true, WarehouseManager.DEFAULT_RESOURCE);
            retry.setTxnId(2000);
            streamLoadManager.addLoadTask(retry, false);
            Assertions.assertEquals(List.of(running), streamLoadManager.getTasksPendingLoadsHistory(null));

            // rl_2 evicts the finished retry, which overflows the list: the finished retry goes, the running task stays
            finishTask(retry);
            addSyncTask(streamLoadManager, testDb, table, 1);
            addSyncTask(streamLoadManager, testDb, table, 2);
            Assertions.assertEquals(List.of(running), streamLoadManager.getTasksPendingLoadsHistory(null));
            Assertions.assertSame(running, streamLoadManager.getSyncSteamLoadTaskByTxnId(1000));
        } finally {
            Config.stream_load_task_keep_max_num = savedKeepMaxNum;
        }
    }

    @Test
    public void testAFinishedMultiStatementLoadHasLoadsHistorySynced() {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(CatalogMocker.TEST_DB_NAME);
        StreamLoadMultiStmtTask multiStmt = streamLoadManager.createMultiStatementLoadTask(
                testDb, "multi_stmt", "", "", 100000, WarehouseManager.DEFAULT_RESOURCE);
        streamLoadManager.addLoadTask(multiStmt, false);
        Assertions.assertEquals(-1L, streamLoadManager.getLatestFinishTime());

        // the finish time of a multi-statement load is the latest of its statements
        Deencapsulation.setField(multiStmt, "state", StreamLoadMultiStmtTask.State.FINISHED);
        new MockUp<StreamLoadMultiStmtTask>() {
            @Mock
            public long getFinishTimestampMs() {
                return 123456L;
            }
        };
        Assertions.assertEquals(123456L, streamLoadManager.getLatestFinishTime());
    }

    private static StreamLoadTask addSyncTask(StreamLoadMgr streamLoadManager, Database db, OlapTable table, int i) {
        StreamLoadTask task = streamLoadManager.createLoadTaskWithoutLock(db, table, "rl_" + i, "", "", 100000, true,
                WarehouseManager.DEFAULT_RESOURCE);
        task.setTxnId(1000 + i);
        streamLoadManager.addLoadTask(task, false);
        return task;
    }

    private static void finishTask(StreamLoadTask task) {
        finishTask(task, System.currentTimeMillis());
    }

    private static void finishTask(StreamLoadTask task, long endTimeMs) {
        Deencapsulation.setField(task, "state", StreamLoadTask.State.FINISHED);
        Deencapsulation.setField(task, "endTimeMs", endTimeMs);
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
