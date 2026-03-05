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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.ActionStatus;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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

}
