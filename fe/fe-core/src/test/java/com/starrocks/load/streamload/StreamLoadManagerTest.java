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
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
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

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 1;
        int channelId = 0;

        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                dbName, tableName, labelName, "", "", timeoutMillis, channelNum, channelId, resp);

        Map<String, StreamLoadTask> idToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "idToStreamLoadTask");
        Assertions.assertEquals(1, idToStreamLoadTask.size());
        StreamLoadTask task = idToStreamLoadTask.values().iterator().next();
        Assertions.assertEquals("label1", task.getLabel());
        Assertions.assertEquals("test_db", task.getDBName());
        Assertions.assertEquals(20000, task.getDBId());
        Assertions.assertEquals("test_tbl", task.getTableName());

        Map<String, StreamLoadTask> dbToLabelToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "dbToLabelToStreamLoadTask");
        Assertions.assertEquals(1, idToStreamLoadTask.size());

    }

    @Test
    public void testChannelIdEqualChannelNum() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 1;
        int channelId = 1;

        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                dbName, tableName, labelName, "", "", timeoutMillis, channelNum, channelId, resp);
        Map<String, StreamLoadTask> idToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "idToStreamLoadTask");
        Assertions.assertEquals(1, idToStreamLoadTask.size());
        StreamLoadTask task = idToStreamLoadTask.values().iterator().next();
        Assertions.assertEquals("CANCELLED", task.getStateName());
    }

    @Test
    public void testGetTaskByName() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 5;
        int channelId = 0;

        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                dbName, tableName, labelName, "", "", timeoutMillis, channelNum, channelId, resp);

        List<AbstractStreamLoadTask> tasks = streamLoadManager.getTaskByName(labelName);
        Assertions.assertEquals(1, tasks.size());
        Assertions.assertEquals("label1", tasks.get(0).getLabel());
        Assertions.assertEquals("test_db", tasks.get(0).getDBName());
        Assertions.assertEquals(20000, tasks.get(0).getDBId());
        Assertions.assertEquals("test_tbl", tasks.get(0).getTableName());
    }

    @Test
    public void testGetTaskByNameWithNullLabelName() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName1 = "label1";
        String labelName2 = "label2";
        long timeoutMillis = 100000;
        int channelNum = 5;
        int channelId = 0;

        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                dbName, tableName, labelName1, "", "", timeoutMillis, channelNum, channelId, resp);
        streamLoadManager.beginLoadTaskFromFrontend(
                dbName, tableName, labelName2, "", "", timeoutMillis, channelNum, channelId, resp);

        List<AbstractStreamLoadTask> tasks = streamLoadManager.getTaskByName(null);
        Assertions.assertEquals(2, tasks.size());
        Assertions.assertEquals("label1", tasks.get(0).getLabel());
        Assertions.assertEquals("label2", tasks.get(1).getLabel());
    }

    @Test
    public void testGetTaskByIdWhenMatched() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 5;
        int channelId = 0;

        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                dbName, tableName, labelName, "", "", timeoutMillis, channelNum, channelId, resp);

        StreamLoadTask task = streamLoadManager.getTaskById(1001L);
        Assertions.assertNotNull(task);
        Assertions.assertEquals("label1", task.getLabel());
        Assertions.assertEquals(1001L, task.getId());
        Assertions.assertEquals("test_db", task.getDBName());
        Assertions.assertEquals(20000, task.getDBId());
        Assertions.assertEquals("test_tbl", task.getTableName());
    }

    @Test
    public void testGetTaskByIdWhenNotMatched() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 5;
        int channelId = 0;

        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromFrontend(
                dbName, tableName, labelName, "", "", timeoutMillis, channelNum, channelId, resp);

        StreamLoadTask task = streamLoadManager.getTaskById(1002L);
        Assertions.assertNull(task);
    }

    @Test
    public void testStreamLoadTaskAfterCommit() throws StarRocksException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label2";
        long timeoutMillis = 100000;

        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTaskFromBackend(
                dbName, tableName, labelName, null, "", "", timeoutMillis, resp, false, WarehouseManager.DEFAULT_RESOURCE, 10001);

        Map<String, StreamLoadTask> idToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "idToStreamLoadTask");

        Assertions.assertEquals(1, idToStreamLoadTask.size());

        StreamLoadTask task = idToStreamLoadTask.get(labelName);

        TransactionState state = new TransactionState();
        task.afterCommitted(state, true);
        Assertions.assertNotEquals(-1, task.commitTimeMs());

        Assertions.assertTrue(task.isUnreversibleState());
        Assertions.assertFalse(task.isFinalState());

        streamLoadManager.cleanSyncStreamLoadTasks();
        Assertions.assertEquals(1, streamLoadManager.getStreamLoadTaskCount());
    }

}
