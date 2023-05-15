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
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    private GlobalTransactionMgr globalTransactionMgr;
    private Database db;

    @Before
    public void setUp() {
        globalTransactionMgr = new GlobalTransactionMgr(globalStateMgr);
        FeConstants.runningUnitTest = true;
    
        try {
            db = CatalogMocker.mockDb();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
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
                globalStateMgr.getDb(anyString);
                minTimes = 0;
                result = db;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        globalTransactionMgr.addDatabaseTransactionMgr(db.getId());
        SystemInfoService systemInfoService = new SystemInfoService();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getCurrentGlobalTransactionMgr();
                minTimes = 0;
                result = globalTransactionMgr;

                GlobalStateMgr.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.getBackendIds(true);
                minTimes = 0;
                result = Lists.newArrayList();

                GlobalStateMgr.getCurrentState().getNextId();
                minTimes = 0;
                result = 1001L;
            }
        };
    }

    @Test
    public void testBeginStreamLoadTask() throws UserException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        
        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 1;
        int channelId = 0;
        
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTask(dbName, tableName, labelName, timeoutMillis, channelNum, channelId, resp);
        
        Map<String, StreamLoadTask> idToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "idToStreamLoadTask");
        Assert.assertEquals(1, idToStreamLoadTask.size());
        StreamLoadTask task = idToStreamLoadTask.values().iterator().next();
        Assert.assertEquals("label1", task.getLabel());
        Assert.assertEquals("test_db", task.getDBName());
        Assert.assertEquals(20000, task.getDBId());
        Assert.assertEquals("test_tbl", task.getTableName());

        Map<String, StreamLoadTask> dbToLabelToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "dbToLabelToStreamLoadTask");
        Assert.assertEquals(1, idToStreamLoadTask.size());
        
    }

    @Test
    public void testChannelIdEqualChannelNum() throws UserException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();
        
        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 1;
        int channelId = 1;
        
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTask(dbName, tableName, labelName, timeoutMillis, channelNum, channelId, resp);
        Map<String, StreamLoadTask> idToStreamLoadTask =
                Deencapsulation.getField(streamLoadManager, "idToStreamLoadTask");
        Assert.assertEquals(1, idToStreamLoadTask.size());
        StreamLoadTask task = idToStreamLoadTask.values().iterator().next();
        Assert.assertEquals("CANCELLED", task.getStateName());
    }

    @Test
    public void testGetTaskByName() throws UserException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 5;
        int channelId = 0;
        
        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTask(dbName, tableName, labelName, timeoutMillis, channelNum, channelId, resp);

        List<StreamLoadTask> tasks = streamLoadManager.getTaskByName(labelName);
        Assert.assertEquals(1, tasks.size());
        Assert.assertEquals("label1", tasks.get(0).getLabel());
        Assert.assertEquals("label1", tasks.get(0).getLabel());
        Assert.assertEquals("test_db", tasks.get(0).getDBName());
        Assert.assertEquals(20000, tasks.get(0).getDBId());
        Assert.assertEquals("test_tbl", tasks.get(0).getTableName());
    }

    @Test
    public void testGetTaskByIdWhenMatched() throws UserException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 5;
        int channelId = 0;

        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTask(dbName, tableName, labelName, timeoutMillis, channelNum, channelId, resp);

        StreamLoadTask task = streamLoadManager.getTaskById(1001L);
        Assert.assertNotNull(task);
        Assert.assertEquals("label1", task.getLabel());
        Assert.assertEquals(1001L, task.getId());
        Assert.assertEquals("test_db", task.getDBName());
        Assert.assertEquals(20000, task.getDBId());
        Assert.assertEquals("test_tbl", task.getTableName());
    }

    @Test
    public void testGetTaskByIdWhenNotMatched() throws UserException {
        StreamLoadMgr streamLoadManager = new StreamLoadMgr();

        String dbName = "test_db";
        String tableName = "test_tbl";
        String labelName = "label1";
        long timeoutMillis = 100000;
        int channelNum = 5;
        int channelId = 0;

        TransactionResult resp = new TransactionResult();
        streamLoadManager.beginLoadTask(dbName, tableName, labelName, timeoutMillis, channelNum, channelId, resp);

        StreamLoadTask task = streamLoadManager.getTaskById(1002L);
        Assert.assertNull(task);
    }
}
