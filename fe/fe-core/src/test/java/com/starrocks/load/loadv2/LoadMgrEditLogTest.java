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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.load.EtlJobType;
import com.starrocks.persist.AlterLoadJobOperationLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class LoadMgrEditLogTest {
    private LoadMgr masterLoadMgr;
    private LoadJobScheduler loadJobScheduler;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create LoadMgr instance
        loadJobScheduler = new LoadJobScheduler();
        masterLoadMgr = new LoadMgr(loadJobScheduler);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // Helper method to create a database with table
    private Database createDatabaseWithTable(long dbId, String dbName, long tableId, String tableName,
                                             long partitionId, long indexId) throws Exception {
        // Create database
        Database database = new Database(dbId, dbName);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(database);
        
        // Create table columns
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("k1", IntegerType.INT);
        col1.setIsKey(true);
        columns.add(col1);
        Column col2 = new Column("v1", IntegerType.BIGINT);
        columns.add(col2);
        
        // Create partition info
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);
        
        // Create distribution info
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(3);
        
        // Create MaterializedIndex
        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, IndexState.NORMAL);
        
        // Create Partition
        Partition partition = new Partition(partitionId, partitionId + 100, tableName, 
                materializedIndex, distributionInfo);
        
        // Create OlapTable
        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        olapTable.setIndexMeta(indexId, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(indexId);
        olapTable.addPartition(partition);
        
        // Register table to database
        database.registerTableUnlocked(olapTable);
        
        return database;
    }

    @Test
    public void testCreateLoadJobFromStmtNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db";
        String label = "test_label";
        String tableName = "test_table";
        long dbId = 10001L;
        long tableId = 20001L;
        long partitionId = 30001L;
        long indexId = 40001L;
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, tableName, partitionId, indexId);
        
        // Create DataDescription
        List<String> filePaths = Lists.newArrayList("hdfs://test/path/file1");
        DataDescription dataDescription = new DataDescription(
                tableName, null, filePaths, null, null, null, null, false, null);
        List<DataDescription> dataDescriptionList = Lists.newArrayList(dataDescription);
        
        // Create LoadStmt
        LabelName labelName = new LabelName(dbName, label);
        ConnectContext context = new ConnectContext();
        BrokerDesc brokerDesc = new BrokerDesc("test_broker", new HashMap<>());
        Map<String, String> properties = new HashMap<>();
        LoadStmt loadStmt = new LoadStmt(labelName, dataDescriptionList, brokerDesc, null, properties);
        loadStmt.setEtlJobType(EtlJobType.BROKER);

        // 2. Verify initial state
        Assertions.assertEquals(0, masterLoadMgr.getLoadJobNum(JobState.PENDING, dbId));

        // 3. Execute createLoadJobFromStmt operation (master side)
        masterLoadMgr.createLoadJobFromStmt(loadStmt, context);

        // 4. Verify master state
        Assertions.assertTrue(masterLoadMgr.getLoadJobNum(JobState.PENDING, dbId) > 0 ||
                masterLoadMgr.getLoadJobNum(JobState.LOADING, dbId) > 0);
        
        List<LoadJob> loadJobs = masterLoadMgr.getLoadJobsByDb(dbId, label, true);
        Assertions.assertFalse(loadJobs.isEmpty());
        LoadJob loadJob = loadJobs.get(0);
        Assertions.assertEquals(label, loadJob.getLabel());
        Assertions.assertEquals(dbId, loadJob.getDbId());

        // 5. Test follower replay functionality
        LoadMgr followerLoadMgr = new LoadMgr(new LoadJobScheduler());
        
        // Verify follower initial state
        Assertions.assertEquals(0, followerLoadMgr.getLoadJobNum(JobState.PENDING, dbId));

        LoadJob replayLoadJob = (LoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_LOAD_JOB_V2);
        
        // Execute follower replay
        followerLoadMgr.replayCreateLoadJob(replayLoadJob);

        // 6. Verify follower state is consistent with master
        List<LoadJob> followerLoadJobs = followerLoadMgr.getLoadJobsByDb(dbId, label, true);
        Assertions.assertFalse(followerLoadJobs.isEmpty());
        LoadJob followerLoadJob = followerLoadJobs.get(0);
        Assertions.assertEquals(label, followerLoadJob.getLabel());
        Assertions.assertEquals(dbId, followerLoadJob.getDbId());
        Assertions.assertEquals(loadJob.getId(), followerLoadJob.getId());
    }

    @Test
    public void testCreateLoadJobFromStmtEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "exception_db";
        String label = "exception_label";
        String tableName = "test_table";
        long dbId = 10002L;
        long tableId = 20002L;
        long partitionId = 30002L;
        long indexId = 40002L;
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, tableName, partitionId, indexId);
        
        // Create DataDescription
        List<String> filePaths = Lists.newArrayList("hdfs://test/path/file1");
        DataDescription dataDescription = new DataDescription(
                tableName, null, filePaths, null, null, null, null, false, null);
        List<DataDescription> dataDescriptionList = Lists.newArrayList(dataDescription);
        
        // Create LoadStmt
        LabelName labelName = new LabelName(dbName, label);
        ConnectContext context = new ConnectContext();
        BrokerDesc brokerDesc = new BrokerDesc("test_broker", new HashMap<>());
        Map<String, String> properties = new HashMap<>();
        LoadStmt loadStmt = new LoadStmt(labelName, dataDescriptionList, brokerDesc, null, properties);
        loadStmt.setEtlJobType(EtlJobType.BROKER);

        // 2. Create a separate LoadMgr for exception testing
        LoadMgr exceptionLoadMgr = new LoadMgr(new LoadJobScheduler());
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        
        // 3. Mock EditLog.logCreateLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateLoadJob(any(LoadJob.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        Assertions.assertEquals(0, exceptionLoadMgr.getLoadJobNum(JobState.PENDING, dbId));
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionLoadMgr.createLoadJobFromStmt(loadStmt, context);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        Assertions.assertEquals(0, exceptionLoadMgr.getLoadJobNum(JobState.PENDING, dbId));
        List<LoadJob> loadJobs = exceptionLoadMgr.getLoadJobsByDb(dbId, label, true);
        Assertions.assertTrue(loadJobs.isEmpty());
    }

    @Test
    public void testRegisterInsertLoadJobNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_insert";
        String label = "insert_test_label";
        long dbId = 10003L;
        long tableId = 20003L;
        long partitionId = 30003L;
        long indexId = 40003L;
        long txnId = 30001L;
        String loadId = "load_id_001";
        String user = "test_user";
        long createTimestamp = System.currentTimeMillis();
        LoadMgr.EstimateStats estimateStats = new LoadMgr.EstimateStats(1000L, 5, 1024000L);
        long timeout = 3600L;
        long warehouseId = 1L;

        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, "test_table", partitionId, indexId);

        // Coordinator and InsertLoadTxnCallback are complex objects, keep them as mock
        Coordinator coordinator = mock(Coordinator.class);
        InsertLoadTxnCallback insertLoadTxnCallback = mock(InsertLoadTxnCallback.class);

        // 2. Verify initial state
        Assertions.assertEquals(0, masterLoadMgr.getLoadJobNum(JobState.LOADING, EtlJobType.INSERT));

        // 3. Execute registerInsertLoadJob operation (master side)
        InsertLoadJob loadJob = masterLoadMgr.registerInsertLoadJob(
                label, dbName, tableId, txnId, loadId, user,
                EtlJobType.INSERT, createTimestamp, estimateStats,
                timeout, warehouseId, coordinator, insertLoadTxnCallback);

        // 4. Verify master state
        Assertions.assertNotNull(loadJob);
        Assertions.assertEquals(label, loadJob.getLabel());
        Assertions.assertEquals(dbId, loadJob.getDbId());
        Assertions.assertEquals(tableId, ((InsertLoadJob) loadJob).getTableId());
        Assertions.assertEquals(txnId, loadJob.getTransactionId());
        
        List<LoadJob> loadJobs = masterLoadMgr.getLoadJobsByDb(dbId, label, true);
        Assertions.assertFalse(loadJobs.isEmpty());
        Assertions.assertEquals(loadJob.getId(), loadJobs.get(0).getId());

        // 5. Test follower replay functionality
        LoadMgr followerLoadMgr = new LoadMgr(new LoadJobScheduler());
        
        // Verify follower initial state
        Assertions.assertEquals(0, followerLoadMgr.getLoadJobNum(JobState.LOADING, EtlJobType.INSERT));

        LoadJob replayLoadJob = (LoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_LOAD_JOB_V2);
        
        // Execute follower replay
        followerLoadMgr.replayCreateLoadJob(replayLoadJob);

        // 6. Verify follower state is consistent with master
        List<LoadJob> followerLoadJobs = followerLoadMgr.getLoadJobsByDb(dbId, label, true);
        Assertions.assertFalse(followerLoadJobs.isEmpty());
        LoadJob followerLoadJob = followerLoadJobs.get(0);
        Assertions.assertEquals(label, followerLoadJob.getLabel());
        Assertions.assertEquals(dbId, followerLoadJob.getDbId());
        Assertions.assertEquals(loadJob.getId(), followerLoadJob.getId());
        Assertions.assertTrue(followerLoadJob instanceof InsertLoadJob);
        Assertions.assertEquals(tableId, ((InsertLoadJob) followerLoadJob).getTableId());
    }

    @Test
    public void testRegisterInsertLoadJobEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "exception_db_insert";
        String label = "insert_exception_label";
        long dbId = 10004L;
        long tableId = 20004L;
        long partitionId = 30004L;
        long indexId = 40004L;
        long txnId = 30002L;
        String loadId = "load_id_002";
        String user = "test_user";
        long createTimestamp = System.currentTimeMillis();
        LoadMgr.EstimateStats estimateStats = new LoadMgr.EstimateStats(1000L, 5, 1024000L);
        long timeout = 3600L;
        long warehouseId = 1L;

        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, "test_table", partitionId, indexId);

        // Coordinator and InsertLoadTxnCallback are complex objects, keep them as mock
        Coordinator coordinator = mock(Coordinator.class);
        InsertLoadTxnCallback insertLoadTxnCallback = mock(InsertLoadTxnCallback.class);

        // 2. Create a separate LoadMgr for exception testing
        LoadMgr exceptionLoadMgr = new LoadMgr(new LoadJobScheduler());
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        
        // 3. Mock EditLog.logCreateLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateLoadJob(any(LoadJob.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionLoadMgr.getLoadJobNum(JobState.LOADING, EtlJobType.INSERT));
        
        // 4. Execute registerInsertLoadJob operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionLoadMgr.registerInsertLoadJob(
                    label, dbName, tableId, txnId, loadId, user,
                    EtlJobType.INSERT, createTimestamp, estimateStats,
                    timeout, warehouseId, coordinator, insertLoadTxnCallback);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionLoadMgr.getLoadJobNum(JobState.LOADING, EtlJobType.INSERT));
        
        List<LoadJob> loadJobs = exceptionLoadMgr.getLoadJobsByDb(dbId, label, true);
        Assertions.assertTrue(loadJobs.isEmpty());
    }

    @Test
    public void testAlterLoadJobNormalCase() throws Exception {
        // 1. Prepare test data - first create a BrokerLoadJob
        String dbName = "test_db_alter";
        String label = "alter_test_label";
        String tableName = "test_table";
        long dbId = 10005L;
        long tableId = 20005L;
        long partitionId = 30005L;
        long indexId = 40005L;
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, tableName, partitionId, indexId);
        
        // Create DataDescription
        List<String> filePaths = Lists.newArrayList("hdfs://test/path/file1");
        DataDescription dataDescription = new DataDescription(
                tableName, null, filePaths, null, null, null, null, false, null);
        List<DataDescription> dataDescriptionList = Lists.newArrayList(dataDescription);
        
        // Create LoadStmt
        LabelName labelName = new LabelName(dbName, label);
        ConnectContext context = new ConnectContext();
        BrokerDesc brokerDesc = new BrokerDesc("test_broker", new HashMap<>());
        Map<String, String> properties = new HashMap<>();
        LoadStmt loadStmt = new LoadStmt(labelName, dataDescriptionList, brokerDesc, null, properties);
        loadStmt.setEtlJobType(EtlJobType.BROKER);

        // Create the load job first
        masterLoadMgr.createLoadJobFromStmt(loadStmt, context);
        
        // Get the created job
        List<LoadJob> loadJobs = masterLoadMgr.getLoadJobsByDb(dbId, label, true);
        Assertions.assertFalse(loadJobs.isEmpty());
        BrokerLoadJob brokerLoadJob = (BrokerLoadJob) loadJobs.get(0);
        long jobId = brokerLoadJob.getId();

        // 2. Prepare AlterLoadStmt
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put(LoadStmt.PRIORITY, "HIGH");
        AlterLoadStmt alterLoadStmt = new AlterLoadStmt(labelName, alterProperties);
        alterLoadStmt.setDbName(dbName);
        alterLoadStmt.getAnalyzedJobProperties().put(LoadStmt.PRIORITY, "HIGH");

        // Verify initial priority (default is NORMAL which is 0)
        Assertions.assertEquals(0, brokerLoadJob.priority);

        // 3. Execute alterLoadJob operation (master side)
        masterLoadMgr.alterLoadJob(alterLoadStmt);

        // 4. Verify master state - priority should be changed
        brokerLoadJob = (BrokerLoadJob) masterLoadMgr.getLoadJob(jobId);
        Assertions.assertNotNull(brokerLoadJob);
        // HIGH priority value (need to check LoadPriority constants, typically HIGH = 3)
        Assertions.assertNotEquals(0, brokerLoadJob.priority);

        // 5. Test follower replay functionality
        LoadMgr followerLoadMgr = new LoadMgr(new LoadJobScheduler());
        
        // First replay the create load job
        LoadJob replayCreateJob = (LoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_LOAD_JOB_V2);
        followerLoadMgr.replayCreateLoadJob(replayCreateJob);
        
        // Then replay the alter load job
        AlterLoadJobOperationLog alterLog = (AlterLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_LOAD_JOB);
        
        // Execute follower replay
        followerLoadMgr.replayAlterLoadJob(alterLog);

        // 6. Verify follower state is consistent with master
        BrokerLoadJob followerBrokerLoadJob = (BrokerLoadJob) followerLoadMgr.getLoadJob(jobId);
        Assertions.assertNotNull(followerBrokerLoadJob);
        Assertions.assertEquals(brokerLoadJob.priority, followerBrokerLoadJob.priority);
    }

    @Test
    public void testAlterLoadJobEditLogException() throws Exception {
        // 1. Prepare test data - first create a BrokerLoadJob
        String dbName = "exception_db_alter";
        String label = "alter_exception_label";
        String tableName = "test_table";
        long dbId = 10006L;
        long tableId = 20006L;
        long partitionId = 30006L;
        long indexId = 40006L;
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, tableName, partitionId, indexId);
        
        // Create DataDescription
        List<String> filePaths = Lists.newArrayList("hdfs://test/path/file1");
        DataDescription dataDescription = new DataDescription(
                tableName, null, filePaths, null, null, null, null, false, null);
        List<DataDescription> dataDescriptionList = Lists.newArrayList(dataDescription);
        
        // Create LoadStmt
        LabelName labelName = new LabelName(dbName, label);
        ConnectContext context = new ConnectContext();
        BrokerDesc brokerDesc = new BrokerDesc("test_broker", new HashMap<>());
        Map<String, String> properties = new HashMap<>();
        LoadStmt loadStmt = new LoadStmt(labelName, dataDescriptionList, brokerDesc, null, properties);
        loadStmt.setEtlJobType(EtlJobType.BROKER);

        // Create a separate LoadMgr for exception testing
        LoadMgr exceptionLoadMgr = new LoadMgr(new LoadJobScheduler());
        
        // Create the load job first
        exceptionLoadMgr.createLoadJobFromStmt(loadStmt, context);
        
        // Get the created job
        List<LoadJob> loadJobs = exceptionLoadMgr.getLoadJobsByDb(dbId, label, true);
        Assertions.assertFalse(loadJobs.isEmpty());
        BrokerLoadJob brokerLoadJob = (BrokerLoadJob) loadJobs.get(0);
        long jobId = brokerLoadJob.getId();
        int initialPriority = brokerLoadJob.priority;

        // 2. Prepare AlterLoadStmt
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put(LoadStmt.PRIORITY, "HIGH");
        AlterLoadStmt alterLoadStmt = new AlterLoadStmt(labelName, alterProperties);
        alterLoadStmt.setDbName(dbName);
        alterLoadStmt.getAnalyzedJobProperties().put(LoadStmt.PRIORITY, "HIGH");

        // 3. Mock EditLog.logAlterLoadJob to throw exception
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterLoadJob(any(AlterLoadJobOperationLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterLoadJob operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionLoadMgr.alterLoadJob(alterLoadStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        brokerLoadJob = (BrokerLoadJob) exceptionLoadMgr.getLoadJob(jobId);
        Assertions.assertNotNull(brokerLoadJob);
        Assertions.assertEquals(initialPriority, brokerLoadJob.priority);
    }
}