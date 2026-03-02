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

package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
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
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class RoutineLoadMgrEditLogTest {
    private RoutineLoadMgr masterRoutineLoadMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create RoutineLoadMgr instance
        masterRoutineLoadMgr = new RoutineLoadMgr();
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
    public void testAddRoutineLoadJobNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db";
        String jobName = "test_routine_load_job";
        String tableName = "test_table";
        long dbId = 10001L;
        long tableId = 20001L;
        long partitionId = 30001L;
        long indexId = 40001L;
        String brokerList = "localhost:9092";
        String topic = "test_topic";
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, tableName, partitionId, indexId);
        
        // Create RoutineLoadJob
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(
                1L, jobName, dbId, tableId, brokerList, topic);
        routineLoadJob.setOrigStmt(new OriginStatementInfo(
                "CREATE ROUTINE LOAD test_db.test_routine_load_job ON test_table FROM KAFKA", 0));

        // 2. Verify initial state
        RoutineLoadJob initialJob = masterRoutineLoadMgr.getJob(1L);
        Assertions.assertNull(initialJob);

        // 3. Execute addRoutineLoadJob operation (master side)
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // 4. Verify master state
        RoutineLoadJob createdJob = masterRoutineLoadMgr.getJob(1L);
        Assertions.assertNotNull(createdJob);
        Assertions.assertEquals(jobName, createdJob.getName());
        Assertions.assertEquals(dbId, createdJob.getDbId());
        Assertions.assertEquals(tableId, createdJob.getTableId());
        Assertions.assertTrue(createdJob instanceof KafkaRoutineLoadJob);
        KafkaRoutineLoadJob kafkaJob = (KafkaRoutineLoadJob) createdJob;
        Assertions.assertEquals(brokerList, kafkaJob.getBrokerList());
        Assertions.assertEquals(topic, kafkaJob.getTopic());

        // 5. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();
        
        // Verify follower initial state
        RoutineLoadJob followerInitialJob = followerRoutineLoadMgr.getJob(1L);
        Assertions.assertNull(followerInitialJob);

        RoutineLoadJob replayJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        
        // Execute follower replay
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayJob);

        // 6. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(1L);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(jobName, followerJob.getName());
        Assertions.assertEquals(dbId, followerJob.getDbId());
        Assertions.assertEquals(tableId, followerJob.getTableId());
        Assertions.assertEquals(createdJob.getId(), followerJob.getId());
        Assertions.assertTrue(followerJob instanceof KafkaRoutineLoadJob);
        KafkaRoutineLoadJob followerKafkaJob = (KafkaRoutineLoadJob) followerJob;
        Assertions.assertEquals(brokerList, followerKafkaJob.getBrokerList());
        Assertions.assertEquals(topic, followerKafkaJob.getTopic());
    }

    @Test
    public void testAddRoutineLoadJobEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "exception_db";
        String jobName = "exception_routine_load_job";
        String tableName = "test_table";
        long dbId = 10002L;
        long tableId = 20002L;
        long partitionId = 30002L;
        long indexId = 40002L;
        String brokerList = "localhost:9092";
        String topic = "test_topic";
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, tableName, partitionId, indexId);
        
        // Create RoutineLoadJob
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(
                2L, jobName, dbId, tableId, brokerList, topic);
        routineLoadJob.setOrigStmt(new OriginStatementInfo(
                "CREATE ROUTINE LOAD exception_db.exception_routine_load_job ON test_table FROM KAFKA", 0));

        // 2. Create a separate RoutineLoadMgr for exception testing
        RoutineLoadMgr exceptionRoutineLoadMgr = new RoutineLoadMgr();
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        
        // 3. Mock EditLog.logCreateRoutineLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateRoutineLoadJob(any(RoutineLoadJob.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        RoutineLoadJob initialJob = exceptionRoutineLoadMgr.getJob(2L);
        Assertions.assertNull(initialJob);

        // 4. Execute addRoutineLoadJob operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        RoutineLoadJob jobAfterException = exceptionRoutineLoadMgr.getJob(2L);
        Assertions.assertNull(jobAfterException);
    }
}

