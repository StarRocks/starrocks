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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TTabletType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class AnalyzeMgrEditLogTest {
    private AnalyzeMgr masterAnalyzeMgr;
    private long testDbId = 100L;
    private long testTableId = 1000L;
    private long testPartitionId = 2000L;
    private static final String TEST_DB_NAME = "test_db";
    private static final String TEST_TABLE_NAME = "test_table";

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create AnalyzeMgr instance
        masterAnalyzeMgr = new AnalyzeMgr();

        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        // Create test database and table
        Database database = new Database(testDbId, TEST_DB_NAME);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(database);

        // Create columns
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("col1", IntegerType.INT);
        col1.setIsKey(true);
        columns.add(col1);

        // Create partition info
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(testPartitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(testPartitionId, (short) 1);
        partitionInfo.setIsInMemory(testPartitionId, false);
        partitionInfo.setTabletType(testPartitionId, TTabletType.TABLET_TYPE_DISK);

        // Create distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList(col1));

        // Create OlapTable
        OlapTable olapTable = new OlapTable(testTableId, TEST_TABLE_NAME, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);

        // Register table
        database.registerTableUnlocked(olapTable);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== Add AnalyzeJob Tests ====================

    @Test
    public void testAddAnalyzeJobNormalCase() throws Exception {
        // 1. Prepare test data
        NativeAnalyzeJob job = new NativeAnalyzeJob(testDbId, testTableId, Lists.newArrayList(),
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.now());

        // 2. Verify initial state
        Assertions.assertEquals(0, masterAnalyzeMgr.getAllAnalyzeJobList().size());

        // 3. Execute addAnalyzeJob operation
        masterAnalyzeMgr.addAnalyzeJob(job);

        // 4. Verify master state
        Assertions.assertEquals(1, masterAnalyzeMgr.getAllAnalyzeJobList().size());
        AnalyzeJob addedJob = masterAnalyzeMgr.getAnalyzeJob(job.getId());
        Assertions.assertNotNull(addedJob);
        Assertions.assertEquals(testDbId, ((NativeAnalyzeJob) addedJob).getDbId());
        Assertions.assertEquals(testTableId, ((NativeAnalyzeJob) addedJob).getTableId());

        // 5. Test follower replay functionality
        AnalyzeMgr followerAnalyzeMgr = new AnalyzeMgr();
        Assertions.assertEquals(0, followerAnalyzeMgr.getAllAnalyzeJobList().size());

        NativeAnalyzeJob replayJob = (NativeAnalyzeJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ADD_ANALYZER_JOB);

        followerAnalyzeMgr.replayAddAnalyzeJob(replayJob);

        // 6. Verify follower state is consistent with master
        Assertions.assertEquals(1, followerAnalyzeMgr.getAllAnalyzeJobList().size());
        AnalyzeJob followerJob = followerAnalyzeMgr.getAnalyzeJob(replayJob.getId());
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(testDbId, ((NativeAnalyzeJob) followerJob).getDbId());
        Assertions.assertEquals(testTableId, ((NativeAnalyzeJob) followerJob).getTableId());
    }

    @Test
    public void testAddAnalyzeJobEditLogException() throws Exception {
        // 1. Prepare test data
        NativeAnalyzeJob job = new NativeAnalyzeJob(testDbId, testTableId, Lists.newArrayList(),
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.now());

        // 2. Create a separate AnalyzeMgr for exception testing
        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());

        // 3. Mock EditLog.logAddAnalyzeJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddAnalyzeJob(any(AnalyzeJob.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getAllAnalyzeJobList().size());

        // 4. Execute addAnalyzeJob operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.addAnalyzeJob(job);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getAllAnalyzeJobList().size());
        AnalyzeJob retrievedJob = exceptionAnalyzeMgr.getAnalyzeJob(job.getId());
        Assertions.assertNull(retrievedJob);
    }

    // ==================== Update AnalyzeJob Tests ====================

    @Test
    public void testUpdateAnalyzeJobWithLogNormalCase() throws Exception {
        // 1. Prepare test data and add job first
        NativeAnalyzeJob job = new NativeAnalyzeJob(testDbId, testTableId, Lists.newArrayList(),
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.now());

        masterAnalyzeMgr.addAnalyzeJob(job);
        Assertions.assertEquals(1, masterAnalyzeMgr.getAllAnalyzeJobList().size());

        // 2. Update job status
        job.setStatus(StatsConstants.ScheduleStatus.FINISH);
        masterAnalyzeMgr.updateAnalyzeJobWithLog(job);

        // 3. Verify master state
        AnalyzeJob updatedJob = masterAnalyzeMgr.getAnalyzeJob(job.getId());
        Assertions.assertNotNull(updatedJob);
        Assertions.assertEquals(StatsConstants.ScheduleStatus.FINISH, updatedJob.getStatus());
    }
    // ==================== Remove AnalyzeJob Tests ====================

    @Test
    public void testRemoveAnalyzeJobNormalCase() throws Exception {
        // 1. Prepare test data and add job first
        NativeAnalyzeJob job = new NativeAnalyzeJob(testDbId, testTableId, Lists.newArrayList(),
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.now());

        masterAnalyzeMgr.addAnalyzeJob(job);
        Assertions.assertEquals(1, masterAnalyzeMgr.getAllAnalyzeJobList().size());

        // 2. Execute removeAnalyzeJob operation
        masterAnalyzeMgr.removeAnalyzeJob(job.getId());

        // 3. Verify master state
        Assertions.assertEquals(0, masterAnalyzeMgr.getAllAnalyzeJobList().size());
        AnalyzeJob retrievedJob = masterAnalyzeMgr.getAnalyzeJob(job.getId());
        Assertions.assertNull(retrievedJob);
    }

    @Test
    public void testRemoveAnalyzeJobEditLogException() throws Exception {
        // 1. Prepare test data and add job first
        NativeAnalyzeJob job = new NativeAnalyzeJob(testDbId, testTableId, Lists.newArrayList(),
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.now());

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addAnalyzeJob(job);
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAllAnalyzeJobList().size());

        // 2. Mock EditLog.logRemoveAnalyzeJob to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveAnalyzeJob(any(AnalyzeJob.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute removeAnalyzeJob operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.removeAnalyzeJob(job.getId());
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAllAnalyzeJobList().size());
        AnalyzeJob retrievedJob = exceptionAnalyzeMgr.getAnalyzeJob(job.getId());
        Assertions.assertNotNull(retrievedJob);
    }

    // ==================== Add AnalyzeStatus Tests ====================

    @Test
    public void testAddAnalyzeStatusNormalCase() throws Exception {
        // 1. Prepare test data
        NativeAnalyzeStatus status = new NativeAnalyzeStatus(1L, testDbId, testTableId,
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                LocalDateTime.now());

        // 2. Verify initial state
        Assertions.assertEquals(0, masterAnalyzeMgr.getAnalyzeStatusMap().size());

        // 3. Execute addAnalyzeStatus operation
        masterAnalyzeMgr.addAnalyzeStatus(status);

        // 4. Verify master state
        Assertions.assertEquals(1, masterAnalyzeMgr.getAnalyzeStatusMap().size());
        AnalyzeStatus addedStatus = masterAnalyzeMgr.getAnalyzeStatus(status.getId());
        Assertions.assertNotNull(addedStatus);
        Assertions.assertEquals(testDbId, ((NativeAnalyzeStatus) addedStatus).getDbId());
        Assertions.assertEquals(testTableId, ((NativeAnalyzeStatus) addedStatus).getTableId());
    }

    @Test
    public void testAddAnalyzeStatusEditLogException() throws Exception {
        // 1. Prepare test data
        NativeAnalyzeStatus status = new NativeAnalyzeStatus(1L, testDbId, testTableId,
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                LocalDateTime.now());

        // 2. Create a separate AnalyzeMgr for exception testing
        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());

        // 3. Mock EditLog.logAddAnalyzeStatus to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddAnalyzeStatus(any(AnalyzeStatus.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getAnalyzeStatusMap().size());

        // 4. Execute addAnalyzeStatus operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.addAnalyzeStatus(status);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getAnalyzeStatusMap().size());
        AnalyzeStatus retrievedStatus = exceptionAnalyzeMgr.getAnalyzeStatus(status.getId());
        Assertions.assertNull(retrievedStatus);
    }

    // ==================== Clear Expired AnalyzeStatus Tests ====================

    @Test
    public void testClearExpiredAnalyzeStatusNormalCase() throws Exception {
        // 1. Prepare test data and add expired status
        NativeAnalyzeStatus expiredStatus = new NativeAnalyzeStatus(1L, testDbId, testTableId,
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                LocalDateTime.now().minusSeconds(Config.statistic_analyze_status_keep_second + 100));

        masterAnalyzeMgr.addAnalyzeStatus(expiredStatus);
        Assertions.assertEquals(1, masterAnalyzeMgr.getAnalyzeStatusMap().size());

        // 2. Execute clearExpiredAnalyzeStatus operation
        masterAnalyzeMgr.clearExpiredAnalyzeStatus();

        // 3. Verify master state
        Assertions.assertEquals(0, masterAnalyzeMgr.getAnalyzeStatusMap().size());
    }

    @Test
    public void testClearExpiredAnalyzeStatusEditLogException() throws Exception {
        // 1. Prepare test data and add expired status
        NativeAnalyzeStatus expiredStatus = new NativeAnalyzeStatus(1L, testDbId, testTableId,
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                LocalDateTime.now().minusSeconds(Config.statistic_analyze_status_keep_second + 100));

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addAnalyzeStatus(expiredStatus);
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAnalyzeStatusMap().size());

        // 2. Mock EditLog.logRemoveAnalyzeStatus to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveAnalyzeStatus(any(AnalyzeStatus.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute clearExpiredAnalyzeStatus operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.clearExpiredAnalyzeStatus();
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAnalyzeStatusMap().size());
        AnalyzeStatus retrievedStatus = exceptionAnalyzeMgr.getAnalyzeStatus(expiredStatus.getId());
        Assertions.assertNotNull(retrievedStatus);
    }

    // ==================== Drop AnalyzeStatus Tests ====================

    @Test
    public void testDropAnalyzeStatusNormalCase() throws Exception {
        // 1. Prepare test data and add status first
        NativeAnalyzeStatus status = new NativeAnalyzeStatus(1L, testDbId, testTableId,
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                LocalDateTime.now());

        masterAnalyzeMgr.addAnalyzeStatus(status);
        Assertions.assertEquals(1, masterAnalyzeMgr.getAnalyzeStatusMap().size());

        // 2. Execute dropAnalyzeStatus operation
        masterAnalyzeMgr.dropAnalyzeStatus(testTableId);

        // 3. Verify master state
        Assertions.assertEquals(0, masterAnalyzeMgr.getAnalyzeStatusMap().size());
    }

    @Test
    public void testDropAnalyzeStatusEditLogException() throws Exception {
        // 1. Prepare test data and add status first
        NativeAnalyzeStatus status = new NativeAnalyzeStatus(1L, testDbId, testTableId,
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                LocalDateTime.now());

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addAnalyzeStatus(status);
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAnalyzeStatusMap().size());

        // 2. Mock EditLog.logRemoveAnalyzeStatus to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveAnalyzeStatus(any(AnalyzeStatus.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute dropAnalyzeStatus operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.dropAnalyzeStatus(testTableId);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAnalyzeStatusMap().size());
        AnalyzeStatus retrievedStatus = exceptionAnalyzeMgr.getAnalyzeStatus(status.getId());
        Assertions.assertNotNull(retrievedStatus);
    }

    // ==================== Drop External AnalyzeStatus Tests ====================

    @Test
    public void testDropExternalAnalyzeStatusNormalCase() throws Exception {
        // 1. Prepare test data and add external status first
        String tableUUID = "test_uuid";
        ExternalAnalyzeStatus status = new ExternalAnalyzeStatus(1L, "catalog", "db", "table",
                tableUUID, Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(), LocalDateTime.now());

        masterAnalyzeMgr.addAnalyzeStatus(status);
        Assertions.assertEquals(1, masterAnalyzeMgr.getAnalyzeStatusMap().size());

        // 2. Execute dropExternalAnalyzeStatus operation
        masterAnalyzeMgr.dropExternalAnalyzeStatus(tableUUID);

        // 3. Verify master state
        Assertions.assertEquals(0, masterAnalyzeMgr.getAnalyzeStatusMap().size());
    }

    @Test
    public void testDropExternalAnalyzeStatusEditLogException() throws Exception {
        // 1. Prepare test data and add external status first
        String tableUUID = "test_uuid";
        ExternalAnalyzeStatus status = new ExternalAnalyzeStatus(1L, "catalog", "db", "table",
                tableUUID, Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(), LocalDateTime.now());

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addAnalyzeStatus(status);
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAnalyzeStatusMap().size());

        // 2. Mock EditLog.logRemoveAnalyzeStatus to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveAnalyzeStatus(any(AnalyzeStatus.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute dropExternalAnalyzeStatus operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.dropExternalAnalyzeStatus(tableUUID);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAnalyzeStatusMap().size());
        AnalyzeStatus retrievedStatus = exceptionAnalyzeMgr.getAnalyzeStatus(status.getId());
        Assertions.assertNotNull(retrievedStatus);
    }

    // ==================== Drop AnalyzeJob Tests ====================

    @Test
    public void testDropAnalyzeJobNormalCase() throws Exception {
        // 1. Prepare test data and add job first
        NativeAnalyzeJob job = new NativeAnalyzeJob(testDbId, testTableId, Lists.newArrayList(),
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.now());

        masterAnalyzeMgr.addAnalyzeJob(job);
        Assertions.assertEquals(1, masterAnalyzeMgr.getAllAnalyzeJobList().size());

        // 2. Execute dropAnalyzeJob operation
        masterAnalyzeMgr.dropAnalyzeJob("default_catalog", "test_db", "test_table");

        // 3. Verify master state
        Assertions.assertEquals(0, masterAnalyzeMgr.getAllAnalyzeJobList().size());
    }

    @Test
    public void testDropAnalyzeJobEditLogException() throws Exception {
        // 1. Prepare test data and add job first
        NativeAnalyzeJob job = new NativeAnalyzeJob(testDbId, testTableId, Lists.newArrayList(),
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.now());

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addAnalyzeJob(job);
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAllAnalyzeJobList().size());

        // 2. Mock EditLog.logRemoveAnalyzeJob to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveAnalyzeJob(any(AnalyzeJob.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute dropAnalyzeJob operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.dropAnalyzeJob("default_catalog", "test_db", "test_table");
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getAllAnalyzeJobList().size());
        AnalyzeJob retrievedJob = exceptionAnalyzeMgr.getAnalyzeJob(job.getId());
        Assertions.assertNotNull(retrievedJob);
    }

    // ==================== Add BasicStatsMeta Tests ====================

    @Test
    public void testAddBasicStatsMetaNormalCase() throws Exception {
        // 1. Prepare test data
        BasicStatsMeta meta = new BasicStatsMeta(testDbId, testTableId, Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp(), 1000L);

        // 2. Verify initial state
        Assertions.assertNull(masterAnalyzeMgr.getTableBasicStatsMeta(testTableId));

        // 3. Execute addBasicStatsMeta operation
        masterAnalyzeMgr.addBasicStatsMeta(meta);

        // 4. Verify master state
        BasicStatsMeta addedMeta = masterAnalyzeMgr.getTableBasicStatsMeta(testTableId);
        Assertions.assertNotNull(addedMeta);
        Assertions.assertEquals(testDbId, addedMeta.getDbId());
        Assertions.assertEquals(testTableId, addedMeta.getTableId());
    }

    @Test
    public void testAddBasicStatsMetaEditLogException() throws Exception {
        // 1. Prepare test data
        BasicStatsMeta meta = new BasicStatsMeta(testDbId, testTableId, Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp(), 1000L);

        // 2. Create a separate AnalyzeMgr for exception testing
        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logAddBasicStatsMeta to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddBasicStatsMeta(any(BasicStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertNull(exceptionAnalyzeMgr.getTableBasicStatsMeta(testTableId));

        // 4. Execute addBasicStatsMeta operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.addBasicStatsMeta(meta);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertNull(exceptionAnalyzeMgr.getTableBasicStatsMeta(testTableId));
    }

    // ==================== Add ExternalBasicStatsMeta Tests ====================

    @Test
    public void testAddExternalBasicStatsMetaNormalCase() throws Exception {
        // 1. Prepare test data
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta("catalog", "db", "table",
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp());

        // 2. Verify initial state
        Assertions.assertNull(masterAnalyzeMgr.getExternalTableBasicStatsMeta("catalog", "db", "table"));

        // 3. Execute addExternalBasicStatsMeta operation
        masterAnalyzeMgr.addExternalBasicStatsMeta(meta);

        // 4. Verify master state
        ExternalBasicStatsMeta addedMeta = masterAnalyzeMgr.getExternalTableBasicStatsMeta("catalog", "db", "table");
        Assertions.assertNotNull(addedMeta);
        Assertions.assertEquals("catalog", addedMeta.getCatalogName());
        Assertions.assertEquals("db", addedMeta.getDbName());
        Assertions.assertEquals("table", addedMeta.getTableName());
    }

    @Test
    public void testAddExternalBasicStatsMetaEditLogException() throws Exception {
        // 1. Prepare test data
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta("catalog", "db", "table",
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp());

        // 2. Create a separate AnalyzeMgr for exception testing
        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logAddExternalBasicStatsMeta to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddExternalBasicStatsMeta(any(ExternalBasicStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertNull(exceptionAnalyzeMgr.getExternalTableBasicStatsMeta("catalog", "db", "table"));

        // 4. Execute addExternalBasicStatsMeta operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.addExternalBasicStatsMeta(meta);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertNull(exceptionAnalyzeMgr.getExternalTableBasicStatsMeta("catalog", "db", "table"));
    }

    // ==================== Remove ExternalBasicStatsMeta Tests ====================

    @Test
    public void testRemoveExternalBasicStatsMetaNormalCase() throws Exception {
        // 1. Prepare test data and add meta first
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta("catalog", "db", "table",
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp());

        masterAnalyzeMgr.addExternalBasicStatsMeta(meta);
        Assertions.assertNotNull(masterAnalyzeMgr.getExternalTableBasicStatsMeta("catalog", "db", "table"));

        // 2. Execute removeExternalBasicStatsMeta operation
        masterAnalyzeMgr.removeExternalBasicStatsMeta("catalog", "db", "table");

        // 3. Verify master state
        Assertions.assertNull(masterAnalyzeMgr.getExternalTableBasicStatsMeta("catalog", "db", "table"));
    }

    @Test
    public void testRemoveExternalBasicStatsMetaEditLogException() throws Exception {
        // 1. Prepare test data and add meta first
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta("catalog", "db", "table",
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp());

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addExternalBasicStatsMeta(meta);
        Assertions.assertNotNull(exceptionAnalyzeMgr.getExternalTableBasicStatsMeta("catalog", "db", "table"));

        // 2. Mock EditLog.logRemoveExternalBasicStatsMeta to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveExternalBasicStatsMeta(any(ExternalBasicStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute removeExternalBasicStatsMeta operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.removeExternalBasicStatsMeta("catalog", "db", "table");
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertNotNull(exceptionAnalyzeMgr.getExternalTableBasicStatsMeta("catalog", "db", "table"));
    }

    // ==================== Remove ExternalHistogramStatsMeta Tests ====================

    @Test
    public void testRemoveExternalHistogramStatsMetaNormalCase() throws Exception {
        // 1. Prepare test data and add meta first
        ExternalHistogramStatsMeta meta = new ExternalHistogramStatsMeta("catalog", "db", "table", "column",
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());

        masterAnalyzeMgr.addExternalHistogramStatsMeta(meta);
        Assertions.assertEquals(1, masterAnalyzeMgr.getExternalHistogramStatsMetaMap().size());

        // 2. Execute removeExternalHistogramStatsMeta operation
        masterAnalyzeMgr.removeExternalHistogramStatsMeta("catalog", "db", "table", Lists.newArrayList("column"));

        // 3. Verify master state
        Assertions.assertEquals(0, masterAnalyzeMgr.getExternalHistogramStatsMetaMap().size());
    }

    @Test
    public void testRemoveExternalHistogramStatsMetaEditLogException() throws Exception {
        // 1. Prepare test data and add meta first
        ExternalHistogramStatsMeta meta = new ExternalHistogramStatsMeta("catalog", "db", "table", "column",
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addExternalHistogramStatsMeta(meta);
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getExternalHistogramStatsMetaMap().size());

        // 2. Mock EditLog.logRemoveExternalHistogramStatsMeta to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveExternalHistogramStatsMeta(any(ExternalHistogramStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute removeExternalHistogramStatsMeta operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.removeExternalHistogramStatsMeta("catalog", "db", "table", Lists.newArrayList("column"));
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getExternalHistogramStatsMetaMap().size());
    }

    // ==================== Add MultiColumnStatsMeta Tests ====================

    @Test
    public void testAddMultiColumnStatsMetaNormalCase() throws Exception {
        // 1. Prepare test data
        Set<Integer> columnIds = new HashSet<>();
        columnIds.add(1);
        columnIds.add(2);
        List<StatsConstants.StatisticsType> statsTypes = Lists.newArrayList(StatsConstants.StatisticsType.MCDISTINCT);
        MultiColumnStatsMeta meta = new MultiColumnStatsMeta(testDbId, testTableId, columnIds,
                StatsConstants.AnalyzeType.FULL, statsTypes, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp());

        // 2. Verify initial state
        Assertions.assertEquals(0, masterAnalyzeMgr.getMultiColumnStatsMetaMap().size());

        // 3. Execute addMultiColumnStatsMeta operation
        masterAnalyzeMgr.addMultiColumnStatsMeta(meta);

        // 4. Verify master state
        Assertions.assertEquals(1, masterAnalyzeMgr.getMultiColumnStatsMetaMap().size());
    }

    @Test
    public void testAddMultiColumnStatsMetaEditLogException() throws Exception {
        // 1. Prepare test data
        Set<Integer> columnIds = new HashSet<>();
        columnIds.add(1);
        columnIds.add(2);
        List<StatsConstants.StatisticsType> statsTypes = Lists.newArrayList(StatsConstants.StatisticsType.MCDISTINCT);
        MultiColumnStatsMeta meta = new MultiColumnStatsMeta(testDbId, testTableId, columnIds,
                StatsConstants.AnalyzeType.FULL, statsTypes, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp());

        // 2. Create a separate AnalyzeMgr for exception testing
        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logAddMultiColumnStatsMeta to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddMultiColumnStatsMeta(any(MultiColumnStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getMultiColumnStatsMetaMap().size());

        // 4. Execute addMultiColumnStatsMeta operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.addMultiColumnStatsMeta(meta);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getMultiColumnStatsMetaMap().size());
    }

    // ==================== Add HistogramStatsMeta Tests ====================

    @Test
    public void testAddHistogramStatsMetaNormalCase() throws Exception {
        // 1. Prepare test data
        HistogramStatsMeta meta = new HistogramStatsMeta(testDbId, testTableId, "column",
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());

        // 2. Verify initial state
        Assertions.assertEquals(0, masterAnalyzeMgr.getHistogramStatsMetaMap().size());

        // 3. Execute addHistogramStatsMeta operation
        masterAnalyzeMgr.addHistogramStatsMeta(meta);

        // 4. Verify master state
        Assertions.assertEquals(1, masterAnalyzeMgr.getHistogramStatsMetaMap().size());
    }

    @Test
    public void testAddHistogramStatsMetaEditLogException() throws Exception {
        // 1. Prepare test data
        HistogramStatsMeta meta = new HistogramStatsMeta(testDbId, testTableId, "column",
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());

        // 2. Create a separate AnalyzeMgr for exception testing
        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logAddHistogramStatsMeta to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddHistogramStatsMeta(any(HistogramStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getHistogramStatsMetaMap().size());

        // 4. Execute addHistogramStatsMeta operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.addHistogramStatsMeta(meta);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getHistogramStatsMetaMap().size());
    }

    // ==================== Add ExternalHistogramStatsMeta Tests ====================

    @Test
    public void testAddExternalHistogramStatsMetaNormalCase() throws Exception {
        // 1. Prepare test data
        ExternalHistogramStatsMeta meta = new ExternalHistogramStatsMeta("catalog", "db", "table", "column",
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());

        // 2. Verify initial state
        Assertions.assertEquals(0, masterAnalyzeMgr.getExternalHistogramStatsMetaMap().size());

        // 3. Execute addExternalHistogramStatsMeta operation
        masterAnalyzeMgr.addExternalHistogramStatsMeta(meta);

        // 4. Verify master state
        Assertions.assertEquals(1, masterAnalyzeMgr.getExternalHistogramStatsMetaMap().size());
    }

    @Test
    public void testAddExternalHistogramStatsMetaEditLogException() throws Exception {
        // 1. Prepare test data
        ExternalHistogramStatsMeta meta = new ExternalHistogramStatsMeta("catalog", "db", "table", "column",
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());

        // 2. Create a separate AnalyzeMgr for exception testing
        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logAddExternalHistogramStatsMeta to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAddExternalHistogramStatsMeta(any(ExternalHistogramStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getExternalHistogramStatsMetaMap().size());

        // 4. Execute addExternalHistogramStatsMeta operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.addExternalHistogramStatsMeta(meta);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionAnalyzeMgr.getExternalHistogramStatsMetaMap().size());
    }

    // ==================== Drop BasicStatsMetaAndData Tests ====================

    @Test
    public void testDropBasicStatsMetaAndDataNormalCase() throws Exception {
        // 1. Prepare test data and add meta first
        BasicStatsMeta meta = new BasicStatsMeta(testDbId, testTableId, Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp(), 1000L);

        masterAnalyzeMgr.addBasicStatsMeta(meta);
        Assertions.assertNotNull(masterAnalyzeMgr.getTableBasicStatsMeta(testTableId));

        // 2. Execute dropBasicStatsMetaAndData operation
        Set<Long> tableIds = new HashSet<>();
        tableIds.add(testTableId);
        com.starrocks.qe.ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);
        statsConnectCtx.setThreadLocalInfo();

        // Mock StatisticExecutor to avoid actual execution
        new mockit.MockUp<StatisticExecutor>() {
            @mockit.Mock
            public boolean dropTableStatistics(com.starrocks.qe.ConnectContext statsConnectCtx, long tableId,
                                              StatsConstants.AnalyzeType analyzeType) {
                return true;
            }

            @mockit.Mock
            public boolean dropTableMultiColumnStatistics(com.starrocks.qe.ConnectContext statsConnectCtx, long tableId) {
                return true;
            }
        };

        masterAnalyzeMgr.dropBasicStatsMetaAndData(statsConnectCtx, tableIds);

        // 3. Verify master state
        Assertions.assertNull(masterAnalyzeMgr.getTableBasicStatsMeta(testTableId));
    }

    @Test
    public void testDropBasicStatsMetaAndDataEditLogException() throws Exception {
        // 1. Prepare test data and add meta first
        BasicStatsMeta meta = new BasicStatsMeta(testDbId, testTableId, Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp(), 1000L);

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addBasicStatsMeta(meta);
        Assertions.assertNotNull(exceptionAnalyzeMgr.getTableBasicStatsMeta(testTableId));

        // 2. Mock EditLog.logRemoveBasicStatsMeta to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveBasicStatsMeta(any(BasicStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Mock StatisticExecutor
        new mockit.MockUp<StatisticExecutor>() {
            @mockit.Mock
            public boolean dropTableStatistics(com.starrocks.qe.ConnectContext statsConnectCtx, long tableId,
                                              StatsConstants.AnalyzeType analyzeType) {
                return true;
            }

            @mockit.Mock
            public boolean dropTableMultiColumnStatistics(com.starrocks.qe.ConnectContext statsConnectCtx, long tableId) {
                return true;
            }
        };

        // 4. Execute dropBasicStatsMetaAndData operation and expect exception
        Set<Long> tableIds = new HashSet<>();
        tableIds.add(testTableId);
        com.starrocks.qe.ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);
        statsConnectCtx.setThreadLocalInfo();

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.dropBasicStatsMetaAndData(statsConnectCtx, tableIds);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertNotNull(exceptionAnalyzeMgr.getTableBasicStatsMeta(testTableId));
    }

    // ==================== Drop HistogramStatsMetaAndData Tests ====================

    @Test
    public void testDropHistogramStatsMetaAndDataNormalCase() throws Exception {
        // 1. Prepare test data and add meta first
        HistogramStatsMeta meta = new HistogramStatsMeta(testDbId, testTableId, "column",
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());

        masterAnalyzeMgr.addHistogramStatsMeta(meta);
        Assertions.assertEquals(1, masterAnalyzeMgr.getHistogramStatsMetaMap().size());

        // 2. Execute dropHistogramStatsMetaAndData operation
        Set<Long> tableIds = new HashSet<>();
        tableIds.add(testTableId);
        com.starrocks.qe.ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);
        statsConnectCtx.setThreadLocalInfo();

        // Mock StatisticExecutor
        new mockit.MockUp<StatisticExecutor>() {
            @mockit.Mock
            public boolean dropHistogram(com.starrocks.qe.ConnectContext statsConnectCtx, long tableId,
                                        List<String> columns) {
                return true;
            }
        };

        masterAnalyzeMgr.dropHistogramStatsMetaAndData(statsConnectCtx, tableIds);

        // 3. Verify master state
        Assertions.assertEquals(0, masterAnalyzeMgr.getHistogramStatsMetaMap().size());
    }

    @Test
    public void testDropHistogramStatsMetaAndDataEditLogException() throws Exception {
        // 1. Prepare test data and add meta first
        HistogramStatsMeta meta = new HistogramStatsMeta(testDbId, testTableId, "column",
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), StatsConstants.buildInitStatsProp());

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addHistogramStatsMeta(meta);
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getHistogramStatsMetaMap().size());

        // 2. Mock EditLog.logRemoveHistogramStatsMeta to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveHistogramStatsMeta(any(HistogramStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Mock StatisticExecutor
        new mockit.MockUp<StatisticExecutor>() {
            @mockit.Mock
            public boolean dropHistogram(com.starrocks.qe.ConnectContext statsConnectCtx, long tableId,
                                        List<String> columns) {
                return true;
            }
        };

        // 4. Execute dropHistogramStatsMetaAndData operation and expect exception
        Set<Long> tableIds = new HashSet<>();
        tableIds.add(testTableId);
        com.starrocks.qe.ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);
        statsConnectCtx.setThreadLocalInfo();

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.dropHistogramStatsMetaAndData(statsConnectCtx, tableIds);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getHistogramStatsMetaMap().size());
    }

    // ==================== Drop MultiColumnStatsMetaAndData Tests ====================

    @Test
    public void testDropMultiColumnStatsMetaAndDataNormalCase() throws Exception {
        // 1. Prepare test data and add meta first
        Set<Integer> columnIds = new HashSet<>();
        columnIds.add(1);
        columnIds.add(2);
        List<StatsConstants.StatisticsType> statsTypes = Lists.newArrayList(StatsConstants.StatisticsType.MCDISTINCT);
        MultiColumnStatsMeta meta = new MultiColumnStatsMeta(testDbId, testTableId, columnIds,
                StatsConstants.AnalyzeType.FULL, statsTypes, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp());

        masterAnalyzeMgr.addMultiColumnStatsMeta(meta);
        Assertions.assertEquals(1, masterAnalyzeMgr.getMultiColumnStatsMetaMap().size());

        // 2. Execute dropMultiColumnStatsMetaAndData operation
        Set<Long> tableIds = new HashSet<>();
        tableIds.add(testTableId);
        com.starrocks.qe.ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);
        statsConnectCtx.setThreadLocalInfo();

        // Mock StatisticExecutor
        new mockit.MockUp<StatisticExecutor>() {
            @mockit.Mock
            public boolean dropTableMultiColumnStatistics(com.starrocks.qe.ConnectContext statsConnectCtx, long tableId) {
                return true;
            }
        };

        masterAnalyzeMgr.dropMultiColumnStatsMetaAndData(statsConnectCtx, tableIds);

        // 3. Verify master state
        Assertions.assertEquals(0, masterAnalyzeMgr.getMultiColumnStatsMetaMap().size());
    }

    @Test
    public void testDropMultiColumnStatsMetaAndDataEditLogException() throws Exception {
        // 1. Prepare test data and add meta first
        Set<Integer> columnIds = new HashSet<>();
        columnIds.add(1);
        columnIds.add(2);
        List<StatsConstants.StatisticsType> statsTypes = Lists.newArrayList(StatsConstants.StatisticsType.MCDISTINCT);
        MultiColumnStatsMeta meta = new MultiColumnStatsMeta(testDbId, testTableId, columnIds,
                StatsConstants.AnalyzeType.FULL, statsTypes, LocalDateTime.now(),
                StatsConstants.buildInitStatsProp());

        AnalyzeMgr exceptionAnalyzeMgr = new AnalyzeMgr();
        exceptionAnalyzeMgr.addMultiColumnStatsMeta(meta);
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getMultiColumnStatsMetaMap().size());

        // 2. Mock EditLog.logRemoveMultiColumnStatsMeta to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveMultiColumnStatsMeta(any(MultiColumnStatsMeta.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Mock StatisticExecutor
        new mockit.MockUp<StatisticExecutor>() {
            @mockit.Mock
            public boolean dropTableMultiColumnStatistics(com.starrocks.qe.ConnectContext statsConnectCtx, long tableId) {
                return true;
            }
        };

        // 4. Execute dropMultiColumnStatsMetaAndData operation and expect exception
        Set<Long> tableIds = new HashSet<>();
        tableIds.add(testTableId);
        com.starrocks.qe.ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);
        statsConnectCtx.setThreadLocalInfo();

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAnalyzeMgr.dropMultiColumnStatsMetaAndData(statsConnectCtx, tableIds);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(1, exceptionAnalyzeMgr.getMultiColumnStatsMetaMap().size());
    }
}

