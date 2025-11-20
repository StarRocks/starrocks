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
import com.google.common.collect.Maps;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.persist.AlterRoutineLoadJobOperationLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.expression.RoutineLoadDataSourceProperties;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class RoutineLoadJobEditLogTest {
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


    // Helper method to create a RoutineLoadJob for testing
    private RoutineLoadJob createRoutineLoadJob(long jobId, String jobName, long dbId, long tableId) {
        String brokerList = "localhost:9092";
        String topic = "test_topic";
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(
                jobId, jobName, dbId, tableId, brokerList, topic);
        routineLoadJob.setOrigStmt(new OriginStatementInfo(
                "CREATE ROUTINE LOAD " + jobName + " ON test_table FROM KAFKA", 0));
        return routineLoadJob;
    }

    // Helper method to create a PulsarRoutineLoadJob for testing
    private RoutineLoadJob createPulsarRoutineLoadJob(long jobId, String jobName, long dbId, long tableId) {
        String serviceUrl = "pulsar://localhost:6650";
        String topic = "test_topic";
        String subscription = "test_subscription";
        PulsarRoutineLoadJob routineLoadJob = new PulsarRoutineLoadJob(
                jobId, jobName, dbId, tableId, serviceUrl, topic, subscription);
        routineLoadJob.setOrigStmt(new OriginStatementInfo(
                "CREATE ROUTINE LOAD " + jobName + " ON test_table FROM PULSAR", 0));
        return routineLoadJob;
    }

    // Helper method to create a KafkaRoutineLoadJob with customKafkaPartitions
    private RoutineLoadJob createKafkaRoutineLoadJobWithCustomPartitions(
            long jobId, String jobName, long dbId, long tableId, List<Integer> customPartitions) {
        String brokerList = "localhost:9092";
        String topic = "test_topic";
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(
                jobId, jobName, dbId, tableId, brokerList, topic);
        routineLoadJob.setOrigStmt(new OriginStatementInfo(
                "CREATE ROUTINE LOAD " + jobName + " ON test_table FROM KAFKA", 0));
        if (customPartitions != null && !customPartitions.isEmpty()) {
            Deencapsulation.setField(routineLoadJob, "customKafkaPartitions", customPartitions);
        }
        return routineLoadJob;
    }

    // Helper method to create a PulsarRoutineLoadJob with customPulsarPartitions
    private RoutineLoadJob createPulsarRoutineLoadJobWithCustomPartitions(
            long jobId, String jobName, long dbId, long tableId, List<String> customPartitions) {
        String serviceUrl = "pulsar://localhost:6650";
        String topic = "test_topic";
        String subscription = "test_subscription";
        PulsarRoutineLoadJob routineLoadJob = new PulsarRoutineLoadJob(
                jobId, jobName, dbId, tableId, serviceUrl, topic, subscription);
        routineLoadJob.setOrigStmt(new OriginStatementInfo(
                "CREATE ROUTINE LOAD " + jobName + " ON test_table FROM PULSAR", 0));
        if (customPartitions != null && !customPartitions.isEmpty()) {
            Deencapsulation.setField(routineLoadJob, "customPulsarPartitions", customPartitions);
        }
        return routineLoadJob;
    }

    // Helper method to create a SmallFile for testing
    private void createSmallFile(long dbId, String catalog, String fileName, long fileId) {
        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        SmallFile smallFile = new SmallFile(dbId, catalog, fileName, fileId, "test_content", 12, "test_md5", true);
        smallFileMgr.replayCreateFile(smallFile);
    }

    @Test
    public void testUpdateStateToPausedNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db";
        String jobName = "test_routine_load_job";
        String tableName = "test_table";
        long dbId = 10001L;
        long tableId = 20001L;
        long partitionId = 30001L;
        long indexId = 40001L;
        long jobId = 1L;
        
        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        
        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        
        // 2. Execute updateState operation (master side) - to PAUSED
        ErrorReason reason = new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, "Test pause reason");
        job.updateState(RoutineLoadJob.JobState.PAUSED, reason);
        
        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, job.getState());
        
        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();
        
        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);
        
        // Then replay the update state operation
        RoutineLoadOperation operation = (RoutineLoadOperation) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CHANGE_ROUTINE_LOAD_JOB_V2);
        
        // Execute follower replay
        followerRoutineLoadMgr.replayChangeRoutineLoadJob(operation);
        
        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, followerJob.getState());
    }

    @Test
    public void testUpdateStateToStoppedNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_stopped";
        String jobName = "test_routine_load_job_stopped";
        String tableName = "test_table";
        long dbId = 10002L;
        long tableId = 20002L;
        long partitionId = 30002L;
        long indexId = 40002L;
        long jobId = 2L;
        
        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        
        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        
        // First change to RUNNING state (which doesn't write EditLog)
        job.updateState(RoutineLoadJob.JobState.RUNNING, null);
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        
        // 2. Execute updateState operation (master side) - to STOPPED
        ErrorReason reason = new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, "Test stop reason");
        job.updateState(RoutineLoadJob.JobState.STOPPED, reason);
        
        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.STOPPED, job.getState());
        
        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();
        
        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);
        
        // Then replay the update state operation
        RoutineLoadOperation operation = (RoutineLoadOperation) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CHANGE_ROUTINE_LOAD_JOB_V2);
        
        // Execute follower replay
        followerRoutineLoadMgr.replayChangeRoutineLoadJob(operation);
        
        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(RoutineLoadJob.JobState.STOPPED, followerJob.getState());
    }

    @Test
    public void testUpdateStateToCancelledNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_cancelled";
        String jobName = "test_routine_load_job_cancelled";
        String tableName = "test_table";
        long dbId = 10003L;
        long tableId = 20003L;
        long partitionId = 30003L;
        long indexId = 40003L;
        long jobId = 3L;
        
        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        
        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        
        // First change to RUNNING state
        job.updateState(RoutineLoadJob.JobState.RUNNING, null);
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        
        // 2. Execute updateState operation (master side) - to CANCELLED
        ErrorReason reason = new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, "Test cancel reason");
        job.updateState(RoutineLoadJob.JobState.CANCELLED, reason);
        
        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.CANCELLED, job.getState());
        
        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();
        
        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);
        
        // Then replay the update state operation
        RoutineLoadOperation operation = (RoutineLoadOperation) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CHANGE_ROUTINE_LOAD_JOB_V2);
        
        // Execute follower replay
        followerRoutineLoadMgr.replayChangeRoutineLoadJob(operation);
        
        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(RoutineLoadJob.JobState.CANCELLED, followerJob.getState());
    }

    @Test
    public void testUpdateStateToNeedScheduleNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_schedule";
        String jobName = "test_routine_load_job_schedule";
        String tableName = "test_table";
        long dbId = 10004L;
        long tableId = 20004L;
        long partitionId = 30004L;
        long indexId = 40004L;
        long jobId = 4L;
        
        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        
        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        
        // First change to PAUSED state
        ErrorReason pauseReason = new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, "Test pause reason");
        job.updateState(RoutineLoadJob.JobState.PAUSED, pauseReason);
        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, job.getState());
        
        // 2. Execute updateState operation (master side) - to NEED_SCHEDULE (resume from PAUSED)
        job.updateState(RoutineLoadJob.JobState.NEED_SCHEDULE, null);
        
        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        
        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();
        
        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);
        
        // Replay the pause operation
        RoutineLoadOperation pauseOp = (RoutineLoadOperation) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CHANGE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayChangeRoutineLoadJob(pauseOp);
        
        // Then replay the update state operation to NEED_SCHEDULE
        RoutineLoadOperation operation = (RoutineLoadOperation) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CHANGE_ROUTINE_LOAD_JOB_V2);
        
        // Execute follower replay
        followerRoutineLoadMgr.replayChangeRoutineLoadJob(operation);
        
        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, followerJob.getState());
    }

    @Test
    public void testUpdateStateToRunningNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_running";
        String jobName = "test_routine_load_job_running";
        String tableName = "test_table";
        long dbId = 10005L;
        long tableId = 20005L;
        long partitionId = 30005L;
        long indexId = 40005L;
        long jobId = 5L;
        
        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        
        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        
        // 2. Execute updateState operation (master side) - to RUNNING
        // Note: RUNNING state doesn't write EditLog, so we only test the state change
        job.updateState(RoutineLoadJob.JobState.RUNNING, null);
        
        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        
        // Note: Since RUNNING doesn't write EditLog, there's no follower replay to test
    }

    @Test
    public void testUpdateStateEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "exception_db";
        String jobName = "exception_routine_load_job";
        String tableName = "test_table";
        long dbId = 10006L;
        long tableId = 20006L;
        long partitionId = 30006L;
        long indexId = 40006L;
        long jobId = 6L;
        
        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        
        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        RoutineLoadJob.JobState initialState = job.getState();
        
        // 2. Create a separate RoutineLoadMgr for exception testing
        RoutineLoadMgr exceptionRoutineLoadMgr = new RoutineLoadMgr();
        exceptionRoutineLoadMgr.addRoutineLoadJob(createRoutineLoadJob(jobId, jobName, dbId, tableId), dbName);
        RoutineLoadJob exceptionJob = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(exceptionJob);
        
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        
        // 3. Mock EditLog.logOpRoutineLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logOpRoutineLoadJob(any(RoutineLoadOperation.class), any());
        
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // 4. Execute updateState operation and expect exception
        final RoutineLoadJob finalExceptionJob = exceptionJob;
        ErrorReason reason = new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, "Test pause reason");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            finalExceptionJob.updateState(RoutineLoadJob.JobState.PAUSED, reason);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 5. Verify leader memory state remains unchanged after exception
        // The state should remain as initial state (NEED_SCHEDULE)
        RoutineLoadJob jobAfterException = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(jobAfterException);
        Assertions.assertEquals(initialState, jobAfterException.getState());
    }

    @Test
    public void testUpdateStateToPausedEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "exception_db_paused";
        String jobName = "exception_routine_load_job_paused";
        String tableName = "test_table";
        long dbId = 10007L;
        long tableId = 20007L;
        long partitionId = 30007L;
        long indexId = 40007L;
        long jobId = 7L;
        
        // Create a separate RoutineLoadMgr for exception testing
        RoutineLoadMgr exceptionRoutineLoadMgr = new RoutineLoadMgr();
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        exceptionRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        
        // Get the job from manager
        RoutineLoadJob job = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        RoutineLoadJob.JobState initialState = job.getState();
        
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        
        // 2. Mock EditLog.logOpRoutineLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logOpRoutineLoadJob(any(RoutineLoadOperation.class), any());
        
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // 3. Execute updateState operation and expect exception
        final RoutineLoadJob finalJob = job;
        ErrorReason reason = new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, "Test pause reason");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            finalJob.updateState(RoutineLoadJob.JobState.PAUSED, reason);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 4. Verify leader memory state remains unchanged after exception
        RoutineLoadJob jobAfterException = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(jobAfterException);
        Assertions.assertEquals(initialState, jobAfterException.getState());
    }

    @Test
    public void testUpdateStateToStoppedEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "exception_db_stopped";
        String jobName = "exception_routine_load_job_stopped";
        String tableName = "test_table";
        long dbId = 10008L;
        long tableId = 20008L;
        long partitionId = 30008L;
        long indexId = 40008L;
        long jobId = 8L;
        
        // Create a separate RoutineLoadMgr for exception testing
        RoutineLoadMgr exceptionRoutineLoadMgr = new RoutineLoadMgr();
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        exceptionRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        
        // Get the job from manager
        RoutineLoadJob job = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        
        // First change to RUNNING state (which doesn't write EditLog)
        job.updateState(RoutineLoadJob.JobState.RUNNING, null);
        RoutineLoadJob.JobState initialState = job.getState();
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, initialState);
        
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        
        // 2. Mock EditLog.logOpRoutineLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logOpRoutineLoadJob(any(RoutineLoadOperation.class), any());
        
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // 3. Execute updateState operation and expect exception
        final RoutineLoadJob finalJob = job;
        ErrorReason reason = new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, "Test stop reason");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            finalJob.updateState(RoutineLoadJob.JobState.STOPPED, reason);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 4. Verify leader memory state remains unchanged after exception
        RoutineLoadJob jobAfterException = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(jobAfterException);
        Assertions.assertEquals(initialState, jobAfterException.getState());
    }

    @Test
    public void testUpdateStateToCancelledEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "exception_db_cancelled";
        String jobName = "exception_routine_load_job_cancelled";
        String tableName = "test_table";
        long dbId = 10009L;
        long tableId = 20009L;
        long partitionId = 30009L;
        long indexId = 40009L;
        long jobId = 9L;
        
        // Create a separate RoutineLoadMgr for exception testing
        RoutineLoadMgr exceptionRoutineLoadMgr = new RoutineLoadMgr();
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        exceptionRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);
        
        // Get the job from manager
        RoutineLoadJob job = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        
        // First change to RUNNING state (which doesn't write EditLog)
        job.updateState(RoutineLoadJob.JobState.RUNNING, null);
        RoutineLoadJob.JobState initialState = job.getState();
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, initialState);
        
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        
        // 2. Mock EditLog.logOpRoutineLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logOpRoutineLoadJob(any(RoutineLoadOperation.class), any());
        
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // 3. Execute updateState operation and expect exception
        final RoutineLoadJob finalJob = job;
        ErrorReason reason = new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, "Test cancel reason");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            finalJob.updateState(RoutineLoadJob.JobState.CANCELLED, reason);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 4. Verify leader memory state remains unchanged after exception
        RoutineLoadJob jobAfterException = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(jobAfterException);
        Assertions.assertEquals(initialState, jobAfterException.getState());
    }

    // ==================== modifyJob tests for Kafka ====================

    @Test
    public void testModifyJobKafkaJobPropertiesNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_kafka";
        String jobName = "test_modify_kafka_job";
        long dbId = 20001L;
        long tableId = 20002L;
        long jobId = 10001L;

        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        int originalDesiredConcurrentNum = (int) Deencapsulation.getField(job, "desireTaskConcurrentNum");

        // 2. Execute modifyJob operation (master side) - modify job properties
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "5");
        jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "1000");
        jobProperties.put(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY, "0.5");
        jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY, "30");
        jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY, "200000");

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName + " PROPERTIES (\"desired_concurrent_number\"=\"5\")", 0);
        job.modifyJob(null, jobProperties, null, originStatement);

        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(5, (int) Deencapsulation.getField(job, "desireTaskConcurrentNum"));
        Assertions.assertEquals(1000L, (long) Deencapsulation.getField(job, "maxErrorNum"));
        Assertions.assertEquals(0.5, (double) Deencapsulation.getField(job, "maxFilterRatio"), 0.001);

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(5, (int) Deencapsulation.getField(followerJob, "desireTaskConcurrentNum"));
        Assertions.assertEquals(1000L, (long) Deencapsulation.getField(followerJob, "maxErrorNum"));
        Assertions.assertEquals(0.5, (double) Deencapsulation.getField(followerJob, "maxFilterRatio"), 0.001);
    }

    @Test
    public void testModifyJobKafkaDataSourcePropertiesNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_kafka_ds";
        String jobName = "test_modify_kafka_ds_job";
        long dbId = 20003L;
        long tableId = 20004L;
        long jobId = 10002L;

        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation (master side) - modify data source properties
        Map<String, String> kafkaProperties = new HashMap<>();
        kafkaProperties.put(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "OFFSET_BEGINNING");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "KAFKA", kafkaProperties);
        
        // Manually populate customKafkaProperties so hasAnalyzedProperties() returns true
        // This simulates what happens during analyze() but without validation
        dataSourceProperties.getCustomKafkaProperties().put(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "OFFSET_BEGINNING");

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName + " FROM KAFKA (\"kafka_default_offset\"=\"OFFSET_BEGINNING\")", 0);
        job.modifyJob(null, null, dataSourceProperties, originStatement);

        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(KafkaProgress.OFFSET_BEGINNING_VAL, ((KafkaRoutineLoadJob) job).getKafkaDefaultOffSet());

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(KafkaProgress.OFFSET_BEGINNING_VAL,
                ((KafkaRoutineLoadJob) followerJob).getKafkaDefaultOffSet());
    }

    @Test
    public void testModifyJobKafkaRoutineLoadDescNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_kafka_desc";
        String jobName = "test_modify_kafka_desc_job";
        long dbId = 20005L;
        long tableId = 20006L;
        long jobId = 10003L;

        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation (master side) - modify routine load desc
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc();
        routineLoadDesc.setColumnSeparator(new ColumnSeparator(","));

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + " COLUMNS TERMINATED BY ','", 0);
        job.modifyJob(routineLoadDesc, null, null, originStatement);

        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        ColumnSeparator masterSeparator = ((KafkaRoutineLoadJob) job).getColumnSeparator();
        Assertions.assertNotNull(masterSeparator);
        Assertions.assertEquals(",", masterSeparator.getColumnSeparator());

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        ColumnSeparator followerSeparator = ((KafkaRoutineLoadJob) followerJob).getColumnSeparator();
        Assertions.assertNotNull(followerSeparator);
        Assertions.assertEquals(",", followerSeparator.getColumnSeparator());
    }

    @Test
    public void testModifyJobKafkaCheckCommonJobPropertiesException() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_kafka_exception";
        String jobName = "test_modify_kafka_exception_job";
        long dbId = 20007L;
        long tableId = 20008L;
        long jobId = 10004L;

        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        int originalDesiredConcurrentNum = (int) Deencapsulation.getField(job, "desireTaskConcurrentNum");

        // 2. Execute modifyJob operation with invalid job properties - should throw exception
        final RoutineLoadJob finalJob = job;
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "invalid_number");

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName + " PROPERTIES (\"desired_concurrent_number\"=\"invalid_number\")", 0);

        // 3. Expect DdlException or NumberFormatException
        Assertions.assertThrows(Exception.class, () -> {
            finalJob.modifyJob(null, jobProperties, null, originStatement);
        });

        // 4. Verify state remains unchanged after exception
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(originalDesiredConcurrentNum, (int) Deencapsulation.getField(job, "desireTaskConcurrentNum"));
    }

    @Test
    public void testModifyJobKafkaCheckDataSourcePropertiesException() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_kafka_ds_exception";
        String jobName = "test_modify_kafka_ds_exception_job";
        long dbId = 20009L;
        long tableId = 20010L;
        long jobId = 10005L;

        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation with invalid data source properties - should throw exception
        final RoutineLoadJob finalJob = job;
        Map<String, String> kafkaProperties = new HashMap<>();
        kafkaProperties.put(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "INVALID_OFFSET");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "KAFKA", kafkaProperties);
        
        // Manually populate customKafkaProperties so hasAnalyzedProperties() returns true
        // This simulates what happens during analyze() but without validation
        dataSourceProperties.getCustomKafkaProperties().put(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, "INVALID_OFFSET");

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName + " FROM KAFKA (\"kafka_default_offset\"=\"INVALID_OFFSET\")", 0);

        // 3. Expect DdlException - the validation happens in checkDataSourceProperties when getKafkaOffset is called
        Assertions.assertThrows(Exception.class, () -> {
            finalJob.modifyJob(null, null, dataSourceProperties, originStatement);
        });

        // 4. Verify that defaultOffset remains unchanged after exception
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertNull(((KafkaRoutineLoadJob) job).getKafkaDefaultOffSet());
    }

    @Test
    public void testModifyJobKafkaEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_kafka_editlog_exception";
        String jobName = "test_modify_kafka_editlog_exception_job";
        long dbId = 20011L;
        long tableId = 20012L;
        long jobId = 10006L;

        // Create a separate RoutineLoadMgr for exception testing
        RoutineLoadMgr exceptionRoutineLoadMgr = new RoutineLoadMgr();
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        exceptionRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        int originalDesiredConcurrentNum = (int) Deencapsulation.getField(job, "desireTaskConcurrentNum");

        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);

        // 2. Mock EditLog.logAlterRoutineLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterRoutineLoadJob(any(AlterRoutineLoadJobOperationLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute modifyJob operation and expect exception
        final RoutineLoadJob finalJob = job;
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "5");
        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName + " PROPERTIES (\"desired_concurrent_number\"=\"5\")", 0);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            finalJob.modifyJob(null, jobProperties, null, originStatement);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        RoutineLoadJob jobAfterException = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(jobAfterException);
        Assertions.assertEquals(originalDesiredConcurrentNum,
                (int) Deencapsulation.getField(jobAfterException, "desireTaskConcurrentNum"));
    }

    // ==================== modifyJob tests for Pulsar ====================

    @Test
    public void testModifyJobPulsarJobPropertiesNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_pulsar";
        String jobName = "test_modify_pulsar_job";
        long dbId = 30001L;
        long tableId = 30002L;
        long jobId = 20001L;

        // Create and add PulsarRoutineLoadJob
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        int originalDesiredConcurrentNum = (int) Deencapsulation.getField(job, "desireTaskConcurrentNum");

        // 2. Execute modifyJob operation (master side) - modify job properties
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "5");
        jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "1000");

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName + " PROPERTIES (\"desired_concurrent_number\"=\"5\")", 0);
        job.modifyJob(null, jobProperties, null, originStatement);

        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(5, (int) Deencapsulation.getField(job, "desireTaskConcurrentNum"));
        Assertions.assertEquals(1000L, (long) Deencapsulation.getField(job, "maxErrorNum"));

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(5, (int) Deencapsulation.getField(followerJob, "desireTaskConcurrentNum"));
        Assertions.assertEquals(1000L, (long) Deencapsulation.getField(followerJob, "maxErrorNum"));
    }

    @Test
    public void testModifyJobPulsarDataSourcePropertiesNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_pulsar_ds";
        String jobName = "test_modify_pulsar_ds_job";
        long dbId = 30003L;
        long tableId = 30004L;
        long jobId = 20002L;

        // Create and add PulsarRoutineLoadJob
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation (master side) - modify data source properties
        Map<String, String> pulsarProperties = new HashMap<>();
        pulsarProperties.put(CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION, "POSITION_EARLIEST");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "PULSAR", pulsarProperties);
        
        // Manually populate customPulsarProperties so hasAnalyzedProperties() returns true
        // This simulates what happens during analyze() but without validation
        dataSourceProperties.getCustomPulsarProperties().put(
                CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION, "POSITION_EARLIEST");

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName +
                        " FROM PULSAR (\"pulsar_default_initial_position\"=\"POSITION_EARLIEST\")", 0);
        job.modifyJob(null, null, dataSourceProperties, originStatement);

        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertTrue(job instanceof PulsarRoutineLoadJob);
        // 获取defaultInitialPosition属性，并校验
        Assertions.assertEquals(PulsarRoutineLoadJob.POSITION_EARLIEST_VAL,
                (long) Deencapsulation.getField(job, "defaultInitialPosition"));

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(PulsarRoutineLoadJob.POSITION_EARLIEST_VAL,
                (long) Deencapsulation.getField(followerJob, "defaultInitialPosition"));
    }

    @Test
    public void testModifyJobPulsarRoutineLoadDescNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_pulsar_desc";
        String jobName = "test_modify_pulsar_desc_job";
        long dbId = 30005L;
        long tableId = 30006L;
        long jobId = 20003L;

        // Create and add PulsarRoutineLoadJob
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation (master side) - modify routine load desc
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc();
        routineLoadDesc.setColumnSeparator(new ColumnSeparator("|"));

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + " COLUMNS TERMINATED BY '|'", 0);
        job.modifyJob(routineLoadDesc, null, null, originStatement);

        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        ColumnSeparator masterSeparator = (ColumnSeparator) Deencapsulation.getField(job, "columnSeparator");
        Assertions.assertNotNull(masterSeparator);
        Assertions.assertEquals("|", masterSeparator.getColumnSeparator());
        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        ColumnSeparator followerSeparator = (ColumnSeparator) Deencapsulation.getField(followerJob, "columnSeparator");
        Assertions.assertNotNull(followerSeparator);
        Assertions.assertEquals("|", followerSeparator.getColumnSeparator());
    }

    @Test
    public void testModifyJobPulsarCheckDataSourcePropertiesException() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_pulsar_ds_exception";
        String jobName = "test_modify_pulsar_ds_exception_job";
        long dbId = 30007L;
        long tableId = 30008L;
        long jobId = 20004L;

        // Create and add PulsarRoutineLoadJob
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation with invalid data source properties - should throw exception
        final RoutineLoadJob finalJob = job;
        Map<String, String> pulsarProperties = new HashMap<>();
        pulsarProperties.put(CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION, "INVALID_POSITION");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "PULSAR", pulsarProperties);
        
        // Manually populate customPulsarProperties so hasAnalyzedProperties() returns true
        // This simulates what happens during analyze() but without validation
        dataSourceProperties.getCustomPulsarProperties().put(
                CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION, "INVALID_POSITION");

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName +
                        " FROM PULSAR (\"pulsar_default_initial_position\"=\"INVALID_POSITION\")", 0);

        // 3. Expect DdlException - the validation happens in checkDataSourceProperties when getPulsarPosition is called
        Assertions.assertThrows(Exception.class, () -> {
            finalJob.modifyJob(null, null, dataSourceProperties, originStatement);
        });
        // 4. Verify that defaultInitialPosition remains unchanged after exception
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertNull((Long) Deencapsulation.getField(job, "defaultInitialPosition"));
    }

    @Test
    public void testModifyJobPulsarEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_pulsar_editlog_exception";
        String jobName = "test_modify_pulsar_editlog_exception_job";
        long dbId = 30009L;
        long tableId = 30010L;
        long jobId = 20005L;

        // Create a separate RoutineLoadMgr for exception testing
        RoutineLoadMgr exceptionRoutineLoadMgr = new RoutineLoadMgr();
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJob(jobId, jobName, dbId, tableId);
        exceptionRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        int originalDesiredConcurrentNum = (int) Deencapsulation.getField(job, "desireTaskConcurrentNum");

        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);

        // 2. Mock EditLog.logAlterRoutineLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterRoutineLoadJob(any(AlterRoutineLoadJobOperationLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute modifyJob operation and expect exception
        final RoutineLoadJob finalJob = job;
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "5");
        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName + " PROPERTIES (\"desired_concurrent_number\"=\"5\")", 0);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            finalJob.modifyJob(null, jobProperties, null, originStatement);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        RoutineLoadJob jobAfterException = exceptionRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(jobAfterException);
        Assertions.assertEquals(originalDesiredConcurrentNum,
                (int) Deencapsulation.getField(jobAfterException, "desireTaskConcurrentNum"));
    }

    @Test
    public void testModifyJobKafkaCheckCommonJobPropertiesMaxErrorNumberException() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_kafka_max_error_exception";
        String jobName = "test_modify_kafka_max_error_exception_job";
        long dbId = 20013L;
        long tableId = 20014L;
        long jobId = 10007L;

        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        long originalMaxErrorNum = (long) Deencapsulation.getField(job, "maxErrorNum");

        // 2. Execute modifyJob operation with invalid max_error_number - should throw exception
        final RoutineLoadJob finalJob = job;
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "invalid_long");

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName + " PROPERTIES (\"max_error_number\"=\"invalid_long\")", 0);

        // 3. Expect NumberFormatException wrapped in DdlException
        Assertions.assertThrows(Exception.class, () -> {
            finalJob.modifyJob(null, jobProperties, null, originStatement);
        });

        // 4. Verify state remains unchanged after exception
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(originalMaxErrorNum, (long) Deencapsulation.getField(job, "maxErrorNum"));
    }

    @Test
    public void testModifyJobKafkaCheckCommonJobPropertiesMaxFilterRatioException() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_modify_kafka_filter_ratio_exception";
        String jobName = "test_modify_kafka_filter_ratio_exception_job";
        long dbId = 20015L;
        long tableId = 20016L;
        long jobId = 10008L;

        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        double originalMaxFilterRatio = (double) Deencapsulation.getField(job, "maxFilterRatio");

        // 2. Execute modifyJob operation with invalid max_filter_ratio - should throw exception
        final RoutineLoadJob finalJob = job;
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY, "invalid_double");

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD " + jobName + " PROPERTIES (\"max_filter_ratio\"=\"invalid_double\")", 0);

        // 3. Expect NumberFormatException wrapped in DdlException
        Assertions.assertThrows(Exception.class, () -> {
            finalJob.modifyJob(null, jobProperties, null, originStatement);
        });

        // 4. Verify state remains unchanged after exception
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(originalMaxFilterRatio, (double) Deencapsulation.getField(job, "maxFilterRatio"), 0.001);
    }

    // ==================== Additional modifyJob tests for Kafka partition offsets ====================

    @Test
    public void testModifyJobKafkaPartitionOffsetsWithCustomPartitions() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_kafka_partition_custom";
        String jobName = "test_kafka_partition_custom_job";
        long dbId = 40001L;
        long tableId = 40002L;
        long jobId = 30001L;

        // Create KafkaRoutineLoadJob with custom partitions
        List<Integer> customPartitions = Lists.newArrayList(0, 1, 2);
        RoutineLoadJob routineLoadJob = createKafkaRoutineLoadJobWithCustomPartitions(
                jobId, jobName, dbId, tableId, customPartitions);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation (master side) - modify partition offsets
        Map<String, String> kafkaProperties = new HashMap<>();
        kafkaProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0,1");
        kafkaProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1000,2000");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "KAFKA", kafkaProperties);
        
        // Analyze properties to populate kafkaPartitionOffsets
        try {
            CreateRoutineLoadStmt.analyzeKafkaPartitionProperty(
                    kafkaProperties.get(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY),
                    Maps.newHashMap(), dataSourceProperties.getKafkaPartitionOffsets());
            CreateRoutineLoadStmt.analyzeKafkaOffsetProperty(
                    kafkaProperties.get(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY),
                    dataSourceProperties.getKafkaPartitionOffsets());
        } catch (Exception e) {
            Assertions.fail("Failed to analyze kafka properties: " + e.getMessage());
        }

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM KAFKA (\"kafka_partitions\"=\"0,1\", \"kafka_offsets\"=\"1000,2000\")", 0);
        job.modifyJob(null, null, dataSourceProperties, originStatement);

        // 3. Verify master state - check that offsets were modified
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        KafkaProgress progress = (KafkaProgress) job.getProgress();
        Map<Integer, Long> partitionOffsets = progress.getPartitionIdToOffset();
        Assertions.assertTrue(partitionOffsets.containsKey(0));
        Assertions.assertTrue(partitionOffsets.containsKey(1));
        Assertions.assertEquals(1000L, partitionOffsets.get(0).longValue());
        Assertions.assertEquals(2000L, partitionOffsets.get(1).longValue());

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        KafkaProgress followerProgress = (KafkaProgress) followerJob.getProgress();
        Map<Integer, Long> followerPartitionOffsets = followerProgress.getPartitionIdToOffset();
        Assertions.assertTrue(followerPartitionOffsets.containsKey(0));
        Assertions.assertTrue(followerPartitionOffsets.containsKey(1));
        Assertions.assertEquals(1000L, followerPartitionOffsets.get(0).longValue());
        Assertions.assertEquals(2000L, followerPartitionOffsets.get(1).longValue());
    }

    @Test
    public void testModifyJobKafkaPartitionOffsetsWithoutCustomPartitions() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_kafka_partition_no_custom";
        String jobName = "test_kafka_partition_no_custom_job";
        long dbId = 40003L;
        long tableId = 40004L;
        long jobId = 30002L;

        // Create KafkaRoutineLoadJob without custom partitions (will use all partitions)
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation (master side) - modify partition offsets
        // Note: Without custom partitions, we need to mock checkCustomPartition to avoid actual kafka connection
        Map<String, String> kafkaProperties = new HashMap<>();
        kafkaProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0,1");
        kafkaProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1000,2000");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "KAFKA", kafkaProperties);
        
        // Analyze properties to populate kafkaPartitionOffsets
        try {
            CreateRoutineLoadStmt.analyzeKafkaPartitionProperty(
                    kafkaProperties.get(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY),
                    Maps.newHashMap(), dataSourceProperties.getKafkaPartitionOffsets());
            CreateRoutineLoadStmt.analyzeKafkaOffsetProperty(
                    kafkaProperties.get(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY),
                    dataSourceProperties.getKafkaPartitionOffsets());
        } catch (Exception e) {
            Assertions.fail("Failed to analyze kafka properties: " + e.getMessage());
        }

        // Mock getAllKafkaPartitions to avoid actual kafka connection
        new MockUp<com.starrocks.common.util.KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                    com.google.common.collect.ImmutableMap<String, String> properties,
                    com.starrocks.warehouse.cngroup.ComputeResource computeResource) throws StarRocksException {
                // Return partitions that include 0 and 1 for testing
                return Lists.newArrayList(0, 1, 2);
            }
        };

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM KAFKA (\"kafka_partitions\"=\"0,1\", \"kafka_offsets\"=\"1000,2000\")", 0);
        job.modifyJob(null, null, dataSourceProperties, originStatement);

        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        KafkaProgress progress = (KafkaProgress) job.getProgress();
        Map<Integer, Long> partitionOffsets = progress.getPartitionIdToOffset();
        Assertions.assertTrue(partitionOffsets.containsKey(0));
        Assertions.assertTrue(partitionOffsets.containsKey(1));
        Assertions.assertEquals(1000L, partitionOffsets.get(0).longValue());
        Assertions.assertEquals(2000L, partitionOffsets.get(1).longValue());

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        KafkaProgress followerProgress = (KafkaProgress) followerJob.getProgress();
        Map<Integer, Long> followerPartitionOffsets = followerProgress.getPartitionIdToOffset();
        Assertions.assertTrue(followerPartitionOffsets.containsKey(0));
        Assertions.assertTrue(followerPartitionOffsets.containsKey(1));
        Assertions.assertEquals(1000L, followerPartitionOffsets.get(0).longValue());
        Assertions.assertEquals(2000L, followerPartitionOffsets.get(1).longValue());
    }

    @Test
    public void testModifyJobKafkaPartitionOffsetsWithCustomPartitionsException() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_kafka_partition_custom_exception";
        String jobName = "test_kafka_partition_custom_exception_job";
        long dbId = 40005L;
        long tableId = 40006L;
        long jobId = 30003L;

        // Create KafkaRoutineLoadJob with custom partitions
        List<Integer> customPartitions = Lists.newArrayList(0, 1);
        RoutineLoadJob routineLoadJob = createKafkaRoutineLoadJobWithCustomPartitions(
                jobId, jobName, dbId, tableId, customPartitions);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation with invalid partition (not in custom partitions) - should throw exception
        Map<String, String> kafkaProperties = new HashMap<>();
        kafkaProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0,2"); // partition 2 is not in custom partitions
        kafkaProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1000,2000");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "KAFKA", kafkaProperties);
        
        // Analyze properties to populate kafkaPartitionOffsets
        try {
            CreateRoutineLoadStmt.analyzeKafkaPartitionProperty(
                    kafkaProperties.get(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY),
                    Maps.newHashMap(), dataSourceProperties.getKafkaPartitionOffsets());
            CreateRoutineLoadStmt.analyzeKafkaOffsetProperty(
                    kafkaProperties.get(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY),
                    dataSourceProperties.getKafkaPartitionOffsets());
        } catch (Exception e) {
            Assertions.fail("Failed to analyze kafka properties: " + e.getMessage());
        }

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM KAFKA (\"kafka_partitions\"=\"0,2\", \"kafka_offsets\"=\"1000,2000\")", 0);

        // 3. Expect DdlException - partition 2 is not in custom partitions
        Assertions.assertThrows(Exception.class, () -> {
            job.modifyJob(null, null, dataSourceProperties, originStatement);
        });

        // After the exception, verify that the job's partition assignment remains unchanged (partitions remain [0,1])
        RoutineLoadJob jobAfterException = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(jobAfterException);
        
        // The customPartitions field should still only contain 0 and 1
        List<Integer> customPartitionsAfter =
                (List<Integer>) Deencapsulation.getField(jobAfterException, "customKafkaPartitions");
        Assertions.assertEquals(2, customPartitionsAfter.size());
        Assertions.assertTrue(customPartitionsAfter.contains(0));
        Assertions.assertTrue(customPartitionsAfter.contains(1));
    }

    @Test
    public void testModifyJobKafkaFilePropertyWithFile() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_kafka_file";
        String jobName = "test_kafka_file_job";
        long dbId = 40007L;
        long tableId = 40008L;
        long jobId = 30004L;

        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Create a small file for testing
        String fileName = "test_file";
        long fileId = 50001L;
        createSmallFile(dbId, KafkaRoutineLoadJob.KAFKA_FILE_CATALOG, fileName, fileId);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation (master side) - modify file property
        Map<String, String> kafkaProperties = new HashMap<>();
        kafkaProperties.put("property.test.key", "FILE:" + fileName);
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "KAFKA", kafkaProperties);
        
        // Manually populate customKafkaProperties so hasAnalyzedProperties() returns true
        dataSourceProperties.getCustomKafkaProperties().put("test.key", "FILE:" + fileName);

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM KAFKA (\"property.test.key\"=\"FILE:" + fileName + "\")", 0);
        job.modifyJob(null, null, dataSourceProperties, originStatement);

        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        KafkaRoutineLoadJob kafkaJob = (KafkaRoutineLoadJob) job;
        Map<String, String> customProperties =
                (Map<String, String>) Deencapsulation.getField(kafkaJob, "customProperties");
        Assertions.assertTrue(customProperties.containsKey("test.key"));
        Assertions.assertTrue(customProperties.get("test.key").contains("FILE:"));

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        KafkaRoutineLoadJob followerKafkaJob = (KafkaRoutineLoadJob) followerJob;
        Map<String, String> followerCustomProperties =
                (Map<String, String>) Deencapsulation.getField(followerKafkaJob, "customProperties");
        Assertions.assertTrue(followerCustomProperties.containsKey("test.key"));
        Assertions.assertTrue(followerCustomProperties.get("test.key").contains("FILE:"));
    }

    @Test
    public void testModifyJobKafkaFilePropertyWithoutFile() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_kafka_file_no_file";
        String jobName = "test_kafka_file_no_file_job";
        long dbId = 40009L;
        long tableId = 40010L;
        long jobId = 30005L;

        // Create and add RoutineLoadJob
        RoutineLoadJob routineLoadJob = createRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation with non-existent file - should throw exception
        String nonExistentFileName = "non_existent_file";
        Map<String, String> kafkaProperties = new HashMap<>();
        kafkaProperties.put("property.test.key", "FILE:" + nonExistentFileName);
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "KAFKA", kafkaProperties);
        
        // Manually populate customKafkaProperties so hasAnalyzedProperties() returns true
        dataSourceProperties.getCustomKafkaProperties().put("test.key", "FILE:" + nonExistentFileName);

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM KAFKA (\"property.test.key\"=\"FILE:" + nonExistentFileName + "\")", 0);

        // 3. Expect DdlException - file does not exist
        Assertions.assertThrows(Exception.class, () -> {
            job.modifyJob(null, null, dataSourceProperties, originStatement);
        });

        // After the exception, verify that the job's file property remains unchanged
        // Check that routine load job's customProperties are unchanged (should not have the new file property)
        Map<String, String> customProperties =
                (Map<String, String>) Deencapsulation.getField(job, "customProperties");
        Assertions.assertFalse(customProperties.containsKey("test.key"),
                "Custom properties should remain unchanged when non-existent file is specified");
    }

    // ==================== Additional modifyJob tests for Pulsar initial positions ====================

    @Test
    public void testModifyJobPulsarInitialPositionsWithCustomPartitions() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_pulsar_position_custom";
        String jobName = "test_pulsar_position_custom_job";
        long dbId = 50001L;
        long tableId = 50002L;
        long jobId = 40001L;

        // Create PulsarRoutineLoadJob with custom partitions
        List<String> customPartitions = Lists.newArrayList("partition-0", "partition-1", "partition-2");
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJobWithCustomPartitions(
                jobId, jobName, dbId, tableId, customPartitions);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation (master side) - modify initial positions
        Map<String, String> pulsarProperties = new HashMap<>();
        pulsarProperties.put(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY, "partition-0,partition-1");
        pulsarProperties.put(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY, 
                PulsarRoutineLoadJob.POSITION_EARLIEST + "," + PulsarRoutineLoadJob.POSITION_LATEST);
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "PULSAR", pulsarProperties);
        
        // Analyze properties to populate pulsarPartitionInitialPositions
        try {
            List<String> pulsarPartitions = Lists.newArrayList();
            CreateRoutineLoadStmt.analyzePulsarPartitionProperty(
                    pulsarProperties.get(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY),
                    Maps.newHashMap(), pulsarPartitions, dataSourceProperties.getPulsarPartitionInitialPositions());
            CreateRoutineLoadStmt.analyzePulsarPositionProperty(
                    pulsarProperties.get(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY),
                    pulsarPartitions, dataSourceProperties.getPulsarPartitionInitialPositions());
        } catch (Exception e) {
            Assertions.fail("Failed to analyze pulsar properties: " + e.getMessage());
        }

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM PULSAR (\"pulsar_partitions\"=\"partition-0,partition-1\", \"pulsar_initial_positions\"=\"" +
                PulsarRoutineLoadJob.POSITION_EARLIEST_VAL + "," + PulsarRoutineLoadJob.POSITION_LATEST_VAL + "\")", 0);
        job.modifyJob(null, null, dataSourceProperties, originStatement);

        // 3. Verify master state - check that positions were modified
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        PulsarProgress progress = (PulsarProgress) job.getProgress();
        // Use Deencapsulation to access private field partitionToInitialPosition
        Map<String, Long> partitionPositions =
                (Map<String, Long>) Deencapsulation.getField(progress, "partitionToInitialPosition");
        Assertions.assertTrue(partitionPositions.containsKey("partition-0"));
        Assertions.assertTrue(partitionPositions.containsKey("partition-1"));
        Assertions.assertEquals(PulsarRoutineLoadJob.POSITION_EARLIEST_VAL, partitionPositions.get("partition-0").longValue());
        Assertions.assertEquals(PulsarRoutineLoadJob.POSITION_LATEST_VAL, partitionPositions.get("partition-1").longValue());

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        PulsarProgress followerProgress = (PulsarProgress) followerJob.getProgress();
        // Use Deencapsulation to access private field partitionToInitialPosition
        Map<String, Long> followerPartitionPositions =
                (Map<String, Long>) Deencapsulation.getField(followerProgress, "partitionToInitialPosition");
        Assertions.assertTrue(followerPartitionPositions.containsKey("partition-0"));
        Assertions.assertTrue(followerPartitionPositions.containsKey("partition-1"));
        Assertions.assertEquals(PulsarRoutineLoadJob.POSITION_EARLIEST_VAL,
                followerPartitionPositions.get("partition-0").longValue());
        Assertions.assertEquals(PulsarRoutineLoadJob.POSITION_LATEST_VAL,
                followerPartitionPositions.get("partition-1").longValue());
    }

    @Test
    public void testModifyJobPulsarInitialPositionsWithoutCustomPartitions() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_pulsar_position_no_custom";
        String jobName = "test_pulsar_position_no_custom_job";
        long dbId = 50003L;
        long tableId = 50004L;
        long jobId = 40002L;

        // Create PulsarRoutineLoadJob without custom partitions
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation with invalid partition (not in custom partitions) - should throw exception
        Map<String, String> pulsarProperties = new HashMap<>();
        pulsarProperties.put(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY, "partition-0");
        pulsarProperties.put(
                CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY, PulsarRoutineLoadJob.POSITION_EARLIEST);
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "PULSAR", pulsarProperties);
        
        // Analyze properties to populate pulsarPartitionInitialPositions
        // We need to populate pulsarPartitionInitialPositions so hasAnalyzedProperties() returns true
        List<String> pulsarPartitions = Lists.newArrayList();
        CreateRoutineLoadStmt.analyzePulsarPartitionProperty(
                pulsarProperties.get(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY),
                Maps.newHashMap(), pulsarPartitions, dataSourceProperties.getPulsarPartitionInitialPositions());
        CreateRoutineLoadStmt.analyzePulsarPositionProperty(
                pulsarProperties.get(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY),
                pulsarPartitions, dataSourceProperties.getPulsarPartitionInitialPositions());

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM PULSAR (\"pulsar_partitions\"=\"partition-0\", \"pulsar_initial_positions\"=\"" +
                PulsarRoutineLoadJob.POSITION_EARLIEST_VAL + "\")", 0);

        // 3. Expect DdlException - partition is not specified in the create statement
        Assertions.assertThrows(Exception.class, () -> {
            job.modifyJob(null, null, dataSourceProperties, originStatement);
        });

        // After the exception, verify that the job's partition assignment remains unchanged (partitions remain [0])
        RoutineLoadJob jobAfterException = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(jobAfterException);
        
        // The customPartitions field should still be empty
        List<String> customPartitionsAfter =
                (List<String>) Deencapsulation.getField(jobAfterException, "customPulsarPartitions");
        Assertions.assertEquals(0, customPartitionsAfter.size());
    }

    @Test
    public void testModifyJobPulsarInitialPositionsWithCustomPartitionsException() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_pulsar_position_custom_exception";
        String jobName = "test_pulsar_position_custom_exception_job";
        long dbId = 50005L;
        long tableId = 50006L;
        long jobId = 40003L;

        // Create PulsarRoutineLoadJob with custom partitions
        List<String> customPartitions = Lists.newArrayList("partition-0", "partition-1");
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJobWithCustomPartitions(
                jobId, jobName, dbId, tableId, customPartitions);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation with invalid partition (not in custom partitions) - should throw exception
        Map<String, String> pulsarProperties = new HashMap<>();
        pulsarProperties.put(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY, "partition-0,partition-2"); // partition-2 is not in custom partitions
        pulsarProperties.put(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY, 
                PulsarRoutineLoadJob.POSITION_EARLIEST + "," + PulsarRoutineLoadJob.POSITION_LATEST);
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "PULSAR", pulsarProperties);
        
        // Analyze properties to populate pulsarPartitionInitialPositions
        // We need to populate pulsarPartitionInitialPositions so hasAnalyzedProperties() returns true
        List<String> pulsarPartitions = Lists.newArrayList();
        CreateRoutineLoadStmt.analyzePulsarPartitionProperty(
                pulsarProperties.get(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY),
                Maps.newHashMap(), pulsarPartitions, dataSourceProperties.getPulsarPartitionInitialPositions());
        CreateRoutineLoadStmt.analyzePulsarPositionProperty(
                pulsarProperties.get(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY),
                pulsarPartitions, dataSourceProperties.getPulsarPartitionInitialPositions());

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM PULSAR (\"pulsar_partitions\"=\"partition-0,partition-2\", \"pulsar_initial_positions\"=\"" +
                PulsarRoutineLoadJob.POSITION_EARLIEST_VAL + "," + PulsarRoutineLoadJob.POSITION_LATEST_VAL + "\")", 0);

        // 3. Expect DdlException - partition-2 is not in custom partitions
        Assertions.assertThrows(Exception.class, () -> {
            job.modifyJob(null, null, dataSourceProperties, originStatement);
        });

        // After the exception, verify that the job's partition assignment remains unchanged (partitions remain [0,1])
        RoutineLoadJob jobAfterException = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(jobAfterException);
        
        // The customPartitions field should still only contain 0 and 1
        List<String> customPartitionsAfter =
                (List<String>) Deencapsulation.getField(jobAfterException, "customPulsarPartitions");
        Assertions.assertEquals(2, customPartitionsAfter.size());
        Assertions.assertTrue(customPartitionsAfter.contains("partition-0"));
        Assertions.assertTrue(customPartitionsAfter.contains("partition-1"));
    }

    @Test
    public void testModifyJobPulsarFilePropertyWithFile() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_pulsar_file";
        String jobName = "test_pulsar_file_job";
        long dbId = 50007L;
        long tableId = 50008L;
        long jobId = 40004L;

        // Create and add PulsarRoutineLoadJob
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Create a small file for testing
        String fileName = "test_pulsar_file";
        long fileId = 50002L;
        createSmallFile(dbId, PulsarRoutineLoadJob.PULSAR_FILE_CATALOG, fileName, fileId);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation (master side) - modify file property
        Map<String, String> pulsarProperties = new HashMap<>();
        pulsarProperties.put("property.test.key", "FILE:" + fileName);
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "PULSAR", pulsarProperties);
        
        // Manually populate customPulsarProperties so hasAnalyzedProperties() returns true
        dataSourceProperties.getCustomPulsarProperties().put("test.key", "FILE:" + fileName);

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM PULSAR (\"property.test.key\"=\"FILE:" + fileName + "\")", 0);
        job.modifyJob(null, null, dataSourceProperties, originStatement);

        // 3. Verify master state
        job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);
        PulsarRoutineLoadJob pulsarJob = (PulsarRoutineLoadJob) job;
        Map<String, String> customProperties =
                (Map<String, String>) Deencapsulation.getField(pulsarJob, "customProperties");
        Assertions.assertTrue(customProperties.containsKey("test.key"));
        Assertions.assertTrue(customProperties.get("test.key").contains("FILE:"));

        // 4. Test follower replay functionality
        RoutineLoadMgr followerRoutineLoadMgr = new RoutineLoadMgr();

        // First replay the create routine load job
        RoutineLoadJob replayCreateJob = (RoutineLoadJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2);
        followerRoutineLoadMgr.replayCreateRoutineLoadJob(replayCreateJob);

        // Then replay the modify job operation
        AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_ROUTINE_LOAD_JOB);

        // Execute follower replay
        followerRoutineLoadMgr.replayAlterRoutineLoadJob(log);

        // 5. Verify follower state is consistent with master
        RoutineLoadJob followerJob = followerRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(followerJob);
        PulsarRoutineLoadJob followerPulsarJob = (PulsarRoutineLoadJob) followerJob;
        Map<String, String> followerCustomProperties =
                (Map<String, String>) Deencapsulation.getField(followerPulsarJob, "customProperties");
        Assertions.assertTrue(followerCustomProperties.containsKey("test.key"));
        Assertions.assertTrue(followerCustomProperties.get("test.key").contains("FILE:"));
    }

    @Test
    public void testModifyJobPulsarFilePropertyWithoutFile() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db_pulsar_file_no_file";
        String jobName = "test_pulsar_file_no_file_job";
        long dbId = 50009L;
        long tableId = 50010L;
        long jobId = 40005L;

        // Create and add PulsarRoutineLoadJob
        RoutineLoadJob routineLoadJob = createPulsarRoutineLoadJob(jobId, jobName, dbId, tableId);
        masterRoutineLoadMgr.addRoutineLoadJob(routineLoadJob, dbName);

        // Get the job from manager
        RoutineLoadJob job = masterRoutineLoadMgr.getJob(jobId);
        Assertions.assertNotNull(job);

        // 2. Execute modifyJob operation with non-existent file - should throw exception
        String nonExistentFileName = "non_existent_pulsar_file";
        Map<String, String> pulsarProperties = new HashMap<>();
        pulsarProperties.put("property.test.key", "FILE:" + nonExistentFileName);
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties(
                "PULSAR", pulsarProperties);
        
        // Manually populate customPulsarProperties so hasAnalyzedProperties() returns true
        dataSourceProperties.getCustomPulsarProperties().put("test.key", "FILE:" + nonExistentFileName);

        OriginStatementInfo originStatement = new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR " + dbName + "." + jobName + 
                " FROM PULSAR (\"property.test.key\"=\"FILE:" + nonExistentFileName + "\")", 0);

        // 3. Expect DdlException - file does not exist
        Assertions.assertThrows(Exception.class, () -> {
            job.modifyJob(null, null, dataSourceProperties, originStatement);
        });

        // After the exception, verify that the job's file property remains unchanged
        // Check that routine load job's customProperties are unchanged (should not have the new file property)
        Map<String, String> customProperties =
                (Map<String, String>) Deencapsulation.getField(job, "customProperties");
        Assertions.assertFalse(customProperties.containsKey("test.key"),
                "Custom properties should remain unchanged when non-existent file is specified");
    }
}

