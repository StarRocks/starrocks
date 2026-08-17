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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/BrokerLoadJobTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo;
import com.starrocks.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.FailMsg;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.NextIdLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.task.LeaderTask;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.CommitRateExceededException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.transaction.TxnStateChangeCallback;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class BrokerLoadJobTest {

    @BeforeAll
    public static void start() {
        MetricRepo.init();
    }

    @Test
    public void testFromLoadStmt(@Injectable LoadStmt loadStmt,
                                 @Injectable LabelName labelName,
                                 @Injectable DataDescription dataDescription,
                                 @Mocked GlobalStateMgr globalStateMgr,
                                 @Injectable Database database) {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);

        String tableName = "table";
        String databaseName = "database";
        new Expectations() {
            {
                loadStmt.getLabel();
                minTimes = 0;
                result = labelName;

                labelName.getDbName();
                minTimes = 0;
                result = databaseName;

                loadStmt.getDataDescriptions();
                minTimes = 0;
                result = dataDescriptionList;
                dataDescription.getTableName();
                minTimes = 0;
                result = tableName;
            }
        };

        try {
            BulkLoadJob.fromLoadStmt(loadStmt, null);
            Assertions.fail();
        } catch (DdlException e) {
            System.out.println("could not find table named " + tableName);
        }

    }

    @Test
    public void testFromLoadStmt2(@Injectable LoadStmt loadStmt,
                                  @Injectable DataDescription dataDescription,
                                  @Injectable LabelName labelName,
                                  @Injectable Database database,
                                  @Injectable OlapTable olapTable,
                                  @Mocked GlobalStateMgr globalStateMgr) {

        String label = "label";
        long dbId = System.currentTimeMillis();
        String tableName = "table";
        String databaseName = "database";
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());

        new Expectations() {
            {
                loadStmt.getLabel();
                minTimes = 0;
                result = labelName;
                labelName.getDbName();
                minTimes = 0;
                result = databaseName;
                labelName.getLabelName();
                minTimes = 0;
                result = label;
                globalStateMgr.getLocalMetastore().getDb(databaseName);
                minTimes = 0;
                result = database;
                loadStmt.getDataDescriptions();
                minTimes = 0;
                result = dataDescriptionList;
                dataDescription.getTableName();
                minTimes = 0;
                result = tableName;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableName);
                minTimes = 0;
                result = olapTable;
                dataDescription.getPartitionNames();
                minTimes = 0;
                result = null;
                database.getId();
                minTimes = 0;
                result = dbId;
                loadStmt.getBrokerDesc();
                minTimes = 0;
                result = brokerDesc;
                loadStmt.getEtlJobType();
                minTimes = 0;
                result = EtlJobType.BROKER;
            }
        };

        try {
            BrokerLoadJob brokerLoadJob = (BrokerLoadJob) BulkLoadJob.fromLoadStmt(loadStmt, null);
            Assertions.assertEquals(Long.valueOf(dbId), Deencapsulation.getField(brokerLoadJob, "dbId"));
            Assertions.assertEquals(label, Deencapsulation.getField(brokerLoadJob, "label"));
            Assertions.assertEquals(JobState.PENDING, Deencapsulation.getField(brokerLoadJob, "state"));
            Assertions.assertEquals(EtlJobType.BROKER, Deencapsulation.getField(brokerLoadJob, "jobType"));
        } catch (DdlException e) {
            Assertions.fail(e.getMessage());
        }

    }

    @Test
    public void testAlterLoad(@Injectable LoadStmt loadStmt,
                              @Injectable AlterLoadStmt alterLoadStmt,
                              @Injectable DataDescription dataDescription,
                              @Injectable LabelName labelName,
                              @Injectable Database database,
                              @Injectable OlapTable olapTable,
                              @Mocked GlobalStateMgr globalStateMgr) {

        String label = "label";
        long dbId = System.currentTimeMillis();
        String tableName = "table";
        String databaseName = "database";
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
        Map<String, String> properties = new HashMap<>();
        properties.put(LoadStmt.PRIORITY, "HIGH");

        new Expectations() {
            {
                loadStmt.getLabel();
                minTimes = 0;
                result = labelName;
                labelName.getDbName();
                minTimes = 0;
                result = databaseName;
                labelName.getLabelName();
                minTimes = 0;
                result = label;
                globalStateMgr.getLocalMetastore().getDb(databaseName);
                minTimes = 0;
                result = database;
                loadStmt.getDataDescriptions();
                minTimes = 0;
                result = dataDescriptionList;
                dataDescription.getTableName();
                minTimes = 0;
                result = tableName;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableName);
                minTimes = 0;
                result = olapTable;
                dataDescription.getPartitionNames();
                minTimes = 0;
                result = null;
                database.getId();
                minTimes = 0;
                result = dbId;
                loadStmt.getBrokerDesc();
                minTimes = 0;
                result = brokerDesc;
                loadStmt.getEtlJobType();
                minTimes = 0;
                result = EtlJobType.BROKER;
                alterLoadStmt.getAnalyzedJobProperties();
                minTimes = 0;
                result = properties;
            }
        };

        try {
            BrokerLoadJob brokerLoadJob = (BrokerLoadJob) BulkLoadJob.fromLoadStmt(loadStmt, null);
            Assertions.assertEquals(Long.valueOf(dbId), Deencapsulation.getField(brokerLoadJob, "dbId"));
            Assertions.assertEquals(label, Deencapsulation.getField(brokerLoadJob, "label"));
            Assertions.assertEquals(JobState.PENDING, Deencapsulation.getField(brokerLoadJob, "state"));
            Assertions.assertEquals(EtlJobType.BROKER, Deencapsulation.getField(brokerLoadJob, "jobType"));
            brokerLoadJob.alterJob(alterLoadStmt);
        } catch (DdlException e) {
            Assertions.fail(e.getMessage());
        }

    }

    @Test
    public void testGetTableNames(@Injectable BrokerFileGroupAggInfo fileGroupAggInfo,
                                  @Injectable BrokerFileGroup brokerFileGroup,
                                  @Mocked GlobalStateMgr globalStateMgr,
                                  @Injectable Database database,
                                  @Injectable Table table) throws MetaNotFoundException {
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);
        String tableName = "table";
        new Expectations() {
            {
                fileGroupAggInfo.getAggKeyToFileGroups();
                minTimes = 0;
                result = aggKeyToFileGroups;
                fileGroupAggInfo.getAllTableIds();
                minTimes = 0;
                result = Sets.newHashSet(1L);
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = database;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), 1L);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = tableName;
            }
        };

        Assertions.assertEquals(1, brokerLoadJob.getTableNamesForShow().size());
        Assertions.assertEquals(true, brokerLoadJob.getTableNamesForShow().contains(tableName));
    }

    @Test
    public void testExecuteJob(@Mocked LeaderTaskExecutor leaderTaskExecutor) throws LoadException {
        new Expectations() {
            {
                leaderTaskExecutor.submit((LeaderTask) any);
                minTimes = 0;
                result = true;
            }
        };

        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        new MockUp<EditLog>() {
            @Mock
            public void logSaveNextId(long nextId, WALApplier walApplier) {
                walApplier.apply(new NextIdLog(nextId));
            }
        };

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.unprotectedExecuteJob();

        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assertions.assertEquals(1, idToTasks.size());
    }

    @Test
    public void testRetryJobAfterAborted(@Injectable TransactionState txnState,
                                         @Injectable boolean txnOperated,
                                         @Injectable String txnStatusChangeReason,
                                         @Mocked LeaderTaskExecutor leaderTaskExecutor,
                                         @Mocked GlobalTransactionMgr globalTransactionMgr) throws LoadException,
            StarRocksException {
        // BrokerLoadJob defers beginTxn from LoadJob.unprotectedExecute() into
        // createLoadingTask (which only runs once the broker pending task resolves
        // file statuses) so the pre-split hook can sync-await without deadlocking
        // the reshard daemon. unprotectedExecuteJob() alone — the retry path —
        // therefore no longer touches GlobalTransactionMgr.beginTransaction; this
        // test actively guards that contract with times = 0 (calling
        // beginTransaction on the retry-submit path would be a regression).
        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, Lists.newArrayList(), anyString, (TUniqueId) any,
                        (TransactionState.TxnCoordinator) any,
                        (TransactionState.LoadJobSourceType) any, anyLong, anyLong, (ComputeResource) any);
                times = 0;
                leaderTaskExecutor.submit((LeaderTask) any);
                minTimes = 0;
                result = true;
            }
        };

        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        new MockUp<EditLog>() {
            @Mock
            public void logSaveNextId(long nextId, WALApplier walApplier) {
                walApplier.apply(new NextIdLog(nextId));
            }

            @Mock
            public void logEndLoadJob(LoadJobFinalOperation loadJobFinalOperation) {

            }
        };

        new MockUp<LoadJob>() {
            @Mock
            public void unprotectUpdateLoadingStatus(TransactionState txnState) {

            }
        };

        // test when retry limit has reached
        BrokerLoadJob brokerLoadJob1 = new BrokerLoadJob();
        brokerLoadJob1.retryTime = 0;
        brokerLoadJob1.unprotectedExecuteJob();
        txnOperated = true;
        txnStatusChangeReason = "broker load job timeout";
        brokerLoadJob1.afterAborted(txnState, txnStatusChangeReason);
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob1, "idToTasks");
        Assertions.assertEquals(0, idToTasks.size());

        // test normal retry after timeout
        BrokerLoadJob brokerLoadJob2 = new BrokerLoadJob();
        brokerLoadJob2.retryTime = 1;
        brokerLoadJob2.unprotectedExecuteJob();
        txnOperated = true;
        txnStatusChangeReason = "broker load job timeout";
        ConnectContext context = new ConnectContext();
        context.setStartTime();
        brokerLoadJob2.setConnectContext(context);
        long createTimestamp = context.getStartTime() - 1;
        brokerLoadJob2.createTimestamp = createTimestamp;
        brokerLoadJob2.timeoutSecond = 0;
        brokerLoadJob2.failInfos = Lists.newArrayList(new TabletFailInfo(1L, 2L));
        brokerLoadJob2.afterAborted(txnState, txnStatusChangeReason);
        idToTasks = Deencapsulation.getField(brokerLoadJob2, "idToTasks");
        Assertions.assertEquals(1, idToTasks.size());
        Assertions.assertTrue(brokerLoadJob2.createTimestamp > createTimestamp);
        Assertions.assertEquals(brokerLoadJob2.createTimestamp, context.getStartTime());
        Assertions.assertTrue(brokerLoadJob2.failInfos.isEmpty());

        // test when txnOperated is false
        BrokerLoadJob brokerLoadJob3 = new BrokerLoadJob();
        brokerLoadJob3.retryTime = 1;
        brokerLoadJob3.unprotectedExecuteJob();
        txnOperated = false;
        txnStatusChangeReason = "broker load job timeout";
        brokerLoadJob3.afterAborted(txnState, txnStatusChangeReason);
        idToTasks = Deencapsulation.getField(brokerLoadJob3, "idToTasks");
        Assertions.assertEquals(1, idToTasks.size());

        // test when txn is finished
        BrokerLoadJob brokerLoadJob4 = new BrokerLoadJob();
        brokerLoadJob4.retryTime = 1;
        brokerLoadJob4.unprotectedExecuteJob();
        txnOperated = true;
        txnStatusChangeReason = "broker load job timeout";
        Deencapsulation.setField(brokerLoadJob4, "state", JobState.FINISHED);
        brokerLoadJob4.afterAborted(txnState, txnStatusChangeReason);
        idToTasks = Deencapsulation.getField(brokerLoadJob4, "idToTasks");
        Assertions.assertEquals(1, idToTasks.size());

        // test that timeout happens in loading task before the job timeout
        BrokerLoadJob brokerLoadJob5 = new BrokerLoadJob();
        new Expectations() {
            {
                brokerLoadJob5.isTimeout();
                result = false;
            }
        };
        brokerLoadJob5.retryTime = 1;
        brokerLoadJob5.unprotectedExecuteJob();
        txnOperated = true;
        txnStatusChangeReason = LoadErrorUtils.BACKEND_BRPC_TIMEOUT.keywords;
        brokerLoadJob5.afterAborted(txnState, txnStatusChangeReason);
        idToTasks = Deencapsulation.getField(brokerLoadJob5, "idToTasks");
        Assertions.assertEquals(1, idToTasks.size());

        // test parse error, should not retry
        BrokerLoadJob brokerLoadJob6 = new BrokerLoadJob();
        brokerLoadJob6.retryTime = 1;
        brokerLoadJob6.unprotectedExecuteJob();
        txnOperated = true;
        txnStatusChangeReason = "parse error, task failed";
        brokerLoadJob6.afterAborted(txnState, txnStatusChangeReason);
        Assertions.assertEquals(JobState.CANCELLED, brokerLoadJob6.getState());
        idToTasks = Deencapsulation.getField(brokerLoadJob6, "idToTasks");
        Assertions.assertEquals(0, idToTasks.size());
    }

    @Test
    public void testPendingTaskOnTaskFailedRetriesWhenRetryTimeRemains(
            @Injectable FailMsg failMsg, @Mocked LeaderTaskExecutor leaderTaskExecutor) throws LoadException {
        // BrokerLoad defaults retryTime to 3. A retryable failure of the
        // CURRENT broker pending task in the no-txn window drives
        // retryWithoutTransaction, which resubmits the pending task —
        // idToTasks therefore holds the freshly submitted pending task and
        // state stays PENDING. Sets up the precondition by calling
        // unprotectedExecuteJob() first so a real pending task with a known
        // signature is tracked in idToTasks.
        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        new MockUp<EditLog>() {
            @Mock
            public void logEndLoadJob(LoadJobFinalOperation loadJobFinalOperation) {
            }
        };
        new Expectations() {
            {
                leaderTaskExecutor.submit((LeaderTask) any);
                minTimes = 0;
                result = true;
            }
        };

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.unprotectedExecuteJob();
        Map<Long, LoadTask> idToTasksBefore = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assertions.assertEquals(1, idToTasksBefore.size(),
                "test precondition: a pending task must be submitted before the failure");
        long pendingTaskId = idToTasksBefore.keySet().iterator().next();

        failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, "load_run_fail");
        brokerLoadJob.onTaskFailed(pendingTaskId, failMsg, null);

        Map<Long, LoadTask> idToTasksAfter = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assertions.assertEquals(1, idToTasksAfter.size(),
                "retry must resubmit a fresh BrokerLoadPendingTask");
        Assertions.assertEquals(JobState.PENDING, brokerLoadJob.getState());
    }

    @Test
    public void testPendingTaskOnTaskFailedIgnoresStaleCallback(
            @Injectable FailMsg failMsg, @Mocked LeaderTaskExecutor leaderTaskExecutor) throws LoadException {
        // A failure callback whose taskId is NOT in idToTasks (left over from a
        // prior cancelled attempt, or simply unknown) must not trigger a retry.
        // The guard rejects it before the resubmit path runs.
        new Expectations() {
            {
                leaderTaskExecutor.submit((LeaderTask) any);
                minTimes = 0;
                result = true;
            }
        };
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.unprotectedExecuteJob();
        Map<Long, LoadTask> idToTasksBefore = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        long pendingTaskId = idToTasksBefore.keySet().iterator().next();
        long staleTaskId = pendingTaskId + 1L;  // an id not in idToTasks

        failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, "load_run_fail");
        brokerLoadJob.onTaskFailed(staleTaskId, failMsg, null);

        Map<Long, LoadTask> idToTasksAfter = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assertions.assertEquals(1, idToTasksAfter.size(),
                "stale callback must not affect idToTasks");
        Assertions.assertEquals(pendingTaskId, idToTasksAfter.keySet().iterator().next(),
                "the original pending task must still be tracked");
        Assertions.assertEquals(JobState.PENDING, brokerLoadJob.getState());
    }

    @Test
    public void testPendingTaskOnTaskFailedSkipsRetryWhenJobAlreadyDone(
            @Injectable FailMsg failMsg, @Mocked LeaderTaskExecutor leaderTaskExecutor) throws LoadException {
        // Guard (a) inside retryWithoutTransaction: if the job already reached a
        // terminal state (CANCELLED / FINISHED / COMMITTED / UNKNOWN) between
        // onTaskFailed releasing its writeLock and retryWithoutTransaction taking
        // it, isTxnDone must short-circuit the retry. We exercise this by
        // pre-flipping state to CANCELLED via Deencapsulation and asserting
        // no retry submit happens (the outer onTaskFailed pre-check fires
        // on the same condition — both must agree the state stays terminal).
        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        new MockUp<EditLog>() {
            @Mock
            public void logSaveNextId(long nextId, WALApplier walApplier) {
            }
        };
        new Expectations() {
            {
                leaderTaskExecutor.submit((LeaderTask) any);
                minTimes = 0;
                result = true;
            }
        };
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.unprotectedExecuteJob();
        Map<Long, LoadTask> idToTasksBefore = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        long pendingTaskId = idToTasksBefore.keySet().iterator().next();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.CANCELLED);

        failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, "load_run_fail");
        brokerLoadJob.onTaskFailed(pendingTaskId, failMsg, null);

        Assertions.assertEquals(JobState.CANCELLED, brokerLoadJob.getState(),
                "terminal state must not be reverted by the retry path");
    }

    @Test
    public void testPendingTaskOnTaskFailedSkipsRetryWhenTaskAlreadyFinished(
            @Injectable FailMsg failMsg, @Mocked LeaderTaskExecutor leaderTaskExecutor) throws LoadException {
        // Guard (b) inside retryWithoutTransaction: a duplicate failure
        // callback for a pending task that already succeeded (taskId in
        // finishedTaskIds) must not trigger a retry. Seed finishedTaskIds
        // with the current pending task's id and assert no resubmit happens.
        new Expectations() {
            {
                leaderTaskExecutor.submit((LeaderTask) any);
                minTimes = 0;
                result = true;
            }
        };
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.unprotectedExecuteJob();
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        long pendingTaskId = idToTasks.keySet().iterator().next();
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        finishedTaskIds.add(pendingTaskId);

        failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, "load_run_fail");
        brokerLoadJob.onTaskFailed(pendingTaskId, failMsg, null);

        Assertions.assertEquals(1, idToTasks.size(),
                "duplicate-failure callback must not resubmit a fresh pending task");
        Assertions.assertEquals(pendingTaskId, idToTasks.keySet().iterator().next(),
                "the original pending task must still be tracked");
        Assertions.assertEquals(JobState.PENDING, brokerLoadJob.getState());
    }

    @Test
    public void testPendingTaskOnTaskFailedSkipsRetryWhenTransactionConcurrentlyBegun(
            @Mocked LeaderTaskExecutor leaderTaskExecutor) throws LoadException {
        // Guard (d) inside retryWithoutTransaction: if beginTxn ran in the
        // window between releasing the outer onTaskFailed write lock and
        // acquiring this one, the normal abort path now handles failures —
        // no-txn retry must skip rather than double-drive the retry.
        //
        // Exercise the guard precisely: keep transactionId == 0 going into
        // onTaskFailed (so the OUTER hasBegunTransaction check at the top of
        // BulkLoadJob.onTaskFailed dispatches to retryWithoutTransaction),
        // then MockUp LoadJob.writeLock to flip transactionId non-zero
        // exactly when retryWithoutTransaction acquires the lock — i.e. the
        // race window. The guard inside the locked region must observe the
        // begun transaction and return without resubmitting.
        // Exactly one submit is expected: the initial pending-task submit from
        // unprotectedExecuteJob below. If guard 4 fails to fire, the retry path
        // would resubmit a second pending task, breaking this expectation.
        new Expectations() {
            {
                leaderTaskExecutor.submit((LeaderTask) any);
                times = 1;
                result = true;
            }
        };
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.unprotectedExecuteJob();
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        long pendingTaskId = idToTasks.keySet().iterator().next();

        // The outer onTaskFailed in BulkLoadJob acquires writeLock first; that
        // call must not flip the field (transactionId stays 0 so the OUTER
        // hasBegunTransaction check routes to retryWithoutTransaction). The
        // SECOND writeLock acquisition (inside retryWithoutTransaction) is the
        // race-window: flip transactionId there so the guard fires.
        AtomicInteger writeLockCalls = new AtomicInteger(0);
        new MockUp<LoadJob>() {
            @Mock
            public void writeLock(Invocation invocation) {
                if (writeLockCalls.incrementAndGet() == 2) {
                    Deencapsulation.setField(brokerLoadJob, "transactionId", 9876L);
                }
                invocation.proceed();
            }
        };

        FailMsg failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, "load_run_fail");
        brokerLoadJob.onTaskFailed(pendingTaskId, failMsg, null);

        Assertions.assertEquals(2, writeLockCalls.get(),
                "guard 4 lives inside retryWithoutTransaction's writeLock — the test must "
                        + "drive both the outer onTaskFailed lock and the inner retryWithoutTransaction lock");
        Assertions.assertEquals(9876L, (long) Deencapsulation.getField(brokerLoadJob, "transactionId"),
                "MockUp must have flipped transactionId on the second writeLock — otherwise guard 4 "
                        + "would observe transactionId == 0 and incorrectly proceed with the no-txn retry");
        Assertions.assertTrue(brokerLoadJob.hasBegunTransaction(),
                "with transactionId == 9876L, hasBegunTransaction() must return true so guard 4 fires");
        Assertions.assertEquals(1, idToTasks.size(),
                "no-txn retry must NOT fire when transaction was concurrently begun");
        Assertions.assertEquals(pendingTaskId, idToTasks.keySet().iterator().next());
        Assertions.assertEquals(JobState.PENDING, brokerLoadJob.getState(),
                "guard 4 returns without changing state — abort flow handles it from here");
    }

    @Test
    public void testBeginTransactionSkipsBeginTxnWhenStateNoLongerPending(
            @Mocked GlobalTransactionMgr globalTransactionMgr) throws Exception {
        // Race-guard inside beginTransaction(): if state moved out of PENDING
        // while the caller was running the pre-split hook (e.g. concurrent
        // processTimeout / user cancel set state to CANCELLED), the helper
        // must NOT open a transaction — opening one at that point would leak
        // it because the GTM callback was already deregistered. Verify by
        // pre-flipping state to CANCELLED via Deencapsulation; assert
        // beginTransaction returns false AND GTM.beginTransaction was never
        // called.
        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, (List<Long>) any, anyString, (TUniqueId) any,
                        (TransactionState.TxnCoordinator) any,
                        (TransactionState.LoadJobSourceType) any, anyLong, anyLong, (ComputeResource) any);
                times = 0;
            }
        };
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.CANCELLED);

        boolean opened = Deencapsulation.invoke(brokerLoadJob, "beginTransaction");
        Assertions.assertFalse(opened, "non-PENDING state must short-circuit beginTransaction");
        Assertions.assertEquals(JobState.CANCELLED, brokerLoadJob.getState(),
                "the helper must not flip state when it short-circuits");
        Assertions.assertFalse(brokerLoadJob.hasBegunTransaction(),
                "the helper must not allocate T_load when it short-circuits");
    }

    @Test
    public void testBeginTransactionCancelsJobWhenBeginTxnThrows(
            @Mocked GlobalTransactionMgr globalTransactionMgr) throws Exception {
        // Failure path: beginTxn delegates to GTM.beginTransaction. When the
        // GTM rejects the load (label collision / quota / etc.) the helper
        // must cancel the job via unprotectedExecuteCancel and return false.
        // Verify by stubbing GTM to throw and asserting state moves to
        // CANCELLED.
        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        new MockUp<EditLog>() {
            @Mock
            public void logEndLoadJob(LoadJobFinalOperation loadJobFinalOperation, WALApplier walApplier) {
                walApplier.apply(loadJobFinalOperation);
            }
        };
        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, (List<Long>) any, anyString, (TUniqueId) any,
                        (TransactionState.TxnCoordinator) any,
                        (TransactionState.LoadJobSourceType) any, anyLong, anyLong, (ComputeResource) any);
                result = new LabelAlreadyUsedException("synthetic label collision");
            }
        };
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        // State stays PENDING — the helper reaches beginTxn and the throw routes to the catch.
        boolean opened = Deencapsulation.invoke(brokerLoadJob, "beginTransaction");
        Assertions.assertFalse(opened, "GTM-rejected beginTxn must return false");
        Assertions.assertEquals(JobState.CANCELLED, brokerLoadJob.getState(),
                "GTM-rejected beginTxn must cancel the job via unprotectedExecuteCancel");
    }

    @Test
    public void testEnsureConnectContextRebuildsFromPersistedSessionVarsOnFailover(
            @Mocked ConnectContext mockContext) throws Exception {
        // FE-failover happy path: context is null and sessionVariables carries the
        // persisted qualifiedUser + userIdentity. ensureConnectContext must rebuild a
        // ConnectContext wired with db / user / identity / roleIds AND the load's
        // persisted warehouse + compute resource. The warehouse restore is the Codex
        // P2 fix: the multi-partition pre-split path's submitForPartitionsCombined →
        // addPartitions reads ctx.getCurrentComputeResource(), so a non-default
        // warehouse must survive the rebuild or partitions get pre-created on the
        // default warehouse. @Mocked ConnectContext lets us verify the setters
        // without standing up a WarehouseMgr (setCurrentWarehouseId resolves a real
        // warehouse otherwise).
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "context", null);
        Deencapsulation.setField(brokerLoadJob, "warehouseId", 9999L);
        ComputeResource persistedResource = Mockito.mock(ComputeResource.class);
        Deencapsulation.setField(brokerLoadJob, "computeResource", persistedResource);
        Map<String, String> sessionVariables = Deencapsulation.getField(brokerLoadJob, "sessionVariables");
        sessionVariables.put(BulkLoadJob.CURRENT_QUALIFIED_USER_KEY, "test_user");
        sessionVariables.put(BulkLoadJob.CURRENT_USER_IDENT_KEY, "'test_user'@'%'");
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getFullName()).thenReturn("test_db");

        Deencapsulation.invoke(brokerLoadJob, "ensureConnectContext", db);

        Assertions.assertNotNull(Deencapsulation.getField(brokerLoadJob, "context"),
                "ensureConnectContext must instantiate a new ConnectContext");
        new Verifications() {
            {
                mockContext.setQualifiedUser("test_user");
                times = 1;
                // Codex P2: the load's persisted warehouse + compute resource are
                // restored so the pre-split hook acts on the load's warehouse.
                mockContext.setCurrentWarehouseId(9999L);
                times = 1;
                mockContext.setCurrentComputeResource(persistedResource);
                times = 1;
            }
        };
    }

    @Test
    public void testEnsureConnectContextThrowsWhenUserMissing() throws Exception {
        // ensureConnectContext rebuilds the ConnectContext from persisted
        // sessionVariables on FE-failover. When CURRENT_QUALIFIED_USER_KEY is
        // missing, it must throw DdlException rather than silently producing a
        // context with no auth. The error branch returns before touching the
        // Database argument's accessors — a bare unstubbed mock is sufficient.
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Map<String, String> sessionVariables = Deencapsulation.getField(brokerLoadJob, "sessionVariables");
        sessionVariables.remove(BulkLoadJob.CURRENT_QUALIFIED_USER_KEY);

        DdlException thrown = Assertions.assertThrows(DdlException.class,
                () -> Deencapsulation.invoke(brokerLoadJob, "ensureConnectContext",
                        Mockito.mock(Database.class)));
        Assertions.assertTrue(thrown.getMessage().contains("user is null"),
                "ensureConnectContext must error explicitly when user is missing");
    }

    @Test
    public void testPendingTaskOnTaskFailedCancelsWhenRetryExhausted(
            @Injectable long taskId, @Injectable FailMsg failMsg) {
        // retryTime == 0 makes isRetryable false → non-retryable → cancel.
        // No tasks remain after the cancel-and-clear path. This branch fires
        // before the no-txn staleness check, so the test doesn't need to set
        // up a tracked pending task.
        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        new MockUp<EditLog>() {
            @Mock
            public void logEndLoadJob(LoadJobFinalOperation loadJobFinalOperation, WALApplier walApplier) {
                walApplier.apply(loadJobFinalOperation);
            }
        };

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.retryTime = 0;
        failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, "load_run_fail");
        brokerLoadJob.onTaskFailed(taskId, failMsg, null);

        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assertions.assertEquals(0, idToTasks.size());
        Assertions.assertEquals(JobState.CANCELLED, brokerLoadJob.getState());
    }

    @Test
    public void testTaskFailedUserCancelType(@Injectable long taskId, @Injectable FailMsg failMsg) {
        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        new MockUp<EditLog>() {
            @Mock
            public void logEndLoadJob(LoadJobFinalOperation loadJobFinalOperation, WALApplier walApplier) {
                walApplier.apply(loadJobFinalOperation);
            }
        };

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        failMsg = new FailMsg(FailMsg.CancelType.USER_CANCEL, "Failed to allocate resource to query: pending timeout");
        brokerLoadJob.onTaskFailed(taskId, failMsg, null);

        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assertions.assertEquals(0, idToTasks.size());
    }

    @Test
    public void testTaskAbortTransactionOnTimeoutFailure(@Mocked GlobalTransactionMgr globalTransactionMgr,
            @Injectable long taskId, @Injectable FailMsg failMsg) throws StarRocksException {
        // BulkLoadJob.onTaskFailed now guards abortTransaction with transactionId != 0
        // because BrokerLoadJob defers beginTxn until after the pre-split hook
        // returns. With a non-zero transactionId set, the abort path is exercised
        // as before.
        List<TabletFailInfo> failInfos = Lists.newArrayList(new TabletFailInfo(1L, 2L));
        new Expectations() {
            {
                globalTransactionMgr.abortTransaction(anyLong, anyLong, anyString, failInfos);
                times = 1;
            }
        };

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "transactionId", 9001L);
        failMsg = new FailMsg(FailMsg.CancelType.UNKNOWN, "[E1008]Reached timeout=7200000ms @127.0.0.1:8060");
        brokerLoadJob.onTaskFailed(taskId, failMsg, new BrokerLoadingTaskAttachment(brokerLoadJob.getId(), failInfos));

        new Expectations() {
            {
                globalTransactionMgr.abortTransaction(anyLong, anyLong, anyString, Lists.newArrayList());
                times = 1;
                result = new StarRocksException("Artificial exception");
            }
        };

        try {
            BrokerLoadJob brokerLoadJob1 = new BrokerLoadJob();
            Deencapsulation.setField(brokerLoadJob1, "transactionId", 9002L);
            failMsg = new FailMsg(FailMsg.CancelType.UNKNOWN, "[E1008]Reached timeout=7200000ms @127.0.0.1:8060");
            brokerLoadJob1.onTaskFailed(taskId, failMsg, null);
        } catch (Exception e) {
            Assertions.fail("should not throw exception");
        }
    }

    @Test
    public void testTaskAbortTransactionSkippedWhenTxnNotBegun(@Mocked GlobalTransactionMgr globalTransactionMgr,
            @Injectable long taskId, @Injectable FailMsg failMsg) throws StarRocksException {
        // After the BrokerLoadJob lifecycle reorder, the broker pending task can
        // fail BEFORE BrokerLoadJob begins its load transaction (the pre-split hook
        // runs between the two). BulkLoadJob.onTaskFailed must NOT call
        // abortTransaction with transactionId == 0 — there is no GTM record to abort
        // and the no-op-with-exception that GTM used to throw is just noise.
        List<TabletFailInfo> failInfos = Lists.newArrayList(new TabletFailInfo(1L, 2L));
        new Expectations() {
            {
                globalTransactionMgr.abortTransaction(anyLong, anyLong, anyString, (List<TabletFailInfo>) any);
                times = 0;
            }
        };

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        // transactionId is 0 by default — the load was cancelled before beginTxn.
        FailMsg msg = new FailMsg(FailMsg.CancelType.UNKNOWN, "broker pending task failed before transaction was begun");
        Assertions.assertDoesNotThrow(() -> brokerLoadJob.onTaskFailed(
                taskId, msg, new BrokerLoadingTaskAttachment(brokerLoadJob.getId(), failInfos)));
    }

    @Test
    public void testPendingTaskOnFinishedWithJobCancelled(@Injectable BrokerPendingTaskAttachment attachment) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.CANCELLED);
        brokerLoadJob.onTaskFinished(attachment);

        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assertions.assertEquals(0, finishedTaskIds.size());
    }

    @Test
    public void testPendingTaskOnFinishedWithDuplicated(@Injectable BrokerPendingTaskAttachment attachment) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Set<Long> finishedTaskIds = Sets.newHashSet();
        long taskId = 1L;
        finishedTaskIds.add(taskId);
        Deencapsulation.setField(brokerLoadJob, "finishedTaskIds", finishedTaskIds);
        new Expectations() {
            {
                attachment.getTaskId();
                minTimes = 0;
                result = taskId;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assertions.assertEquals(0, idToTasks.size());
    }

    @Test
    public void testLoadingTaskOnFinishedWithUnfinishedTask(@Injectable BrokerLoadingTaskAttachment attachment,
                                                            @Injectable LoadTask loadTask1,
                                                            @Injectable LoadTask loadTask2) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 1;
                attachment.getTaskId();
                minTimes = 0;
                result = 1L;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assertions.assertEquals(1, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assertions.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assertions.assertEquals("1", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assertions.assertEquals(50, progress);
    }

    @Test
    public void testLoadingTaskOnFinishedWithErrorNum(@Injectable BrokerLoadingTaskAttachment attachment1,
                                                      @Injectable BrokerLoadingTaskAttachment attachment2,
                                                      @Injectable LoadTask loadTask1,
                                                      @Injectable LoadTask loadTask2,
                                                      @Mocked EditLog editLog,
                                                      @Mocked GlobalStateMgr globalStateMgr) {
        new FakeEditLog();
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment2.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 20;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 1;
                attachment2.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 2;
                attachment1.getTaskId();
                minTimes = 0;
                result = 1L;
                attachment2.getTaskId();
                minTimes = 0;
                result = 2L;
            }
        };

        brokerLoadJob.onTaskFinished(attachment1);
        brokerLoadJob.onTaskFinished(attachment2);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assertions.assertEquals(2, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assertions.assertEquals("30", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assertions.assertEquals("3", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assertions.assertEquals(99, progress);
        Assertions.assertEquals(JobState.CANCELLED, Deencapsulation.getField(brokerLoadJob, "state"));
    }

    @Test
    public void testLoadingTaskOnFinished(@Injectable BrokerLoadingTaskAttachment attachment1,
                                          @Injectable LoadTask loadTask1,
                                          @Mocked GlobalStateMgr globalStateMgr,
                                          @Injectable Database database) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 0;
                attachment1.getTaskId();
                minTimes = 0;
                result = 1L;
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = database;
            }
        };

        brokerLoadJob.onTaskFinished(attachment1);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assertions.assertEquals(1, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assertions.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assertions.assertEquals("0", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assertions.assertEquals(99, progress);
    }

    @Test
    public void testLoadingTaskOnFinishedPartialUpdate(@Injectable BrokerPendingTaskAttachment attachment1,
                                          @Injectable LoadTask loadTask1,
                                          @Mocked GlobalStateMgr globalStateMgr,
                                          @Injectable Database database) throws DdlException {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Map<String, String> properties = Maps.newHashMap();
        properties.put(LoadStmt.PARTIAL_UPDATE_MODE, "column");
        properties.put(LoadStmt.MERGE_CONDITION, "v1");
        brokerLoadJob.setJobProperties(properties);
        brokerLoadJob.onTaskFinished(attachment1);
    }

    @Test
    public void testExecuteReplayOnAborted(@Injectable TransactionState txnState,
                                           @Injectable LoadJobFinalOperation attachment,
                                           @Injectable EtlStatus etlStatus) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                attachment.getLoadingStatus();
                minTimes = 0;
                result = etlStatus;
                attachment.getProgress();
                minTimes = 0;
                result = 99;
                attachment.getFinishTimestamp();
                minTimes = 0;
                result = 1;
                attachment.getJobState();
                minTimes = 0;
                result = JobState.CANCELLED;
            }
        };
        brokerLoadJob.replayTxnAttachment(txnState);
        Assertions.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assertions.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assertions.assertEquals(JobState.CANCELLED, brokerLoadJob.getState());
    }

    @Test
    public void testReplayOnAbortedAfterFailure(@Injectable TransactionState txnState,
                                                @Injectable LoadJobFinalOperation attachment,
                                                @Injectable FailMsg failMsg) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.setId(1);
        GlobalTransactionMgr globalTxnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        globalTxnMgr.getCallbackFactory().addCallback(brokerLoadJob);

        // 1. The job will be keep when the failure is timeout
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                txnState.getReason();
                minTimes = 0;
                result = "load timeout";
            }
        };

        brokerLoadJob.replayOnAborted(txnState);
        TxnStateChangeCallback callback = globalTxnMgr.getCallbackFactory().getCallback(1);
        Assertions.assertNotNull(callback);

        // 2. The job will be discard when parse error
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                txnState.getReason();
                minTimes = 0;
                result = "parse error";
            }
        };
        brokerLoadJob.replayOnAborted(txnState);
        callback = globalTxnMgr.getCallbackFactory().getCallback(1);
        Assertions.assertNull(callback);
    }

    @Test
    public void testExecuteReplayOnVisible(@Injectable TransactionState txnState,
                                           @Injectable LoadJobFinalOperation attachment,
                                           @Injectable EtlStatus etlStatus) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                attachment.getLoadingStatus();
                minTimes = 0;
                result = etlStatus;
                attachment.getProgress();
                minTimes = 0;
                result = 99;
                attachment.getFinishTimestamp();
                minTimes = 0;
                result = 1;
                attachment.getJobState();
                minTimes = 0;
                result = JobState.LOADING;
            }
        };
        brokerLoadJob.replayTxnAttachment(txnState);
        Assertions.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assertions.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assertions.assertEquals(JobState.LOADING, brokerLoadJob.getState());
    }

    @Test
    public void testCommitRateExceeded(@Injectable BrokerLoadingTaskAttachment attachment1,
                                       @Injectable LoadTask loadTask1,
                                       @Mocked GlobalStateMgr globalStateMgr,
                                       @Injectable Database database,
                                       @Mocked GlobalTransactionMgr transactionMgr) throws StarRocksException {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 0;
                attachment1.getTaskId();
                minTimes = 0;
                result = 1L;
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = database;
                globalStateMgr.getCurrentState().getGlobalTransactionMgr();
                result = transactionMgr;
                transactionMgr.commitTransaction(anyLong, anyLong, (List<TabletCommitInfo>) any,
                        (List<TabletFailInfo>) any, (TxnCommitAttachment) any);
                result = new CommitRateExceededException(100, System.currentTimeMillis() + 10);
                result = null;
            }
        };

        brokerLoadJob.onTaskFinished(attachment1);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assertions.assertEquals(1, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assertions.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assertions.assertEquals("0", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assertions.assertEquals(99, progress);
    }

    @Test
    public void testSetProperties(@Injectable BrokerPendingTaskAttachment attachment1,
                                                       @Injectable LoadTask loadTask1,
                                                       @Mocked GlobalStateMgr globalStateMgr,
                                  @Injectable Database database) throws AnalysisException, DdlException {

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Map<String, String> properties = Maps.newHashMap();
        properties.put(LoadStmt.JSONPATHS, "[\"$.key2\"");
        properties.put(LoadStmt.STRIP_OUTER_ARRAY, "true");
        properties.put(LoadStmt.JSONROOT, "$.key1");
        brokerLoadJob.setJobProperties(properties);

        LoadJob.JSONOptions options = Deencapsulation.getField(brokerLoadJob, "jsonOptions");

        Assertions.assertEquals("[\"$.key2\"", options.jsonPaths);
        Assertions.assertTrue(options.stripOuterArray);
        Assertions.assertEquals("$.key1", options.jsonRoot);
    }

    @Test
    public void testBuildLoadingTasksThrowsWhenTargetTableDroppedDuringAwait(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked Locker locker,
            @Injectable Database db, @Injectable OlapTable snapshotTable,
            @Injectable BrokerDesc brokerDesc) {
        // M1: the OlapTable captured in snapshotPerTableInputsUnderReadLock can go
        // stale across the up-to-300s pre-split await. buildLoadingTasksUnderReadLock
        // re-resolves the table by id under the fresh DB READ lock; if it was dropped
        // (getTable -> null) the load must fail cleanly with MetaNotFoundException
        // rather than plan against the stale reference.
        new Expectations() {
            {
                db.getId();
                result = 100L;
                minTimes = 0;
                snapshotTable.getId();
                result = 2001L;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(anyLong, anyLong);
                result = null;
            }
        };
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        List<BrokerLoadJob.PreSplitHookInput> perTableInputs = List.of(
                new BrokerLoadJob.PreSplitHookInput(snapshotTable, List.of(), List.of()));

        Throwable thrown = Assertions.assertThrows(Throwable.class, () ->
                Deencapsulation.invoke(brokerLoadJob, "buildLoadingTasksUnderReadLock", db, perTableInputs, brokerDesc));
        assertStaleTableMetaNotFound(thrown, 2001L);
    }

    @Test
    public void testBuildLoadingTasksThrowsWhenTargetTableReplacedDuringAwait(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked Locker locker,
            @Injectable Database db, @Injectable OlapTable snapshotTable,
            @Injectable OlapTable replacementTable, @Injectable BrokerDesc brokerDesc) {
        // M1 (replace case): a DROP + CREATE round-trip during the await yields a
        // different OlapTable instance under the same id. The identity comparison
        // (currentTable != snapshotTable) must reject it, not silently plan against
        // the new table.
        new Expectations() {
            {
                db.getId();
                result = 100L;
                minTimes = 0;
                snapshotTable.getId();
                result = 2001L;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(anyLong, anyLong);
                result = replacementTable;
            }
        };
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        List<BrokerLoadJob.PreSplitHookInput> perTableInputs = List.of(
                new BrokerLoadJob.PreSplitHookInput(snapshotTable, List.of(), List.of()));

        Throwable thrown = Assertions.assertThrows(Throwable.class, () ->
                Deencapsulation.invoke(brokerLoadJob, "buildLoadingTasksUnderReadLock", db, perTableInputs, brokerDesc));
        assertStaleTableMetaNotFound(thrown, 2001L);
    }

    /** Walk the cause chain for the M1 stale-table MetaNotFoundException. */
    private static void assertStaleTableMetaNotFound(Throwable thrown, long tableId) {
        for (Throwable cursor = thrown; cursor != null; cursor = cursor.getCause()) {
            if (cursor instanceof MetaNotFoundException) {
                Assertions.assertTrue(cursor.getMessage().contains("was dropped or replaced"),
                        "expected stale-table message, got: " + cursor.getMessage());
                Assertions.assertTrue(cursor.getMessage().contains(String.valueOf(tableId)),
                        "message must name the table id, got: " + cursor.getMessage());
                return;
            }
        }
        Assertions.fail("expected MetaNotFoundException in the cause chain, got: " + thrown);
    }
}
