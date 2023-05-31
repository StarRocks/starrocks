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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/BrokerLoadJob.java

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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DataQualityException;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LoadPriority;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.FailMsg;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.persist.AlterLoadJobOperationLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * There are 3 steps in BrokerLoadJob: BrokerPendingTask, LoadLoadingTask, CommitAndPublishTxn.
 * Step1: BrokerPendingTask will be created on method of unprotectedExecuteJob.
 * Step2: LoadLoadingTasks will be created by the method of onTaskFinished when BrokerPendingTask is finished.
 * Step3: CommitAndPublicTxn will be called by the method of onTaskFinished when all of LoadLoadingTasks are finished.
 */
public class BrokerLoadJob extends BulkLoadJob {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadJob.class);
    private ConnectContext context;
    private List<LoadLoadingTask> newLoadingTasks = Lists.newArrayList();

    // only for log replay
    public BrokerLoadJob() {
        super();
        this.jobType = EtlJobType.BROKER;
    }

    // for ut
    public void setConnectContext(ConnectContext context) {
        this.context = context;
    }

    public BrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt, ConnectContext context)
            throws MetaNotFoundException {
        super(dbId, label, originStmt);
        this.timeoutSecond = Config.broker_load_default_timeout_second;
        this.brokerDesc = brokerDesc;
        this.jobType = EtlJobType.BROKER;
        this.context = context;
    }

    @Override
    public void beginTxn()
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException {
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);
        transactionId = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, Lists.newArrayList(fileGroupAggInfo.getAllTableIds()), label, null,
                        new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                        TransactionState.LoadJobSourceType.BATCH_LOAD_JOB, id,
                        timeoutSecond);
    }

    @Override
    public void alterJob(AlterLoadStmt stmt) throws DdlException {
        writeLock();

        try {
            if (stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY)) {
                priority = LoadPriority.priorityByName(stmt.getAnalyzedJobProperties().get(LoadStmt.PRIORITY));
                AlterLoadJobOperationLog log = new AlterLoadJobOperationLog(id,
                        stmt.getAnalyzedJobProperties());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterLoadJob(log);

                for (LoadTask loadTask : newLoadingTasks) {
                    GlobalStateMgr.getCurrentState().getLoadingLoadTaskScheduler().updatePriority(
                            loadTask.getSignature(),
                            priority);
                }
            }

        } finally {
            writeUnlock();
        }

    }

    @Override
    public void replayAlterJob(AlterLoadJobOperationLog log) {
        if (log.getJobProperties().containsKey(LoadStmt.PRIORITY)) {
            priority = LoadPriority.priorityByName(log.getJobProperties().get(LoadStmt.PRIORITY));
        }
    }

    @Override
    protected void unprotectedExecuteJob() throws LoadException {
        LoadTask task = new BrokerLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(), brokerDesc);
        idToTasks.put(task.getSignature(), task);
        submitTask(GlobalStateMgr.getCurrentState().getPendingLoadTaskScheduler(), task);
    }

    /**
     * Situation1: When attachment is instance of BrokerPendingTaskAttachment, this method is called by broker pending task.
     * LoadLoadingTask will be created after BrokerPendingTask is finished.
     * Situation2: When attachment is instance of BrokerLoadingTaskAttachment, this method is called by LoadLoadingTask.
     * CommitTxn will be called after all of LoadingTasks are finished.
     *
     * @param attachment
     */
    @Override
    public void onTaskFinished(TaskAttachment attachment) {
        if (attachment instanceof BrokerPendingTaskAttachment) {
            onPendingTaskFinished((BrokerPendingTaskAttachment) attachment);
        } else if (attachment instanceof BrokerLoadingTaskAttachment) {
            onLoadingTaskFinished((BrokerLoadingTaskAttachment) attachment);
        }
    }

    /**
     * step1: divide job into loading task
     * step2: init the plan of task
     * step3: submit tasks into loadingTaskExecutor
     *
     * @param attachment BrokerPendingTaskAttachment
     */
    private void onPendingTaskFinished(BrokerPendingTaskAttachment attachment) {
        writeLock();
        try {
            // check if job has been cancelled
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }

            if (finishedTaskIds.contains(attachment.getTaskId())) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("task_id", attachment.getTaskId())
                        .add("error_msg", "this is a duplicated callback of pending task "
                                + "when broker already has loading task")
                        .build());
                return;
            }

            // add task id into finishedTaskIds
            finishedTaskIds.add(attachment.getTaskId());
        } finally {
            writeUnlock();
        }

        try {
            Database db = getDb();
            createLoadingTask(db, attachment);
        } catch (Exception e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "Failed to divide job into loading task.")
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
            return;
        }
    }

    private void createLoadingTask(Database db, BrokerPendingTaskAttachment attachment) throws UserException {
        // divide job into broker loading task by table
        db.readLock();
        try {
            for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : fileGroupAggInfo.getAggKeyToFileGroups()
                    .entrySet()) {
                FileGroupAggKey aggKey = entry.getKey();
                List<BrokerFileGroup> brokerFileGroups = entry.getValue();
                long tableId = aggKey.getTableId();
                OlapTable table = (OlapTable) db.getTable(tableId);
                if (table == null) {
                    LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                            .add("database_id", dbId)
                            .add("table_id", tableId)
                            .add("error_msg", "Failed to divide job into loading task when table not found")
                            .build());
                    throw new MetaNotFoundException("Failed to divide job into loading task when table "
                            + tableId + " not found");
                }

                if (context == null) {
                    context = new ConnectContext();
                    context.setDatabase(db.getFullName());
                    if (sessionVariables.get(CURRENT_QUALIFIED_USER_KEY) != null) {
                        context.setQualifiedUser(sessionVariables.get(CURRENT_QUALIFIED_USER_KEY));
                        context.setCurrentUserIdentity(UserIdentity.fromString(sessionVariables.get(CURRENT_USER_IDENT_KEY)));
                        context.setCurrentRoleIds(UserIdentity.fromString(sessionVariables.get(CURRENT_USER_IDENT_KEY)));
                    } else {
                        throw new DdlException("Failed to divide job into loading task when user is null");
                    }
                }

                String mergeCondition = (brokerDesc == null) ? "" : brokerDesc.getMergeConditionStr();
                TPartialUpdateMode mode = TPartialUpdateMode.UNKNOWN_MODE;
                if (partialUpdateMode.equals("column")) {
                    mode = TPartialUpdateMode.COLUMN_MODE;
                } else if (partialUpdateMode.equals("auto")) {
                    mode = TPartialUpdateMode.AUTO_MODE;
                } else if (partialUpdateMode.equals("row")) {
                    mode = TPartialUpdateMode.ROW_MODE;
                }
                // Generate loading task and init the plan of task
                LoadLoadingTask task = new LoadLoadingTask(db, table, brokerDesc,
                        brokerFileGroups, getDeadlineMs(), loadMemLimit,
                        strictMode, transactionId, this, timezone, timeoutSecond,
                        createTimestamp, partialUpdate, mergeCondition, sessionVariables,
                        context,  TLoadJobType.BROKER, priority, originStmt, mode);
                UUID uuid = UUID.randomUUID();
                TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                task.init(loadId, attachment.getFileStatusByTable(aggKey), attachment.getFileNumByTable(aggKey));
                // update total loading task scan range num
                idToTasks.put(task.getSignature(), task);
                // idToTasks contains previous LoadPendingTasks, so idToTasks is just used to save all tasks.
                // use newLoadingTasks to save new created loading tasks and submit them later.
                newLoadingTasks.add(task);
                // load id will be added to loadStatistic when executing this task

                // save all related tables and rollups in transaction state
                TransactionState txnState =
                        GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionState(dbId, transactionId);
                if (txnState == null) {
                    throw new UserException("txn does not exist: " + transactionId);
                }
                txnState.addTableIndexes(table);
            }

        } finally {
            db.readUnlock();
        }

        // Submit task outside the database lock, cause it may take a while if task queue is full.
        for (LoadTask loadTask : newLoadingTasks) {
            submitTask(GlobalStateMgr.getCurrentState().getLoadingLoadTaskScheduler(), loadTask);
        }
    }

    private void onLoadingTaskFinished(BrokerLoadingTaskAttachment attachment) {
        writeLock();
        try {
            // check if job has been cancelled
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }

            // check if task has been finished
            if (finishedTaskIds.contains(attachment.getTaskId())) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("task_id", attachment.getTaskId())
                        .add("error_msg", "this is a duplicated callback of loading task").build());
                return;
            }

            // update loading status
            finishedTaskIds.add(attachment.getTaskId());
            updateLoadingStatus(attachment);

            // begin commit txn when all of loading tasks have been finished
            if (finishedTaskIds.size() != idToTasks.size()) {
                return;
            }
        } finally {
            writeUnlock();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("commit_infos", Joiner.on(",").join(commitInfos))
                    .build());
        }

        // check data quality
        if (!checkDataQuality()) {
            cancelJobWithoutCheck(
                    new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED, DataQualityException.QUALITY_FAIL_MSG),
                    true, true);
            return;
        }
        Database db = null;
        try {
            db = getDb();
        } catch (MetaNotFoundException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "db has been deleted when job is loading")
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true, true);
            return;
        }
        db.writeLock();
        try {
            LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("txn_id", transactionId)
                    .add("msg", "Load job try to commit txn")
                    .build());
            GlobalStateMgr.getCurrentGlobalTransactionMgr().commitTransaction(
                    dbId, transactionId, commitInfos, failInfos,
                    new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp,
                            finishTimestamp, state, failMsg));
            MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
            // collect table-level metrics
            loadingStatus.travelTableCounters(kv -> {
                TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(kv.getKey());
                if (kv.getValue().containsKey(TableMetricsEntity.TABLE_LOAD_BYTES)) {
                    entity.counterBrokerLoadBytesTotal
                            .increase(kv.getValue().get(TableMetricsEntity.TABLE_LOAD_BYTES));
                }
                if (kv.getValue().containsKey(TableMetricsEntity.TABLE_LOAD_ROWS)) {
                    entity.counterBrokerLoadRowsTotal
                            .increase(kv.getValue().get(TableMetricsEntity.TABLE_LOAD_ROWS));
                }
                if (kv.getValue().containsKey(TableMetricsEntity.TABLE_LOAD_FINISHED)) {
                    entity.counterBrokerLoadFinishedTotal
                            .increase(kv.getValue().get(TableMetricsEntity.TABLE_LOAD_FINISHED));
                }
            });
        } catch (UserException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("error_msg", "Failed to commit txn with error:" + e.getMessage())
                    .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true, true);
            return;
        } finally {
            db.writeUnlock();
        }
    }

    private void updateLoadingStatus(BrokerLoadingTaskAttachment attachment) {
        loadingStatus.replaceCounter(DPP_ABNORMAL_ALL,
                increaseCounter(DPP_ABNORMAL_ALL, attachment.getCounter(DPP_ABNORMAL_ALL)));
        loadingStatus.replaceCounter(DPP_NORMAL_ALL,
                increaseCounter(DPP_NORMAL_ALL, attachment.getCounter(DPP_NORMAL_ALL)));
        loadingStatus.replaceCounter(UNSELECTED_ROWS,
                increaseCounter(UNSELECTED_ROWS, attachment.getCounter(UNSELECTED_ROWS)));
        if (attachment.getTrackingUrl() != null) {
            loadingStatus.setTrackingUrl(attachment.getTrackingUrl());
        }
        if (!attachment.getRejectedRecordPaths().isEmpty()) {
            loadingStatus.setRejectedRecordPaths(attachment.getRejectedRecordPaths());
        }
        commitInfos.addAll(attachment.getCommitInfoList());
        failInfos.addAll(attachment.getFailInfoList());
        progress = (int) ((double) finishedTaskIds.size() / idToTasks.size() * 100);
        if (progress == 100) {
            loadingStatus.getLoadStatistic().setLoadFinish();
            progress = 99;
        }
        // collect table-level metrics
        LoadTask task = idToTasks.get(attachment.getTaskId());
        if (!(task instanceof LoadLoadingTask)) {
            return;
        }
        Table table = ((LoadLoadingTask) task).getTargetTable();
        if (null == table) {
            return;
        }
        long tableId = table.getId();
        if (attachment.getCounters().containsKey(DPP_NORMAL_ALL)) {
            loadingStatus.increaseTableCounter(tableId, TableMetricsEntity.TABLE_LOAD_ROWS,
                    Long.parseLong(attachment.getCounter(DPP_NORMAL_ALL)));
        }
        if (attachment.getCounters().containsKey(LOADED_BYTES)) {
            loadingStatus.increaseTableCounter(tableId, TableMetricsEntity.TABLE_LOAD_BYTES,
                    Long.parseLong(attachment.getCounter(LOADED_BYTES)));
        }
        loadingStatus.increaseTableCounter(tableId, TableMetricsEntity.TABLE_LOAD_FINISHED, 1L);
    }

    @Override
    public void updateProgress(TReportExecStatusParams params) {
        writeLock();
        try {
            super.updateProgress(params);
            if (!loadingStatus.getLoadStatistic().getLoadFinish()) {
                if (jobType == EtlJobType.BROKER) {
                    progress = (int) ((double) loadingStatus.getLoadStatistic().sourceScanBytes() /
                            loadingStatus.getLoadStatistic().totalFileSize() * 100);
                } else {
                    progress = (int) ((double) loadingStatus.getLoadStatistic().totalSourceLoadBytes() /
                            loadingStatus.getLoadStatistic().totalFileSize() * 100);
                }
                if (progress >= 100) {
                    progress = 99;
                }
            }
        } finally {
            writeUnlock();
        }
    }

    private String increaseCounter(String key, String deltaValue) {
        long value = 0;
        if (loadingStatus.getCounters().containsKey(key)) {
            value = Long.valueOf(loadingStatus.getCounters().get(key));
        }
        if (deltaValue != null) {
            value += Long.valueOf(deltaValue);
        }
        return String.valueOf(value);
    }
}
