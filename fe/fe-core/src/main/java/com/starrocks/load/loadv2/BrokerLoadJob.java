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
import com.starrocks.alter.reshard.presplit.BrokerLoadPreSplitHook;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DataQualityException;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LoadPriority;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.FailMsg;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.persist.AlterLoadJobOperationLog;
import com.starrocks.persist.BrokerPropertiesPersistInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.task.PriorityLeaderTask;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.CommitRateExceededException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.RunningTxnExceedException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.warehouse.WarehouseIdleChecker;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

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
    private long writeDurationMs = 0;
    private LoadStmt stmt;

    // only for log replay
    public BrokerLoadJob() {
        super();
        this.jobType = EtlJobType.BROKER;
    }

    // for ut
    public void setConnectContext(ConnectContext context) {
        this.context = context;
    }

    public BrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc, LoadStmt stmt, ConnectContext context)
            throws MetaNotFoundException {
        super(dbId, label, stmt != null ? stmt.getOrigStmt() : null);
        this.timeoutSecond = Config.broker_load_default_timeout_second;
        this.brokerPersistInfo =
                brokerDesc != null ? new BrokerPropertiesPersistInfo(brokerDesc.getName(), brokerDesc.getProperties()) : null;
        this.jobType = EtlJobType.BROKER;
        this.context = context;
        this.stmt = stmt;
        if (context != null) {
            this.warehouseId = context.getCurrentWarehouseId();
        }
    }

    @Override
    public void beginTxn()
            throws LabelAlreadyUsedException, RunningTxnExceedException, AnalysisException, DuplicatedRequestException {
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);
        transactionId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .beginTransaction(dbId, Lists.newArrayList(fileGroupAggInfo.getAllTableIds()), label, null,
                        new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                        TransactionState.LoadJobSourceType.BATCH_LOAD_JOB, id,
                        timeoutSecond, computeResource);
    }

    /**
     * Override the framework's {@link LoadJob#unprotectedExecute()} to defer both
     * {@link #beginTxn()} and the {@code PENDING → LOADING} state transition until
     * the pre-split hook returns inside {@link #createLoadingTask}.
     *
     * <p>The framework default — {@code beginTxn} then {@code unprotectedExecuteJob} —
     * would allocate {@code T_load} before the pre-split hook fires. The reshard
     * daemon's cleanup-phase {@code isPreviousTransactionsFinished(endTransactionId, ...)}
     * wait would then include {@code T_load}, deadlocking against any synchronous
     * pre-split await. Deferring {@code beginTxn} until after the hook keeps
     * {@code T_load > endTransactionId} by construction.
     *
     * <p>State stays {@code PENDING} during the pending-task + hook window so
     * {@link LoadJob#processTimeout} can still cancel a stuck load. A
     * {@code LOADING} state with {@code transactionId == 0} would otherwise be a
     * zombie: the GTM has no txn to time out, and {@code processTimeout} only acts
     * on {@code PENDING}.
     */
    @Override
    public void unprotectedExecute() throws LabelAlreadyUsedException, RunningTxnExceedException, AnalysisException,
            DuplicatedRequestException, LoadException {
        if (state != JobState.PENDING) {
            return;
        }
        unprotectedExecuteJob();
        // beginTxn() and unprotectedUpdateState(JobState.LOADING) intentionally
        // omitted — both move into createLoadingTask after the pre-split hook.
    }

    @Override
    public void alterJob(AlterLoadStmt stmt) throws DdlException {
        writeLock();

        try {
            if (stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY)) {
                int prio = LoadPriority.priorityByName(stmt.getAnalyzedJobProperties().get(LoadStmt.PRIORITY));
                AlterLoadJobOperationLog log = new AlterLoadJobOperationLog(id,
                        stmt.getAnalyzedJobProperties());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterLoadJob(log, wal -> setPriority(prio));

                for (LoadTask loadTask : newLoadingTasks) {
                    GlobalStateMgr.getCurrentState().getLoadingLoadTaskScheduler().updatePriority(
                            loadTask.getSignature(),
                            prio);
                }
            }

        } finally {
            writeUnlock();
        }

    }

    @Override
    public void replayAlterJob(AlterLoadJobOperationLog log) {
        if (log.getJobProperties().containsKey(LoadStmt.PRIORITY)) {
            setPriority(LoadPriority.priorityByName(log.getJobProperties().get(LoadStmt.PRIORITY)));
        }
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    protected void unprotectedExecuteJob() throws LoadException {
        LoadTask task = new BrokerLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(),
                brokerPersistInfo == null ? null :
                        new BrokerDesc(brokerPersistInfo.getName(), brokerPersistInfo.getProperties()));
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
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, e.getMessage()), true);
            return;
        }
    }

    /**
     * Orchestrate the deferred Broker Load lifecycle (see {@link #unprotectedExecute()}
     * for why each step is where it is). The flow reads top-to-bottom: snapshot
     * inputs, fire the pre-split hook (which sync-awaits the daemon), then open
     * the load transaction and build/submit the actual loading tasks against the
     * post-pre-split tablet layout.
     */
    private void createLoadingTask(Database db, BrokerPendingTaskAttachment attachment) throws StarRocksException {
        BrokerDesc brokerDesc = newBrokerDescFromPersistInfo();
        ensureConnectContext(db);
        List<PreSplitHookInput> perTableInputs = snapshotPerTableInputsUnderReadLock(db, attachment);

        // Fire the hook OUTSIDE the DB lock (sampling can take seconds).
        // See unprotectedExecute() for why T_load is deferred until after this returns.
        // shouldAbort = this::isTxnDone: if the job goes terminal during the wait
        // (processTimeout, user cancel), the hook releases the pending-task scheduler
        // slot promptly instead of holding it until the post-submit deadline expires.
        firePreSplitHooks(context, db, brokerDesc, computeResource, perTableInputs, sessionVariables,
                this::isTxnDone);

        if (!beginTransaction()) {
            return;
        }
        buildLoadingTasksUnderReadLock(db, perTableInputs, brokerDesc);
        // Submit outside the DB lock; submit() can block when the loading-task scheduler queue is full.
        for (LoadTask loadTask : newLoadingTasks) {
            submitTask(GlobalStateMgr.getCurrentState().getLoadingLoadTaskScheduler(), loadTask);
        }
    }

    private BrokerDesc newBrokerDescFromPersistInfo() {
        return brokerPersistInfo == null
                ? null
                : new BrokerDesc(brokerPersistInfo.getName(), brokerPersistInfo.getProperties());
    }

    /**
     * Rebuild the {@link ConnectContext} from persisted session vars when it was
     * lost across FE failover. Both the pre-split hook's {@code bindScope} and
     * {@link LoadLoadingTask} need a non-null context.
     */
    private void ensureConnectContext(Database db) throws DdlException {
        if (context != null) {
            return;
        }
        String qualifiedUser = sessionVariables.get(CURRENT_QUALIFIED_USER_KEY);
        if (qualifiedUser == null) {
            throw new DdlException("Failed to divide job into loading task when user is null");
        }
        UserIdentity userIdentity = UserIdentityUtils.fromString(sessionVariables.get(CURRENT_USER_IDENT_KEY));
        if (userIdentity == null) {
            throw new DdlException("Failed to divide job into loading task when user identity is missing or invalid");
        }
        context = new ConnectContext();
        context.setDatabase(db.getFullName());
        context.setQualifiedUser(qualifiedUser);
        context.setCurrentUserIdentity(userIdentity);
        context.setCurrentRoleIds(userIdentity);
        // Restore the load's warehouse + compute resource (both persisted) onto the
        // rebuilt context so the pre-split hook acts on the load's warehouse, not the
        // default. The multi-partition path's submitForPartitionsCombined →
        // LocalMetastore.addPartitions(ctx, ...) reads ctx.getCurrentComputeResource();
        // without this, an FE-failover-replayed partitioned load on a non-default
        // warehouse would pre-create its partitions on the default warehouse while the
        // load txn + tasks run on the persisted one. Warehouse id MUST precede the
        // compute resource — ConnectContext re-acquires the resource if the two disagree.
        context.setCurrentWarehouseId(warehouseId);
        context.setCurrentComputeResource(computeResource);
    }

    /**
     * Snapshot, under a single DB READ lock, the per-table inputs the pre-split
     * hook needs: table reference + broker file groups + resolved file statuses
     * from the pending task. Intentionally does NOT build {@link LoadLoadingTask}s
     * here — those must be built AFTER {@code beginTxn} so
     * {@code LoadPlanner.plan()} sees the post-pre-split tablet layout.
     */
    private List<PreSplitHookInput> snapshotPerTableInputsUnderReadLock(
            Database db, BrokerPendingTaskAttachment attachment) throws MetaNotFoundException {
        Map<FileGroupAggKey, List<BrokerFileGroup>> fileGroupsByAggregationKey =
                fileGroupAggInfo.getAggKeyToFileGroups();
        List<PreSplitHookInput> perTableInputs = new ArrayList<>(fileGroupsByAggregationKey.size());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : fileGroupsByAggregationKey.entrySet()) {
                FileGroupAggKey aggregationKey = entry.getKey();
                long tableId = aggregationKey.getTableId();
                OlapTable targetTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(db.getId(), tableId);
                if (targetTable == null) {
                    LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                            .add("database_id", dbId)
                            .add("table_id", tableId)
                            .add("error_msg", "Failed to divide job into loading task when table not found")
                            .build());
                    throw new MetaNotFoundException("Failed to divide job into loading task when table "
                            + tableId + " not found");
                }
                perTableInputs.add(new PreSplitHookInput(
                        targetTable, entry.getValue(), attachment.getFileStatusByTable(aggregationKey)));
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        return perTableInputs;
    }

    /**
     * Call {@link #beginTxn()} and transition {@code PENDING → LOADING}
     * atomically under the job write lock. Returns {@code false} when:
     * <ul>
     *   <li>the job is no longer {@code PENDING} (a concurrent timeout or user
     *       cancel reached a final state while we waited on the pre-split hook
     *       — beginning a txn at that point would leak it because the GTM
     *       callback was already deregistered);</li>
     *   <li>{@code beginTxn} rejects the load (label collision, quota, etc.)
     *       — the job is cancelled before returning.</li>
     * </ul>
     * Caller must stop processing when this returns {@code false}. See
     * {@link #unprotectedExecute()} for the deferred-{@code beginTxn} rationale.
     */
    private boolean beginTransaction() {
        writeLock();
        try {
            if (state != JobState.PENDING) {
                LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("msg", "broker load job is no longer PENDING after pre-split hook; "
                                + "skip beginTxn and transition")
                        .build());
                return false;
            }
            beginTxn();
            unprotectedUpdateState(JobState.LOADING);
            return true;
        } catch (LabelAlreadyUsedException | RunningTxnExceedException | AnalysisException
                | DuplicatedRequestException transactionFailure) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("error_msg", "Failed to begin broker-load transaction after pre-split hook: "
                            + transactionFailure.getMessage())
                    .build(), transactionFailure);
            // unprotectedExecuteCancel guards abortTransaction with hasBegunTransaction()
            // so the no-txn-yet case is handled cleanly.
            unprotectedExecuteCancel(
                    new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, transactionFailure.getMessage()),
                    true, true);
            return false;
        } finally {
            writeUnlock();
        }
    }

    private static TPartialUpdateMode resolvePartialUpdateThriftMode(String partialUpdateMode) {
        return switch (partialUpdateMode) {
            case "column" -> TPartialUpdateMode.COLUMN_UPSERT_MODE;
            case "auto" -> TPartialUpdateMode.AUTO_MODE;
            case "row" -> TPartialUpdateMode.ROW_MODE;
            default -> TPartialUpdateMode.UNKNOWN_MODE;
        };
    }

    /**
     * Build {@link LoadLoadingTask}s and call {@code task.prepare()} under a
     * fresh DB READ lock. {@code LoadPlanner.plan()} reads table metadata and
     * pins the sink plan to whatever tablet layout is currently visible — and
     * by the time we get here the pre-split hook has already returned, so the
     * layout is the post-pre-split one.
     *
     * <p>The {@code OlapTable} captured in {@link #snapshotPerTableInputsUnderReadLock}
     * is stale: up to {@code tablet_pre_split_post_submit_wait_seconds} (300s
     * default) elapses between the snapshot and this method while the pre-split
     * hook sync-awaits the reshard daemon. Re-resolve each table by id under
     * the freshly-acquired READ lock and identity-check against the snapshot —
     * if the table was dropped or replaced by DROP/CREATE under the same id,
     * fail the load cleanly rather than planning against the wrong object.
     */
    private void buildLoadingTasksUnderReadLock(
            Database db, List<PreSplitHookInput> perTableInputs, BrokerDesc brokerDesc) throws StarRocksException {
        TPartialUpdateMode partialUpdateThriftMode = resolvePartialUpdateThriftMode(partialUpdateMode);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            for (PreSplitHookInput input : perTableInputs) {
                OlapTable snapshotTable = input.targetTable();
                long tableId = snapshotTable.getId();
                OlapTable currentTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(db.getId(), tableId);
                if (currentTable != snapshotTable) {
                    throw new MetaNotFoundException("Target table " + tableId
                            + " was dropped or replaced while pre-split await was in flight");
                }
                TUniqueId loadId = UUIDUtil.genTUniqueId();
                int fileNum = input.fileStatuses().stream().mapToInt(List::size).sum();

                LoadLoadingTask task = new LoadLoadingTask.Builder()
                        .setDb(db)
                        .setTable(currentTable)
                        .setBrokerDesc(brokerDesc)
                        .setFileGroups(input.fileGroups())
                        .setJobDeadlineMs(getDeadlineMs())
                        .setExecMemLimit(loadMemLimit)
                        .setStrictMode(strictMode)
                        .setTxnId(transactionId)
                        .setCallback(this)
                        .setTimezone(timezone).setTimeoutS(timeoutSecond)
                        .setCreateTimestamp(createTimestamp)
                        .setPartialUpdate(partialUpdate)
                        .setMergeConditionStr(mergeCondition)
                        .setSessionVariables(sessionVariables)
                        .setContext(context)
                        .setLoadJobType(TLoadJobType.BROKER)
                        .setPriority(priority)
                        .setOriginStmt(originStmt)
                        .setLoadStmt(stmt)
                        .setPartialUpdateMode(partialUpdateThriftMode)
                        .setFileStatusList(input.fileStatuses())
                        .setFileNum(fileNum)
                        .setLoadId(loadId)
                        .setJSONOptions(jsonOptions)
                        .setComputeResource(computeResource)
                        .build();

                task.prepare();

                idToTasks.put(task.getSignature(), task);
                loadIds.add(DebugUtil.printId(loadId));
                newLoadingTasks.add(task);
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

    /**
     * Fire the Sample-Based Tablet Pre-Split hook for each per-table input
     * snapshot. The hook sync-awaits the reshard daemon's FINISHED transition
     * (single- and multi-partition paths) — see
     * {@link BrokerLoadPreSplitHook}. We bind the job's {@link ConnectContext}
     * so the coordinator's session-var check sees the load's session, not
     * whatever stale thread-local is set. We also apply the persisted opt-out
     * value so a submit-time {@code SET enable_tablet_pre_split = false}
     * survives FE failover (the recreated context otherwise has the default
     * value).
     *
     * <p>{@code shouldAbort} is threaded through each per-table hook so the
     * sync-await releases its scheduler slot promptly when the calling load
     * goes terminal. The outer-loop check exits before the NEXT table's hook
     * fires (the current table's hook already obeys {@code shouldAbort}
     * inside its await).
     *
     * <p>Package-private so {@code BrokerLoadJobPreSplitFiringTest} can drive
     * it directly without standing up the full {@code createLoadingTask}
     * fixture.
     */
    static void firePreSplitHooks(
            ConnectContext context, Database db, BrokerDesc brokerDesc, ComputeResource computeResource,
            List<PreSplitHookInput> preSplitInputs, Map<String, String> sessionVariables,
            BooleanSupplier shouldAbort) {
        if (preSplitInputs.isEmpty()) {
            return;
        }
        String persistedPreSplitOptOut = sessionVariables.get(SessionVariable.ENABLE_TABLET_PRE_SPLIT);
        if (persistedPreSplitOptOut != null) {
            context.getSessionVariable().setEnableTabletPreSplit(Boolean.parseBoolean(persistedPreSplitOptOut));
        }
        try (ConnectContext.ScopeGuard ignored = context.bindScope()) {
            for (PreSplitHookInput input : preSplitInputs) {
                if (shouldAbort.getAsBoolean()) {
                    return;
                }
                BrokerLoadPreSplitHook.maybeRunPreSplit(
                        context, db, input.targetTable(), brokerDesc,
                        input.fileGroups(), input.fileStatuses(), computeResource, shouldAbort);
            }
        }
    }

    /** Per-table inputs captured under the DB read lock so the pre-split hook can fire outside it. */
    record PreSplitHookInput(
            OlapTable targetTable, List<BrokerFileGroup> fileGroups,
            List<List<TBrokerFileStatus>> fileStatuses) {
    }

    @Override
    public void afterAborted(TransactionState txnState, String txnStatusChangeReason) {
        writeLock();
        try {
            // check if job has been completed
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                WarehouseIdleChecker.updateJobLastFinishTime(warehouseId,
                        "BrokerLoad: jobId[" + id + "] label[" + label + "]");
                return;
            }

            FailMsg failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, txnStatusChangeReason);
            boolean needRetry = isRetryable(failMsg);
            if (!needRetry) {
                // record attachment in load job
                unprotectUpdateLoadingStatus(txnState);
                // cancel load job
                unprotectedExecuteCancel(failMsg, true, false);
                return;
            }

            failMsg.setMsg(txnStatusChangeReason + ". Retry again");
            unprotectedRetryFromPending(failMsg,
                    String.format("broker load job %d with txn id %d failed",
                            id, txnState.getTransactionId()));
        } finally {
            writeUnlock();
        }
    }

    /**
     * Restart the load as a fresh PENDING attempt: log, decrement retryTime,
     * clear in-flight task state, flip state, and call
     * {@link #unprotectedExecute()} to resubmit. Caller must hold the job
     * write lock. Used by both {@link #afterAborted} (GTM-driven retry after
     * txn abort) and {@link #retryWithoutTransaction} (no-txn retry when the
     * pending task fails before {@code beginTxn}).
     */
    private void unprotectedRetryFromPending(FailMsg failMsg, String retryReason) {
        LOG.warn("{}; start retry, remaining retry time: {}", retryReason, retryTime);
        retryTime--;
        unprotectedClearTasksBeforeRetry(failMsg);
        try {
            state = JobState.PENDING;
            unprotectedExecute();
        } catch (Exception e) {
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL, e.getMessage()), true);
        }
    }

    @Override
    public void afterVisible(TransactionState txnState) {
        super.afterVisible(txnState);
        WarehouseIdleChecker.updateJobLastFinishTime(warehouseId, "BrokerLoad: jobId[" + id + "] label[" + label + "]");
    }

    /**
     * This method is used to replay the cancelled state of load job
     *
     * @param txnState
     */
    @Override
    public void replayOnAborted(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, txnState.getReason());
            finishTimestamp = txnState.getFinishTime();
            state = JobState.CANCELLED;
            if (!isRetryable(failMsg)) {
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
                return;
            }
            retryTime--;
        } finally {
            writeUnlock();
        }
    }

    @Override
    protected void reset() {
        super.reset();
        if (context != null) {
            context.setStartTime();
            createTimestamp = context.getStartTime();
        }
    }

    protected void unprotectedClearTasksBeforeRetry(FailMsg failMsg) {
        // get load ids of all loading tasks, we will cancel their coordinator process later
        List<TUniqueId> loadIds = Lists.newArrayList();
        for (PriorityLeaderTask loadTask : idToTasks.values()) {
            if (loadTask instanceof LoadLoadingTask) {
                loadIds.add(((LoadLoadingTask) loadTask).getLoadId());
            }
        }
        newLoadingTasks.clear();
        reset();

        this.failMsg = null; // when retry, user should not see previous fail msg
        this.progress = 0; // reset progress
        // Clear the previous attempt's transaction id so hasBegunTransaction()
        // reflects only the new attempt. The previous txn was already aborted by
        // the GTM (or never begun, when the pending task failed before beginTxn).
        // Leaving it set would make the new attempt's abort/cancel paths target
        // an already-aborted txn.
        this.transactionId = 0L;
        // cancel all running coordinators, so that the scheduler's worker thread will be released
        for (TUniqueId loadId : loadIds) {
            Coordinator coordinator = QeProcessorImpl.INSTANCE.getCoordinator(loadId);
            if (coordinator != null) {
                coordinator.cancel(failMsg.getMsg());
            }
        }
    }

    /**
     * Drive the retry path directly when the broker pending task fails BEFORE
     * {@link #beginTxn()} runs (during the pre-split hook window). No GTM
     * record exists to abort, so {@code afterAborted} cannot drive retry —
     * mirror its retry block here. Stale callbacks from a prior attempt's
     * tasks are ignored under the job write lock.
     */
    @Override
    protected void retryWithoutTransaction(long taskId, FailMsg failMsg) {
        writeLock();
        try {
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "broker load is already in a final state; skip retry")
                        .build());
                return;
            }
            // Skip duplicate failure callbacks for an already-succeeded pending task.
            if (finishedTaskIds.contains(taskId)) {
                LOG.debug("broker load job {}: duplicate failure callback for already-finished " +
                        "task {}; skip no-txn retry", id, taskId);
                return;
            }
            // Skip stale callbacks from a prior attempt — only the current attempt's
            // pending task should drive retry. LoadTask fires its onTaskFinished /
            // onTaskFailed callback exactly once per task instance, and the instanceof
            // check excludes LoadLoadingTask entries that share idToTasks once
            // beginTxn has run (those go through the normal abort path instead).
            LoadTask failedTask = idToTasks.get(taskId);
            if (!(failedTask instanceof BrokerLoadPendingTask)) {
                LOG.debug("broker load job {}: failure callback for task {} is not the current " +
                        "pending task (got {}); skip no-txn retry",
                        id, taskId, failedTask == null ? "null" : failedTask.getClass().getSimpleName());
                return;
            }
            // Recheck under the lock: if beginTxn ran concurrently, the normal abort
            // path handles failures from now on — don't double-drive retry.
            if (hasBegunTransaction()) {
                LOG.debug("broker load job {}: transaction was begun concurrently; " +
                        "skip no-txn retry path", id);
                return;
            }
            FailMsg retryMsg = new FailMsg(failMsg.getCancelType(),
                    failMsg.getMsg() + ". Retry again (pending task failed before transaction was begun)");
            unprotectedRetryFromPending(retryMsg,
                    String.format("broker load job %d pending-task failed before transaction was begun", id));
        } finally {
            writeUnlock();
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
                    new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED,
                            DataQualityException.QUALITY_FAIL_MSG +
                                    ". You can find detailed error message from running `TrackingSQL`."),
                    true);
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
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true);
            return;
        }
        while (true) {
            try {
                commitTransactionUnderDatabaseLock(db);
                break;
            } catch (CommitRateExceededException e) {
                // Sleep and retry.
                ThreadUtil.sleepAtLeastIgnoreInterrupts(Math.max(e.getAllowCommitTime() - System.currentTimeMillis(), 0));
            } catch (StarRocksException e) {
                cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true);
                break;
            }
        }
    }

    private void commitTransactionUnderDatabaseLock(Database db) throws StarRocksException {
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                .add("txn_id", transactionId)
                .add("msg", "Load job try to commit txn")
                .build());
        // Update the write duration before committing the transaction.
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        TransactionState transactionState = transactionMgr.getTransactionState(dbId, transactionId);
        if (transactionState != null) {
            transactionState.setWriteDurationMs(writeDurationMs);
        }

        List<Long> tableIdList = transactionState.getTableIdList();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), tableIdList, LockType.WRITE);
        try {
            transactionMgr.commitTransaction(dbId, transactionId, commitInfos, failInfos,
                    new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp, finishTimestamp, state,
                            failMsg));

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
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), tableIdList, LockType.WRITE);
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
        writeDurationMs += attachment.getWriteDurationMs();
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

    @Override
    protected void updateTabletFailInfos(TaskAttachment attachment) {
        if (attachment instanceof BrokerLoadingTaskAttachment) {
            failInfos.addAll(((BrokerLoadingTaskAttachment) attachment).getFailInfoList());
        }
    }
}
