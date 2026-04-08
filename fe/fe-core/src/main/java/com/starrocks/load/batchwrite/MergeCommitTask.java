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

package com.starrocks.load.batchwrite;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.Version;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.LoadErrorUtils;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TEtlState;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A task responsible for executing a load.
 */
public class MergeCommitTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitTask.class);

    // Initialized in constructor ==================================
    private final TableId tableId;
    private final String label;
    private final TUniqueId loadId;
    private final StreamLoadInfo streamLoadInfo;
    private final StreamLoadKvParams loadParameters;
    private final Set<Long> coordinatorBackendIds;
    private final int batchWriteIntervalMs;
    private final Coordinator.Factory coordinatorFactory;
    private final MergeCommitTaskCallback mergeCommitTaskCallback;
    private final TimeTrace timeTrace;
    private final AtomicReference<Throwable> failure;

    // Initialized in beginTxn() ==================================
    private long txnId = -1;

    // Initialized in executeLoad() ==================================
    ConnectContext context;
    LoadPlanner loadPlanner;
    private Coordinator coordinator;
    private List<TabletCommitInfo> tabletCommitInfo;
    private List<TabletFailInfo> tabletFailInfo;
    private LoadJobFinalOperation loadJobFinalOperation;
    private boolean collectProfileSuccess = false;

    public MergeCommitTask(
            TableId tableId,
            String label,
            TUniqueId loadId,
            StreamLoadInfo streamLoadInfo,
            int batchWriteIntervalMs,
            StreamLoadKvParams loadParameters,
            Set<Long> coordinatorBackendIds,
            Coordinator.Factory coordinatorFactory,
            MergeCommitTaskCallback mergeCommitTaskCallback) {
        this.tableId = tableId;
        this.label = label;
        this.loadId = loadId;
        this.streamLoadInfo = streamLoadInfo;
        this.batchWriteIntervalMs = batchWriteIntervalMs;
        this.loadParameters = loadParameters;
        this.coordinatorBackendIds = coordinatorBackendIds;
        this.coordinatorFactory = coordinatorFactory;
        this.mergeCommitTaskCallback = mergeCommitTaskCallback;
        this.timeTrace = new TimeTrace();
        this.failure = new AtomicReference<>();
    }

    @Override
    public void run() {
        try {
            beginTxn();
            executeLoad();
            commitAndPublishTxn();
        } catch (Exception e) {
            failure.set(e);
            abortTxn(e);
            LOG.error("Failed to execute load, label: {}, load id: {}, txn id: {}",
                    label, DebugUtil.printId(loadId), txnId, e);
        } finally {
            mergeCommitTaskCallback.finish(this);
            timeTrace.finishTimeMs = System.currentTimeMillis();
            MergeCommitMetricRegistry.getInstance().updateLoadLatency(timeTrace.totalCostMs());
            reportProfile();
            LOG.debug("Finish load, label: {}, load id: {}, txn_id: {}, {}",
                    label, DebugUtil.printId(loadId), txnId, timeTrace.summary());
        }
    }

<<<<<<< HEAD
=======
    /**
     * Resolves the target database.
     *
     * @return the resolved {@link Database}
     * @throws LoadException if the database does not exist
     */
    private Database getDatabase() throws LoadException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new LoadException(String.format("Database '%s' does not exist", tableRef.getDbName()));
        }
        return db;
    }

    /**
     * Resolves the target table from the given database.
     *
     * @param database database to resolve the table from
     * @return the resolved {@link OlapTable}
     * @throws LoadException if the table does not exist or is not an {@link OlapTable}
     */
    private OlapTable getOlapTable(Database database) throws LoadException {
        Table table = database.getTable(tableRef.getTableName());
        if (table == null) {
            throw new LoadException(String.format(
                    "Table '%s.%s' does not exist", tableRef.getDbName(), tableRef.getTableName()));
        }
        if (!(table instanceof OlapTable)) {
            throw new LoadException(String.format(
                    "Unsupported table type. table='%s.%s', type=%s",
                        tableRef.getDbName(), tableRef.getTableName(), table.getType()));
        }
        return (OlapTable) table;
    }

    /**
     * Creates an internal {@link ConnectContext} used to execute the load.
     */
    private ConnectContext createConnectContext(OlapTable table) {
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        context.setQualifiedUser(UserIdentity.ROOT.getUser());
        context.setCurrentComputeResource(streamLoadInfo.getComputeResource());

        if (table.enableLoadProfile()) {
            long sampleIntervalMs = Config.load_profile_collect_interval_second * 1000;
            if (sampleIntervalMs > 0 &&
                    System.currentTimeMillis() - table.getLastCollectProfileTime() > sampleIntervalMs) {
                context.getSessionVariable().setEnableProfile(true);
                table.updateLastCollectProfileTime();
            }
            context.getSessionVariable().setBigQueryProfileThreshold(
                    Config.stream_load_profile_collect_threshold_second + "s");
            // do not enable runtime profile report currently
            context.getSessionVariable().setRuntimeProfileReportInterval(-1);
        }
        return context;
    }

    /**
     * Builds the execution plan.
     *
     * @param database target database
     * @param table target OLAP table
     * @param connectContext execution context used by planner
     * @param timeoutMs max time to wait for the table lock and planning
     * @return a planned {@link LoadPlanner}
     * @throws Exception if table lock cannot be acquired, planning fails, or any unexpected error occurs
     */
    private LoadPlanner createLoadPlan(Database database, OlapTable table, ConnectContext connectContext, long timeoutMs)
            throws Exception {
        try (Timer ignored = Tracers.watchScope(Tracers.Module.BASE, "LoadPlanner")) {
            Locker locker = new Locker();
            boolean locked =
                    locker.tryLockTablesWithIntensiveDbLock(database.getId(), Lists.newArrayList(table.getId()),
                            LockType.READ,
                            timeoutMs, TimeUnit.MILLISECONDS);
            if (!locked) {
                throw new LockTimeoutException(
                        String.format("Timed out acquiring read lock. db=%s, table=%s, timeoutMs=%s",
                                tableRef.getDbName(), tableRef.getTableName(), timeoutMs));
            }
            try (var scope = connectContext.bindScope()) {
                LoadPlanner loadPlanner = new LoadPlanner(taskId, loadId, txnId, database.getId(),
                        tableRef.getDbName(), table, streamLoadInfo.isStrictMode(), streamLoadInfo.getTimezone(),
                        streamLoadInfo.isPartialUpdate(), connectContext, null,
                        streamLoadInfo.getLoadMemLimit(), streamLoadInfo.getExecMemLimit(),
                        streamLoadInfo.getNegative(), backendIds.size(), streamLoadInfo.getColumnExprDescs(),
                        streamLoadInfo, label, streamLoadInfo.getTimeout());
                loadPlanner.setBatchWrite(mergeCommitIntervalMs,
                        ImmutableMap.<String, String>builder()
                                .putAll(loadKvParams.toMap()).build(), backendIds);
                loadPlanner.plan();
                return loadPlanner;
            } finally {
                locker.unLockTablesWithIntensiveDbLock(database.getId(), Lists.newArrayList(table.getId()),
                        LockType.READ);
            }
        }
    }

    /**
     * Executes the load using the given {@link Coordinator}.
     *
     * @param connectContext execution context
     * @param coordinator execution coordinator
     * @param timeoutMs total time budget for the load stage (including waiting for execution)
     * @return load output required for commit
     * @throws Exception if execution times out, execution status is not OK, or other errors occur
     */
    private LoadResult executeLoad(ConnectContext connectContext, Coordinator coordinator, long timeoutMs)
            throws Exception {
        try {
            connectContext.setThreadLocalInfo();
            QeProcessorImpl.INSTANCE.registerQuery(loadId, coordinator);
            coordinator.exec();
            loadTimeTrace.execWaitStartTimeMs.set(System.currentTimeMillis());
            boolean timeout = !coordinator.join(toJoinTimeoutSeconds(timeoutMs));
            if (timeout) {
                throw new LoadException(String.format("Load execution timed out after %s ms", timeoutMs));
            }

            Status status = coordinator.getExecStatus();
            if (!status.ok()) {
                throw new LoadException(String.format("Load execution failed. errorCode=%s, message=%s",
                        status.getErrorCodeString(), status.getErrorMsg()));
            }

            List<TabletCommitInfo> tabletCommitInfos = TabletCommitInfo.fromThrift(coordinator.getCommitInfos());
            List<TabletFailInfo> tabletFailInfos = TabletFailInfo.fromThrift(coordinator.getFailInfos());
            LoadStats loadStats = getLoadDataStats(coordinator);
            return new LoadResult(tabletCommitInfos, tabletFailInfos, loadStats);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
            ConnectContext.remove();
        }
    }

    /**
     * Converts a millisecond timeout to the {@link Coordinator#join(int)} second granularity.
     *
     * <p>We round up to avoid losing almost one second due to integer division.
     */
    private static int toJoinTimeoutSeconds(long timeoutMs) {
        if (timeoutMs <= 0) {
            return 0;
        }
        long timeoutSeconds = (timeoutMs + 999) / 1000;
        return timeoutSeconds >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) timeoutSeconds;
    }

    /**
     * Parses load counters from coordinator and builds a {@link LoadStats}.
     */
    private LoadStats getLoadDataStats(Coordinator coordinator) {
        Map<String, String> loadCounters = coordinator.getLoadCounters();
        long normalRows = Long.parseLong(loadCounters.getOrDefault(LoadEtlTask.DPP_NORMAL_ALL, "0"));
        long normalBytes = Long.parseLong(loadCounters.getOrDefault(LoadJob.LOADED_BYTES, "0"));
        long abnormalRows = Long.parseLong(loadCounters.getOrDefault(LoadEtlTask.DPP_ABNORMAL_ALL, "0"));
        long unselectedRows = Long.parseLong(loadCounters.getOrDefault(LoadJob.UNSELECTED_ROWS, "0"));
        return new LoadStats(normalRows, normalBytes, abnormalRows, unselectedRows,
                coordinator.getTrackingUrl(), coordinator.getRejectedRecordPaths());
    }

    @VisibleForTesting
    boolean awaitPublish(VisibleStateWaiter waiter, long publishTimeoutMs) throws InterruptedException {
        return waiter.await(publishTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private void updateMetrics() {
        LoadStats loadStats = this.loadStats;
        if (loadStats != null) {
            MergeCommitMetricRegistry.getInstance().incLoadData(loadStats.normalRows, loadStats.normalBytes);
        }
        MergeCommitMetricRegistry.getInstance().updateLoadLatency(loadTimeTrace.totalCostMs());
        if (taskState == TaskState.FINISHED) {
            MergeCommitMetricRegistry.getInstance().incSuccessTask();
        } else {
            MergeCommitMetricRegistry.getInstance().incFailTask();
        }
        MergeCommitMetricRegistry.getInstance().updateRunningTask(-1L);
    }

    /**
     * Collects and pushes runtime profile if profile is enabled.
     *
     * @param connectContext execution context; if null, profiling is skipped
     * @param planner load planner; if null, profiling is skipped
     * @param coordinator execution coordinator; if null, profiling is skipped
     */
    private void collectProfile(ConnectContext connectContext, LoadPlanner planner, Coordinator coordinator) {
        if (connectContext == null || planner == null || coordinator == null) {
            return;
        }
        if (!connectContext.isProfileEnabled() && !LoadErrorUtils.enableProfileAfterError(coordinator)) {
            return;
        }

        AtomicLong collectProfileCostMs = new AtomicLong(0);
        RuntimeProfile executionProfile = null;
        try (ScopedTimer ignored = new ScopedTimer(collectProfileCostMs)) {
            coordinator.collectProfileSync();
            executionProfile = coordinator.buildQueryProfile(true);
        } catch (Exception e) {
            LOG.warn("Failed to collect profile. db={}, table={}, label={}, txnId={}, loadId={}",
                    tableRef.getDbName(), tableRef.getTableName(),
                    label, txnId, DebugUtil.printId(loadId), e);
        }

        try {
            RuntimeProfile profile = new RuntimeProfile("Load");
            RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
            java.time.ZoneId profileZone = TimeUtils.getTimeZone().toZoneId();
            summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(loadId));
            summaryProfile.addInfoString(ProfileManager.START_TIME,
                    TimeUtils.longToTimeStringWithTimeZone(loadTimeTrace.createTimeMs, profileZone));
            summaryProfile.addInfoString(ProfileManager.END_TIME,
                    TimeUtils.longToTimeStringWithTimeZone(loadTimeTrace.endTimeMs.get(), profileZone));
            summaryProfile.addInfoString(ProfileManager.TOTAL_TIME,
                    DebugUtil.getPrettyStringMs(loadTimeTrace.totalCostMs()));
            summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
            summaryProfile.addInfoString(ProfileManager.QUERY_STATE, taskState.name());
            summaryProfile.addInfoString("State Message", taskStateMessage);
            summaryProfile.addInfoString(ProfileManager.LOAD_TYPE, LOAD_TYPE_NAME);
            summaryProfile.addInfoString(ProfileKeyDictionary.STARROCKS_VERSION,
                    String.format("%s-%s", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH));
            summaryProfile.addInfoString("Default Db", tableRef.getDbName());
            summaryProfile.addInfoString("Table", tableRef.getTableName());
            summaryProfile.addInfoString("Sql Statement", loadKvParams.toString());
            summaryProfile.addInfoString(ProfileManager.WAREHOUSE_CNGROUP, warehouseName);
            summaryProfile.addInfoString(ProfileManager.PROFILE_COLLECT_TIME,
                    DebugUtil.getPrettyStringMs(collectProfileCostMs.get()));
            summaryProfile.addInfoString(ProfileKeyDictionary.IS_PROFILE_ASYNC, String.valueOf(true));
            summaryProfile.addInfoString("Pending Time",
                    DebugUtil.getPrettyStringMs(loadTimeTrace.pendingCostMs.get()));
            summaryProfile.addInfoString("Label", label);
            summaryProfile.addInfoString("Txn ID", String.valueOf(txnId));
            summaryProfile.addInfoString("Txn Commit Time",
                    DebugUtil.getPrettyStringMs(loadTimeTrace.commitCostMs.get()));
            summaryProfile.addInfoString("Txn Publish Time",
                    DebugUtil.getPrettyStringMs(loadTimeTrace.publishCostMs.get()));
            LoadStats loadStats = this.loadStats;
            if (loadStats != null) {
                summaryProfile.addInfoString("RowsNormal", String.valueOf(loadStats.normalRows));
                summaryProfile.addInfoString("BytesNormal", DebugUtil.getPrettyStringBytes(loadStats.normalBytes));
                summaryProfile.addInfoString("RowsAbnormal", String.valueOf(loadStats.abnormalRows));
                summaryProfile.addInfoString("RowsUnselected", String.valueOf(loadStats.unselectedRows));
            }
            profile.addChild(summaryProfile);
            RuntimeProfile plannerProfile = new RuntimeProfile("Planner");
            profile.addChild(plannerProfile);
            Tracers.toRuntimeProfile(plannerProfile);
            if (executionProfile != null) {
                profile.addChild(executionProfile);
            }
            ProfileManager.getInstance().pushProfile(planner.getExecPlan().getProfilingPlan(), profile);
        } catch (Exception e) {
            LOG.warn("Failed to build profile. db={}, table={}, label={}, txnId={}, loadId={}",
                    tableRef.getDbName(), tableRef.getTableName(),
                    label, txnId, DebugUtil.printId(loadId), e);
        }
    }

    /**
     * Cancels the task as best effort.
     *
     * @param reason cancellation reason
     */
    public void cancel(String reason) {
        try {
            transitionToState(TaskState.CANCELLED, reason, null, () -> {});
            LOG.info("Cancel task. db={}, table={}, taskId={}, label={}, loadId={}, txnId={}, reason={}",
                    tableRef.getDbName(), tableRef.getTableName(),
                    taskId, label, DebugUtil.printId(loadId), txnId, reason);
        } catch (Exception e) {
            LOG.warn("Failed to cancel task. db={}, table={}, taskId={}, label={}, loadId={}, txnId={}, reason={}",
                    tableRef.getDbName(), tableRef.getTableName(),
                    taskId, label, DebugUtil.printId(loadId), txnId, reason, e);
        }
    }

    /**
     * Transitions the internal state machine to {@code targetState}.
     *
     * <p>This method acquires {@link #lock} and updates {@link #taskState}, {@link #taskStateMessage} and
     * {@link #taskStateCancelHandler} atomically. The {@code afterStateChange} callback (if any) is executed
     * under the same lock, and should only update fields protected by {@link #lock}.
     *
     * <p>When transitioning to {@link TaskState#CANCELLED}, the previous {@link #taskStateCancelHandler} (if any)
     * is invoked <b>after</b> releasing the lock, to avoid holding the lock while cancelling external resources.
     */
    private void transitionToState(TaskState targetState, String targetStateMsg,
            TaskStateCancelHandler taskStateCancelHandler, Runnable afterStateChange) throws Exception {
        TaskState prevTaskState;
        TaskStateCancelHandler prevTaskStateCancelHandler;
        StateTransitionObserver observer = stateTransitionObserver;
        try (AutoCloseable ignored = CloseableLock.lock(lock)) {
            EnumSet<TaskState> transition = TASK_STATE_TRANSITION.getOrDefault(taskState, EnumSet.noneOf(TaskState.class));
            if (!transition.contains(targetState)) {
                throw new Exception(String.format("Cannot transition state from %s to %s", taskState, targetState));
            }
            prevTaskState = taskState;
            prevTaskStateCancelHandler = this.taskStateCancelHandler;
            taskState = targetState;
            taskStateMessage = targetStateMsg;
            this.taskStateCancelHandler = taskStateCancelHandler;
            if (afterStateChange != null) {
                afterStateChange.run();
            }
            LOG.debug("Transition state from {} to {}. db={}, table={}, taskId={}, label={}, loadId={}, txnId={}, stateMsg={}",
                    prevTaskState, targetState, tableRef.getDbName(), tableRef.getTableName(),
                    taskId, label, DebugUtil.printId(loadId), txnId, targetStateMsg);
        }
        if (targetState.isFinalState()) {
            loadTimeTrace.endTimeMs.set(System.currentTimeMillis());
        }
        if (observer != null && prevTaskState != null) {
            observer.onTransition(prevTaskState, targetState, targetStateMsg, taskStateCancelHandler != null);
        }
        if (targetState == TaskState.CANCELLED && prevTaskStateCancelHandler != null) {
            prevTaskStateCancelHandler.cancel(targetStateMsg);
        }
    }

    Pair<TaskState, String> getTaskState() {
        this.lock.lock();
        try {
            return new Pair<>(taskState, taskStateMessage);
        } finally {
            this.lock.unlock();
        }
    }

    @VisibleForTesting
    void setStateTransitionObserver(StateTransitionObserver observer) {
        this.stateTransitionObserver = observer;
    }

    /**
     * Checks if this execution is active and can accept new load requests.
     *
     * <p>A task is considered active if it is not in a final state and either has not started joining
     * execution yet, or its join start time is within the merge-commit interval window.
     *
     * @return {@code true} if the task is active, otherwise {@code false}
     */
    public boolean isActive() {
        if (taskState.isFinalState() || taskState == TaskState.COMMITTING || taskState == TaskState.COMMITTED) {
            return false;
        }
        long execWaitStartTimeMs = loadTimeTrace.execWaitStartTimeMs.get();
        return execWaitStartTimeMs <= 0 || (System.currentTimeMillis() - execWaitStartTimeMs < mergeCommitIntervalMs);
    }

    /**
     * Returns the backend IDs used by the coordinator.
     */
    public Set<Long> getBackendIds() {
        return Collections.unmodifiableSet(backendIds);
    }

    /**
     * Checks whether the given backend ID is contained in the execution backend set.
     */
    public boolean containsBackend(long backendId) {
        return backendIds.contains(backendId);
    }

    private static String getErrorTrackingSql(long taskId) {
        return "SELECT tracking_log FROM information_schema.load_tracking_logs WHERE JOB_ID=" + taskId;
    }

    // ================ inherited methods from AbstractTxnStateChangeCallback ================

    @Override
    public long getId() {
        return taskId;
    }

    @Override
    public void afterAborted(TransactionState txnState, String txnStatusChangeReason)
            throws StarRocksException {
        // This transaction abort must come from outside, because run() removes the callback before abort txn.
        cancel(txnStatusChangeReason);
    }

    // ================ inherited methods from AbstractStreamLoadTask ================

    @Override
    public String getDBName() {
        return  tableRef.getDbName();
    }

    @Override
    public long getDBId() {
        return dbId;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getTableName() {
        return tableRef.getTableName();
    }

    @Override
>>>>>>> fc5770df2e ([BugFix] Display profile START_TIME/END_TIME with session timezone (#71429))
    public String getLabel() {
        return label;
    }

    public long getTxnId() {
        return txnId;
    }

    public Set<Long> getBackendIds() {
        return Collections.unmodifiableSet(coordinatorBackendIds);
    }

    /**
     * Checks if the given backend id is contained in the coordinator backend IDs.
     */
    public boolean containCoordinatorBackend(long backendId) {
        return coordinatorBackendIds.contains(backendId);
    }

    /**
     * Checks if this batch is active and can accept new load requests.
     */
    public boolean isActive() {
        if (failure.get() != null) {
            return false;
        }
        long joinPlanTimeMs = timeTrace.joinPlanTimeMs.get();
        return joinPlanTimeMs <= 0 || (System.currentTimeMillis() - joinPlanTimeMs < batchWriteIntervalMs);
    }

    public Throwable getFailure() {
        return failure.get();
    }

    private void beginTxn() throws Exception {
        timeTrace.beginTxnTimeMs = System.currentTimeMillis();
        Pair<Database, OlapTable> pair = getDbAndTable();
        txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                pair.first.getId(), Lists.newArrayList(pair.second.getId()), label,
                TransactionState.TxnCoordinator.fromThisFE(),
                TransactionState.LoadJobSourceType.FRONTEND_STREAMING,
                streamLoadInfo.getTimeout(), streamLoadInfo.getWarehouseId());
    }

    private void commitAndPublishTxn() throws Exception {
        timeTrace.commitTxnTimeMs = System.currentTimeMillis();
        Database database = getDb();
        long publishTimeoutMs =
                streamLoadInfo.getTimeout() * 1000L - (timeTrace.commitTxnTimeMs - timeTrace.beginTxnTimeMs);
        boolean publishSuccess = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().commitAndPublishTransaction(
                database, txnId, tabletCommitInfo, tabletFailInfo, publishTimeoutMs, null);
        if (!publishSuccess) {
            LOG.warn("Publish timeout, txn_id: {}, label: {}, total timeout: {} ms, publish timeout: {} ms",
                        txnId, label, streamLoadInfo.getTimeout() * 1000, publishTimeoutMs);
        }
    }

    private void abortTxn(Throwable reason) {
        if (txnId == -1) {
            return;
        }
        try {
            Database database = getDb();
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                    database.getId(), txnId, reason == null ? "" : reason.getMessage());
        } catch (Exception e) {
            LOG.error("Failed to abort transaction {}", txnId, e);
        }
    }

    private void executeLoad() throws Exception {
        try {
            timeTrace.executeLoadTimeMs = System.currentTimeMillis();
            context = new ConnectContext();
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            context.setQualifiedUser(UserIdentity.ROOT.getUser());
            context.setThreadLocalInfo();

            Pair<Database, OlapTable> pair = getDbAndTable();
            // although merge commit uses pipeline engine, use table property to control the profile same as stream load
            if (pair.second.enableLoadProfile()) {
                long sampleIntervalMs = Config.load_profile_collect_interval_second * 1000;
                if (sampleIntervalMs > 0 &&
                        System.currentTimeMillis() - pair.second.getLastCollectProfileTime() > sampleIntervalMs) {
                    context.getSessionVariable().setEnableProfile(true);
                    pair.second.updateLastCollectProfileTime();
                }
                context.getSessionVariable().setBigQueryProfileThreshold(
                        Config.stream_load_profile_collect_threshold_second + "s");
                // do not enable runtime profile report currently
                context.getSessionVariable().setRuntimeProfileReportInterval(-1);
            }

            loadPlanner = new LoadPlanner(-1, loadId, txnId, pair.first.getId(),
                    tableId.getDbName(), pair.second, streamLoadInfo.isStrictMode(), streamLoadInfo.getTimezone(),
                    streamLoadInfo.isPartialUpdate(), context, null,
                    streamLoadInfo.getLoadMemLimit(), streamLoadInfo.getExecMemLimit(),
                    streamLoadInfo.getNegative(), coordinatorBackendIds.size(), streamLoadInfo.getColumnExprDescs(),
                    streamLoadInfo, label, streamLoadInfo.getTimeout());
            loadPlanner.setWarehouseId(streamLoadInfo.getWarehouseId());
            loadPlanner.setBatchWrite(batchWriteIntervalMs,
                    ImmutableMap.<String, String>builder()
                            .putAll(loadParameters.toMap()).build(), coordinatorBackendIds);
            loadPlanner.plan();
            timeTrace.deployPlanTimeMs = System.currentTimeMillis();
            coordinator = coordinatorFactory.createStreamLoadScheduler(loadPlanner);
            QeProcessorImpl.INSTANCE.registerQuery(loadId, coordinator);
            coordinator.exec();
            int waitSecond = streamLoadInfo.getTimeout() -
                    (int) (System.currentTimeMillis() - timeTrace.createTimeMs) / 1000;
            timeTrace.joinPlanTimeMs.set(System.currentTimeMillis());
            if (coordinator.join(waitSecond)) {
                Status status = coordinator.getExecStatus();
                if (!status.ok()) {
                    throw new LoadException(
                            String.format("Failed to execute load, status code: %s, error message: %s",
                                    status.getErrorCodeString(), status.getErrorMsg()));
                }
                tabletCommitInfo = TabletCommitInfo.fromThrift(coordinator.getCommitInfos());
                tabletFailInfo = TabletFailInfo.fromThrift(coordinator.getFailInfos());

                // TODO add more information such as progress, unfinished backends
                loadJobFinalOperation = new LoadJobFinalOperation();
                EtlStatus etlStatus = loadJobFinalOperation.getLoadingStatus();
                etlStatus.setState(TEtlState.FINISHED);
                etlStatus.setCounters(coordinator.getLoadCounters());
                if (coordinator.getTrackingUrl() != null) {
                    etlStatus.setTrackingUrl(coordinator.getTrackingUrl());
                }
                if (!coordinator.getRejectedRecordPaths().isEmpty()) {
                    etlStatus.setRejectedRecordPaths(coordinator.getRejectedRecordPaths());
                }
                long loadedRows = Long.parseLong(
                        etlStatus.getCounters().getOrDefault(LoadEtlTask.DPP_NORMAL_ALL, "0"));
                long loadBytes = Long.parseLong(
                        etlStatus.getCounters().getOrDefault(LoadJob.LOADED_BYTES, "0"));
                MergeCommitMetricRegistry.getInstance().incLoadData(loadedRows, loadBytes);
                long filteredRows = Long.parseLong(
                        etlStatus.getCounters().getOrDefault(LoadEtlTask.DPP_ABNORMAL_ALL, "0"));
                double maxFilterRatio = loadParameters.getMaxFilterRatio().orElse(0.0);
                if (isProfileEnabled()) {
                    try {
                        coordinator.collectProfileSync();
                        collectProfileSuccess = true;
                    } catch (Exception e) {
                        LOG.error("Failed to collect profile, label: {}, txn id: {}, load id: {}",
                                label, DebugUtil.printId(loadId), txnId, e);
                    }
                }
                if (filteredRows > (filteredRows + loadedRows) * maxFilterRatio) {
                    throw new LoadException(String.format("There is data quality issue, please check the " +
                                    "tracking url for details. Max filter ratio: %s. The tracking url: %s",
                                    maxFilterRatio, coordinator.getTrackingUrl()));
                }
            } else {
                throw new LoadException(
                        String.format("Timeout to execute load after waiting for %s seconds", waitSecond));
            }
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }
    }

    private Database getDb() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(tableId.getDbName());
        if (db == null) {
            throw new LoadException(String.format("Database %s does not exist", tableId.getDbName()));
        }

        return db;
    }

    private Pair<Database, OlapTable> getDbAndTable() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(tableId.getDbName());
        if (db == null) {
            throw new LoadException(String.format("Database %s does not exist", tableId.getDbName()));
        }

        Table table = db.getTable(tableId.getTableName());
        if (table == null) {
            throw new LoadException(String.format(
                    "Table [%s.%s] does not exist", tableId.getDbName(), tableId.getTableName()));
        }
        return Pair.create(db, (OlapTable) table);
    }

    private boolean isProfileEnabled() {
        return (context != null && context.isProfileEnabled()) || LoadErrorUtils.enableProfileAfterError(coordinator);
    }

    private void reportProfile() {
        if (!isProfileEnabled()) {
            return;
        }
        RuntimeProfile profile = new RuntimeProfile("Load");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(loadId));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(timeTrace.createTimeMs));
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(timeTrace.finishTimeMs));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(timeTrace.totalCostMs()));
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
        summaryProfile.addInfoString(ProfileManager.LOAD_TYPE, "MERGE_COMMIT");
        summaryProfile.addInfoString("StarRocks Version",
                String.format("%s-%s", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH));
        summaryProfile.addInfoString("Default Db", tableId.getDbName());
        summaryProfile.addInfoString("Sql Statement",
                String.format("merge commit, table: %s, label: %s, %s",
                        tableId.getTableName(), label, loadParameters.toString()));
        summaryProfile.addInfoString(ProfileManager.VARIABLES, "{}");
        summaryProfile.addInfoString("NonDefaultSessionVariables", "{}");
        summaryProfile.addInfoString("TxnId", txnId == -1 ? "N/A" : String.valueOf(txnId));
        summaryProfile.addInfoString("Backends", coordinatorBackendIds.toString());
        summaryProfile.addInfoString("Time Trace", timeTrace.summary());
        if (failure.get() != null) {
            summaryProfile.addInfoString("Exception", failure.get().getMessage());
        }
        if (loadJobFinalOperation != null) {
            EtlStatus etlStatus = loadJobFinalOperation.getLoadingStatus();
            summaryProfile.addInfoString("LoadResult", String.format("loadRows: %s, filterRows: %s, loadBytes: %s",
                    etlStatus.getCounters().getOrDefault(LoadEtlTask.DPP_NORMAL_ALL, "0"),
                    etlStatus.getCounters().getOrDefault(LoadEtlTask.DPP_ABNORMAL_ALL, "0"),
                    etlStatus.getCounters().getOrDefault(LoadJob.LOADED_BYTES, "0")));
        }
        profile.addChild(summaryProfile);
        if (collectProfileSuccess) {
            profile.addChild(coordinator.buildQueryProfile(true));
        }
        ProfileManager.getInstance().pushProfile(
                loadPlanner == null ? null : loadPlanner.getExecPlan().getProfilingPlan(), profile);
    }

    @VisibleForTesting
    Set<Long> getCoordinatorBackendIds() {
        return coordinatorBackendIds;
    }

    @VisibleForTesting
    Coordinator getCoordinator() {
        return coordinator;
    }

    @VisibleForTesting
    TimeTrace getTimeTrace() {
        return timeTrace;
    }

    // Trace the timing of various stages of the load operation.
    static class TimeTrace {
        long createTimeMs;
        long beginTxnTimeMs = -1;
        long executeLoadTimeMs = -1;
        long deployPlanTimeMs = -1;
        AtomicLong joinPlanTimeMs = new AtomicLong(-1);
        long commitTxnTimeMs = -1;
        long finishTimeMs = -1;

        public TimeTrace() {
            this.createTimeMs = System.currentTimeMillis();
        }

        public long totalCostMs() {
            return finishTimeMs > 0 ? finishTimeMs - createTimeMs : System.currentTimeMillis() - createTimeMs;
        }

        String summary() {
            StringBuilder sb = new StringBuilder();
            sb.append("total: ").append(totalCostMs()).append(" ms");
            appendTraceItem(sb, ", pending: ", createTimeMs, beginTxnTimeMs);
            appendTraceItem(sb, ", begin txn: ", beginTxnTimeMs, executeLoadTimeMs);
            appendTraceItem(sb, ", plan: ", executeLoadTimeMs, deployPlanTimeMs);
            appendTraceItem(sb, ", deploy: ", deployPlanTimeMs, joinPlanTimeMs.get());
            appendTraceItem(sb, ", load: ", joinPlanTimeMs.get(), commitTxnTimeMs);
            appendTraceItem(sb, ", commit/publish txn: ", commitTxnTimeMs, finishTimeMs);
            return sb.toString();
        }

        private void appendTraceItem(StringBuilder sb, String msg, long startTimeMs, long endTimeMs) {
            if (startTimeMs > 0) {
                sb.append(msg).append(endTimeMs - startTimeMs).append(" ms");
            }
        }
    }
}
