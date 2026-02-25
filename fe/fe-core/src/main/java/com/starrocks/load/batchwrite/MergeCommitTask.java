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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.CloseableLock;
import com.starrocks.common.Config;
import com.starrocks.common.DataQualityException;
import com.starrocks.common.LoadException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.TimeoutWatcher;
import com.starrocks.common.Version;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LoadPriority;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.load.LoadConstants;
import com.starrocks.load.loadv2.LoadErrorUtils;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.VisibleStateWaiter;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * Executes a single merge-commit load.
 *
 * <p>This task drives the end-to-end load lifecycle:</p>
 *
 * <ul>
 *   <li>Begin transaction</li>
 *   <li>Generate execution plan</li>
 *   <li>Execute the load via {@link Coordinator}</li>
 *   <li>Commit and wait for publish/visibility</li>
 *   <li>Finalize metrics, callback, and optional runtime profile reporting</li>
 * </ul>
 *
 * <p>Execution is modeled as a simple state machine to ensure that state transitions
 * and related fields are updated atomically. The execution can be cancelled.
 */
public class MergeCommitTask extends AbstractStreamLoadTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitTask.class);

    private static final String LOAD_TYPE_NAME = "MERGE_COMMIT";

    /**
     * Internal execution states for this task.
     *
     * <p>The allowed transitions are defined in {@link #TASK_STATE_TRANSITION}.
     */
    enum TaskState {
        /** Created but not started yet. */
        PENDING(false),
        /** Beginning the transaction. */
        BEGINNING_TXN(false),
        /** Planning the load execution. */
        PLANNING(false),
        /** Executing the load and waiting for completion. */
        EXECUTING(false),
        /** Committing the transaction. */
        COMMITTING(false),
        /** Transaction committed; waiting for publish/visibility. */
        COMMITTED(false),
        /** Fully finished (success or publish timeout); no further work expected. */
        FINISHED(true),
        /** Aborted due to an internal failure in {@link #run()}. */
        ABORTED(true),
        /** Cancelled explicitly. */
        CANCELLED(true);

        /** Whether this is a final state. */
        private final boolean finalState;

        TaskState(boolean finalState) {
            this.finalState = finalState;
        }

        public boolean isFinalState() {
            return finalState;
        }
    }

    /**
     * Allowed state transitions.
     *
     * <p>State transition diagram:</p>
     *
     * <pre>
     *      +--------> ABORTED  <-----    --+-----------+------------+
     *     /            ^                   ^           ^            |
     *    /             |                   |           |            |
     * PENDING    --> BEGINNING_TXN --> PLANNING --> LOADING --> COMMITTING --> COMMITTED --> FINISHED
     *    \             |                   |           |
     *     \            v                   v           v
     *      +--------> CANCELLED <----------+-----------+
     * </pre>
     *
     * <p>Notes:</p>
     * <ul>
     *   <li>{@link TaskState#COMMITTING} must not transition to {@link TaskState#CANCELLED}. {@code CANCELLED} means an
     *       explicit/user cancel, but once commit is started the transaction may have already been accepted by the
     *       transaction manager and there is no safe "undo" point. Allowing {@code COMMITTING -> CANCELLED} would
     *       make it possible to observe "CANCELLED" while the transaction later becomes COMMITTED, causing a
     *       task-state/transaction-state divergence.
     *   </li>
     *   <li>{@link TaskState#COMMITTING} is still allowed to transition to {@link TaskState#ABORTED} because the transaction
     *       commit can fail. In that case, {@link #run()} aborts the transaction and terminates the task as {@code ABORTED}.
     *       If the commit succeeds, task transitions to {@link TaskState#COMMITTED} and will never end up as {@code ABORTED}.
     *   </li>
     * </ul>
     */
    private static final Map<TaskState, EnumSet<TaskState>> TASK_STATE_TRANSITION = new EnumMap<>(TaskState.class);
    static {
        TASK_STATE_TRANSITION.put(TaskState.PENDING,
                EnumSet.of(TaskState.BEGINNING_TXN, TaskState.CANCELLED, TaskState.ABORTED));
        TASK_STATE_TRANSITION.put(TaskState.BEGINNING_TXN,
                EnumSet.of(TaskState.PLANNING, TaskState.CANCELLED, TaskState.ABORTED));
        TASK_STATE_TRANSITION.put(TaskState.PLANNING,
                EnumSet.of(TaskState.EXECUTING, TaskState.CANCELLED, TaskState.ABORTED));
        TASK_STATE_TRANSITION.put(TaskState.EXECUTING,
                EnumSet.of(TaskState.COMMITTING, TaskState.CANCELLED, TaskState.ABORTED));
        TASK_STATE_TRANSITION.put(TaskState.COMMITTING,
                EnumSet.of(TaskState.COMMITTED, TaskState.ABORTED));
        TASK_STATE_TRANSITION.put(TaskState.COMMITTED, EnumSet.of(TaskState.FINISHED));
        TASK_STATE_TRANSITION.put(TaskState.FINISHED, EnumSet.noneOf(TaskState.class));
        TASK_STATE_TRANSITION.put(TaskState.CANCELLED, EnumSet.noneOf(TaskState.class));
        TASK_STATE_TRANSITION.put(TaskState.ABORTED, EnumSet.noneOf(TaskState.class));
    }

    /** Unique identifier of this merge-commit task. */
    private final long taskId;
    /** Target database id. */
    private final long dbId;
    /** Target table identifier (database name + table name). */
    private final TableId tableRef;
    /** Load label used for transaction bookkeeping and visibility. */
    private final String label;
    /** Query/load identifier used to register the in-flight execution. */
    private final TUniqueId loadId;
    /** User-provided stream load request parameters. */
    private final StreamLoadInfo streamLoadInfo;
    /** Key-value load parameters (e.g. max filter ratio). */
    private final StreamLoadKvParams loadKvParams;
    /** The user to request the load. */
    private final String user;
    /** Warehouse name for execution. */
    private final String warehouseName;
    /** Backend IDs selected for execution. */
    private final Set<Long> backendIds;
    /** Merge-commit window size in milliseconds. */
    private final int mergeCommitIntervalMs;
    /** Factory for creating the {@link Coordinator} used by stream load. */
    private final Coordinator.Factory coordinatorFactory;
    /** Callback invoked once the task finishes (in {@link #run()}). */
    private final MergeCommitTaskCallback completionCallback;
    /** Tracks the timing of key stages. */
    private final TimeTrace loadTimeTrace;
    /** Guards state transitions and mutable execution fields. */
    private final ReentrantLock lock;

    // ==================  The following members are mutated during execution and are protected by {@link #lock}.

    // Current state.
    private volatile TaskState taskState;
    // The message describing the current state. For CANCELLED/ABORTED, it's the reason.
    // For FINISHED, describe whether publish success before load timeouts
    private volatile String taskStateMessage = null;
    // Cancel handler for the current non-final state; invoked when the task is cancelled in that state.
    // It may be null depending on the current state (e.g. states without meaningful cancel action).
    private volatile TaskStateCancelHandler taskStateCancelHandler = null;
    // Transaction id; set in {@link #run()} after transaction begins.
    private volatile long txnId = -1;
    // Load statistics; set when entering COMMITTING in {@link #run()}.
    private volatile LoadStats loadStats = null;
    // Observes state transitions for unit tests.
    private volatile StateTransitionObserver stateTransitionObserver = null;

    /**
     * Creates a new merge-commit task.
     *
     * @param taskId unique task identifier
     * @param dbId target database id
     * @param tableRef target table identifier
     * @param label load label used for transaction bookkeeping
     * @param loadId query/load identifier
     * @param streamLoadInfo stream load request parameters
     * @param mergeCommitIntervalMs merge-commit interval in milliseconds
     * @param loadKvParams key-value load parameters (e.g. max filter ratio)
     * @param user the user to request the load
     * @param warehouseName warehouse name used for execution
     * @param backendIds backend IDs chosen for execution
     * @param coordinatorFactory factory to create {@link Coordinator} instances
     * @param completionCallback completion callback invoked from {@link #run()}
     */
    public MergeCommitTask(
            long taskId,
            long dbId,
            TableId tableRef,
            String label,
            TUniqueId loadId,
            StreamLoadInfo streamLoadInfo,
            int mergeCommitIntervalMs,
            StreamLoadKvParams loadKvParams,
            String user,
            String warehouseName,
            Set<Long> backendIds,
            Coordinator.Factory coordinatorFactory,
            MergeCommitTaskCallback completionCallback) {
        this.taskId = taskId;
        this.dbId = dbId;
        this.tableRef = tableRef;
        this.label = label;
        this.loadId = loadId;
        this.streamLoadInfo = streamLoadInfo;
        this.loadKvParams = loadKvParams;
        this.mergeCommitIntervalMs = mergeCommitIntervalMs;
        this.user = user;
        this.warehouseName = warehouseName;
        this.backendIds = backendIds;
        this.coordinatorFactory = coordinatorFactory;
        this.completionCallback = completionCallback;
        this.loadTimeTrace = new TimeTrace();
        this.lock = new ReentrantLock();
        this.taskState = TaskState.PENDING;
    }

    /**
     * Runs the merge-commit load.
     *
     * <p>On success, the task reaches {@link TaskState#FINISHED}. On failure, it transitions to
     * {@link TaskState#ABORTED} and aborts the transaction if it has been created.
     */
    @Override
    public void run() {
        long startTimeMs = System.currentTimeMillis();
        loadTimeTrace.startTimeMs.set(startTimeMs);
        loadTimeTrace.pendingCostMs.set(startTimeMs - loadTimeTrace.createTimeMs);
        TimeoutWatcher timeoutWatcher = new TimeoutWatcher(streamLoadInfo.getTimeout() * 1000L);
        long localTxnId = -1;
        ConnectContext connectContext = null;
        LoadPlanner loadPlanner = null;
        Coordinator coordinator = null;
        try {
            // 1) Resolve database/table and begin transaction.
            transitionToState(TaskState.BEGINNING_TXN, "", null, () -> {});
            final Database database = getDatabase();
            final OlapTable table = getOlapTable(database);
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().addCallback(this);
            localTxnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                    dbId, Lists.newArrayList(table.getId()), label, null,
                    TransactionState.TxnCoordinator.fromThisFE(),
                    TransactionState.LoadJobSourceType.FRONTEND_STREAMING,
                    taskId, streamLoadInfo.getTimeout(), streamLoadInfo.getComputeResource());

            // 2) Build execution plan and prepare coordinator.
            final long finalTxnId = localTxnId;
            transitionToState(TaskState.PLANNING, "", null, () -> {
                this.txnId = finalTxnId;
            });
            connectContext = createConnectContext(table);
            // use tracer to get profile for planner/scheduler
            Tracers.register(connectContext);
            Tracers.init(connectContext, "TIMER", null);
            loadPlanner = createLoadPlan(database, table, connectContext, timeoutWatcher.getLeftTimeoutMillis());
            coordinator = coordinatorFactory.createStreamLoadScheduler(loadPlanner);

            // 3) Execute load and collect load statistics.
            final Coordinator finalCoordinator = coordinator;
            transitionToState(TaskState.EXECUTING, "", new ExecutingStateCancelHandler(finalCoordinator), () -> {});
            LoadResult loadResult = executeLoad(connectContext, coordinator, timeoutWatcher.getLeftTimeoutMillis());

            // 4) Validate data quality and commit transaction.
            final LoadStats loadStats = loadResult.loadStats;
            transitionToState(TaskState.COMMITTING, "", null, () -> this.loadStats = loadStats);
            long selectedRows = loadStats.abnormalRows + loadStats.normalRows;
            double maxFilterRatio = loadKvParams.getMaxFilterRatio().orElse(0.0);
            if (loadStats.abnormalRows > selectedRows * maxFilterRatio) {
                throw new DataQualityException(String.format(
                        "There is a data quality issue. Please check the tracking URL or SQL for details. " +
                                "Tracking URL: %s. Tracking SQL: %s",
                        loadStats.errorTrackingUrl.orElse("N/A"), getErrorTrackingSql(taskId)));
            }
            VisibleStateWaiter waiter;
            try (ScopedTimer ignored = new ScopedTimer(loadTimeTrace.commitCostMs)) {
                // Currently task is not persisted, so no need to persist attachment for replaying
                waiter = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().retryCommitOnRateLimitExceeded(
                        database, localTxnId, loadResult.tabletCommitInfos, loadResult.tabletFailInfos,
                        null, timeoutWatcher.getLeftTimeoutMillis());
                loadTimeTrace.commitTimeMs.set(System.currentTimeMillis());
            }

            // 5) Wait for publish/visibility.
            transitionToState(TaskState.COMMITTED, "", null, () -> {});
            String publishFailMsg = "";
            try (ScopedTimer ignored = new ScopedTimer(loadTimeTrace.publishCostMs)) {
                long publishTimeoutMs = timeoutWatcher.getLeftTimeoutMillis();
                boolean timeout = !awaitPublish(waiter, publishTimeoutMs);
                if (timeout) {
                    throw new Exception(String.format("publish timed out after %sms", publishTimeoutMs));
                }
            } catch (Exception e) {
                String error = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                publishFailMsg = "publish failed: " + error;
                LOG.warn("Publish failed. db={}, table={}, label={}, txnId={}, error={}",
                        tableRef.getDbName(), tableRef.getTableName(), label, localTxnId, error);
            }

            // 6) Mark finished.
            transitionToState(TaskState.FINISHED, publishFailMsg, null, () -> {});
        } catch (Exception e) {
            final Pair<TaskState, String> currentState = getTaskState();
            final boolean cancelled = currentState.first == TaskState.CANCELLED;
            String exceptionMsg = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            String abortReason = cancelled ? currentState.second : exceptionMsg;
            try {
                if (localTxnId != -1) {
                    // remove callback before abortTransaction(), so that the afterAborted() callback will not be called again
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(taskId);
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(dbId, localTxnId, abortReason);
                }
            } catch (Exception ie) {
                LOG.warn("Failed to abort transaction. db={}, table={}, label={}, txnId={}, reason={}",
                        tableRef.getDbName(), tableRef.getTableName(), label, localTxnId, abortReason, ie);
            }
            if (!cancelled) {
                try {
                    transitionToState(TaskState.ABORTED, abortReason, null, () -> {});
                } catch (Exception ie) {
                    LOG.warn("Failed to transition to ABORTED state. db={}, table={}, label={}, loadId={}, txnId={}",
                            tableRef.getDbName(), tableRef.getTableName(),
                            label, DebugUtil.printId(loadId), localTxnId, ie);
                }
                LOG.warn("Merge-commit load failed. db={}, table={}, label={}, txnId={}, reason={}",
                        tableRef.getDbName(), tableRef.getTableName(), label, localTxnId, abortReason);
            }
        } finally {
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory().removeCallback(taskId);
            completionCallback.finish(this);
            updateMetrics();
            collectProfile(connectContext, loadPlanner, coordinator);
            // close tracer after profile collection
            Tracers.close();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finish merge-commit load. db={}, table={}, label={}, loadId={}, txnId={}, parameters={}, {}",
                        tableRef.getDbName(), tableRef.getTableName(),
                        label, DebugUtil.printId(loadId), localTxnId, loadKvParams, loadTimeTrace);
            }
        }
    }

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
            summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(loadId));
            summaryProfile.addInfoString(ProfileManager.START_TIME,
                    TimeUtils.longToTimeString(loadTimeTrace.createTimeMs));
            summaryProfile.addInfoString(ProfileManager.END_TIME,
                    TimeUtils.longToTimeString(loadTimeTrace.endTimeMs.get()));
            summaryProfile.addInfoString(ProfileManager.TOTAL_TIME,
                    DebugUtil.getPrettyStringMs(loadTimeTrace.totalCostMs()));
            summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
            summaryProfile.addInfoString(ProfileManager.QUERY_STATE, taskState.name());
            summaryProfile.addInfoString("State Message", taskStateMessage);
            summaryProfile.addInfoString(ProfileManager.LOAD_TYPE, LOAD_TYPE_NAME);
            summaryProfile.addInfoString("StarRocks Version",
                    String.format("%s-%s", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH));
            summaryProfile.addInfoString("Default Db", tableRef.getDbName());
            summaryProfile.addInfoString("Table", tableRef.getTableName());
            summaryProfile.addInfoString("Sql Statement", loadKvParams.toString());
            summaryProfile.addInfoString(ProfileManager.WAREHOUSE_CNGROUP, warehouseName);
            summaryProfile.addInfoString(ProfileManager.PROFILE_COLLECT_TIME,
                    DebugUtil.getPrettyStringMs(collectProfileCostMs.get()));
            summaryProfile.addInfoString("IsProfileAsync", String.valueOf(true));
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
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
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
    public String getTableName() {
        return tableRef.getTableName();
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public long getTxnId() {
        return txnId;
    }

    @Override
    public String getStateName() {
        return convertTaskStateToLoadState();
    }

    @Override
    public boolean isFinalState() {
        return taskState.isFinalState();
    }

    @Override
    public long createTimeMs() {
        return loadTimeTrace.createTimeMs;
    }

    @Override
    public long endTimeMs() {
        return loadTimeTrace.endTimeMs.get();
    }

    @Override
    public long getFinishTimestampMs() {
        return endTimeMs();
    }

    @Override
    public String getStringByType() {
        return LOAD_TYPE_NAME;
    }

    @Override
    public boolean checkNeedRemove(long currentMs, boolean isForce) {
        if (!isFinalState()) {
            return false;
        }
        long endTime = endTimeMs();
        if (endTime < 0) {
            return false;
        }
        return isForce || ((currentMs - endTime) > Config.stream_load_task_keep_max_second * 1000L);
    }

    @Override
    public List<TLoadInfo> toThrift() {
        return getLoadInfoWithLock("toThrift", () -> {
            TLoadInfo info = new TLoadInfo();
            info.setJob_id(taskId);
            info.setType(getStringByType());
            info.setDb(tableRef.getDbName());
            info.setTable(tableRef.getTableName());
            info.setUser(user);
            info.setLabel(label);
            info.setLoad_id(DebugUtil.printId(loadId));
            info.setTxn_id(txnId);
            if (ProfileManager.getInstance().hasProfile(DebugUtil.printId(loadId))) {
                info.setProfile_id(DebugUtil.printId(loadId));
            }
            info.setProperties(new Gson().toJson(loadKvParams.toMap()));
            info.setPriority(LoadPriority.NORMAL);
            info.setState(convertTaskStateToLoadState());
            info.setError_msg(taskStateMessage);

            LoadStats stats = this.loadStats;
            if (stats != null) {
                String trackingUrl = stats.errorTrackingUrl.orElse(null);
                if (trackingUrl != null && !trackingUrl.isEmpty()) {
                    info.setUrl(trackingUrl);
                    info.setTracking_sql(getErrorTrackingSql(taskId));
                }
                List<String> rejectedPaths = stats.rejectedRecordPaths.orElse(null);
                if (rejectedPaths != null && !rejectedPaths.isEmpty()) {
                    info.setRejected_record_path(Joiner.on(", ").join(rejectedPaths));
                }
                info.setNum_scan_rows(stats.normalRows + stats.abnormalRows + stats.unselectedRows);
                // not have stat for scan bytes, and use normal bytes an approximation
                info.setNum_scan_bytes(stats.normalBytes);
                info.setNum_sink_rows(stats.normalRows);
                info.setNum_filtered_rows(stats.abnormalRows);
                info.setNum_unselected_rows(stats.unselectedRows);
            }

            info.setCreate_time(TimeUtils.longToTimeString(loadTimeTrace.createTimeMs));
            info.setLoad_start_time(TimeUtils.longToTimeString(loadTimeTrace.startTimeMs.get()));
            info.setLoad_commit_time(TimeUtils.longToTimeString(loadTimeTrace.commitTimeMs.get()));
            info.setLoad_finish_time(TimeUtils.longToTimeString(endTimeMs()));

            info.setWarehouse(warehouseName);
            info.setRuntime_details(getRuntimeDetails());
            long execStartTime = loadTimeTrace.execWaitStartTimeMs.get();
            long mergeWindowElapsedMs;
            if (execStartTime < 0) {
                mergeWindowElapsedMs = 0;
            } else if (loadTimeTrace.endTimeMs.get() > 0) {
                mergeWindowElapsedMs = loadTimeTrace.endTimeMs.get() - execStartTime;
            } else {
                mergeWindowElapsedMs = System.currentTimeMillis() - execStartTime;
            }
            double progress = (double) Math.min(mergeCommitIntervalMs, mergeWindowElapsedMs) / mergeCommitIntervalMs;
            info.setProgress(String.format("Merge Window %.2f%%", progress * 100));
            return info;
        });
    }

    @Override
    public List<List<String>> getShowInfo() {
        return getLoadInfoWithLock("getShowInfo", () -> {
            List<String> row = Lists.newArrayList();
            row.add(label);
            row.add(String.valueOf(taskId));
            row.add(DebugUtil.printId(loadId));
            row.add(String.valueOf(txnId));
            row.add(tableRef.getDbName());
            row.add(tableRef.getTableName());
            row.add(convertTaskStateToLoadState());
            row.add(taskStateMessage);
            
            LoadStats stats = this.loadStats;
            String trackingUrl = null;
            if (stats != null) {
                trackingUrl = stats.errorTrackingUrl.orElse(null);
            }
            row.add(trackingUrl != null && !trackingUrl.isEmpty() ? trackingUrl : "");
            
            row.add(String.valueOf(backendIds.size()));
            row.add(String.valueOf(taskState == TaskState.COMMITTED ? backendIds.size() : 0));

            if (stats != null) {
                row.add(String.valueOf(stats.normalRows));
                row.add(String.valueOf(stats.abnormalRows));
                row.add(String.valueOf(stats.unselectedRows));
                row.add(String.valueOf(stats.normalBytes));
            } else {
                row.add("0");
                row.add("0");
                row.add("0");
                row.add("0");
            }
            
            row.add(String.valueOf(streamLoadInfo.getTimeout()));
            row.add(TimeUtils.longToTimeString(loadTimeTrace.createTimeMs));
            row.add(TimeUtils.longToTimeString(loadTimeTrace.startTimeMs.get()));
            row.add(TimeUtils.longToTimeString(loadTimeTrace.startTimeMs.get()));
            long commitTime = loadTimeTrace.commitTimeMs.get();
            long commitCost = loadTimeTrace.commitCostMs.get();
            long startPreparingTime = commitTime > 0 && commitCost > 0 ? Math.max(0, commitTime - commitCost) : -1;
            row.add(TimeUtils.longToTimeString(startPreparingTime));
            row.add(TimeUtils.longToTimeString(commitTime));
            row.add(TimeUtils.longToTimeString(endTimeMs()));
            
            // MergeCommitTask doesn't have channel states
            row.add("");
            row.add(getStringByType());

            if (trackingUrl != null && !trackingUrl.isEmpty()) {
                row.add(getErrorTrackingSql(taskId));
            } else {
                row.add("");
            }
            return row;
        });
    }

    @Override
    public List<List<String>> getShowBriefInfo() {
        return getLoadInfoWithLock("getShowBriefInfo", () -> {
            List<String> row = Lists.newArrayList();
            row.add(label);
            row.add(String.valueOf(taskId));
            row.add(tableRef.getDbName());
            row.add(tableRef.getTableName());
            row.add(convertTaskStateToLoadState());
            return row;
        });
    }

    @Override
    public List<TStreamLoadInfo> toStreamLoadThrift() {
        return getLoadInfoWithLock("toStreamLoadThrift", () -> {
            TStreamLoadInfo info = new TStreamLoadInfo();
            info.setLabel(label);
            info.setId(taskId);
            info.setLoad_id(DebugUtil.printId(loadId));
            info.setTxn_id(txnId);
            info.setDb_name(tableRef.getDbName());
            info.setTable_name(tableRef.getTableName());
            info.setState(convertTaskStateToLoadState());
            info.setError_msg(taskStateMessage);

            LoadStats stats = this.loadStats;
            if (stats != null) {
                String trackingUrl = stats.errorTrackingUrl.orElse(null);
                if (trackingUrl != null && !trackingUrl.isEmpty()) {
                    info.setTracking_url(trackingUrl);
                    info.setTracking_sql(getErrorTrackingSql(taskId));
                }
                info.setNum_rows_normal(stats.normalRows);
                info.setNum_rows_ab_normal(stats.abnormalRows);
                info.setNum_rows_unselected(stats.unselectedRows);
                info.setNum_load_bytes(stats.normalBytes);
            }

            info.setChannel_num(backendIds.size());
            info.setPrepared_channel_num(taskState == TaskState.COMMITTED ? backendIds.size() : 0);

            info.setTimeout_second(streamLoadInfo.getTimeout());
            info.setCreate_time_ms(TimeUtils.longToTimeString(loadTimeTrace.createTimeMs));
            info.setBefore_load_time_ms(TimeUtils.longToTimeString(loadTimeTrace.startTimeMs.get()));
            info.setStart_loading_time_ms(TimeUtils.longToTimeString(loadTimeTrace.startTimeMs.get()));
            long commitTime = loadTimeTrace.commitTimeMs.get();
            long commitCost = loadTimeTrace.commitCostMs.get();
            long startPreparingTime = commitTime > 0 && commitCost > 0 ? Math.max(0, commitTime - commitCost) : -1;
            info.setStart_preparing_time_ms(TimeUtils.longToTimeString(startPreparingTime));
            info.setFinish_preparing_time_ms(TimeUtils.longToTimeString(commitTime));
            info.setEnd_time_ms(TimeUtils.longToTimeString(endTimeMs()));

            info.setChannel_state("");
            info.setType(getStringByType());
            return info;
        });
    }

    /**
     * Safely retrieves load information while holding the task's lock.
     *
     * @param source the name of the calling method
     * @param infoBuilder the callable that builds the information object
     * @param <T> the type of information object to build (e.g., {@link TLoadInfo}, {@link List}{@code <String>})
     * @return a list containing the built information object, or an empty list if an exception occurred
     */
    private <T> List<T> getLoadInfoWithLock(String source, Callable<T> infoBuilder) {
        List<T> results = new ArrayList<>();
        try (AutoCloseable ignored = CloseableLock.lock(lock)) {
            T info = infoBuilder.call();
            results.add(info);
        } catch (Exception e) {
            LOG.debug("Failed for {}, db={}, table={}, taskId={}, label={}, loadId={}, txnId={}",
                    source, tableRef.getDbName(), tableRef.getTableName(), taskId, label, DebugUtil.printId(loadId), txnId, e);
        }
        return results;
    }

    private String convertTaskStateToLoadState() {
        TaskState state = taskState;
        return switch (state) {
            case PENDING, COMMITTED, FINISHED -> state.name();
            case CANCELLED, ABORTED -> "CANCELLED";
            default -> "LOADING";
        };
    }

    private String getRuntimeDetails() {
        TreeMap<String, Object> details = Maps.newTreeMap();
        details.put(LoadConstants.RUNTIME_DETAILS_LOAD_ID, DebugUtil.printId(loadId));
        details.put(LoadConstants.RUNTIME_DETAILS_TXN_ID, txnId);
        details.put(LoadConstants.RUNTIME_DETAILS_BACKENDS, backendIds);
        details.put("task_state", taskState.name());
        BiConsumer<String, AtomicLong> timeSetter = (name, time) -> {
            long value = time.get();
            if (value >= 0) {
                details.put(name, value);
            }
        };
        timeSetter.accept("pending_time_ms", loadTimeTrace.pendingCostMs);
        timeSetter.accept("commit_time_ms", loadTimeTrace.commitCostMs);
        timeSetter.accept("publish_time_ms", loadTimeTrace.publishCostMs);
        Gson gson = new Gson();
        return gson.toJson(details);
    }

    // =============== inherited methods from  LoadJobWithWarehouse ==============

    @Override
    public long getCurrentWarehouseId() {
        return streamLoadInfo.getComputeResource().getWarehouseId();
    }

    @Override
    public boolean isFinal() {
        return isFinalState();
    }

    static class LoadResult {
        final List<TabletCommitInfo> tabletCommitInfos;
        final List<TabletFailInfo> tabletFailInfos;
        final LoadStats loadStats;

        public LoadResult(List<TabletCommitInfo> tabletCommitInfos, List<TabletFailInfo> tabletFailInfos,
                          LoadStats loadStats) {
            this.tabletCommitInfos = tabletCommitInfos;
            this.tabletFailInfos = tabletFailInfos;
            this.loadStats = loadStats;
        }
    }

    // =============== inherited methods from AbstractStreamLoadTask, but not used by MergeCommitTask ==============

    @Override
    public void beginTxnFromFrontend(TransactionResult resp) {
        throw new UnsupportedOperationException("MergeCommitTask uses run() method instead");
    }

    @Override
    public void beginTxnFromFrontend(int channelId, int channelNum, TransactionResult resp) {
        throw new UnsupportedOperationException("MergeCommitTask uses run() method instead");
    }

    @Override
    public void beginTxnFromBackend(TUniqueId requestId, String clientIp, long backendId, TransactionResult resp) {
        throw new UnsupportedOperationException("MergeCommitTask uses run() method instead");
    }

    @Override
    public TNetworkAddress tryLoad(int channelId, String tableName, TransactionResult resp) throws StarRocksException {
        throw new UnsupportedOperationException("MergeCommitTask uses run() method instead");
    }

    @Override
    public TNetworkAddress executeTask(int channelId, String tableName, HttpHeaders headers, TransactionResult resp) {
        throw new UnsupportedOperationException("MergeCommitTask uses run() method instead");
    }

    @Override
    public void prepareChannel(int channelId, String tableName, HttpHeaders headers, TransactionResult resp) {
        throw new UnsupportedOperationException("MergeCommitTask uses run() method instead");
    }

    @Override
    public void waitCoordFinishAndPrepareTxn(long preparedTimeoutMs, TransactionResult resp) {
        throw new UnsupportedOperationException("MergeCommitTask uses run() method instead");
    }

    @Override
    public void commitTxn(HttpHeaders headers, TransactionResult resp) throws StarRocksException {
        throw new UnsupportedOperationException("MergeCommitTask uses run() method instead");
    }

    @Override
    public void manualCancelTask(TransactionResult resp) throws StarRocksException {
        throw new UnsupportedOperationException("MergeCommitTask uses cancel() method instead");
    }

    @Override
    public boolean checkNeedPrepareTxn() {
        throw new UnsupportedOperationException("MergeCommitTask uses cancel() method instead");
    }

    @Override
    public boolean isDurableLoadState() {
        return false;
    }

    @Override
    public void cancelAfterRestart() {
        // do nothing
    }

    @Override
    public void init() {
        // do nothing
    }

    // ===============  inherited methods from GsonPreProcessable/GsonPostProcessable ==============

    @Override
    public void gsonPreProcess() {
        // do nothing
    }

    @Override
    public void gsonPostProcess() {
        // do nothing
    }

    // ================ methods for testing ================

    @VisibleForTesting
    public TUniqueId getLoadId() {
        return loadId;
    }

    @VisibleForTesting
    public boolean isPlanDeployed() {
        return loadTimeTrace.execWaitStartTimeMs.get() > 0;
    }

    /**
     * Statistics about rows/bytes processed by the load execution.
     */
    private static class LoadStats {
        final long normalRows;
        final long normalBytes;
        final long abnormalRows;
        final long unselectedRows;
        final Optional<String> errorTrackingUrl;
        final Optional<List<String>> rejectedRecordPaths;

        public LoadStats(long normalRows, long normalBytes, long abnormalRows, long unselectedRows,
                         String errorTrackingUrl, List<String> rejectedRecordPaths) {
            this.normalRows = normalRows;
            this.normalBytes = normalBytes;
            this.abnormalRows = abnormalRows;
            this.unselectedRows = unselectedRows;
            this.errorTrackingUrl = Optional.ofNullable(errorTrackingUrl);
            this.rejectedRecordPaths = Optional.ofNullable(rejectedRecordPaths);
        }
    }

    private static class ScopedTimer implements AutoCloseable {

        private final AtomicLong cost;
        private final long startTimeMs;

        public ScopedTimer(AtomicLong cost) {
            this.cost = cost;
            this.startTimeMs = System.currentTimeMillis();
        }

        @Override
        public void close() {
            cost.set(System.currentTimeMillis() - startTimeMs);
        }
    }

    /**
     * Traces the timing of various stages of the load operation.
     */
    private static class TimeTrace {
        /** Task creation timestamp (milliseconds since epoch). */
        final long createTimeMs;
        /** Timestamp when the task starts running (milliseconds since epoch). */
        final AtomicLong startTimeMs = new AtomicLong(-1);
        /** Task end timestamp (milliseconds since epoch); {@code -1} if not finished yet. */
        final AtomicLong endTimeMs = new AtomicLong(-1);
        /** Time spent in {@link TaskState#PENDING} (milliseconds). */
        final AtomicLong pendingCostMs = new AtomicLong(-1);
        /**
         * Timestamp when waiting for {@link Coordinator#join(int)} starts (milliseconds since epoch).
         * {@code -1} if not started.
         */
        final AtomicLong execWaitStartTimeMs = new AtomicLong(-1);
        /** Timestamp when the transaction is committed (milliseconds since epoch). */
        final AtomicLong commitTimeMs = new AtomicLong(-1);
        /** Time spent committing the transaction (milliseconds). */
        final AtomicLong commitCostMs = new AtomicLong(-1);
        /** Time spent waiting for publish/visibility (milliseconds). */
        final AtomicLong publishCostMs = new AtomicLong(-1);

        public TimeTrace() {
            this.createTimeMs = System.currentTimeMillis();
        }

        public long totalCostMs() {
            long timestamp = endTimeMs.get();
            return timestamp > 0 ? timestamp - createTimeMs : System.currentTimeMillis() - createTimeMs;
        }

        @Override
        public String toString() {
            return "TimeTrace{" +
                    "createTimeMs=" + createTimeMs +
                    ", endTimeMs=" + endTimeMs +
                    ", pendingCostMs=" + pendingCostMs +
                    ", execWaitStartTimeMs=" + execWaitStartTimeMs +
                    ", commitCostMs=" + commitCostMs +
                    ", publishCostMs=" + publishCostMs +
                    '}';
        }
    }

    /**
     * A handler to cancel the current state.
     */
    private abstract static class TaskStateCancelHandler {

        protected TaskStateCancelHandler(TaskState taskState) {
            if (taskState == null) {
                throw new IllegalArgumentException("task state is null");
            }
            if (taskState.isFinalState()) {
                throw new IllegalArgumentException("Final state has no cancel handler: " + taskState);
            }
        }

        public abstract void cancel(String reason);
    }

    /**
     * The handler to cancel {@link TaskState#EXECUTING}.
     *
     * <p>It wraps the {@link Coordinator} running the load so that the in-flight execution can be cancelled.
     */
    private static final class ExecutingStateCancelHandler extends TaskStateCancelHandler {
        private final Coordinator coordinator;

        private ExecutingStateCancelHandler(Coordinator coordinator) {
            super(TaskState.EXECUTING);
            this.coordinator = coordinator;
        }

        @Override
        public void cancel(String reason) {
            if (coordinator != null) {
                coordinator.cancel(PPlanFragmentCancelReason.USER_CANCEL, reason);
            }
        }
    }

    @VisibleForTesting
    interface StateTransitionObserver {
        void onTransition(TaskState fromState, TaskState toState, String toStateMsg, boolean hasCancelHandler);
    }
}
