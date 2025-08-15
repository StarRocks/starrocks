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
// limitations under the License

package com.starrocks.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.mv.BaseMVRefreshProcessor;
import com.starrocks.scheduler.mv.MVRefreshExecutor;
import com.starrocks.scheduler.mv.MVRefreshProcessorFactory;
import com.starrocks.scheduler.mv.MVTraceUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.scheduler.TaskRun.MV_ID;

public class MVTaskRunProcessor extends BaseTaskRunProcessor implements MVRefreshExecutor {
    // used to generate unique statement IDs for the MV refresh task
    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);

    private Logger logger;
    private Database db;
    private MaterializedView mv;

    // used to store the mv task run context
    private MvTaskRunContext mvTaskRunContext;
    // used to store the mv refresh processor
    private BaseMVRefreshProcessor mvRefreshProcessor;
    // used to store the old transaction visible wait timeout to be restored after mv refresh task run
    private long oldTransactionVisibleWaitTimeout;
    // metrics entity for the mv
    private IMaterializedViewMetricsEntity mvMetricsEntity;
    // only trigger to post process when mv has been refreshed successfully
    private Constants.TaskRunState taskRunState = Constants.TaskRunState.FAILED;
    // runtime profile
    @VisibleForTesting
    private RuntimeProfile runtimeProfile;

    public MVTaskRunProcessor() {
    }

    @VisibleForTesting
    @Override
    public void prepare(TaskRunContext context) throws Exception {
        // NOTE: mvId is set in Task's properties when creating
        final Map<String, String> properties = context.getProperties();
        final long mvId = Long.parseLong(properties.get(MV_ID));
        this.db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(context.ctx.getDatabase());
        if (this.db == null) {
            throw new DmlException("database " + context.ctx.getDatabase() + " do not exist.");
        }

        final Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId);
        if (table == null || !(table instanceof MaterializedView)) {
            throw new DmlException(String.format("materialized view:%s in database:%s do not exist when refreshing",
                    mvId, context.ctx.getDatabase()));
        }
        this.mv = (MaterializedView) table;
        this.logger = MVTraceUtils.getLogger(mv, MVTaskRunProcessor.class);

        // NOTE: mvId is set in Task's properties when creating
        if (!mv.isActive()) {
            MVActiveChecker.tryToActivate(mv);
            logger.info("Activated the MV before refreshing: {}", mv.getName());
        }

        // metrics entity
        this.mvMetricsEntity = MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
        if (!mv.isActive()) {
            String errorMsg = String.format("Materialized view: %s/%d is not active due to %s.",
                    mv.getName(), mvId, mv.getInactiveReason());
            logger.warn(errorMsg);
            mvMetricsEntity.increaseRefreshJobStatus(Constants.TaskRunState.FAILED);
            throw new DmlException(errorMsg);
        }

        // wait util transaction is visible for mv refresh task
        // because mv will update base tables' visible version after insert, the mv's visible version
        // should keep up with the base tables, or it will return outdated result.
        final ConnectContext connectContext = context.getCtx();
        this.oldTransactionVisibleWaitTimeout = connectContext.getSessionVariable().getTransactionVisibleWaitTimeout();
        connectContext.getSessionVariable().setTransactionVisibleWaitTimeout(Long.MAX_VALUE / 1000);

        // Initialize status's job id which is used to track a batch of task runs.
        final String jobId = properties.containsKey(TaskRun.START_TASK_RUN_ID) ?
                properties.get(TaskRun.START_TASK_RUN_ID) : context.getTaskRunId();
        if (context.getStatus() != null) {
            context.getStatus().setStartTaskRunId(jobId);
        }

        // prepare mv context
        this.mvTaskRunContext = new MvTaskRunContext(context);
        // prepare partition ttl number
        int partitionTTLNumber = mv.getTableProperty().getPartitionTTLNumber();
        this.mvTaskRunContext.setPartitionTTLNumber(partitionTTLNumber);

        this.mvRefreshProcessor = MVRefreshProcessorFactory.INSTANCE.newProcessor(db, mv, mvTaskRunContext, mvMetricsEntity);
        logger.info("finish prepare refresh mv:{}, jobId: {}", mvId, jobId);
    }

    /**
     * Get the execution plan for refreshing the materialized view.
     * @return the execution plan for refreshing the materialized view, or null if no refresh is needed.
     * @throws Exception if an error occurs while getting the execution plan.
     */
    public ExecPlan getMVRefreshExecPlan() throws Exception {
        Preconditions.checkNotNull(mvTaskRunContext);
        Preconditions.checkNotNull(mvRefreshProcessor);

        // get exec plan
        BaseMVRefreshProcessor.ProcessExecPlan processExecPlan =
                mvRefreshProcessor.getProcessExecPlan(mvTaskRunContext);
        if (processExecPlan == null || processExecPlan.state() != Constants.TaskRunState.SUCCESS) {
            logger.info("No need to refresh mv: {}, because the materialized view is up to date.", mv.getName());
            return null;
        }
        return processExecPlan.execPlan();
    }

    @Override
    public Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception {
        // init to collect the base timer for refresh profile
        Tracers.register(context.getCtx());
        final QueryDebugOptions queryDebugOptions = context.getCtx().getSessionVariable().getQueryDebugOptions();
        final Tracers.Mode mvRefreshTraceMode = queryDebugOptions.getMvRefreshTraceMode();
        final Tracers.Module mvRefreshTraceModule = queryDebugOptions.getMvRefreshTraceModule();
        Tracers.init(mvRefreshTraceMode, mvRefreshTraceModule, true, false);

        final ConnectContext connectContext = context.getCtx();
        final QueryMaterializationContext queryMVContext = new QueryMaterializationContext();
        connectContext.setQueryMVContext(queryMVContext);
        try {
            // do refresh
            try (Timer ignored = Tracers.watchScope("MVRefreshDoWholeRefresh")) {
                // refresh mv
                Preconditions.checkState(mv != null);
                mvMetricsEntity = MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
                this.taskRunState = retryProcessTaskRun(context);
                // update metrics
                mvMetricsEntity.increaseRefreshJobStatus(taskRunState);
                connectContext.getState().setOk();
            }
        } catch (Exception e) {
            if (mvMetricsEntity != null) {
                mvMetricsEntity.increaseRefreshJobStatus(Constants.TaskRunState.FAILED);
            }
            connectContext.getState().setError(e.getMessage());
            throw e;
        } finally {
            try {
                // If mv's not active, mvContext may be null.
                if (mvTaskRunContext != null && connectContext != null) {
                    connectContext.getSessionVariable().setTransactionVisibleWaitTimeout(oldTransactionVisibleWaitTimeout);
                }
                // reset query mv context to avoid affecting other tasks
                queryMVContext.clear();
                connectContext.setQueryMVContext(null);

                if (FeConstants.runningUnitTest) {
                    runtimeProfile = new RuntimeProfile();
                    Tracers.toRuntimeProfile(runtimeProfile);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("refresh mv trace logs: {}", Tracers.getTrace(mvRefreshTraceMode));
                }
            } catch (Exception e) {
                logger.error("Failed to close Tracers", e);
            }
        }
        return this.taskRunState;
    }

    /**
     * Retry the `doRefreshMaterializedView` method to avoid insert fails in occasional cases.
     */
    private Constants.TaskRunState retryProcessTaskRun(TaskRunContext taskRunContext) throws DmlException {
        // Use current connection variables instead of mvContext's session variables to be better debug.
        int maxRefreshMaterializedViewRetryNum = mvRefreshProcessor.getRetryTimes(taskRunContext.getCtx());
        logger.info("start to refresh mv with retry times:{}", maxRefreshMaterializedViewRetryNum);

        Throwable lastException = null;
        int lockFailedTimes = 0;
        int refreshFailedTimes = 0;
        while (refreshFailedTimes < maxRefreshMaterializedViewRetryNum &&
                lockFailedTimes < Config.max_mv_refresh_try_lock_failure_retry_times) {
            try {
                if (refreshFailedTimes > 0) {
                    UUID uuid = UUID.randomUUID();
                    ConnectContext context = taskRunContext.getCtx();
                    logger.info("transfer QueryId: {} to {}", DebugUtil.printId(context.getQueryId()),
                            DebugUtil.printId(uuid));
                    context.setExecutionId(
                            new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                }
                Tracers.record("MVRefreshRetryTimes", String.valueOf(refreshFailedTimes));
                Tracers.record("MVRefreshLockRetryTimes", String.valueOf(lockFailedTimes));
                return mvRefreshProcessor.doProcessTaskRun(taskRunContext, this);
            } catch (LockTimeoutException e) {
                // if lock timeout, retry to refresh
                lockFailedTimes += 1;
                logger.warn("refresh mv failed at {}th time because try lock failed: {}",
                        lockFailedTimes, DebugUtil.getStackTrace(e));
                lastException = e;
            } catch (Throwable e) {
                refreshFailedTimes += 1;
                logger.warn("refresh mv failed at {}th time: {}", refreshFailedTimes, DebugUtil.getRootStackTrace(e));
                lastException = e;
            }

            // sleep some time if it is not the last retry time
            Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        }

        // throw the last exception if all retries failed
        String errorMsg = MvUtils.shrinkToSize(DebugUtil.getRootStackTrace(lastException), MAX_FIELD_VARCHAR_LENGTH);
        throw new DmlException("Refresh mv %s failed after %s times, try lock failed: %s, error-msg : " +
                "%s", lastException, mv.getName(), refreshFailedTimes, lockFailedTimes, errorMsg);
    }

    @Override
    @VisibleForTesting
    public void executePlan(ExecPlan execPlan, InsertStmt insertStmt) throws Exception {
        Preconditions.checkNotNull(execPlan);
        Preconditions.checkNotNull(insertStmt);

        ConnectContext ctx = mvTaskRunContext.getCtx();
        if (mvTaskRunContext.getTaskRun().isKilled()) {
            logger.warn("[QueryId:{}] refresh materialized view {} is killed", ctx.getQueryId(),
                    mv.getName());
            throw new StarRocksException("User Cancelled");
        }

        StmtExecutor executor = StmtExecutor.newInternalExecutor(ctx, insertStmt);
        ctx.setExecutor(executor);
        if (ctx.getParent() != null && ctx.getParent().getExecutor() != null) {
            StmtExecutor parentStmtExecutor = ctx.getParent().getExecutor();
            parentStmtExecutor.registerSubStmtExecutor(executor);
        }
        ctx.setStmtId(STMT_ID_GENERATOR.incrementAndGet());

        logger.info("[QueryId:{}] start to refresh mv in DML", ctx.getQueryId());
        try {
            executor.handleDMLStmtWithProfile(execPlan, insertStmt);
        } catch (Exception e) {
            logger.warn("[QueryId:{}] refresh mv {} failed in DML", ctx.getQueryId(), e);
            throw e;
        } finally {
            logger.info("[QueryId:{}] finished to refresh mv in DML", ctx.getQueryId());
            auditAfterExec(mvTaskRunContext, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
        }
    }

    public MvTaskRunContext getMvTaskRunContext() {
        return this.mvTaskRunContext;
    }

    public BaseMVRefreshProcessor getMVRefreshProcessor() {
        return this.mvRefreshProcessor;
    }

    @VisibleForTesting
    public RuntimeProfile getRuntimeProfile() {
        return runtimeProfile;
    }

    private String getPostRun(ConnectContext ctx, MaterializedView mv) {
        // check whether it's enabled to analyze MV task after task run for each task run,
        // so the analyze_for_mv can be set in session variable dynamically
        if (mv == null) {
            return "";
        }
        return TaskBuilder.getAnalyzeMVStmt(ctx, mv.getName());
    }

    @Override
    public void postTaskRun(TaskRunContext context) throws Exception {
        if (taskRunState != Constants.TaskRunState.SUCCESS) {
            return;
        }
        // recreate post run context for each task run
        final ConnectContext ctx = context.getCtx();
        final String postRun = getPostRun(ctx, mv);
        // visible for tests
        if (mvTaskRunContext != null) {
            mvTaskRunContext.setPostRun(postRun);
        }
        context.setPostRun(postRun);
        if (StringUtils.isNotEmpty(postRun)) {
            ctx.executeSql(postRun);
        }
    }
}
