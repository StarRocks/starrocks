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

package com.starrocks.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockParams;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.HivePartitionDataInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.TableUpdateArbitrator;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.mv.MVPCTMetaRepairer;
import com.starrocks.scheduler.mv.MVPCTRefreshListPartitioner;
import com.starrocks.scheduler.mv.MVPCTRefreshNonPartitioner;
import com.starrocks.scheduler.mv.MVPCTRefreshPartitioner;
import com.starrocks.scheduler.mv.MVPCTRefreshPlanBuilder;
import com.starrocks.scheduler.mv.MVPCTRefreshRangePartitioner;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;

/**
 * Core logic of materialized view refresh task run
 * PartitionBasedMvRefreshProcessor is not thread safe for concurrent runs of the same materialized view
 */
public class PartitionBasedMvRefreshProcessor extends BaseTaskRunProcessor {

    private static final Logger LOG = LogManager.getLogger(PartitionBasedMvRefreshProcessor.class);
    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);

    public static final String MV_ID = "mvId";
    // session.enable_spill
    public static final String MV_SESSION_ENABLE_SPILL =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.ENABLE_SPILL;
    // session.query_timeout
    public static final String MV_SESSION_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.QUERY_TIMEOUT;
    // default query timeout for mv: 1 hour
    private static final int MV_DEFAULT_QUERY_TIMEOUT = 3600;


    private Database db;
    private MaterializedView materializedView;
    private MvTaskRunContext mvContext;

    // Collect all bases tables of the materialized view to be updated meta after mv refresh success.
    // format :     table id -> <base table info, snapshot table>
    private Map<Long, TableSnapshotInfo> snapshotBaseTables = Maps.newHashMap();

    private long oldTransactionVisibleWaitTimeout;

    @VisibleForTesting
    private RuntimeProfile runtimeProfile;
    private MVPCTRefreshPartitioner mvRefreshPartitioner;

    // represents the refresh job final job status
    public enum RefreshJobStatus {
        SUCCESS,
        FAILED,
        EMPTY,
    }

    // for testing
    private TaskRun nextTaskRun = null;

    public PartitionBasedMvRefreshProcessor() {
    }

    @VisibleForTesting
    public MvTaskRunContext getMvContext() {
        return mvContext;
    }

    @VisibleForTesting
    public void setMvContext(MvTaskRunContext mvContext) {
        this.mvContext = mvContext;
    }

    @VisibleForTesting
    public RuntimeProfile getRuntimeProfile() {
        return runtimeProfile;
    }

    public TaskRun getNextTaskRun() {
        return nextTaskRun;
    }

    // Core logics:
    // 1. prepare to check some conditions
    // 2. sync partitions with base tables(add or drop partitions, which will be optimized  by dynamic partition creation later)
    // 3. decide which partitions of materialized view to refresh and the corresponding base tables' source partitions
    // 4. construct the refresh sql and execute it
    // 5. update the source table version map if refresh task completes successfully
    @Override
    public void processTaskRun(TaskRunContext context) {
        // register tracers
        Tracers.register(context.getCtx());
        // init to collect the base timer for refresh profile
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.BASE, true, false);

        IMaterializedViewMetricsEntity mvEntity = null;
        ConnectContext connectContext = context.getCtx();
        QueryMaterializationContext queryMVContext = new QueryMaterializationContext();
        connectContext.setQueryMVContext(queryMVContext);
        try {
            // prepare
            try (Timer ignored = Tracers.watchScope("MVRefreshPrepare")) {
                prepare(context);
            }

            // do refresh
            try (Timer ignored = Tracers.watchScope("MVRefreshDoWholeRefresh")) {
                // refresh mv
                Preconditions.checkState(materializedView != null);
                mvEntity = MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(materializedView.getMvId());
                RefreshJobStatus status = doMvRefresh(context, mvEntity);
                // update metrics
                mvEntity.increaseRefreshJobStatus(status);
                connectContext.getState().setOk();
            }
        } catch (Exception e) {
            if (mvEntity != null) {
                mvEntity.increaseRefreshJobStatus(RefreshJobStatus.FAILED);
            }
            connectContext.getState().setError(e.getMessage());
            throw e;
        } finally {
            // reset query mv context to avoid affecting other tasks
            queryMVContext.clear();
            connectContext.setQueryMVContext(null);

            if (FeConstants.runningUnitTest) {
                runtimeProfile = new RuntimeProfile();
                Tracers.toRuntimeProfile(runtimeProfile);
            }

            Tracers.close();
            postProcess();
        }
    }

    /**
     * Sync partitions of base tables and check whether they are changing anymore
     */
    private boolean syncAndCheckPartitions(TaskRunContext context,
                                           IMaterializedViewMetricsEntity mvEntity) throws AnalysisException {
        // collect partition infos of ref base tables
        LOG.info("start to sync and check partitions for mv: {}", materializedView.getName());
        int retryNum = 0;
        boolean checked = false;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (!checked && retryNum++ < Config.max_mv_check_base_table_change_retry_times) {
            mvEntity.increaseRefreshRetryMetaCount(1L);
            try (Timer ignored = Tracers.watchScope("MVRefreshExternalTable")) {
                // refresh external table meta cache before sync partitions
                refreshExternalTable(context);
            }

            try (Timer ignored = Tracers.watchScope("MVRefreshSyncPartitions")) {
                // sync partitions between materialized view and base tables out of lock
                // do it outside lock because it is a time-cost operation
                syncPartitions();
            }

            try (Timer ignored = Tracers.watchScope("MVRefreshCheckBaseTableChange")) {
                // check whether there are partition changes for base tables, eg: partition rename
                // retry to sync partitions if any base table changed the partition infos
                if (checkBaseTablePartitionChange(materializedView)) {
                    LOG.info("materialized view:{} base partition has changed. retry to sync partitions, retryNum:{}",
                            materializedView.getName(), retryNum);
                    // sleep 100ms
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    continue;
                }
            }
            checked = true;
        }
        Tracers.record("MVRefreshSyncPartitionsRetryTimes", String.valueOf(retryNum));

        LOG.info("materialized view {} after checking partitions change {} times: {}, costs: {} ms",
                materializedView.getName(), retryNum, checked, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return checked;
    }

    private Set<String> checkMvToRefreshedPartitions(TaskRunContext context) throws AnalysisException {
        Set<String> mvToRefreshedPartitions = null;
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(db, materializedView, LockType.READ, Config.mv_refresh_try_lock_timeout_ms,
                TimeUnit.MILLISECONDS)) {
            LOG.warn("Failed to lock database: {} in checkMvToRefreshedPartitions for mv refresh {}", db.getFullName(),
                    materializedView.getName());
            throw new LockTimeoutException("Failed to lock database: " + db.getFullName());
        }
        try {
            Set<String> mvPotentialPartitionNames = Sets.newHashSet();
            mvToRefreshedPartitions = getPartitionsToRefreshForMaterializedView(context.getProperties(),
                    mvPotentialPartitionNames);
            if (mvToRefreshedPartitions.isEmpty()) {
                LOG.info("no partitions to refresh for materialized view {}", materializedView.getName());
                return mvToRefreshedPartitions;
            }

            // Only refresh the first partition refresh number partitions, other partitions will generate new tasks
            filterPartitionByRefreshNumber(mvToRefreshedPartitions, mvPotentialPartitionNames, materializedView);
            int partitionRefreshNumber = materializedView.getTableProperty().getPartitionRefreshNumber();
            LOG.info("Filter partitions to refresh for materialized view {}, partitionRefreshNumber={}, " +
                            "partitionsToRefresh:{}, mvPotentialPartitionNames:{}, next start:{}, next end:{}",
                    materializedView.getName(), partitionRefreshNumber, mvToRefreshedPartitions, mvPotentialPartitionNames,
                    mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd());
        } finally {
            locker.unLockTableWithIntensiveDbLock(db, materializedView, LockType.READ);
        }
        return mvToRefreshedPartitions;
    }

    private RefreshJobStatus doMvRefresh(TaskRunContext context, IMaterializedViewMetricsEntity mvEntity) {
        long startRefreshTs = System.currentTimeMillis();

        // refresh materialized view
        RefreshJobStatus result = doRefreshMaterializedViewWithRetry(context, mvEntity);

        // do not generate next task run if the current task run is killed
        if (mvContext.hasNextBatchPartition() && !mvContext.getTaskRun().isKilled()) {
            generateNextTaskRun();
        }

        long refreshDurationMs = System.currentTimeMillis() - startRefreshTs;
        LOG.info("Refresh {} success, cost time(s): {}", materializedView.getName(),
                DebugUtil.DECIMAL_FORMAT_SCALE_3.format(refreshDurationMs / 1000.0));
        mvEntity.updateRefreshDuration(refreshDurationMs);
        return result;
    }

    private void logMvToRefreshInfoIntoTaskRun(Set<String> finalMvToRefreshedPartitions,
                                               Map<String, Set<String>> finalRefTablePartitionNames) {
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setMvPartitionsToRefresh(finalMvToRefreshedPartitions);
            extraMessage.setRefBasePartitionsToRefreshMap(finalRefTablePartitionNames);
        });
    }

    /**
     * Retry the `doRefreshMaterializedView` method to avoid insert fails in occasional cases.
     */
    private RefreshJobStatus doRefreshMaterializedViewWithRetry(TaskRunContext taskRunContext,
                                                                IMaterializedViewMetricsEntity mvEntity) throws DmlException {
        // Use current connection variables instead of mvContext's session variables to be better debug.
        int maxRefreshMaterializedViewRetryNum = getMaxRefreshMaterializedViewRetryNum(taskRunContext.getCtx());
        LOG.info("start to refresh mv:{} with retry times:{}, try lock failure retry times:{}",
                materializedView.getName(), maxRefreshMaterializedViewRetryNum,
                Config.max_mv_refresh_try_lock_failure_retry_times);

        Throwable lastException = null;
        int lockFailedTimes = 0;
        int refreshFailedTimes = 0;
        while (refreshFailedTimes < maxRefreshMaterializedViewRetryNum &&
                lockFailedTimes < Config.max_mv_refresh_try_lock_failure_retry_times) {
            try {
                Tracers.record("MVRefreshRetryTimes", String.valueOf(refreshFailedTimes));
                Tracers.record("MVRefreshLockRetryTimes", String.valueOf(lockFailedTimes));
                return doRefreshMaterializedView(taskRunContext, mvEntity);
            } catch (LockTimeoutException e) {
                // if lock timeout, retry to refresh
                lockFailedTimes += 1;
                LOG.warn("Refresh materialized view {} failed at {}th time because try lock failed: {}",
                        this.materializedView.getName(), lockFailedTimes, ExceptionUtils.getStackTrace(e));
                lastException = e;
            } catch (Throwable e) {
                refreshFailedTimes += 1;
                LOG.warn("Refresh materialized view {} failed at {}th time: {}",
                        this.materializedView.getName(), refreshFailedTimes, ExceptionUtils.getStackTrace(e));
                lastException = e;
            }

            // sleep some time if it is not the last retry time
            Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        }

        // throw the last exception if all retries failed
        Preconditions.checkState(lastException != null);
        String errorMsg = lastException.getMessage();
        if (lastException instanceof NullPointerException) {
            errorMsg = ExceptionUtils.getStackTrace(lastException);
        }
        // field ERROR_MESSAGE in information_schema.task_runs length is 65535
        errorMsg = errorMsg.length() > MAX_FIELD_VARCHAR_LENGTH ?
                errorMsg.substring(0, MAX_FIELD_VARCHAR_LENGTH) : errorMsg;
        throw new DmlException("Refresh materialized view %s failed after retrying %s times(try-lock %s times), error-msg : " +
                "%s", lastException, this.materializedView.getName(), refreshFailedTimes, lockFailedTimes, errorMsg);
    }

    private static int getMaxRefreshMaterializedViewRetryNum(ConnectContext currConnectCtx) {
        int maxRefreshMaterializedViewRetryNum = 1;
        if (currConnectCtx != null && currConnectCtx.getSessionVariable() != null) {
            maxRefreshMaterializedViewRetryNum =
                    currConnectCtx.getSessionVariable().getQueryDebugOptions().getMaxRefreshMaterializedViewRetryNum();
            if (maxRefreshMaterializedViewRetryNum <= 0) {
                maxRefreshMaterializedViewRetryNum = 1;
            }
        }
        maxRefreshMaterializedViewRetryNum = Math.max(Config.max_mv_refresh_failure_retry_times,
                maxRefreshMaterializedViewRetryNum);
        return maxRefreshMaterializedViewRetryNum;
    }

    private RefreshJobStatus doRefreshMaterializedView(TaskRunContext context,
                                                       IMaterializedViewMetricsEntity mvEntity) throws Exception {

        ///// 1. check to refresh partition names of base tables and materialized view
        try (Timer ignored = Tracers.watchScope("MVRefreshSyncAndCheckPartitions")) {
            if (!syncAndCheckPartitions(context, mvEntity)) {
                throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                        materializedView.getName()));
            }
        }

        Set<String> mvToRefreshedPartitions = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshSyncAndCheckPartitions")) {
            mvToRefreshedPartitions = checkMvToRefreshedPartitions(context);
            if (Objects.isNull(mvToRefreshedPartitions) || mvToRefreshedPartitions.isEmpty()) {
                return RefreshJobStatus.EMPTY;
            }
        }
        // ref table of materialized view : refreshed partition names
        Map<TableSnapshotInfo, Set<String>> refTableRefreshPartitions = getRefTableRefreshPartitions(mvToRefreshedPartitions);
        // ref table of materialized view : refreshed partition names
        Map<String, Set<String>> refTablePartitionNames = refTableRefreshPartitions.entrySet().stream()
                .collect(Collectors.toMap(x -> x.getKey().getName(), Map.Entry::getValue));
        LOG.info("materialized:{}, mvToRefreshedPartitions:{}, refTableRefreshPartitions:{}",
                materializedView.getName(), mvToRefreshedPartitions, refTableRefreshPartitions);
        // add a message into information_schema
        logMvToRefreshInfoIntoTaskRun(mvToRefreshedPartitions, refTablePartitionNames);
        updateBaseTablePartitionSnapshotInfos(refTableRefreshPartitions);

        ///// 2. execute the ExecPlan of insert stmt
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshPrepareRefreshPlan")) {
            insertStmt = prepareRefreshPlan(mvToRefreshedPartitions, refTablePartitionNames);
        }
        try (Timer ignored = Tracers.watchScope("MVRefreshMaterializedView")) {
            refreshMaterializedView(mvContext, mvContext.getExecPlan(), insertStmt);
        }

        ///// 3. insert execute successfully, update the meta of materialized view according to ExecPlan
        try (Timer ignored = Tracers.watchScope("MVRefreshUpdateMeta")) {
            updateMeta(mvToRefreshedPartitions, mvContext.getExecPlan(), refTableRefreshPartitions);
        }

        return RefreshJobStatus.SUCCESS;
    }

    /**
     * Prepare the statement and plan for mv refreshing, considering the partitions of ref table
     */
    private InsertStmt prepareRefreshPlan(Set<String> mvToRefreshedPartitions,
                                          Map<String, Set<String>> refTablePartitionNames) throws AnalysisException {
        // 1. Prepare context
        ConnectContext ctx = mvContext.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase())
                .setWarehouse(ctx.getCurrentWarehouseName());

        // 2. Prepare variables
        changeDefaultConnectContextIfNeeded(ctx);

        // 3. AST
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshParser")) {
            insertStmt = generateInsertAst(mvToRefreshedPartitions, materializedView, ctx);
        }

        PlannerMetaLocker locker = new PlannerMetaLocker(ctx, insertStmt);
        ExecPlan execPlan = null;
        if (!locker.tryLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database in prepareRefreshPlan");
        }

        MVPCTRefreshPlanBuilder planBuilder = new MVPCTRefreshPlanBuilder(materializedView, mvContext, mvRefreshPartitioner);
        try {
            // 4. Analyze and prepare a partition & Rebuild insert statement by
            // considering to-refresh partitions of ref tables/ mv
            try (Timer ignored = Tracers.watchScope("MVRefreshAnalyzer")) {
                insertStmt = planBuilder.analyzeAndBuildInsertPlan(insertStmt, refTablePartitionNames, ctx);
                // Must set execution id before StatementPlanner.plan
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
            }

            // 5. generate insert stmt's exec plan, make thread local ctx existed
            try (ConnectContext.ScopeGuard guard = ctx.bindScope(); Timer ignored = Tracers.watchScope("MVRefreshPlanner")) {
                execPlan = StatementPlanner.planInsertStmt(locker, insertStmt, ctx);
            }
        } finally {
            locker.unlock();
        }

        QueryDebugOptions debugOptions = ctx.getSessionVariable().getQueryDebugOptions();
        // log the final mv refresh plan for each refresh for better trace and debug
        if (LOG.isDebugEnabled() || debugOptions.isEnableQueryTraceLog()) {
            LOG.info("MV Refresh Final Plan" +
                            "\nMV: {}" +
                            "\nMV PartitionsToRefresh: {}" +
                            "\nBase PartitionsToScan: {}" +
                            "\nInsert Plan:\n{}",
                    materializedView.getName(),
                    String.join(",", mvToRefreshedPartitions), refTablePartitionNames,
                    execPlan != null ? execPlan.getExplainString(StatementBase.ExplainLevel.VERBOSE) : "");
        } else {
            LOG.info("MV Refresh Final Plan, mv: {}, MV PartitionsToRefresh: {}, Base PartitionsToScan: {}",
                    materializedView.getName(), String.join(",", mvToRefreshedPartitions), refTablePartitionNames);
        }
        mvContext.setExecPlan(execPlan);
        LOG.info("prepared refresh plan for mv:{}", materializedView.getName());
        return insertStmt;
    }

    /**
     * Change default connect context when for mv refresh this is because:
     * - MV Refresh may take much resource to load base tables' data into the final materialized view.
     * - Those changes are set by default and also able to be changed by users for their needs.
     *
     * @param mvConnectCtx
     */
    private void changeDefaultConnectContextIfNeeded(ConnectContext mvConnectCtx) {
        // add resource group if resource group is enabled
        TableProperty mvProperty = materializedView.getTableProperty();
        SessionVariable mvSessionVariable = mvConnectCtx.getSessionVariable();
        if (mvSessionVariable.isEnableResourceGroup()) {
            String rg = ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME;
            if (mvProperty != null && !Strings.isNullOrEmpty(mvProperty.getResourceGroup())) {
                rg = mvProperty.getResourceGroup();
            }
            mvSessionVariable.setResourceGroup(rg);
        }

        // enable spill by default for mv if spill is not set by default and
        // `session.enable_spill` session variable is not set.
        if (Config.enable_materialized_view_spill &&
                !mvSessionVariable.isEnableSpill() &&
                !mvProperty.getProperties().containsKey(MV_SESSION_ENABLE_SPILL)) {
            mvSessionVariable.setEnableSpill(true);
        }

        // change `query_timeout` to 1 hour by default for better user experience.
        if (!mvProperty.getProperties().containsKey(MV_SESSION_TIMEOUT)) {
            mvSessionVariable.setQueryTimeoutS(MV_DEFAULT_QUERY_TIMEOUT);
        }

        // set enable_insert_strict by default
        if (!isMVPropertyContains(SessionVariable.ENABLE_INSERT_STRICT)) {
            mvSessionVariable.setEnableInsertStrict(false);
        }
        // enable profile by default for mv refresh task
        if (!isMVPropertyContains(SessionVariable.ENABLE_PROFILE)) {
            mvSessionVariable.setEnableProfile(true);
        }
    }

    private boolean isMVPropertyContains(String key) {
        String mvKey = PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + key;
        return materializedView.getTableProperty().getProperties().containsKey(mvKey);
    }

    private void postProcess() {
        mvContext.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(oldTransactionVisibleWaitTimeout);
    }

    public MVTaskRunExtraMessage getMVTaskRunExtraMessage() {
        if (this.mvContext.status == null) {
            return null;
        }
        return this.mvContext.status.getMvTaskRunExtraMessage();
    }

    @VisibleForTesting
    public void filterPartitionByRefreshNumber(Set<String> partitionsToRefresh,
                                               Set<String> mvPotentialPartitionNames,
                                               MaterializedView materializedView) {
        // refresh all partition when it's a sync refresh, otherwise updated partitions may be lost.
        if (mvContext.executeOption != null && mvContext.executeOption.getIsSync()) {
            return;
        }
        // ignore if mv is not partitioned.
        if (!materializedView.isPartitionedTable()) {
            return;
        }
        // ignore if partition_fresh_limit is not set
        int partitionRefreshNumber = materializedView.getTableProperty().getPartitionRefreshNumber();
        if (partitionRefreshNumber <= 0) {
            return;
        }

        // do filter actions
        mvRefreshPartitioner.filterPartitionByRefreshNumber(partitionsToRefresh, mvPotentialPartitionNames);
    }

    private void generateNextTaskRun() {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Map<String, String> properties = mvContext.getProperties();
        long mvId = Long.parseLong(properties.get(MV_ID));
        String taskName = TaskBuilder.getMvTaskName(mvId);
        Map<String, String> newProperties = Maps.newHashMap();
        for (Map.Entry<String, String> proEntry : properties.entrySet()) {
            if (proEntry.getValue() != null) {
                newProperties.put(proEntry.getKey(), proEntry.getValue());
            }
        }
        newProperties.put(TaskRun.PARTITION_START, mvContext.getNextPartitionStart());
        newProperties.put(TaskRun.PARTITION_END, mvContext.getNextPartitionEnd());
        if (mvContext.getStatus() != null) {
            newProperties.put(TaskRun.START_TASK_RUN_ID, mvContext.getStatus().getStartTaskRunId());
        }
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setNextPartitionStart(mvContext.getNextPartitionStart());
            extraMessage.setNextPartitionEnd(mvContext.getNextPartitionEnd());
        });

        // Partition refreshing task run should have the HIGHEST priority, and be scheduled before other tasks
        // Otherwise this round of partition refreshing would be staved and never got finished
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.HIGHEST.value(), true, newProperties);
        LOG.info("[MV] Generate a task to refresh next batches of partitions for MV {}-{}, start={}, end={}",
                materializedView.getName(), materializedView.getId(),
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd());

        if (properties.containsKey(TaskRun.IS_TEST) && properties.get(TaskRun.IS_TEST).equalsIgnoreCase("true")) {
            // for testing
            TaskRun taskRun = TaskRunBuilder
                    .newBuilder(taskManager.getTask(taskName))
                    .properties(option.getTaskRunProperties())
                    .setExecuteOption(option)
                    .build();
            nextTaskRun = taskRun;
        } else {
            taskManager.executeTask(taskName, option);
        }
    }

    private void refreshExternalTable(TaskRunContext context) {
        List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
        // use it if refresh external table fails
        ConnectContext connectContext = context.getCtx();
        List<Pair<Table, BaseTableInfo>> toRepairTables = new ArrayList<>();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Optional<Database> dbOpt = GlobalStateMgr.getCurrentState().getMetadataMgr().getDatabase(baseTableInfo);
            if (dbOpt.isEmpty()) {
                LOG.warn("database {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getDbInfoStr(), materializedView.getName());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }

            Optional<Table> optTable = MvUtils.getTable(baseTableInfo);
            if (optTable.isEmpty()) {
                LOG.warn("table {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getTableInfoStr(), materializedView.getName());
                materializedView.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            // refresh old table
            Table table = optTable.get();
            if (table.isNativeTableOrMaterializedView() || table.isConnectorView()) {
                LOG.info("No need to refresh table:{} because it is native table or materialized view or connector view",
                        baseTableInfo.getTableInfoStr());
                continue;
            }
            connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                    baseTableInfo.getDbName(), table, Lists.newArrayList(), true);
            // should clear query cache
            connectContext.getGlobalStateMgr().getMetadataMgr().removeQueryMetadata();

            // check new table
            Optional<Table> optNewTable = MvUtils.getTable(baseTableInfo);
            if (optNewTable.isEmpty()) {
                LOG.warn("table {} does not exist after refreshing materialized view:{}",
                        baseTableInfo.getTableInfoStr(), materializedView.getName());
                materializedView.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            Table newTable = optNewTable.get();
            if ((newTable instanceof HiveTable)
                    && ((HiveTable) newTable).getHiveTableType() == HiveTable.HiveTableType.EXTERNAL_TABLE) {
                toRepairTables.add(Pair.create(newTable, baseTableInfo));
            } else {
                LOG.info("Table:{} is not an hive external table, no need to repair",
                        baseTableInfo.getTableInfoStr());
            }
        }

        // do repair if needed
        if (!toRepairTables.isEmpty()) {
            MVPCTMetaRepairer repairer = new MVPCTMetaRepairer(db, materializedView);
            for (Pair<Table, BaseTableInfo> pair : toRepairTables) {
                repairer.repairTableIfNeeded(pair.first, pair.second);
            }
        }
    }

    /**
     * After materialized view is refreshed, update materialized view's meta info to record history refreshes.
     *
     * @param refTableAndPartitionNames : refreshed base table and its partition names mapping.
     */
    private void updateMeta(Set<String> mvRefreshedPartitions,
                            ExecPlan execPlan,
                            Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        LOG.info("start to update meta for mv:{}", materializedView.getName());

        // check
        Table mv = db.getTable(materializedView.getId());
        if (mv == null) {
            throw new DmlException(
                    "update meta failed. materialized view:" + materializedView.getName() + " not exist");
        }
        // check
        if (mvRefreshedPartitions == null || refTableAndPartitionNames == null) {
            LOG.info("no partitions to refresh for materialized view {}, mvRefreshedPartitions:{}, " +
                            "refTableAndPartitionNames:{}", materializedView.getName(), mvRefreshedPartitions,
                    refTableAndPartitionNames);
            return;
        }

        // NOTE: update all base tables meta to be used in mv rewrite.
        Map<Boolean, List<TableSnapshotInfo>> snapshotInfoSplits = snapshotBaseTables.values()
                .stream()
                .collect(Collectors.partitioningBy(s -> s.getBaseTable().isNativeTableOrMaterializedView()));
        Set<Long> refBaseTableIds = refTableAndPartitionNames.keySet().stream()
                .map(t -> t.getId())
                .collect(Collectors.toSet());

        Locker locker = new Locker();
        // update the meta if succeed
        if (!locker.lockDatabaseAndCheckExist(db, materializedView, LockType.WRITE)) {
            LOG.warn("Failed to lock database: {} in updateMeta for mv refresh {}", db.getFullName(),
                    materializedView.getName());
            throw new DmlException("update meta failed. database:" + db.getFullName() + " not exist");
        }

        try {
            MaterializedView.MvRefreshScheme mvRefreshScheme = materializedView.getRefreshScheme();
            MaterializedView.AsyncRefreshContext refreshContext = mvRefreshScheme.getAsyncRefreshContext();

            // update materialized view partition to ref base table partition names meta
            updateAssociatedPartitionMeta(refreshContext, mvRefreshedPartitions, refTableAndPartitionNames);

            if (snapshotInfoSplits.get(true) != null) {
                updateMetaForOlapTable(refreshContext, snapshotInfoSplits.get(true), refBaseTableIds);
            }
            if (snapshotInfoSplits.get(false) != null) {
                updateMetaForExternalTable(refreshContext, snapshotInfoSplits.get(false), refBaseTableIds);
            }
        } catch (Exception e) {
            LOG.warn("update final meta failed after mv refreshed:", e);
            throw e;
        } finally {
            locker.unLockTableWithIntensiveDbLock(db, materializedView, LockType.WRITE);
        }

        // update mv status message
        updateTaskRunStatus(status -> {
            try {
                MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
                Map<String, Set<String>> baseTableRefreshedPartitionsByExecPlan =
                        getBaseTableRefreshedPartitionsByExecPlan(execPlan);
                extraMessage.setBasePartitionsToRefreshMap(baseTableRefreshedPartitionsByExecPlan);
            } catch (Exception e) {
                // just log warn and no throw exceptions for an updating task runs message.
                LOG.warn("update task run messages failed:", e);
            }
        });
    }

    /**
     * Update materialized view partition to ref base table partition names meta, this is used in base table's partition
     * changes.
     * eg:
     * base table has dropped one partitioned, we can only drop the vesion map of associated materialized view's
     * partitions rather than the whole table.
     */
    private void updateAssociatedPartitionMeta(MaterializedView.AsyncRefreshContext refreshContext,
                                               Set<String> mvRefreshedPartitions,
                                               Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        Map<String, Map<Table, Set<String>>> mvToBaseNameRefs = mvContext.getMvRefBaseTableIntersectedPartitions();
        if (Objects.isNull(mvToBaseNameRefs) || Objects.isNull(refTableAndPartitionNames) ||
                refTableAndPartitionNames.isEmpty()) {
            return;
        }

        try {
            Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap =
                    refreshContext.getMvPartitionNameRefBaseTablePartitionMap();
            for (String mvRefreshedPartition : mvRefreshedPartitions) {
                Map<Table, Set<String>> mvToBaseNameRef = mvToBaseNameRefs.get(mvRefreshedPartition);
                for (TableSnapshotInfo snapshotInfo : refTableAndPartitionNames.keySet()) {
                    Table refBaseTable = snapshotInfo.getBaseTable();
                    if (!mvToBaseNameRef.containsKey(refBaseTable)) {
                        continue;
                    }
                    Set<String> realBaseTableAssociatedPartitions = Sets.newHashSet();
                    for (String refBaseTableAssociatedPartition : mvToBaseNameRef.get(refBaseTable)) {
                        realBaseTableAssociatedPartitions.addAll(
                                convertMVPartitionNameToRealPartitionName(refBaseTable,
                                        refBaseTableAssociatedPartition));
                    }
                    mvPartitionNameRefBaseTablePartitionMap
                            .put(mvRefreshedPartition, realBaseTableAssociatedPartitions);
                }
            }
        } catch (Exception e) {
            LOG.warn("Update materialized view {} with the associated ref base table partitions failed: ",
                    materializedView.getName(), e);
        }
    }

    private void updateMetaForOlapTable(MaterializedView.AsyncRefreshContext refreshContext,
                                        List<TableSnapshotInfo> changedTablePartitionInfos,
                                        Set<Long> refBaseTableIds) {
        LOG.info("update meta for mv {} with olap tables:{}, refBaseTableIds:{}", materializedView.getName(),
                changedTablePartitionInfos, refBaseTableIds);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableVisibleVersionMap();
        boolean hasNextPartitionToRefresh = mvContext.hasNextBatchPartition();
        // update version map of materialized view
        for (TableSnapshotInfo snapshotInfo : changedTablePartitionInfos) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            // Non-ref-base-tables should be update meta at the last refresh, otherwise it may
            // cause wrong results for rewrite or refresh.
            // eg:
            // tblA : partition table, has partitions: p0, p1, p2
            // tblB : non-partition table
            // MV: tblA a join tblB b on a.dt=b.dt
            // case: tblB has been updated,
            // run1: tblA(p0) + tblB, (X)
            // run2: tblA(p1) + tblB, (X)
            // run3: tblA(p2) + tblB, (Y)
            // In the run1/run2 should only update the tblA's partition info, but tblB's partition
            // info meta should be updated at the last refresh.
            if (hasNextPartitionToRefresh && !refBaseTableIds.contains(snapshotTable.getId())) {
                LOG.info("Skip update meta for olap base table {} with partitions info: {}, " +
                                "because it is not a ref base table of materialized view {}",
                        snapshotTable.getName(), snapshotInfo.getRefreshedPartitionInfos(), materializedView.getName());
                continue;
            }
            Long tableId = snapshotTable.getId();
            currentVersionMap.computeIfAbsent(tableId, (v) -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.get(tableId);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = snapshotInfo.getRefreshedPartitionInfos();
            LOG.info("Update materialized view {} meta for base table {} with partitions info: {}, old partition infos:{}",
                    materializedView.getName(), snapshotTable.getName(), partitionInfoMap, currentTablePartitionInfo);
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // FIXME: If base table's partition has been dropped, should drop the according version partition too?
            // remove partition info of not-exist partition for snapshot table from version map
            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                currentTablePartitionInfo.keySet().removeIf(partitionName ->
                        !snapshotOlapTable.getVisiblePartitionNames().contains(partitionName));
            }
        }
        if (!changedTablePartitionInfos.isEmpty()) {
            ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                    new ChangeMaterializedViewRefreshSchemeLog(materializedView);
            Collection<Map<String, MaterializedView.BasePartitionInfo>> allChangedPartitionInfos =
                    changedTablePartitionInfos
                            .stream()
                            .map(snapshot -> snapshot.getRefreshedPartitionInfos())
                            .collect(Collectors.toList());
            long maxChangedTableRefreshTime =
                    MvUtils.getMaxTablePartitionInfoRefreshTime(allChangedPartitionInfos);
            materializedView.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
            GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
        }
    }

    private void updateMetaForExternalTable(MaterializedView.AsyncRefreshContext refreshContext,
                                            List<TableSnapshotInfo> changedTablePartitionInfos,
                                            Set<Long> refBaseTableIds) {
        LOG.info("update meta for mv {} with external tables:{}, refBaseTableIds:{}", materializedView.getName(),
                changedTablePartitionInfos, refBaseTableIds);
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableInfoVisibleVersionMap();
        boolean hasNextBatchPartition = mvContext.hasNextBatchPartition();
        // update version map of materialized view
        for (TableSnapshotInfo snapshotInfo : changedTablePartitionInfos) {
            BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
            Table snapshotTable = snapshotInfo.getBaseTable();
            // Non-ref-base-tables should be update meta at the last refresh, otherwise it may
            // cause wrong results for rewrite or refresh.
            // eg:
            // tblA : partition table, has partitions: p0, p1, p2
            // tblB : non-partition table
            // MV: tblA a join tblB b on a.dt=b.dt
            // case: tblB has been updated,
            // run1: tblA(p0) + tblB, (X)
            // run2: tblA(p1) + tblB, (X)
            // run3: tblA(p2) + tblB, (Y)
            // In the run1/run2 should only update the tblA's partition info, but tblB's partition
            // info meta should be updated at the last refresh.
            if (hasNextBatchPartition && !refBaseTableIds.contains(snapshotTable.getId())) {
                LOG.info("Skip update meta for external base table {} with partitions info: {}, " +
                                "because it is not a ref base table of materialized view {}",
                        snapshotTable.getName(), snapshotInfo.getRefreshedPartitionInfos(), materializedView.getName());
                continue;
            }
            currentVersionMap.computeIfAbsent(baseTableInfo, (v) -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo = currentVersionMap.get(baseTableInfo);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = snapshotInfo.getRefreshedPartitionInfos();
            LOG.info("Update materialized view {} meta for external base table {} with partitions info: {}, " +
                            "old partition infos:{}", materializedView.getName(), snapshotTable.getName(),
                    partitionInfoMap, currentTablePartitionInfo);
            // overwrite old partition names
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // FIXME: If base table's partition has been dropped, should drop the according version partition too?
            // remove partition info of not-exist partition for snapshot table from version map
            Set<String> partitionNames = Sets.newHashSet(PartitionUtil.getPartitionNames(snapshotTable));
            currentTablePartitionInfo.keySet().removeIf(partitionName -> !partitionNames.contains(partitionName));
        }
        if (!changedTablePartitionInfos.isEmpty()) {
            ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                    new ChangeMaterializedViewRefreshSchemeLog(materializedView);
            Collection<Map<String, MaterializedView.BasePartitionInfo>> allChangedPartitionInfos =
                    changedTablePartitionInfos
                            .stream()
                            .map(snapshot -> snapshot.getRefreshedPartitionInfos())
                            .collect(Collectors.toList());
            long maxChangedTableRefreshTime = MvUtils.getMaxTablePartitionInfoRefreshTime(allChangedPartitionInfos);
            materializedView.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
            GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
        }
    }

    @VisibleForTesting
    public void prepare(TaskRunContext context) {
        Map<String, String> properties = context.getProperties();
        // NOTE: mvId is set in Task's properties when creating
        long mvId = Long.parseLong(properties.get(MV_ID));
        db = GlobalStateMgr.getCurrentState().getDb(context.ctx.getDatabase());
        if (db == null) {
            LOG.warn("database {} do not exist when refreshing materialized view:{}", context.ctx.getDatabase(), mvId);
            throw new DmlException("database " + context.ctx.getDatabase() + " do not exist.");
        }
        Table table = db.getTable(mvId);
        if (table == null) {
            LOG.warn("materialized view:{} in database:{} do not exist when refreshing", mvId,
                    context.ctx.getDatabase());
            throw new DmlException(String.format("materialized view:%s in database:%s do not exist when refreshing",
                    mvId, context.ctx.getDatabase()));
        }
        materializedView = (MaterializedView) table;

        // try to activate the mv before refresh
        if (!materializedView.isActive()) {
            MVActiveChecker.tryToActivate(materializedView);
            LOG.info("Activated the MV before refreshing: {}", materializedView.getName());
        }

        IMaterializedViewMetricsEntity mvEntity =
                MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(materializedView.getMvId());
        if (!materializedView.isActive()) {
            String errorMsg = String.format("Materialized view: %s/%d is not active due to %s.",
                    materializedView.getName(), mvId, materializedView.getInactiveReason());
            LOG.warn(errorMsg);
            mvEntity.increaseRefreshJobStatus(RefreshJobStatus.FAILED);
            throw new DmlException(errorMsg);
        }
        // wait util transaction is visible for mv refresh task
        // because mv will update base tables' visible version after insert, the mv's visible version
        // should keep up with the base tables, or it will return outdated result.
        oldTransactionVisibleWaitTimeout = context.ctx.getSessionVariable().getTransactionVisibleWaitTimeout();
        context.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(Long.MAX_VALUE / 1000);

        // Initialize status's job id which is used to track a batch of task runs.
        String jobId = properties.containsKey(TaskRun.START_TASK_RUN_ID) ?
                properties.get(TaskRun.START_TASK_RUN_ID) : context.getTaskRunId();
        if (context.status != null) {
            context.status.setStartTaskRunId(jobId);
        }

        // prepare mv context
        mvContext = new MvTaskRunContext(context);
        // prepare partition ttl number
        int partitionTTLNumber = materializedView.getTableProperty().getPartitionTTLNumber();
        mvContext.setPartitionTTLNumber(partitionTTLNumber);
        // prepare mv refresh partitioner
        this.mvRefreshPartitioner = buildMvRefreshPartitioner(materializedView, context);

        LOG.info("finish prepare refresh of mv:{}, mv name:{}, jobId: {}", mvId, materializedView.getName(), jobId);
    }

    /**
     * Create a mv refresh partitioner by the mv's partition info.
     */
    private MVPCTRefreshPartitioner buildMvRefreshPartitioner(MaterializedView mv, TaskRunContext context) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            return new MVPCTRefreshNonPartitioner(mvContext, context, db, mv);
        } else if (partitionInfo.isRangePartition()) {
            return new MVPCTRefreshRangePartitioner(mvContext, context, db, mv);
        } else if (partitionInfo.isListPartition()) {
            return new MVPCTRefreshListPartitioner(mvContext, context, db, mv);
        } else {
            throw new DmlException(String.format("materialized view:%s in database:%s refresh failed: partition info %s not " +
                    "supported", mv.getName(), context.ctx.getDatabase(), partitionInfo));
        }
    }

    /**
     * Sync base table's partition infos to be used later.
     */
    private boolean syncPartitions() throws AnalysisException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // collect base table snapshot infos
        snapshotBaseTables = collectBaseTableSnapshotInfos(materializedView);

        // prepare ref base table and partition column
        // NOTE: This should be after @refreshExternalTable since the base table may be changed and repaired again.
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();

        if (!partitionInfo.isUnPartitioned()) {
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
            Table refBaseTable = partitionTableAndColumn.first;
            if (!snapshotBaseTables.containsKey(refBaseTable.getId())) {
                throw new DmlException(String.format("The ref base table %s of materialized view %s is not existed in snapshot.",
                        refBaseTable.getName(), materializedView.getName()));
            }
            mvContext.setRefBaseTable(snapshotBaseTables.get(refBaseTable.getId()).getBaseTable());
            mvContext.setRefBaseTablePartitionColumn(partitionTableAndColumn.second);
        }

        // do sync partitions (add or drop partitions) for materialized view
        boolean result = mvRefreshPartitioner.syncAddOrDropPartitions();
        LOG.info("finish sync partitions. mv:{}, cost(ms): {}", materializedView.getName(),
                stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return result;
    }

    /**
     * TODO: merge it with {@code MaterializedView#getDirectTableAndPartitionColumn} but `getDirectTableAndPartitionColumn`
     * only use the existed base table info but {@code getRefBaseTableAndPartitionColumn} only uses the table name to keep
     * mv's partition and ref base bases' relation.
     * </p>
     * return the ref base table and column that materialized view's partition column
     * derives from if it exists, otherwise return null.
     */
    private Pair<Table, Column> getRefBaseTableAndPartitionColumn(Map<Long, TableSnapshotInfo> tableSnapshotInfos) {
        SlotRef slotRef = MaterializedView.getRefBaseTablePartitionSlotRef(materializedView);
        for (TableSnapshotInfo snapshotInfo : tableSnapshotInfos.values()) {
            BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
            Table table = snapshotInfo.getBaseTable();
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(baseTableInfo.getTableName())) {
                return Pair.create(table, table.getColumn(slotRef.getColumnName()));
            }
        }
        return Pair.create(null, null);
    }

    @VisibleForTesting
    public Set<String> getPartitionsToRefreshForMaterializedView(Map<String, String> properties,
                                                                 Set<String> mvPotentialPartitionNames)
            throws AnalysisException {
        String start = properties.get(TaskRun.PARTITION_START);
        String end = properties.get(TaskRun.PARTITION_END);
        boolean force = Boolean.parseBoolean(properties.get(TaskRun.FORCE));
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        Set<String> needRefreshMvPartitionNames = getPartitionsToRefreshForMaterializedView(partitionInfo,
                start, end, force, mvPotentialPartitionNames);

        // update mv extra message
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setForceRefresh(force);
            extraMessage.setPartitionStart(start);
            extraMessage.setPartitionEnd(end);
        });

        return needRefreshMvPartitionNames;
    }

    /**
     * Update task run status's extra message to add more information for information_schema if possible.
     *
     * @param action
     */
    private void updateTaskRunStatus(Consumer<TaskRunStatus> action) {
        if (this.mvContext.status != null) {
            action.accept(this.mvContext.status);
        }
    }

    /**
     * @param mvPartitionInfo : materialized view's partition info
     * @param start           : materialized view's refresh start in this task run
     * @param end             : materialized view's refresh end in this task run
     * @param force           : whether this task run is force or not
     * @return the partitions to refresh for materialized view
     * @throws AnalysisException
     */
    private Set<String> getPartitionsToRefreshForMaterializedView(
            PartitionInfo mvPartitionInfo, String start,
            String end, boolean force, Set<String> mvPotentialPartitionNames) throws AnalysisException {
        if (force && start == null && end == null) {
            // Force refresh
            int partitionTTLNumber = mvContext.getPartitionTTLNumber();
            return mvRefreshPartitioner.getMVPartitionsToRefreshWithForce(partitionTTLNumber);
        } else {
            return mvRefreshPartitioner.getMVPartitionsToRefresh(mvPartitionInfo, snapshotBaseTables,
                    start, end, force, mvPotentialPartitionNames);
        }
    }

    /**
     * Build an AST for insert stmt
     */
    private InsertStmt generateInsertAst(Set<String> materializedViewPartitions, MaterializedView materializedView,
                                         ConnectContext ctx) {
        // TODO: Support use mv when refreshing mv.
        ctx.getSessionVariable().setEnableMaterializedViewRewrite(false);
        String definition = mvContext.getDefinition();
        InsertStmt insertStmt =
                (InsertStmt) SqlParser.parse(definition, ctx.getSessionVariable()).get(0);
        // set target partitions
        insertStmt.setTargetPartitionNames(new PartitionNames(false, new ArrayList<>(materializedViewPartitions)));
        // insert overwrite mv must set system = true
        insertStmt.setSystem(true);
        // if materialized view has set sort keys, materialized view's output columns
        // may be different from the defined query's output.
        // so set materialized view's defined outputs as target columns.
        List<Integer> queryOutputIndexes = materializedView.getQueryOutputIndices();
        List<Column> baseSchema = materializedView.getBaseSchema();
        if (queryOutputIndexes != null && baseSchema.size() == queryOutputIndexes.size()) {
            List<String> targetColumnNames = queryOutputIndexes.stream()
                    .map(baseSchema::get)
                    .map(Column::getName)
                    .map(String::toLowerCase) // case insensitive
                    .collect(Collectors.toList());
            insertStmt.setTargetColumnNames(targetColumnNames);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generate refresh materialized view {} insert-overwrite statement, " +
                            "materialized view's target partition names:{}, " +
                            "materialized view's target columns: {}, " +
                            "definition:{}",
                    materializedView.getName(),
                    Joiner.on(",").join(materializedViewPartitions),
                    insertStmt.getTargetColumnNames() == null ? "" : Joiner.on(",").join(insertStmt.getTargetColumnNames()),
                    definition);
        }
        return insertStmt;
    }

    private boolean checkBaseTablePartitionHasChanged(TableSnapshotInfo snapshotInfo) {
        try {
            BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
            Table snapshotTable = snapshotInfo.getBaseTable();

            Optional<Table> optTable = MvUtils.getTableWithIdentifier(baseTableInfo);
            if (optTable.isEmpty()) {
                return true;
            }
            Table table = optTable.get();
            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapShotOlapTable = (OlapTable) snapshotTable;
                PartitionInfo snapshotPartitionInfo = snapShotOlapTable.getPartitionInfo();
                if (snapshotPartitionInfo.isUnPartitioned()) {
                    Set<String> partitionNames = ((OlapTable) table).getVisiblePartitionNames();
                    if (!snapShotOlapTable.getVisiblePartitionNames().equals(partitionNames)) {
                        // there is partition rename
                        return true;
                    }
                } else if (snapshotPartitionInfo.isListPartition()) {
                    OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                    Map<String, PListCell> snapshotPartitionMap = snapshotOlapTable.getListPartitionItems();
                    Map<String, PListCell> currentPartitionMap = snapshotOlapTable.getListPartitionItems();
                    if (ListPartitionDiffer.hasListPartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                } else {
                    Map<String, Range<PartitionKey>> snapshotPartitionMap = snapShotOlapTable.getRangePartitionMap();
                    Map<String, Range<PartitionKey>> currentPartitionMap = ((OlapTable) table).getRangePartitionMap();
                    if (SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                }
            } else if (ConnectorPartitionTraits.isSupported(snapshotTable.getType())) {
                if (snapshotTable.isUnPartitioned()) {
                    return false;
                } else {
                    PartitionInfo mvPartitionInfo = materializedView.getPartitionInfo();
                    // TODO: Support list partition later.
                    // do not need to check base partition table changed when mv is not partitioned
                    if (!(mvPartitionInfo.isRangePartition())) {
                        return false;
                    }
                    Pair<Table, Column> partitionTableAndColumn = materializedView.getDirectTableAndPartitionColumn();
                    Column partitionColumn = partitionTableAndColumn.second;
                    // TODO: need to consider(non ref-base table's change)
                    // For Non-partition based base table, it's not necessary to check the partition changed.
                    if (!snapshotTable.equals(partitionTableAndColumn.first)
                            || !snapshotTable.containColumn(partitionColumn.getName())) {
                        return false;
                    }
                    Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.getPartitionKeyRange(
                            snapshotTable, partitionColumn, MaterializedView.getPartitionExpr(materializedView));
                    Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.getPartitionKeyRange(
                            table, partitionColumn, MaterializedView.getPartitionExpr(materializedView));
                    if (SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                }
            }
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition change failed", e);
            return true;
        }
        return false;
    }

    /**
     * Check whether the base table's partition has changed or not. Wait to refresh until all mv's base tables
     * don't change again.
     *
     * @return: true if the base table's partition has changed, otherwise false.
     */
    private boolean checkBaseTablePartitionChange(MaterializedView materializedView) {
        LockParams lockParams = collectDatabases(materializedView);
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(lockParams,
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            LOG.warn("Failed to lock database: {} in checkBaseTablePartitionChange for mv refresh: {}",
                    lockParams, materializedView.getName());
            throw new LockTimeoutException("Failed to lock database: " + lockParams
                    + " in checkBaseTablePartitionChange");
        }
        // check snapshotBaseTables and current tables in catalog
        try {
            return snapshotBaseTables.values().stream().anyMatch(this::checkBaseTablePartitionHasChanged);
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
        }
    }

    @VisibleForTesting
    public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan, InsertStmt insertStmt)
            throws Exception {
        Preconditions.checkNotNull(execPlan);
        Preconditions.checkNotNull(insertStmt);
        ConnectContext ctx = mvContext.getCtx();

        if (mvContext.getTaskRun().isKilled()) {
            LOG.warn("[QueryId:{}] refresh materialized view {} is killed", ctx.getQueryId(),
                    materializedView.getName());
            throw new UserException("User Cancelled");
        }

        StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
        ctx.setExecutor(executor);
        if (ctx.getParent() != null && ctx.getParent().getExecutor() != null) {
            StmtExecutor parentStmtExecutor = ctx.getParent().getExecutor();
            parentStmtExecutor.registerSubStmtExecutor(executor);
        }
        ctx.setStmtId(STMT_ID_GENERATOR.incrementAndGet());
        LOG.info("[QueryId:{}] start to refresh materialized view {}", ctx.getQueryId(), materializedView.getName());
        try {
            executor.handleDMLStmtWithProfile(execPlan, insertStmt);
        } catch (Exception e) {
            LOG.warn("[QueryId:{}] refresh materialized view {} failed: {}", ctx.getQueryId(), materializedView.getName(), e);
            throw e;
        } finally {
            LOG.info("[QueryId:{}] finished to refresh materialized view {}", ctx.getQueryId(),
                    materializedView.getName());
            auditAfterExec(mvContext, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
        }
    }

    /**
     * Collect all deduplicated databases of the materialized view's base tables.
     *
     * @param materializedView: the materialized view to check
     * @return: the deduplicated databases of the materialized view's base tables,
     * throw exception if the database does not exist.
     */
    private LockParams collectDatabases(MaterializedView materializedView) {
        LockParams lockParams = new LockParams();
        for (BaseTableInfo baseTableInfo : materializedView.getBaseTableInfos()) {
            Optional<Database> dbOpt = GlobalStateMgr.getCurrentState().getMetadataMgr().getDatabase(baseTableInfo);
            if (dbOpt.isEmpty()) {
                LOG.warn("database {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getDbInfoStr(), materializedView.getName());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }
            Database db = dbOpt.get();
            lockParams.add(db, baseTableInfo.getTableId());
        }
        return lockParams;
    }

    /**
     * Collect all base table snapshot infos for the materialized view which the snapshot infos are kept and used in the final
     * update meta phase.
     * </p>
     * NOTE:
     * 1. deep copy of the base table's metadata may be time costing, we can optimize it later.
     * 2. no needs to lock the base table's metadata since the metadata is not changed during the refresh process.
     *
     * @param materializedView the materialized view to collect
     * @return the base table and its snapshot info map
     */
    @VisibleForTesting
    public Map<Long, TableSnapshotInfo> collectBaseTableSnapshotInfos(MaterializedView materializedView) {
        Map<Long, TableSnapshotInfo> tables = Maps.newHashMap();
        List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();

        LockParams lockParams = collectDatabases(materializedView);
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(lockParams, LockType.READ, Config.mv_refresh_try_lock_timeout_ms,
                TimeUnit.MILLISECONDS)) {
            LOG.warn("Failed to lock database: {} in collectBaseTableSnapshotInfos for mv refresh: {}",
                    lockParams, materializedView.getName());
            throw new LockTimeoutException("Failed to lock database: " + lockParams
                    + " in collectBaseTableSnapshotInfos");
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                Optional<Table> tableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
                if (tableOpt.isEmpty()) {
                    LOG.warn("table {} do not exist when refreshing materialized view:{}",
                            baseTableInfo.getTableInfoStr(), materializedView.getName());
                    throw new DmlException("Materialized view base table: %s not exist.",
                            baseTableInfo.getTableInfoStr());
                }
                Table table = tableOpt.get();
                if (table.isView()) {
                    // skip to collect snapshots for views
                } else if (table.isOlapTable()) {
                    OlapTable copied = DeepCopy.copyWithGson(table, OlapTable.class);
                    if (copied == null) {
                        throw new DmlException("Failed to copy olap table: %s", table.getName());
                    }
                    tables.put(table.getId(), new TableSnapshotInfo(baseTableInfo, copied));
                } else if (table.isOlapMaterializedView()) {
                    MaterializedView copied = DeepCopy.copyWithGson(table, MaterializedView.class);
                    if (copied == null) {
                        throw new DmlException("Failed to copy materialized view: %s", table.getName());
                    }
                    tables.put(table.getId(), new TableSnapshotInfo(baseTableInfo, copied));
                } else if (table.isCloudNativeTable()) {
                    LakeTable copied = DeepCopy.copyWithGson(table, LakeTable.class);
                    if (copied == null) {
                        throw new DmlException("Failed to copy lake table: %s", table.getName());
                    }
                    tables.put(table.getId(), new TableSnapshotInfo(baseTableInfo, copied));
                } else if (table.isCloudNativeMaterializedView()) {
                    LakeMaterializedView copied = DeepCopy.copyWithGson(table, LakeMaterializedView.class);
                    if (copied == null) {
                        throw new DmlException("Failed to copy lake materialized view: %s", table.getName());
                    }
                    tables.put(table.getId(), new TableSnapshotInfo(baseTableInfo, copied));
                } else {
                    // for other table types, use the table directly which needs to lock if visits the table metadata.
                    tables.put(table.getId(), new TableSnapshotInfo(baseTableInfo, table));
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
        }
        LOG.info("Collect base table snapshot infos for materialized view: {}, cost: {} ms",
                materializedView.getName(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return tables;
    }

    @VisibleForTesting
    public void updateBaseTablePartitionSnapshotInfos(Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        // NOTE: For each task run, ref-base table's incremental partition and all non-ref base tables' partitions
        // are refreshed, so we need record it into materialized view.
        // NOTE: We don't use the pruned partition infos from ExecPlan because the optimized partition infos are not
        // exact to describe which partitions are refreshed.
        Map<TableSnapshotInfo, Set<String>> baseTableAndPartitionNames = Maps.newHashMap();
        for (Map.Entry<TableSnapshotInfo, Set<String>> e : refTableAndPartitionNames.entrySet()) {
            TableSnapshotInfo snapshotInfo = e.getKey();
            Table baseTable = snapshotInfo.getBaseTable();
            Set<String> realPartitionNames = e.getValue().stream()
                    .flatMap(name -> convertMVPartitionNameToRealPartitionName(baseTable, name).stream())
                    .collect(Collectors.toSet());
            baseTableAndPartitionNames.put(snapshotInfo, realPartitionNames);
        }

        // Update all base tables' version map to track all tables' changes used in query rewrite.
        Map<TableSnapshotInfo, Set<String>> nonRefTableAndPartitionNames = getNonRefTableRefreshPartitions();
        if (!nonRefTableAndPartitionNames.isEmpty()) {
            baseTableAndPartitionNames.putAll(nonRefTableAndPartitionNames);
        }

        for (Map.Entry<TableSnapshotInfo, Set<String>> e : baseTableAndPartitionNames.entrySet()) {
            TableSnapshotInfo snapshotInfo = e.getKey();
            Set<String> refreshedPartitionNames = e.getValue();
            Map<String, MaterializedView.BasePartitionInfo> refreshedPartitionInfos =
                    getRefreshedPartitionInfos(snapshotInfo, refreshedPartitionNames);
            snapshotInfo.setRefreshedPartitionInfos(refreshedPartitionInfos);
        }
    }

    /**
     * For external table, the partition name is normalized which should convert it into original partition name.
     * <p>
     * For multi-partition columns, `refTableAndPartitionNames` is not fully exact to describe which partitions
     * of ref base table are refreshed, use `getSelectedPartitionInfosOfExternalTable` later if we can solve the multi
     * partition columns problem.
     * eg:
     * partitionName1 : par_col=0/par_date=2020-01-01 => p20200101
     * partitionName2 : par_col=1/par_date=2020-01-01 => p20200101
     */
    private Set<String> convertMVPartitionNameToRealPartitionName(Table table, String mvPartitionName) {
        if (!table.isNativeTableOrMaterializedView()) {
            Map<Table, Map<String, Set<String>>> refBaseTableRangePartitionMap
                    = mvContext.getExternalRefBaseTableMVPartitionMap();
            Preconditions.checkState(refBaseTableRangePartitionMap.containsKey(table));
            return refBaseTableRangePartitionMap.get(table).get(mvPartitionName);
        } else {
            return Sets.newHashSet(mvPartitionName);
        }
    }

    /**
     * @param mvToRefreshedPartitions :  to-refreshed materialized view partition names
     * @return : return to-refreshed base table's table name and partition names mapping
     */
    @VisibleForTesting
    public Map<TableSnapshotInfo, Set<String>> getRefTableRefreshPartitions(Set<String> mvToRefreshedPartitions) {
        Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames = Maps.newHashMap();
        Map<String, Map<Table, Set<String>>> mvToBaseNameRefs = mvContext.getMvRefBaseTableIntersectedPartitions();
        if (mvToBaseNameRefs == null) {
            return refTableAndPartitionNames;
        }
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            Set<String> needRefreshTablePartitionNames = null;
            for (String mvPartitionName : mvToRefreshedPartitions) {
                if (!mvToBaseNameRefs.containsKey(mvPartitionName)) {
                    continue;
                }
                Map<Table, Set<String>> mvToBaseNameRef = mvToBaseNameRefs.get(mvPartitionName);
                if (mvToBaseNameRef.containsKey(snapshotTable)) {
                    if (needRefreshTablePartitionNames == null) {
                        needRefreshTablePartitionNames = Sets.newHashSet();
                    }
                    // The table in this map has related partition with mv
                    // It's ok to add empty set for a table, means no partition corresponding to this mv partition
                    needRefreshTablePartitionNames.addAll(mvToBaseNameRef.get(snapshotTable));
                } else {
                    LOG.info("MV {}'s refTable {} is not found in `mvRefBaseTableIntersectedPartitions` " +
                            "because of empty update", materializedView.getName(), snapshotTable.getName());
                }
            }
            if (needRefreshTablePartitionNames != null) {
                refTableAndPartitionNames.put(snapshotInfo, needRefreshTablePartitionNames);
            }
        }
        return refTableAndPartitionNames;
    }

    /**
     * Return all non-ref base table and refreshed partitions.
     */
    private Map<TableSnapshotInfo, Set<String>> getNonRefTableRefreshPartitions() {
        Map<TableSnapshotInfo, Set<String>> tableNamePartitionNames = Maps.newHashMap();
        Map<Table, Map<String, Set<String>>> baseTableToMvNameRefs = mvContext.getRefBaseTableMVIntersectedPartitions();
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table table = snapshotInfo.getBaseTable();
            if (baseTableToMvNameRefs != null && baseTableToMvNameRefs.containsKey(table)) {
                // do nothing
            } else {
                if (table.isNativeTableOrMaterializedView()) {
                    tableNamePartitionNames.put(snapshotInfo, ((OlapTable) table).getVisiblePartitionNames());
                } else if (table.isView()) {
                    // do nothing
                } else {
                    tableNamePartitionNames.put(snapshotInfo, Sets.newHashSet(PartitionUtil.getPartitionNames(table)));
                }
            }
        }
        return tableNamePartitionNames;
    }

    /**
     * Collect base table and its refreshed partition infos based on refreshed table infos.
     */
    private Map<String, MaterializedView.BasePartitionInfo> getRefreshedPartitionInfos(
            TableSnapshotInfo snapshotInfo, Set<String> refreshedPartitionNames) {
        Table baseTable = snapshotInfo.getBaseTable();
        BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
        if (baseTable.isNativeTableOrMaterializedView()) {
            Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
            OlapTable olapTable = (OlapTable) baseTable;
            for (String partitionName : refreshedPartitionNames) {
                Partition partition = olapTable.getPartition(partitionName);
                // it's ok to skip because only existed partitinos are updated in the version map.
                if (partition == null) {
                    LOG.warn("Partition {} not found in base table {} in refreshing {}, refreshedPartitionNames:{}",
                            partitionName, baseTable.getName(), materializedView.getName(), refreshedPartitionNames);
                    continue;
                }
                MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                        partition.getId(), partition.getVisibleVersion(), partition.getVisibleVersionTime());
                partitionInfos.put(partition.getName(), basePartitionInfo);
            }
            LOG.info("Collect olap base table {}'s refreshed partition infos: {}", baseTable.getName(), partitionInfos);
            return partitionInfos;
        } else if (ConnectorPartitionTraits.isSupportPCTRefresh(baseTable.getType())) {
            return getSelectedPartitionInfos(baseTable, Lists.newArrayList(refreshedPartitionNames), baseTableInfo);
        } else {
            // FIXME: base table does not support partition-level refresh and does not update the meta
            //  in materialized view.
            LOG.warn("Refresh materialized view {} with non-supported-partition-level refresh base table {}",
                    materializedView.getName(), baseTable.getName());
            return Maps.newHashMap();
        }
    }

    /**
     * @param table                  : input table to collect refresh partition infos
     * @param selectedPartitionNames : input table refreshed partition names
     * @param baseTableInfo          : input table's base table info
     * @return : return the given table's refresh partition infos
     */
    private Map<String, MaterializedView.BasePartitionInfo> getSelectedPartitionInfos(Table table,
                                                                                      List<String> selectedPartitionNames,
                                                                                      BaseTableInfo baseTableInfo) {
        // sort selectedPartitionNames before the for loop, otherwise the order of partition names may be
        // different in selectedPartitionNames and partitions and will lead to infinite partition refresh.
        Collections.sort(selectedPartitionNames);
        Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
        List<com.starrocks.connector.PartitionInfo> partitions = GlobalStateMgr.
                getCurrentState().getMetadataMgr().getPartitions(baseTableInfo.getCatalogName(), table,
                        selectedPartitionNames);
        for (int index = 0; index < selectedPartitionNames.size(); ++index) {
            long modifiedTime = partitions.get(index).getModifiedTime();
            String partitionName = selectedPartitionNames.get(index);
            MaterializedView.BasePartitionInfo basePartitionInfo =
                    new MaterializedView.BasePartitionInfo(-1, modifiedTime, modifiedTime);
            TableUpdateArbitrator.UpdateContext updateContext = new TableUpdateArbitrator.UpdateContext(
                    table,
                    -1,
                    Lists.newArrayList(partitionName));
            if (table instanceof HiveTable
                    && ((HiveTable) table).getHiveTableType() == HiveTable.HiveTableType.EXTERNAL_TABLE) {
                TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
                if (arbitrator != null) {
                    Map<String, Optional<HivePartitionDataInfo>> partitionDataInfos = arbitrator.getPartitionDataInfos();
                    Preconditions.checkState(partitionDataInfos.size() == 1);
                    if (partitionDataInfos.get(partitionName).isPresent()) {
                        HivePartitionDataInfo hivePartitionDataInfo = partitionDataInfos.get(partitionName).get();
                        basePartitionInfo.setExtLastFileModifiedTime(hivePartitionDataInfo.getLastFileModifiedTime());
                        basePartitionInfo.setFileNumber(hivePartitionDataInfo.getFileNumber());
                    }
                }
            }
            partitionInfos.put(partitionName, basePartitionInfo);
        }
        return partitionInfos;
    }

    /**
     * Extract refreshed/scanned base table and its refreshed partition names
     * NOTE: this is used to trace in task_runs.
     */
    private Map<String, Set<String>> getBaseTableRefreshedPartitionsByExecPlan(
            ExecPlan execPlan) {
        Map<String, Set<String>> baseTableRefreshPartitionNames = Maps.newHashMap();
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        for (ScanNode scanNode : scanNodes) {
            if (scanNode instanceof OlapScanNode) {
                OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                OlapTable olapTable = olapScanNode.getOlapTable();
                if (olapScanNode.getSelectedPartitionNames() != null &&
                        !olapScanNode.getSelectedPartitionNames().isEmpty()) {
                    baseTableRefreshPartitionNames.put(olapTable.getName(),
                            new HashSet<>(olapScanNode.getSelectedPartitionNames()));
                } else {
                    List<Long> selectedPartitionIds = olapScanNode.getSelectedPartitionIds();
                    Set<String> selectedPartitionNames = selectedPartitionIds.stream()
                            .map(p -> olapTable.getPartition(p).getName())
                            .collect(Collectors.toSet());
                    baseTableRefreshPartitionNames.put(olapTable.getName(), selectedPartitionNames);
                }
            } else if (scanNode instanceof HdfsScanNode) {
                HdfsScanNode hdfsScanNode = (HdfsScanNode) scanNode;
                HiveTable hiveTable = (HiveTable) hdfsScanNode.getHiveTable();
                Optional<BaseTableInfo> baseTableInfoOptional = materializedView.getBaseTableInfos().stream().filter(
                                baseTableInfo -> baseTableInfo.getTableIdentifier().equals(hiveTable.getTableIdentifier())).
                        findAny();
                if (!baseTableInfoOptional.isPresent()) {
                    continue;
                }
                Set<String> selectedPartitionNames = Sets.newHashSet(
                        getSelectedPartitionNamesOfHiveTable(hiveTable, hdfsScanNode));
                baseTableRefreshPartitionNames.put(hiveTable.getName(), selectedPartitionNames);
            } else {
                // do nothing.
            }
        }
        return baseTableRefreshPartitionNames;
    }

    /**
     * Extract hive partition names from hdfs scan node.
     */
    private List<String> getSelectedPartitionNamesOfHiveTable(HiveTable hiveTable, HdfsScanNode hdfsScanNode) {
        List<String> partitionColumnNames = hiveTable.getPartitionColumnNames();
        List<String> selectedPartitionNames;
        if (hiveTable.isUnPartitioned()) {
            selectedPartitionNames = Lists.newArrayList(hiveTable.getTableName());
        } else {
            Collection<Long> selectedPartitionIds = hdfsScanNode.getScanNodePredicates().getSelectedPartitionIds();
            List<PartitionKey> selectedPartitionKey = Lists.newArrayList();
            for (Long selectedPartitionId : selectedPartitionIds) {
                selectedPartitionKey
                        .add(hdfsScanNode.getScanNodePredicates().getIdToPartitionKey().get(selectedPartitionId));
            }
            selectedPartitionNames = selectedPartitionKey.stream().map(partitionKey ->
                    PartitionUtil.toHivePartitionName(partitionColumnNames, partitionKey)).collect(Collectors.toList());
        }
        return selectedPartitionNames;
    }

    @VisibleForTesting
    public Map<Long, TableSnapshotInfo> getSnapshotBaseTables() {
        return snapshotBaseTables;
    }
}
