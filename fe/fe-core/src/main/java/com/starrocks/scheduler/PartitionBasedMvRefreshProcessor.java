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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.ConnectorTableInfo;
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
import com.starrocks.scheduler.mv.MVPCTRefreshPlanBuilder;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ListPartitionDiff;
import com.starrocks.sql.common.MvPartitionDiffResult;
import com.starrocks.sql.common.PartitionDiffer;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.catalog.MvRefreshArbiter.getMvBaseTableUpdateInfo;
import static com.starrocks.catalog.MvRefreshArbiter.needToRefreshTable;
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

    private static final int CREATE_PARTITION_BATCH_SIZE = 64;

    private Database db;
    private MaterializedView materializedView;
    private MvTaskRunContext mvContext;

    // Collect all bases tables of the materialized view to be updated meta after mv refresh success.
    // format :     table id -> <base table info, snapshot table>
    private Map<Long, TableSnapshotInfo> snapshotBaseTables = Maps.newHashMap();

    private long oldTransactionVisibleWaitTimeout;

    // represents the refresh job final job status
    public enum RefreshJobStatus {
        SUCCESS,
        FAILED,
        EMPTY,
    }

    // for testing
    private TaskRun nextTaskRun = null;

    @VisibleForTesting
    public MvTaskRunContext getMvContext() {
        return mvContext;
    }

    @VisibleForTesting
    public void setMvContext(MvTaskRunContext mvContext) {
        this.mvContext = mvContext;
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
    public void processTaskRun(TaskRunContext context) throws Exception {
        Tracers.register();

        prepare(context);

        Preconditions.checkState(materializedView != null);
        IMaterializedViewMetricsEntity mvEntity =
                MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(materializedView.getMvId());

        ConnectContext connectContext = context.getCtx();
        try {
            RefreshJobStatus status = doMvRefresh(context, mvEntity);
            mvEntity.increaseRefreshJobStatus(status);
            connectContext.getState().setOk();
        } catch (Exception e) {
            mvEntity.increaseRefreshJobStatus(RefreshJobStatus.FAILED);
            connectContext.getState().setError(e.getMessage());
            throw e;
        } finally {
            postProcess();
            Tracers.close();
        }
    }

    /**
     * Sync partitions of base tables and check whether they are changing anymore
     */
    private boolean syncAndCheckPartitions(TaskRunContext context, IMaterializedViewMetricsEntity mvEntity) {
        // collect partition infos of ref base tables
        LOG.info("start to sync and check partitions for mv: {}", materializedView.getName());
        int retryNum = 0;
        boolean checked = false;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (!checked && retryNum++ < Config.max_mv_check_base_table_change_retry_times) {
            mvEntity.increaseRefreshRetryMetaCount(1L);
            // refresh external table meta cache before sync partitions
            refreshExternalTable(context);
            // sync partitions between materialized view and base tables out of lock
            // do it outside lock because it is a time-cost operation
            syncPartitions(context);
            // check whether there are partition changes for base tables, eg: partition rename
            // retry to sync partitions if any base table changed the partition infos
            if (checkBaseTablePartitionChange(materializedView)) {
                LOG.info("materialized view:{} base partition has changed. retry to sync partitions, retryNum:{}",
                        materializedView.getName(), retryNum);
                // sleep 100ms
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                continue;
            }
            checked = true;
        }
        LOG.info("materialized view {} after checking partitions change {} times: {}, costs: {} ms",
                materializedView.getName(), retryNum, checked, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return checked;
    }

    private Set<String> checkMvToRefreshedPartitions(TaskRunContext context) throws AnalysisException {
        Set<String> mvToRefreshedPartitions = null;
        Locker locker = new Locker();
        if (!locker.tryLockDatabase(db, LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
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
            LOG.info("materialized view partitions to refresh:{}", mvToRefreshedPartitions);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
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
        LOG.info("start to refresh mv:{} with retry", materializedView.getName());
        // Use current connection variables instead of mvContext's session variables to be better debug.
        int maxRefreshMaterializedViewRetryNum = getMaxRefreshMaterializedViewRetryNum(taskRunContext.getCtx());

        Throwable lastException = null;
        int lockFailedTimes = 0;
        int refreshFailedTimes = 0;
        while (refreshFailedTimes < maxRefreshMaterializedViewRetryNum &&
                lockFailedTimes < Config.max_mv_refresh_try_lock_failure_retry_times) {
            try {
                return doRefreshMaterializedView(taskRunContext, mvEntity);
            } catch (LockTimeoutException e) {
                // if lock timeout, retry to refresh
                lockFailedTimes += 1;
                LOG.warn("Refresh materialized view {} failed at {}th time because try lock failed: {}",
                        this.materializedView.getName(), lockFailedTimes, e);
                lastException = e;
            } catch (Throwable e) {
                refreshFailedTimes += 1;
                LOG.warn("Refresh materialized view {} failed at {}th time: {}",
                        this.materializedView.getName(), refreshFailedTimes, e);
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
        if (!syncAndCheckPartitions(context, mvEntity)) {
            throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                    materializedView.getName()));
        }

        Set<String> mvToRefreshedPartitions = checkMvToRefreshedPartitions(context);
        if (Objects.isNull(mvToRefreshedPartitions) || mvToRefreshedPartitions.isEmpty()) {
            return RefreshJobStatus.EMPTY;
        }
        // ref table of materialized view : refreshed partition names
        Map<TableSnapshotInfo, Set<String>> refTableRefreshPartitions = getRefTableRefreshPartitions(mvToRefreshedPartitions);
        // ref table of materialized view : refreshed partition names
        Map<String, Set<String>> refTablePartitionNames = refTableRefreshPartitions.entrySet().stream()
                .collect(Collectors.toMap(x -> x.getKey().getBaseTable().getName(), Map.Entry::getValue));
        LOG.info("materialized view:{} source partitions :{}",
                materializedView.getName(), refTableRefreshPartitions);
        // add a message into information_schema
        logMvToRefreshInfoIntoTaskRun(mvToRefreshedPartitions, refTablePartitionNames);
        updateBaseTablePartitionSnapshotInfos(refTableRefreshPartitions);

        ///// 2. execute the ExecPlan of insert stmt
        InsertStmt insertStmt = prepareRefreshPlan(mvToRefreshedPartitions, refTablePartitionNames);
        refreshMaterializedView(mvContext, mvContext.getExecPlan(), insertStmt);

        ///// 3. insert execute successfully, update the meta of materialized view according to ExecPlan
        updateMeta(mvToRefreshedPartitions, mvContext.getExecPlan(), refTableRefreshPartitions);

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
                .setDb(ctx.getDatabase());

        // 2. Prepare variables
        changeDefaultConnectContextIfNeeded(ctx);

        // 3. AST
        InsertStmt insertStmt = generateInsertAst(mvToRefreshedPartitions, materializedView, ctx);

        PlannerMetaLocker locker = new PlannerMetaLocker(ctx, insertStmt);
        ExecPlan execPlan = null;
        if (!locker.tryLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database in prepareRefreshPlan");
        }

        MVPCTRefreshPlanBuilder planBuilder = new MVPCTRefreshPlanBuilder(materializedView, mvContext);
        try {
            // 4. Analyze and prepare partition & Rebuild insert statement by
            // considering to-refresh partitions of ref tables/ mv
            insertStmt = planBuilder.analyzeAndBuildInsertPlan(insertStmt, refTablePartitionNames, ctx);
            // Must set execution id before StatementPlanner.plan
            ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));

            // 5. generate insert stmt's exec plan, make thread local ctx existed
            try (var guard = ctx.bindScope()) {
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
            String rg = mvProperty.getResourceGroup();
            if (rg == null || rg.isEmpty()) {
                rg = ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME;
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
        Map<String, Range<PartitionKey>> rangePartitionMap = materializedView.getRangePartitionMap();
        if (partitionRefreshNumber >= rangePartitionMap.size()) {
            return;
        }

        Map<String, Range<PartitionKey>> mappedPartitionsToRefresh = Maps.newHashMap();
        for (String partitionName : partitionsToRefresh) {
            mappedPartitionsToRefresh.put(partitionName, rangePartitionMap.get(partitionName));
        }
        LinkedHashMap<String, Range<PartitionKey>> sortedPartition = mappedPartitionsToRefresh.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(RangeUtils.RANGE_COMPARATOR))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        Iterator<String> partitionNameIter = sortedPartition.keySet().iterator();
        String mvRefreshPartition = "";
        for (int i = 0; i < partitionRefreshNumber; i++) {
            if (partitionNameIter.hasNext()) {
                mvRefreshPartition = partitionNameIter.next();
            }

            // NOTE: if mv's need to refresh partitions in the many-to-many mappings, no need to filter to
            // avoid data lose.
            // eg:
            // ref table's partitions:
            //  p0:   [2023-07-27, 2023-07-30)
            //  p1:   [2023-07-30, 2023-08-02) X
            //  p2:   [2023-08-02, 2023-08-05)
            // materialized view's partition:
            //  p0:   [2023-07-01, 2023-08-01)
            //  p1:   [2023-08-01, 2023-09-01)
            //  p2:   [2023-09-01, 2023-10-01)
            //
            // If partitionRefreshNumber is 1, ref table's p1 has been updated, then mv's partition [p0, p1]
            // needs to be refreshed.
            // Run1: mv's p0, refresh will update ref-table's p1 into version mapping(since incremental refresh)
            // Run2: mv's p1, refresh check ref-table's p1 has been refreshed, skip to refresh.
            // BTW, since the refresh has already scanned the needed base tables' data, it's better to update
            // more mv's partitions as more as possible.
            // TODO: But it may cause much memory to refresh many partitions, support fine-grained partition refresh later.
            if (!mvPotentialPartitionNames.isEmpty() && mvPotentialPartitionNames.contains(mvRefreshPartition)) {
                return;
            }
        }
        String nextPartitionStart = null;
        String endPartitionName = null;
        if (partitionNameIter.hasNext()) {
            String startPartitionName = partitionNameIter.next();
            Range<PartitionKey> partitionKeyRange = mappedPartitionsToRefresh.get(startPartitionName);
            nextPartitionStart = AnalyzerUtils.parseLiteralExprToDateString(partitionKeyRange.lowerEndpoint(), 0);
            endPartitionName = startPartitionName;
            partitionsToRefresh.remove(endPartitionName);
        }
        while (partitionNameIter.hasNext()) {
            endPartitionName = partitionNameIter.next();
            partitionsToRefresh.remove(endPartitionName);
        }

        mvContext.setNextPartitionStart(nextPartitionStart);

        if (endPartitionName != null) {
            PartitionKey upperEndpoint = mappedPartitionsToRefresh.get(endPartitionName).upperEndpoint();
            mvContext.setNextPartitionEnd(AnalyzerUtils.parseLiteralExprToDateString(upperEndpoint, 0));
        } else {
            // partitionNameIter has just been traversed, and endPartitionName is not updated
            // will cause endPartitionName == null
            mvContext.setNextPartitionEnd(null);
        }
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

            Table table = optTable.get();
            if (!table.isNativeTableOrMaterializedView() && !table.isConnectorView()) {
                context.getCtx().getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, Lists.newArrayList(), true);
                // should clear query cache
                context.getCtx().getGlobalStateMgr().getMetadataMgr().removeQueryMetadata();
                Optional<Table> optNewTable = MvUtils.getTable(baseTableInfo);
                if (optTable.isEmpty()) {
                    LOG.warn("table {} does not exist after refreshing materialized view:{}",
                            baseTableInfo.getTableInfoStr(), materializedView.getName());
                    materializedView.setInactiveAndReason(
                            MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                    throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
                }
                Table newTable = optNewTable.get();
                if (!(newTable instanceof HiveTable)
                        || ((HiveTable) newTable).getHiveTableType() != HiveTable.HiveTableType.EXTERNAL_TABLE) {
                    continue;
                }
                if (!baseTableInfo.getTableIdentifier().equals(newTable.getTableIdentifier())) {
                    // table identifier changed, original table may be dropped and recreated
                    // consider auto refresh partition limit
                    // format: l_shipdate=1998-01-02
                    // consider __HIVE_DEFAULT_PARTITION__
                    LOG.info("base table:{} identifier has changed from:{} to:{}",
                            baseTableInfo.getTableName(), baseTableInfo.getTableIdentifier(), newTable.getTableIdentifier());
                    Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap =
                            materializedView.getBaseTableRefreshInfo(baseTableInfo);
                    if (partitionInfoMap == null || partitionInfoMap.isEmpty()) {
                        return;
                    }
                    boolean isAutoRefresh = materializedView.getRefreshScheme().isAsync();
                    int autoRefreshPartitionsLimit = -1;
                    if (isAutoRefresh) {
                        // only work for auto refresh
                        // for manual refresh, we respect the partition range specified by user
                        autoRefreshPartitionsLimit = materializedView.getTableProperty().getAutoRefreshPartitionsLimit();
                    }
                    List<String> partitionNames = Lists.newArrayList(partitionInfoMap.keySet());
                    TableUpdateArbitrator.UpdateContext updateContext = new TableUpdateArbitrator.UpdateContext(
                            newTable,
                            autoRefreshPartitionsLimit,
                            partitionNames);
                    TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
                    if (arbitrator == null) {
                        return;
                    }
                    Map<String, Optional<HivePartitionDataInfo>> partitionDataInfos = arbitrator.getPartitionDataInfos();
                    List<String> updatedPartitionNames =
                            getUpdatedPartitionNames(partitionNames, partitionInfoMap, partitionDataInfos);
                    LOG.info("try to get updated partitions names based on data." +
                                    " partitionNames:{}, isAutoRefresh:{}," +
                                    " autoRefreshPartitionsLimit:{}, updatedPartitionNames:{}",
                            partitionNames, isAutoRefresh, autoRefreshPartitionsLimit, updatedPartitionNames);
                    // if partition is not modified, change the last refresh time to update
                    repairMvBaseTableMeta(materializedView, baseTableInfo, newTable, updatedPartitionNames);
                }
            }
        }
    }

    private List<String> getUpdatedPartitionNames(
            List<String> partitionNames,
            Map<String, MaterializedView.BasePartitionInfo> tablePartitionInfoMap,
            Map<String, Optional<HivePartitionDataInfo>> partitionDataInfos) {
        List<String> updatedPartitionNames = Lists.newArrayList();
        for (int i = 0; i < partitionNames.size(); i++) {
            String partitionName = partitionNames.get(i);
            if (!partitionDataInfos.containsKey(partitionName)) {
                continue;
            }
            MaterializedView.BasePartitionInfo basePartitionInfo = tablePartitionInfoMap.get(partitionName);
            Optional<HivePartitionDataInfo> partitionDataInfoOptional = partitionDataInfos.get(partitionName);
            if (partitionDataInfoOptional.isEmpty()) {
                updatedPartitionNames.add(partitionNames.get(i));
            } else {
                HivePartitionDataInfo hivePartitionDataInfo = partitionDataInfoOptional.get();
                // if file last modified time changed or file number under partition change,
                // the partition is treated as changed
                if (basePartitionInfo.getExtLastFileModifiedTime() != hivePartitionDataInfo.getLastFileModifiedTime()
                        || basePartitionInfo.getFileNumber() != hivePartitionDataInfo.getFileNumber()) {
                    updatedPartitionNames.add(partitionNames.get(i));
                }
            }
        }
        return updatedPartitionNames;
    }

    private void repairMvBaseTableMeta(
            MaterializedView mv, BaseTableInfo oldBaseTableInfo,
            Table newTable, List<String> updatedPartitionNames) {
        if (oldBaseTableInfo.isInternalCatalog()) {
            return;
        }

        // acquire db write lock to modify meta of mv
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
            throw new DmlException("repair mv meta failed. database:" + db.getFullName() + " not exist");
        }
        try {
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = mv.getBaseTableRefreshInfo(oldBaseTableInfo);
            Map<String, MaterializedView.BasePartitionInfo> newPartitionInfoMap = Maps.newHashMap();
            for (Map.Entry<String, MaterializedView.BasePartitionInfo> entry : partitionInfoMap.entrySet()) {
                if (updatedPartitionNames.contains(entry.getKey())) {
                    newPartitionInfoMap.put(entry.getKey(), entry.getValue());
                } else {
                    List<String> baseTablePartitionNames = Lists.newArrayList(partitionInfoMap.keySet());
                    Map<String, com.starrocks.connector.PartitionInfo> newPartitionInfos =
                            PartitionUtil.getPartitionNameWithPartitionInfo(newTable, baseTablePartitionNames);
                    if (newPartitionInfos.containsKey(entry.getKey())) {
                        MaterializedView.BasePartitionInfo oldBasePartitionInfo = entry.getValue();
                        com.starrocks.connector.PartitionInfo newPartitionInfo = newPartitionInfos.get(entry.getKey());
                        MaterializedView.BasePartitionInfo newBasePartitionInfo = new MaterializedView.BasePartitionInfo(
                                entry.getValue().getId(), newPartitionInfo.getModifiedTime(), newPartitionInfo.getModifiedTime());
                        newBasePartitionInfo.setExtLastFileModifiedTime(oldBasePartitionInfo.getExtLastFileModifiedTime());
                        newBasePartitionInfo.setFileNumber(oldBasePartitionInfo.getFileNumber());
                        newPartitionInfoMap.put(entry.getKey(), newBasePartitionInfo);
                    } else {
                        // if the partition does not exist in new table,
                        // keep the partition's last modified time as old
                        // which will be refreshed
                        newPartitionInfoMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> baseTableInfoMapMap =
                    mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap();
            // create new base table info with newTable.getTableIdentifier()
            BaseTableInfo newBaseTableInfo = new BaseTableInfo(
                    oldBaseTableInfo.getCatalogName(),
                    oldBaseTableInfo.getDbName(),
                    oldBaseTableInfo.getTableName(), newTable.getTableIdentifier());
            baseTableInfoMapMap.remove(oldBaseTableInfo);
            baseTableInfoMapMap.put(newBaseTableInfo, newPartitionInfoMap);

            List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
            baseTableInfos.remove(oldBaseTableInfo);
            baseTableInfos.add(newBaseTableInfo);

            ConnectorTableInfo connectorTableInfo = GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr()
                    .getConnectorTableInfo(oldBaseTableInfo.getCatalogName(), oldBaseTableInfo.getDbName(),
                            oldBaseTableInfo.getTableIdentifier());
            ConnectorTableInfo newConnectorTableInfo = ConnectorTableInfo.builder()
                    .setRelatedMaterializedViews(connectorTableInfo.getRelatedMaterializedViews())
                    .build();
            GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr().removeConnectorTableInfo(
                    oldBaseTableInfo.getCatalogName(), oldBaseTableInfo.getDbName(),
                    oldBaseTableInfo.getTableIdentifier(), connectorTableInfo);
            GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr().addConnectorTableInfo(
                    newBaseTableInfo.getCatalogName(), newBaseTableInfo.getDbName(),
                    newBaseTableInfo.getTableIdentifier(), newConnectorTableInfo);
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
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
        Locker locker = new Locker();
        // update the meta if succeed
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
            throw new DmlException("update meta failed. database:" + db.getFullName() + " not exist");
        }
        try {
            // check
            Table mv = db.getTable(materializedView.getId());
            if (mv == null) {
                throw new DmlException(
                        "update meta failed. materialized view:" + materializedView.getName() + " not exist");
            }

            // check
            if (mvRefreshedPartitions == null || refTableAndPartitionNames == null) {
                return;
            }

            MaterializedView.MvRefreshScheme mvRefreshScheme = materializedView.getRefreshScheme();
            MaterializedView.AsyncRefreshContext refreshContext = mvRefreshScheme.getAsyncRefreshContext();

            // update materialized view partition to ref base table partition names meta
            updateAssociatedPartitionMeta(refreshContext, mvRefreshedPartitions, refTableAndPartitionNames);

            // NOTE: update all base tables meta to be used in mv rewrite.
            Map<Boolean, List<TableSnapshotInfo>> snapshotInfoSplits = snapshotBaseTables.values()
                    .stream()
                    .collect(Collectors.partitioningBy(s -> s.getBaseTable().isNativeTableOrMaterializedView()));
            Set<Long> refBaseTableIds = refTableAndPartitionNames.keySet().stream()
                    .map(t -> t.getBaseTable().getId())
                    .collect(Collectors.toSet());
            if (snapshotInfoSplits.get(true) != null) {
                updateMetaForOlapTable(refreshContext, snapshotInfoSplits.get(true), refBaseTableIds);
            }
            if (snapshotInfoSplits.get(false) != null) {
                updateMetaForExternalTable(refreshContext, snapshotInfoSplits.get(false), refBaseTableIds);
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
        } catch (Exception e) {
            LOG.warn("update final meta failed after mv refreshed:", e);
            throw e;
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
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
            Table table = MvUtils.getTableChecked(baseTableInfo);
            Set<String> partitionNames =
                    Sets.newHashSet(PartitionUtil.getPartitionNames(table));
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

    private void prepare(TaskRunContext context) {
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

        mvContext = new MvTaskRunContext(context);
        LOG.info("finish prepare refresh of mv:{}, mv name:{}, jobId: {}", mvId, materializedView.getName(), jobId);
    }

    /**
     * Sync base table's partition infos to be used later.
     */
    private void syncPartitions(TaskRunContext context) {
        snapshotBaseTables = collectBaseTableSnapshotInfos(materializedView);

        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        if (!(partitionInfo instanceof SinglePartitionInfo)) {
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
            mvContext.setRefBaseTable(partitionTableAndColumn.first);
            mvContext.setRefBaseTablePartitionColumn(partitionTableAndColumn.second);
        }
        int partitionTTLNumber = materializedView.getTableProperty().getPartitionTTLNumber();
        mvContext.setPartitionTTLNumber(partitionTTLNumber);

        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            syncPartitionsForExpr(context);
        } else if (partitionInfo instanceof ListPartitionInfo) {
            syncPartitionsForList();
        }
        LOG.info("finish sync partitions. mv:{}", materializedView.getName());
    }

    /**
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

    private void syncPartitionsForExpr(TaskRunContext context) {
        Pair<String, String> partitionRange = Pair.create(
                context == null ? null : context.getProperties().get(TaskRun.PARTITION_START),
                context == null ? null : context.getProperties().get(TaskRun.PARTITION_END));
        MvPartitionDiffResult result = PartitionDiffer.computePartitionRangeDiff(db, materializedView, partitionRange);
        if (result == null) {
            return;
        }
        Map<String, Range<PartitionKey>> deletes = result.rangePartitionDiff.getDeletes();

        // Delete old partitions and then add new partitions because the old and new partitions may overlap
        for (String mvPartitionName : deletes.keySet()) {
            dropPartition(db, materializedView, mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] delete partitions range [{}]",
                materializedView.getName(), deletes);

        // Create new added materialized views' ranges
        Map<String, String> partitionProperties = getPartitionProperties(materializedView);
        DistributionDesc distributionDesc = getDistributionDesc(materializedView);
        Map<String, Range<PartitionKey>> adds = result.rangePartitionDiff.getAdds();
        addRangePartitions(db, materializedView, adds, partitionProperties, distributionDesc);
        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            result.mvRangePartitionMap.put(mvPartitionName, addEntry.getValue());
        }
        LOG.info("The process of synchronizing materialized view [{}] add partitions range [{}]",
                materializedView.getName(), adds);

        // used to get partitions to refresh
        Map<Table, Expr> tableToExprMap = materializedView.getTableToPartitionExprMap();
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = SyncPartitionUtils
                .generateBaseRefMap(result.refBaseTablePartitionMap, tableToExprMap, result.mvRangePartitionMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = SyncPartitionUtils
                .generateMvRefMap(result.mvRangePartitionMap, tableToExprMap, result.refBaseTablePartitionMap);
        mvContext.setMvRangePartitionMap(result.mvRangePartitionMap);
        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableRangePartitionMap(result.refBaseTablePartitionMap);
        mvContext.setExternalRefBaseTableMVPartitionMap(result.refBaseTableMVPartitionMap);
    }

    private void syncPartitionsForList() {
        Table partitionBaseTable = mvContext.getRefBaseTable();
        Column partitionColumn = mvContext.getRefBaseTablePartitionColumn();

        ListPartitionDiff listPartitionDiff;
        Map<String, List<List<String>>> baseListPartitionMap;

        Map<String, List<List<String>>> listPartitionMap = materializedView.getListPartitionMap();
        Locker locker = new Locker();
        if (!locker.tryLockDatabase(db, LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database: " + db.getFullName() + " in syncPartitionsForList");
        }
        try {
            baseListPartitionMap = PartitionUtil.getPartitionList(partitionBaseTable, partitionColumn);
            listPartitionDiff = SyncPartitionUtils.getListPartitionDiff(baseListPartitionMap, listPartitionMap);
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        Map<String, List<List<String>>> deletes = listPartitionDiff.getDeletes();

        // We should delete the old partition first and then add the new one,
        // because the old and new partitions may overlap

        for (String mvPartitionName : deletes.keySet()) {
            dropPartition(db, materializedView, mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] delete partitions range [{}]",
                materializedView.getName(), deletes);

        Map<String, String> partitionProperties = getPartitionProperties(materializedView);
        DistributionDesc distributionDesc = getDistributionDesc(materializedView);
        Map<String, List<List<String>>> adds = listPartitionDiff.getAdds();
        addListPartitions(db, materializedView, adds, partitionProperties, distributionDesc);

        LOG.info("The process of synchronizing materialized view [{}] add partitions list [{}]",
                materializedView.getName(), adds);

        // used to get partitions to refresh
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = Maps.newHashMap();
        for (String partitionName : baseListPartitionMap.keySet()) {
            Map<String, Set<String>> basePartitionNameRef = Maps.newHashMap();
            basePartitionNameRef.put(partitionName, Sets.newHashSet(partitionName));
            baseToMvNameRef.put(partitionBaseTable, basePartitionNameRef);
        }
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = Maps.newHashMap();
        for (String partitionName : listPartitionMap.keySet()) {
            Map<Table, Set<String>> mvPartitionNameRef = Maps.newHashMap();
            mvPartitionNameRef.put(partitionBaseTable, Sets.newHashSet(partitionName));
            mvToBaseNameRef.put(partitionName, mvPartitionNameRef);
        }
        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableListPartitionMap(baseListPartitionMap);
    }

    private static boolean supportPartitionRefresh(Table table) {
        return ConnectorPartitionTraits.build(table).supportPartitionRefresh();
    }

    /**
     * Whether non-partitioned materialized view needs to be refreshed or not, it needs refresh when:
     * - its base table is not supported refresh by partition.
     * - its base table has updated.
     */
    private boolean isNonPartitionedMVNeedToRefresh() {
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            if (!supportPartitionRefresh(snapshotTable)) {
                return true;
            }
            if (needToRefreshTable(materializedView, snapshotTable)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether partitioned materialized view needs to be refreshed or not base on the non-ref base tables, it needs refresh when:
     * - its non-ref base table except un-supported base table has updated.
     */
    private boolean isPartitionedMVNeedToRefreshBaseOnNonRefTables(Table partitionTable) {
        Map<Table, Column> tableColumnMap = materializedView.getRelatedPartitionTableAndColumn();
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            if (snapshotTable.getId() == partitionTable.getId()) {
                continue;
            }
            if (!supportPartitionRefresh(snapshotTable)) {
                continue;
            }
            if (tableColumnMap.containsKey(snapshotTable)) {
                continue;
            }
            if (needToRefreshTable(materializedView, snapshotTable)) {
                return true;
            }
        }
        return false;
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
     * @return
     * @throws AnalysisException
     */
    private Set<String> getPartitionsToRefreshForMaterializedView(
            PartitionInfo mvPartitionInfo, String start,
            String end, boolean force, Set<String> mvPotentialPartitionNames) throws AnalysisException {
        int partitionTTLNumber = mvContext.getPartitionTTLNumber();

        // Force refresh
        if (force && start == null && end == null) {
            if (mvPartitionInfo instanceof SinglePartitionInfo) {
                return materializedView.getVisiblePartitionNames();
            } else {
                if (mvPartitionInfo instanceof ListPartitionInfo) {
                    return materializedView.getValidListPartitionMap(partitionTTLNumber).keySet();
                } else {
                    return materializedView.getValidRangePartitionMap(partitionTTLNumber).keySet();
                }
            }
        }

        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();
        if (mvPartitionInfo instanceof SinglePartitionInfo) {
            // non-partitioned materialized view
            if (force || isNonPartitionedMVNeedToRefresh()) {
                return materializedView.getVisiblePartitionNames();
            }
        } else if (mvPartitionInfo instanceof ExpressionRangePartitionInfo) {
            // range partitioned materialized views
            Expr partitionExpr = materializedView.getFirstPartitionRefTableExpr();
            Table refBaseTable = mvContext.getRefBaseTable();

            boolean isAutoRefresh = mvContext.getTaskType().isAutoRefresh();
            Set<String> mvRangePartitionNames = SyncPartitionUtils.getPartitionNamesByRangeWithPartitionLimit(
                    materializedView, start, end, partitionTTLNumber, isAutoRefresh);
            LOG.info("Get partition names by range with partition limit, start: {}, end: {}, partitionTTLNumber: {}," +
                            " isAutoRefresh: {}, mvRangePartitionNames: {}",
                    start, end, partitionTTLNumber, isAutoRefresh, mvRangePartitionNames);

            // check non-ref base tables
            if (isPartitionedMVNeedToRefreshBaseOnNonRefTables(refBaseTable)) {
                if (start == null && end == null) {
                    // if non partition table changed, should refresh all partitions of materialized view
                    return mvRangePartitionNames;
                } else {
                    if (isAutoRefresh) {
                        // If this refresh comes from PERIODIC/EVENT_TRIGGER, it should not respect the config to do the
                        // refresh rather than only checking the ref table's update.
                        // eg:
                        //  MV: SELECT t2.k1 as k1, t2.k2 as k2, t1.k1 as k3, t1.k2 as k4 FROM t1 join t2 on t1.k1=t2.k1;
                        //  RefTable    : t1, Partition: p0,p1,p2
                        //  NonRefTable : t2, Partition: p0,p1,p2
                        // when t2 is updated, it will trigger refresh of ref-table's all partitions.
                        // if `partition_refresh_number` is 1, then:
                        // - The 1th TaskRun: start=null,end=null,  ref-table refresh partition: p0
                        // - The 2th TaskRun: start=p1,end=p2,      ref-table refresh partition: p1
                        // - The 3th TaskRun: start=p2,end=p2,      ref-table refresh partition: p2
                        // if use method below, it will break in the 2th TaskRun because ref-table has not updated in the
                        // specific start and end ranges.
                        return mvRangePartitionNames;
                    } else {
                        // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                        // it should be refreshed according to the user-specified range, not all partitions.
                        return getMvPartitionNamesToRefresh(refBaseTable,
                                mvRangePartitionNames, true);
                    }
                }
            }

            Map<Table, Column> partitionTablesAndColumn = materializedView.getRelatedPartitionTableAndColumn();
            if (partitionExpr instanceof SlotRef) {
                // check related partition table
                for (Table table : partitionTablesAndColumn.keySet()) {
                    needRefreshMvPartitionNames.addAll(getMvPartitionNamesToRefresh(
                            table, mvRangePartitionNames, force));
                }
            } else if (partitionExpr instanceof FunctionCallExpr) {
                // check related partition table
                for (Table table : partitionTablesAndColumn.keySet()) {
                    needRefreshMvPartitionNames.addAll(getMvPartitionNamesToRefresh(
                            table, mvRangePartitionNames, force));
                }

                Map<Table, Set<String>> baseChangedPartitionNames =
                        getBasePartitionNamesByMVPartitionNames(needRefreshMvPartitionNames);
                if (baseChangedPartitionNames.isEmpty()) {
                    return needRefreshMvPartitionNames;
                }

                List<TableWithPartitions> baseTableWithPartitions = baseChangedPartitionNames.keySet().stream()
                        .map(x -> new TableWithPartitions(x, baseChangedPartitionNames.get(x)))
                        .collect(Collectors.toList());
                Map<Table, Map<String, Range<PartitionKey>>> refBaseTableRangePartitionMap =
                        mvContext.getRefBaseTableRangePartitionMap();
                Map<String, Range<PartitionKey>> mvRangePartitionMap = mvContext.getMvRangePartitionMap();
                if (materializedView.isCalcPotentialRefreshPartition(baseTableWithPartitions,
                        refBaseTableRangePartitionMap, needRefreshMvPartitionNames, mvRangePartitionMap)) {
                    // because the relation of partitions between materialized view and base partition table is n : m,
                    // should calculate the candidate partitions recursively.
                    LOG.info("Start calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                            " baseChangedPartitionNames: {}", needRefreshMvPartitionNames, baseChangedPartitionNames);
                    SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                            mvContext.getRefBaseTableMVIntersectedPartitions(),
                            mvContext.getMvRefBaseTableIntersectedPartitions(),
                            mvPotentialPartitionNames);
                    LOG.info("Finish calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                            " baseChangedPartitionNames: {}", needRefreshMvPartitionNames, baseChangedPartitionNames);
                }
            }
        } else if (mvPartitionInfo instanceof ListPartitionInfo) {
            // list partitioned materialized view
            Table partitionTable = mvContext.getRefBaseTable();
            boolean isAutoRefresh = mvContext.getTaskType().isAutoRefresh();
            Set<String> mvListPartitionNames = SyncPartitionUtils.getPartitionNamesByListWithPartitionLimit(
                    materializedView, start, end, partitionTTLNumber, isAutoRefresh);

            // check non-ref base tables
            if (isPartitionedMVNeedToRefreshBaseOnNonRefTables(partitionTable)) {
                if (start == null && end == null) {
                    // if non partition table changed, should refresh all partitions of materialized view
                    return mvListPartitionNames;
                } else {
                    // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                    // it should be refreshed according to the user-specified range, not all partitions.
                    return getMvPartitionNamesToRefresh(partitionTable,
                            mvListPartitionNames, true);
                }
            }

            // check the ref base table
            return getMvPartitionNamesToRefresh(partitionTable, mvListPartitionNames,
                    force);
        } else {
            throw new DmlException("unsupported partition info type:" + mvPartitionInfo.getClass().getName());
        }
        return needRefreshMvPartitionNames;
    }

    private Set<String> getMvPartitionNamesToRefresh(Table refBaseTable,
                                                     Set<String> mvRangePartitionNames,
                                                     boolean force) {
        // refresh all mv partitions when the ref base table is not supported partition refresh
        if (force || !supportPartitionRefresh(refBaseTable)) {
            LOG.info("The ref base table {} is not supported partition refresh, refresh all " +
                    "partitions of materialized view: {}", refBaseTable.getName(), materializedView.getName());
            return Sets.newHashSet(mvRangePartitionNames);
        }

        // step1: check updated partition names in the ref base table and add it to the refresh candidate
        MvBaseTableUpdateInfo mvBaseTableUpdateInfo = getMvBaseTableUpdateInfo(materializedView, refBaseTable, false, false);
        if (mvBaseTableUpdateInfo == null) {
            LOG.warn("Cannot find the updated partition info of ref base table {} of mv: {}",
                    refBaseTable.getName(), materializedView.getName());
            return mvRangePartitionNames;
        }

        // step2: fetch the corresponding materialized view partition names as the need to refresh partitions
        Set<String> updatePartitionNames = mvBaseTableUpdateInfo.getToRefreshPartitionNames();
        Set<String> result = getMVPartitionNamesByBasePartitionNames(refBaseTable, updatePartitionNames);
        result.retainAll(mvRangePartitionNames);
        LOG.info("The ref base table {} has updated partitions: {}, the corresponding mv partitions to refresh: {}, " +
                        "mvRangePartitionNames: {}", refBaseTable.getName(), updatePartitionNames, result, mvRangePartitionNames);
        return result;
    }

    /**
     * @param basePartitionNames : ref base table partition names to check.
     * @return : Return mv corresponding partition names to the ref base table partition names.
     */
    private Set<String> getMVPartitionNamesByBasePartitionNames(Table baseTable, Set<String> basePartitionNames) {
        Set<String> result = Sets.newHashSet();
        Map<Table, Map<String, Set<String>>> refBaseTableMVPartitionMaps = mvContext.getRefBaseTableMVIntersectedPartitions();
        if (refBaseTableMVPartitionMaps == null || !refBaseTableMVPartitionMaps.containsKey(baseTable)) {
            return basePartitionNames;
        }
        Map<String, Set<String>> refBaseTableMVPartitionMap = refBaseTableMVPartitionMaps.get(baseTable);
        for (String basePartitionName : basePartitionNames) {
            if (refBaseTableMVPartitionMap.containsKey(basePartitionName)) {
                result.addAll(refBaseTableMVPartitionMap.get(basePartitionName));
            } else {
                LOG.warn("Cannot find need refreshed ref base table partition from synced partition info: {}",
                        basePartitionName);
            }
        }
        return result;
    }

    /**
     * @param mvPartitionNames : the need to refresh materialized view partition names
     * @return : the corresponding ref base table partition names to the materialized view partition names
     */
    private Map<Table, Set<String>> getBasePartitionNamesByMVPartitionNames(Set<String> mvPartitionNames) {
        Map<Table, Set<String>> result = new HashMap<>();
        Map<String, Map<Table, Set<String>>> mvRefBaseTablePartitionMaps =
                mvContext.getMvRefBaseTableIntersectedPartitions();
        for (String mvPartitionName : mvPartitionNames) {
            if (mvRefBaseTablePartitionMaps == null || !mvRefBaseTablePartitionMaps.containsKey(mvPartitionName)) {
                LOG.warn("Cannot find need refreshed mv table partition from synced partition info: {}",
                        mvPartitionName);
                continue;
            }
            Map<Table, Set<String>> mvRefBaseTablePartitionMap = mvRefBaseTablePartitionMaps.get(mvPartitionName);
            for (Map.Entry<Table, Set<String>> entry : mvRefBaseTablePartitionMap.entrySet()) {
                Table baseTable = entry.getKey();
                Set<String> baseTablePartitions = entry.getValue();
                if (result.containsKey(baseTable)) {
                    // If the result already contains the base table name, add all new partitions to the existing set
                    result.get(baseTable).addAll(baseTablePartitions);
                } else {
                    // If the result doesn't contain the base table name, put the new set into the map
                    result.put(baseTable, Sets.newHashSet(baseTablePartitions));
                }
            }
        }
        return result;
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
                if (snapshotPartitionInfo instanceof SinglePartitionInfo) {
                    Set<String> partitionNames = ((OlapTable) table).getVisiblePartitionNames();
                    if (!snapShotOlapTable.getVisiblePartitionNames().equals(partitionNames)) {
                        // there is partition rename
                        return true;
                    }
                } else if (snapshotPartitionInfo instanceof ListPartitionInfo) {
                    Map<String, List<List<String>>> snapshotPartitionMap =
                            snapShotOlapTable.getListPartitionMap();
                    Map<String, List<List<String>>> currentPartitionMap =
                            ((OlapTable) table).getListPartitionMap();
                    if (SyncPartitionUtils.hasListPartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
                        return true;
                    }
                } else {
                    Map<String, Range<PartitionKey>> snapshotPartitionMap =
                            snapShotOlapTable.getRangePartitionMap();
                    Map<String, Range<PartitionKey>> currentPartitionMap =
                            ((OlapTable) table).getRangePartitionMap();
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
                    if (!(mvPartitionInfo instanceof ExpressionRangePartitionInfo)) {
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
     * @return: true if the base table's partition has changed, otherwise false.
     */
    private boolean checkBaseTablePartitionChange(MaterializedView materializedView) {
        List<Database> dbs = collectDatabases(materializedView);
        Locker locker = new Locker();
        if (!locker.tryLockDatabases(dbs, LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database: " + Joiner.on(",").join(dbs)
                    + " in checkBaseTablePartitionChange");
        }
        // check snapshotBaseTables and current tables in catalog
        try {
            if (snapshotBaseTables.values().stream().anyMatch(this::checkBaseTablePartitionHasChanged)) {
                return true;
            }
        } finally {
            locker.unlockDatabases(dbs, LockType.READ);
        }
        return false;
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
        ctx.getSessionVariable().setEnableInsertStrict(false);
        // enable profile by default for mv refresh task
        ctx.getSessionVariable().setEnableProfile(true);
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
     * @param materializedView: the materialized view to check
     * @return: the deduplicated databases of the materialized view's base tables,
     * throw exception if the database do not exist.
     */
    List<Database> collectDatabases(MaterializedView materializedView) {
        Map<Long, Database> databaseMap = Maps.newHashMap();
        for (BaseTableInfo baseTableInfo : materializedView.getBaseTableInfos()) {
            Optional<Database> dbOpt = GlobalStateMgr.getCurrentState().getMetadataMgr().getDatabase(baseTableInfo);
            if (dbOpt.isEmpty()) {
                LOG.warn("database {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getDbInfoStr(), materializedView.getName());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }
            Database db = dbOpt.get();
            databaseMap.put(db.getId(), db);
        }
        return Lists.newArrayList(databaseMap.values());
    }

    /**
     * Collect all base table snapshot infos for the materialized view which the snapshot infos are kept and used in the final
     * update meta phase.
     * </p>
     * NOTE:
     * 1. deep copy of the base table's metadata may be time costing, we can optimize it later.
     * 2. no needs to lock the base table's metadata since the metadata is not changed during the refresh process.
     * @param materializedView the materialized view to collect
     * @return the base table and its snapshot info map
     */
    @VisibleForTesting
    public Map<Long, TableSnapshotInfo> collectBaseTableSnapshotInfos(MaterializedView materializedView) {
        Map<Long, TableSnapshotInfo> tables = Maps.newHashMap();
        List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();

        List<Database> dbs = collectDatabases(materializedView);
        Locker locker = new Locker();
        if (!locker.tryLockDatabases(dbs, LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database: " + Joiner.on(",").join(dbs)
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
            locker.unlockDatabases(dbs, LockType.READ);
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

    private Map<String, String> getPartitionProperties(MaterializedView materializedView) {
        Map<String, String> partitionProperties = new HashMap<>(4);
        partitionProperties.put("replication_num",
                String.valueOf(materializedView.getDefaultReplicationNum()));
        partitionProperties.put("storage_medium", materializedView.getStorageMedium());
        String storageCooldownTime =
                materializedView.getTableProperty().getProperties().get("storage_cooldown_time");
        if (storageCooldownTime != null
                && !storageCooldownTime.equals(String.valueOf(DataProperty.MAX_COOLDOWN_TIME_MS))) {
            // cast long str to time str e.g.  '1587473111000' -> '2020-04-21 15:00:00'
            String storageCooldownTimeStr = TimeUtils.longToTimeString(Long.parseLong(storageCooldownTime));
            partitionProperties.put("storage_cooldown_time", storageCooldownTimeStr);
        }
        return partitionProperties;
    }

    private DistributionDesc getDistributionDesc(MaterializedView materializedView) {
        DistributionInfo distributionInfo = materializedView.getDefaultDistributionInfo();
        if (distributionInfo instanceof HashDistributionInfo) {
            List<String> distColumnNames = new ArrayList<>();
            for (Column distributionColumn : ((HashDistributionInfo) distributionInfo).getDistributionColumns()) {
                distColumnNames.add(distributionColumn.getName());
            }
            return new HashDistributionDesc(distributionInfo.getBucketNum(), distColumnNames);
        } else {
            return new RandomDistributionDesc();
        }
    }

    private void addRangePartitions(Database database, MaterializedView materializedView,
                                    Map<String, Range<PartitionKey>> adds, Map<String, String> partitionProperties,
                                    DistributionDesc distributionDesc) {
        if (adds.isEmpty()) {
            return;
        }
        List<PartitionDesc> partitionDescs = Lists.newArrayList();

        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            Range<PartitionKey> partitionKeyRange = addEntry.getValue();

            String lowerBound = partitionKeyRange.lowerEndpoint().getKeys().get(0).getStringValue();
            String upperBound = partitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue();
            boolean isMaxValue = partitionKeyRange.upperEndpoint().isMaxValue();
            PartitionValue upperPartitionValue;
            if (isMaxValue) {
                upperPartitionValue = PartitionValue.MAX_VALUE;
            } else {
                upperPartitionValue = new PartitionValue(upperBound);
            }
            PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(
                    Collections.singletonList(new PartitionValue(lowerBound)),
                    Collections.singletonList(upperPartitionValue));
            SingleRangePartitionDesc singleRangePartitionDesc =
                    new SingleRangePartitionDesc(false, mvPartitionName, partitionKeyDesc, partitionProperties);
            partitionDescs.add(singleRangePartitionDesc);
        }

        // create partitions in small batch, to avoid create too many partitions at once
        for (List<PartitionDesc> batch : ListUtils.partition(partitionDescs, CREATE_PARTITION_BATCH_SIZE)) {
            RangePartitionDesc rangePartitionDesc =
                    new RangePartitionDesc(materializedView.getPartitionColumnNames(), batch);
            AddPartitionClause alterPartition = new AddPartitionClause(rangePartitionDesc, distributionDesc,
                    partitionProperties, false);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(
                        database, materializedView.getName(), alterPartition);
            } catch (Exception e) {
                throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                        database.getFullName(), materializedView.getName());
            }
            Uninterruptibles.sleepUninterruptibly(Config.mv_create_partition_batch_interval_ms, TimeUnit.MILLISECONDS);
        }
    }

    private void addListPartitions(Database database, MaterializedView materializedView,
                                   Map<String, List<List<String>>> adds, Map<String, String> partitionProperties,
                                   DistributionDesc distributionDesc) {
        if (adds.isEmpty()) {
            return;
        }

        for (Map.Entry<String, List<List<String>>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            List<List<String>> partitionKeyList = addEntry.getValue();
            MultiItemListPartitionDesc multiItemListPartitionDesc =
                    new MultiItemListPartitionDesc(false, mvPartitionName, partitionKeyList, partitionProperties);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(
                        database, materializedView.getName(), new AddPartitionClause(
                                multiItemListPartitionDesc, distributionDesc,
                                partitionProperties, false));
            } catch (Exception e) {
                throw new DmlException("add list partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                        database.getFullName(), materializedView.getName());
            }
        }
    }

    private void dropPartition(Database db, MaterializedView materializedView, String mvPartitionName) {
        String dropPartitionName = materializedView.getPartition(mvPartitionName).getName();
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
            throw new DmlException("drop partition failed. database:" + db.getFullName() + " not exist");
        }
        try {
            // check
            Table mv = db.getTable(materializedView.getId());
            if (mv == null) {
                throw new DmlException("drop partition failed. mv:" + materializedView.getName() + " not exist");
            }
            Partition mvPartition = mv.getPartition(dropPartitionName);
            if (mvPartition == null) {
                throw new DmlException("drop partition failed. partition:" + dropPartitionName + " not exist");
            }

            GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(
                    db, materializedView,
                    new DropPartitionClause(false, dropPartitionName, false, true));
        } catch (Exception e) {
            throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                    db.getFullName(), materializedView.getName());
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
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
                MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                        partition.getId(), partition.getVisibleVersion(), partition.getVisibleVersionTime());
                partitionInfos.put(partition.getName(), basePartitionInfo);
            }
            LOG.info("Collect olap base table {}'s refreshed partition infos: {}", baseTable.getName(), partitionInfos);
            return partitionInfos;
        } else if (baseTable.isHiveTable() || baseTable.isIcebergTable() || baseTable.isJDBCTable() ||
                baseTable.isPaimonTable()) {
            return getSelectedPartitionInfos(baseTable, Lists.newArrayList(refreshedPartitionNames),
                    baseTableInfo);
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
