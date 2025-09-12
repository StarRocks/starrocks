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

package com.starrocks.scheduler.mv;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections.CollectionUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.scheduler.TaskRun.MV_ID;
import static com.starrocks.scheduler.TaskRun.MV_UNCOPYABLE_PROPERTIES;

/**
 * PCT(Partition Change Tracking) based materialized view refresh processor which is designed to refresh materialized views
 * based on partition changes in the base tables.
 * MVPCTBasedRefreshProcessor is not thread safe for concurrent runs of the same materialized view
 */
public final class MVPCTBasedRefreshProcessor extends BaseMVRefreshProcessor {

    public MVPCTBasedRefreshProcessor(Database db, MaterializedView mv,
                                      MvTaskRunContext mvContext,
                                      IMaterializedViewMetricsEntity mvEntity) {
        super(db, mv, mvContext, mvEntity, MVPCTBasedRefreshProcessor.class);
    }

    // Core logics:
    // 1. prepare to check some conditions
    // 2. sync partitions with base tables(add or drop partitions, which will be optimized  by dynamic partition creation later)
    // 3. decide which partitions of mv to refresh and the corresponding base tables' source partitions
    // 4. construct the refresh sql and execute it
    // 5. update the source table version map if refresh task completes successfully
    @Override
    public Constants.TaskRunState doProcessTaskRun(TaskRunContext taskRunContext,
                                                   MVRefreshExecutor executor) throws Exception {
        Stopwatch watch = Stopwatch.createStarted();

        // log mv basic info, it may throw exception if mv is invalid since base table has dropped
        try {
            logger.debug("refBaseTablePartitionExprMap:{},refBaseTablePartitionSlotMap:{}, " +
                            "refBaseTablePartitionColumnMap:{},baseTableInfos:{}",
                    mv.getRefBaseTablePartitionExprs(), mv.getRefBaseTablePartitionSlots(),
                    mv.getRefBaseTablePartitionColumns(), MvUtils.formatBaseTableInfos(mv.getBaseTableInfos()));
        } catch (Throwable e) {
            logger.warn("Log mv basic info failed:", e);
        }

        // refresh materialized view
        Constants.TaskRunState result = doRefreshMaterializedView(taskRunContext, executor);

        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        logger.info("refresh mv success, cost time(ms): {}", DebugUtil.DECIMAL_FORMAT_SCALE_3.format(elapsed));
        mvEntity.updateRefreshDuration(elapsed);

        return result;
    }

    @Override
    public int getRetryTimes(ConnectContext connectContext) {
        int maxRefreshMaterializedViewRetryNum = 1;
        if (connectContext != null && connectContext.getSessionVariable() != null) {
            maxRefreshMaterializedViewRetryNum =
                    connectContext.getSessionVariable().getQueryDebugOptions().getMaxRefreshMaterializedViewRetryNum();
            if (maxRefreshMaterializedViewRetryNum <= 0) {
                maxRefreshMaterializedViewRetryNum = 1;
            }
        }
        maxRefreshMaterializedViewRetryNum = Math.max(Config.max_mv_refresh_failure_retry_times,
                maxRefreshMaterializedViewRetryNum);
        return maxRefreshMaterializedViewRetryNum;
    }

    @Override
    public ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) throws Exception {
        // sync and check partitions of base tables
        syncAndCheckPCTPartitions(taskRunContext);

        // check to refresh partitions of mv and base tables
        try (Timer ignored = Tracers.watchScope("MVRefreshCheckMVToRefreshPartitions")) {
            updatePCTToRefreshMetas(taskRunContext);
            if (CollectionUtils.isEmpty(pctMVToRefreshedPartitions)) {
                return new ProcessExecPlan(Constants.TaskRunState.SKIPPED, null, null);
            }
        }

        // execute the ExecPlan of insert stmt
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshPrepareRefreshPlan")) {
            insertStmt = prepareRefreshPlan(pctMVToRefreshedPartitions, pctRefTablePartitionNames);
        }
        return new ProcessExecPlan(Constants.TaskRunState.SUCCESS, mvContext.getExecPlan(), insertStmt);
    }

    private Constants.TaskRunState doRefreshMaterializedView(TaskRunContext context,
                                                             MVRefreshExecutor executor) throws Exception {
        final ProcessExecPlan processExecPlan = getProcessExecPlan(context);
        if (processExecPlan.state() == Constants.TaskRunState.SKIPPED) {
            logger.info("MV {} refresh task skipped, no partitions to refresh", mv.getName());
            return Constants.TaskRunState.SKIPPED;
        }

        ExecPlan mvExecPlan = processExecPlan.execPlan();
        try (Timer ignored = Tracers.watchScope("MVRefreshMaterializedView")) {
            InsertStmt insertStmt = processExecPlan.insertStmt();
            executor.executePlan(mvExecPlan, insertStmt);
        }

        // insert execute successfully, update the meta of mv according to ExecPlan
        try (Timer ignored = Tracers.watchScope("MVRefreshUpdateMeta")) {
            updatePCTMeta(mvExecPlan, pctMVToRefreshedPartitions, pctRefTableRefreshPartitions);
        }

        // do not generate next task run if the current task run is killed
        if (mvContext.hasNextBatchPartition() && !mvContext.getTaskRun().isKilled()) {
            generateNextTaskRun();
        }

        return Constants.TaskRunState.SUCCESS;
    }

    /**
     * Prepare the statement and plan for mv refreshing, considering the partitions of ref table
     */
    private InsertStmt prepareRefreshPlan(Set<String> mvToRefreshedPartitions, Map<String, Set<String>> refTablePartitionNames)
            throws AnalysisException, LockTimeoutException {
        // Prepare refresh connect context
        ConnectContext ctx = mvContext.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
                .setDb(ctx.getDatabase())
                .setWarehouse(ctx.getCurrentWarehouseName())
                .setCNGroup(ctx.getCurrentComputeResourceName());

        // Prepare refresh variables
        final Set<Table> baseTables = snapshotBaseTables.values()
                .stream()
                .map(BaseTableSnapshotInfo::getBaseTable)
                .collect(Collectors.toSet());
        changeDefaultConnectContextIfNeeded(ctx, baseTables);

        // Generate AST of insert statement
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshParser")) {
            insertStmt = generateInsertAst(ctx, mvToRefreshedPartitions, mv.getTaskDefinition());
        }

        PlannerMetaLocker locker = new PlannerMetaLocker(ctx, insertStmt);
        ExecPlan execPlan = null;
        if (!locker.tryLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database in prepareRefreshPlan");
        }

        MVPCTRefreshPlanBuilder planBuilder = new MVPCTRefreshPlanBuilder(db, mv, mvContext, mvRefreshPartitioner);
        try {
            // Analyze and prepare a partition & Rebuild insert statement by
            // considering to-refresh partitions of ref tables/ mv
            try (Timer ignored = Tracers.watchScope("MVRefreshAnalyzer")) {
                insertStmt = planBuilder.analyzeAndBuildInsertPlan(insertStmt,
                        mvToRefreshedPartitions, refTablePartitionNames, ctx);
                // Must set execution id before StatementPlanner.plan
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
            }

            // Generate insert stmt's exec plan, make thread local ctx existed
            try (ConnectContext.ScopeGuard guard = ctx.bindScope(); Timer ignored = Tracers.watchScope("MVRefreshPlanner")) {
                execPlan = StatementPlanner.planInsertStmt(locker, insertStmt, ctx);
            }
        } finally {
            locker.unlock();
        }

        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage message = status.getMvTaskRunExtraMessage();
            if (message == null) {
                return;
            }
            Map<String, String> planBuildMessage = planBuilder.getPlanBuilderMessage();
            logger.info("MV Refresh PlanBuilderMessage: {}", planBuildMessage);
            message.setPlanBuilderMessage(planBuildMessage);
        });

        QueryDebugOptions debugOptions = ctx.getSessionVariable().getQueryDebugOptions();
        // log the final mv refresh plan for each refresh for better trace and debug
        if (logger.isDebugEnabled() || debugOptions.isEnableQueryTraceLog()) {
            logger.info("MV Refresh Final Plan\nMV PartitionsToRefresh: {}\nBase PartitionsToScan: {}\n" +
                            "Insert Plan:\n{}",
                    String.join(",", mvToRefreshedPartitions), refTablePartitionNames,
                    execPlan != null ? execPlan.getExplainString(StatementBase.ExplainLevel.VERBOSE) : "");
        } else {
            logger.info("MV Refresh Final Plan, MV PartitionsToRefresh: {}, Base PartitionsToScan: {}",
                    String.join(",", mvToRefreshedPartitions), refTablePartitionNames);
        }

        mvContext.setExecPlan(execPlan);
        return insertStmt;
    }

    private void generateNextTaskRun() {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Map<String, String> properties = mvContext.getProperties();
        long mvId = Long.parseLong(properties.get(MV_ID));
        String taskName = TaskBuilder.getMvTaskName(mvId);
        Map<String, String> newProperties = Maps.newHashMap();
        for (Map.Entry<String, String> proEntry : properties.entrySet()) {
            // skip uncopyable properties: force/partition_values/... which only can be set specifically.
            if (proEntry.getKey() == null || proEntry.getValue() == null
                    || MV_UNCOPYABLE_PROPERTIES.contains(proEntry.getKey())) {
                continue;
            }
            newProperties.put(proEntry.getKey(), proEntry.getValue());
        }
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isListPartition()) {
            //TODO: partition values may be too long, need to be optimized later.
            newProperties.put(TaskRun.PARTITION_VALUES, mvContext.getNextPartitionValues());
        } else {
            newProperties.put(TaskRun.PARTITION_START, mvContext.getNextPartitionStart());
            newProperties.put(TaskRun.PARTITION_END, mvContext.getNextPartitionEnd());
        }
        if (mvContext.getStatus() != null) {
            newProperties.put(TaskRun.START_TASK_RUN_ID, mvContext.getStatus().getStartTaskRunId());
        }
        // warehouse
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            newProperties.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
        }
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setNextPartitionStart(mvContext.getNextPartitionStart());
            extraMessage.setNextPartitionEnd(mvContext.getNextPartitionEnd());
            extraMessage.setNextPartitionValues(mvContext.getNextPartitionValues());
        });

        // Partition refreshing task run should have the HIGHER priority, and be scheduled before other tasks
        // Otherwise this round of partition refreshing would be staved and never got finished
        ExecuteOption executeOption = mvContext.getExecuteOption();
        int priority = executeOption.getPriority() > Constants.TaskRunPriority.LOWEST.value() ?
                executeOption.getPriority() : Constants.TaskRunPriority.HIGHER.value();
        ExecuteOption option = new ExecuteOption(priority, true, newProperties);
        logger.info("[MV] Generate a task to refresh next batches of partitions for MV {}-{}, start={}, end={}, " +
                        "priority={}, properties={}", mv.getName(), mv.getId(),
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(), priority, properties);

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

    @VisibleForTesting
    public Map<Long, BaseTableSnapshotInfo> getSnapshotBaseTables() {
        return snapshotBaseTables;
    }

    @Override
    protected BaseTableSnapshotInfo buildBaseTableSnapshotInfo(BaseTableInfo baseTableInfo, Table table) {
        return new PCTTableSnapshotInfo(baseTableInfo, table);
    }

    public MVPCTRefreshPartitioner getMvRefreshPartitioner() {
        return mvRefreshPartitioner;
    }
}