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

package com.starrocks.scheduler.mv;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockParams;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.pct.MVPCTMetaRepairer;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshListPartitioner;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshNonPartitioner;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshPartitioner;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshRangePartitioner;
import com.starrocks.scheduler.mv.pct.PCTTableSnapshotInfo;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellUtils;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PartitionNameSetMap;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Base class for materialized view refresh processor.
 */
public abstract class BaseMVRefreshProcessor {
    // session.enable_spill
    protected static final String MV_SESSION_ENABLE_SPILL =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.ENABLE_SPILL;
    // session.query_timeout. Deprecated, only for compatibility with old version
    protected static final String MV_SESSION_QUERY_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.QUERY_TIMEOUT;
    // session.insert_timeout
    protected static final String MV_SESSION_INSERT_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.INSERT_TIMEOUT;

    protected final Database db;
    protected final MaterializedView mv;
    protected final IMaterializedViewMetricsEntity mvEntity;
    protected final MvTaskRunContext mvContext;
    protected final Logger logger;
    // Collect all bases tables of the mv to be updated meta after mv refresh success.
    // format :     table id -> <base table info, snapshot table>
    protected final MVPCTRefreshPartitioner mvRefreshPartitioner;
    protected final MVRefreshParams mvRefreshParams;
    // current refresh mode, can be changed in the refresh's runtime for `auto` mode
    protected MaterializedView.RefreshMode currentRefreshMode;

    // Collect all base table snapshot infos for the mv which the snapshot infos are kept
    // and used in the final update meta.
    protected Map<Long, BaseTableSnapshotInfo> snapshotBaseTables = Maps.newHashMap();
    // PCT related fields
    protected PCellSortedSet pctMVToRefreshedPartitions = null;
    protected PCellSetMapping pctRefTablePartitionNames = null;
    protected Map<BaseTableSnapshotInfo, PCellSortedSet> pctRefTableRefreshPartitions = null;
    // for testing
    protected TaskRun nextTaskRun = null;
    // whether to enable precise refresh for external table base tables
    protected final boolean isEnableExternalTablePreciseRefresh;

    /**
     * A record to hold the exec plan and insert statement for a task run.
     *
     * @param state      the state of the task run
     * @param execPlan   the execution plan for the task run
     * @param insertStmt the insert statement for the task run
     */
    public record ProcessExecPlan(Constants.TaskRunState state,
                                  ExecPlan execPlan,
                                  InsertStmt insertStmt) {
    }

    public BaseMVRefreshProcessor(Database db, MaterializedView mv,
                                  MvTaskRunContext mvContext,
                                  IMaterializedViewMetricsEntity mvEntity,
                                  MaterializedView.RefreshMode refreshMode,
                                  Class<?> clazz) {
        this.db = db;
        this.mv = mv;
        this.mvContext = mvContext;
        this.mvEntity = mvEntity;
        this.logger = MVTraceUtils.getLogger(mv, clazz);
        this.mvRefreshParams = new MVRefreshParams(mv, mvContext.getProperties());
        // prepare mv refresh partitioner
        this.mvRefreshPartitioner = buildMvRefreshPartitioner(mv, mvContext, mvRefreshParams);
        this.currentRefreshMode = refreshMode;
        this.isEnableExternalTablePreciseRefresh = isEnableExternalTablePreciseRefresh();
        // init the refresh mode
        updateTaskRunStatus(status -> {
            status.getMvTaskRunExtraMessage().setRefreshMode(currentRefreshMode.name());
        });
    }

    /**
     * Get the process execution plan for the task run which can be used for explain or execution.
     *
     * @param taskRunContext the task run context which contains the task run information
     * @return the process execution plan which contains the state, exec plan and insert statement
     * @throws Exception if any error occurs during the process
     */
    public abstract ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) throws Exception;

    /**
     * Process the task run with the given context and executor.
     *
     * @param taskRunContext the task run context which contains the task run information
     * @param executor       the executor to execute the task run
     * @return the state of the task run after processing
     * @throws Exception if any error occurs during the process
     */
    public abstract Constants.TaskRunState execProcessExecPlan(TaskRunContext taskRunContext,
                                                               ProcessExecPlan processExecPlan,
                                                               MVRefreshExecutor executor) throws Exception;

    /**
     * Build a base table snapshot info for the given base table info and table, It can be different snapshot infos for
     * different mv refresh processors.
     *
     * @param baseTableInfo the base table info to build the snapshot info
     * @param table         the table to build the snapshot info
     * @return the base table snapshot info which contains the table id, table name and the snapshot table
     */
    public abstract BaseTableSnapshotInfo buildBaseTableSnapshotInfo(BaseTableInfo baseTableInfo,
                                                                     Table table);

    /**
     * Generate the next task run to be processed and set it to the nextTaskRun field.
     */
    public abstract void generateNextTaskRunIfNeeded();

    /**
     * Update the version meta after the mv refresh is successful.
     * @param execPlan the exec plan used for the mv refresh
     * @param mvRefreshedPartitions the refreshed partitions of the mv
     * @param refTableAndPartitionNames the refreshed partitions of the base tables
     */
    public abstract void updateVersionMeta(ExecPlan execPlan,
                                           PCellSortedSet mvRefreshedPartitions,
                                           Map<BaseTableSnapshotInfo, PCellSortedSet> refTableAndPartitionNames);

    public MVRefreshParams getMvRefreshParams() {
        return mvRefreshParams;
    }

    /**
     * Get the retry times for the mv refresh processor.
     *
     * @param connectContext the current connect context
     * @return the retry times for the mv refresh processor, default is 1
     */
    public int getRetryTimes(ConnectContext connectContext) {
        return 1;
    }

    /**
     * Create a mv refresh partitioner by the mv's partition info.
     */
    private MVPCTRefreshPartitioner buildMvRefreshPartitioner(MaterializedView mv,
                                                              TaskRunContext context,
                                                              MVRefreshParams mvRefreshParams) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            return new MVPCTRefreshNonPartitioner(mvContext, context, db, mv, mvRefreshParams);
        } else if (partitionInfo.isRangePartition()) {
            return new MVPCTRefreshRangePartitioner(mvContext, context, db, mv, mvRefreshParams);
        } else if (partitionInfo.isListPartition()) {
            return new MVPCTRefreshListPartitioner(mvContext, context, db, mv, mvRefreshParams);
        } else {
            throw new DmlException(String.format("materialized view:%s in database:%s refresh failed: partition info %s not " +
                    "supported", mv.getName(), db.getFullName(), partitionInfo));
        }
    }

    /**
     * Get the next task run to be processed.
     *
     * @return the next task run to be processed, null if no next task run
     */
    public TaskRun getNextTaskRun() {
        return nextTaskRun;
    }

    /**
     * Get the materialized view task run context which contains the task run information.
     */
    @VisibleForTesting
    public MvTaskRunContext getMvContext() {
        return mvContext;
    }

    /**
     * Change default connect context when for mv refresh this is because:
     * - MV Refresh may take much resource to load base tables' data into the final materialized view.
     * - Those changes are set by default and also able to be changed by users for their needs.
     *
     * @param mvConnectCtx the connect context for the materialized view refresh
     */
    protected void changeDefaultConnectContextIfNeeded(ConnectContext mvConnectCtx,
                                                       Set<Table> baseTables) {
        // add resource group if resource group is enabled
        final TableProperty mvProperty = mv.getTableProperty();
        final SessionVariable mvSessionVariable = mvConnectCtx.getSessionVariable();
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

        if (!mvProperty.getProperties().containsKey(MV_SESSION_INSERT_TIMEOUT)
                && mvProperty.getProperties().containsKey(MV_SESSION_QUERY_TIMEOUT)) {
            // for compatibility
            mvProperty.getProperties().put(MV_SESSION_INSERT_TIMEOUT,
                    mvProperty.getProperties().get(MV_SESSION_QUERY_TIMEOUT));
        }

        // set insert_max_filter_ratio by default
        if (!isMVPropertyContains(SessionVariable.INSERT_MAX_FILTER_RATIO)) {
            mvSessionVariable.setInsertMaxFilterRatio(Config.mv_refresh_fail_on_filter_data ? 0 : 1);
        }
        // enable profile by default for mv refresh task
        if (!isMVPropertyContains(SessionVariable.ENABLE_PROFILE) && !mvSessionVariable.isEnableProfile()) {
            mvSessionVariable.setEnableProfile(Config.enable_mv_refresh_collect_profile);
        }
        // set the default new_planner_optimize_timeout for mv refresh
        if (!isMVPropertyContains(SessionVariable.NEW_PLANNER_OPTIMIZER_TIMEOUT)) {
            mvSessionVariable.setOptimizerExecuteTimeout(Config.mv_refresh_default_planner_optimize_timeout);
        }
        // set enable_materialized_view_rewrite by default
        if (!isMVPropertyContains(SessionVariable.ENABLE_MATERIALIZED_VIEW_REWRITE)
                && Config.enable_mv_refresh_query_rewrite) {
            // Only enable mv rewrite when there are more than one related mvs that can be rewritten by other mvs.
            if (isEnableMVRefreshQueryRewrite(mvConnectCtx, baseTables)) {
                mvSessionVariable.setEnableMaterializedViewRewrite(Config.enable_mv_refresh_query_rewrite);
                mvSessionVariable.setEnableMaterializedViewRewriteForInsert(Config.enable_mv_refresh_query_rewrite);
            }
        }
        // set nested_mv_rewrite_max_level by default, only rewrite one level
        if (!isMVPropertyContains(SessionVariable.NESTED_MV_REWRITE_MAX_LEVEL)) {
            mvSessionVariable.setNestedMvRewriteMaxLevel(1);
        }
        // always exclude the current mv name from rewrite
        mvSessionVariable.setQueryExcludingMVNames(mv.getName());
        mvConnectCtx.setUseConnectorMetadataCache(Optional.of(true));
    }

    private boolean isMVPropertyContains(String key) {
        final String mvKey = PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + key;
        return mv.getTableProperty().getProperties().containsKey(mvKey);
    }

    private boolean isEnableMVRefreshQueryRewrite(ConnectContext ctx,
                                                  Set<Table> baseTables) {
        return MvUtils.getRelatedMvs(ctx, 1, baseTables).size() > 1;
    }

    /**
     * Whether to enable precise refresh for external table base tables.
     * @return true if precise refresh is enabled for external table base tables, false otherwise
     */
    private boolean isEnableExternalTablePreciseRefresh() {
        if (!Config.enable_materialized_view_external_table_precise_refresh) {
            return false;
        }
        // if any base table is external table, enable precise refresh
        final List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            final Optional<Table> optTable = MvUtils.getTable(baseTableInfo);
            if (optTable.isEmpty()) {
                continue;
            }
            final Table table = optTable.get();
            if (!table.isCloudNativeTableOrMaterializedView()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Sync and check the partitions of the materialized view and its base tables.
     * @param taskRunContext the task run context which contains the task run information
     * @throws Exception if any error occurs during the sync and check
     */
    protected void syncAndCheckPCTPartitions(TaskRunContext taskRunContext) throws Exception {
        // The candidate partition info is used to refresh the external table
        Map<BaseTableSnapshotInfo, PCellSortedSet> baseTableCandidatePartitions = Maps.newHashMap();
        if (isEnableExternalTablePreciseRefresh) {
            try (Timer ignored = Tracers.watchScope("MVRefreshComputeCandidatePartitions")) {
                if (!syncPartitions()) {
                    throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                            mv.getName()));
                }
                PCellSortedSet mvCandidatePartition = getPCTMVToRefreshedPartitions(true);
                baseTableCandidatePartitions = getPCTRefTableRefreshPartitions(mvCandidatePartition);
            } catch (Exception e) {
                logger.warn("failed to compute candidate partitions in sync partitions",
                        DebugUtil.getRootStackTrace(e));
                // Since at here we sync partitions before the refreshExternalTable, the situation may happen that
                // the base-table not exists before refreshExternalTable, so we just need to swallow this exception
                if (e.getMessage() == null || !e.getMessage().contains("not exist")) {
                    throw e;
                }
            }
        }

        // Refresh the partition information of these base-table partitions, and create mv partitions if needed
        try (Timer ignored = Tracers.watchScope("MVRefreshSyncAndCheckPartitions")) {
            if (!syncAndCheckPCTPartitions(baseTableCandidatePartitions)) {
                throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                        mv.getName()));
            }
        }
    }

    /**
     * Sync base table's partition infos to be used later.
     */
    protected boolean syncPartitions() throws AnalysisException, LockTimeoutException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        // collect base table snapshot infos
        this.snapshotBaseTables = collectBaseTableSnapshotInfos();

        if (!mvContext.isExplain() && mvRefreshParams.isNonTentativeForce()) {
            // drop existing partitions for force refresh
            PCellSortedSet toRefreshPartitions = mvRefreshPartitioner.getMVPartitionsToRefreshByParams();
            if (toRefreshPartitions != null && !toRefreshPartitions.isEmpty()) {
                logger.info("force refresh, drop partitions: [{}]",
                        Joiner.on(",").join(toRefreshPartitions.getPartitionNames()));

                // first lock and drop partitions from a visible map
                Locker locker = new Locker();
                if (!locker.tryLockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.WRITE,
                        Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                    logger.warn("failed to lock database: {} in syncPartitions for force refresh", db.getFullName());
                    throw new DmlException("Force refresh failed, database:" + db.getFullName() + " not exist");
                }
                try {
                    // for non-partitioned MVs, or for complete refresh of partitioned MVs, just clear the visible
                    // version map directly since all partitions will be refreshed.
                    if (!mv.isPartitionedTable() || mvRefreshParams.isCompleteRefresh()) {
                        mv.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMap();
                    } else {
                        mv.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMapByMVPartitions(
                                toRefreshPartitions.getPartitionNames());
                    }
                } catch (Exception e) {
                    logger.warn("failed to clear version map {} for force refresh",
                            Joiner.on(",").join(toRefreshPartitions.getPartitionNames()),
                            DebugUtil.getRootStackTrace(e));
                    throw new AnalysisException("failed to clear version map for force refresh: " + e.getMessage());
                } finally {
                    locker.unLockTableWithIntensiveDbLock(db.getId(), this.mv.getId(), LockType.WRITE);
                }
            }
        }
        // do sync partitions (add or drop partitions) for materialized view
        boolean result = mvRefreshPartitioner.syncAddOrDropPartitions();
        logger.info("finish sync partitions, cost(ms): {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return result;
    }

    protected void updatePCTToRefreshMetas(TaskRunContext taskRunContext) throws Exception {
        this.pctMVToRefreshedPartitions = getPCTMVToRefreshedPartitions(false);
        // ref table of mv : refreshed partition names
        this.pctRefTableRefreshPartitions = getPCTRefTableRefreshPartitions(pctMVToRefreshedPartitions);
        // ref table of mv : refreshed partition names
        this.pctRefTablePartitionNames = PCellSetMapping.of(pctRefTableRefreshPartitions.entrySet().stream()
                .collect(Collectors.toMap(x -> x.getKey().getName(), Map.Entry::getValue)));
        this.updatePCTBaseTableSnapshotInfos(pctRefTableRefreshPartitions);
        // add a message into information_schema
        this.updatePCTMVToRefreshInfoIntoTaskRun(pctMVToRefreshedPartitions, pctRefTablePartitionNames);
        logger.info("mvToRefreshedPartitions:{}, refTableRefreshPartitions:{}",
                pctMVToRefreshedPartitions, pctRefTableRefreshPartitions);
    }

    /**
     * Build an AST for insert stmt
     *
     * @param ctx:                    connect context
     * @param mvTargetPartitionNames: the partitions to be refreshed
     */
    protected InsertStmt generateInsertAst(ConnectContext ctx,
                                           PCellSortedSet mvTargetPartitionNames,
                                           String definition) throws AnalysisException {
        final InsertStmt insertStmt =
                (InsertStmt) SqlParser.parse(definition, ctx.getSessionVariable()).get(0);
        // set target partitions
        if (PCellUtils.isNotEmpty(mvTargetPartitionNames)) {
            PartitionRef partitionRef = new PartitionRef(Lists.newArrayList(mvTargetPartitionNames.getPartitionNames()),
                    false, NodePosition.ZERO);
            insertStmt.setTargetPartitionNames(partitionRef);
        }

        // insert overwrite mv must set system = true
        insertStmt.setSystem(true);
        // if mv has set sort keys, materialized view's output columns
        // may be different from the defined query's output.
        // so set materialized view's defined outputs as target columns.
        final List<Integer> queryOutputIndexes = mv.getQueryOutputIndices();
        final List<Column> baseSchema = mv.getBaseSchemaWithoutGeneratedColumn();
        if (queryOutputIndexes != null && baseSchema.size() == queryOutputIndexes.size()) {
            final List<String> targetColumnNames = queryOutputIndexes.stream()
                    .map(baseSchema::get)
                    .map(Column::getName)
                    .map(String::toLowerCase) // case insensitive
                    .collect(Collectors.toList());
            insertStmt.setTargetColumnNames(targetColumnNames);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("generate insert-overwrite statement, materialized view's target partition names:{}, " +
                            "mv's target columns: {}, definition:{}",
                    mvTargetPartitionNames,
                    insertStmt.getTargetColumnNames() == null ? ""
                            : Joiner.on(",").join(insertStmt.getTargetColumnNames()),
                    definition);
        }
        return insertStmt;
    }

    /**
     * Update task run status's extra message to add more information for information_schema if possible.
     *
     * @param action: a consumer to update the task run status
     */
    protected void updateTaskRunStatus(Consumer<TaskRunStatus> action) {
        if (this.mvContext == null || this.mvContext.getStatus() == null) {
            return;
        }

        // ignore exception for update task run status
        try {
            action.accept(this.mvContext.getStatus());
        } catch (Exception e) {
            logger.warn("failed to update task run status for mv refresh, task run id: {}, error: {}",
                    this.mvContext.getTaskRunId(), DebugUtil.getRootStackTrace(e));
        }
    }

    /**
     * Get the MVTaskRunExtraMessage from the mv context's status.
     *
     * @return the MVTaskRunExtraMessage if exists, null otherwise
     */
    @VisibleForTesting
    public MVTaskRunExtraMessage getMVTaskRunExtraMessage() {
        if (this.mvContext.getStatus() == null) {
            return null;
        }
        return this.mvContext.getStatus().getMvTaskRunExtraMessage();
    }

    protected void refreshExternalTable(Map<BaseTableSnapshotInfo, PCellSortedSet> baseTableCandidatePartitions) {
        final List<Pair<Table, BaseTableInfo>> toRepairTables = new ArrayList<>();
        // use it if refresh external table fails
        final ConnectContext connectContext = mvContext.getCtx();
        final List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            final Optional<Database> dbOpt =
                    GlobalStateMgr.getCurrentState().getMetadataMgr().getDatabase(connectContext, baseTableInfo);
            if (dbOpt.isEmpty()) {
                logger.warn("database {} do not exist in refreshing materialized view", baseTableInfo.getDbInfoStr());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }

            final Optional<Table> optTable = MvUtils.getTable(baseTableInfo);
            if (optTable.isEmpty()) {
                logger.warn("table {} do not exist when refreshing materialized view", baseTableInfo.getTableInfoStr());
                mv.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            // refresh old table
            final Table table = optTable.get();
            // if table is native table or materialized view or connector view or external table, no need to refresh
            if (table.isNativeTableOrMaterializedView() || table.isView()
                    || MaterializedViewAnalyzer.isExternalTableFromResource(table)) {
                logger.debug("No need to refresh table:{} because it is native table or mv or connector view",
                        baseTableInfo.getTableInfoStr());
                continue;
            }
            final BaseTableSnapshotInfo snapshotInfo = buildBaseTableSnapshotInfo(baseTableInfo, table);
            final PCellSortedSet basePartitions = baseTableCandidatePartitions.get(snapshotInfo);
            if (PCellUtils.isNotEmpty(basePartitions)) {
                // only refresh referenced partitions, to reduce metadata overhead
                final List<String> realPartitionNames = basePartitions.stream()
                        .flatMap(pCell -> mvContext.getExternalTableRealPartitionName(table, pCell.name()).stream())
                        .collect(Collectors.toList());
                connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, realPartitionNames, false);
            } else {
                // refresh the whole table, which may be costly in extreme case
                connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), table, Lists.newArrayList(), true);
            }
            // should clear query cache
            connectContext.getGlobalStateMgr().getMetadataMgr().removeQueryMetadata();

            // check new table
            final Optional<Table> optNewTable = MvUtils.getTable(baseTableInfo);
            if (optNewTable.isEmpty()) {
                logger.warn("table {} does not exist after refreshing materialized view",
                        baseTableInfo.getTableInfoStr());
                mv.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

            // only collect to-repair tables when the table is different from the old one by checking the table identifier
            final Table newTable = optNewTable.get();
            if (!baseTableInfo.getTableIdentifier().equals(table.getTableIdentifier())) {
                logger.info("table {} changed after refreshing materialized view, old id: {}, new id: {}",
                        baseTableInfo.getTableInfoStr(), table.getTableIdentifier(), newTable.getTableIdentifier());
                if (currentRefreshMode.isIncremental()) {
                    throw new SemanticException("Materialized view base table: %s changed, " +
                            "cannot do incremental refresh in %s mode.",
                            baseTableInfo.getTableInfoStr(), currentRefreshMode);
                }
                toRepairTables.add(Pair.create(newTable, baseTableInfo));
            }
        }

        // do repair if needed
        if (!toRepairTables.isEmpty()) {
            logger.info("need to repair mv:{} for base table changed: {}",
                    mv.getName(), Joiner.on(",").join(toRepairTables.stream()
                            .map(t -> t.second.getTableInfoStr()).iterator()));
            MVPCTMetaRepairer.repairMetaIfNeeded(db, mv, toRepairTables);
        }
    }

    /**
     * Collect all deduplicated databases of the materialized view's base tables.
     * @return: the deduplicated databases of the materialized view's base tables,
     * throw exception if the database does not exist.
     */
    protected LockParams collectDatabases() {
        final LockParams lockParams = new LockParams();
        final ConnectContext connectContext = mvContext.getCtx();
        for (BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
            Optional<Database> dbOpt = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getDatabase(connectContext, baseTableInfo);
            if (dbOpt.isEmpty()) {
                logger.warn("database {} do not exist", baseTableInfo.getDbInfoStr());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }
            Database db = dbOpt.get();
            lockParams.add(db, baseTableInfo.getTableId());
        }
        return lockParams;
    }

    /**
     * Collect all base table snapshot infos for the mv which the snapshot infos are kept and used in the final
     * update meta phase.
     * 1. deep copy of the base table's metadata may be time costing, we can optimize it later.
     * 2. no needs to lock the base table's metadata since the metadata is not changed during the refresh process.
     * @return the base table and its snapshot info map
     */
    @VisibleForTesting
    public Map<Long, BaseTableSnapshotInfo> collectBaseTableSnapshotInfos() throws LockTimeoutException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        final LockParams lockParams = collectDatabases();
        final Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(lockParams, LockType.READ, Config.mv_refresh_try_lock_timeout_ms,
                TimeUnit.MILLISECONDS)) {
            logger.warn("failed to lock database: {} in collectBaseTableSnapshotInfos for mv refresh", lockParams);
            throw new LockTimeoutException("Failed to lock database: " + lockParams + " in collectBaseTableSnapshotInfos");
        }

        final Map<Long, BaseTableSnapshotInfo> tables = Maps.newHashMap();
        try {
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                final Optional<Table> tableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
                if (tableOpt.isEmpty()) {
                    logger.warn("table {} doesn't exist", baseTableInfo.getTableInfoStr());
                    throw new DmlException("Materialized view base table: %s not exist.",
                            baseTableInfo.getTableInfoStr());
                }

                // NOTE: DeepCopy.copyWithGson is very time costing, use `copyOnlyForQuery` to reduce the cost.
                // TODO: Implement a `SnapshotTable` later which can use the copied table or transfer to the real table.
                final Table table = tableOpt.get();

                // Check if the table is an Iceberg table with partition evolution
                if (table instanceof IcebergTable) {
                    IcebergTable icebergTable = (IcebergTable) table;
                    if (icebergTable.getNativeTable().specs().size() > 1) {
                        throw new DmlException("Do not support refresh materialized view when base iceberg table " +
                                table.getName() + " has done partition evolution");
                    }
                }

                if (table.isNativeTableOrMaterializedView()) {
                    OlapTable copied = null;
                    if (table.isOlapOrCloudNativeTable()) {
                        copied = new OlapTable();
                    } else {
                        copied = new MaterializedView();
                    }
                    final OlapTable olapTable = (OlapTable) table;
                    olapTable.copyOnlyForQuery(copied);
                    tables.put(table.getId(), buildBaseTableSnapshotInfo(baseTableInfo, copied));
                } else if (table.isView()) {
                    // skip to collect snapshots for views
                } else {
                    // for other table types, use the table directly which needs to lock if visits the table metadata.
                    tables.put(table.getId(), buildBaseTableSnapshotInfo(baseTableInfo, table));
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
        }
        logger.info("collect base table snapshot infos cost: {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return tables;
    }

    /**
     * @param tentative: if true, it means this is called in the first phase to compute candidate partitions and not the
     *                 standard phase to get final partitions to refresh.
     */
    @VisibleForTesting
    public PCellSortedSet getPCTMVToRefreshedPartitions(boolean tentative) throws AnalysisException, LockTimeoutException {
        // change mv refresh params if needed
        mvRefreshParams.setIsTentative(tentative);

        final PCellSortedSet mvToRefreshedPartitions = mvRefreshPartitioner.getMVToRefreshedPartitions(snapshotBaseTables);
        // update mv extra message
        if (!mvRefreshParams.isTentative()) {
            updateTaskRunStatus(status -> {
                MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
                extraMessage.setForceRefresh(mvRefreshParams.isForce());
                extraMessage.setPartitionStart(mvRefreshParams.getRangeStart());
                extraMessage.setPartitionEnd(mvRefreshParams.getRangeEnd());
            });
        }
        return mvToRefreshedPartitions;
    }

    /**
     * return to-refreshed base table's table name and partition names mapping
     */
    @VisibleForTesting
    public Map<BaseTableSnapshotInfo, PCellSortedSet> getPCTRefTableRefreshPartitions(PCellSortedSet mvToRefreshedPartitions) {
        Map<BaseTableSnapshotInfo, PCellSortedSet> refTableAndPartitionNames = Maps.newHashMap();
        Map<String, Map<Table, PCellSortedSet>> mvToBaseNameRefs = mvContext.getMvRefBaseTableIntersectedPartitions();
        if (mvToBaseNameRefs == null || mvToBaseNameRefs.isEmpty()) {
            return refTableAndPartitionNames;
        }
        for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            PCellSortedSet needRefreshTablePartitionNames = null;
            for (PCellWithName pCell : mvToRefreshedPartitions.getPartitions()) {
                String mvPartitionName = pCell.name();
                if (!mvToBaseNameRefs.containsKey(mvPartitionName)) {
                    continue;
                }
                Map<Table, PCellSortedSet> mvToBaseNameRef = mvToBaseNameRefs.get(mvPartitionName);
                if (mvToBaseNameRef.containsKey(snapshotTable)) {
                    if (needRefreshTablePartitionNames == null) {
                        needRefreshTablePartitionNames = PCellSortedSet.of();
                    }
                    // The table in this map has related partition with mv
                    // It's ok to add empty set for a table, means no partition corresponding to this mv partition
                    needRefreshTablePartitionNames.addAll(mvToBaseNameRef.get(snapshotTable));
                } else {
                    logger.info("ref-base-table {} is not found in `mvRefBaseTableIntersectedPartitions` " +
                            "because of empty update", snapshotTable.getName());
                }
            }
            if (needRefreshTablePartitionNames != null) {
                refTableAndPartitionNames.put(snapshotInfo, needRefreshTablePartitionNames);
            }
        }
        return refTableAndPartitionNames;
    }

    /**
     * Sync partitions of base tables and check whether they are changing anymore
     */
    protected boolean syncAndCheckPCTPartitions(Map<BaseTableSnapshotInfo, PCellSortedSet> baseTableCandidatePartitions)
            throws AnalysisException, LockTimeoutException {
        // collect partition infos of ref base tables
        int retryNum = 0;
        boolean checked = false;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (!checked && retryNum++ < Config.max_mv_check_base_table_change_retry_times) {
            mvEntity.increaseRefreshRetryMetaCount(1L);
            try (Timer ignored = Tracers.watchScope("MVRefreshExternalTable")) {
                // refresh external table meta cache before sync partitions
                refreshExternalTable(baseTableCandidatePartitions);
            }

            if (!isEnableExternalTablePreciseRefresh || retryNum > 1) {
                try (Timer ignored = Tracers.watchScope("MVRefreshSyncPartitions")) {
                    // sync partitions between mv and base tables out of lock
                    // do it outside lock because it is a time-cost operation
                    if (!syncPartitions()) {
                        logger.warn("Sync partitions failed.");
                        return false;
                    }
                }
            }

            try (Timer ignored = Tracers.watchScope("MVRefreshCheckBaseTableChange")) {
                // check whether there are partition changes for base tables, eg: partition rename
                // retry to sync partitions if any base table changed the partition infos
                if (checkPCTBaseTablePartitionChange()) {
                    logger.info("materialized view base partition has changed. " +
                            "retry to sync partitions, retryNum:{}", retryNum);
                    // sleep 100ms
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    continue;
                }
            }
            checked = true;
        }
        Tracers.record("MVRefreshSyncPartitionsRetryTimes", String.valueOf(retryNum));
        logger.info("sync and check mv partition changing after {} times: {}, costs: {} ms",
                retryNum, checked, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return checked;
    }

    /**
     * Check whether the base table's partition has changed or not. Wait to refresh until all mv's base tables
     * don't change again.
     *
     * @return: true if the base table's partition has changed, otherwise false.
     */
    private boolean checkPCTBaseTablePartitionChange() throws LockTimeoutException {
        LockParams lockParams = collectDatabases();
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(lockParams,
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            logger.warn("failed to lock database: {} in checkBaseTablePartitionChange", lockParams);
            throw new LockTimeoutException("Failed to lock database: " + lockParams
                    + " in checkBaseTablePartitionChange");
        }
        // check snapshotBaseTables and current tables in catalog
        try {
            return snapshotBaseTables.values().stream()
                    .anyMatch(snapshotInfo -> ((PCTTableSnapshotInfo) snapshotInfo).hasBaseTableChanged(mv));
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
        }
    }

    protected void updatePCTMVToRefreshInfoIntoTaskRun(PCellSortedSet finalMvToRefreshedPartitions,
                                                       PCellSetMapping finalRefTablePartitionNames) {
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setMvPartitionsToRefresh(finalMvToRefreshedPartitions.getPartitionNames());
            extraMessage.setRefBasePartitionsToRefreshMap(finalRefTablePartitionNames.getRefTablePartitionNames());
        });
    }

    @VisibleForTesting
    public void updatePCTBaseTableSnapshotInfos(Map<BaseTableSnapshotInfo, PCellSortedSet> refTableAndPartitionNames) {
        Map<Table, PCellSetMapping> baseTableToMvNameRefs = mvContext.getRefBaseTableMVIntersectedPartitions();
        // update partition infos for each base table snapshot info
        for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            if (!(snapshotInfo instanceof PCTTableSnapshotInfo)) {
                continue; // only update PCTTableSnapshotInfo
            }
            PCTTableSnapshotInfo pctTableSnapshotInfo = (PCTTableSnapshotInfo) snapshotInfo;
            Table baseTable = snapshotInfo.getBaseTable();
            List<String> partitionNames;
            if (refTableAndPartitionNames.containsKey(snapshotInfo)) {
                if (baseTableToMvNameRefs == null || !baseTableToMvNameRefs.containsKey(baseTable)) {
                    logger.warn("materialized view {} has no mv name references for base table {}, " +
                                    "so use the refTableAndPartitionNames directly",
                            mv.getName(), baseTable.getName());
                    continue;
                }
                partitionNames = refTableAndPartitionNames.get(snapshotInfo).stream()
                        .flatMap(pCell ->
                                mvContext.getExternalTableRealPartitionName(baseTable, pCell.name()).stream())
                        .collect(Collectors.toList());
            } else {
                partitionNames = getPCTNonRefTableRefreshPartitions(baseTable);
            }
            pctTableSnapshotInfo.updatePartitionInfos(partitionNames);
        }
    }

    /**
     * Return all non-ref base table and refreshed partitions.
     */
    private List<String> getPCTNonRefTableRefreshPartitions(Table table) {
        // Ensure the result is a new list to be sorted later.
        if (table.isNativeTableOrMaterializedView()) {
            return Lists.newArrayList(((OlapTable) table).getVisiblePartitionNames());
        } else if (MVPCTRefreshPartitioner.isPartitionRefreshSupported(table)) {
            return Lists.newArrayList(PartitionUtil.getPartitionNames(table));
        } else {
            return Lists.newArrayList();
        }
    }

    /**
     * After mv is refreshed, update materialized view's meta info to record history refreshes.
     * @param refTableAndPartitionNames : refreshed base table and its partition names mapping.
     */
    public void updatePCTMeta(ExecPlan execPlan,
                              PCellSortedSet mvRefreshedPartitions,
                              Map<BaseTableSnapshotInfo, PCellSortedSet> refTableAndPartitionNames,
                              Map<BaseTableInfo, TvrVersionRange> tempMvTvrVersionRangeMap) {
        // check
        Table mv = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), this.mv.getId());
        if (mv == null) {
            throw new DmlException("update meta failed. materialized view:" + this.mv.getName() + " not exist");
        }
        // check
        if (mvRefreshedPartitions == null || refTableAndPartitionNames == null) {
            logger.info("no partitions to refresh, mvRefreshedPartitions:{}, refTableAndPartitionNames:{}",
                    mvRefreshedPartitions, refTableAndPartitionNames);
            return;
        }

        // update mv's version info
        Set<Long> refBaseTableIds = refTableAndPartitionNames.keySet().stream()
                .map(t -> t.getId())
                .collect(Collectors.toSet());

        Locker locker = new Locker();
        // update the meta if succeed
        if (!locker.tryLockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.WRITE,
                Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            logger.warn("failed to lock database: {} in updateMeta for mv refresh", db.getFullName());
            throw new DmlException("update meta failed. database:" + db.getFullName() + " not exist");
        }

        MVVersionManager mvVersionManager = new MVVersionManager(this.mv, mvContext);
        try {
            mvVersionManager.updateMVVersionInfo(snapshotBaseTables, mvRefreshedPartitions,
                    refBaseTableIds, refTableAndPartitionNames, tempMvTvrVersionRangeMap);
        } catch (Exception e) {
            logger.warn("update final meta failed after mv refreshed:", DebugUtil.getRootStackTrace(e));
            throw e;
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), this.mv.getId(), LockType.WRITE);
        }

        // update mv status message
        updateTaskRunStatus(status -> {
            try {
                MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
                PartitionNameSetMap baseTableRefreshedPartitionsByExecPlan =
                        MVTraceUtils.getBaseTableRefreshedPartitionsByExecPlan(this.mv, execPlan);
                extraMessage.setBasePartitionsToRefreshMap(
                        baseTableRefreshedPartitionsByExecPlan.getBasePartitionsToRefreshMap());
            } catch (Exception e) {
                // just log warn and no throw exceptions for an updating task runs message.
                logger.warn("update task run messages failed:", DebugUtil.getRootStackTrace(e));
            }
        });
    }
}
