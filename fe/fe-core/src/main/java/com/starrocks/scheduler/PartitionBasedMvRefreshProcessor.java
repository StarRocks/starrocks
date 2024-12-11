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
<<<<<<< HEAD
=======
import com.google.common.base.Strings;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.analysis.Expr;
<<<<<<< HEAD
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
=======
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
<<<<<<< HEAD
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.SinglePartitionInfo;
=======
import com.starrocks.catalog.ResourceGroup;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
<<<<<<< HEAD
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionUtil;
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
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PartitionDiffer;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.common.RangePartitionDiff;
=======
import com.starrocks.common.StarRocksException;
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
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.mv.MVPCTMetaRepairer;
import com.starrocks.scheduler.mv.MVPCTRefreshListPartitioner;
import com.starrocks.scheduler.mv.MVPCTRefreshNonPartitioner;
import com.starrocks.scheduler.mv.MVPCTRefreshPartitioner;
import com.starrocks.scheduler.mv.MVPCTRefreshPlanBuilder;
import com.starrocks.scheduler.mv.MVPCTRefreshRangePartitioner;
import com.starrocks.scheduler.mv.MVRefreshParams;
import com.starrocks.scheduler.mv.MVTraceUtils;
import com.starrocks.scheduler.mv.MVVersionManager;
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections.CollectionUtils;
<<<<<<< HEAD
import org.apache.commons.collections4.ListUtils;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
<<<<<<< HEAD
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
=======
import java.util.Collections;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
<<<<<<< HEAD
=======
import static com.starrocks.scheduler.TaskRun.MV_ID;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

/**
 * Core logic of materialized view refresh task run
 * PartitionBasedMvRefreshProcessor is not thread safe for concurrent runs of the same materialized view
 */
public class PartitionBasedMvRefreshProcessor extends BaseTaskRunProcessor {

    private static final Logger LOG = LogManager.getLogger(PartitionBasedMvRefreshProcessor.class);
    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);

<<<<<<< HEAD
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

    private Database database;
    private MaterializedView materializedView;
    private MvTaskRunContext mvContext;
    // table id -> <base table info, snapshot table>
    private Map<Long, Pair<BaseTableInfo, Table>> snapshotBaseTables = Maps.newHashMap();
=======
    // session.enable_spill
    private static final String MV_SESSION_ENABLE_SPILL =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.ENABLE_SPILL;
    // session.query_timeout. Deprecated, only for compatibility with old version
    private static final String MV_SESSION_QUERY_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.QUERY_TIMEOUT;
    // session.insert_timeout
    private static final String MV_SESSION_INSERT_TIMEOUT =
            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + SessionVariable.INSERT_TIMEOUT;

    private Database db;
    private MaterializedView materializedView;
    private MvTaskRunContext mvContext;

    // Collect all bases tables of the materialized view to be updated meta after mv refresh success.
    // format :     table id -> <base table info, snapshot table>
    private Map<Long, TableSnapshotInfo> snapshotBaseTables = Maps.newHashMap();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    private long oldTransactionVisibleWaitTimeout;

    @VisibleForTesting
    private RuntimeProfile runtimeProfile;
<<<<<<< HEAD
=======
    private MVPCTRefreshPartitioner mvRefreshPartitioner;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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
<<<<<<< HEAD
=======
        // register tracers
        Tracers.register(context.getCtx());
        QueryDebugOptions queryDebugOptions = context.getCtx().getSessionVariable().getQueryDebugOptions();
        // init to collect the base timer for refresh profile
        Tracers.Mode mvRefreshTraceMode = queryDebugOptions.getMvRefreshTraceMode();
        Tracers.Module mvRefreshTraceModule = queryDebugOptions.getMvRefreshTraceModule();
        Tracers.init(mvRefreshTraceMode, mvRefreshTraceModule, true, false);

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        IMaterializedViewMetricsEntity mvEntity = null;
        ConnectContext connectContext = context.getCtx();
        QueryMaterializationContext queryMVContext = new QueryMaterializationContext();
        connectContext.setQueryMVContext(queryMVContext);
        try {
            // prepare
<<<<<<< HEAD
            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshPrepare")) {
=======
            try (Timer ignored = Tracers.watchScope("MVRefreshPrepare")) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                prepare(context);
            }

            // do refresh
<<<<<<< HEAD
            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshDoWholeRefresh")) {
=======
            try (Timer ignored = Tracers.watchScope("MVRefreshDoWholeRefresh")) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
                connectContext.getPlannerProfile().build(runtimeProfile);
            }
=======
                Tracers.toRuntimeProfile(runtimeProfile);
            }
            LOG.info("Refresh mv {} trace logs: {}", materializedView.getName(), Tracers.getTrace(mvRefreshTraceMode));
            Tracers.close();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            postProcess();
        }
    }

    /**
     * Sync partitions of base tables and check whether they are changing anymore
     */
    private boolean syncAndCheckPartitions(TaskRunContext context,
                                           IMaterializedViewMetricsEntity mvEntity,
<<<<<<< HEAD
                                           Map<Table, Set<String>> baseTableCandidatePartitions) {
        // collect partition infos of ref base tables
=======
                                           Map<TableSnapshotInfo, Set<String>> baseTableCandidatePartitions)
            throws AnalysisException, LockTimeoutException {
        // collect partition infos of ref base tables
        LOG.debug("Start to sync and check partitions for mv: {}", materializedView.getName());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        int retryNum = 0;
        boolean checked = false;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (!checked && retryNum++ < Config.max_mv_check_base_table_change_retry_times) {
            mvEntity.increaseRefreshRetryMetaCount(1L);
<<<<<<< HEAD

            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshExternalTable")) {
=======
            try (Timer ignored = Tracers.watchScope("MVRefreshExternalTable")) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                // refresh external table meta cache before sync partitions
                refreshExternalTable(context, baseTableCandidatePartitions);
            }

            if (!Config.enable_materialized_view_external_table_precise_refresh) {
<<<<<<< HEAD
                try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshSyncPartitions")) {
                    // sync partitions between materialized view and base tables out of lock
                    // do it outside lock because it is a time-cost operation
                    syncPartitions(context);
                }
            }

            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshCheckBaseTableChange")) {
=======
                try (Timer ignored = Tracers.watchScope("MVRefreshSyncPartitions")) {
                    // sync partitions between materialized view and base tables out of lock
                    // do it outside lock because it is a time-cost operation
                    if (!syncPartitions()) {
                        LOG.warn("Sync partitions failed for materialized view: {}", materializedView.getName());
                        return false;
                    }
                }
            }

            try (Timer ignored = Tracers.watchScope("MVRefreshCheckBaseTableChange")) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        PlannerProfile.addCustomProperties("MVRefreshSyncPartitionsRetryTimes", String.valueOf(retryNum));

        LOG.info("materialized view {} after checking partitions change {} times: {}, costs: {} ms",
=======
        Tracers.record("MVRefreshSyncPartitionsRetryTimes", String.valueOf(retryNum));

        LOG.info("Sync and check materialized view {} partition changing after {} times: {}, costs: {} ms",
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                materializedView.getName(), retryNum, checked, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return checked;
    }

<<<<<<< HEAD
    private Set<String> checkMvToRefreshedPartitions(TaskRunContext context, boolean tentative)
            throws AnalysisException {
        if (!database.tryReadLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database: " + database.getFullName());
        }
        try {
            Set<String> mvToRefreshedPartitions = getPartitionsToRefreshForMaterializedView(context.getProperties(),
                    tentative);
=======
    /**
     * Compute the partitioned to be refreshed in this task, according to [start, end), ttl, and other context info
     * If it's tentative, only return the result rather than modify any state
     * IF it's not, it would modify the context state, like `NEXT_PARTITION_START`
     */
    private Set<String> checkMvToRefreshedPartitions(TaskRunContext context, boolean tentative)
            throws AnalysisException, LockTimeoutException {
        Set<String> mvToRefreshedPartitions = null;
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(db.getId(), materializedView.getId(),
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            LOG.warn("Failed to lock database: {} in checkMvToRefreshedPartitions for mv refresh {}", db.getFullName(),
                    materializedView.getName());
            throw new LockTimeoutException("Failed to lock database: " + db.getFullName());
        }
        try {
            Set<String> mvPotentialPartitionNames = Sets.newHashSet();
            mvToRefreshedPartitions = getPartitionsToRefreshForMaterializedView(
                    context.getProperties(), mvPotentialPartitionNames, tentative);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (mvToRefreshedPartitions.isEmpty()) {
                LOG.info("no partitions to refresh for materialized view {}", materializedView.getName());
                return mvToRefreshedPartitions;
            }
<<<<<<< HEAD
            // Only refresh the first partition refresh number partitions, other partitions will generate new tasks
            filterPartitionByRefreshNumber(mvToRefreshedPartitions, materializedView, tentative);
            LOG.info("materialized view partitions to refresh:{}", mvToRefreshedPartitions);
            return mvToRefreshedPartitions;
        } finally {
            database.readUnlock();
        }
=======

            // Only refresh the first partition refresh number partitions, other partitions will generate new tasks
            filterPartitionByRefreshNumber(mvToRefreshedPartitions, mvPotentialPartitionNames, materializedView,
                    tentative);
            int partitionRefreshNumber = materializedView.getTableProperty().getPartitionRefreshNumber();
            LOG.info("Filter partitions to refresh for materialized view {}, partitionRefreshNumber={}, " +
                            "partitionsToRefresh:{}, mvPotentialPartitionNames:{}, next start:{}, next end:{}, " +
                            "next list values:{}",
                    materializedView.getName(), partitionRefreshNumber, mvToRefreshedPartitions, mvPotentialPartitionNames,
                    mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(), mvContext.getNextPartitionValues());
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), materializedView.getId(), LockType.READ);
        }
        return mvToRefreshedPartitions;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    private RefreshJobStatus doMvRefresh(TaskRunContext context, IMaterializedViewMetricsEntity mvEntity) {
        long startRefreshTs = System.currentTimeMillis();

<<<<<<< HEAD
=======
        // log mv basic info, it may throw exception if mv is invalid since base table has dropped
        try {
            LOG.debug("Start to refresh mv:{}, refBaseTablePartitionExprMap:{}," +
                            "refBaseTablePartitionSlotMap:{}, refBaseTablePartitionColumnMap:{}," +
                            "baseTableInfos:{}", materializedView.getName(),
                    materializedView.getRefBaseTablePartitionExprs(), materializedView.getRefBaseTablePartitionSlots(),
                    materializedView.getRefBaseTablePartitionColumns(),
                    MvUtils.formatBaseTableInfos(materializedView.getBaseTableInfos()));
        } catch (Throwable e) {
            LOG.warn("Log mv basic info failed:", e);
        }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        // refresh materialized view
        RefreshJobStatus result = doRefreshMaterializedViewWithRetry(context, mvEntity);

        // do not generate next task run if the current task run is killed
        if (mvContext.hasNextBatchPartition() && !mvContext.getTaskRun().isKilled()) {
            generateNextTaskRun();
        }

        long refreshDurationMs = System.currentTimeMillis() - startRefreshTs;
<<<<<<< HEAD
        LOG.info("Refresh {} success, cost time(s): {}", materializedView.getName(),
                DebugUtil.DECIMAL_FORMAT_SCALE_3.format(refreshDurationMs / 1000.0));
=======
        LOG.info("Refresh {} success, cost time(ms): {}", materializedView.getName(),
                DebugUtil.DECIMAL_FORMAT_SCALE_3.format(refreshDurationMs));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
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
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
     * Retry the `doRefreshMaterializedView` method to avoid insert fails in occasional cases.
     */
    private RefreshJobStatus doRefreshMaterializedViewWithRetry(TaskRunContext taskRunContext,
                                                                IMaterializedViewMetricsEntity mvEntity) throws DmlException {
        // Use current connection variables instead of mvContext's session variables to be better debug.
        int maxRefreshMaterializedViewRetryNum = getMaxRefreshMaterializedViewRetryNum(taskRunContext.getCtx());
<<<<<<< HEAD
=======
        LOG.info("Start to refresh mv:{} with retry times:{}",
                materializedView.getName(), maxRefreshMaterializedViewRetryNum);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        Throwable lastException = null;
        int lockFailedTimes = 0;
        int refreshFailedTimes = 0;
        while (refreshFailedTimes < maxRefreshMaterializedViewRetryNum &&
                lockFailedTimes < Config.max_mv_refresh_try_lock_failure_retry_times) {
            try {
<<<<<<< HEAD
                PlannerProfile.addCustomProperties("MVRefreshRetryTimes", String.valueOf(refreshFailedTimes));
                PlannerProfile.addCustomProperties("MVRefreshLockRetryTimes", String.valueOf(lockFailedTimes));
=======
                Tracers.record("MVRefreshRetryTimes", String.valueOf(refreshFailedTimes));
                Tracers.record("MVRefreshLockRetryTimes", String.valueOf(lockFailedTimes));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                return doRefreshMaterializedView(taskRunContext, mvEntity);
            } catch (LockTimeoutException e) {
                // if lock timeout, retry to refresh
                lockFailedTimes += 1;
                LOG.warn("Refresh materialized view {} failed at {}th time because try lock failed: {}",
                        this.materializedView.getName(), lockFailedTimes, DebugUtil.getStackTrace(e));
                lastException = e;
            } catch (Throwable e) {
                refreshFailedTimes += 1;
                LOG.warn("Refresh materialized view {} failed at {}th time: {}",
                        this.materializedView.getName(), refreshFailedTimes, DebugUtil.getRootStackTrace(e));
                lastException = e;
            }

            // sleep some time if it is not the last retry time
            Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        }

        // throw the last exception if all retries failed
        String errorMsg = MvUtils.shrinkToSize(DebugUtil.getRootStackTrace(lastException), MAX_FIELD_VARCHAR_LENGTH);
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
        // 0. Compute the base-table partitions to check for external table
        // The candidate partition info is used to refresh the external table
<<<<<<< HEAD
        Map<Table, Set<String>> baseTableCandidatePartitions = Maps.newHashMap();
        if (Config.enable_materialized_view_external_table_precise_refresh) {
            try (PlannerProfile.ScopedTimer ignored =
                    PlannerProfile.getScopedTimer("MVRefreshComputeCandidatePartitions")) {
                syncPartitions(context);
=======
        Map<TableSnapshotInfo, Set<String>> baseTableCandidatePartitions = Maps.newHashMap();
        if (Config.enable_materialized_view_external_table_precise_refresh) {
            try (Timer ignored = Tracers.watchScope("MVRefreshComputeCandidatePartitions")) {
                if (!syncPartitions()) {
                    throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                            materializedView.getName()));
                }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                Set<String> mvCandidatePartition = checkMvToRefreshedPartitions(context, true);
                baseTableCandidatePartitions = getRefTableRefreshPartitions(mvCandidatePartition);
            } catch (Exception e) {
                LOG.warn("Failed to compute candidate partitions for materialized view {} in sync partitions",
<<<<<<< HEAD
                        materializedView.getName(), e);
=======
                        materializedView.getName(), DebugUtil.getRootStackTrace(e));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                // Since at here we sync partitions before the refreshExternalTable, the situation may happen that
                // the base-table not exists before refreshExternalTable, so we just need to swallow this exception
                if (e.getMessage() == null || !e.getMessage().contains("not exist")) {
                    throw e;
                }
            }
        }

        // 1. Refresh the partition information of these base-table partitions, and create mv partitions if needed
<<<<<<< HEAD
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshSyncAndCheckPartitions")) {
=======
        try (Timer ignored = Tracers.watchScope("MVRefreshSyncAndCheckPartitions")) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (!syncAndCheckPartitions(context, mvEntity, baseTableCandidatePartitions)) {
                throw new DmlException(String.format("materialized view %s refresh task failed: sync partition failed",
                        materializedView.getName()));
            }
        }

        Set<String> mvToRefreshedPartitions = null;
<<<<<<< HEAD
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshCheckMvToRefreshedPartitions")) {
=======
        Map<TableSnapshotInfo, Set<String>> refTableRefreshPartitions = null;
        Map<String, Set<String>> refTablePartitionNames = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshCheckMVToRefreshPartitions")) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            mvToRefreshedPartitions = checkMvToRefreshedPartitions(context, false);
            if (CollectionUtils.isEmpty(mvToRefreshedPartitions)) {
                return RefreshJobStatus.EMPTY;
            }
<<<<<<< HEAD
        }

        // Get to refreshed base table partition infos.
        Map<Table, Set<String>> refTableRefreshPartitions = getRefTableRefreshPartitions(mvToRefreshedPartitions);
        Map<String, Set<String>> refTablePartitionNames = refTableRefreshPartitions.entrySet().stream()
                .collect(Collectors.toMap(x -> x.getKey().getName(), Map.Entry::getValue));
        LOG.info("materialized:{}, mvToRefreshedPartitions:{}, refTableRefreshPartitions:{}",
                materializedView.getName(), mvToRefreshedPartitions, refTableRefreshPartitions);
        // add a message into information_schema
        logMvToRefreshInfoIntoTaskRun(mvToRefreshedPartitions, refTablePartitionNames);

        ///// 2. execute the ExecPlan of insert stmt
        InsertStmt insertStmt = null;
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshPrepareRefreshPlan")) {
            insertStmt = prepareRefreshPlan(mvToRefreshedPartitions, refTablePartitionNames);
        }
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshMaterializedView")) {
=======
            // ref table of materialized view : refreshed partition names
            refTableRefreshPartitions = getRefTableRefreshPartitions(mvToRefreshedPartitions);
            // ref table of materialized view : refreshed partition names
            refTablePartitionNames = refTableRefreshPartitions.entrySet().stream()
                    .collect(Collectors.toMap(x -> x.getKey().getName(), Map.Entry::getValue));
            LOG.info("Check mv {} to refresh partitions: mvToRefreshedPartitions:{}, " +
                            "refTableRefreshPartitions:{}",
                    materializedView.getName(), mvToRefreshedPartitions, refTableRefreshPartitions);
            // add a message into information_schema
            logMvToRefreshInfoIntoTaskRun(mvToRefreshedPartitions, refTablePartitionNames);
            updateBaseTablePartitionSnapshotInfos(refTableRefreshPartitions);
        }

        ///// 2. execute the ExecPlan of insert stmt
        InsertStmt insertStmt = null;
        try (Timer ignored = Tracers.watchScope("MVRefreshPrepareRefreshPlan")) {
            insertStmt = prepareRefreshPlan(mvToRefreshedPartitions, refTablePartitionNames);
        }
        try (Timer ignored = Tracers.watchScope("MVRefreshMaterializedView")) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            refreshMaterializedView(mvContext, mvContext.getExecPlan(), insertStmt);
        }

        ///// 3. insert execute successfully, update the meta of materialized view according to ExecPlan
<<<<<<< HEAD
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshUpdateMeta")) {
=======
        try (Timer ignored = Tracers.watchScope("MVRefreshUpdateMeta")) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            updateMeta(mvToRefreshedPartitions, mvContext.getExecPlan(), refTableRefreshPartitions);
        }

        return RefreshJobStatus.SUCCESS;
    }

    /**
     * Prepare the statement and plan for mv refreshing, considering the partitions of ref table
     */
<<<<<<< HEAD
    private InsertStmt prepareRefreshPlan(Set<String> mvToRefreshedPartitions,
                                          Map<String, Set<String>> refTablePartitionNames) throws AnalysisException {
=======
    private InsertStmt prepareRefreshPlan(Set<String> mvToRefreshedPartitions, Map<String, Set<String>> refTablePartitionNames)
            throws AnalysisException, LockTimeoutException {
        long startTime = System.currentTimeMillis();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        // 1. Prepare context
        ConnectContext ctx = mvContext.getCtx();
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(mvContext.getRemoteIp())
                .setUser(ctx.getQualifiedUser())
<<<<<<< HEAD
                .setDb(ctx.getDatabase());
=======
                .setDb(ctx.getDatabase())
                .setWarehouse(ctx.getCurrentWarehouseName());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        // 2. Prepare variables
        changeDefaultConnectContextIfNeeded(ctx);

        // 3. AST
        InsertStmt insertStmt = null;
<<<<<<< HEAD
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshParser")) {
            insertStmt = generateInsertAst(mvToRefreshedPartitions, materializedView, ctx);
        }

        // 4. Analyze and prepare partition
        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(ctx, insertStmt);
        ExecPlan execPlan = null;
        if (!StatementPlanner.tryLock(dbs, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock databases: " + Joiner.on(",").join(dbs.values().stream()
                    .map(Database::getFullName).collect(Collectors.toList())));
        }

        MVPCTRefreshPlanBuilder planBuilder = new MVPCTRefreshPlanBuilder(materializedView, mvContext);
        try {
            // 4. Analyze and prepare partition & Rebuild insert statement by
            // considering to-refresh partitions of ref tables/ mv
            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("MVRefreshAnalyzer")) {
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                insertStmt = planBuilder.analyzeAndBuildInsertPlan(insertStmt, refTablePartitionNames, ctx);
                // Must set execution id before StatementPlanner.plan
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
            }

            // 5. generate insert stmt's exec plan, make thread local ctx existed
<<<<<<< HEAD
            try (ConnectContext.ScopeGuard guard = ctx.bindScope(); PlannerProfile.ScopedTimer ignored
                    = PlannerProfile.getScopedTimer("MVRefreshPlanner")) {
                execPlan = StatementPlanner.planInsertStmt(dbs, insertStmt, ctx);
            }
        } catch (Throwable e) {
            LOG.warn("prepareRefreshPlan for mv {} failed", materializedView.getName(), e);
            throw e;
        } finally {
            StatementPlanner.unLock(dbs);
        }

=======
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
            LOG.info("MV Refresh PlanBuilderMessage: {}", planBuildMessage);
            message.setPlanBuilderMessage(planBuildMessage);
        });

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD

=======
        LOG.info("Finished mv refresh plan for {}, costs:{}(ms)",
                materializedView.getName(), System.currentTimeMillis() - startTime);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
            String rg = mvProperty.getResourceGroup();
            if (rg == null || rg.isEmpty()) {
                rg = ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME;
=======
            String rg = ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME;
            if (mvProperty != null && !Strings.isNullOrEmpty(mvProperty.getResourceGroup())) {
                rg = mvProperty.getResourceGroup();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
            mvSessionVariable.setResourceGroup(rg);
        }

        // enable spill by default for mv if spill is not set by default and
        // `session.enable_spill` session variable is not set.
        if (Config.enable_materialized_view_spill &&
<<<<<<< HEAD
                !mvSessionVariable.getEnableSpill() &&
=======
                !mvSessionVariable.isEnableSpill() &&
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                !mvProperty.getProperties().containsKey(MV_SESSION_ENABLE_SPILL)) {
            mvSessionVariable.setEnableSpill(true);
        }

<<<<<<< HEAD
        // change `query_timeout` to 1 hour by default for better user experience.
        if (!mvProperty.getProperties().containsKey(MV_SESSION_TIMEOUT)) {
            mvSessionVariable.setQueryTimeoutS(MV_DEFAULT_QUERY_TIMEOUT);
        }

        // set enable_insert_strict by default
        if (!isMVPropertyContains(SessionVariable.ENABLE_INSERT_STRICT)) {
            mvSessionVariable.setEnableInsertStrict(false);
        }
=======
        if (!mvProperty.getProperties().containsKey(MV_SESSION_INSERT_TIMEOUT)
                && mvProperty.getProperties().containsKey(MV_SESSION_QUERY_TIMEOUT)) {
            // for compatibility
            mvProperty.getProperties().put(MV_SESSION_INSERT_TIMEOUT, mvProperty.getProperties().get(MV_SESSION_QUERY_TIMEOUT));
        }

        // set insert_max_filter_ratio by default
        if (!isMVPropertyContains(SessionVariable.INSERT_MAX_FILTER_RATIO)) {
            mvSessionVariable.setInsertMaxFilterRatio(Config.mv_refresh_fail_on_filter_data ? 0 : 1);
        }
        // enable profile by default for mv refresh task
        if (!isMVPropertyContains(SessionVariable.ENABLE_PROFILE)) {
            mvSessionVariable.setEnableProfile(true);
        }
        // set the default new_planner_optimize_timeout for mv refresh
        if (!isMVPropertyContains(SessionVariable.NEW_PLANNER_OPTIMIZER_TIMEOUT)) {
            mvSessionVariable.setOptimizerExecuteTimeout(Config.mv_refresh_default_planner_optimize_timeout);
        }
        // set enable_materialized_view_rewrite by default
        if (!isMVPropertyContains(SessionVariable.ENABLE_MATERIALIZED_VIEW_REWRITE) && Config.enable_mv_refresh_query_rewrite) {
            // Only enable mv rewrite when there are more than one related mvs that can be rewritten by other mvs.
            Set<Table> baseTables = snapshotBaseTables.values().stream()
                    .map(t -> t.getBaseTable()).collect(Collectors.toSet());
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
        mvSessionVariable.setQueryExcludingMVNames(materializedView.getName());
        mvConnectCtx.setUseConnectorMetadataCache(Optional.of(true));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    private boolean isMVPropertyContains(String key) {
        String mvKey = PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + key;
        return materializedView.getTableProperty().getProperties().containsKey(mvKey);
    }

    private void postProcess() {
<<<<<<< HEAD
        if (mvContext != null) {
=======
        // If mv's not active, mvContext may be null.
        if (mvContext != null && mvContext.ctx != null) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            mvContext.ctx.getSessionVariable().setTransactionVisibleWaitTimeout(oldTransactionVisibleWaitTimeout);
        }
    }

<<<<<<< HEAD
=======
    private boolean isEnableMVRefreshQueryRewrite(ConnectContext ctx,
                                                  Set<Table> baseTables) {
        Set<MaterializedView> relatedMvs = MvUtils.getRelatedMvs(ctx, 1, baseTables);
        LOG.info("Refresh mv {} with related mvs: {}", materializedView.getName(), relatedMvs);
        // only enable mv rewrite when there are more than one related mvs
        return relatedMvs.size() > 1;
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public MVTaskRunExtraMessage getMVTaskRunExtraMessage() {
        if (this.mvContext.status == null) {
            return null;
        }
        return this.mvContext.status.getMvTaskRunExtraMessage();
    }

    @VisibleForTesting
<<<<<<< HEAD
    public void filterPartitionByRefreshNumber(Set<String> partitionsToRefresh, MaterializedView materializedView) {
        filterPartitionByRefreshNumber(partitionsToRefresh, materializedView, false);
    }

    public void filterPartitionByRefreshNumber(Set<String> partitionsToRefresh,
                                               MaterializedView materializedView,
                                               boolean tentative) {
=======
    public void filterPartitionByRefreshNumber(Set<String> partitionsToRefresh,
                                               Set<String> mvPotentialPartitionNames,
                                               MaterializedView materializedView) {
        filterPartitionByRefreshNumber(partitionsToRefresh, mvPotentialPartitionNames, materializedView, false);
    }

    public void filterPartitionByRefreshNumber(Set<String> partitionsToRefresh,
                                               Set<String> mvPotentialPartitionNames,
                                               MaterializedView materializedView,
                                               boolean tentative) {
        // refresh all partition when it's a sync refresh, otherwise updated partitions may be lost.
        if (mvContext.executeOption != null && mvContext.executeOption.getIsSync()) {
            return;
        }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        // ignore if mv is not partitioned.
        if (!materializedView.isPartitionedTable()) {
            return;
        }
        // ignore if partition_fresh_limit is not set
        int partitionRefreshNumber = materializedView.getTableProperty().getPartitionRefreshNumber();
        if (partitionRefreshNumber <= 0) {
            return;
        }
<<<<<<< HEAD
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
        for (int i = 0; i < partitionRefreshNumber; i++) {
            if (partitionNameIter.hasNext()) {
                partitionNameIter.next();
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

        if (!tentative) {
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
=======

        // do filter actions
        mvRefreshPartitioner.filterPartitionByRefreshNumber(partitionsToRefresh, mvPotentialPartitionNames, tentative);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        newProperties.put(TaskRun.PARTITION_START, mvContext.getNextPartitionStart());
        newProperties.put(TaskRun.PARTITION_END, mvContext.getNextPartitionEnd());
        // Partition refreshing task run should have the HIGHEST priority, and be scheduled before other tasks
        // Otherwise this round of partition refreshing would be staved and never got finished
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.HIGHEST.value(), true, newProperties);
        taskManager.executeTask(taskName, option);
        LOG.info("[MV] Generate a task to refresh next batches of partitions for MV {}-{}, start={}, end={}",
                materializedView.getName(), materializedView.getId(),
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd());
    }

    private void refreshExternalTable(TaskRunContext context,
                                      Map<Table, Set<String>> baseTableCandidatePartitions) {
        List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Database db = baseTableInfo.getDb();
            if (db == null) {
=======
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
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
        updateTaskRunStatus(status -> {
            MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
            extraMessage.setNextPartitionStart(mvContext.getNextPartitionStart());
            extraMessage.setNextPartitionEnd(mvContext.getNextPartitionEnd());
            extraMessage.setNextPartitionValues(mvContext.getNextPartitionValues());
        });

        // Partition refreshing task run should have the HIGHER priority, and be scheduled before other tasks
        // Otherwise this round of partition refreshing would be staved and never got finished
        int priority = mvContext.executeOption.getPriority() > Constants.TaskRunPriority.LOWEST.value() ?
                mvContext.executeOption.getPriority() : Constants.TaskRunPriority.HIGHER.value();
        ExecuteOption option = new ExecuteOption(priority, true, newProperties);
        LOG.info("[MV] Generate a task to refresh next batches of partitions for MV {}-{}, start={}, end={}, " +
                        "priority={}", materializedView.getName(), materializedView.getId(),
                mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(), priority);

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

    private void refreshExternalTable(TaskRunContext context,
                                      Map<TableSnapshotInfo, Set<String>> baseTableCandidatePartitions) {
        List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
        // use it if refresh external table fails
        ConnectContext connectContext = context.getCtx();
        List<Pair<Table, BaseTableInfo>> toRepairTables = new ArrayList<>();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Optional<Database> dbOpt = GlobalStateMgr.getCurrentState().getMetadataMgr().getDatabase(baseTableInfo);
            if (dbOpt.isEmpty()) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                LOG.warn("database {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getDbInfoStr(), materializedView.getName());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }

<<<<<<< HEAD
            Table table = baseTableInfo.getTable();
            if (table == null) {
=======
            Optional<Table> optTable = MvUtils.getTable(baseTableInfo);
            if (optTable.isEmpty()) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                LOG.warn("table {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getTableInfoStr(), materializedView.getName());
                materializedView.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(baseTableInfo.getTableName()));
                throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
            }

<<<<<<< HEAD
            if (!table.isNativeTableOrMaterializedView() && !table.isHiveView()) {
                ConnectContext connectContext = context.getCtx();
                Set<String> basePartitions = baseTableCandidatePartitions.get(table);
                if (CollectionUtils.isNotEmpty(basePartitions)) {
                    // only refresh referenced partitions, to reduce metadata overhead
                    List<String> realPartitionNames = basePartitions.stream()
                            .flatMap(name -> convertMVPartitionNameToRealPartitionName(table, name).stream())
                            .collect(Collectors.toList());
                    connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                            baseTableInfo.getDbName(), table, realPartitionNames, false);
                } else {
                    // refresh the whole table, which may be costly in extreme case
                    connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
                            baseTableInfo.getDbName(), table, Lists.newArrayList(), true);
                }
=======
            // refresh old table
            Table table = optTable.get();
            if (table.isNativeTableOrMaterializedView() || table.isConnectorView()) {
                LOG.debug("No need to refresh table:{} because it is native table or materialized view or connector view",
                        baseTableInfo.getTableInfoStr());
                continue;
            }
            TableSnapshotInfo snapshotInfo = new TableSnapshotInfo(baseTableInfo, table);
            Set<String> basePartitions = baseTableCandidatePartitions.get(snapshotInfo);
            if (CollectionUtils.isNotEmpty(basePartitions)) {
                // only refresh referenced partitions, to reduce metadata overhead
                List<String> realPartitionNames = basePartitions.stream()
                        .flatMap(name -> mvContext.getExternalTableRealPartitionName(table, name).stream())
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
                            Map<Table, Set<String>> refTableAndPartitionNames) {
        LOG.info("start to update meta for mv:{}", materializedView.getName());

        // check
        Table mv = database.getTable(materializedView.getId());
=======
                            Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        LOG.debug("Start to update meta for mv:{}", materializedView.getName());

        // check
        Table mv = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), materializedView.getId());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD

        // NOTE: For each task run, ref-base table's incremental partition and all non-ref base tables' partitions
        // are refreshed, so we need record it into materialized view.
        // NOTE: We don't use the pruned partition infos from ExecPlan because the optimized partition infos are not
        // exact to describe which partitions are refreshed.
        Map<Table, Set<String>> baseTableAndPartitionNames = Maps.newHashMap();
        for (Map.Entry<Table, Set<String>> e : refTableAndPartitionNames.entrySet()) {
            Set<String> realPartitionNames =
                    e.getValue().stream()
                            .flatMap(name -> convertMVPartitionNameToRealPartitionName(e.getKey(), name).stream())
                            .collect(Collectors.toSet());
            baseTableAndPartitionNames.put(e.getKey(), realPartitionNames);
        }
        Map<Table, Set<String>> nonRefTableAndPartitionNames = getNonRefTableRefreshPartitions();
        if (!nonRefTableAndPartitionNames.isEmpty()) {
            baseTableAndPartitionNames.putAll(nonRefTableAndPartitionNames);
        }
        // update the meta if succeed
        if (!database.writeLockAndCheckExist()) {
            throw new DmlException("update meta failed. database:" + database.getFullName() + " not exist");
        }

        try {
            MaterializedView.MvRefreshScheme mvRefreshScheme = materializedView.getRefreshScheme();
            MaterializedView.AsyncRefreshContext refreshContext = mvRefreshScheme.getAsyncRefreshContext();

            // update materialized view partition to ref base table partition names meta
            updateAssociatedPartitionMeta(refreshContext, mvRefreshedPartitions, refTableAndPartitionNames);

            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> changedOlapTablePartitionInfos =
                    getSelectedPartitionInfosOfOlapTable(baseTableAndPartitionNames);
            Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> changedExternalTablePartitionInfos
                    = getSelectedPartitionInfosOfExternalTable(baseTableAndPartitionNames);
            Preconditions.checkState(changedOlapTablePartitionInfos.size() + changedExternalTablePartitionInfos.size()
                    <= baseTableAndPartitionNames.size());
            updateMetaForOlapTable(refreshContext, changedOlapTablePartitionInfos);
            updateMetaForExternalTable(refreshContext, changedExternalTablePartitionInfos);
        } catch (Exception e) {
            LOG.warn("update final meta failed after mv refreshed:", e);
            throw e;
        } finally {
            database.writeUnlock();
        }

        // add message into information_schema
        if (this.getMVTaskRunExtraMessage() != null) {
            try {
                MVTaskRunExtraMessage extraMessage = getMVTaskRunExtraMessage();
                Map<String, Set<String>> baseTableRefreshedPartitionsByExecPlan =
                        getBaseTableRefreshedPartitionsByExecPlan(execPlan);
                extraMessage.setBasePartitionsToRefreshMap(baseTableRefreshedPartitionsByExecPlan);
            } catch (Exception e) {
                // just log warn and no throw exceptions for updating task runs message.
                LOG.warn("update task run messages failed:", e);
            }
        }
    }

    private void updateAssociatedPartitionMeta(MaterializedView.AsyncRefreshContext refreshContext,
                                               Set<String> mvRefreshedPartitions,
                                               Map<Table, Set<String>> refTableAndPartitionNames) {
        Map<String, Set<String>> mvToBaseNameRef = mvContext.getMvRefBaseTableIntersectedPartitions();
        if (mvToBaseNameRef != null) {
            try {
                Table refBaseTable = refTableAndPartitionNames.keySet().iterator().next();
                Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap =
                        refreshContext.getMvPartitionNameRefBaseTablePartitionMap();
                for (String mvRefreshedPartition : mvRefreshedPartitions) {
                    Set<String> realBaseTableAssociatedPartitions = Sets.newHashSet();
                    for (String refBaseTableAssociatedPartition : mvToBaseNameRef.get(mvRefreshedPartition)) {
                        realBaseTableAssociatedPartitions.addAll(
                                convertMVPartitionNameToRealPartitionName(refBaseTable,
                                        refBaseTableAssociatedPartition));
                    }
                    mvPartitionNameRefBaseTablePartitionMap
                            .put(mvRefreshedPartition, realBaseTableAssociatedPartitions);
                }

            } catch (Exception e) {
                LOG.warn("Update materialized view {} with the associated ref base table partitions failed: ",
                        materializedView.getName(), e);
            }
        }
    }

    private void updateMetaForOlapTable(MaterializedView.AsyncRefreshContext refreshContext,
                                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> changedTablePartitionInfos) {
        LOG.info("update meta for mv {} with olap tables:{}", materializedView.getName(),
                changedTablePartitionInfos);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableVisibleVersionMap();
        Table partitionTable = null;
        if (mvContext.hasNextBatchPartition()) {
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
            partitionTable = partitionTableAndColumn.first;
        }
        // update version map of materialized view
        for (Map.Entry<Long, Map<String, MaterializedView.BasePartitionInfo>> tableEntry
                : changedTablePartitionInfos.entrySet()) {
            Long tableId = tableEntry.getKey();
            if (partitionTable != null && tableId != partitionTable.getId()) {
                continue;
            }
            currentVersionMap.computeIfAbsent(tableId, (v) -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.get(tableId);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = tableEntry.getValue();
            LOG.info("Update materialized view {} meta for base table {} with partitions info: {}, old partition infos:{}",
                    materializedView.getName(), tableId, partitionInfoMap, currentTablePartitionInfo);
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // remove partition info of not-exist partition for snapshot table from version map
            Table snapshotTable = Preconditions.checkNotNull(snapshotBaseTables.get(tableId),
                    "base table not found: " + tableId).second;
            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                currentTablePartitionInfo.keySet().removeIf(partitionName ->
                        !snapshotOlapTable.getVisiblePartitionNames().contains(partitionName));
            }
        }
        if (!changedTablePartitionInfos.isEmpty()) {
            ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                    new ChangeMaterializedViewRefreshSchemeLog(materializedView);
            long maxChangedTableRefreshTime =
                    MvUtils.getMaxTablePartitionInfoRefreshTime(changedTablePartitionInfos.values());
            materializedView.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
            GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
        }
    }

    private void updateMetaForExternalTable(
            MaterializedView.AsyncRefreshContext refreshContext,
            Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> changedTablePartitionInfos) {
        LOG.info("update meta for mv {} with external tables:{}", materializedView.getName(),
                changedTablePartitionInfos);
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableInfoVisibleVersionMap();
        BaseTableInfo partitionTableInfo = null;
        if (mvContext.hasNextBatchPartition()) {
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
            partitionTableInfo =
                    Preconditions.checkNotNull(snapshotBaseTables.get(partitionTableAndColumn.first.getId()),
                            "base table not found: " + partitionTableAndColumn.first.getId())
                            .first;
        }
        // update version map of materialized view
        for (Map.Entry<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> tableEntry
                : changedTablePartitionInfos.entrySet()) {
            BaseTableInfo baseTableInfo = tableEntry.getKey();
            if (partitionTableInfo != null && !partitionTableInfo.equals(baseTableInfo)) {
                continue;
            }
            currentVersionMap.computeIfAbsent(baseTableInfo, (v) -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.get(baseTableInfo);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = tableEntry.getValue();
            LOG.info("Update materialized view {} meta for external base table {} with partitions info: {}, " +
                            "old partition infos:{}", materializedView.getName(), baseTableInfo.getTableId(),
                    partitionInfoMap, currentTablePartitionInfo);
            // overwrite old partition names
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // remove partition info of not-exist partition for snapshot table from version map
            Set<String> partitionNames =
                    Sets.newHashSet(PartitionUtil.getPartitionNames(baseTableInfo.getTableChecked()));
            currentTablePartitionInfo.keySet().removeIf(partitionName -> !partitionNames.contains(partitionName));
        }
        if (!changedTablePartitionInfos.isEmpty()) {
            ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                    new ChangeMaterializedViewRefreshSchemeLog(materializedView);
            long maxChangedTableRefreshTime =
                    MvUtils.getMaxTablePartitionInfoRefreshTime(changedTablePartitionInfos.values());
            materializedView.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
            GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
        }
    }

    private void prepare(TaskRunContext context) {
        Map<String, String> properties = context.getProperties();
        // NOTE: mvId is set in Task's properties when creating
        long mvId = Long.parseLong(properties.get(MV_ID));
        database = GlobalStateMgr.getCurrentState().getDb(context.ctx.getDatabase());
        if (database == null) {
            LOG.warn("database {} do not exist when refreshing materialized view:{}", context.ctx.getDatabase(), mvId);
            throw new DmlException("database " + context.ctx.getDatabase() + " do not exist.");
        }
        Table table = database.getTable(mvId);
        if (table == null) {
            LOG.warn("materialized view:{} in database:{} do not exist when refreshing", mvId,
                    context.ctx.getDatabase());
            throw new DmlException("database " + context.ctx.getDatabase() + " do not exist.");
=======
        // update mv's version info
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


        MVVersionManager mvVersionManager = new MVVersionManager(materializedView, mvContext);
        try {
            mvVersionManager.updateMVVersionInfo(snapshotBaseTables, mvRefreshedPartitions,
                    refBaseTableIds, refTableAndPartitionNames);
        } catch (Exception e) {
            LOG.warn("update final meta failed after mv refreshed:", DebugUtil.getRootStackTrace(e));
            throw e;
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), materializedView.getId(), LockType.WRITE);
        }

        // update mv status message
        updateTaskRunStatus(status -> {
            try {
                MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
                Map<String, Set<String>> baseTableRefreshedPartitionsByExecPlan =
                        MVTraceUtils.getBaseTableRefreshedPartitionsByExecPlan(materializedView, execPlan);
                extraMessage.setBasePartitionsToRefreshMap(baseTableRefreshedPartitionsByExecPlan);
            } catch (Exception e) {
                // just log warn and no throw exceptions for an updating task runs message.
                LOG.warn("update task run messages failed:", DebugUtil.getRootStackTrace(e));
            }
        });
    }

    @VisibleForTesting
    public void prepare(TaskRunContext context) {
        Map<String, String> properties = context.getProperties();
        // NOTE: mvId is set in Task's properties when creating
        long mvId = Long.parseLong(properties.get(MV_ID));
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(context.ctx.getDatabase());
        if (db == null) {
            LOG.warn("database {} do not exist when refreshing materialized view:{}", context.ctx.getDatabase(), mvId);
            throw new DmlException("database " + context.ctx.getDatabase() + " do not exist.");
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId);
        if (table == null) {
            LOG.warn("materialized view:{} in database:{} do not exist when refreshing", mvId,
                    context.ctx.getDatabase());
            throw new DmlException(String.format("materialized view:%s in database:%s do not exist when refreshing",
                    mvId, context.ctx.getDatabase()));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        mvContext = new MvTaskRunContext(context);
=======

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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    /**
     * Sync base table's partition infos to be used later.
     */
<<<<<<< HEAD
    private void syncPartitions(TaskRunContext context) {
        snapshotBaseTables = collectBaseTables(materializedView);
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();

        if (!(partitionInfo instanceof SinglePartitionInfo)) {
            Pair<Table, Column> partitionTableAndColumn = getRefBaseTableAndPartitionColumn(snapshotBaseTables);
            Table refBaseTable = partitionTableAndColumn.first;
            if (!snapshotBaseTables.containsKey(refBaseTable.getId())) {
                throw new DmlException("The ref base table {} of materialized view {} is not existed in snapshot.",
                        refBaseTable.getName(), materializedView.getName());
            }
            mvContext.setRefBaseTable(snapshotBaseTables.get(refBaseTable.getId()).second);
            mvContext.setRefBaseTablePartitionColumn(partitionTableAndColumn.second);
        }
        int partitionTTLNumber = materializedView.getTableProperty().getPartitionTTLNumber();
        mvContext.setPartitionTTLNumber(partitionTTLNumber);

        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            syncPartitionsForExpr(context);
        }
    }

    /**
     * @param tables : base tables of the materialized view
     * @return : return the ref base table and column that materialized view's partition column
     * derives from if it exists, otherwise return null.
     */
    private Pair<Table, Column> getRefBaseTableAndPartitionColumn(
            Map<Long, Pair<BaseTableInfo, Table>> tables) {
        SlotRef slotRef = MaterializedView.getRefBaseTablePartitionSlotRef(materializedView);
        for (Pair<BaseTableInfo, Table> tableInfo : tables.values()) {
            BaseTableInfo baseTableInfo = tableInfo.first;
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(baseTableInfo.getTableName())) {
                Table table = tableInfo.second;
                return Pair.create(table, table.getColumn(slotRef.getColumnName()));
            }
        }
        return Pair.create(null, null);
    }

    private void syncPartitionsForExpr(TaskRunContext context) {
        Expr partitionExpr = materializedView.getFirstPartitionRefTableExpr();
        Pair<Table, Column> partitionTableAndColumn = materializedView.getBaseTableAndPartitionColumn();
        Table refBaseTable = partitionTableAndColumn.first;
        Preconditions.checkNotNull(refBaseTable);
        Column refBaseTablePartitionColumn = partitionTableAndColumn.second;
        Preconditions.checkNotNull(refBaseTablePartitionColumn);

        RangePartitionDiff rangePartitionDiff = new RangePartitionDiff();

        int partitionTTLNumber = materializedView.getTableProperty().getPartitionTTLNumber();
        mvContext.setPartitionTTLNumber(partitionTTLNumber);
        Map<String, Range<PartitionKey>> mvPartitionMap = materializedView.getRangePartitionMap();
        if (!database.tryReadLock(Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database: " + database.getFullName());
        }

        Map<String, Range<PartitionKey>> mvRangePartitionMap = materializedView.getRangePartitionMap();
        Map<String, Range<PartitionKey>> refBaseTablePartitionMap;
        Map<String, Set<String>> refBaseTableMVPartitionMap = Maps.newHashMap();
        try {
            // Collect the ref base table's partition range map.
            refBaseTablePartitionMap = PartitionUtil.getPartitionKeyRange(
                    refBaseTable, refBaseTablePartitionColumn, partitionExpr);

            // To solve multi partition columns' problem of external table, record the mv partition name to all the same
            // partition names map here.
            if (!refBaseTable.isNativeTableOrMaterializedView()) {
                refBaseTableMVPartitionMap = PartitionUtil.getMVPartitionNameMapOfExternalTable(refBaseTable,
                        refBaseTablePartitionColumn, PartitionUtil.getPartitionNames(refBaseTable));
            }

            Column partitionColumn =
                    ((RangePartitionInfo) materializedView.getPartitionInfo()).getPartitionColumns().get(0);
            PartitionDiffer differ = PartitionDiffer.build(materializedView, context);
            rangePartitionDiff = PartitionUtil.getPartitionDiff(
                    partitionExpr, partitionColumn, refBaseTablePartitionMap, mvPartitionMap, differ);
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return;
        } finally {
            database.readUnlock();
        }

        Map<String, Range<PartitionKey>> deletes = rangePartitionDiff.getDeletes();

        // Delete old partitions and then add new partitions because the old and new partitions may overlap
        for (Map.Entry<String, Range<PartitionKey>> deleteEntry : deletes.entrySet()) {
            String mvPartitionName = deleteEntry.getKey();
            dropPartition(database, materializedView, mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] delete partitions range [{}]",
                materializedView.getName(), deletes);

        // Create new added materialized views' ranges
        Map<String, String> partitionProperties = getPartitionProperties(materializedView);
        DistributionDesc distributionDesc = getDistributionDesc(materializedView);
        Map<String, Range<PartitionKey>> adds = rangePartitionDiff.getAdds();

        addPartitions(database, materializedView, adds, partitionProperties, distributionDesc);
        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            mvRangePartitionMap.put(mvPartitionName, addEntry.getValue());
        }
        LOG.info("The process of synchronizing materialized view [{}] add partitions range [{}]",
                materializedView.getName(), adds);

        // used to get partitions to refresh
        Map<String, Set<String>> baseToMvNameRef = SyncPartitionUtils
                .getIntersectedPartitions(refBaseTablePartitionMap, mvRangePartitionMap);
        Map<String, Set<String>> mvToBaseNameRef = SyncPartitionUtils
                .getIntersectedPartitions(mvRangePartitionMap, refBaseTablePartitionMap);

        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableRangePartitionMap(refBaseTablePartitionMap);
        mvContext.setExternalRefBaseTableMVPartitionMap(refBaseTableMVPartitionMap);
    }

    private boolean needToRefreshTable(Table table) {
        return CollectionUtils.isNotEmpty(materializedView.getUpdatedPartitionNamesOfTable(table, false));
    }

    private static boolean supportPartitionRefresh(Table table) {
        if (table.getType() == Table.TableType.VIEW) {
            return false;
        }
        return ConnectorPartitionTraits.build(table).supportPartitionRefresh();
    }

    /**
     * Whether non-partitioned materialized view needs to be refreshed or not, it needs refresh when:
     * - its base table is not supported refresh by partition.
     * - its base table has updated.
     */
    private boolean isNonPartitionedMVNeedToRefresh() {
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table snapshotTable = tablePair.second;
            if (!supportPartitionRefresh(snapshotTable)) {
                return true;
            }
            if (needToRefreshTable(snapshotTable)) {
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
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table snapshotTable = tablePair.second;
            if (snapshotTable.getId() == partitionTable.getId()) {
                continue;
            }
            if (!supportPartitionRefresh(snapshotTable)) {
                continue;
            }
            if (needToRefreshTable(snapshotTable)) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    public Set<String> getPartitionsToRefreshForMaterializedView(Map<String, String> properties, boolean tentative)
            throws AnalysisException {
        String start = properties.get(TaskRun.PARTITION_START);
        String end = properties.get(TaskRun.PARTITION_END);
        boolean force = Boolean.parseBoolean(properties.get(TaskRun.FORCE));
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        Set<String> needRefreshMvPartitionNames = getPartitionsToRefreshForMaterializedView(partitionInfo,
                start, end, force || tentative);
        // update stats
        if (!tentative && this.getMVTaskRunExtraMessage() != null) {
            MVTaskRunExtraMessage extraMessage = this.getMVTaskRunExtraMessage();
            extraMessage.setForceRefresh(force);
            extraMessage.setPartitionStart(start);
            extraMessage.setPartitionEnd(end);
        }
=======
    private boolean syncPartitions() throws AnalysisException, LockTimeoutException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // collect base table snapshot infos
        snapshotBaseTables = collectBaseTableSnapshotInfos(materializedView);

        // do sync partitions (add or drop partitions) for materialized view
        boolean result = mvRefreshPartitioner.syncAddOrDropPartitions();
        LOG.info("Finish sync {} partitions, cost: {}(ms)", materializedView.getName(),
                stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return result;
    }

    /**
     * If it's tentative, don't really consider the staleness of MV, but all potential partitions
     */
    @VisibleForTesting
    public Set<String> getPartitionsToRefreshForMaterializedView(Map<String, String> properties,
                                                                 Set<String> mvPotentialPartitionNames,
                                                                 boolean tentative)
            throws AnalysisException {
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        MVRefreshParams mvRefreshParams = new MVRefreshParams(partitionInfo, properties, tentative);
        Set<String> needRefreshMvPartitionNames = getPartitionsToRefreshForMaterializedView(partitionInfo,
                mvRefreshParams, mvPotentialPartitionNames);

        // update mv extra message
        if (!tentative) {
            updateTaskRunStatus(status -> {
                MVTaskRunExtraMessage extraMessage = status.getMvTaskRunExtraMessage();
                extraMessage.setForceRefresh(mvRefreshParams.isForce());
                extraMessage.setPartitionStart(mvRefreshParams.getRangeStart());
                extraMessage.setPartitionEnd(mvRefreshParams.getRangeEnd());
            });
        }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return needRefreshMvPartitionNames;
    }

    /**
<<<<<<< HEAD
     * @param mvPartitionInfo : materialized view's partition info
     * @param start           : materialized view's refresh start in this task run
     * @param end             : materialized view's refresh end in this task run
     * @param force           : whether this task run is force or not
     * @return
     * @throws AnalysisException
     */
    private Set<String> getPartitionsToRefreshForMaterializedView(PartitionInfo mvPartitionInfo,
                                                                  String start,
                                                                  String end,
                                                                  boolean force) throws AnalysisException {
        int partitionTTLNumber = mvContext.getPartitionTTLNumber();

        // Force refresh
        if (force && start == null && end == null) {
            if (mvPartitionInfo instanceof SinglePartitionInfo) {
                return materializedView.getVisiblePartitionNames();
            } else {
                return materializedView.getValidPartitionMap(partitionTTLNumber).keySet();
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
            Expr partitionExpr = MaterializedView.getPartitionExpr(materializedView);
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
                    // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                    // it should be refreshed according to the user-specified range, not all partitions.
                    return getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(refBaseTable,
                            mvRangePartitionNames, true);
                }
            }

            // check the ref base table
            if (partitionExpr instanceof SlotRef) {
                return getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(refBaseTable, mvRangePartitionNames,
                        force);
            } else if (partitionExpr instanceof FunctionCallExpr) {
                needRefreshMvPartitionNames = getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(refBaseTable,
                        mvRangePartitionNames, force);

                Set<String> baseChangedPartitionNames =
                        getBasePartitionNamesByMVPartitionNames(needRefreshMvPartitionNames);
                // because the relation of partitions between materialized view and base partition table is n : m,
                // should calculate the candidate partitions recursively.
                LOG.debug("Start calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                        " baseChangedPartitionNames: {}", needRefreshMvPartitionNames, baseChangedPartitionNames);
                SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                        mvContext.getRefBaseTableMVIntersectedPartitions(),
                        mvContext.getMvRefBaseTableIntersectedPartitions());
                LOG.debug("Finish calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                        " baseChangedPartitionNames: {}", needRefreshMvPartitionNames, baseChangedPartitionNames);
            }
        } else {
            throw new DmlException("unsupported partition info type:" + mvPartitionInfo.getClass().getName());
        }
        return needRefreshMvPartitionNames;
    }

    private Set<String> getMVPartitionNamesToRefreshByRangePartitionNamesAndForce(Table refBaseTable,
                                                                                  Set<String> mvRangePartitionNames,
                                                                                  boolean force) {
        if (force || !supportPartitionRefresh(refBaseTable)) {
            LOG.info("The ref base table {} is not supported partition refresh, refresh all " +
                    "partitions of materialized view: {}", refBaseTable.getName(), materializedView.getName());
            return Sets.newHashSet(mvRangePartitionNames);
        }

        // step1: check updated partition names in the ref base table and add it to the refresh candidate
        Set<String> updatePartitionNames = materializedView.getUpdatedPartitionNamesOfTable(refBaseTable, false);
        if (updatePartitionNames == null) {
            LOG.warn("Cannot find the updated partition info of ref base table {} of mv: {}",
                    refBaseTable.getName(), materializedView.getName());
            return mvRangePartitionNames;
        }

        // step2: fetch the corresponding materialized view partition names as the need to refresh partitions
        Set<String> result = getMVPartitionNamesByBasePartitionNames(updatePartitionNames);
        result.retainAll(mvRangePartitionNames);
        LOG.info("The ref base table {} has updated partitions: {}, the corresponding mv partitions to refresh: {}, " +
                        "mvRangePartitionNames: {}", refBaseTable.getName(), updatePartitionNames, result, mvRangePartitionNames);
        return result;
    }

    /**
     * @param basePartitionNames : ref base table partition names to check.
     * @return : Return mv corresponding partition names to the ref base table partition names.
     */
    private Set<String> getMVPartitionNamesByBasePartitionNames(Set<String> basePartitionNames) {
        Set<String> result = Sets.newHashSet();
        Map<String, Set<String>> refBaseTableMVPartitionMap = mvContext.getRefBaseTableMVIntersectedPartitions();
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
    private Set<String> getBasePartitionNamesByMVPartitionNames(Set<String> mvPartitionNames) {
        Set<String> result = Sets.newHashSet();
        Map<String, Set<String>> mvRefBaseTablePartitionMap = mvContext.getMvRefBaseTableIntersectedPartitions();
        for (String mvPartitionName : mvPartitionNames) {
            if (mvRefBaseTablePartitionMap.containsKey(mvPartitionName)) {
                result.addAll(mvRefBaseTablePartitionMap.get(mvPartitionName));
            } else {
                LOG.warn("Cannot find need refreshed mv table partition from synced partition info: {}",
                        mvPartitionName);
            }
        }
        return result;
=======
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
     * @param mvRefreshParams : materialized view's refresh params
     * @return the partitions to refresh for materialized view
     * @throws AnalysisException
     */
    private Set<String> getPartitionsToRefreshForMaterializedView(PartitionInfo mvPartitionInfo,
                                                                  MVRefreshParams mvRefreshParams,
                                                                  Set<String> mvPotentialPartitionNames)
            throws AnalysisException {
        if (mvRefreshParams.isForceCompleteRefresh()) {
            // Force refresh
            return mvRefreshPartitioner.getMVPartitionsToRefreshWithForce();
        } else {
            return mvRefreshPartitioner.getMVPartitionsToRefresh(mvPartitionInfo, snapshotBaseTables,
                    mvRefreshParams, mvPotentialPartitionNames);
        }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    /**
     * Build an AST for insert stmt
     */
    private InsertStmt generateInsertAst(Set<String> materializedViewPartitions, MaterializedView materializedView,
                                         ConnectContext ctx) {
<<<<<<< HEAD
        // TODO: Support use mv when refreshing mv.
        ctx.getSessionVariable().setEnableMaterializedViewRewrite(false);
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
                    insertStmt.getTargetColumnNames() == null ? "" :
                            Joiner.on(",").join(insertStmt.getTargetColumnNames()),
=======
                    insertStmt.getTargetColumnNames() == null ? "" : Joiner.on(",").join(insertStmt.getTargetColumnNames()),
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    definition);
        }
        return insertStmt;
    }

<<<<<<< HEAD
    /**
     * Collect all deduplicated databases of the materialized view's base tables.
     * @param materializedView: the materialized view to check
     * @return: the databases of the materialized view's base tables, throw exception if the database do not exist.
     */
    List<Database> collectDatabases(MaterializedView materializedView) {
        Map<Long, Database> databaseMap = Maps.newHashMap();
        for (BaseTableInfo baseTableInfo : materializedView.getBaseTableInfos()) {
            Database db = baseTableInfo.getDb();
            if (db == null) {
                LOG.warn("database {} do not exist when refreshing materialized view:{}",
                        baseTableInfo.getDbInfoStr(), materializedView.getName());
                throw new DmlException("database " + baseTableInfo.getDbInfoStr() + " do not exist.");
            }
            databaseMap.put(db.getId(), db);
        }
        return Lists.newArrayList(databaseMap.values());
    }

    private boolean checkBaseTableSnapshotInfoChanged(BaseTableInfo baseTableInfo,
                                                      Table snapshotTable) {
        try {
            Table table = baseTableInfo.getTable();
            if (table == null) {
                return true;
            }

            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapShotOlapTable = (OlapTable) snapshotTable;
                PartitionInfo snapshotPartitionInfo = snapShotOlapTable.getPartitionInfo();
                if (snapshotPartitionInfo instanceof SinglePartitionInfo) {
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    Set<String> partitionNames = ((OlapTable) table).getVisiblePartitionNames();
                    if (!snapShotOlapTable.getVisiblePartitionNames().equals(partitionNames)) {
                        // there is partition rename
                        return true;
                    }
<<<<<<< HEAD
                } else {
                    Map<String, Range<PartitionKey>> snapshotPartitionMap =
                            snapShotOlapTable.getRangePartitionMap();
                    Map<String, Range<PartitionKey>> currentPartitionMap =
                            ((OlapTable) table).getRangePartitionMap();
                    boolean changed =
                            SyncPartitionUtils.hasPartitionChange(snapshotPartitionMap, currentPartitionMap);
                    if (changed) {
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                        return true;
                    }
                }
            } else if (ConnectorPartitionTraits.isSupported(snapshotTable.getType())) {
                if (snapshotTable.isUnPartitioned()) {
<<<<<<< HEAD
                    if (!table.isUnPartitioned()) {
                        return true;
                    }
                } else {
                    PartitionInfo mvPartitionInfo = materializedView.getPartitionInfo();
                    // do not need to check base partition table changed when mv is not partitioned
                    if (!(mvPartitionInfo instanceof ExpressionRangePartitionInfo)) {
                        return false;
                    }
                    Pair<Table, Column> partitionTableAndColumn =
                            getRefBaseTableAndPartitionColumn(snapshotBaseTables);
                    Column partitionColumn = partitionTableAndColumn.second;
                    // For Non-partition based base table, it's not necessary to check the partition changed.
                    if (!snapshotTable.equals(partitionTableAndColumn.first)
                            || !snapshotTable.containColumn(partitionColumn.getName())) {
                        return false;
                    }

                    Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.getPartitionKeyRange(
                            snapshotTable, partitionColumn, MaterializedView.getPartitionExpr(materializedView));
                    Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.getPartitionKeyRange(
                            table, partitionColumn, MaterializedView.getPartitionExpr(materializedView));
                    boolean changed =
                            SyncPartitionUtils.hasPartitionChange(snapshotPartitionMap, currentPartitionMap);
                    if (changed) {
=======
                    return false;
                } else {
                    PartitionInfo mvPartitionInfo = materializedView.getPartitionInfo();
                    // TODO: Support list partition later.
                    // do not need to check base partition table changed when mv is not partitioned
                    if (!(mvPartitionInfo.isRangePartition())) {
                        return false;
                    }
                    Map<Table, List<Column>> partitionTableAndColumns = materializedView.getRefBaseTablePartitionColumns();
                    // For Non-partition based base table, it's not necessary to check the partition changed.
                    if (!partitionTableAndColumns.containsKey(snapshotTable)) {
                        return false;
                    }
                    List<Column> partitionColumns = partitionTableAndColumns.get(snapshotTable);
                    Preconditions.checkArgument(partitionColumns.size() == 1,
                            "Only support one partition column in range partition");
                    Column partitionColumn = partitionColumns.get(0);
                    Optional<Expr> rangePartitionExprOpt = materializedView.getRangePartitionFirstExpr();
                    if (rangePartitionExprOpt.isEmpty()) {
                        return false;
                    }
                    Expr rangePartitionExpr = rangePartitionExprOpt.get();
                    Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.getPartitionKeyRange(
                            snapshotTable, partitionColumn, rangePartitionExpr);
                    Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.getPartitionKeyRange(
                            table, partitionColumn, rangePartitionExpr);
                    if (SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap)) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                        return true;
                    }
                }
            }
<<<<<<< HEAD
        } catch (UserException e) {
            LOG.warn("Materialized view compute partition change failed", e);
=======
        } catch (StarRocksException e) {
            LOG.warn("Materialized view compute partition change failed", DebugUtil.getRootStackTrace(e));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return true;
        }
        return false;
    }

<<<<<<< HEAD
    private boolean checkBaseTablePartitionChange(MaterializedView mv) {
        List<Database> dbs = collectDatabases(mv);
        // check snapshotBaseTables and current tables in catalog
        if (!StatementPlanner.tryLockDatabases(dbs, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock databases: " + Joiner.on(",").join(dbs.stream()
                    .map(Database::getFullName).collect(Collectors.toList())));
        }
        try {
            if (snapshotBaseTables.values().stream().anyMatch(t -> checkBaseTableSnapshotInfoChanged(t.first, t.second))) {
                return true;
            }
        } finally {
            StatementPlanner.unlockDatabases(dbs);
        }
        return false;
=======
    /**
     * Check whether the base table's partition has changed or not. Wait to refresh until all mv's base tables
     * don't change again.
     *
     * @return: true if the base table's partition has changed, otherwise false.
     */
    private boolean checkBaseTablePartitionChange(MaterializedView materializedView) throws LockTimeoutException {
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
            throw new UserException("User Cancelled");
=======
            throw new StarRocksException("User Cancelled");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
    @VisibleForTesting
    public Map<Long, Pair<BaseTableInfo, Table>> collectBaseTables(MaterializedView materializedView) {
        Map<Long, Pair<BaseTableInfo, Table>> tables = Maps.newHashMap();
        List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();

        List<Database> dbs = collectDatabases(materializedView);
        if (!StatementPlanner.tryLockDatabases(dbs, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock databases: " + Joiner.on(",").join(dbs.stream()
                    .map(Database::getFullName).collect(Collectors.toList())));
=======
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
    public Map<Long, TableSnapshotInfo> collectBaseTableSnapshotInfos(MaterializedView materializedView)
            throws LockTimeoutException {
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
<<<<<<< HEAD
                Table table = baseTableInfo.getTable();
                if (table == null) {
                    LOG.warn("table {} do not exist when refreshing materialized view:{}",
                            baseTableInfo.getTableInfoStr(), materializedView.getName());
                    throw new DmlException("Materialized view base table: %s not exist.", baseTableInfo.getTableInfoStr());
=======
                Optional<Table> tableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
                if (tableOpt.isEmpty()) {
                    LOG.warn("table {} do not exist when refreshing materialized view:{}",
                            baseTableInfo.getTableInfoStr(), materializedView.getName());
                    throw new DmlException("Materialized view base table: %s not exist.",
                            baseTableInfo.getTableInfoStr());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                }

                // NOTE: DeepCopy.copyWithGson is very time costing, use `copyOnlyForQuery` to reduce the cost.
                // TODO: Implement a `SnapshotTable` later which can use the copied table or transfer to the real table.
<<<<<<< HEAD
=======
                Table table = tableOpt.get();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                if (table.isNativeTableOrMaterializedView()) {
                    OlapTable copied = null;
                    if (table.isOlapOrCloudNativeTable()) {
                        copied = new OlapTable();
                    } else {
                        copied = new MaterializedView();
                    }
                    OlapTable olapTable = (OlapTable) table;
                    olapTable.copyOnlyForQuery(copied);
<<<<<<< HEAD
                    tables.put(table.getId(), Pair.create(baseTableInfo, copied));
=======
                    tables.put(table.getId(), new TableSnapshotInfo(baseTableInfo, copied));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                } else if (table.isView()) {
                    // skip to collect snapshots for views
                } else {
                    // for other table types, use the table directly which needs to lock if visits the table metadata.
<<<<<<< HEAD
                    tables.put(table.getId(), Pair.create(baseTableInfo, table));
                }
            }
        } finally {
            StatementPlanner.unlockDatabases(dbs);
=======
                    tables.put(table.getId(), new TableSnapshotInfo(baseTableInfo, table));
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(lockParams, LockType.READ);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        LOG.info("Collect base table snapshot infos for materialized view: {}, cost: {} ms",
                materializedView.getName(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return tables;
    }

<<<<<<< HEAD
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

    private void addPartitions(Database database, MaterializedView materializedView,
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

    private void dropPartition(Database database, MaterializedView materializedView, String mvPartitionName) {
        String dropPartitionName = materializedView.getPartition(mvPartitionName).getName();
        if (!database.writeLockAndCheckExist()) {
            throw new DmlException("drop partition failed. database:" + database.getFullName() + " not exist");
        }
        try {
            // check
            Table mv = database.getTable(materializedView.getId());
            if (mv == null) {
                throw new DmlException("drop partition failed. mv:" + materializedView.getName() + " not exist");
            }
            Partition mvPartition = mv.getPartition(dropPartitionName);
            if (mvPartition == null) {
                throw new DmlException("drop partition failed. partition:" + dropPartitionName + " not exist");
            }

            GlobalStateMgr.getCurrentState().dropPartition(
                    database, materializedView,
                    new DropPartitionClause(false, dropPartitionName, false, true));
        } catch (Exception e) {
            throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                    database.getFullName(), materializedView.getName());
        } finally {
            database.writeUnlock();
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
            Map<String, Set<String>> refBaseTableRangePartitionMap = mvContext.getExternalRefBaseTableMVPartitionMap();
            Preconditions.checkState(refBaseTableRangePartitionMap.containsKey(mvPartitionName));
            return refBaseTableRangePartitionMap.get(mvPartitionName);
        } else {
            return Sets.newHashSet(mvPartitionName);
=======
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
                    .flatMap(name -> mvContext.getExternalTableRealPartitionName(baseTable, name).stream())
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    /**
     * @param mvToRefreshedPartitions :  to-refreshed materialized view partition names
     * @return : return to-refreshed base table's table name and partition names mapping
     */
<<<<<<< HEAD
    private Map<Table, Set<String>> getRefTableRefreshPartitions(Set<String> mvToRefreshedPartitions) {
        Table refBaseTable = mvContext.getRefBaseTable();
        Map<Table, Set<String>> refTableAndPartitionNames = Maps.newHashMap();
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table table = tablePair.second;
            if (refBaseTable != null && refBaseTable == table) {
                Set<String> needRefreshTablePartitionNames = Sets.newHashSet();
                Map<String, Set<String>> mvToBaseNameRef = mvContext.getMvRefBaseTableIntersectedPartitions();
                for (String mvPartitionName : mvToRefreshedPartitions) {
                    needRefreshTablePartitionNames.addAll(mvToBaseNameRef.get(mvPartitionName));
                }
                refTableAndPartitionNames.put(table, needRefreshTablePartitionNames);
                return refTableAndPartitionNames;
=======
    @VisibleForTesting
    public Map<TableSnapshotInfo, Set<String>> getRefTableRefreshPartitions(Set<String> mvToRefreshedPartitions) {
        Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames = Maps.newHashMap();
        Map<String, Map<Table, Set<String>>> mvToBaseNameRefs = mvContext.getMvRefBaseTableIntersectedPartitions();
        if (mvToBaseNameRefs == null || mvToBaseNameRefs.isEmpty()) {
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }
        return refTableAndPartitionNames;
    }

    /**
     * Return all non-ref base table and refreshed partitions.
     */
<<<<<<< HEAD
    private Map<Table, Set<String>> getNonRefTableRefreshPartitions() {
        Table partitionTable = mvContext.getRefBaseTable();
        Map<Table, Set<String>> tableNamePartitionNames = Maps.newHashMap();
        for (Pair<BaseTableInfo, Table> tablePair : snapshotBaseTables.values()) {
            Table table = tablePair.second;
            if (partitionTable != null && partitionTable.equals(table)) {
                // do nothing
            } else {
                if (table.isNativeTableOrMaterializedView()) {
                    tableNamePartitionNames.put(table, ((OlapTable) table).getVisiblePartitionNames());
                } else if (table.isView()) {
                    // do nothing
                } else {
                    tableNamePartitionNames.put(table, Sets.newHashSet(PartitionUtil.getPartitionNames(table)));
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                }
            }
        }
        return tableNamePartitionNames;
    }

    /**
<<<<<<< HEAD
     * Collect base olap tables and its partition infos based on refreshed table infos.
     *
     * @param baseTableAndPartitionNames : refreshed base table and its partition names mapping.
     * @return
     */
    private Map<Long, Map<String, MaterializedView.BasePartitionInfo>> getSelectedPartitionInfosOfOlapTable(
            Map<Table, Set<String>> baseTableAndPartitionNames) {
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> changedOlapTablePartitionInfos = Maps.newHashMap();
        for (Map.Entry<Table, Set<String>> entry : baseTableAndPartitionNames.entrySet()) {
            if (entry.getKey().isNativeTableOrMaterializedView()) {
                Map<String, MaterializedView.BasePartitionInfo> partitionInfos = Maps.newHashMap();
                OlapTable olapTable = (OlapTable) entry.getKey();
                for (String partitionName : entry.getValue()) {
                    Partition partition = olapTable.getPartition(partitionName);
                    MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                            partition.getId(), partition.getVisibleVersion(), partition.getVisibleVersionTime());
                    partitionInfos.put(partition.getName(), basePartitionInfo);
                }
                LOG.info("Collect olap base table {}'s refreshed partition infos: {}", olapTable.getName(), partitionInfos);
                changedOlapTablePartitionInfos.put(olapTable.getId(), partitionInfos);
            }
        }
        return changedOlapTablePartitionInfos;
    }

    /**
     * Collect base hive tables and its partition infos based on refreshed table infos.
     *
     * @param baseTableAndPartitionNames : refreshed base table and its partition names mapping.
     * @return
     */
    private Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> getSelectedPartitionInfosOfExternalTable(
            Map<Table, Set<String>> baseTableAndPartitionNames) {
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> changedOlapTablePartitionInfos =
                Maps.newHashMap();
        for (Map.Entry<Table, Set<String>> entry : baseTableAndPartitionNames.entrySet()) {
            if (entry.getKey().isHiveTable() || entry.getKey().isJDBCTable() || entry.getKey().isIcebergTable()) {
                Table table = entry.getKey();
                Optional<BaseTableInfo> baseTableInfoOptional = materializedView.getBaseTableInfos().stream().filter(
                                baseTableInfo -> baseTableInfo.getTableIdentifier().equals(table.getTableIdentifier())).
                        findAny();
                if (!baseTableInfoOptional.isPresent()) {
                    continue;
                }
                BaseTableInfo baseTableInfo = baseTableInfoOptional.get();
                Map<String, MaterializedView.BasePartitionInfo> partitionInfos =
                        getSelectedPartitionInfos(table, Lists.newArrayList(entry.getValue()),
                                baseTableInfo);
                changedOlapTablePartitionInfos.put(baseTableInfo, partitionInfos);
            }
        }
        return changedOlapTablePartitionInfos;
=======
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
                // it's ok to skip because only existed partitions are updated in the version map.
                if (partition == null) {
                    LOG.warn("Partition {} not found in base table {} in refreshing {}, refreshedPartitionNames:{}",
                            partitionName, baseTable.getName(), materializedView.getName(), refreshedPartitionNames);
                    continue;
                }
                MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                        partition.getId(),
                        partition.getDefaultPhysicalPartition().getVisibleVersion(),
                        partition.getDefaultPhysicalPartition().getVisibleVersionTime());
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
            partitionInfos.put(selectedPartitionNames.get(index),
                    new MaterializedView.BasePartitionInfo(-1, modifiedTime, modifiedTime));
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        return partitionInfos;
    }

<<<<<<< HEAD
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
}
=======
    @VisibleForTesting
    public Map<Long, TableSnapshotInfo> getSnapshotBaseTables() {
        return snapshotBaseTables;
    }
}
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
