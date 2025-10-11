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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PCell;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.catalog.MvRefreshArbiter.getMvBaseTableUpdateInfo;
import static com.starrocks.catalog.MvRefreshArbiter.needsToRefreshTable;
import static com.starrocks.sql.optimizer.rule.transformation.partition.PartitionSelector.getExpiredPartitionsByRetentionCondition;

/**
 * MV PCT Refresh Partitioner for Partitioned Materialized View which provide utility methods associated partitions during mv
 * refresh.
 */
public abstract class MVPCTRefreshPartitioner {
    protected  static final int CREATE_PARTITION_BATCH_SIZE = 64;

    protected final MvTaskRunContext mvContext;
    protected final TaskRunContext context;
    protected final Database db;
    protected final MaterializedView mv;
    private final Logger logger;

    public MVPCTRefreshPartitioner(MvTaskRunContext mvContext,
                                   TaskRunContext context,
                                   Database db,
                                   MaterializedView mv) {
        this.mvContext = mvContext;
        this.context = context;
        this.db = db;
        this.mv = mv;
        this.logger = MVTraceUtils.getLogger(mv, MVPCTRefreshPartitioner.class);
    }

    /**
     * Sync mv and base tables partitions, add if base tables add partitions, drop partitions if base tables drop or changed
     * partitions.
     */
    public abstract boolean syncAddOrDropPartitions() throws AnalysisException, LockTimeoutException;

    /**
     * Generate partition predicate for mv refresh according ref base table changed partitions.
     *
     * @param refBaseTable:               ref base table to check.
     * @param refBaseTablePartitionNames: ref base table partition names to check.
     * @param mvPartitionSlotRefs:        mv partition slot ref to generate partition predicate.
     * @throws AnalysisException
     * @return: Return partition predicate for mv refresh.
     */
    public abstract Expr generatePartitionPredicate(Table refBaseTable,
                                                    Set<String> refBaseTablePartitionNames,
                                                    List<Expr> mvPartitionSlotRefs) throws AnalysisException;

    /**
     * Generate partition predicate for mv refresh based on the mv partition names.
     * @param tableName: materialized view table name(db + name)
     * @param mvPartitionNames: materialized view partition names to check.
     * @return : partition predicate for mv refresh.
     * @throws AnalysisException
     */
    public abstract Expr generateMVPartitionPredicate(TableName tableName,
                                                      Set<String> mvPartitionNames) throws AnalysisException;

    /**
     * Get mv partition names to refresh based on the mv refresh params.
     */
    public abstract Set<String> getMVPartitionsToRefreshByParams(MVRefreshParams mvRefreshParams) throws AnalysisException;

    /**
     * Get mv partitions to refresh based on the ref base table partitions.
     *
     * @param mvPartitionInfo:           mv partition info to check.
     * @param snapshotBaseTables:        snapshot base tables to check.
     * @param mvPotentialPartitionNames: mv potential partition names to check.
     * @throws AnalysisException
     * @return: Return mv partitions to refresh based on the ref base table partitions.
     */
    public abstract Set<String> getMVPartitionsToRefresh(PartitionInfo mvPartitionInfo,
                                                         Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                         MVRefreshParams mvRefreshParams,
                                                         Set<String> mvPotentialPartitionNames) throws AnalysisException;

    public Set<String> getMVPartitionsToRefreshWithForce(MVRefreshParams mvRefreshParams) throws AnalysisException {
        Set<String> toRefreshPartitions = getMVPartitionsToRefreshByParams(mvRefreshParams);
        if (CollectionUtils.isEmpty(toRefreshPartitions) || !mv.isPartitionedTable()) {
            return toRefreshPartitions;
        }
        Map<String, PCell> mvListPartitionMap = mv.getPartitionCells(Optional.empty());
        Map<String, PCell> validToRefreshPartitions = toRefreshPartitions.stream()
                .filter(mvListPartitionMap::containsKey)
                .map(name -> Pair.create(name, mvListPartitionMap.get(name)))
                .collect(Collectors.toMap(x -> x.first, x -> x.second));
        filterPartitionsByTTL(validToRefreshPartitions, true);
        return validToRefreshPartitions.keySet();
    }

    /**
     * Get mv partition names with TTL based on the ref base table partitions.
     *
     * @param materializedView: materialized view to check.
     * @param isAutoRefresh:    is auto refresh or not.
     * @throws AnalysisException
     * @return: mv to refresh partition names with TTL based on the ref base table partitions.
     */
    public abstract Set<String> getMVPartitionNamesWithTTL(MaterializedView materializedView,
                                                           MVRefreshParams mvRefreshParams,
                                                           boolean isAutoRefresh) throws AnalysisException;

    /**
<<<<<<< HEAD
     * Filter to refresh partitions by refresh number.
     *
     * @param mvPartitionsToRefresh     : mv partitions to refresh.
     * @param mvPotentialPartitionNames : mv potential partition names to check.
     * @param tentative                 see {@link com.starrocks.scheduler.PartitionBasedMvRefreshProcessor}
     */
    public abstract void filterPartitionByRefreshNumber(Set<String> mvPartitionsToRefresh,
                                                        Set<String> mvPotentialPartitionNames,
                                                        boolean tentative);

    public abstract void filterPartitionByAdaptiveRefreshNumber(Set<String> mvPartitionsToRefresh,
                                                        Set<String> mvPotentialPartitionNames,
                                                        boolean tentative);
=======
     * Filter to refresh partitions by partition refresh number.
     * @param partitionsToRefresh: partitions to refresh.
     */
    @VisibleForTesting
    public abstract void filterPartitionByRefreshNumber(
            PCellSortedSet partitionsToRefresh,
            MaterializedView.PartitionRefreshStrategy refreshStrategy);

    /**
     * Check whether to calculate the potential partitions to refresh or not. When the base table changed partitions
     * contain many-to-many relation partitions with mv partitions, should calculate the potential partitions to refresh.
     */
    public abstract boolean isCalcPotentialRefreshPartition(Map<Table, PCellSortedSet> baseChangedPartitionNames,
                                                            PCellSortedSet mvPartitions);
    /**
     * Calculate the associated potential partitions to refresh according to the partitions to refresh.
     * NOTE: This must be called after filterMVToRefreshPartitions, otherwise it may lose some potential to-refresh mv partitions
     * which will cause filtered insert load.
     * @param result: partitions to refresh for materialized view which will be changed in this method.
     */
    public PCellSortedSet calcPotentialMVRefreshPartitions(PCellSortedSet result) {
        // check non-ref base tables or force refresh
        Map<Table, Set<String>> baseChangedPartitionNames = getBasePartitionNamesByMVPartitionNames(result);
        if (baseChangedPartitionNames.isEmpty()) {
            logger.info("Cannot get associated base table change partitions from mv's refresh partitions {}",
                    result);
            return result;
        }

        // use base table's changed partitions instead of to-refresh partitions to decide
        Map<Table, PCellSortedSet> baseChangedPCellsSortedSet = toBaseTableWithSortedSet(baseChangedPartitionNames);
        Map<String, PCell> mvRangePartitionMap = mvContext.getMVToCellMap();
        Set<String> mvToRefreshPartitionNames = result.getPartitionNames();
        if (isCalcPotentialRefreshPartition(baseChangedPCellsSortedSet, result)) {
            // because the relation of partitions between materialized view and base partition table is n : m,
            // should calculate the candidate partitions recursively.
            logger.info("Start calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                    " baseChangedPartitionNames: {}", result, baseChangedPCellsSortedSet);
            Set<String> potentialMvToRefreshPartitionNames = Sets.newHashSet(mvToRefreshPartitionNames);
            SyncPartitionUtils.calcPotentialRefreshPartition(potentialMvToRefreshPartitionNames,
                    baseChangedPartitionNames,
                    mvContext.getRefBaseTableMVIntersectedPartitions(),
                    mvContext.getMvRefBaseTableIntersectedPartitions(),
                    mvToRefreshPotentialPartitions);
            Set<String> newMvToRefreshPartitionNames =
                    Sets.difference(potentialMvToRefreshPartitionNames, mvToRefreshPartitionNames);
            for (String partitionName : newMvToRefreshPartitionNames) {
                PCell pCell = mvRangePartitionMap.get(partitionName);
                if (pCell == null) {
                    logger.warn("Cannot find mv partition name range cell:{}", partitionName);
                    continue;
                }
                result.add(PCellWithName.of(partitionName, pCell));
            }
            logger.info("Finish calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                    " baseChangedPartitionNames: {}", result, baseChangedPartitionNames);
        }
        return result;
    }

    /**
     * Filter mv to refresh partitions by some properties, like auto_partition_refresh_number.
     * @param mvToRefreshedPartitions: partitions to refresh for materialized view
     */
    public void filterMVToRefreshPartitionsByProperty(PCellSortedSet mvToRefreshedPartitions) {
        // do nothing by default
    }

    /**
     * @return the partitions to refresh for materialized view
     */
    private PCellSortedSet getMVPartitionsToRefresh(Map<Long, BaseTableSnapshotInfo> snapshotBaseTables)
            throws AnalysisException {
        if (mvRefreshParams.isForce()) {
            // Force refresh
            return getMVPartitionsToRefreshWithForce();
        } else {
            return getMVPartitionsToRefreshWithCheck(snapshotBaseTables);
        }
    }

    /**
     * Compute the partitioned to be refreshed in this task, according to [start, end), ttl, and other context info
     * If it's tentative, only return the result rather than modify any state
     * IF it's not, it would modify the context state, like `NEXT_PARTITION_START`
     */
    public PCellSortedSet getMVToRefreshedPartitions(Map<Long, BaseTableSnapshotInfo> snapshotBaseTables)
            throws AnalysisException, LockTimeoutException {
        PCellSortedSet mvToRefreshedPartitions = null;
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(db.getId(), mv.getId(),
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            logger.warn("failed to lock database: {} in checkMvToRefreshedPartitions", db.getFullName());
            throw new LockTimeoutException("Failed to lock database: " + db.getFullName());
        }

        try {
            mvToRefreshedPartitions = getMVPartitionsToRefresh(snapshotBaseTables);
            if (mvToRefreshedPartitions == null || mvToRefreshedPartitions.isEmpty()) {
                logger.info("no partitions to refresh for materialized view");
                return mvToRefreshedPartitions;
            }
            // filter partitions to avoid refreshing too many partitions
            filterMVToRefreshPartitions(mvToRefreshedPartitions);

            // calculate the associated potential partitions to refresh
            mvToRefreshedPartitions = calcPotentialMVRefreshPartitions(mvToRefreshedPartitions);

            int partitionRefreshNumber = mv.getTableProperty().getPartitionRefreshNumber();
            logger.info("filter partitions to refresh partitionRefreshNumber={}, partitionsToRefresh:{}, " +
                            "mvPotentialPartitionNames:{}, next start:{}, next end:{}, next list values:{}",
                    partitionRefreshNumber, mvToRefreshedPartitions, mvToRefreshPotentialPartitions,
                    mvContext.getNextPartitionStart(), mvContext.getNextPartitionEnd(), mvContext.getNextPartitionValues());
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.READ);
        }
        return mvToRefreshedPartitions;
    }

    private void filterMVToRefreshPartitions(PCellSortedSet mvToRefreshedPartitions) {
        if (mvToRefreshedPartitions == null || mvToRefreshedPartitions.isEmpty()) {
            return;
        }

        // first filter partitions by user's config(eg: auto_refresh_partition_number)
        filterMVToRefreshPartitionsByProperty(mvToRefreshedPartitions);
        logger.info("after filterMVToRefreshPartitionsByProperty, partitionsToRefresh: {}",
                mvToRefreshedPartitions);
        if (mvToRefreshedPartitions.isEmpty() || mvToRefreshedPartitions.size() <= 1) {
            return;
        }
        // refresh all partition when it's a sync refresh, otherwise updated partitions may be lost.
        ExecuteOption executeOption = mvContext.getExecuteOption();
        if (executeOption != null && executeOption.getIsSync()) {
            return;
        }
        // ignore if mv is not partitioned.
        if (!mv.isPartitionedTable()) {
            return;
        }

        // filter partitions by partition refresh strategy
        boolean hasUnsupportedTableType = mv.getBaseTableTypes().stream()
                .anyMatch(type -> !SUPPORTED_TABLE_TYPES_FOR_ADAPTIVE_MV_REFRESH.contains(type));
        if (hasUnsupportedTableType) {
            filterPartitionByRefreshNumber(mvToRefreshedPartitions, MaterializedView.PartitionRefreshStrategy.STRICT);
        } else {
            MaterializedView.PartitionRefreshStrategy partitionRefreshStrategy = mv.getPartitionRefreshStrategy();
            filterPartitionByRefreshNumber(mvToRefreshedPartitions, partitionRefreshStrategy);
        }
        logger.info("after filterPartitionByAdaptive, partitionsToRefresh: {}",
                mvToRefreshedPartitions);
    }
>>>>>>> ee66eb3b3f ([Enhancement] Change default_mv_partition_refresh_strategy to adaptive by default (#63594))

    /**
     * Determines the number of partitions to refresh based on the given refresh strategy.
     *
     * <p>
     * This method supports two refresh strategies for materialized views (MVs):
     * <ul>
     *     <li><b>STRICT</b> (default): Always refresh a fixed number of partitions,
     *         as configured by {@code partition_refresh_number} in the MV's table property.</li>
     *     <li><b>ADAPTIVE</b>: Dynamically determines how many partitions to refresh
     *         based on the statistics (row count and data size) of the referenced base table partitions.
     *         This strategy helps reduce refresh cost for large partitions.
     *     </li>
     * </ul>
     *
     * <p>
     * Since external table statistics may be easily outdated or incomplete, adaptive refresh
     * may fail due to missing or invalid metadata. In such cases, the method automatically
     * falls back to the STRICT strategy to ensure the MV can still be refreshed correctly.
     *
     * @param toRefreshPartitions sorted partition names to be refreshed.
     * @param refreshStrategy     The refresh strategy: either ADAPTIVE or STRICT.
     * @return The number of partitions to refresh.
     */
<<<<<<< HEAD
    public int getRefreshNumberByMode(Iterator<String> sortedPartitionIterator,
                                       MaterializedView.PartitionRefreshStrategy refreshStrategy) {
=======
    public int getPartitionRefreshNumberAdaptive(PCellSortedSet toRefreshPartitions,
                                                 MaterializedView.PartitionRefreshStrategy refreshStrategy) {
>>>>>>> ee66eb3b3f ([Enhancement] Change default_mv_partition_refresh_strategy to adaptive by default (#63594))
        try {
            switch (refreshStrategy) {
                case ADAPTIVE:
                    return getAdaptivePartitionRefreshNumber(toRefreshPartitions);
                case STRICT:
                default:
                    return getRefreshNumberByDefaultMode(toRefreshPartitions);
            }
        } catch (Exception e) {
            logger.warn("Adaptive refresh failed for mode '{}', falling back to STRICT mode. Reason: {}",
                    refreshStrategy, e.getMessage(), e);
            return getRefreshNumberByDefaultMode(toRefreshPartitions);
        }
    }

<<<<<<< HEAD
    protected abstract int getAdaptivePartitionRefreshNumber(Iterator<String> partitionNameIter)
            throws MVAdaptiveRefreshException;
=======
    /**
     * Get partition refresh number by default mode, which is used to be compatible with old versions and the following
     * conditions are met:
     * - partition_refresh_strategy is not set strict by user
     * - partition_refresh_number is not set by user
     */
    private int getRefreshNumberByDefaultMode(PCellSortedSet torRefreshPartitions) {
        int defaultPartitionRefreshNumber = mv.getTableProperty().getPartitionRefreshNumber();
        TableProperty tableProperty = mv.getTableProperty();
        // if user has set partition_refresh_number, use it directly
        if (tableProperty == null || tableProperty.isSetPartitionRefreshNumber()
                || defaultPartitionRefreshNumber != Config.default_mv_partition_refresh_number) {
            return defaultPartitionRefreshNumber;
        }
        MaterializedView.PartitionRefreshStrategy partitionRefreshStrategy =
                MaterializedView.PartitionRefreshStrategy.of(tableProperty.getPartitionRefreshStrategy());
        // if partition_refresh_strategy is strict, use partition_refresh_number directly
        if (tableProperty.isSetPartitionRefreshStrategy()
                && MaterializedView.PartitionRefreshStrategy.STRICT.equals(partitionRefreshStrategy)) {
            return defaultPartitionRefreshNumber;
        }
        // if the number of partitions to refresh is not too many, use it directly
        int toRefreshPartitionNum = torRefreshPartitions.size();
        if (toRefreshPartitionNum <= Config.mv_max_partitions_num_per_refresh) {
            return defaultPartitionRefreshNumber;
        }
        // to be compatible with old version, use adaptive mode if partition_refresh_strategy is not set
        int toRefreshPartitionNumPerTaskRun =
                (int) Math.ceil((double) toRefreshPartitionNum / Math.max(1, Config.task_runs_concurrency));

        // if there are too many partitions to refresh, limit the number of partitions to refresh per task run
        int finalToRefreshPartitionNumPerTaskRun = Math.min(toRefreshPartitionNumPerTaskRun,
                Config.mv_max_partitions_num_per_refresh);
        return finalToRefreshPartitionNumPerTaskRun;
    }

    public int getAdaptivePartitionRefreshNumber(PCellSortedSet toRefreshPartitions)
            throws MVAdaptiveRefreshException {
        if (!mv.isPartitionedTable()) {
            return toRefreshPartitions.size();
        }

        Map<String, Map<Table, Set<String>>> mvToBaseNameRefs = mvContext.getMvRefBaseTableIntersectedPartitions();
        MVRefreshPartitionSelector mvRefreshPartitionSelector =
                new MVRefreshPartitionSelector(Config.mv_max_rows_per_refresh, Config.mv_max_bytes_per_refresh,
                        Config.mv_max_partitions_num_per_refresh, mvContext.getExternalRefBaseTableMVPartitionMap());
        int adaptiveRefreshNumber = 0;
        for (PCellWithName pCellWithName : toRefreshPartitions.partitions()) {
            String mvRefreshPartition = pCellWithName.name();
            Map<Table, Set<String>> refBaseTablesPartitions = mvToBaseNameRefs.get(mvRefreshPartition);
            if (mvRefreshPartitionSelector.canAddPartition(refBaseTablesPartitions)) {
                mvRefreshPartitionSelector.addPartition(refBaseTablesPartitions);
                adaptiveRefreshNumber++;
            } else {
                break;
            }
        }
        return adaptiveRefreshNumber;
    }
>>>>>>> ee66eb3b3f ([Enhancement] Change default_mv_partition_refresh_strategy to adaptive by default (#63594))

    /**
     * Check whether the base table is supported partition refresh or not.
     */
    public static boolean isPartitionRefreshSupported(Table baseTable) {
        // An external table is not supported to refresh by partition.
        return ConnectorPartitionTraits.isSupportPCTRefresh(baseTable.getType()) &&
                !MaterializedViewAnalyzer.isExternalTableFromResource(baseTable);
    }

    /**
     * Get mv partitions to refresh based on the ref base table partitions and its updated partitions.
     * @param refBaseTable            : ref base table to check.
     * @param baseTablePartitionNames : ref base table partition names to check.
     * @return : Return mv corresponding partition names to the ref base table partition names, null if sync info don't contain.
     */
    protected Set<String> getMvPartitionNamesToRefresh(Table refBaseTable,
                                                       Set<String> baseTablePartitionNames) {
        Set<String> result = Sets.newHashSet();
        Map<Table, Map<String, Set<String>>> refBaseTableMVPartitionMaps = mvContext.getRefBaseTableMVIntersectedPartitions();
        if (refBaseTableMVPartitionMaps == null || !refBaseTableMVPartitionMaps.containsKey(refBaseTable)) {
            logger.warn("Cannot find need refreshed ref base table partition from synced partition info: {}, " +
                    "refBaseTableMVPartitionMaps: {}", refBaseTable, refBaseTableMVPartitionMaps);
            return null;
        }
        Map<String, Set<String>> refBaseTableMVPartitionMap = refBaseTableMVPartitionMaps.get(refBaseTable);
        for (String basePartitionName : baseTablePartitionNames) {
            if (!refBaseTableMVPartitionMap.containsKey(basePartitionName)) {
                logger.warn("Cannot find need refreshed ref base table partition from synced partition info: {}, " +
                        "refBaseTableMVPartitionMaps: {}", basePartitionName, refBaseTableMVPartitionMaps);
                // refBaseTableMVPartitionMap may not contain basePartitionName if it's filtered by ttl.
                continue;
            }
            result.addAll(refBaseTableMVPartitionMap.get(basePartitionName));
        }
        return result;
    }

    /**
     * Get mv partitions to refresh based on the ref base table partitions.
     * @param mvPartitionNames all mv partition names
     * @return mv partitions to refresh based on the ref base table partitions
     */
    protected Set<String> getMvPartitionNamesToRefresh(Set<String> mvPartitionNames) {
        Set<String> result = Sets.newHashSet();
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        for (Table baseTable : refBaseTablePartitionColumns.keySet()) {
            // refresh all mv partitions when the ref base table is not supported partition refresh
            if (!isPartitionRefreshSupported(baseTable)) {
                logger.info("The ref base table {} is not supported partition refresh, refresh all " +
                        "partitions of mv {}: {}", baseTable.getName(), mv.getName(), mvPartitionNames);
                return mvPartitionNames;
            }

            // check the updated partition names in the ref base table
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo = getMvBaseTableUpdateInfo(mv, baseTable,
                    false, false);
            if (mvBaseTableUpdateInfo == null) {
                throw new DmlException(String.format("Find the updated partition info of ref base table %s of mv " +
                        "%s failed, current mv partitions:%s", baseTable.getName(), mv.getName(), mvPartitionNames));
            }
            Set<String> refBaseTablePartitionNames = mvBaseTableUpdateInfo.getToRefreshPartitionNames();
            if (refBaseTablePartitionNames.isEmpty()) {
                logger.info("The ref base table {} has no updated partitions, and no update related mv partitions: {}",
                        baseTable.getName(), mvPartitionNames);
                continue;
            }

            // fetch the corresponding materialized view partition names as the need to refresh partitions
            Set<String> ans = getMvPartitionNamesToRefresh(baseTable, refBaseTablePartitionNames);
            if (ans == null) {
                throw new DmlException(String.format("Find the corresponding mv partition names of ref base table %s failed," +
                        " mv %s:, ref partitions: %s", baseTable.getName(), mv.getName(), refBaseTablePartitionNames));
            }
            ans.retainAll(mvPartitionNames);
            logger.info("The ref base table {} has updated partitions: {}, the corresponding " +
                            "mv partitions to refresh: {}, " + "mvRangePartitionNames: {}", baseTable.getName(),
                    refBaseTablePartitionNames, ans, mvPartitionNames);
            result.addAll(ans);
        }
        return result;
    }

    /**
     * Whether partitioned materialized view needs to be refreshed or not base on the non-ref base tables, it needs refresh when:
     * - its non-ref base table except un-supported base table has updated.
     */
    protected boolean needsRefreshBasedOnNonRefTables(Map<Long, TableSnapshotInfo> snapshotBaseTables) {
        Map<Table, List<Column>> tableColumnMap = mv.getRefBaseTablePartitionColumns();
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            if (!isPartitionRefreshSupported(snapshotTable)) {
                continue;
            }
            if (tableColumnMap.containsKey(snapshotTable)) {
                continue;
            }
            if (needsToRefreshTable(mv, snapshotInfo.getBaseTableInfo(), snapshotTable, false)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether non-partitioned materialized view needs to be refreshed or not, it needs refresh when:
     * - its base table is not supported refresh by partition.
     * - its base table has updated.
     */
    public static boolean isNonPartitionedMVNeedToRefresh(Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                          MaterializedView mv) {
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            if (!isPartitionRefreshSupported(snapshotTable)) {
                return true;
            }
            if (needsToRefreshTable(mv, snapshotInfo.getBaseTableInfo(), snapshotTable, false)) {
                return true;
            }
        }
        return false;
    }

    public void dropPartition(Database db, MaterializedView materializedView, String mvPartitionName) {
        String dropPartitionName = materializedView.getPartition(mvPartitionName).getName();
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(db.getId(), materializedView.getId(), LockType.WRITE,
                Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            logger.warn("Fail to lock database {} in drop partition for mv refresh {}", db.getFullName(),
                    materializedView.getName());
            throw new DmlException("drop partition failed. database:" + db.getFullName() + " not exist");
        }
        try {
            // check
            Table mv = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), materializedView.getId());
            if (mv == null) {
                throw new DmlException("drop partition failed. mv:" + materializedView.getName() + " not exist");
            }
            Partition mvPartition = mv.getPartition(dropPartitionName);
            if (mvPartition == null) {
                throw new DmlException("drop partition failed. partition:" + dropPartitionName + " not exist");
            }

            DropPartitionClause dropPartitionClause = new DropPartitionClause(false, dropPartitionName, false, true);
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(materializedView);
            analyzer.analyze(new ConnectContext(), dropPartitionClause);

            GlobalStateMgr.getCurrentState().getLocalMetastore().dropPartition(db, materializedView, dropPartitionClause);
        } catch (Exception e) {
            throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                    db.getFullName(), materializedView.getName());
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), materializedView.getId(), LockType.WRITE);
        }
    }

    /**
     * @param mvPartitionNames : the need to refresh materialized view partition names
     * @return : the corresponding ref base table partition names to the materialized view partition names
     */
    protected Map<Table, Set<String>> getBasePartitionNamesByMVPartitionNames(Set<String> mvPartitionNames) {
        Map<Table, Set<String>> result = new HashMap<>();
        Map<String, Map<Table, Set<String>>> mvRefBaseTablePartitionMaps =
                mvContext.getMvRefBaseTableIntersectedPartitions();
        for (String mvPartitionName : mvPartitionNames) {
            if (mvRefBaseTablePartitionMaps == null || !mvRefBaseTablePartitionMaps.containsKey(mvPartitionName)) {
                logger.warn("Cannot find need refreshed mv table partition from synced partition info: {}",
                        mvPartitionName);
                continue;
            }
            Map<Table, Set<String>> mvRefBaseTablePartitionMap = mvRefBaseTablePartitionMaps.get(mvPartitionName);
            for (Map.Entry<Table, Set<String>> entry : mvRefBaseTablePartitionMap.entrySet()) {
                Table baseTable = entry.getKey();
                Set<String> baseTablePartitions = entry.getValue();
                // If the result already contains the base table name, add all new partitions to the existing set
                // If the result doesn't contain the base table name, put the new set into the map
                result.computeIfAbsent(baseTable, k -> Sets.newHashSet()).addAll(baseTablePartitions);
            }
        }
        return result;
    }

    /**
     * Filter partitions by ttl, save the kept partitions and return the next task run partition values.
     * @param toRefreshPartitions the partitions to refresh/add
     * @return the next task run partition list cells after the reserved partition_ttl_number
     */
    protected void filterPartitionsByTTL(Map<String, PCell> toRefreshPartitions,
                                         boolean isMockPartitionIds) {
        if (!CollectionUtils.sizeIsEmpty(toRefreshPartitions)) {
            // filter partitions by partition_retention_condition
            String ttlCondition = mv.getTableProperty().getPartitionRetentionCondition();
            if (!Strings.isNullOrEmpty(ttlCondition)) {
                List<String> expiredPartitionNames = getExpiredPartitionsByRetentionCondition(db, mv, ttlCondition,
                        toRefreshPartitions, isMockPartitionIds);
                // remove the expired partitions
                if (CollectionUtils.isNotEmpty(expiredPartitionNames)) {
                    logger.info("Filter partitions by partition_retention_condition, ttl_condition:{}, expired:{}",
                            ttlCondition, expiredPartitionNames);
                    expiredPartitionNames.stream()
                            .forEach(toRefreshPartitions::remove);
                }
            }
        }
    }
<<<<<<< HEAD
=======

    /**
     * Filter partitions with retention ttl condition, remove the expired partitions from the toRefreshPartitions.
     */
    private List<String> getExpiredPartitionsWithRetention(String ttlCondition,
                                                           Map<String, PCell> toRefreshPartitions,
                                                           boolean isMockPartitionIds) {
        return getExpiredPartitionsByRetentionCondition(db, mv, ttlCondition, toRefreshPartitions, isMockPartitionIds);
    }

    protected void filterPartitionsByTTL(PCellSortedSet toRefreshPartitions,
                                         boolean isMockPartitionIds) {
        if (toRefreshPartitions == null || toRefreshPartitions.isEmpty()) {
            return;
        }
        // filter partitions by partition_retention_condition
        String ttlCondition = mv.getTableProperty().getPartitionRetentionCondition();
        if (Strings.isNullOrEmpty(ttlCondition)) {
            return;
        }
        // convert PCellWithName to PCell
        Map<String, PCell> toRefreshPartitionMap = toRefreshPartitions.partitions()
                .stream()
                .map(p -> Pair.create(p.name(), p.cell()))
                .collect(Collectors.toMap(
                        p -> p.first,
                        p -> p.second
                ));
        List<String> toRemovePartitions =
                getExpiredPartitionsWithRetention(ttlCondition, toRefreshPartitionMap, isMockPartitionIds);
        if (CollectionUtils.isNotEmpty(toRemovePartitions)) {
            toRemovePartitions.stream()
                    .filter(p -> toRefreshPartitionMap.containsKey(p))
                    .map(p -> PCellWithName.of(p, toRefreshPartitionMap.get(p)))
                    .forEach(toRefreshPartitions::remove);
        }
    }

    /**
     * Get the partition ttl limit for the refreshed mv.
     */
    protected int getPartitionTTLLimit() {
        int partitionTTLNumber = mvContext.getPartitionTTLNumber();
        if (partitionTTLNumber > 0) {
            return partitionTTLNumber;
        } else {
            return TableProperty.INVALID;
        }
    }

    /**
     * Return the limit of partitions to refresh which is configured in the mv's table property.
     * NOTE:
     * - This parameter is used to limit the number of partitions to refresh after partition_ttl which is used for
     * the refreshed mv partitions to avoid refreshing too many partitions at once.
     * - But this parameter will make the mv's result unequal to the defined query's result, since it may refresh fewer
     * partitions than its needs.
     * - This parameter has different meanings with the old versions which it will limit the number of partitions to refresh
     * no matter its auto refresh or manual refresh from v4.0.
     */
    protected int getRefreshPartitionLimit() {
        int autoRefreshPartitionsLimit = mv.getTableProperty().getAutoRefreshPartitionsLimit();
        if (autoRefreshPartitionsLimit > 0) {
            return autoRefreshPartitionsLimit;
        } else {
            return TableProperty.INVALID;
        }
    }

    protected PCellSortedSet toPCellSortedSet(Set<String> partitionNames, Map<String, PCell> partitionToCells) {
        List<PCellWithName> pCellWithNames = Lists.newArrayList();
        for (String partitionName : partitionNames) {
            if (!partitionToCells.containsKey(partitionName)) {
                logger.warn("Cannot find partition name range cell:{}", partitionName);
                continue;
            }
            PCell pCell = partitionToCells.get(partitionName);
            pCellWithNames.add(PCellWithName.of(partitionName, pCell));
        }
        return PCellSortedSet.of(pCellWithNames);
    }

    protected Map<Table, PCellSortedSet> toBaseTableWithSortedSet(Map<Table, Set<String>> baseToPartitionNames) {
        Map<Table, PCellSortedSet> result = new HashMap<>();
        Map<Table, Map<String, PCell>> refBaseTableRangePartitionMap = mvContext.getRefBaseTableToCellMap();
        for (Map.Entry<Table, Set<String>> entry : baseToPartitionNames.entrySet()) {
            Table baseTable = entry.getKey();
            Set<String> partitionNames = entry.getValue();
            if (!refBaseTableRangePartitionMap.containsKey(baseTable)) {
                logger.warn("Cannot find base table partition name to range cell map: {}", baseTable.getName());
                continue;
            }
            Map<String, PCell> partitionToCells = refBaseTableRangePartitionMap.get(baseTable);
            PCellSortedSet pCellSortedSet = toPCellSortedSet(partitionNames, partitionToCells);
            result.put(baseTable, pCellSortedSet);
        }
        return result;
    }

    /**
     * Get the iterator of to refresh partitions according to config.
     */
    public Iterator<PCellWithName> getToRefreshPartitionsIterator(PCellSortedSet toRefreshPartitions,
                                                                  boolean isAscending) {
        if (isAscending) {
            return toRefreshPartitions.iterator();
        } else {
            return toRefreshPartitions.descendingIterator();
        }
    }
>>>>>>> ee66eb3b3f ([Enhancement] Change default_mv_partition_refresh_strategy to adaptive by default (#63594))
}
