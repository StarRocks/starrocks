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

import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
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
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.common.DmlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

import static com.starrocks.catalog.MvRefreshArbiter.getMvBaseTableUpdateInfo;
import static com.starrocks.catalog.MvRefreshArbiter.needsToRefreshTable;

/**
 * MV PCT Refresh Partitioner for Partitioned Materialized View which provide utility methods associated partitions during mv
 * refresh.
 */
public abstract class MVPCTRefreshPartitioner {
    private static final Logger LOG = LogManager.getLogger(MVPCTRefreshPartitioner.class);

    protected final MvTaskRunContext mvContext;
    protected final TaskRunContext context;
    protected final Database db;
    protected final MaterializedView mv;

    public MVPCTRefreshPartitioner(MvTaskRunContext mvContext,
                                   TaskRunContext context,
                                   Database db,
                                   MaterializedView mv) {
        this.mvContext = mvContext;
        this.context = context;
        this.db = db;
        this.mv = mv;
    }

    /**
     * Sync mv and base tables partitions, add if base tables add partitions, drop partitions if base tables drop or changed
     * partitions.
     */
    public abstract boolean syncAddOrDropPartitions() throws AnalysisException, LockTimeoutException;

    /**
     * Generate partition predicate for mv refresh according ref base table changed partitions.
     * @param refBaseTable: ref base table to check.
     * @param refBaseTablePartitionNames: ref base table partition names to check.
     * @param mvPartitionSlotRef: mv partition slot ref to generate partition predicate.
     * @return: Return partition predicate for mv refresh.
     * @throws AnalysisException
     */
    public abstract Expr generatePartitionPredicate(Table refBaseTable,
                                                    Set<String> refBaseTablePartitionNames,
                                                    Expr mvPartitionSlotRef) throws AnalysisException;

    /**
     * Get mv partitions to refresh based on the ref base table partitions.
     * @param mvPartitionInfo: mv partition info to check.
     * @param snapshotBaseTables: snapshot base tables to check.
     * @param start: start partition name to check.
     * @param end: end partition name to check.
     * @param force: force to refresh or not.
     * @param mvPotentialPartitionNames: mv potential partition names to check.
     * @return: Return mv partitions to refresh based on the ref base table partitions.
     * @throws AnalysisException
     */
    public abstract Set<String> getMVPartitionsToRefresh(PartitionInfo mvPartitionInfo,
                                                         Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                         String start, String end, boolean force,
                                                         Set<String> mvPotentialPartitionNames) throws AnalysisException;
    public abstract Set<String> getMVPartitionsToRefreshWithForce(int partitionTTLNumber) throws AnalysisException;

    /**
     * Get mv partition names with TTL based on the ref base table partitions.
     * @param materializedView: materialized view to check.
     * @param start: start partition name to refresh.
     * @param end: end partition name to refresh.
     * @param partitionTTLNumber: mv partition TTL number.
     * @param isAutoRefresh: is auto refresh or not.
     * @return: mv to refresh partition names with TTL based on the ref base table partitions.
     * @throws AnalysisException
     */
    public abstract Set<String> getMVPartitionNamesWithTTL(MaterializedView materializedView,
                                                           String start, String end,
                                                           int partitionTTLNumber,
                                                           boolean isAutoRefresh) throws AnalysisException;

    /**
     * Filter to refresh partitions by refresh number.
     *
     * @param mvPartitionsToRefresh     : mv partitions to refresh.
     * @param mvPotentialPartitionNames : mv potential partition names to check.
     * @param tentative see {@link com.starrocks.scheduler.PartitionBasedMvRefreshProcessor#checkMvToRefreshedPartitions}
     */
    public abstract void filterPartitionByRefreshNumber(Set<String> mvPartitionsToRefresh,
                                                        Set<String> mvPotentialPartitionNames,
                                                        boolean tentative);

    /**
     * Check whether the base table is supported partition refresh or not.
     */
    public static boolean isPartitionRefreshSupported(Table baseTable) {
        return ConnectorPartitionTraits.isSupportPCTRefresh(baseTable.getType());
    }

    /**
     * Get mv partitions to refresh based on the ref base table partitions and its updated partitions.
     * @param refBaseTable : ref base table to check.
     * @param baseTablePartitionNames : ref base table partition names to check.
     * @return : Return mv corresponding partition names to the ref base table partition names, null if sync info don't contain.
     */
    protected Set<String> getMvPartitionNamesToRefresh(Table refBaseTable,
                                                       Set<String> baseTablePartitionNames) {
        Set<String> result = Sets.newHashSet();
        Map<Table, Map<String, Set<String>>> refBaseTableMVPartitionMaps = mvContext.getRefBaseTableMVIntersectedPartitions();
        if (refBaseTableMVPartitionMaps == null || !refBaseTableMVPartitionMaps.containsKey(refBaseTable)) {
            LOG.warn("Cannot find need refreshed ref base table partition from synced partition info: {}, " +
                            "refBaseTableMVPartitionMaps: {}", refBaseTable, refBaseTableMVPartitionMaps);
            return null;
        }
        Map<String, Set<String>> refBaseTableMVPartitionMap = refBaseTableMVPartitionMaps.get(refBaseTable);
        for (String basePartitionName : baseTablePartitionNames) {
            if (!refBaseTableMVPartitionMap.containsKey(basePartitionName)) {
                LOG.warn("Cannot find need refreshed ref base table partition from synced partition info: {}, " +
                                "refBaseTableMVPartitionMaps: {}", basePartitionName, refBaseTableMVPartitionMaps);
                return null;
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
        Map<Table, Column> refBaseTableAndColumns = mv.getRefBaseTablePartitionColumns();
        for (Map.Entry<Table, Column> e : refBaseTableAndColumns.entrySet()) {
            Table baseTable = e.getKey();
            
            // refresh all mv partitions when the ref base table is not supported partition refresh
            if (!isPartitionRefreshSupported(baseTable)) {
                LOG.info("The ref base table {} is not supported partition refresh, refresh all " +
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
                LOG.info("The ref base table {} has no updated partitions, and no update related mv partitions: {}",
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
            LOG.info("The ref base table {} has updated partitions: {}, the corresponding " +
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
        Map<Table, Column> tableColumnMap = mv.getRefBaseTablePartitionColumns();
        for (TableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            if (!isPartitionRefreshSupported(snapshotTable)) {
                continue;
            }
            if (tableColumnMap.containsKey(snapshotTable)) {
                continue;
            }
            if (needsToRefreshTable(mv, snapshotTable, false)) {
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
            if (needsToRefreshTable(mv, snapshotTable, false)) {
                return true;
            }
        }
        return false;
    }

    protected void dropPartition(Database db, MaterializedView materializedView, String mvPartitionName) {
        String dropPartitionName = materializedView.getPartition(mvPartitionName).getName();
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, materializedView, LockType.WRITE)) {
            LOG.warn("Fail to lock database {} in drop partition for mv refresh {}", db.getFullName(),
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

            GlobalStateMgr.getCurrentState().getStarRocksMeta().dropPartition(db, materializedView, dropPartitionClause);
        } catch (Exception e) {
            throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                    db.getFullName(), materializedView.getName());
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), materializedView.getId(), LockType.WRITE);
        }
    }
}
