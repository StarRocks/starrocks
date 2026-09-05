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


package com.starrocks.scheduler.mv.pct;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.mv.pct.BaseToMVPartitionMapping;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshParams;
import com.starrocks.scheduler.mv.MVTraceUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PartitionDiff;
import com.starrocks.sql.common.PartitionDiffResult;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class MVPCTRefreshListPartitioner extends MVPCTRefreshPartitioner {
    private final ListPartitionDiffer differ;
    final Logger logger;

    public MVPCTRefreshListPartitioner(MvTaskRunContext mvContext,
                                       TaskRunContext context,
                                       Database db,
                                       MaterializedView mv,
                                       MVRefreshParams mvRefreshParams) {
        super(mvContext, context, db, mv, mvRefreshParams);
        this.differ = new ListPartitionDiffer(mv, queryRewriteParams);
        this.logger = MVTraceUtils.getLogger(mv, MVPCTRefreshListPartitioner.class);
    }

    public PCellSortedSet getMVPartitionsToRefreshByParams() {
        if (mvRefreshParams.isCompleteRefresh()) {
            return mv.getListPartitionItems();
        } else {
            Set<PListCell> pListCells = mvRefreshParams.getListValues();
            PCellSortedSet mvPartitions = mv.getListPartitionItems();
            PCellSortedSet mvFilteredPartitions = PCellSortedSet.of();
            for (PCellWithName pCellWithName : mvPartitions.getPartitions()) {
                PListCell mvListCell = (PListCell) pCellWithName.cell();
                if (mvListCell.getItemSize() == 1) {
                    // if list value is a single value, check it directly
                    if (pListCells.contains(mvListCell)) {
                        mvFilteredPartitions.add(pCellWithName);
                    }
                } else {
                    // if list values is multi values, split it into single values and check it then.
                    if (mvListCell.toSingleValueCells().stream().anyMatch(i -> pListCells.contains(i))) {
                        mvFilteredPartitions.add(pCellWithName);
                    }
                }
            }
            return mvFilteredPartitions;
        }
    }

    @Override
    public boolean syncAddOrDropPartitions() throws LockTimeoutException {
        // collect mv partition items with lock
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(db.getId(), mv.getId(),
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException(String.format("Materialized view %s.%s refresh failed: " +
                    "failed to acquire read lock on database %s within %d ms when syncing list partitions",
                    db.getFullName(), mv.getName(), db.getFullName(), Config.mv_refresh_try_lock_timeout_ms));
        }

        PartitionDiffResult result;
        try {
            differ.setPinnedRanges(mvContext.getRefreshRuntimeState().getPinnedTvrMap());
            result = differ.computePartitionDiff(null);
            if (result == null) {
                logger.warn("compute list partition diff failed, result is null");
                return false;
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.READ);
        }
        logger.info("The process of synchronizing materialized view [{}] partition diff result: {}",
                mv.getName(), result);

        // drop old partitions and add new partitions
        final PartitionDiff partitionDiff = result.diff;
        // We should delete the old partition first and then add the new one,
        // because the old and new partitions may overlap
        final PCellSortedSet deletes = partitionDiff.getDeletes();
        for (String mvPartitionName : deletes.getPartitionNames()) {
            dropPartition(db, mv, mvPartitionName);
        }
        logger.info("The process of synchronizing materialized view [{}] delete partitions list [{}]",
                mv.getName(), deletes);

        // add partitions
        final Map<String, String> partitionProperties = MvUtils.getPartitionProperties(mv);
        final DistributionDesc distributionDesc = MvUtils.getDistributionDesc(mv);
        final PCellSortedSet adds = partitionDiff.getAdds();
        // filter by partition ttl
        filterPartitionsByTTL(adds, true);
        // add partitions for mv
        addListPartitions(db, mv, adds, partitionProperties, distributionDesc);
        logger.info("The process of synchronizing materialized view [{}] add partitions list [{}]",
                mv.getName(), adds);

        // add into mv context
        result.mvPartitionToCells.addAll(adds);
        final Map<Table, PCellSortedSet> refBaseTableCells =
                BaseToMVPartitionMapping.extractCells(result.refBaseTablePartitionMap);
        // base table -> Map<partition name -> mv partition names>
        final Map<Table, PCellSetMapping> baseToMvNameRef =
                differ.generateBaseRefMap(refBaseTableCells, result.mvPartitionToCells);
        // mv partition name -> Map<base table -> base partition names>
        final Map<String, Map<Table, PCellSortedSet>> mvToBaseNameRef =
                differ.generateMvRefMap(result.mvPartitionToCells, refBaseTableCells);
        publishTopology(new PCTPartitionTopology(result.mvPartitionToCells, result.refBaseTablePartitionMap,
                baseToMvNameRef, mvToBaseNameRef));
        return true;
    }

    @Override
    public Expr generatePartitionPredicate(Table refBaseTable, PCellSortedSet refBaseTablePartitionNames,
                                           List<Expr> mvPartitionSlotRefs) throws AnalysisException {
        return new PCTPredicateBuilder(this).buildPartitionPredicate(refBaseTable,
                refBaseTablePartitionNames, mvPartitionSlotRefs);
    }

    @Override
    public Expr generateMVPartitionPredicate(TableName tableName,
                                             PCellSortedSet mvPartitionNames) throws AnalysisException {
        return new PCTPredicateBuilder(this).buildMVPartitionPredicate(tableName, mvPartitionNames);
    }

    @Override
    public PCellSortedSet getMVPartitionsToRefreshWithCheck(Map<Long, BaseTableSnapshotInfo> snapshotBaseTables) {
        // list partitioned materialized view
        boolean isAutoRefresh = isAutomaticRefresh();
        PCellSortedSet mvListPartitionNames = getMVPartitionNamesWithTTL(isAutoRefresh);

        // check non-ref base tables
        if (mvRefreshParams.isForce() || needsRefreshBasedOnNonRefTables(snapshotBaseTables)) {
            if (mvRefreshParams.isCompleteRefresh()) {
                // if non-partition table changed, should refresh all partitions of materialized view
                return mvListPartitionNames;
            } else {
                // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                // it should be refreshed according to the user-specified range, not all partitions.
                return getMvPartitionNamesToRefresh(mvListPartitionNames);
            }
        } else {
            // check the ref base table
            return getMvPartitionNamesToRefresh(mvListPartitionNames);
        }
    }

    @Override
    public boolean isCalcPotentialRefreshPartition(Map<Table, PCellSortedSet> baseChangedPCellsSortedSet,
                                                   PCellSortedSet mvPartitions) {
        // TODO: If base table's list partitions contain multi values, should calculate potential partitions.
        // Only check base table's partition values intersection with mv's to-refresh partitions later.
        return baseChangedPCellsSortedSet.entrySet()
                .stream()
                .anyMatch(e -> e.getValue().getPCells().stream().anyMatch(l -> ((PListCell) l).getItemSize() > 1));
    }

    @Override
    public PCellSortedSet getMVPartitionNamesWithTTL(boolean isAutoRefresh) {
        // if the user specifies the start and end ranges, only refresh the specified partitions
        boolean isCompleteRefresh = mvRefreshParams.isCompleteRefresh();
        PCellSortedSet mvListPartitionMap;
        if (!isCompleteRefresh) {
            mvListPartitionMap = PCellSortedSet.of();
            Set<PListCell> pListCells = mvRefreshParams.getListValues();
            PCellSortedSet mvPartitions = mv.getListPartitionItems();
            for (PCellWithName e : mvPartitions.getPartitions()) {
                PListCell mvListCell = (PListCell) e.cell();
                if (mvListCell.getItemSize() == 1) {
                    // if list value is a single value, check it directly
                    if (pListCells.contains(e.cell()))  {
                        mvListPartitionMap.add(e.name(), e.cell());
                    }
                } else {
                    // if list values is multi values, split it into single values and check it then.
                    if (mvListCell.toSingleValueCells().stream().anyMatch(i -> pListCells.contains(i))) {
                        mvListPartitionMap.add(e.name(), e.cell());
                    }
                }
            }
        } else {
            mvListPartitionMap = mv.getPartitionCells(Optional.empty());
        }
        // filter all valid partitions by partition_retention_condition
        filterPartitionsByTTL(mvListPartitionMap, false);
        return mvListPartitionMap;
    }

    @Override
    public void filterPartitionByRefreshNumber(PCellSortedSet toRefreshPartitions,
                                               MaterializedView.PartitionRefreshStrategy refreshStrategy) {
        // filter by partition ttl
        filterPartitionsByTTL(toRefreshPartitions, false);
        if (toRefreshPartitions == null || toRefreshPartitions.isEmpty()) {
            return;
        }
        // dynamically get the number of partitions to be refreshed this time
        int partitionRefreshNumber = getPartitionRefreshNumberAdaptive(toRefreshPartitions, refreshStrategy);
        if (partitionRefreshNumber <= 0 || partitionRefreshNumber >= toRefreshPartitions.size()) {
            return;
        }

        // filter invalid cells from input
        PCellSortedSet mvPartitions = mv.getListPartitionItems();
        Set<PCellWithName> invalidCells = toRefreshPartitions.stream()
                .filter(cell -> !mvPartitions.containsName(cell.name()))
                .collect(Collectors.toSet());
        invalidCells.forEach(cell -> {
            logger.warn("Materialized view [{}] to refresh partition name {}, value {} is invalid, remove it",
                    mv.getName(), cell.name(), cell.cell());
            toRefreshPartitions.remove(cell);
        });

        int i = 0;
        // refresh the recent partitions first
        Iterator<PCellWithName> iterator = getToRefreshPartitionsIterator(toRefreshPartitions, 
                Config.materialized_view_refresh_ascending);
        while (i < partitionRefreshNumber && iterator.hasNext()) {
            PCellWithName pCell = iterator.next();
            // remove potential mv partitions from to-refresh partitions since they are added only for being affected.
            if (mvToRefreshPotentialPartitions.containsName(pCell.name())) {
                continue;
            }
            i++;
            logger.debug("Materialized view [{}] to refresh partition name {}, value {}",
                    mv.getName(), pCell.name(), pCell.cell());
        }

        // get next partition values for next refresh
        Set<PListCell> nextPartitionValues = Sets.newHashSet();
        while (iterator.hasNext()) {
            PCellWithName pCell = iterator.next();
            // remove potential mv partitions from to-refresh partitions since they are added only for being affected.
            if (mvToRefreshPotentialPartitions.containsName(pCell.name())) {
                continue;
            }
            nextPartitionValues.add((PListCell) pCell.cell());
            iterator.remove();
        }
        logger.info("Filter partitions by refresh number, ttl_number:{}, result:{}, remains:{}",
                partitionRefreshNumber, toRefreshPartitions, nextPartitionValues);
        if (CollectionUtils.isEmpty(nextPartitionValues)) {
            return;
        }
        if (!mvRefreshParams.isTentative()) {
            // partitionNameIter has just been traversed, and endPartitionName is not updated
            // will cause endPartitionName == null
            mvContext.setNextPartitionValues(PListCell.batchSerialize(nextPartitionValues));
        }
    }

    private void addListPartitions(Database database, MaterializedView materializedView,
                                   PCellSortedSet adds, Map<String, String> partitionProperties,
                                   DistributionDesc distributionDesc) {
        if (adds == null || adds.isEmpty()) {
            return;
        }

        // support to add partitions by batch
        List<PartitionDesc> partitionDescs = Lists.newArrayList();
        for (PCellWithName addEntry : adds.getPartitions()) {
            String mvPartitionName = addEntry.name();
            PListCell partitionCell = (PListCell) addEntry.cell();
            List<List<String>> partitionItems = partitionCell.getPartitionItems();
            // the order is not guaranteed
            MultiItemListPartitionDesc multiItemListPartitionDesc =
                    new MultiItemListPartitionDesc(false, mvPartitionName, partitionItems, partitionProperties);
            partitionDescs.add(multiItemListPartitionDesc);
        }

        for (List<PartitionDesc> batch : ListUtils.partition(partitionDescs, CREATE_PARTITION_BATCH_SIZE)) {
            ListPartitionDesc listPartitionDesc = new ListPartitionDesc(mv.getPartitionColumnNames(), batch);
            AddPartitionClause addPartitionClause =
                    new AddPartitionClause(listPartitionDesc, distributionDesc, partitionProperties, false);
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(mv);
            analyzer.analyze(mvContext.getCtx(), addPartitionClause);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(
                        mvContext.getCtx(), database, mv.getName(), addPartitionClause);
            } catch (Exception e) {
                throw new DmlException("Materialized view %s.%s refresh failed: " +
                        "failed to add list partition, db: %s, cause: %s",
                        e, database.getFullName(), mv.getName(), database.getFullName(), e.getMessage());
            }
            Uninterruptibles.sleepUninterruptibly(Config.mv_create_partition_batch_interval_ms, TimeUnit.MILLISECONDS);
        }
    }
}
