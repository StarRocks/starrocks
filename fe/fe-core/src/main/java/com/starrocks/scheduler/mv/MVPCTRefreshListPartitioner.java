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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PartitionDiff;
import com.starrocks.sql.common.PartitionDiffResult;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergPartitionUtils.getIcebergTablePartitionPredicateExpr;

public final class MVPCTRefreshListPartitioner extends MVPCTRefreshPartitioner {
    private final ListPartitionDiffer differ;
    private final Logger logger;

    public MVPCTRefreshListPartitioner(MvTaskRunContext mvContext,
                                       TaskRunContext context,
                                       Database db,
                                       MaterializedView mv) {
        super(mvContext, context, db, mv);
        this.differ = new ListPartitionDiffer(mv, false);
        this.logger = MVTraceUtils.getLogger(mv, MVPCTRefreshListPartitioner.class);
    }

    @Override
    public boolean syncAddOrDropPartitions() throws LockTimeoutException {
        // collect mv partition items with lock
        Locker locker = new Locker();
        if (!locker.tryLockDatabase(db.getId(), LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException("Failed to lock database: " + db.getFullName() + " in syncPartitionsForList");
        }

        PartitionDiffResult result;
        try {
            result = differ.computePartitionDiff(null);
            if (result == null) {
                logger.warn("compute list partition diff failed, result is null");
                return false;
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        // drop old partitions and add new partitions
        final PartitionDiff partitionDiff = result.diff;
        // We should delete the old partition first and then add the new one,
        // because the old and new partitions may overlap
        final Map<String, PCell> deletes = partitionDiff.getDeletes();
        for (String mvPartitionName : deletes.keySet()) {
            dropPartition(db, mv, mvPartitionName);
        }
        logger.info("The process of synchronizing materialized view [{}] delete partitions list [{}]",
                mv.getName(), deletes);

        // add partitions
        final Map<String, String> partitionProperties = MvUtils.getPartitionProperties(mv);
        final DistributionDesc distributionDesc = MvUtils.getDistributionDesc(mv);
        final Map<String, PCell> adds = partitionDiff.getAdds();
        // filter by partition ttl
        filterPartitionsByTTL(adds, true);
        // add partitions for mv
        addListPartitions(db, mv, adds, partitionProperties, distributionDesc);
        logger.info("The process of synchronizing materialized view [{}] add partitions list [{}]",
                mv.getName(), adds);

        // add into mv context
        result.mvPartitionToCells.putAll(adds);
        final Map<Table, Map<String, PCell>> refBaseTablePartitionMap = result.refBaseTablePartitionMap;
        // base table -> Map<partition name -> mv partition names>
        final Map<Table, Map<String, Set<String>>> baseToMvNameRef =
                differ.generateBaseRefMap(refBaseTablePartitionMap, result.mvPartitionToCells);
        // mv partition name -> Map<base table -> base partition names>
        final Map<String, Map<Table, Set<String>>> mvToBaseNameRef =
                differ.generateMvRefMap(result.mvPartitionToCells, refBaseTablePartitionMap);
        mvContext.setMVToCellMap(result.mvPartitionToCells);
        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableToCellMap(refBaseTablePartitionMap);
        mvContext.setExternalRefBaseTableMVPartitionMap(result.getRefBaseTableMVPartitionMap());
        return true;
    }

    @Override
    public Expr generatePartitionPredicate(Table refBaseTable, Set<String> refBaseTablePartitionNames,
                                           List<Expr> mvPartitionSlotRefs) throws AnalysisException {
        Map<Table, Map<String, PCell>> basePartitionMaps = mvContext.getRefBaseTableToCellMap();
        if (basePartitionMaps.isEmpty()) {
            return null;
        }
        Map<String, PCell> baseListPartitionMap = basePartitionMaps.get(refBaseTable);
        if (baseListPartitionMap == null) {
            logger.warn("Generate incremental partition predicate failed, " +
                    "basePartitionMaps:{} contains no refBaseTable:{}", basePartitionMaps, refBaseTable);
            return null;
        }
        if (baseListPartitionMap.isEmpty()) {
            return new BoolLiteral(true);
        }

        // base table's partition columns, not mv's partition columns
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        if (refBaseTablePartitionColumns == null || !refBaseTablePartitionColumns.containsKey(refBaseTable)) {
            logger.warn("Generate incremental partition failed, partitionTableAndColumn {} contains no ref table {}",
                    refBaseTablePartitionColumns, refBaseTable);
            return null;
        }
        // base table's partition columns that are referred by mv
        List<Column> refPartitionColumns = refBaseTablePartitionColumns.get(refBaseTable);
        if (refPartitionColumns.size() != mvPartitionSlotRefs.size()) {
            logger.warn("Generate incremental partition failed, refPartitionColumns size {} != mvPartitionSlotRefs size {}",
                    refPartitionColumns.size(), mvPartitionSlotRefs.size());
            return null;
        }
        return genPartitionPredicate(refBaseTable, refBaseTablePartitionNames, mvPartitionSlotRefs,
                refPartitionColumns, baseListPartitionMap);
    }

    private static Expr getRefBaseTablePartitionPredicateExpr(Table table,
                                                              String partitionColumn,
                                                              SlotRef slotRef,
                                                              List<Expr> selectedPartitionValues) {
        if (!(table instanceof IcebergTable)) {
            return MvUtils.convertToInPredicate(slotRef, selectedPartitionValues);
        } else {
            IcebergTable icebergTable = (IcebergTable) table;
            return getIcebergTablePartitionPredicateExpr(icebergTable, partitionColumn, slotRef, selectedPartitionValues);
        }
    }

    private static @Nullable Expr genPartitionPredicate(
            Table refBaseTable,
            Set<String> refBaseTablePartitionNames,
            List<Expr> refBaseTablePartitionSlotRefs,
            List<Column> refPartitionColumns,
            Map<String, PCell> baseListPartitionMap) throws AnalysisException {
        Preconditions.checkArgument(refBaseTablePartitionSlotRefs.size() == refPartitionColumns.size());
        if (refPartitionColumns.size() == 1) {
            boolean isContainsNullPartition = false;
            Column refPartitionColumn = refPartitionColumns.get(0);
            List<Expr> selectedPartitionValues = Lists.newArrayList();
            Type partitionType = refPartitionColumn.getType();
            for (String tablePartitionName : refBaseTablePartitionNames) {
                PListCell cell = (PListCell) baseListPartitionMap.get(tablePartitionName);
                for (List<String> values : cell.getPartitionItems()) {
                    if (refPartitionColumns.size() != values.size()) {
                        return null;
                    }
                    LiteralExpr partitionValue = new PartitionValue(values.get(0)).getValue(partitionType);
                    if (partitionValue.isConstantNull()) {
                        isContainsNullPartition = true;
                        continue;
                    }
                    selectedPartitionValues.add(partitionValue);
                }
            }
            SlotRef refBaseTablePartitionSlotRef = (SlotRef) refBaseTablePartitionSlotRefs.get(0);
            Expr inPredicate = getRefBaseTablePartitionPredicateExpr(refBaseTable, refPartitionColumn.getName(),
                    refBaseTablePartitionSlotRef, selectedPartitionValues);
            // NOTE: If target partition values contain `null partition`, the generated predicate should
            // contain `is null` predicate rather than `in (null) or = null` because the later one is not correct.
            if (isContainsNullPartition) {
                IsNullPredicate isNullPredicate = new IsNullPredicate(refBaseTablePartitionSlotRef, false);
                return Expr.compoundOr(Lists.newArrayList(inPredicate, isNullPredicate));
            } else {
                return inPredicate;
            }
        } else {
            List<Expr> partitionPredicates = Lists.newArrayList();
            for (String tablePartitionName : refBaseTablePartitionNames) {
                PListCell cell = (PListCell) baseListPartitionMap.get(tablePartitionName);
                for (List<String> values : cell.getPartitionItems()) {
                    if (refPartitionColumns.size() != values.size()) {
                        return null;
                    }
                    List<Expr> predicates = Lists.newArrayList();
                    for (int i = 0; i < refPartitionColumns.size(); i++) {
                        Column refPartitionColumn = refPartitionColumns.get(i);
                        Type partitionType = refPartitionColumn.getType();
                        LiteralExpr partitionValue = new PartitionValue(values.get(i)).getValue(partitionType);
                        Expr refBaseTablePartitionExpr = refBaseTablePartitionSlotRefs.get(i);
                        Expr predicate;
                        if (partitionValue.isConstantNull()) {
                            // NOTE: If target partition values contain `null partition`, the generated predicate should
                            // contain `is null` predicate rather than `in (null) or = null` because the later one is not correct.
                            predicate = new IsNullPredicate(refBaseTablePartitionExpr, false);
                        } else {
                            predicate = getRefBaseTablePartitionPredicateExpr(refBaseTable, refPartitionColumn.getName(),
                                    (SlotRef) refBaseTablePartitionExpr, Lists.newArrayList(partitionValue));
                        }
                        predicates.add(predicate);
                    }
                    partitionPredicates.add(Expr.compoundAnd(predicates));
                }
            }
            return Expr.compoundOr(partitionPredicates);
        }
    }

    @Override
    public Set<String> getMVPartitionsToRefreshWithForce() {
        Map<String, PCell> mvValidListPartitionMapMap = mv.getPartitionCells(Optional.empty());
        filterPartitionsByTTL(mvValidListPartitionMapMap, false);
        return mvValidListPartitionMapMap.keySet();
    }

    @Override
    public Set<String> getMVPartitionsToRefresh(PartitionInfo mvPartitionInfo,
                                                Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                MVRefreshParams mvRefreshParams,
                                                Set<String> mvPotentialPartitionNames) {
        // list partitioned materialized view
        boolean isAutoRefresh = mvContext.getTaskType().isAutoRefresh();
        Set<String> mvListPartitionNames = getMVPartitionNamesWithTTL(mv, mvRefreshParams, isAutoRefresh);

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
            Set<String> mvToRefreshPartitionNames = getMvPartitionNamesToRefresh(mvListPartitionNames);
            Map<Table, Set<String>> baseChangedPartitionNames =
                    getBasePartitionNamesByMVPartitionNames(mvToRefreshPartitionNames);
            if (baseChangedPartitionNames.isEmpty()) {
                logger.info("Cannot get associated base table change partitions from mv's refresh partitions {}",
                        mvToRefreshPartitionNames);
                return mvToRefreshPartitionNames;
            }
            // because the relation of partitions between materialized view and base partition table is n : m,
            // should calculate the candidate partitions recursively.
            if (isCalcPotentialRefreshPartition()) {
                logger.info("Start calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                        " baseChangedPartitionNames: {}", mvToRefreshPartitionNames, baseChangedPartitionNames);
                SyncPartitionUtils.calcPotentialRefreshPartition(mvToRefreshPartitionNames, baseChangedPartitionNames,
                        mvContext.getRefBaseTableMVIntersectedPartitions(),
                        mvContext.getMvRefBaseTableIntersectedPartitions(),
                        mvPotentialPartitionNames);
                logger.info("Finish calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                        " baseChangedPartitionNames: {}", mvToRefreshPartitionNames, baseChangedPartitionNames);
            }
            return mvToRefreshPartitionNames;
        }
    }

    public boolean isCalcPotentialRefreshPartition() {
        // TODO: If base table's list partitions contain multi values, should calculate potential partitions.
        // Only check base table's partition values intersection with mv's to-refresh partitions later.
        Map<Table, Map<String, PCell>> refBaseTableRangePartitionMap = mvContext.getRefBaseTableToCellMap();
        return refBaseTableRangePartitionMap.entrySet()
                .stream()
                .anyMatch(e -> e.getValue().values().stream().anyMatch(l -> ((PListCell) l).getItemSize() > 1));
    }

    @Override
    public Set<String> getMVPartitionNamesWithTTL(MaterializedView mv,
                                                  MVRefreshParams mvRefreshParams,
                                                  boolean isAutoRefresh) {
        // if the user specifies the start and end ranges, only refresh the specified partitions
        boolean isCompleteRefresh = mvRefreshParams.isCompleteRefresh();
        Map<String, PCell> mvListPartitionMap = Maps.newHashMap();
        if (!isCompleteRefresh) {
            Set<PListCell> pListCells = mvRefreshParams.getListValues();
            Map<String, PListCell> mvPartitions = mv.getListPartitionItems();
            for (Map.Entry<String, PListCell> e : mvPartitions.entrySet()) {
                PListCell mvListCell = e.getValue();
                if (mvListCell.getItemSize() == 1) {
                    // if list value is a single value, check it directly
                    if (pListCells.contains(e.getValue()))  {
                        mvListPartitionMap.put(e.getKey(), e.getValue());
                    }
                } else {
                    // if list values is multi values, split it into single values and check it then.
                    if (mvListCell.toSingleValueCells().stream().anyMatch(i -> pListCells.contains(i))) {
                        mvListPartitionMap.put(e.getKey(), e.getValue());
                    }
                }
            }
        } else {
            mvListPartitionMap = mv.getPartitionCells(Optional.empty());
        }
        // filter all valid partitions by partition_retention_condition
        filterPartitionsByTTL(mvListPartitionMap, false);
        return mvListPartitionMap.keySet();
    }

    @Override
    public void filterPartitionByRefreshNumber(Set<String> mvPartitionsToRefresh,
                                               Set<String> mvPotentialPartitionNames, boolean tentative) {
        Map<String, PCell> partitionToCells = Maps.newHashMap();
        Map<String, PListCell> listPartitionMap = mv.getListPartitionItems();
        for (String partitionName : mvPartitionsToRefresh) {
            PListCell listCell = listPartitionMap.get(partitionName);
            if (listCell == null) {
                logger.warn("Partition {} is not found in materialized view {}", partitionName, mv.getName());
                continue;
            }
            partitionToCells.put(partitionName, listCell);
        }

        // filter by partition ttl
        filterPartitionsByTTL(partitionToCells, false);
        if (CollectionUtils.sizeIsEmpty(partitionToCells)) {
            return;
        }
        Map<String, PListCell> toRefreshPartitions = partitionToCells.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> (PListCell) e.getValue()));

        // filter by partition refresh number
        int filterNumber = mv.getTableProperty().getPartitionRefreshNumber();
        // TODO: Sort by List Partition's value is weird because there maybe meaningless or un-sortable,
        // users should take care of `partition_ttl_number` for list partition.
        LinkedHashMap<String, PListCell> sortedPartition = toRefreshPartitions.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue())) // reverse order(max heap)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        Iterator<Map.Entry<String, PListCell>> iter = sortedPartition.entrySet().iterator();
        // iterate partition_ttl_number times
        for (int i = 0; i < filterNumber; i++) {
            if (iter.hasNext()) {
                iter.next();
            }
        }
        // compute next partition list cells in the next task run
        Set<PListCell> nextPartitionValues = Sets.newHashSet();
        while (iter.hasNext()) {
            Map.Entry<String, PListCell> entry = iter.next();
            nextPartitionValues.add(entry.getValue());
            // remove the partition which is not reserved
            toRefreshPartitions.remove(entry.getKey());
        }
        logger.info("Filter partitions by partition_ttl_number, ttl_number:{}, result:{}, remains:{}",
                filterNumber, toRefreshPartitions, nextPartitionValues);
        // do filter input mvPartitionsToRefresh since it's a reference
        mvPartitionsToRefresh.retainAll(toRefreshPartitions.keySet());
        if (CollectionUtils.isEmpty(nextPartitionValues)) {
            return;
        }
        if (!tentative) {
            // partitionNameIter has just been traversed, and endPartitionName is not updated
            // will cause endPartitionName == null
            mvContext.setNextPartitionValues(PListCell.batchSerialize(nextPartitionValues));
        }
    }

    private void addListPartitions(Database database, MaterializedView materializedView,
                                   Map<String, PCell> adds, Map<String, String> partitionProperties,
                                   DistributionDesc distributionDesc) {
        if (adds == null || adds.isEmpty()) {
            return;
        }

        // TODO: support to add partitions by batch
        for (Map.Entry<String, PCell> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            PListCell partitionCell = (PListCell) addEntry.getValue();
            List<List<String>> partitionItems = partitionCell.getPartitionItems();
            // the order is not guaranteed
            MultiItemListPartitionDesc multiItemListPartitionDesc =
                    new MultiItemListPartitionDesc(false, mvPartitionName, partitionItems, partitionProperties);
            AddPartitionClause addPartitionClause =
                    new AddPartitionClause(multiItemListPartitionDesc, distributionDesc, partitionProperties, false);
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(mv);
            analyzer.analyze(mvContext.getCtx(), addPartitionClause);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(
                        mvContext.getCtx(), database, mv.getName(), addPartitionClause);
            } catch (Exception e) {
                throw new DmlException("add list partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                        database.getFullName(), mv.getName());
            }
        }
    }
}
