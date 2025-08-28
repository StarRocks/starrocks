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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableWithPartitions;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.common.PartitionDiffResult;
import com.starrocks.sql.common.RangePartitionDiffer;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.SyncPartitionUtils.createRange;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getStr2DateExpr;

public final class MVPCTRefreshRangePartitioner extends MVPCTRefreshPartitioner {
    private final Logger logger;

    private final RangePartitionDiffer differ;
    public MVPCTRefreshRangePartitioner(MvTaskRunContext mvContext,
                                        TaskRunContext context,
                                        Database db,
                                        MaterializedView mv) {
        super(mvContext, context, db, mv);
        this.differ = new RangePartitionDiffer(mv, false, null);
        this.logger = MVTraceUtils.getLogger(mv, MVPCTRefreshRangePartitioner.class);
    }

    @Override
    public boolean syncAddOrDropPartitions() throws AnalysisException {
        String start = context == null ? null : context.getProperties().get(TaskRun.PARTITION_START);
        String end = context == null ? null : context.getProperties().get(TaskRun.PARTITION_END);
        Optional<Column> partitionColumnOpt = mv.getRangePartitionFirstColumn();
        Preconditions.checkState(partitionColumnOpt.isPresent());
        Column partitionColumn = partitionColumnOpt.get();
        Range<PartitionKey> rangeToInclude = SyncPartitionUtils.createRange(start, end, partitionColumn);
        PartitionDiffResult result = differ.computePartitionDiff(rangeToInclude);
        if (result == null) {
            logger.warn("compute range partition diff failed: mv: {}", mv.getName());
            return false;
        }

        // Delete old partitions and then add new partitions because the old and new partitions may overlap
        Map<String, PCell> deletes = result.diff.getDeletes();
        for (String mvPartitionName : deletes.keySet()) {
            dropPartition(db, mv, mvPartitionName);
        }
        logger.info("The process of synchronizing materialized view delete partitions range [{}]", deletes);

        // Create new added materialized views' ranges
        Map<String, PCell> adds = result.diff.getAdds();
        Map<String, PCell> mvPartitionToCells = result.mvPartitionToCells;
        // filter partition ttl for all add ranges
        filterPartitionsByTTL(adds, true);
        Map<String, String> partitionProperties = MvUtils.getPartitionProperties(mv);
        DistributionDesc distributionDesc = MvUtils.getDistributionDesc(mv);
        addRangePartitions(db, mv, adds, partitionProperties, distributionDesc);
        adds.entrySet().stream().forEach(entry -> mvPartitionToCells.put(entry.getKey(), entry.getValue()));
        logger.info("The process of synchronizing materialized view [{}] add partitions range [{}]",
                mv.getName(), adds);

        // used to get partitions to refresh
        Map<Table, Map<String, Set<String>>> baseToMvNameRef =
                differ.generateBaseRefMap(result.refBaseTablePartitionMap, mvPartitionToCells);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef =
                differ.generateMvRefMap(mvPartitionToCells, result.refBaseTablePartitionMap);

        mvContext.setMVToCellMap(mvPartitionToCells);
        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableToCellMap(result.refBaseTablePartitionMap);
        mvContext.setExternalRefBaseTableMVPartitionMap(result.getRefBaseTableMVPartitionMap());
        return true;
    }

    @Override
    public Expr generatePartitionPredicate(Table table, Set<String> refBaseTablePartitionNames,
                                           List<Expr> mvPartitionSlotRefs) throws AnalysisException {
        List<Range<PartitionKey>> sourceTablePartitionRange = Lists.newArrayList();
        Map<Table, Map<String, PCell>> refBaseTablePartitionCells = mvContext.getRefBaseTableToCellMap();
        for (String partitionName : refBaseTablePartitionNames) {
            Preconditions.checkArgument(refBaseTablePartitionCells.containsKey(table));
            PRangeCell rangeCell = (PRangeCell) refBaseTablePartitionCells.get(table).get(partitionName);
            sourceTablePartitionRange.add(rangeCell.getRange());
        }
        sourceTablePartitionRange = MvUtils.mergeRanges(sourceTablePartitionRange);
        // for nested mv, the base table may be another mv, which is partition by str2date(dt, '%Y%m%d')
        // here we should convert date into '%Y%m%d' format
        Map<Table, List<Column>> partitionTableAndColumn = mv.getRefBaseTablePartitionColumns();
        if (!partitionTableAndColumn.containsKey(table)) {
            logger.warn("Cannot generate mv refresh partition predicate because cannot decide the partition column of table {}," +
                    "partitionTableAndColumn:{}", table.getName(), partitionTableAndColumn);
            return null;
        }
        List<Column> refPartitionColumns = partitionTableAndColumn.get(table);
        Preconditions.checkState(refPartitionColumns.size() == 1);
        Optional<Expr> partitionExprOpt = mv.getRangePartitionFirstExpr();
        if (partitionExprOpt.isEmpty()) {
            return null;
        }
        Expr partitionExpr = partitionExprOpt.get();
        boolean isConvertToDate = PartitionUtil.isConvertToDate(partitionExpr, refPartitionColumns.get(0));
        if (isConvertToDate && partitionExpr instanceof FunctionCallExpr
                && !sourceTablePartitionRange.isEmpty() && MvUtils.isDateRange(sourceTablePartitionRange.get(0))) {
            Optional<FunctionCallExpr> functionCallExprOpt = getStr2DateExpr(partitionExpr);
            if (!functionCallExprOpt.isPresent()) {
                logger.warn("Invalid partition expr:{}", partitionExpr);
                return null;
            }
            FunctionCallExpr functionCallExpr = functionCallExprOpt.get();
            Preconditions.checkState(
                    functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE));
            String dateFormat = ((StringLiteral) functionCallExpr.getChild(1)).getStringValue();
            List<Range<PartitionKey>> converted = Lists.newArrayList();
            for (Range<PartitionKey> range : sourceTablePartitionRange) {
                Range<PartitionKey> varcharPartitionKey = MvUtils.convertToVarcharRange(range, dateFormat);
                converted.add(varcharPartitionKey);
            }
            sourceTablePartitionRange = converted;
        }
        if (mvPartitionSlotRefs.size() != 1) {
            logger.warn("Cannot generate mv refresh partition predicate because mvPartitionSlotRefs size is not 1, " +
                    "mvPartitionSlotRefs:{}", mvPartitionSlotRefs);
            return null;
        }
        Expr mvPartitionSlotRef = mvPartitionSlotRefs.get(0);
        List<Expr> partitionPredicates =
                MvUtils.convertRange(mvPartitionSlotRef, sourceTablePartitionRange);
        // range contains the min value could be null value
        Optional<Range<PartitionKey>> nullRange = sourceTablePartitionRange.stream().
                filter(range -> range.lowerEndpoint().isMinValue()).findAny();
        if (nullRange.isPresent()) {
            Expr isNullPredicate = new IsNullPredicate(mvPartitionSlotRef, false);
            partitionPredicates.add(isNullPredicate);
        }

        return Expr.compoundOr(partitionPredicates);
    }

    @Override
    public Expr generateMVPartitionPredicate(TableName tableName,
                                             Set<String> mvPartitionNames) throws AnalysisException {
        if (mvPartitionNames.isEmpty()) {
            return new BoolLiteral(true);
        }
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            logger.warn("Cannot generate mv refresh partition predicate because mvPartitionExpr is invalid");
            return null;
        }
        ExpressionRangePartitionInfo rangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        List<Expr> mvPartitionExprs = rangePartitionInfo.getPartitionExprs(mv.getIdToColumn());
        if (mvPartitionExprs.size() != 1) {
            logger.warn("Cannot generate mv refresh partition predicate because mvPartitionExpr's size is not 1");
            return null;
        }
        Expr partitionExpr = mvPartitionExprs.get(0);

        List<Range<PartitionKey>> mvPartitionRange = Lists.newArrayList();
        Map<String, PCell> mvToCellMap = mvContext.getMVToCellMap();
        for (String partitionName : mvPartitionNames) {
            Preconditions.checkArgument(mvToCellMap.containsKey(partitionName));
            PRangeCell rangeCell = (PRangeCell) mvToCellMap.get(partitionName);
            mvPartitionRange.add(rangeCell.getRange());
        }
        mvPartitionRange = MvUtils.mergeRanges(mvPartitionRange);

        List<Expr> partitionPredicates =
                MvUtils.convertRange(partitionExpr, mvPartitionRange);
        // range contains the min value could be null value
        Optional<Range<PartitionKey>> nullRange = mvPartitionRange.stream().
                filter(range -> range.lowerEndpoint().isMinValue()).findAny();
        if (nullRange.isPresent()) {
            Expr isNullPredicate = new IsNullPredicate(partitionExpr, false);
            partitionPredicates.add(isNullPredicate);
        }
        return Expr.compoundOr(partitionPredicates);
    }

    @Override
    public PCellSortedSet getMVPartitionsToRefresh(PartitionInfo mvPartitionInfo,
                                                   Map<Long, BaseTableSnapshotInfo> snapshotBaseTables,
                                                   MVRefreshParams mvRefreshParams,
                                                   Set<String> mvPotentialPartitionNames) throws AnalysisException {
        // range partitioned materialized views
        boolean isAutoRefresh = mvContext.getTaskType().isAutoRefresh();
        int partitionTTLNumber = mvContext.getPartitionTTLNumber();
        boolean isRefreshMvBaseOnNonRefTables = needsRefreshBasedOnNonRefTables(snapshotBaseTables);
        String start = mvRefreshParams.getRangeStart();
        String end = mvRefreshParams.getRangeEnd();
        boolean force = mvRefreshParams.isForce();
        PCellSortedSet mvRangePartitionNames = getMVPartitionNamesWithTTL(mv, mvRefreshParams, isAutoRefresh);
        logger.info("Get partition names by range with partition limit, mv name: {}, start: {}, end: {}, force:{}, " +
                        "partitionTTLNumber: {}, isAutoRefresh: {}, mvRangePartitionNames: {}, isRefreshMvBaseOnNonRefTables:{}",
                mv.getName(), start, end, force, partitionTTLNumber, isAutoRefresh, mvRangePartitionNames,
                isRefreshMvBaseOnNonRefTables);
        PCellSortedSet toRefreshPartitions = getMVPartitionsToRefreshImpl(mvRefreshParams,
                mvPotentialPartitionNames, mvRangePartitionNames, isRefreshMvBaseOnNonRefTables, isAutoRefresh, force);
        if (toRefreshPartitions.isEmpty()) {
            return PCellSortedSet.of();
        }

        // filter partitions by refresh partition limit
        int refreshPartitionLimit = getRefreshPartitionLimit();
        if (mvRefreshParams.isCompleteRefresh() &&
                refreshPartitionLimit > 0 && toRefreshPartitions.size() > refreshPartitionLimit) {
            // remove the oldest partitions
            int toRemoveNum = toRefreshPartitions.size() - refreshPartitionLimit;
            return toRefreshPartitions.skip(toRemoveNum);
        }
        return toRefreshPartitions;
    }

    public PCellSortedSet getMVPartitionsToRefreshImpl(MVRefreshParams mvRefreshParams,
                                                       Set<String> mvPotentialPartitionNames,
                                                       PCellSortedSet mvPCellWithNames,
                                                       boolean isRefreshMvBaseOnNonRefTables,
                                                       boolean isAutoRefresh,
                                                       boolean force) {
        // check non-ref base tables or force refresh
        if (force || isRefreshMvBaseOnNonRefTables) {
            if (mvRefreshParams.isCompleteRefresh()) {
                // if non-partition table changed, should refresh all partitions of materialized view
                return mvPCellWithNames;
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
                    return mvPCellWithNames;
                } else if (force) {
                    // should refresh all related partitions if user want to do force refresh
                    return mvPCellWithNames;
                } else {
                    // If the user specifies the start and end ranges, and the non-partitioned table still changes,
                    // it should be refreshed according to the user-specified range, not all partitions.
                    return getMvPartitionNamesToRefresh(mvPCellWithNames);
                }
            }
        }

        // check the related partition table
        PCellSortedSet result = getMvPartitionNamesToRefresh(mvPCellWithNames);
        if (result.isEmpty()) {
            logger.info("No need to refresh materialized view partitions");
            return result;
        }

        Map<Table, Set<String>> baseChangedPartitionNames = getBasePartitionNamesByMVPartitionNames(result);
        if (baseChangedPartitionNames.isEmpty()) {
            logger.info("Cannot get associated base table change partitions from mv's refresh partitions {}",
                    result);
            return result;
        }

        List<TableWithPartitions> baseTableWithPartitions = baseChangedPartitionNames.keySet().stream()
                .map(x -> new TableWithPartitions(x, baseChangedPartitionNames.get(x)))
                .collect(Collectors.toList());
        Map<Table, Map<String, PCell>> refBaseTableRangePartitionMap =
                mvContext.getRefBaseTableToCellMap();
        Map<String, PCell> mvRangePartitionMap = mvContext.getMVToCellMap();
        Set<String> mvToRefreshPartitionNames = result.getPartitionNames();
        if (mv.isCalcPotentialRefreshPartition(baseTableWithPartitions,
                refBaseTableRangePartitionMap, mvToRefreshPartitionNames, mvRangePartitionMap)) {
            // because the relation of partitions between materialized view and base partition table is n : m,
            // should calculate the candidate partitions recursively.
            logger.info("Start calcPotentialRefreshPartition, needRefreshMvPartitionNames: {}," +
                    " baseChangedPartitionNames: {}", result, baseChangedPartitionNames);
            Set<String> potentialMvToRefreshPartitionNames = Sets.newHashSet(mvToRefreshPartitionNames);
            SyncPartitionUtils.calcPotentialRefreshPartition(potentialMvToRefreshPartitionNames,
                    baseChangedPartitionNames,
                    mvContext.getRefBaseTableMVIntersectedPartitions(),
                    mvContext.getMvRefBaseTableIntersectedPartitions(),
                    mvPotentialPartitionNames);
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

    @Override
    public PCellSortedSet getMVPartitionsToRefreshWithForce() throws AnalysisException {
        int partitionTTLNumber = mvContext.getPartitionTTLNumber();
        PCellSortedSet inputRanges = getValidRangePartitionMap(partitionTTLNumber);
        filterPartitionsByTTL(inputRanges, false);
        return inputRanges;
    }

    @Override
    public PCellSortedSet getMVPartitionNamesWithTTL(MaterializedView mv, MVRefreshParams mvRefreshParams,
                                                     boolean isAutoRefresh) throws AnalysisException {
        PCellSortedSet mvPartitionCells = PCellSortedSet.of();
        int partitionTTLNumber = getPartitionTTLLimit();
        boolean hasPartitionRange = !mvRefreshParams.isCompleteRefresh();
        if (hasPartitionRange) {
            String start = mvRefreshParams.getRangeStart();
            String end = mvRefreshParams.getRangeEnd();
            Column partitionColumn = mv.getRangePartitionFirstColumn().get();
            Range<PartitionKey> rangeToInclude = createRange(start, end, partitionColumn);
            PCellSortedSet sortedPCells = getValidRangePartitionMap(partitionTTLNumber);
            for (PCellWithName pCellWrapper : sortedPCells.partitions()) {
                PRangeCell pRangeCell = (PRangeCell) pCellWrapper.cell();
                Range<PartitionKey> rangeToCheck = pRangeCell.getRange();
                int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
                int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
                if (!(lowerCmp >= 0 || upperCmp <= 0)) {
                    mvPartitionCells.add(pCellWrapper);
                }
            }
        } else {
            mvPartitionCells = getValidRangePartitionMap(partitionTTLNumber);
        }
        // filter by partition ttl conditions
        filterPartitionsByTTL(mvPartitionCells, false);
        return mvPartitionCells;
    }

    /**
     * Get the sorted valid range partition map, which includes the last N partitions.
     * NOTE: This can be only used for range partitioned table which its partition value can be sorted.
     * NOTE: The result is sorted by the lower endpoint of the range.
     */
    private PCellSortedSet getValidRangePartitionMap(int lastPartitionNum) throws AnalysisException {
        Map<String, Range<PartitionKey>> rangePartitionMap = mv.getRangePartitionMap();
        if (rangePartitionMap.isEmpty()) {
            return PCellSortedSet.of();
        }
        List<PCellWithName> pCellWithNames = rangePartitionMap
                .entrySet()
                .stream()
                .map(e -> PCellWithName.of(e.getKey(), new PRangeCell(e.getValue())))
                .collect(Collectors.toList());
        // ensure the partition cells are sorted by the range
        PCellSortedSet pCells = PCellSortedSet.of(pCellWithNames);
        // less than 0 means not set
        if (lastPartitionNum < 0) {
            return pCells;
        }
        int partitionNum = rangePartitionMap.size();
        if (lastPartitionNum > partitionNum) {
            return pCells;
        }
        return pCells.limit(lastPartitionNum);
    }

    protected void filterPartitionsByTTL(PCellSortedSet toRefreshPartitions,
                                         boolean isMockPartitionIds) {
        super.filterPartitionsByTTL(toRefreshPartitions, isMockPartitionIds);

        // filter partitions by partition_ttl_number
        int partitionTTLNumber = mv.getTableProperty().getPartitionTTLNumber();
        int toRefreshPartitionNum = toRefreshPartitions.size();
        if (partitionTTLNumber > 0 && toRefreshPartitionNum > partitionTTLNumber) {
            // remove the oldest partitions
            int toRemoveNum = toRefreshPartitionNum - partitionTTLNumber;
            toRefreshPartitions.removeWithSize(toRemoveNum);
        }
    }

    @Override
    public void filterPartitionByRefreshNumber(PCellSortedSet mvPartitionsToRefresh,
                                               Set<String> mvPotentialPartitionNames,
                                               boolean tentative) {
        filterPartitionByRefreshNumberInternal(mvPartitionsToRefresh, mvPotentialPartitionNames, tentative,
                MaterializedView.PartitionRefreshStrategy.STRICT);
    }

    @Override
    public void filterPartitionByAdaptiveRefreshNumber(PCellSortedSet mvPartitionsToRefresh,
                                                       Set<String> mvPotentialPartitionNames,
                                                       boolean tentative) {
        filterPartitionByRefreshNumberInternal(mvPartitionsToRefresh, mvPotentialPartitionNames, tentative,
                MaterializedView.PartitionRefreshStrategy.ADAPTIVE);
    }

    public void filterPartitionByRefreshNumberInternal(PCellSortedSet mvPartitionsToRefresh,
                                                       Set<String> mvPotentialPartitionNames,
                                                       boolean tentative,
                                                       MaterializedView.PartitionRefreshStrategy refreshStrategy) {
        int partitionRefreshNumber = mv.getTableProperty().getPartitionRefreshNumber();
        Map<String, Range<PartitionKey>> mvRangePartitionMap = mv.getRangePartitionMap();
        if (partitionRefreshNumber <= 0 || partitionRefreshNumber >= mvRangePartitionMap.size()) {
            return;
        }

        Iterator<PCellWithName> inputIter = mvPartitionsToRefresh.iterator();
        while (inputIter.hasNext()) {
            PCellWithName pCellWithName = inputIter.next();
            String mvPartitionName = pCellWithName.name();
            // skip if partition is not in the mv's partition range map
            if (!mvRangePartitionMap.containsKey(mvPartitionName)) {
                logger.warn("Partition {} is not in the materialized view's partition range map, " +
                        "remove it from refresh list", mvPartitionName);
                mvPartitionsToRefresh.remove(pCellWithName);
            }
        }

        LinkedList<PCellWithName> sortedPartition = mvPartitionsToRefresh
                .stream()
                .collect(Collectors.toCollection(LinkedList::new));
        Iterator<PCellWithName> sortedIterator = Config.materialized_view_refresh_ascending
                ? sortedPartition.iterator() : sortedPartition.descendingIterator();
        // dynamically get the number of partitions to be refreshed this time
        partitionRefreshNumber = getRefreshNumberByMode(sortedIterator, refreshStrategy);

        PCellWithName tmpPCellWithName = null;
        for (int i = 0; i < partitionRefreshNumber; i++) {
            if (sortedIterator.hasNext()) {
                tmpPCellWithName = sortedIterator.next();
                sortedIterator.remove();
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
            if (!mvPotentialPartitionNames.isEmpty() && mvPotentialPartitionNames.contains(tmpPCellWithName)) {
                return;
            }
        }
        if (!Config.materialized_view_refresh_ascending) {
            sortedIterator = sortedPartition.iterator();
        }
        setNextPartitionStartAndEnd(mvPartitionsToRefresh, sortedIterator, tentative);
    }

    public int getAdaptivePartitionRefreshNumber(
            Iterator<PCellWithName> iterator) throws MVAdaptiveRefreshException {
        Map<String, Map<Table, Set<String>>> mvToBaseNameRefs = mvContext.getMvRefBaseTableIntersectedPartitions();
        MVRefreshPartitionSelector mvRefreshPartitionSelector =
                new MVRefreshPartitionSelector(Config.mv_max_rows_per_refresh, Config.mv_max_bytes_per_refresh,
                        Config.mv_max_partitions_num_per_refresh, mvContext.getExternalRefBaseTableMVPartitionMap());

        int adaptiveRefreshNumber = 0;
        while (iterator.hasNext()) {
            PCellWithName pCellWithName = iterator.next();
            String mvRefreshPartition = pCellWithName.name();
            Map<Table, Set<String>> refBaseTablesPartitions = mvToBaseNameRefs.get(mvRefreshPartition);
            if (mvRefreshPartitionSelector.canAddPartition(refBaseTablesPartitions)) {
                mvRefreshPartitionSelector.addPartition(refBaseTablesPartitions);
                iterator.remove();
                adaptiveRefreshNumber++;
            } else {
                break;
            }
        }
        return adaptiveRefreshNumber;
    }

    private void setNextPartitionStartAndEnd(PCellSortedSet partitionsToRefresh,
                                             Iterator<PCellWithName> iterator,
                                             boolean tentative) {
        String nextPartitionStart = null;
        PCellWithName end = null;
        if (iterator.hasNext()) {
            PCellWithName start = iterator.next();
            PRangeCell pRangeCell = (PRangeCell) start.cell();
            Range<PartitionKey> partitionKeyRange = pRangeCell.getRange();
            nextPartitionStart = AnalyzerUtils.parseLiteralExprToDateString(partitionKeyRange.lowerEndpoint(), 0);
            end = start;
            partitionsToRefresh.remove(end);
        }
        while (iterator.hasNext()) {
            end = iterator.next();
            iterator.remove();
            partitionsToRefresh.remove(end);
        }

        if (!tentative) {
            mvContext.setNextPartitionStart(nextPartitionStart);

            if (end != null) {
                PRangeCell pRangeCell = (PRangeCell) end.cell();
                PartitionKey upperEndpoint = pRangeCell.getRange().upperEndpoint();
                mvContext.setNextPartitionEnd(AnalyzerUtils.parseLiteralExprToDateString(upperEndpoint, 0));
            } else {
                // partitionNameIter has just been traversed, and endPartitionName is not updated
                // will cause endPartitionName == null
                mvContext.setNextPartitionEnd(null);
            }
        }
    }

    private void addRangePartitions(Database database, MaterializedView materializedView,
                                    Map<String, PCell> adds,
                                    Map<String, String> partitionProperties,
                                    DistributionDesc distributionDesc) {
        if (adds.isEmpty()) {
            return;
        }
        List<PartitionDesc> partitionDescs = Lists.newArrayList();

        for (Map.Entry<String, PCell> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            Preconditions.checkArgument(addEntry.getValue() instanceof PRangeCell);
            Range<PartitionKey> partitionKeyRange = ((PRangeCell) addEntry.getValue()).getRange();
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
                    new RangePartitionDesc(mv.getPartitionColumnNames(), batch);
            AddPartitionClause alterPartition = new AddPartitionClause(rangePartitionDesc, distributionDesc,
                    partitionProperties, false);
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(mv);
            analyzer.analyze(mvContext.getCtx(), alterPartition);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().addPartitions(mvContext.getCtx(),
                        database, mv.getName(), alterPartition);
            } catch (Exception e) {
                throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                        database.getFullName(), mv.getName());
            }
            Uninterruptibles.sleepUninterruptibly(Config.mv_create_partition_batch_interval_ms, TimeUnit.MILLISECONDS);
        }
    }
}
