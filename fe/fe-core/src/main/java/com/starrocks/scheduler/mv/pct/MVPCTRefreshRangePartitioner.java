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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshParams;
import com.starrocks.scheduler.mv.MVTraceUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.common.PartitionDiffResult;
import com.starrocks.sql.common.RangePartitionDiffer;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
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
                                        MaterializedView mv,
                                        MVRefreshParams mvRefreshParams) {
        super(mvContext, context, db, mv, mvRefreshParams);
        this.differ = new RangePartitionDiffer(mv, false, null);
        this.logger = MVTraceUtils.getLogger(mv, MVPCTRefreshRangePartitioner.class);
    }

    public PCellSortedSet getMVPartitionsToRefreshByParams() throws AnalysisException {
        if (mvRefreshParams.isCompleteRefresh()) {
            return mv.getRangePartitionMap();
        } else {
            String start = mvRefreshParams.getRangeStart();
            String end = mvRefreshParams.getRangeEnd();
            if (StringUtils.isEmpty(start) && StringUtils.isEmpty(end)) {
                return PCellSortedSet.of();
            }
            // range partition must have partition column, and its column size must be 1
            Column partitionColumn = mv.getRangePartitionFirstColumn().get();
            Range<PartitionKey> rangeToInclude = createRange(start, end, partitionColumn);
            PCellSortedSet rangePartitionMap = mv.getRangePartitionMap();
            if (rangePartitionMap.isEmpty()) {
                return PCellSortedSet.of();
            }
            PCellSortedSet result = PCellSortedSet.of();
            for (PCellWithName entry : rangePartitionMap.getPartitions()) {
                Range<PartitionKey> rangeToCheck = ((PRangeCell) entry.cell()).getRange();
                if (RangePartitionDiffer.isRangeIncluded(rangeToInclude, rangeToCheck)) {
                    result.add(new PCellWithName(entry.name(), new PRangeCell(rangeToCheck)));
                }
            }
            return result;
        }
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
        logger.info("Materialized view [{}] partition diff result: {}",
                mv.getName(), result);

        // Delete old partitions and then add new partitions because the old and new partitions may overlap
        PCellSortedSet deletes = result.diff.getDeletes();
        for (String mvPartitionName : deletes.getPartitionNames()) {
            dropPartition(db, mv, mvPartitionName);
        }
        logger.info("The process of synchronizing materialized view delete partitions range [{}]", deletes);

        // Create new added materialized views' ranges
        PCellSortedSet adds = result.diff.getAdds();
        PCellSortedSet mvPartitionToCells = result.mvPartitionToCells;
        // filter partition ttl for all add ranges
        filterPartitionsByTTL(adds, true);
        Map<String, String> partitionProperties = MvUtils.getPartitionProperties(mv);
        DistributionDesc distributionDesc = MvUtils.getDistributionDesc(mv);
        addRangePartitions(db, mv, adds, partitionProperties, distributionDesc);
        mvPartitionToCells.addAll(adds);
        logger.info("The process of synchronizing materialized view [{}] add partitions range [{}]",
                mv.getName(), adds);

        // used to get partitions to refresh
        Map<Table, PCellSetMapping> baseToMvNameRef =
                differ.generateBaseRefMap(result.refBaseTablePartitionMap, mvPartitionToCells);
        Map<String, Map<Table, PCellSortedSet>> mvToBaseNameRef =
                differ.generateMvRefMap(mvPartitionToCells, result.refBaseTablePartitionMap);

        mvContext.setMVToCellMap(mvPartitionToCells);
        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableToCellMap(result.refBaseTablePartitionMap);
        mvContext.setExternalRefBaseTableMVPartitionMap(result.getRefBaseTableMVPartitionMap());
        return true;
    }

    @Override
    public Expr generatePartitionPredicate(Table table, PCellSortedSet refBaseTablePartitionNames,
                                           List<Expr> mvPartitionSlotRefs) throws AnalysisException {
        List<Range<PartitionKey>> sourceTablePartitionRange = Lists.newArrayList();
        Map<Table, PCellSortedSet> refBaseTablePartitionCells = mvContext.getRefBaseTableToCellMap();
        if (!refBaseTablePartitionCells.containsKey(table)) {
            throw new AnalysisException("Cannot generate mv refresh partition predicate because cannot find " +
                    "the ref base table partition cells for table:" + table.getName());
        }
        for (String partitionName : refBaseTablePartitionNames.getPartitionNames()) {
            PRangeCell rangeCell = (PRangeCell) refBaseTablePartitionCells.get(table).getPCell(partitionName);
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

        return ExprUtils.compoundOr(partitionPredicates);
    }

    @Override
    public Expr generateMVPartitionPredicate(TableName tableName,
                                             PCellSortedSet mvPartitionNames) throws AnalysisException {
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
        PCellSortedSet mvToCellMap = mvContext.getMVToCellMap();
        for (String partitionName : mvPartitionNames.getPartitionNames()) {
            Preconditions.checkArgument(mvToCellMap.containsName(partitionName));
            PRangeCell rangeCell = (PRangeCell) mvToCellMap.getPCell(partitionName);
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
        return ExprUtils.compoundOr(partitionPredicates);
    }

    @Override
    public PCellSortedSet getMVPartitionsToRefreshWithCheck(Map<Long, BaseTableSnapshotInfo> snapshotBaseTables)
            throws AnalysisException {
        // range partitioned materialized views
        boolean isAutoRefresh = isAutomaticRefresh();
        boolean isRefreshMvBaseOnNonRefTables = needsRefreshBasedOnNonRefTables(snapshotBaseTables);
        PCellSortedSet result = getMVPartitionsToRefreshImpl(isRefreshMvBaseOnNonRefTables, isAutoRefresh);
        logger.info("get mv partition to refresh, refresh params:{}, " +
                        "isAutoRefresh: {}, isRefreshMvBaseOnNonRefTables:{}, result:{}",
                mvRefreshParams, isAutoRefresh, isRefreshMvBaseOnNonRefTables, result);
        return result;
    }

    public PCellSortedSet getMVPartitionsToRefreshImpl(boolean isRefreshMvBaseOnNonRefTables,
                                                       boolean isAutoRefresh) throws AnalysisException {
        PCellSortedSet mvPCellWithNames = getMVPartitionNamesWithTTL(isAutoRefresh);
        if (mvRefreshParams.isForce() || isRefreshMvBaseOnNonRefTables) {
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
                } else if (mvRefreshParams.isForce()) {
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
        return getMvPartitionNamesToRefresh(mvPCellWithNames);
    }

    @Override
    public void filterMVToRefreshPartitionsByProperty(PCellSortedSet mvToRefreshedPartitions) {
        // filter partitions by refresh partition limit
        int refreshPartitionLimit = getRefreshPartitionLimit();
        if (mvRefreshParams.isCompleteRefresh() &&
                refreshPartitionLimit > 0 && mvToRefreshedPartitions.size() > refreshPartitionLimit) {
            // remove the oldest partitions
            int toRemoveNum = mvToRefreshedPartitions.size() - refreshPartitionLimit;
            mvToRefreshedPartitions.removeFromStart(toRemoveNum);
        }
    }

    @Override
    public boolean isCalcPotentialRefreshPartition(Map<Table, PCellSortedSet> baseChangedPartitionNames,
                                                   PCellSortedSet mvPartitions) {
        // add potential partitions to refresh into mvPartitions
        return SyncPartitionUtils.isCalcPotentialRefreshPartition(baseChangedPartitionNames, mvPartitions);
    }

    @Override
    public PCellSortedSet getMVPartitionNamesWithTTL(boolean isAutoRefresh) throws AnalysisException {
        PCellSortedSet mvPartitionCells = PCellSortedSet.of();
        int partitionTTLNumber = getPartitionTTLLimit();
        boolean hasPartitionRange = !mvRefreshParams.isCompleteRefresh();
        if (hasPartitionRange) {
            String start = mvRefreshParams.getRangeStart();
            String end = mvRefreshParams.getRangeEnd();
            Column partitionColumn = mv.getRangePartitionFirstColumn().get();
            Range<PartitionKey> rangeToInclude = createRange(start, end, partitionColumn);
            PCellSortedSet sortedPCells = getValidRangePartitionMap(partitionTTLNumber);
            for (PCellWithName pCellWrapper : sortedPCells.getPartitions()) {
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
        PCellSortedSet rangePartitionMap = mv.getRangePartitionMap();
        if (rangePartitionMap.isEmpty()) {
            return PCellSortedSet.of();
        }
        List<PCellWithName> pCellWithNames = rangePartitionMap
                .getPartitions()
                .stream()
                .map(e -> PCellWithName.of(e.name(), new PRangeCell(((PRangeCell) e.cell()).getRange())))
                .collect(Collectors.toList());
        // ensure the partition cells are sorted by the range
        PCellSortedSet pCells = PCellSortedSet.of(pCellWithNames);
        // less than 0 means not set
        if (lastPartitionNum < 0) {
            return pCells;
        }
        int partitionNum = pCells.size();
        if (lastPartitionNum > partitionNum) {
            return pCells;
        }
        // get the last N partitions
        pCells.reserveToSize(lastPartitionNum);
        return pCells;
    }

    public void filterPartitionsByTTL(PCellSortedSet toRefreshPartitions,
                                      boolean isMockPartitionIds) {
        super.filterPartitionsByTTL(toRefreshPartitions, isMockPartitionIds);

        // filter partitions by partition_ttl_number
        int partitionTTLNumber = mv.getTableProperty().getPartitionTTLNumber();
        int toRefreshPartitionNum = toRefreshPartitions.size();
        if (partitionTTLNumber > 0 && toRefreshPartitionNum > partitionTTLNumber) {
            // remove the oldest partitions
            int toRemoveNum = toRefreshPartitionNum - partitionTTLNumber;
            toRefreshPartitions.removeFromStart(toRemoveNum);
        }
    }

    @Override
    public void filterPartitionByRefreshNumber(PCellSortedSet mvPartitionsToRefresh,
                                               MaterializedView.PartitionRefreshStrategy refreshStrategy) {
        int partitionRefreshNumber = mv.getTableProperty().getPartitionRefreshNumber();
        PCellSortedSet mvRangePartitionMap = mv.getRangePartitionMap();
        if (partitionRefreshNumber <= 0 || partitionRefreshNumber >= mvRangePartitionMap.size()
                || partitionRefreshNumber >= mvPartitionsToRefresh.size()) {
            return;
        }

        // remove invalid cells from the input to-refresh partitions
        Set<PCellWithName> invalidCells = mvPartitionsToRefresh.getPartitions()
                .stream()
                .filter(pCell -> !mvRangePartitionMap.containsName(pCell.name()))
                .collect(Collectors.toSet());
        invalidCells.forEach(cell -> {
            logger.info("partition {} is not in mv's range partition map, remove it from to-refresh partitions",
                    cell.name());
            mvPartitionsToRefresh.remove(cell);
        });

        // dynamically get the number of partitions to be refreshed this time
        partitionRefreshNumber = getPartitionRefreshNumberAdaptive(mvPartitionsToRefresh, refreshStrategy);
        if (partitionRefreshNumber <= 0 || mvPartitionsToRefresh.size() <= partitionRefreshNumber) {
            return;
        }

        boolean isAscending = Config.materialized_view_refresh_ascending;
        Iterator<PCellWithName> iterator = getToRefreshPartitionsIterator(mvPartitionsToRefresh, isAscending);
        int i = 0;
        while (i < partitionRefreshNumber && iterator.hasNext()) {
            PCellWithName pCell = iterator.next();
            // remove potential mv partitions from to-refresh partitions since they are added only for being affected.
            if (mvToRefreshPotentialPartitions.containsName(pCell.name())) {
                continue;
            }
            i++;
            logger.debug("need to refresh partition name {}, value {}",
                    mv.getName(), pCell.name(), pCell.cell());
        }

        // no matter ascending or descending, we should always keep start is less than end
        // so we need to update the next start and end partition names in descending order
        String start = null;
        String end = null;
        PCellWithName endCell = null;
        while (iterator.hasNext()) {
            PCellWithName pCell = iterator.next();
            // remove potential mv partitions from to-refresh partitions since they are added only for being affected.
            if (mvToRefreshPotentialPartitions.containsName(pCell.name())) {
                continue;
            }
            start = getNextPartitionVal(pCell, isAscending, true);
            endCell = pCell;
            iterator.remove();
            break;
        }
        while (iterator.hasNext()) {
            endCell = iterator.next();
            iterator.remove();
        }

        if (!mvRefreshParams.isTentative()) {
            String prevStart = mvRefreshParams.getRangeStart();
            String prevEnd = mvRefreshParams.getRangeEnd();
            // partitionNameIter has just been traversed, and endPartitionName is not updated
            // will cause endPartitionName == null
            if (endCell != null) {
                end = getNextPartitionVal(endCell, isAscending, false);
            }
            String newStart = isAscending ? start : end;
            String newEnd = isAscending ? end : start;
            // check new start/end whether is fine
            if (isAscending) {
                if (prevStart != null && prevStart.equals(newStart)) {
                    // in ascending start should not be equal to prevStart, otherwise dead loop may happen
                    throw new SemanticException("Generate new task run start is the same to the previous in ascending, dead " +
                            "loop happens. start:%s, end:%s", newStart, newEnd);
                }
            } else {
                if (prevEnd != null && prevEnd.equals(newEnd)) {
                    // end descending end should not be equal to prevEnd, otherwise dead loop may happen
                    throw new SemanticException("Generate new task run end is the same to the previous in descending, dead " +
                            "loop happens. start:%s, end:%s", newStart, newEnd);
                }
            }
            mvContext.setNextPartitionStart(newStart);
            mvContext.setNextPartitionEnd(newEnd);
        }
    }
    private String getNextPartitionVal(PCellWithName pCell,
                                       boolean isAscending,
                                       boolean isStart) {
        PRangeCell pRangeCell = (PRangeCell) pCell.cell();
        Range<PartitionKey> partitionKeyRange = pRangeCell.getRange();
        PartitionKey endpoint = (isAscending ^ isStart) ?
                partitionKeyRange.upperEndpoint() : partitionKeyRange.lowerEndpoint();
        return AnalyzerUtils.parseLiteralExprToDateString(endpoint, 0);
    }

    private void addRangePartitions(Database database, MaterializedView materializedView,
                                    PCellSortedSet adds,
                                    Map<String, String> partitionProperties,
                                    DistributionDesc distributionDesc) {
        if (adds.isEmpty()) {
            return;
        }
        List<PartitionDesc> partitionDescs = Lists.newArrayList();

        for (PCellWithName addEntry : adds.getPartitions()) {
            String mvPartitionName = addEntry.name();
            Preconditions.checkArgument(addEntry.cell() instanceof PRangeCell);
            Range<PartitionKey> partitionKeyRange = ((PRangeCell) addEntry.cell()).getRange();
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
