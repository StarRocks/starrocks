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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
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
import com.starrocks.sql.common.RangePartitionDiffResult;
import com.starrocks.sql.common.RangePartitionDiffer;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
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
    private static final int CREATE_PARTITION_BATCH_SIZE = 64;

    private static final Logger LOG = LogManager.getLogger(MVPCTRefreshRangePartitioner.class);

    public MVPCTRefreshRangePartitioner(MvTaskRunContext mvContext,
                                        TaskRunContext context,
                                        Database db,
                                        MaterializedView mv) {
        super(mvContext, context, db, mv);
    }

    @Override
    public boolean syncAddOrDropPartitions() throws AnalysisException {
        String start = context == null ? null : context.getProperties().get(TaskRun.PARTITION_START);
        String end = context == null ? null : context.getProperties().get(TaskRun.PARTITION_END);
        Optional<Column> partitionColumnOpt = mv.getPartitionColumn();
        Preconditions.checkState(partitionColumnOpt.isPresent());
        Column partitionColumn = partitionColumnOpt.get();
        Range<PartitionKey> rangeToInclude = SyncPartitionUtils.createRange(start, end, partitionColumn);
        RangePartitionDiffResult result = RangePartitionDiffer.computeRangePartitionDiff(mv, rangeToInclude, false);
        if (result == null) {
            // TODO: throw exception?
            LOG.warn("compute range partition diff failed: mv: {}", mv.getName());
            return false;
        }
        Map<String, Range<PartitionKey>> deletes = result.rangePartitionDiff.getDeletes();

        // Delete old partitions and then add new partitions because the old and new partitions may overlap
        for (String mvPartitionName : deletes.keySet()) {
            dropPartition(db, mv, mvPartitionName);
        }
        LOG.info("The process of synchronizing materialized view [{}] delete partitions range [{}]",
                mv.getName(), deletes);

        // Create new added materialized views' ranges
        Map<String, String> partitionProperties = MvUtils.getPartitionProperties(mv);
        DistributionDesc distributionDesc = MvUtils.getDistributionDesc(mv);
        Map<String, Range<PartitionKey>> adds = result.rangePartitionDiff.getAdds();
        addRangePartitions(db, mv, adds, partitionProperties, distributionDesc);
        adds.entrySet().stream().forEach(entry -> result.mvRangePartitionMap.put(entry.getKey(), entry.getValue()));
        LOG.info("The process of synchronizing materialized view [{}] add partitions range [{}]",
                mv.getName(), adds);

        // used to get partitions to refresh
        Map<Table, Expr> tableToExprMap = mv.getRefBaseTablePartitionExprs();
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = RangePartitionDiffer
                .generateBaseRefMap(result.refBaseTablePartitionMap, tableToExprMap, result.mvRangePartitionMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = RangePartitionDiffer
                .generateMvRefMap(result.mvRangePartitionMap, tableToExprMap, result.refBaseTablePartitionMap);
        mvContext.setMvRangePartitionMap(result.mvRangePartitionMap);
        mvContext.setRefBaseTableMVIntersectedPartitions(baseToMvNameRef);
        mvContext.setMvRefBaseTableIntersectedPartitions(mvToBaseNameRef);
        mvContext.setRefBaseTableRangePartitionMap(result.refBaseTablePartitionMap);
        mvContext.setExternalRefBaseTableMVPartitionMap(result.refBaseTableMVPartitionMap);
        return true;
    }

    @Override
    public Expr generatePartitionPredicate(Table table, Set<String> refBaseTablePartitionNames,
                                           Expr mvPartitionSlotRef) throws AnalysisException {
        List<Range<PartitionKey>> sourceTablePartitionRange = Lists.newArrayList();
        for (String partitionName : refBaseTablePartitionNames) {
            sourceTablePartitionRange.add(mvContext.getRefBaseTableRangePartitionMap()
                    .get(table).get(partitionName));
        }
        sourceTablePartitionRange = MvUtils.mergeRanges(sourceTablePartitionRange);
        // for nested mv, the base table may be another mv, which is partition by str2date(dt, '%Y%m%d')
        // here we should convert date into '%Y%m%d' format
        Expr partitionExpr = mv.getPartitionExpr();
        Map<Table, Column> partitionTableAndColumn = mv.getRefBaseTablePartitionColumns();
        if (!partitionTableAndColumn.containsKey(table)) {
            LOG.warn("Cannot generate mv refresh partition predicate because cannot decide the partition column of table {}," +
                    "partitionTableAndColumn:{}", table.getName(), partitionTableAndColumn);
            return null;
        }
        boolean isConvertToDate = PartitionUtil.isConvertToDate(partitionExpr, partitionTableAndColumn.get(table));
        if (isConvertToDate && partitionExpr instanceof FunctionCallExpr
                && !sourceTablePartitionRange.isEmpty() && MvUtils.isDateRange(sourceTablePartitionRange.get(0))) {
            Optional<FunctionCallExpr> functionCallExprOpt = getStr2DateExpr(partitionExpr);
            if (!functionCallExprOpt.isPresent()) {
                LOG.warn("invalid partition expr:{}", partitionExpr);
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
    public Set<String> getMVPartitionsToRefresh(PartitionInfo mvPartitionInfo,
                                                Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                String start, String end, boolean force,
                                                Set<String> mvPotentialPartitionNames) throws AnalysisException {
        // range partitioned materialized views
        boolean isAutoRefresh = mvContext.getTaskType().isAutoRefresh();
        int partitionTTLNumber = mvContext.getPartitionTTLNumber();
        boolean isRefreshMvBaseOnNonRefTables = needsRefreshBasedOnNonRefTables(snapshotBaseTables);
        Set<String> mvRangePartitionNames = getMVPartitionNamesWithTTL(mv, start, end, partitionTTLNumber, isAutoRefresh);
        LOG.info("Get partition names by range with partition limit, start: {}, end: {}, force:{}, " +
                        "partitionTTLNumber: {}, isAutoRefresh: {}, mvRangePartitionNames: {}, isRefreshMvBaseOnNonRefTables:{}",
                start, end, force, partitionTTLNumber, isAutoRefresh, mvRangePartitionNames, isRefreshMvBaseOnNonRefTables);

        // check non-ref base tables or force refresh
        if (force || isRefreshMvBaseOnNonRefTables) {
            if (start == null && end == null) {
                // if non-partition table changed, should refresh all partitions of materialized view
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
                    return getMvPartitionNamesToRefresh(mvRangePartitionNames);
                }
            }
        }

        // check related partition table
        Set<String> needRefreshMvPartitionNames = getMvPartitionNamesToRefresh(mvRangePartitionNames);
        if (needRefreshMvPartitionNames.isEmpty()) {
            LOG.info("No need to refresh materialized view partitions, mv: {}", mv.getName());
            return needRefreshMvPartitionNames;
        }

        Map<Table, Set<String>> baseChangedPartitionNames = getBasePartitionNamesByMVPartitionNames(needRefreshMvPartitionNames);
        if (baseChangedPartitionNames.isEmpty()) {
            LOG.info("Cannot get associated base table change partitions from mv's refresh partitions {}, mv: {}",
                    needRefreshMvPartitionNames, mv.getName());
            return needRefreshMvPartitionNames;
        }

        List<TableWithPartitions> baseTableWithPartitions = baseChangedPartitionNames.keySet().stream()
                .map(x -> new TableWithPartitions(x, baseChangedPartitionNames.get(x)))
                .collect(Collectors.toList());
        Map<Table, Map<String, Range<PartitionKey>>> refBaseTableRangePartitionMap =
                mvContext.getRefBaseTableRangePartitionMap();
        Map<String, Range<PartitionKey>> mvRangePartitionMap = mvContext.getMvRangePartitionMap();
        if (mv.isCalcPotentialRefreshPartition(baseTableWithPartitions,
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
        return needRefreshMvPartitionNames;
    }

    @Override
    public Set<String> getMVPartitionsToRefreshWithForce(int partitionTTLNumber) throws AnalysisException {
        return mv.getValidRangePartitionMap(partitionTTLNumber).keySet();
    }

    @Override
    public Set<String> getMVPartitionNamesWithTTL(MaterializedView materializedView, String start, String end,
                                                  int partitionTTLNumber, boolean isAutoRefresh) throws AnalysisException {
        int autoRefreshPartitionsLimit = materializedView.getTableProperty().getAutoRefreshPartitionsLimit();
        boolean hasPartitionRange = StringUtils.isNoneEmpty(start) || StringUtils.isNoneEmpty(end);

        if (hasPartitionRange) {
            Set<String> result = Sets.newHashSet();
            Column partitionColumn = materializedView.getPartitionColumn().get();
            Range<PartitionKey> rangeToInclude = createRange(start, end, partitionColumn);
            Map<String, Range<PartitionKey>> rangeMap = materializedView.getValidRangePartitionMap(partitionTTLNumber);
            for (Map.Entry<String, Range<PartitionKey>> entry : rangeMap.entrySet()) {
                Range<PartitionKey> rangeToCheck = entry.getValue();
                int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
                int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
                if (!(lowerCmp >= 0 || upperCmp <= 0)) {
                    result.add(entry.getKey());
                }
            }
            return result;
        }

        int lastPartitionNum;
        if (partitionTTLNumber > 0 && isAutoRefresh && autoRefreshPartitionsLimit > 0) {
            lastPartitionNum = Math.min(partitionTTLNumber, autoRefreshPartitionsLimit);
        } else if (isAutoRefresh && autoRefreshPartitionsLimit > 0) {
            lastPartitionNum = autoRefreshPartitionsLimit;
        } else if (partitionTTLNumber > 0) {
            lastPartitionNum = partitionTTLNumber;
        } else {
            lastPartitionNum = TableProperty.INVALID;
        }

        return materializedView.getValidRangePartitionMap(lastPartitionNum).keySet();
    }

    @Override
    public void filterPartitionByRefreshNumber(Set<String> mvPartitionsToRefresh,
                                               Set<String> mvPotentialPartitionNames,
                                               boolean tentative) {
        int partitionRefreshNumber = mv.getTableProperty().getPartitionRefreshNumber();
        Map<String, Range<PartitionKey>> mvRangePartitionMap = mv.getRangePartitionMap();
        if (partitionRefreshNumber <= 0 || partitionRefreshNumber >= mvRangePartitionMap.size()) {
            return;
        }
        Map<String, Range<PartitionKey>> mappedPartitionsToRefresh = Maps.newHashMap();
        Iterator<String> mvToRefreshPartitionsIter = mvPartitionsToRefresh.iterator();
        while (mvToRefreshPartitionsIter.hasNext()) {
            String mvPartitionName = mvToRefreshPartitionsIter.next();
            // skip if partition is not in the mv's partition range map
            if (!mvRangePartitionMap.containsKey(mvPartitionName)) {
                LOG.warn("Partition {} is not in the materialized view's partition range map for mv:{}, " +
                        "remove it from refresh list", mvPartitionName, mv.getName());
                mvToRefreshPartitionsIter.remove();
                continue;
            }
            mappedPartitionsToRefresh.put(mvPartitionName, mvRangePartitionMap.get(mvPartitionName));
        }
        LinkedList<String> sortedPartition = mappedPartitionsToRefresh.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(RangeUtils.RANGE_COMPARATOR))
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(LinkedList::new));

        Iterator<String> partitionNameIter = Config.materialized_view_refresh_ascending
                ? sortedPartition.iterator() : sortedPartition.descendingIterator();
        String mvRefreshPartition = "";
        for (int i = 0; i < partitionRefreshNumber; i++) {
            if (partitionNameIter.hasNext()) {
                mvRefreshPartition = partitionNameIter.next();
                partitionNameIter.remove();
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
        if (!Config.materialized_view_refresh_ascending) {
            partitionNameIter = sortedPartition.iterator();
        }
        setNextPartitionStartAndEnd(mvPartitionsToRefresh, mappedPartitionsToRefresh, partitionNameIter, tentative);
    }

    private void setNextPartitionStartAndEnd(Set<String> partitionsToRefresh,
                                             Map<String, Range<PartitionKey>> mappedPartitionsToRefresh,
                                             Iterator<String> partitionNameIter,
                                             boolean tentative) {
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
                LOG.warn("Cannot find need refreshed mv table partition from synced partition info: {}",
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
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(materializedView);
            analyzer.analyze(mvContext.getCtx(), alterPartition);
            try {
                GlobalStateMgr.getCurrentState().getStarRocksMeta().addPartitions(mvContext.getCtx(),
                        database, materializedView.getName(), alterPartition);
            } catch (Exception e) {
                throw new DmlException("Expression add partition failed: %s, db: %s, table: %s", e, e.getMessage(),
                        database.getFullName(), materializedView.getName());
            }
            Uninterruptibles.sleepUninterruptibly(Config.mv_create_partition_batch_interval_ms, TimeUnit.MILLISECONDS);
        }
    }
}
