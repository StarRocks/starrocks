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


package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MaterializedViewOptimizer {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewOptimizer.class);

    private List<ColumnRefOperator> outputExpressions;

    public OptExpression optimize(MaterializedView mv,
                                  ColumnRefFactory columnRefFactory,
                                  ConnectContext connectContext,
                                  Set<String> mvPartitionNamesToRefresh) {
        String mvSql = mv.getViewDefineSql();
        Pair<OptExpression, LogicalPlan> plans = MvUtils.getRuleOptimizedLogicalPlan(mvSql, columnRefFactory, connectContext);
        if (plans == null) {
            return null;
        }
        outputExpressions = plans.second.getOutputColumn();
        OptExpression mvPlan = plans.first;
        if (mv.getPartitionInfo() instanceof ExpressionRangePartitionInfo && !mvPartitionNamesToRefresh.isEmpty()) {
            boolean ret = updateScanWithPartitionRange(mv, mvPlan, mvPartitionNamesToRefresh);
            if (!ret) {
                return null;
            }
        }
        return mvPlan;
    }

    public List<ColumnRefOperator> getOutputExpressions() {
        return outputExpressions;
    }

    // try to get partial partition predicates of partitioned mv.
    // eg, mv1's base partition table is t1, partition column is k1 and has two partition:
    // p1:[2022-01-01, 2022-01-02), p1 is updated(refreshed),
    // p2:[2022-01-02, 2022-01-03), p2 is outdated,
    // then this function will add predicate:
    // k1 >= "2022-01-01" and k1 < "2022-01-02" to scan node of t1
    public boolean updateScanWithPartitionRange(MaterializedView mv,
                                                OptExpression mvPlan,
                                                Set<String> mvPartitionNamesToRefresh) {
        Pair<Table, Column> partitionTableAndColumns = mv.getPartitionTableAndColumn();
        if (partitionTableAndColumns == null) {
            return false;
        }

        Table partitionByTable = partitionTableAndColumns.first;
        List<Range<PartitionKey>> latestBaseTableRanges =
                getLatestPartitionRangeForTable(partitionByTable, partitionTableAndColumns.second,
                        mv, mvPartitionNamesToRefresh);
        if (latestBaseTableRanges.isEmpty()) {
            // if there isn't an updated partition, do not rewrite
            return false;
        }

        Column partitionColumn = partitionTableAndColumns.second;
        List<OptExpression> scanExprs = MvUtils.collectScanExprs(mvPlan);
        for (OptExpression scanExpr : scanExprs) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) scanExpr.getOp();
            Table scanTable = scanOperator.getTable();
            if ((scanTable.isLocalTable() && !scanTable.equals(partitionTableAndColumns.first))
                    || (!scanTable.isLocalTable()) && !scanTable.getTableIdentifier().equals(
                    partitionTableAndColumns.first.getTableIdentifier())) {
                continue;
            }
            ColumnRefOperator columnRef = scanOperator.getColumnReference(partitionColumn);
            List<ScalarOperator> partitionPredicates = MvUtils.convertRanges(columnRef, latestBaseTableRanges);
            ScalarOperator partialPartitionPredicate = Utils.compoundOr(partitionPredicates);
            ScalarOperator originalPredicate = scanOperator.getPredicate();
            ScalarOperator newPredicate = Utils.compoundAnd(originalPredicate, partialPartitionPredicate);
            scanOperator.setPredicate(newPredicate);
        }
        return true;
    }

    private List<Range<PartitionKey>> getLatestPartitionRangeForTable(Table partitionByTable,
                                                                      Column partitionColumn,
                                                                      MaterializedView mv,
                                                                      Set<String> mvPartitionNamesToRefresh) {
        Set<String> modifiedPartitionNames = mv.getUpdatedPartitionNamesOfTable(partitionByTable);
        List<Range<PartitionKey>> baseTableRanges = getLatestPartitionRange(partitionByTable, partitionColumn,
                modifiedPartitionNames);
        List<Range<PartitionKey>> mvRanges = getLatestPartitionRangeForLocalTable(mv, mvPartitionNamesToRefresh);
        List<Range<PartitionKey>> latestBaseTableRanges = Lists.newArrayList();
        for (Range<PartitionKey> range : baseTableRanges) {
            if (mvRanges.stream().anyMatch(mvRange -> mvRange.encloses(range))) {
                latestBaseTableRanges.add(range);
            }
        }
        latestBaseTableRanges = MvUtils.mergeRanges(latestBaseTableRanges);
        return latestBaseTableRanges;
    }

    private List<Range<PartitionKey>> getLatestPartitionRangeForLocalTable(OlapTable partitionTable,
                                                                           Set<String> modifiedPartitionNames) {
        // partitions that will be excluded
        Set<Long> filteredIds = Sets.newHashSet();
        for (Partition p : partitionTable.getPartitions()) {
            if (modifiedPartitionNames.contains(p.getName()) || !p.hasData()) {
                filteredIds.add(p.getId());
            }
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionTable.getPartitionInfo();
        return rangePartitionInfo.getRangeList(filteredIds, false);
    }

    private List<Range<PartitionKey>> getLatestPartitionRange(Table table, Column partitionColumn,
                                                              Set<String> modifiedPartitionNames) {
        if (table.isLocalTable()) {
            return getLatestPartitionRangeForLocalTable((OlapTable) table, modifiedPartitionNames);
        } else {
            Map<String, Range<PartitionKey>> partitionMap;
            try {
                partitionMap = PartitionUtil.getPartitionRange(table, partitionColumn);
            } catch (UserException e) {
                LOG.warn("Materialized view Optimizer compute partition range failed.", e);
                return Lists.newArrayList();
            }
            return partitionMap.entrySet().stream().filter(entry -> !modifiedPartitionNames.contains(entry.getKey())).
                    map(Map.Entry::getValue).collect(Collectors.toList());
        }
    }
}
