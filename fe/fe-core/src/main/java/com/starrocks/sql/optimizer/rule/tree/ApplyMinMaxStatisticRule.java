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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.statistic.StatisticUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

// This Rule should be after Low cardinality rewrite Rules
public class ApplyMinMaxStatisticRule implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (!ConnectContext.get().getSessionVariable().enableGroupByCompressedKey()) {
            return root;
        }
        // collect aggregate operators
        List<PhysicalHashAggregateOperator> aggLists = Lists.newArrayList();
        Utils.extractOperator(root, aggLists, op -> OperatorType.PHYSICAL_HASH_AGG.equals(op.getOpType()));
        // collect
        ColumnRefSet groupByRefSets = new ColumnRefSet();
        for (PhysicalHashAggregateOperator agg : aggLists) {
            for (ColumnRefOperator groupBy : agg.getGroupBys()) {
                groupByRefSets.union(groupBy.getId());
            }
        }

        Map<Integer, Pair<ConstantOperator, ConstantOperator>> infos = Maps.newHashMap();
        // Numeric/date group-by keys that a min/max stats manager could supply a range for.
        ColumnRefSet statsCandidates = new ColumnRefSet();

        // Pass 1, without touching the stats manager: take the ranges global dicts give for free and find
        // the keys the manager could cover.
        List<PhysicalOlapScanOperator> scanOperators = Utils.extractPhysicalOlapScanOperator(root).stream()
                .filter(scan -> !((OlapTable) scan.getTable()).inputHasTempPartition(scan.getSelectedPartitionId()))
                .collect(Collectors.toList());
        for (PhysicalOlapScanOperator scanOperator : scanOperators) {
            final Map<Integer, ColumnDict> globalDicts =
                    scanOperator.getGlobalDicts().stream().collect(Collectors.toMap(p -> p.first, p -> p.second));
            OlapTable table = (OlapTable) scanOperator.getTable();
            if (scanOperator.getProjection() != null) {
                for (var entry : scanOperator.getProjection().getColumnRefMap().entrySet()) {
                    if (groupByRefSets.contains(entry.getKey()) &&
                            entry.getValue() instanceof DictMappingOperator mappingOperator) {
                        ColumnRefOperator column = mappingOperator.getDictColumn();
                        if (!column.getType().isNumericType() && !column.getType().isDate()) {
                            continue;
                        }
                        if (globalDicts.containsKey(column.getId())) {
                            final ConstantOperator min = ConstantOperator.createVarchar("0");
                            final ColumnDict columnDict = globalDicts.get(column.getId());
                            // A value-adding dict-mapping key (e.g. `case when x is null then '-'`)
                            // can add one code beyond the base dict; reserve it, or the compressed
                            // group-by key width overflows and decode fails on "key :0".
                            final ConstantOperator max =
                                    ConstantOperator.createVarchar("" + (columnDict.getDictSize() + 1));
                            infos.put(entry.getKey().getId(), new Pair<>(min, max));
                        }
                    }
                }
            }
            for (ColumnRefOperator column : scanOperator.getColRefToColumnMetaMap().keySet()) {
                if (groupByRefSets.contains(column.getId())) {
                    if (!column.getType().isNumericType() && !column.getType().isDate()) {
                        continue;
                    }
                    if (globalDicts.containsKey(column.getId())) {
                        final ConstantOperator min = ConstantOperator.createVarchar("0");
                        final ColumnDict columnDict = globalDicts.get(column.getId());
                        final ConstantOperator max = ConstantOperator.createVarchar("" + columnDict.getDictSize());
                        infos.put(column.getId(), new Pair<>(min, max));
                        continue;
                    }
                    // Fallback to the internal table-level min/max stats manager for the remaining
                    // numeric/date group-by keys that are not dict-encoded.
                    if (table.getColumn(column.getName()) != null) {
                        statsCandidates.union(column);
                    }
                }
            }
        }

        // An aggregation gets the compressed key only if every one of its group-by keys has a range, so a key
        // that can never have one (a string without a global dict, an expression, ...) rules the whole
        // aggregation out. Asking the manager for the other keys of such an aggregation would only trigger
        // [_META_] collections over whole tables whose result could never be used.
        ColumnRefSet statsNeeded = new ColumnRefSet();
        for (PhysicalHashAggregateOperator agg : aggLists) {
            boolean mayCompress = agg.getGroupBys().stream()
                    .allMatch(groupBy -> infos.containsKey(groupBy.getId()) || statsCandidates.contains(groupBy));
            if (mayCompress) {
                agg.getGroupBys().stream().filter(statsCandidates::contains).forEach(statsNeeded::union);
            }
        }

        // Pass 2: ask the manager, only for the keys an aggregation can use.
        for (PhysicalOlapScanOperator scanOperator : scanOperators) {
            List<ColumnRefOperator> columns = scanOperator.getColRefToColumnMetaMap().keySet().stream()
                    .filter(statsNeeded::contains).collect(Collectors.toList());
            if (columns.isEmpty()) {
                continue;
            }
            OlapTable table = (OlapTable) scanOperator.getTable();
            // Walks every partition of the table, so only paid for a scan that needs it.
            final Long lastUpdateTime = StatisticUtils.getTableLastUpdateTimestamp(table);
            if (null == lastUpdateTime) {
                continue;
            }
            for (ColumnRefOperator column : columns) {
                Column c = table.getColumn(column.getName());
                Optional<IMinMaxStatsMgr.ColumnMinMax> minMax = IMinMaxStatsMgr.internalInstance()
                        .getStats(new ColumnIdentifier(table.getId(), c.getColumnId()),
                                new StatsVersion(-1, lastUpdateTime));
                if (minMax.isEmpty()) {
                    continue;
                }
                final ConstantOperator min = ConstantOperator.createVarchar(minMax.get().minValue());
                final ConstantOperator max = ConstantOperator.createVarchar(minMax.get().maxValue());
                infos.put(column.getId(), new Pair<>(min, max));
            }
        }

        for (PhysicalHashAggregateOperator agg : aggLists) {
            List<Pair<ConstantOperator, ConstantOperator>> groupByMinMaxStats = Lists.newArrayList();
            for (ColumnRefOperator groupBy : agg.getGroupBys()) {
                final Pair<ConstantOperator, ConstantOperator> minMaxStats = infos.get(groupBy.getId());
                if (minMaxStats != null) {
                    groupByMinMaxStats.add(minMaxStats);
                }
            }
            if (groupByMinMaxStats.size() == agg.getGroupBys().size()) {
                agg.setGroupByMinMaxStatistic(groupByMinMaxStats);
            }
        }

        return root;
    }
}
