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

        List<PhysicalOlapScanOperator> scanOperators = Utils.extractPhysicalOlapScanOperator(root);
        for (PhysicalOlapScanOperator scanOperator : scanOperators) {
            final Map<Integer, ColumnDict> globalDicts =
                    scanOperator.getGlobalDicts().stream().collect(Collectors.toMap(p -> p.first, p -> p.second));
            OlapTable table = (OlapTable) scanOperator.getTable();
            final Long lastUpdateTime = StatisticUtils.getTableLastUpdateTimestamp(table);
            if (null == lastUpdateTime) {
                continue;
            }
            if (table.inputHasTempPartition(scanOperator.getSelectedPartitionId())) {
                continue;
            }
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
                            final ConstantOperator max = ConstantOperator.createVarchar("" + columnDict.getDictSize());
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
