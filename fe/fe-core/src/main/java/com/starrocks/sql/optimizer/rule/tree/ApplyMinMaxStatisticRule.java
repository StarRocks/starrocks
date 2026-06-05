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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
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

        List<PhysicalOlapScanOperator> olapScans = Utils.extractPhysicalOlapScanOperator(root);
        for (PhysicalOlapScanOperator scan : olapScans) {
            OlapTable table = (OlapTable) scan.getTable();
            final Long lastUpdateTime = StatisticUtils.getTableLastUpdateTimestamp(table);
            if (null == lastUpdateTime) {
                continue;
            }
            if (table.inputHasTempPartition(scan.getSelectedPartitionId())) {
                continue;
            }
            Map<Integer, ColumnDict> globalDicts = toGlobalDictMap(scan.getGlobalDicts());
            // Dict-encoded group-by keys (shared with the iceberg branch below).
            collectDictGroupByMinMax(scan, groupByRefSets, globalDicts, infos);
            // Fallback to the internal table-level min/max stats manager for the remaining
            // numeric/date group-by keys that are not dict-encoded.
            for (ColumnRefOperator column : scan.getColRefToColumnMetaMap().keySet()) {
                if (!isUncoveredGroupByKey(column, groupByRefSets, infos)) {
                    continue;
                }
                Column c = table.getColumn(column.getName());
                if (c == null) {
                    continue;
                }
                putStatsMgrMinMax(column, infos, IMinMaxStatsMgr.internalInstance(),
                        new ColumnIdentifier(table.getId(), c.getColumnId()),
                        new StatsVersion(-1, lastUpdateTime));
            }
        }

        // Iceberg scans. After low-cardinality rewrite the varchar group-by becomes TYPE_INT with
        // codes in [1, dictSize]; the runtime contract is enforced by GlobalDictNotMatch retry on
        // the BE side. globalDicts is only populated for parquet scans (gated in
        // DecodeCollector.visitPhysicalIcebergScan), so non-parquet falls through naturally.
        List<PhysicalScanOperator> icebergScans = Lists.newArrayList();
        Utils.extractOperator(root, icebergScans,
                op -> OperatorType.PHYSICAL_ICEBERG_SCAN.equals(op.getOpType()));
        for (PhysicalScanOperator scan : icebergScans) {
            PhysicalIcebergScanOperator iceberg = (PhysicalIcebergScanOperator) scan;
            IcebergTable table = (IcebergTable) iceberg.getTable();
            Map<Integer, ColumnDict> globalDicts = toGlobalDictMap(iceberg.getGlobalDicts());
            // Dict-encoded group-by keys (shared with the OLAP branch above).
            collectDictGroupByMinMax(iceberg, groupByRefSets, globalDicts, infos);
            // Fallback to IcebergColumnMinMaxMgr for the remaining numeric/date group-by keys. Take
            // the snapshot from the scan's own TvrVersionRange — analyzer already pinned it — so the
            // cache key matches the exact snapshot BE will read. Null/empty/non-single-snapshot
            // ranges (e.g. TvrTableDelta) skip the fallback.
            TvrVersionRange tvr = iceberg.getTvrVersionRange();
            Long snapshotId = (tvr instanceof TvrTableSnapshot snap && !snap.isEmpty())
                    ? snap.getSnapshotId() : null;
            if (snapshotId == null) {
                continue;
            }
            for (ColumnRefOperator column : iceberg.getColRefToColumnMetaMap().keySet()) {
                if (!isUncoveredGroupByKey(column, groupByRefSets, infos)) {
                    continue;
                }
                Column c = table.getColumn(column.getName());
                if (c == null) {
                    continue;
                }
                putStatsMgrMinMax(column, infos, IMinMaxStatsMgr.icebergInstance(),
                        new ColumnIdentifier(table.getUUID(), c.getColumnId()),
                        new StatsVersion(-1, snapshotId));
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

    // Numeric/date columns are the only ones whose dict codes form the contiguous [0, dictSize]
    // range the aggregator's compressed-key path can use as group-by min/max bounds.
    private static boolean isNumericOrDate(ColumnRefOperator column) {
        return column.getType().isNumericType() || column.getType().isDate();
    }

    private static Map<Integer, ColumnDict> toGlobalDictMap(List<Pair<Integer, ColumnDict>> globalDicts) {
        return globalDicts.stream().collect(Collectors.toMap(p -> p.first, p -> p.second));
    }

    // A numeric/date group-by key on this scan that no scan has supplied min/max for yet.
    private static boolean isUncoveredGroupByKey(ColumnRefOperator column, ColumnRefSet groupByRefSets,
            Map<Integer, Pair<ConstantOperator, ConstantOperator>> infos) {
        return groupByRefSets.contains(column.getId()) && isNumericOrDate(column)
                && !infos.containsKey(column.getId());
    }

    // Emit (0, dictSize) min/max for every group-by key backed by a global dict on this scan — both
    // the projection's DictMappingOperator form and bare scan columns. Shared by the OLAP and
    // iceberg branches so their dict handling stays identical.
    private static void collectDictGroupByMinMax(PhysicalScanOperator scan, ColumnRefSet groupByRefSets,
            Map<Integer, ColumnDict> globalDicts, Map<Integer, Pair<ConstantOperator, ConstantOperator>> infos) {
        if (scan.getProjection() != null) {
            for (var entry : scan.getProjection().getColumnRefMap().entrySet()) {
                if (groupByRefSets.contains(entry.getKey())
                        && entry.getValue() instanceof DictMappingOperator mappingOperator) {
                    ColumnRefOperator column = mappingOperator.getDictColumn();
                    if (isNumericOrDate(column) && globalDicts.containsKey(column.getId())) {
                        infos.put(entry.getKey().getId(), dictMinMax(globalDicts.get(column.getId())));
                    }
                }
            }
        }
        for (ColumnRefOperator column : scan.getColRefToColumnMetaMap().keySet()) {
            if (groupByRefSets.contains(column.getId()) && isNumericOrDate(column)
                    && globalDicts.containsKey(column.getId())) {
                infos.put(column.getId(), dictMinMax(globalDicts.get(column.getId())));
            }
        }
    }

    private static Pair<ConstantOperator, ConstantOperator> dictMinMax(ColumnDict dict) {
        return new Pair<>(ConstantOperator.createVarchar("0"),
                ConstantOperator.createVarchar("" + dict.getDictSize()));
    }

    private static void putStatsMgrMinMax(ColumnRefOperator column,
            Map<Integer, Pair<ConstantOperator, ConstantOperator>> infos, IMinMaxStatsMgr mgr,
            ColumnIdentifier columnId, StatsVersion version) {
        Optional<IMinMaxStatsMgr.ColumnMinMax> minMax = mgr.getStats(columnId, version);
        if (minMax.isEmpty()) {
            return;
        }
        infos.put(column.getId(), new Pair<>(ConstantOperator.createVarchar(minMax.get().minValue()),
                ConstantOperator.createVarchar(minMax.get().maxValue())));
    }
}
