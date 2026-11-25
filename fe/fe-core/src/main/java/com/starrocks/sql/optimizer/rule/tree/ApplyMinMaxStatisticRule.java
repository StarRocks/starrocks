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
        // Numeric/date group-by keys that a min/max stats manager could supply a range for.
        ColumnRefSet statsCandidates = new ColumnRefSet();

        // Pass 1, without touching the stats managers: take the ranges global dicts give for free and find
        // the keys the managers could cover.
        List<PhysicalOlapScanOperator> olapScans = Utils.extractPhysicalOlapScanOperator(root).stream()
                .filter(scan -> !((OlapTable) scan.getTable()).inputHasTempPartition(scan.getSelectedPartitionId()))
                .collect(Collectors.toList());
        for (PhysicalOlapScanOperator scan : olapScans) {
            OlapTable table = (OlapTable) scan.getTable();
            Map<Integer, ColumnDict> globalDicts = toGlobalDictMap(scan.getGlobalDicts());
            // Dict-encoded group-by keys (shared with the iceberg branch below).
            collectDictGroupByMinMax(scan, groupByRefSets, globalDicts, infos);
            // Fallback to the internal table-level min/max stats manager for the remaining
            // numeric/date group-by keys that are not dict-encoded.
            for (ColumnRefOperator column : scan.getColRefToColumnMetaMap().keySet()) {
                if (isUncoveredGroupByKey(column, groupByRefSets, infos) && table.getColumn(column.getName()) != null) {
                    statsCandidates.union(column);
                }
            }
        }

        // Iceberg scans. After low-cardinality rewrite the varchar group-by becomes TYPE_INT with
        // codes in [1, dictSize]; the runtime contract is enforced by GlobalDictNotMatch retry on
        // the BE side. globalDicts is only populated for parquet scans (gated in
        // DecodeCollector.visitPhysicalIcebergScan), so non-parquet falls through naturally.
        List<PhysicalIcebergScanOperator> icebergScans = Lists.newArrayList();
        Utils.extractOperator(root, icebergScans,
                op -> OperatorType.PHYSICAL_ICEBERG_SCAN.equals(op.getOpType()));
        for (PhysicalIcebergScanOperator iceberg : icebergScans) {
            IcebergTable table = (IcebergTable) iceberg.getTable();
            Map<Integer, ColumnDict> globalDicts = toGlobalDictMap(iceberg.getGlobalDicts());
            // Dict-encoded group-by keys (shared with the OLAP branch above).
            collectDictGroupByMinMax(iceberg, groupByRefSets, globalDicts, infos);
            // The remaining numeric/date group-by keys fall back to IcebergColumnMinMaxMgr, which needs a
            // single snapshot (see below).
            if (icebergSnapshotId(iceberg) == null) {
                continue;
            }
            for (ColumnRefOperator column : iceberg.getColRefToColumnMetaMap().keySet()) {
                if (isUncoveredGroupByKey(column, groupByRefSets, infos) && table.getColumn(column.getName()) != null) {
                    statsCandidates.union(column);
                }
            }
        }

        // An aggregation gets the compressed key only if every one of its group-by keys has a range, so a key
        // that can never have one (a string without a global dict, an expression, ...) rules the whole
        // aggregation out. Asking the managers for the other keys of such an aggregation would only trigger
        // [_META_] collections over whole tables whose result could never be used.
        ColumnRefSet statsNeeded = new ColumnRefSet();
        for (PhysicalHashAggregateOperator agg : aggLists) {
            boolean mayCompress = agg.getGroupBys().stream()
                    .allMatch(groupBy -> infos.containsKey(groupBy.getId()) || statsCandidates.contains(groupBy));
            if (mayCompress) {
                agg.getGroupBys().stream().filter(statsCandidates::contains).forEach(statsNeeded::union);
            }
        }

        // Pass 2: ask the managers, only for the keys an aggregation can use.
        for (PhysicalOlapScanOperator scan : olapScans) {
            List<ColumnRefOperator> columns = scan.getColRefToColumnMetaMap().keySet().stream()
                    .filter(statsNeeded::contains).collect(Collectors.toList());
            if (columns.isEmpty()) {
                continue;
            }
            OlapTable table = (OlapTable) scan.getTable();
            // Walks every partition of the table, so only paid for a scan that needs it.
            final Long lastUpdateTime = StatisticUtils.getTableLastUpdateTimestamp(table);
            if (null == lastUpdateTime) {
                continue;
            }
            for (ColumnRefOperator column : columns) {
                Column c = table.getColumn(column.getName());
                putStatsMgrMinMax(column, infos, IMinMaxStatsMgr.internalInstance(),
                        new ColumnIdentifier(table.getId(), c.getColumnId()),
                        new StatsVersion(-1, lastUpdateTime));
            }
        }
        for (PhysicalIcebergScanOperator iceberg : icebergScans) {
            List<ColumnRefOperator> columns = iceberg.getColRefToColumnMetaMap().keySet().stream()
                    .filter(statsNeeded::contains).collect(Collectors.toList());
            if (columns.isEmpty()) {
                continue;
            }
            IcebergTable table = (IcebergTable) iceberg.getTable();
            Long snapshotId = icebergSnapshotId(iceberg);
            for (ColumnRefOperator column : columns) {
                Column c = table.getColumn(column.getName());
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

    // Take the snapshot from the scan's own TvrVersionRange -- analyzer already pinned it -- so the cache key
    // matches the exact snapshot BE will read. Null/empty/non-single-snapshot ranges (e.g. TvrTableDelta)
    // return null and skip the IcebergColumnMinMaxMgr fallback.
    private static Long icebergSnapshotId(PhysicalIcebergScanOperator iceberg) {
        TvrVersionRange tvr = iceberg.getTvrVersionRange();
        return (tvr instanceof TvrTableSnapshot snap && !snap.isEmpty()) ? snap.getSnapshotId() : null;
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

    // Emit min/max for every group-by key on this scan that holds global dict codes: both
    // the projection's DictDefine mappings and bare scan columns. Shared by the OLAP and
    // iceberg branches so their dict handling stays identical.
    private static void collectDictGroupByMinMax(PhysicalScanOperator scan, ColumnRefSet groupByRefSets,
            Map<Integer, ColumnDict> globalDicts, Map<Integer, Pair<ConstantOperator, ConstantOperator>> infos) {
        if (scan.getProjection() != null) {
            for (var entry : scan.getProjection().getColumnRefMap().entrySet()) {
                if (groupByRefSets.contains(entry.getKey())
                        && entry.getValue() instanceof DictMappingOperator mappingOperator) {
                    // A mapping that keeps its expression's type (DictDecode) outputs the expression's
                    // values, not codes of a derived dict, so the dict size says nothing about its range.
                    if (mappingOperator.getType().matchesType(mappingOperator.getOriginScalaOperator().getType())) {
                        continue;
                    }
                    ColumnRefOperator column = mappingOperator.getDictColumn();
                    if (isNumericOrDate(column) && globalDicts.containsKey(column.getId())) {
                        // A dict-mapping expression maps each input code, including the dict's NULL
                        // slot, to at most one output value, so the derived result dict can hold up
                        // to base dict size + 1 codes (a value-adding expression such as
                        // `case when x is null then '-' ...` grows it by one). Reserve that extra
                        // code for the compressed group-by key range, otherwise it overflows the
                        // packed key width and decode fails with "Dict can't take cover all key".
                        infos.put(entry.getKey().getId(), dictMinMaxForMapping(globalDicts.get(column.getId())));
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

    private static Pair<ConstantOperator, ConstantOperator> dictMinMaxForMapping(ColumnDict dict) {
        return new Pair<>(ConstantOperator.createVarchar("0"),
                ConstantOperator.createVarchar("" + (dict.getDictSize() + 1)));
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
