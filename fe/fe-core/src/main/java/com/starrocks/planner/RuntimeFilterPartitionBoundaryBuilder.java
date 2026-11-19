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

package com.starrocks.planner;

import com.google.common.collect.Range;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.thrift.TPartitionBoundary;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class RuntimeFilterPartitionBoundaryBuilder {
    private RuntimeFilterPartitionBoundaryBuilder() {
    }

    static List<TPartitionBoundary> build(
            OlapTable table, List<Long> selectedPartitionIds, Map<SlotRef, Integer> probeSlots, int listValueLimit) {
        if (probeSlots.isEmpty()) {
            return List.of();
        }
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (partitionInfo instanceof RangePartitionInfo rangeInfo) {
            return buildRangeBoundaries(table, selectedPartitionIds, probeSlots,
                    rangeInfo.getPartitionColumns(table.getIdToColumn()).size() > 1, rangeInfo);
        }
        if (partitionInfo instanceof ListPartitionInfo listInfo) {
            return buildListBoundaries(
                    table, selectedPartitionIds, probeSlots, listInfo, listValueLimit);
        }
        return List.of();
    }

    private static List<TPartitionBoundary> buildRangeBoundaries(
            OlapTable table, List<Long> selectedPartitionIds, Map<SlotRef, Integer> probeSlots,
            boolean multiColumn, RangePartitionInfo rangeInfo) {
        List<TPartitionBoundary> boundaries = new ArrayList<>();
        for (SlotRef probeSlot : probeSlots.keySet()) {
            for (long partitionId : selectedPartitionIds) {
                Partition partition = table.getPartition(partitionId);
                Range<PartitionKey> range = rangeInfo.getRange(partitionId);
                if (partition == null || range == null || range.isEmpty()) {
                    continue;
                }
                TPartitionBoundary boundary = new TPartitionBoundary();
                boundary.setSlot_id(probeSlot.getSlotId().asInt());
                if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
                    LiteralExpr lower = range.lowerEndpoint().getKeys().get(0);
                    if (lower.getType().isIntegerType() || lower.getType().isDate()) {
                        boundary.setRange_lower_int(compactValue(lower));
                    } else {
                        boundary.setRange_lower(ExprToThrift.treeToThrift(lower));
                    }
                } else {
                    boundary.setContains_null(true);
                }
                if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
                    LiteralExpr upper = range.upperEndpoint().getKeys().get(0);
                    if (upper.getType().isIntegerType() || upper.getType().isDate()) {
                        boundary.setRange_upper_int(compactValue(upper));
                    } else {
                        boundary.setRange_upper(ExprToThrift.treeToThrift(upper));
                    }
                    if (multiColumn) {
                        boundary.setRange_upper_closed(true);
                    }
                }
                if (!boundary.isSetRange_lower() && !boundary.isSetRange_upper()
                        && !boundary.isSetRange_lower_int() && !boundary.isSetRange_upper_int()) {
                    continue;
                }
                setPhysicalPartitionIds(partition, boundary);
                if (boundary.getPhysical_partition_idsSize() > 0) {
                    boundaries.add(boundary);
                }
            }
        }
        return boundaries;
    }

    // Encode integers directly and DATE as yyyymmdd, the form computePartitionRange already sends.
    private static long compactValue(LiteralExpr literal) {
        if (literal instanceof DateLiteral date) {
            return date.getYear() * 10000 + date.getMonth() * 100 + date.getDay();
        }
        return literal.getLongValue();
    }

    private static List<TPartitionBoundary> buildListBoundaries(
            OlapTable table, List<Long> selectedPartitionIds, Map<SlotRef, Integer> probeSlots,
            ListPartitionInfo listInfo, int listValueLimit) {
        List<TPartitionBoundary> boundaries = new ArrayList<>();
        for (Map.Entry<SlotRef, Integer> entry : probeSlots.entrySet()) {
            SlotRef probeSlot = entry.getKey();
            int columnIndex = entry.getValue();
            Type columnType = probeSlot.getType();
            for (long partitionId : selectedPartitionIds) {
                Partition partition = table.getPartition(partitionId);
                List<LiteralExpr> values = getListPartitionValues(listInfo, partitionId, columnIndex);
                if (partition == null || values == null) {
                    continue;
                }
                Set<LiteralExpr> uniqueValues = new LinkedHashSet<>(values);
                if (uniqueValues.size() > listValueLimit) {
                    continue;
                }
                boolean containsNull = uniqueValues.removeIf(LiteralExpr::isConstantNull);
                if (uniqueValues.isEmpty() && !containsNull) {
                    continue;
                }
                TPartitionBoundary boundary = new TPartitionBoundary();
                boundary.setSlot_id(probeSlot.getSlotId().asInt());
                boundary.setContains_null(containsNull);
                // NULL-only partitions use an empty literal list.
                if (!uniqueValues.isEmpty() && (columnType.isIntegerType() || columnType.isDate())) {
                    List<Long> compactValues = new ArrayList<>(uniqueValues.size());
                    for (LiteralExpr value : uniqueValues) {
                        compactValues.add(compactValue(value));
                    }
                    boundary.setList_int_values(compactValues);
                } else {
                    boundary.setList_values(ExprToThrift.treesToThrift(new ArrayList<>(uniqueValues)));
                }
                setPhysicalPartitionIds(partition, boundary);
                if (boundary.getPhysical_partition_idsSize() > 0) {
                    boundaries.add(boundary);
                }
            }
        }
        return boundaries;
    }

    private static List<LiteralExpr> getListPartitionValues(
            ListPartitionInfo listInfo, long partitionId, int columnIndex) {
        List<LiteralExpr> singleColumnValues = listInfo.getLiteralExprValues().get(partitionId);
        if (singleColumnValues != null) {
            return columnIndex == 0 ? singleColumnValues : null;
        }
        List<List<LiteralExpr>> tuples = listInfo.getMultiLiteralExprValues().get(partitionId);
        if (tuples == null) {
            return null;
        }
        List<LiteralExpr> values = new ArrayList<>(tuples.size());
        for (List<LiteralExpr> tuple : tuples) {
            if (columnIndex >= tuple.size()) {
                return null;
            }
            values.add(tuple.get(columnIndex));
        }
        return values;
    }

    private static void setPhysicalPartitionIds(Partition partition, TPartitionBoundary boundary) {
        List<Long> physicalPartitionIds = new ArrayList<>();
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            if (!physicalPartition.hasStorageData()) {
                continue;
            }
            physicalPartitionIds.add(physicalPartition.getId());
        }
        boundary.setPhysical_partition_ids(physicalPartitionIds);
    }
}
