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

package com.starrocks.sql.common.mv;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.common.PartitionMapping;
import com.starrocks.type.PrimitiveType;

import java.util.Comparator;
import java.util.List;

import static com.starrocks.sql.common.SyncPartitionUtils.getMVPartitionName;
import static com.starrocks.sql.common.SyncPartitionUtils.toPartitionKey;

/**
 * {@link MVMirrorRangePartitionMapper} mirrors the base table's ACTUAL partition boundaries into the mv
 * partition ranges instead of unrolling them into fixed-granularity cells.
 *
 * <p>Each (merged) base table partition range is snapped to the mv granularity boundary and kept as a single
 * mv partition, then de-duplicated. This is exactly the desired behavior for mixed-granularity base tables:
 * <pre>
 *  base table partition ranges : [2021-05-01, 2021-06-01) (a merged month)
 *                                [2024-06-01, 2024-06-02) (a day)
 *  mv partition expr           : date_trunc('day', dt)
 *
 *  mv's partition map result   :
 *                           p20210501_20210601 : [2021-05-01, 2021-06-01)  (stays month, 1:1 refresh)
 *                           p20240601_20240602 : [2024-06-01, 2024-06-02)  (stays day)
 * </pre>
 *
 * <p>Relationship with the other mappers:
 * <ul>
 *   <li>For a base partition that is exactly one granularity unit wide (the normal day partition of a
 *       {@code date_trunc('day')} table, including discrete/multi-union cases), mirror is IDENTICAL to
 *       {@link MVEagerRangePartitionMapper}, so enabling it is a no-op there.</li>
 *   <li>It only diverges from eager for coarser-than-granularity base partitions (merged months), which is
 *       the intended mixed-granularity behavior.</li>
 *   <li>Unlike {@link MVLazyRangePartitionMapper}, it never fills the gaps between discrete base partitions,
 *       so it never creates empty MV partitions.</li>
 *   <li>If the snapped ranges still intersect (possible only for unaligned, cross-granularity base partitions),
 *       it falls back to the eager mapper so the output is never weaker than the default.</li>
 * </ul>
 */
public class MVMirrorRangePartitionMapper extends MVRangePartitionMapper {
    public static final MVMirrorRangePartitionMapper INSTANCE = new MVMirrorRangePartitionMapper();

    @Override
    public PCellSortedSet toMappingRanges(PCellSortedSet baseRangeMap,
                                          String granularity,
                                          PrimitiveType partitionType) {
        List<Range<PartitionKey>> snappedRanges = Lists.newArrayList();
        for (PCellWithName rangeEntry : baseRangeMap.getPartitions()) {
            PRangeCell rangeCell = rangeEntry.cell().cast();
            // Snap the base partition's endpoints to the mv granularity, keeping it as a single mv partition.
            // The lazy mapper's single-range method is exactly this snap logic (lower floor / upper ceil with
            // MIN/MAX handling), so reuse it here instead of duplicating it.
            PartitionMapping mapping = MVLazyRangePartitionMapper.INSTANCE.toMappingRanges(
                    rangeCell.getRange(), granularity);
            try {
                PartitionKey lowerKey = toPartitionKey(mapping.getLowerDateTime(), partitionType);
                PartitionKey upperKey = toPartitionKey(mapping.getUpperDateTime(), partitionType);
                snappedRanges.add(Range.closedOpen(lowerKey, upperKey));
            } catch (AnalysisException e) {
                throw new SemanticException("Convert to PartitionMapping failed:", e);
            }
        }
        if (snappedRanges.isEmpty()) {
            return PCellSortedSet.of();
        }

        // sort by lower bound then upper bound, then de-duplicate and check no two ranges intersect
        snappedRanges.sort(Comparator.comparing((Range<PartitionKey> range) -> range.lowerEndpoint())
                .thenComparing((Range<PartitionKey> range) -> range.upperEndpoint()));
        PCellSortedSet result = PCellSortedSet.of();
        Range<PartitionKey> prev = null;
        for (Range<PartitionKey> range : snappedRanges) {
            if (prev != null) {
                int lowerCmp = range.lowerEndpoint().compareTo(prev.lowerEndpoint());
                if (lowerCmp == 0) {
                    // same lower bound: equal upper bound is a duplicate, otherwise they intersect
                    if (range.upperEndpoint().compareTo(prev.upperEndpoint()) == 0) {
                        continue;
                    }
                    return MVEagerRangePartitionMapper.INSTANCE.toMappingRanges(baseRangeMap, granularity, partitionType);
                }
                if (prev.upperEndpoint().compareTo(range.lowerEndpoint()) > 0) {
                    // non-aligned / overlapping input: fall back to eager to guarantee non-intersected output
                    return MVEagerRangePartitionMapper.INSTANCE.toMappingRanges(baseRangeMap, granularity, partitionType);
                }
            }
            prev = range;
            DateLiteral lowerDate = (DateLiteral) range.lowerEndpoint().getKeys().get(0);
            DateLiteral upperDate = (DateLiteral) range.upperEndpoint().getKeys().get(0);
            String mvPartitionName = getMVPartitionName(
                    lowerDate.toLocalDateTime(), upperDate.toLocalDateTime(), granularity);
            result.add(mvPartitionName, PRangeCell.of(range));
        }
        return result;
    }
}
