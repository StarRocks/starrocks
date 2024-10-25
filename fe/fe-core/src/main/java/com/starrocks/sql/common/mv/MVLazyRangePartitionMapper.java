// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.PartitionMapping;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.common.SyncPartitionUtils.convertToDatePartitionRange;
import static com.starrocks.sql.common.SyncPartitionUtils.getLowerDateTime;
import static com.starrocks.sql.common.SyncPartitionUtils.getMVPartitionName;
import static com.starrocks.sql.common.SyncPartitionUtils.getUpperDateTime;
import static com.starrocks.sql.common.SyncPartitionUtils.toPartitionKey;

/**
 * {@link MVLazyRangePartitionMapper} will create mv partition ranges by the granularity of mv partition expr lazily, it first
 * deduces partitions by the partition ranges of the base table, then maps/unrolls the base table partition range by the
 * granularity of mv.
 *
 * eg:
 *  base table partition range  : [2021-01-01, 2021-01-03),
 *  mv partition expr           : date_trunc('day', dt)
 *
 *  mv's partition map result   :
 *                           p0 : [2021-01-01, 2021-01-03),
 * rather than:
 *                           p0 : [2021-01-01, 2021-01-02),
 *                           p1 : [2021-01-02, 2021-01-03),
 * Lazy mode will generate less partition ranges, but it may generate intersected partition ranges which we will handle it in
 * {@link com.starrocks.sql.common.PartitionDiffer#computePartitionRangeDiff}.
 */
public class MVLazyRangePartitionMapper extends MVRangePartitionMapper {
    public static final MVLazyRangePartitionMapper INSTANCE = new MVLazyRangePartitionMapper();

    @Override
    public Map<String, Range<PartitionKey>> toMappingRanges(Map<String, Range<PartitionKey>> baseRangeMap,
                                                            String granularity,
                                                            PrimitiveType partitionType) {
        Set<LocalDateTime> timePointSet = Sets.newTreeSet();
        for (Map.Entry<String, Range<PartitionKey>> rangeEntry : baseRangeMap.entrySet()) {
            PartitionMapping mappedRange = toMappingRanges(rangeEntry.getValue(), granularity);
            // this mappedRange may exist range overlap
            timePointSet.add(mappedRange.getLowerDateTime());
            timePointSet.add(mappedRange.getUpperDateTime());
        }
        List<LocalDateTime> timePointList = Lists.newArrayList(timePointSet);
        // deal overlap
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        if (timePointList.size() < 2) {
            return result;
        }
        for (int i = 1; i < timePointList.size(); i++) {
            try {
                LocalDateTime lowerDateTime = timePointList.get(i - 1);
                LocalDateTime upperDateTime = timePointList.get(i);
                PartitionKey lowerPartitionKey = toPartitionKey(lowerDateTime, partitionType);
                PartitionKey upperPartitionKey = toPartitionKey(upperDateTime, partitionType);
                String mvPartitionName = getMVPartitionName(lowerDateTime, upperDateTime, granularity);
                result.put(mvPartitionName, Range.closedOpen(lowerPartitionKey, upperPartitionKey));
            } catch (AnalysisException ex) {
                throw new SemanticException("Convert to DateLiteral failed:", ex);
            }
        }
        return result;
    }

    public PartitionMapping toMappingRanges(Range<PartitionKey> baseRange,
                                            String granularity) {
        // assume expr partition must be DateLiteral and only one partition
        baseRange = convertToDatePartitionRange(baseRange);
        LiteralExpr lowerExpr = baseRange.lowerEndpoint().getKeys().get(0);
        LiteralExpr upperExpr = baseRange.upperEndpoint().getKeys().get(0);
        Preconditions.checkArgument(lowerExpr instanceof DateLiteral);
        DateLiteral lowerDate = (DateLiteral) lowerExpr;
        LocalDateTime lowerDateTime = lowerDate.toLocalDateTime();
        LocalDateTime truncLowerDateTime = getLowerDateTime(lowerDateTime, granularity);

        DateLiteral upperDate;
        LocalDateTime truncUpperDateTime;
        if (upperExpr instanceof MaxLiteral) {
            upperDate = new DateLiteral(Type.DATE, true);
            truncUpperDateTime = upperDate.toLocalDateTime();
        } else {
            upperDate = (DateLiteral) upperExpr;
            truncUpperDateTime = getUpperDateTime(upperDate.toLocalDateTime(), granularity);
        }
        return new PartitionMapping(truncLowerDateTime, truncUpperDateTime);
    }
}
