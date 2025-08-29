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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.PartitionMapping;
import com.starrocks.sql.common.SyncPartitionUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.common.SyncPartitionUtils.convertToDatePartitionRange;
import static com.starrocks.sql.common.SyncPartitionUtils.getLowerDateTime;
import static com.starrocks.sql.common.SyncPartitionUtils.getMVPartitionName;
import static com.starrocks.sql.common.SyncPartitionUtils.toPartitionKey;

/**
 * {@link MVEagerRangePartitionMapper} will map/unroll the base table partition range by the granularity of mv partition expr.
 * eg:
 *  base table partition range  : [2021-01-01, 2021-01-03),
 *  mv partition expr           : date_trunc('day', dt)
 *
 *  mv's partition map result   :
 *                           p0 : [2021-01-01, 2021-01-02),
 *                           p1 : [2021-01-02, 2021-01-03),
 * Eager mode will generate more partitions than lazy mode, but it's more accurate and it will not generate intersected
 * partition ranges.
 */
public class MVEagerRangePartitionMapper extends MVRangePartitionMapper {
    public static final MVEagerRangePartitionMapper INSTANCE = new MVEagerRangePartitionMapper();

    @Override
    public PartitionRangeWrapper toMappingRanges(Map<String, Range<PartitionKey>> baseRangeMap,
                                                 String granularity, PrimitiveType partitionType,
                                                 MaterializedView mv) {

        Set<PartitionMapping> mappings = Sets.newHashSet();
        for (Map.Entry<String, Range<PartitionKey>> rangeEntry : baseRangeMap.entrySet()) {
            List<PartitionMapping> rangeMappings = toMappingRanges(rangeEntry.getValue(), granularity);
            mappings.addAll(rangeMappings);
        }

        // Generate the virtual partition mapping
        Set<PartitionMapping> virtualPartitionMapping = generateVirtualPartitionMapping(granularity, mappings, mv);
        mappings.addAll(virtualPartitionMapping);

        Map<String, Range<PartitionKey>> result = generatePartitionRange(granularity, partitionType, mappings);
        Map<String, Range<PartitionKey>> virtualPartitionRangeMap = generatePartitionRange(granularity, partitionType,
                virtualPartitionMapping);
        return new PartitionRangeWrapper(result, virtualPartitionRangeMap);
    }

    /**
     * Generates virtual partition mappings for a mv.
     *
     * Key processing steps:
     * 1. Extract and validate partition information from the materialized view
     * 2. Analyze function expressions to validate granularity and collect range information
     * 3. Determine the maximum range and starting point for mapping generation
     * 4. Generate virtual partition mappings using the determined parameters
     *
     * @param granularity      The time unit granularity (e.g., "day", "month", "year")
     * @param originMappings   The original set of partition mappings to base calculations on
     * @param mv               The materialized view containing partition expressions
     * @return                 A set of generated virtual partition mappings, empty if invalid conditions
     * @throws SemanticException If an unsupported function is encountered
     */
    private Set<PartitionMapping> generateVirtualPartitionMapping(String granularity,
                                                                   Set<PartitionMapping> originMappings,
                                                                   MaterializedView mv) {
        Set<PartitionMapping> result = Sets.newHashSet();
        if (mv == null || CollectionUtils.isEmpty(originMappings)
                || CollectionUtils.isEmpty(mv.getUnionOtherOutputExpression())) {
            return result;
        }
        SlotRef mvPartitionSlotRef = mv.getPartitionExprMaps().entrySet().stream()
                .findFirst()
                .map(Map.Entry::getValue)
                .orElse(null);
        if (mvPartitionSlotRef == null) {
            return result;
        }

        Map<String, List<Integer>> func2Range = new HashMap<>();
        for (Expr unionExpr : mv.getUnionOtherOutputExpression()) {
            if (!(unionExpr instanceof FunctionCallExpr functionCallExpr)) {
                continue;
            }
            if (!functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                continue;
            }

            for (Expr child : functionCallExpr.getChildren()) {
                if (child instanceof StringLiteral granularityLiteral) {
                    if (!granularityLiteral.getStringValue().equalsIgnoreCase(granularity)) {
                        return result;
                    }
                } else if (child instanceof TimestampArithmeticExpr timestampArithmeticExpr) {
                    String functionName = timestampArithmeticExpr.getFuncName();
                    for (Expr timestampArithmeticExprChild : timestampArithmeticExpr.getChildren()) {
                        if (timestampArithmeticExprChild instanceof SlotRef slotRefChild) {
                            if (!slotRefChild.getColumnName().equalsIgnoreCase(mvPartitionSlotRef.getColumnName())) {
                                return result;
                            }
                        } else if (timestampArithmeticExprChild instanceof IntLiteral intLiteralChild) {
                            func2Range.computeIfAbsent(functionName, k -> new ArrayList<>())
                                    .add(Integer.valueOf(intLiteralChild.getStringValue()));
                        }
                    }
                }
            }
        }
        if (func2Range.isEmpty()) {
            return result;
        }

        // Find the max range function
        Map<String, Integer> calculateMaxRanges = calculateMaxRanges(func2Range);
        calculateMaxRanges.forEach((functionName, maxRange) -> {
            // Generate partition mapping set
            // For example, func2Range: DATE_ADD->1, if the original partition mappings are:
            // 2023-10-01 00:00:00, 2023-10-02 00:00:00
            // 2023-10-02 00:00:00, 2023-10-03 00:00:00
            // 2023-10-10 00:00:00, 2023-10-11 00:00:00
            // Generate the virtual partitions:
            // 2023-10-03 00:00:00, 2023-10-04 00:00:00
            // 2023-10-11 00:00:00, 2023-10-12 00:00:00
            for (PartitionMapping originMapping : originMappings) {
                PartitionMapping tempMapping = new PartitionMapping(
                        originMapping.getLowerDateTime(), originMapping.getUpperDateTime());

                for (int i = 0; i < maxRange; i++) {
                    LocalDateTime lowerDateTime = SyncPartitionUtils.nextDateTime(tempMapping.getLowerDateTime(),
                            granularity, functionName);
                    LocalDateTime upperDateTime = SyncPartitionUtils.nextDateTime(tempMapping.getUpperDateTime(),
                            granularity, functionName);
                    PartitionMapping nextMapping = new PartitionMapping(lowerDateTime, upperDateTime);
                    tempMapping = nextMapping;
                    result.add(nextMapping);
                }
            }

        });

        result.removeAll(originMappings);
        return result;
    }

    /**
     * Calculates the maximum range value for each function in the given map.
     *
     * Example:
     * Input: {"DATE_ADD": [1, 2, 3], "DATE_SUB": [5, 6]}
     * Output: {"DATE_ADD": 3, "DATE_SUB": 6}
     *Add commentMore actions
     * @param func2Range A map where the key is a function name and the value is a list of integers representing ranges.
     * @return A map where the key is a function name and the value is the maximum integer range for that function.
     */
    private Map<String, Integer> calculateMaxRanges(Map<String, List<Integer>> func2Range) {
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry : func2Range.entrySet()) {
            String function = entry.getKey();
            List<Integer> values = entry.getValue();
            int max = values.stream().mapToInt(Integer::intValue).max().orElse(0);
            result.put(function, max);
        }
        return result;
    }

    private Map<String, Range<PartitionKey>> generatePartitionRange(String granularity, PrimitiveType partitionType,
                                                                    Set<PartitionMapping> mappings) {
        Map<String, Range<PartitionKey>> result = Maps.newTreeMap();
        try {
            for (PartitionMapping mappedRange : mappings) {
                LocalDateTime lowerDateTime = mappedRange.getLowerDateTime();
                LocalDateTime upperDateTime = mappedRange.getUpperDateTime();

                // mv partition name
                String mvPartitionName = getMVPartitionName(lowerDateTime, upperDateTime, granularity);
                // mv partition key
                PartitionKey lowerPartitionKey = toPartitionKey(lowerDateTime, partitionType);
                PartitionKey upperPartitionKey = toPartitionKey(upperDateTime, partitionType);
                Range<PartitionKey> range = Range.closedOpen(lowerPartitionKey, upperPartitionKey);
                result.put(mvPartitionName, range);
            }
        } catch (AnalysisException e) {
            throw new SemanticException("Convert to PartitionMapping failed:", e);
        }
        return result;
    }

    private static LocalDateTime toLocalDateTime(LiteralExpr literal) {
        DateLiteral dateLiteral;
        if (literal instanceof MaxLiteral) {
            dateLiteral = new DateLiteral(Type.DATE, true);
            return dateLiteral.toLocalDateTime();
        } else {
            dateLiteral = (DateLiteral) literal;
            return dateLiteral.toLocalDateTime();
        }
    }

    /**
     * Get the mv partition mapping by the base partition range and mv partition granularity.
     * @param baseRange base table partition range which should be date/datetime type
     * @param granularity mv partition granularity
     * @return mv partition mapping with the base partition range's lower bound and upper bound in granularity
     * @throws AnalysisException
     */
    public List<PartitionMapping> toMappingRanges(Range<PartitionKey> baseRange,
                                                  String granularity) {
        // assume expr partition must be DateLiteral and only one partition
        baseRange = convertToDatePartitionRange(baseRange);
        LiteralExpr lowerExpr = baseRange.lowerEndpoint().getKeys().get(0);
        LiteralExpr upperExpr = baseRange.upperEndpoint().getKeys().get(0);
        Preconditions.checkArgument(lowerExpr instanceof DateLiteral);
        LocalDateTime lowerDateTime = toLocalDateTime(lowerExpr);
        LocalDateTime upperDateTime = toLocalDateTime(upperExpr);

        // if the lower bound is min, return directly
        boolean isLowerMin = lowerExpr.isMinValue();
        if (isLowerMin) {
            PartitionMapping nextMapping = new PartitionMapping(lowerDateTime, upperDateTime);
            return Lists.newArrayList(nextMapping);
        }
        // if the upper bound is max, return directly
        LocalDateTime curLowerDateTime = getLowerDateTime(lowerDateTime, granularity);
        boolean isUpperMax = upperExpr instanceof MaxLiteral;
        if (isUpperMax) {
            PartitionMapping nextMapping = new PartitionMapping(curLowerDateTime, upperDateTime);
            return Lists.newArrayList(nextMapping);
        }

        // if the upper bound has covered original upper bound, return directly
        List<PartitionMapping> result = Lists.newArrayList();
        // iterate the current upper bound to be greater than input upper time.
        LocalDateTime curUpperDateTime = curLowerDateTime;
        while (upperDateTime.isAfter(curUpperDateTime)) {
            // compute the next upper bound by current upper bound
            LocalDateTime nextUpperDateTime = SyncPartitionUtils.nextUpperDateTime(curUpperDateTime, granularity);
            PartitionMapping nextMapping = new PartitionMapping(curUpperDateTime, nextUpperDateTime);

            // update curLowerDateTime
            curUpperDateTime = nextUpperDateTime;
            result.add(nextMapping);
        }
        return result;
    }
}
