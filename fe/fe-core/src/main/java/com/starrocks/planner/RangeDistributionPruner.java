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

import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.sql.ast.expression.LiteralExpr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RangeDistributionPruner implements DistributionPruner {
    // tablets in order
    private final List<Tablet> tabletInOrder;
    // range distribution columns
    private final List<Column> rangeDistributionColumns;
    // distribution column filters
    private final Map<String, PartitionColumnFilter> distributionColumnFilters;

    public RangeDistributionPruner(List<Tablet> tabletInOrder,
                                   List<Column> rangeDistributionColumns,
                                   Map<String, PartitionColumnFilter> distributionColumnFilters) {
        this.tabletInOrder = tabletInOrder;
        this.rangeDistributionColumns = rangeDistributionColumns;
        this.distributionColumnFilters = distributionColumnFilters;
    }

    @Override
    public Collection<Long> prune() {
        if (distributionColumnFilters == null || distributionColumnFilters.isEmpty()) {
            return tabletInOrder.stream().map(Tablet::getId).collect(Collectors.toSet());
        }

        List<List<Variant>> lowerValuesLists = new ArrayList<>();
        List<List<Variant>> upperValuesLists = new ArrayList<>();

        int complex = 1;
        for (Column column : rangeDistributionColumns) {
            PartitionColumnFilter filter = distributionColumnFilters.get(column.getName());
            // No filter on this column => prune range by the prefix keys
            if (filter == null) {
                break;
            }

            List<Variant> lowerValues = new ArrayList<>();
            List<Variant> upperValues = new ArrayList<>();

            // 1. IN predicate literals (point queries)
            List<LiteralExpr> inPredicateLiterals = filter.getInPredicateLiterals();
            if (inPredicateLiterals != null && !inPredicateLiterals.isEmpty()) {
                for (LiteralExpr expr : inPredicateLiterals) {
                    Variant v = Variant.of(column.getType(), expr.getStringValue());
                    lowerValues.add(v);
                    upperValues.add(v);
                }
            }

            // 2. Range bounds (>, >=, <, <=, BETWEEN, =)
            Variant colMin = null;
            Variant colMax = null;
            LiteralExpr lowerBound = filter.getLowerBound();
            if (lowerBound != null) {
                colMin = Variant.of(column.getType(), lowerBound.getStringValue());
            }

            LiteralExpr upperBound = filter.getUpperBound();
            if (upperBound != null) {
                colMax = Variant.of(column.getType(), upperBound.getStringValue());
            }

            // 3. Fill unbounded sides with type-wide min/max
            boolean hasRangeBound = colMin != null || colMax != null;
            if (colMin == null) {
                colMin = Variant.minVariant(column.getType());
            }
            if (colMax == null) {
                colMax = Variant.maxVariant(column.getType());
            }
            if (hasRangeBound) {
                lowerValues.add(colMin);
                upperValues.add(colMax);
            } else if (lowerValues.isEmpty() && upperValues.isEmpty()) {
                // no any predicate on this column => prune range by the prefix keys
                break;
            }

            complex = complex * lowerValues.size();
            if (complex > Config.max_distribution_pruner_recursion_depth) {
                // prevent combinatorial explosion, early stop
                break;
            }
            lowerValuesLists.add(lowerValues);
            upperValuesLists.add(upperValues);
        }

        // fast path: no range pruning is needed
        if (lowerValuesLists.isEmpty() && upperValuesLists.isEmpty()) {
            return tabletInOrder.stream().map(Tablet::getId).collect(Collectors.toSet());
        }

        List<Variant> currentLowerValue = new ArrayList<>();
        List<Variant> currentUpperValue = new ArrayList<>();
        return prune(0, lowerValuesLists, upperValuesLists, currentLowerValue, currentUpperValue);
    }

    private Collection<Long> prune(int columnIdx, List<List<Variant>> lowerValuesLists,
                                   List<List<Variant>> upperValuesLists,
                                   List<Variant> currentLowerValue,
                                   List<Variant> currentUpperValue) {
        // lowerValuesLists / upperValuesLists size may less than rangeDistributionColumns size
        if (columnIdx == lowerValuesLists.size()) {
            Tuple lowerTuple = new Tuple(currentLowerValue);
            Tuple upperTuple = new Tuple(currentUpperValue);
            // For simplicity, we use the inclusive range [lowerTuple, upperTuple] to represent the query range.
            Range<Tuple> queryRange = Range.of(lowerTuple, upperTuple, true, true);

            Set<Long> result = Sets.newHashSet();
            for (Tablet tablet : tabletInOrder) {
                Range<Tuple> tabletRange = tablet.getRange();
                // If range information is missing, keep the tablet
                if (tabletRange == null) {
                    result.add(tablet.getId());
                    continue;
                }
                if (tabletRange.isOverlapping(queryRange)) {
                    result.add(tablet.getId());
                }
            }
            return result;
        }

        List<Variant> candidateLowerValues = lowerValuesLists.get(columnIdx);
        List<Variant> candidateUpperValues = upperValuesLists.get(columnIdx);
        Set<Long> resultSet = Sets.newHashSet();
        for (int i = 0; i < candidateLowerValues.size(); i++) {
            currentLowerValue.add(candidateLowerValues.get(i));
            currentUpperValue.add(candidateUpperValues.get(i));
            resultSet.addAll(prune(columnIdx + 1, lowerValuesLists, upperValuesLists, currentLowerValue,
                    currentUpperValue));
            currentLowerValue.remove(currentLowerValue.size() - 1);
            currentUpperValue.remove(currentUpperValue.size() - 1);

            if (resultSet.size() >= tabletInOrder.size()) {
                break;
            }
        }
        return resultSet;
    }
}