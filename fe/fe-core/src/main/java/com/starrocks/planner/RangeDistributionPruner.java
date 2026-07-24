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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class RangeDistributionPruner implements DistributionPruner {
    // tablets in range order; unused (empty) when degradeToFullScan is true
    private final TreeMap<Range<Tuple>, Long> tabletInOrder;
    // all tablet ids captured from the input, in input order; used as the full-scan fallback result
    private final List<Long> allTabletIds;
    // true when a captured tablet range's arity doesn't match rangeDistributionColumns; a concurrent
    // metadata-only flip can transiently mix old-arity and new-arity tablet ranges, so we fall back to
    // scanning every tablet instead of asserting or silently dropping tablets
    private final boolean degradeToFullScan;
    // range distribution columns
    private final List<Column> rangeDistributionColumns;
    // distribution column filters
    private final Map<String, PartitionColumnFilter> distributionColumnFilters;

    public RangeDistributionPruner(List<Tablet> tabletsInOrder,
                                   List<Column> rangeDistributionColumns,
                                   Map<String, PartitionColumnFilter> distributionColumnFilters) {
        this.rangeDistributionColumns = rangeDistributionColumns;
        this.distributionColumnFilters = distributionColumnFilters;

        // Single pass: capture each tablet's id and range (never call Tablet#getRange() twice) and check
        // its arity in the same iteration, adding the id BEFORE the arity check and never breaking on a
        // mismatch (only flag `degrade`). So a concurrent flip can't swap the range reference between
        // capture and use, and a mismatch found partway through never truncates the id list already
        // captured. The TreeMap is built only after the loop completes.
        List<Long> ids = new ArrayList<>(tabletsInOrder.size());
        List<Range<Tuple>> ranges = new ArrayList<>(tabletsInOrder.size());
        boolean degrade = false;
        for (Tablet tablet : tabletsInOrder) {
            TabletRange tabletRange = tablet.getRange();
            Preconditions.checkState(tabletRange != null && tabletRange.getRange() != null, "Tablet range is null");
            Range<Tuple> range = tabletRange.getRange();
            ids.add(tablet.getId());
            ranges.add(range);

            if (!range.isAll()) {
                int rangeColumnCount = 0;
                if (!range.isMinimum()) {
                    rangeColumnCount = range.getLowerBound().getValues().size();
                } else if (!range.isMaximum()) {
                    rangeColumnCount = range.getUpperBound().getValues().size();
                }
                if (rangeColumnCount != rangeDistributionColumns.size()) {
                    degrade = true;
                }
            }
        }

        this.allTabletIds = ids;
        this.degradeToFullScan = degrade;
        TreeMap<Range<Tuple>, Long> tree = new TreeMap<>();
        if (!degrade) {
            for (int i = 0; i < ids.size(); i++) {
                tree.put(ranges.get(i), ids.get(i));
            }
        }
        this.tabletInOrder = tree;
    }

    @Override
    public Collection<Long> prune() {
        if (degradeToFullScan) {
            return new ArrayList<>(allTabletIds);
        }

        if (distributionColumnFilters == null || distributionColumnFilters.isEmpty() ||
            (tabletInOrder.size() == 1 && tabletInOrder.firstEntry().getKey().isAll())) {
            return new ArrayList<>(tabletInOrder.values());
        }

        List<List<Variant>> lowerValuesList = new ArrayList<>();
        List<List<Variant>> upperValuesList = new ArrayList<>();

        List<List<Boolean>> lowerValuesInclusiveList = new ArrayList<>();
        List<List<Boolean>> upperValuesInclusiveList = new ArrayList<>();

        int complex = 1;
        for (Column column : rangeDistributionColumns) {
            PartitionColumnFilter filter = distributionColumnFilters.get(column.getName());
            // No filter / function call on this column => prune range by the prefix keys
            if (filter == null || filter.isFromFunctionCall()) {
                break;
            }

            List<Variant> lowerValues = new ArrayList<>();
            List<Variant> upperValues = new ArrayList<>();
            List<Boolean> lowerValuesInclusive = new ArrayList<>();
            List<Boolean> upperValuesInclusive = new ArrayList<>();

            // 1. IN predicate literals (point queries)
            List<LiteralExpr> inPredicateLiterals = filter.getInPredicateLiterals();
            if (inPredicateLiterals != null && !inPredicateLiterals.isEmpty()) {
                for (LiteralExpr expr : inPredicateLiterals) {
                    // A NULL entry in an IN list never matches any row (SQL three-valued logic), so
                    // it selects no tablets -- skip it. This also avoids parsing the literal "NULL"
                    // as the column type.
                    if (expr instanceof NullLiteral) {
                        continue;
                    }
                    Variant v = Variant.of(column.getType(), expr.getStringValue());
                    lowerValues.add(v);
                    upperValues.add(v);
                    lowerValuesInclusive.add(true);
                    upperValuesInclusive.add(true);
                }
            }

            // 2. Range bounds (>, >=, <, <=, BETWEEN, =)
            Variant colMin = null;
            Variant colMax = null;
            LiteralExpr lowerBound = filter.getLowerBound();
            if (lowerBound != null) {
                colMin = toVariant(column.getType(), lowerBound);
            }

            LiteralExpr upperBound = filter.getUpperBound();
            if (upperBound != null) {
                colMax = toVariant(column.getType(), upperBound);
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
                lowerValuesInclusive.add(filter.lowerBoundInclusive);
                upperValuesInclusive.add(filter.upperBoundInclusive);
            } else if (lowerValues.isEmpty() && upperValues.isEmpty()) {
                // no any predicate on this column => prune range by the prefix keys
                break;
            }

            complex = complex * lowerValues.size();
            if (complex > Config.max_distribution_pruner_recursion_depth) {
                // prevent combinatorial explosion, early stop
                break;
            }
            lowerValuesList.add(lowerValues);
            upperValuesList.add(upperValues);
            lowerValuesInclusiveList.add(lowerValuesInclusive);
            upperValuesInclusiveList.add(upperValuesInclusive);
        }

        // fast path: no range pruning is needed
        if (lowerValuesList.isEmpty() && upperValuesList.isEmpty()) {
            return new ArrayList<>(tabletInOrder.values());
        }

        List<Variant> currentLowerValue = new ArrayList<>();
        List<Variant> currentUpperValue = new ArrayList<>();
        List<Boolean> currentLowerValueInclusive = new ArrayList<>();
        List<Boolean> currentUpperValueInclusive = new ArrayList<>();
        return prune(0, lowerValuesList, upperValuesList, lowerValuesInclusiveList, upperValuesInclusiveList,
                     currentLowerValue, currentUpperValue, currentLowerValueInclusive, currentUpperValueInclusive);
    }

    // Prune logic here is very similar to the one in RangePartitionPruner. Basically, it transaform sql range predicates
    // into Range<Tuple> and then use binary search to find the overlapping tablets. But this transformation is not equivalent
    // to the sql range predicates, for example: 1 <= c1 <= 2 AND 10 <= c2 <= 20 is subset of (1, 10) <= (c1, c2) <= (2, 20)
    // But for simplicity, we still use the transformation to prune the tablets here and optimize it later.
    // TODO: optimize the pruning logic by Skip-Scan
    private Collection<Long> prune(int columnIdx, List<List<Variant>> lowerValuesList,
                                   List<List<Variant>> upperValuesList,
                                   List<List<Boolean>> lowerValuesInclusiveList,
                                   List<List<Boolean>> upperValuesInclusiveList,
                                   List<Variant> currentLowerValue,
                                   List<Variant> currentUpperValue,
                                   List<Boolean> currentLowerValueInclusive,
                                   List<Boolean> currentUpperValueInclusive) {
        // lowerValuesList / upperValuesList size may less than rangeDistributionColumns size
        if (columnIdx == lowerValuesList.size()) {
            int prefixKeyLength = columnIdx;
            // Fill remaining columns with [-inf, +inf] to ensure the query range matches the tablet range length
            for (int i = prefixKeyLength; i < rangeDistributionColumns.size(); i++) {
                Column col = rangeDistributionColumns.get(i);
                currentLowerValue.add(Variant.minVariant(col.getType()));
                currentUpperValue.add(Variant.maxVariant(col.getType()));
                // Fill remaining columns with inclusive to ensure the filling values do not affect the judgement of
                // inclusive flag of the query range
                currentLowerValueInclusive.add(true);
                currentUpperValueInclusive.add(true);
            }

            Tuple lowerTuple = new Tuple(currentLowerValue);
            Tuple upperTuple = new Tuple(currentUpperValue);
            boolean lowerInclusive = currentLowerValueInclusive.stream().allMatch(Boolean::booleanValue);
            boolean upperInclusive = currentUpperValueInclusive.stream().allMatch(Boolean::booleanValue);
            Range<Tuple> queryRange = Range.of(lowerTuple, upperTuple, lowerInclusive, upperInclusive);

            Set<Long> result = Sets.newHashSet();
            NavigableMap<Range<Tuple>, Long> subMap;
            Entry<Range<Tuple>, Long> lower = tabletInOrder.lowerEntry(queryRange);
            Entry<Range<Tuple>, Long> upper = tabletInOrder.higherEntry(queryRange);

            if (lower == null && upper == null) {
                subMap = tabletInOrder;
            } else if (lower != null && upper != null) {
                subMap = tabletInOrder.subMap(lower.getKey(), false, upper.getKey(), false);
            } else if (lower != null) {
                subMap = tabletInOrder.tailMap(lower.getKey(), false);
            } else {
                subMap = tabletInOrder.headMap(upper.getKey(), false);
            }

            if (subMap != null && !subMap.isEmpty()) {
                result.addAll(subMap.values());
            }

            for (int i = prefixKeyLength; i < rangeDistributionColumns.size(); i++) {
                currentLowerValue.remove(currentLowerValue.size() - 1);
                currentUpperValue.remove(currentUpperValue.size() - 1);
                currentLowerValueInclusive.remove(currentLowerValueInclusive.size() - 1);
                currentUpperValueInclusive.remove(currentUpperValueInclusive.size() - 1);
            }

            return result;
        }

        List<Variant> candidateLowerValues = lowerValuesList.get(columnIdx);
        List<Variant> candidateUpperValues = upperValuesList.get(columnIdx);
        Set<Long> resultSet = Sets.newHashSet();
        for (int i = 0; i < candidateLowerValues.size(); i++) {
            currentLowerValue.add(candidateLowerValues.get(i));
            currentUpperValue.add(candidateUpperValues.get(i));
            currentLowerValueInclusive.add(lowerValuesInclusiveList.get(columnIdx).get(i));
            currentUpperValueInclusive.add(upperValuesInclusiveList.get(columnIdx).get(i));
            resultSet.addAll(prune(columnIdx + 1,
                    lowerValuesList, upperValuesList,
                    lowerValuesInclusiveList, upperValuesInclusiveList,
                    currentLowerValue, currentUpperValue,
                    currentLowerValueInclusive, currentUpperValueInclusive));
            currentLowerValueInclusive.remove(currentLowerValueInclusive.size() - 1);
            currentUpperValueInclusive.remove(currentUpperValueInclusive.size() - 1);
            currentLowerValue.remove(currentLowerValue.size() - 1);
            currentUpperValue.remove(currentUpperValue.size() - 1);

            if (resultSet.size() >= tabletInOrder.size()) {
                break;
            }
        }
        return resultSet;
    }

    // Convert a range-bound literal to a Variant. An IS NULL predicate reaches the pruner as a
    // NullLiteral bound whose getStringValue() is the string "NULL". A NULL sort-key value is encoded
    // as NullVariant, which sorts as the smallest real value (matching the BE null-first key
    // encoding), so a NULL bound must compare as NullVariant -- passing "NULL" to Variant.of() would
    // parse it as the column type and throw for numeric/date types.
    private static Variant toVariant(Type type, LiteralExpr literal) {
        return literal instanceof NullLiteral ? Variant.nullVariant(type)
                : Variant.of(type, literal.getStringValue());
    }
}
