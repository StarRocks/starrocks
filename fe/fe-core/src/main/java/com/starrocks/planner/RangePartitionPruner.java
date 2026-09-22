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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/RangePartitionPruner.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeMap;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangePartitionPruner implements PartitionPruner {
    private static final Logger LOG = LogManager.getLogger(RangePartitionPruner.class);

    private Map<Long, Range<PartitionKey>> partitionRangeMap;
    private List<Column> partitionColumns;
    private Map<String, PartitionColumnFilter> partitionColumnFilters;

    public RangePartitionPruner(Map<Long, Range<PartitionKey>> rangeMap,
                                List<Column> columns,
                                Map<String, PartitionColumnFilter> filters) {
        partitionRangeMap = rangeMap;
        partitionColumns = columns;
        partitionColumnFilters = filters;
    }

    private List<Long> prune(RangeMap<PartitionKey, Long> rangeMap,
                             int columnIdx,
                             PartitionKey minKey,
                             PartitionKey maxKey,
                             int complex)
            throws AnalysisException {
        LOG.debug("column idx {}, column filters {}", columnIdx, partitionColumnFilters);
        // the last column in partition Key
        if (columnIdx == partitionColumns.size()) {
            try {
                return Lists.newArrayList(rangeMap.subRangeMap(Range.closed(minKey, maxKey)).asMapOfRanges().values());
            } catch (IllegalArgumentException e) {
                return Lists.newArrayList();
            }
        }
        // no filter in this column
        Column keyColumn = partitionColumns.get(columnIdx);
        PartitionColumnFilter filter = partitionColumnFilters.get(keyColumn.getName());
        if (null == filter) {
            minKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getPrimitiveType()), false),
                    keyColumn.getPrimitiveType());
            maxKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getPrimitiveType()), true),
                    keyColumn.getPrimitiveType());
            List<Long> result;
            try {
                result = Lists.newArrayList(
                        rangeMap.subRangeMap(Range.closed(minKey, maxKey)).asMapOfRanges().values());
            } catch (IllegalArgumentException e) {
                result = Lists.newArrayList();
            }
            minKey.popColumn();
            maxKey.popColumn();
            return result;
        }
        List<LiteralExpr> inPredicateLiterals = filter.getInPredicateLiterals();
        int inPredicateMaxLen = 100;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            inPredicateMaxLen = ConnectContext.get().getSessionVariable().getRangePrunerPredicateMaxLen();
        }

        boolean isConvertToDate = PartitionUtil.isConvertToDate(keyColumn, filter);
        LiteralExpr lowerBound = filter.getLowerBound();
        LiteralExpr upperBound = filter.getUpperBound();
        if (null == inPredicateLiterals || ((lowerBound != null || upperBound != null) &&
                inPredicateLiterals.size() * complex > inPredicateMaxLen)) {
            if (filter.lowerBoundInclusive && filter.upperBoundInclusive
                    && lowerBound != null && upperBound != null
                    && 0 == lowerBound.compareLiteral(upperBound)) {

                // eg: [10, 10], [null, null]
                if (lowerBound instanceof NullLiteral && upperBound instanceof NullLiteral) {
<<<<<<< HEAD
                    // replace Null with min value
                    LiteralExpr minKeyValue = LiteralExpr.createInfinity(
                            Type.fromPrimitiveType(keyColumn.getPrimitiveType()), false);
                    minKey.pushColumn(minKeyValue, keyColumn.getPrimitiveType());
                    maxKey.pushColumn(minKeyValue, keyColumn.getPrimitiveType());
                } else {
                    LiteralExpr lowerBoundExpr = filter.getLowerBound(isConvertToDate);
                    LiteralExpr upperBoundExpr = filter.getUpperBound(isConvertToDate);
                    minKey.pushColumn(lowerBoundExpr, keyColumn.getPrimitiveType());
                    maxKey.pushColumn(upperBoundExpr, keyColumn.getPrimitiveType());
=======
                    return pruneIsNull(rangeMap, columnIdx, minKey);
>>>>>>> cccf7a5 ([BugFix] Look up a NULL partition key below the minimum, not at it (#79257))
                }
                LiteralExpr lowerBoundExpr = filter.getLowerBound(isConvertToDate);
                LiteralExpr upperBoundExpr = filter.getUpperBound(isConvertToDate);
                minKey.pushColumn(lowerBoundExpr, keyColumn.getPrimitiveType());
                maxKey.pushColumn(upperBoundExpr, keyColumn.getPrimitiveType());
                List<Long> result = prune(rangeMap, columnIdx + 1, minKey, maxKey, complex);
                minKey.popColumn();
                maxKey.popColumn();
                return result;
            }

            // no in predicate
            BoundType lowerType = filter.lowerBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
            BoundType upperType = filter.upperBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
            int pushMinCount = 0;
            int pushMaxCount = 0;
            int lastColumnId = partitionColumns.size() - 1;
            if (lowerBound != null) {
                minKey.pushColumn(filter.getLowerBound(isConvertToDate), keyColumn.getPrimitiveType());
                pushMinCount++;
                if (filter.lowerBoundInclusive && columnIdx != lastColumnId) {
                    Column column = partitionColumns.get(columnIdx + 1);
                    Type type = Type.fromPrimitiveType(column.getPrimitiveType());
                    minKey.pushColumn(LiteralExpr.createInfinity(type, false), column.getPrimitiveType());
                    pushMinCount++;
                }
            } else {
                Type type = Type.fromPrimitiveType(keyColumn.getPrimitiveType());
                minKey.pushColumn(LiteralExpr.createInfinity(type, false), keyColumn.getPrimitiveType());
                pushMinCount++;
            }
            if (upperBound != null) {
                maxKey.pushColumn(filter.getUpperBound(isConvertToDate), keyColumn.getPrimitiveType());
                pushMaxCount++;
                if (filter.upperBoundInclusive && columnIdx != lastColumnId) {
                    Column column = partitionColumns.get(columnIdx + 1);
                    maxKey.pushColumn(
                            LiteralExpr.createInfinity(Type.fromPrimitiveType(column.getPrimitiveType()), true),
                            column.getPrimitiveType());
                    pushMaxCount++;
                }
            } else {
                maxKey.pushColumn(
                        LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getPrimitiveType()), true),
                        keyColumn.getPrimitiveType());
                pushMaxCount++;
            }

            List<Long> result;
            try {
                result = Lists.newArrayList(rangeMap.subRangeMap(
                        Range.range(minKey, lowerType, maxKey, upperType)).asMapOfRanges().values());
            } catch (IllegalArgumentException e) {
                result = Lists.newArrayList();
            }

            for (; pushMinCount > 0; pushMinCount--) {
                minKey.popColumn();
            }
            for (; pushMaxCount > 0; pushMaxCount--) {
                maxKey.popColumn();
            }
            return result;
        }
        Set<Long> resultSet = Sets.newHashSet();
        int newComplex = inPredicateLiterals.size() * complex;
        for (LiteralExpr expr : inPredicateLiterals) {
            if (isConvertToDate) {
                LiteralExpr convertExpr = PartitionUtil.convertToDateLiteral(expr);
                Preconditions.checkState(convertExpr.getType().equals(keyColumn.getType()));
                minKey.pushColumn(convertExpr, keyColumn.getPrimitiveType());
                maxKey.pushColumn(convertExpr, keyColumn.getPrimitiveType());
            } else {
                Preconditions.checkState(expr.getType().equals(keyColumn.getType()));
                minKey.pushColumn(expr, keyColumn.getPrimitiveType());
                maxKey.pushColumn(expr, keyColumn.getPrimitiveType());
            }
            Collection<Long> subList = prune(rangeMap, columnIdx + 1, minKey, maxKey, newComplex);
            resultSet.addAll(subList);
            minKey.popColumn();
            maxKey.popColumn();
        }

        return new ArrayList<>(resultSet);
    }

    // `col IS NULL` does not mean `col = MIN`. A NULL sorts strictly below every value of its own
    // column, and the row is then placed by ordinary tuple order: on RANGE(a, b) with
    // p0 = [MIN, (5,-128)) and p1 = [(5,-128), (5,10)), the row (5, NULL) lands in p0 while
    // (40, NULL) lands in the partition holding (40, ...). Looking the key up as `prefix + MIN`
    // therefore reads one partition too high whenever a partition boundary falls exactly on
    // `prefix + MIN` -- the rows are just below that boundary, the lookup lands just above it, and
    // the partition actually holding them is pruned away. So take the partitions below the
    // boundary instead, with the upper end open so the partition that starts on it is excluded.
    private List<Long> pruneIsNull(RangeMap<PartitionKey, Long> rangeMap, int columnIdx, PartitionKey prefix)
            throws AnalysisException {
        PartitionKey boundary = new PartitionKey(Lists.newArrayList(prefix.getKeys()),
                Lists.newArrayList(prefix.getTypes()));
        for (int i = columnIdx; i < partitionColumns.size(); i++) {
            Column column = partitionColumns.get(i);
            boundary.pushColumn(
                    LiteralExprFactory.createInfinity(TypeFactory.createType(column.getPrimitiveType()), false),
                    column.getPrimitiveType());
        }
        PartitionKey globalMin = PartitionKey.createInfinityPartitionKey(partitionColumns, false);
        try {
            // A leading IS NULL puts the boundary at the global minimum, and there is nothing below
            // it to select: the first partition's lower endpoint is that same key standing in for
            // -infinity, so the NULL rows sit inside it rather than beneath it. Ask for the
            // partition holding that key.
            if (boundary.compareTo(globalMin) <= 0) {
                return Lists.newArrayList(
                        rangeMap.subRangeMap(Range.closed(globalMin, globalMin)).asMapOfRanges().values());
            }
            // Everything below the boundary, of which only the last entry can hold the NULLs: the
            // ranges are disjoint and ascending, so that is the one covering the key immediately
            // below it. Keeping the rest would be sound but would scan partitions that cannot match.
            Collection<Long> below =
                    rangeMap.subRangeMap(Range.closedOpen(globalMin, boundary)).asMapOfRanges().values();
            return below.isEmpty() ? Lists.newArrayList() : Lists.newArrayList(Iterables.getLast(below));
        } catch (IllegalArgumentException e) {
            return Lists.newArrayList();
        }
    }

    public List<Long> prune() throws AnalysisException {
        PartitionKey minKey = new PartitionKey();
        PartitionKey maxKey = new PartitionKey();
        // Map to RangeMapTree
        RangeMap<PartitionKey, Long> rangeMap = TreeRangeMap.create();
        for (Map.Entry<Long, Range<PartitionKey>> entry : partitionRangeMap.entrySet()) {
            rangeMap.put(entry.getValue(), entry.getKey());
        }
        return prune(rangeMap, 0, minKey, maxKey, 1);
    }
}
