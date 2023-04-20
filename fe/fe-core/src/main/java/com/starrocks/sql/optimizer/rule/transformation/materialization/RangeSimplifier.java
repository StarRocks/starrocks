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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RangeSimplifier {
    private final List<ScalarOperator> srcPredicates;

    public RangeSimplifier(List<ScalarOperator> srcPredicates) {
        this.srcPredicates = srcPredicates;
    }

    // check whether target range predicates are contained in srcPredicates
    // all ScalarOperator should be BinaryPredicateOperator,
    // left is ColumnRefOperator and right is ConstantOperator
    public ScalarOperator simplify(List<ScalarOperator> targets) {
        try {
            Map<Integer, Range<ConstantOperator>> srcColumnIdToRange = extractColumnIdRangeMapping(srcPredicates);
            if (srcColumnIdToRange == null) {
                return null;
            }
            Map<Integer, Range<ConstantOperator>> targetColumnIdToRange = extractColumnIdRangeMapping(targets);
            if (targetColumnIdToRange == null) {
                return null;
            }

            List<Integer> resultColumnIds = Lists.newArrayList();
            for (Map.Entry<Integer, Range<ConstantOperator>> targetEntry : targetColumnIdToRange.entrySet()) {
                // Source columnId range must contain any target columnId.
                if (!srcColumnIdToRange.containsKey(targetEntry.getKey())
                        && !targetEntry.getValue().hasUpperBound()
                        && !targetEntry.getValue().hasLowerBound()) {
                    return null;
                }
                Range<ConstantOperator> srcRange = srcColumnIdToRange.get(targetEntry.getKey());
                Range<ConstantOperator> targetRange = targetEntry.getValue();
                if (srcRange.equals(targetRange)) {
                    continue;
                } else if (targetRange.encloses(srcRange)) {
                    resultColumnIds.add(targetEntry.getKey());
                } else {
                    // can not be rewritten
                    return null;
                }
            }

            for (Map.Entry<Integer, Range<ConstantOperator>> srcEntry : srcColumnIdToRange.entrySet()) {
                if (!targetColumnIdToRange.containsKey(srcEntry.getKey())) {
                    resultColumnIds.add(srcEntry.getKey());
                }
            }
            if (resultColumnIds.isEmpty()) {
                return ConstantOperator.createBoolean(true);
            } else {
                List<ScalarOperator> rewrittenRangePredicates = Lists.newArrayList();
                for (int columnId : resultColumnIds) {
                    Range<ConstantOperator> columnRange = srcColumnIdToRange.get(columnId);
                    if (isSingleValueRange(columnRange)) {
                        List<ScalarOperator> columnPredicates = srcPredicates.stream().filter(
                                predicate -> isScalarForColumns(predicate, columnId)
                        ).collect(Collectors.toList());
                        Preconditions.checkState(!columnPredicates.isEmpty());
                        BinaryPredicateOperator binary = (BinaryPredicateOperator) columnPredicates.get(0);
                        BinaryPredicateOperator eqBinary = new BinaryPredicateOperator(
                                BinaryPredicateOperator.BinaryType.EQ,
                                binary.getChild(0), columnRange.lowerEndpoint());
                        rewrittenRangePredicates.add(eqBinary);
                    } else {
                        List<ScalarOperator> columnScalars = srcPredicates.stream().filter(
                                predicate -> isScalarForColumns(predicate, columnId)
                        ).collect(Collectors.toList());
                        columnScalars = filterScalarOperators(columnScalars, columnRange);
                        rewrittenRangePredicates.addAll(columnScalars);
                    }
                }
                return Utils.compoundAnd(rewrittenRangePredicates);
            }
        } catch (Exception e) {
            return null;
        }
    }

    private Map<Integer, Range<ConstantOperator>> extractColumnIdRangeMapping(List<ScalarOperator> predicates) {
        Map<Integer, Range<ConstantOperator>> columnIdToRange = Maps.newHashMap();
        for (ScalarOperator rangePredicate : predicates) {
            Preconditions.checkState(rangePredicate instanceof BinaryPredicateOperator);
            Preconditions.checkState(rangePredicate.getChild(0) instanceof ColumnRefOperator);
            Preconditions.checkState(rangePredicate.getChild(1) instanceof ConstantOperator);
            BinaryPredicateOperator srcBinary = (BinaryPredicateOperator) rangePredicate;
            Preconditions.checkState(srcBinary.getBinaryType().isRange() || srcBinary.getBinaryType().isEqual());
            ColumnRefOperator srcColumn = (ColumnRefOperator) srcBinary.getChild(0);
            ConstantOperator srcConstant = (ConstantOperator) srcBinary.getChild(1);
            if (!columnIdToRange.containsKey(srcColumn.getId())) {
                columnIdToRange.put(srcColumn.getId(), Range.all());
            }
            Range<ConstantOperator> columnRange = columnIdToRange.get(srcColumn.getId());
            Range<ConstantOperator> range = range(srcBinary.getBinaryType(), srcConstant);
            columnRange = columnRange.intersection(range);
            if (columnRange.isEmpty()) {
                return null;
            }
            columnIdToRange.put(srcColumn.getId(), columnRange);
        }
        return columnIdToRange;
    }

    private List<ScalarOperator> filterScalarOperators(
            List<ScalarOperator> columnScalars, Range<ConstantOperator> validRange) {
        List<ScalarOperator> results = Lists.newArrayList();
        ;
        for (ScalarOperator candidate : columnScalars) {
            if (isRedundantPredicate(candidate, validRange)) {
                continue;
            }
            results.add(candidate);
        }
        return results;
    }

    private boolean isRedundantPredicate(ScalarOperator scalarOperator, Range<ConstantOperator> validRange) {
        Preconditions.checkState(scalarOperator instanceof BinaryPredicateOperator);
        Preconditions.checkState(scalarOperator.getChild(0) instanceof ColumnRefOperator);
        Preconditions.checkState(scalarOperator.getChild(1) instanceof ConstantOperator);
        BinaryPredicateOperator binary = scalarOperator.cast();
        ConstantOperator right = binary.getChild(1).cast();
        Range predicateRange = range(binary.getBinaryType(), right);
        // only range border's predicate is non redundant
        if (predicateRange.hasLowerBound()
                && validRange.hasLowerBound()
                && predicateRange.lowerBoundType() == validRange.lowerBoundType()
                && predicateRange.lowerEndpoint().equals(validRange.lowerEndpoint())) {
            // predicate is lower border
            return false;
        }
        if (predicateRange.hasUpperBound()
                && validRange.hasUpperBound()
                && predicateRange.upperBoundType() == validRange.upperBoundType()
                && predicateRange.upperEndpoint().equals(validRange.upperEndpoint())) {
            // predicate is upper border
            return false;
        }
        return true;
    }

    private boolean isScalarForColumns(ScalarOperator predicate, int columnId) {
        BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
        ColumnRefOperator targetColumn = (ColumnRefOperator) binary.getChild(0);
        return targetColumn.getId() == columnId;
    }

    private boolean isSingleValueRange(Range<ConstantOperator> range) {
        // 3 <= range <= 3
        return range.hasLowerBound()
                && range.hasUpperBound()
                && range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.CLOSED
                && range.lowerEndpoint() == range.upperEndpoint();
    }

    private <C extends Comparable<C>> Range<C> range(BinaryPredicateOperator.BinaryType type, C value) {
        switch (type) {
            case EQ:
                return Range.singleton(value);
            case GE:
                return Range.atLeast(value);
            case GT:
                return Range.greaterThan(value);
            case LE:
                return Range.atMost(value);
            case LT:
                return Range.lessThan(value);
            default:
                throw new UnsupportedOperationException("unsupported type:" + type);
        }
    }
}
