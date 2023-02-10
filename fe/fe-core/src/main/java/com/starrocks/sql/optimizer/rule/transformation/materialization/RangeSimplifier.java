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
import java.util.concurrent.atomic.AtomicReference;
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
            Map<Integer, Range<ConstantOperator>> srcColumnIdToRange = Maps.newHashMap();
            for (ScalarOperator rangePredicate : srcPredicates) {
                Preconditions.checkState(rangePredicate instanceof BinaryPredicateOperator);
                Preconditions.checkState(rangePredicate.getChild(0) instanceof ColumnRefOperator);
                Preconditions.checkState(rangePredicate.getChild(1) instanceof ConstantOperator);
                BinaryPredicateOperator srcBinary = (BinaryPredicateOperator) rangePredicate;
                Preconditions.checkState(srcBinary.getBinaryType().isRange() || srcBinary.getBinaryType().isEqual());
                ColumnRefOperator srcColumn = (ColumnRefOperator) srcBinary.getChild(0);
                ConstantOperator srcConstant = (ConstantOperator) srcBinary.getChild(1);
                if (!srcColumnIdToRange.containsKey(srcColumn.getId())) {
                    srcColumnIdToRange.put(srcColumn.getId(), Range.all());
                }
                Range<ConstantOperator> columnRange = srcColumnIdToRange.get(srcColumn.getId());
                Range<ConstantOperator> range = range(srcBinary.getBinaryType(), srcConstant);
                columnRange = columnRange.intersection(range);
                if (columnRange.isEmpty()) {
                    return null;
                }
                srcColumnIdToRange.put(srcColumn.getId(), columnRange);
            }
            Map<Integer, Range<ConstantOperator>> targetColumnIdToRange = Maps.newHashMap();
            for (ScalarOperator rangePredicate : targets) {
                Preconditions.checkState(rangePredicate instanceof BinaryPredicateOperator);
                Preconditions.checkState(rangePredicate.getChild(0) instanceof ColumnRefOperator);
                Preconditions.checkState(rangePredicate.getChild(1) instanceof ConstantOperator);
                BinaryPredicateOperator srcBinary = (BinaryPredicateOperator) rangePredicate;
                Preconditions.checkState(srcBinary.getBinaryType().isRange() || srcBinary.getBinaryType().isEqual());
                ColumnRefOperator targetColumn = (ColumnRefOperator) srcBinary.getChild(0);
                ConstantOperator targetConstant = (ConstantOperator) srcBinary.getChild(1);
                if (!targetColumnIdToRange.containsKey(targetColumn.getId())) {
                    targetColumnIdToRange.put(targetColumn.getId(), Range.all());
                }
                Range<ConstantOperator> columnRange = targetColumnIdToRange.get(targetColumn.getId());
                Range<ConstantOperator> range = range(srcBinary.getBinaryType(), targetConstant);
                columnRange = columnRange.intersection(range);
                if (columnRange.isEmpty()) {
                    return null;
                }
                targetColumnIdToRange.put(targetColumn.getId(), columnRange);
            }

            List<Integer> resultColumnIds = Lists.newArrayList();

            for (Map.Entry<Integer, Range<ConstantOperator>> targetEntry : targetColumnIdToRange.entrySet()) {
                if (!srcColumnIdToRange.containsKey(targetEntry.getKey())
                        && !targetEntry.getValue().hasUpperBound()
                        && !targetEntry.getValue().hasLowerBound()) {
                    return  null;
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
                AtomicReference<ScalarOperator> result = new AtomicReference<>();
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
                        result.set(Utils.compoundAnd(result.get(), eqBinary));
                    } else {
                        List<ScalarOperator> columnScalars = srcPredicates.stream().filter(
                                predicate -> isScalarForColumns(predicate, columnId)
                        ).collect(Collectors.toList());
                        columnScalars.forEach(
                                predicate -> result.set(Utils.compoundAnd(result.get(), predicate))
                        );
                    }
                }
                return result.get();
            }
        } catch (Exception e) {
            return null;
        }
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
