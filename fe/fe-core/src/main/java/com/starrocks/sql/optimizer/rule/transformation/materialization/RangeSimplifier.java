// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
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

public class RangeSimplifier {
    private List<ScalarOperator> srcPredicates;

    public RangeSimplifier(List<ScalarOperator> srcPredicates) {
        this.srcPredicates = srcPredicates;
    }

    // check whether target range predicates are contained in srcPredicates
    // all ScalarOperator should be BinaryPredicateOperator, left is ColumnRefOperator and right is ConstantOperator
    public ScalarOperator simplify(List<ScalarOperator> targets) {
        try {
            Map<Integer, Range> srcColumnIdToRange = Maps.newHashMap();
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
                Range columnRange = srcColumnIdToRange.get(srcColumn.getId());
                Range range = range(srcBinary.getBinaryType(), srcConstant);
                columnRange = columnRange.intersection(range);
                if (columnRange.isEmpty()) {
                    return null;
                }
                srcColumnIdToRange.put(srcColumn.getId(), columnRange);
            }
            Map<Integer, Range> targetColumnIdToRange = Maps.newHashMap();
            for (ScalarOperator rangePredicate : targets) {
                Preconditions.checkState(rangePredicate instanceof BinaryPredicateOperator);
                Preconditions.checkState(rangePredicate.getChild(0) instanceof ColumnRefOperator);
                Preconditions.checkState(rangePredicate.getChild(1) instanceof ConstantOperator);
                BinaryPredicateOperator srcBinary = (BinaryPredicateOperator) rangePredicate;
                Preconditions.checkState(srcBinary.getBinaryType().isRange() || srcBinary.getBinaryType().isEqual());
                ColumnRefOperator srcColumn = (ColumnRefOperator) srcBinary.getChild(0);
                ConstantOperator srcConstant = (ConstantOperator) srcBinary.getChild(1);
                if (!targetColumnIdToRange.containsKey(srcColumn.getId())) {
                    targetColumnIdToRange.put(srcColumn.getId(), Range.all());
                }
                Range columnRange = targetColumnIdToRange.get(srcColumn.getId());
                Range range = range(srcBinary.getBinaryType(), srcConstant);
                columnRange = columnRange.intersection(range);
                if (columnRange.isEmpty()) {
                    return null;
                }
                targetColumnIdToRange.put(srcColumn.getId(), columnRange);
            }

            List<Integer> resultColumnId = Lists.newArrayList();
            /*
            for (Map.Entry<Integer, Range> srcEntry : srcColumnIdToRange.entrySet()) {
                if (!targetColumnIdToRange.containsKey(srcEntry.getKey())
                        && !srcEntry.getValue().hasUpperBound()
                        && !srcEntry.getValue().hasLowerBound()) {
                    return  null;
                }
                Range targetRange = targetColumnIdToRange.get(srcEntry.getKey());
                Range srcRange = srcEntry.getValue();
                if (srcRange.equals(targetRange)) {
                    continue;
                } else if (targetRange.encloses(srcRange)) {
                    resultColumnId.add(srcEntry.getKey());
                } else {
                    // can not be rewritten
                    return null;
                }
            }

             */

            for (Map.Entry<Integer, Range> targetEntry : targetColumnIdToRange.entrySet()) {
                if (!srcColumnIdToRange.containsKey(targetEntry.getKey())
                        && !targetEntry.getValue().hasUpperBound()
                        && !targetEntry.getValue().hasLowerBound()) {
                    return  null;
                }
                Range srcRange = srcColumnIdToRange.get(targetEntry.getKey());
                Range targetRange = targetEntry.getValue();
                if (srcRange.equals(targetRange)) {
                    continue;
                } else if (targetRange.encloses(srcRange)) {
                    resultColumnId.add(targetEntry.getKey());
                } else {
                    // can not be rewritten
                    return null;
                }
            }

            for (Map.Entry<Integer, Range> srcEntry : srcColumnIdToRange.entrySet()) {
                if (!targetColumnIdToRange.containsKey(srcEntry.getKey())) {
                    resultColumnId.add(srcEntry.getKey());
                }
            }
            if (resultColumnId.isEmpty()) {
                return ConstantOperator.createBoolean(true);
            } else {
                List<ScalarOperator> resultPredicts = Lists.newArrayList();
                for (ScalarOperator src : srcPredicates) {
                    BinaryPredicateOperator binary = (BinaryPredicateOperator) src;
                    ColumnRefOperator targetColumn = (ColumnRefOperator) binary.getChild(0);
                    if (resultColumnId.contains(targetColumn.getId())) {
                        resultPredicts.add(src);
                    }
                }
                return Utils.compoundAnd(resultPredicts);
            }
        } catch (Exception e) {
            return null;
        }
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
                throw new RuntimeException("unsupported type:" + type);
        }
    }
}
