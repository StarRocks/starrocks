// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
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
            Map<Integer, Range> columnIdToRange = Maps.newHashMap();
            for (ScalarOperator rangePredicate : srcPredicates) {
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
                Range columnRange = columnIdToRange.get(srcColumn.getId());
                Range range = range(srcBinary.getBinaryType(), srcConstant);
                columnRange = columnRange.intersection(range);
                if (columnRange.isEmpty()) {
                    return null;
                }
            }
            for (ScalarOperator target : targets) {
                Preconditions.checkState(target instanceof BinaryPredicateOperator);
                Preconditions.checkState(target.getChild(0) instanceof ColumnRefOperator);
                Preconditions.checkState(target.getChild(1) instanceof ConstantOperator);
                BinaryPredicateOperator binary = (BinaryPredicateOperator) target;
                Preconditions.checkState(binary.getBinaryType().isRange() || binary.getBinaryType().isEqual());
                ColumnRefOperator targetColumn = (ColumnRefOperator) binary.getChild(0);
                if (!columnIdToRange.containsKey(targetColumn.getId())) {
                    return null;
                }
                ConstantOperator targetConstant = (ConstantOperator) binary.getChild(1);
                Range targetRange = range(binary.getBinaryType(), targetConstant);
                Range srcRange = columnIdToRange.get(targetColumn.getId());
                if (!srcRange.encloses(targetRange)) {
                    return null;
                }
                srcRange = srcRange.intersection(targetRange);
                columnIdToRange.put(targetColumn.getId(), srcRange);
            }
            boolean allEmpty = true;
            for (Map.Entry<Integer, Range> entry : columnIdToRange.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    allEmpty = false;
                    break;
                }
            }
            if (allEmpty) {
                return ConstantOperator.createBoolean(true);
            } else {
                return Utils.compoundAnd(targets);
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
