// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PartitionColPredicateExtractor extends ScalarOperatorVisitor<ScalarOperator, Void> {

    private ColumnRefSet partitionColumnSet;

    public PartitionColPredicateExtractor(RangePartitionInfo rangePartitionInfo,
                                          Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        List<ColumnRefOperator> columnRefOperators = Lists.newArrayList();
        Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        columnRefOperators.add(columnMetaToColRefMap.get(partitionColumn));
        partitionColumnSet = new ColumnRefSet(columnRefOperators);
    }

    public ScalarOperator extract(ScalarOperator predicate) {
        predicate = predicate.clone();
        List<ScalarOperator> splitPredicates = Utils.extractConjuncts(predicate).stream()
                .filter(e -> !e.isFromPredicateRangeDerive()).collect(Collectors.toList());
        ScalarOperator scalarOperator = Utils.compoundAnd(splitPredicates);
        return scalarOperator.accept(this, null);
    }

    @Override
    public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
        return null;
    }

    public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
        return literal;
    }

    public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
        if (partitionColumnSet.containsAll(variable.getUsedColumns())) {
            return variable;
        }
        return null;
    }

    public ScalarOperator visitCall(CallOperator call, Void context) {
        return null;
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
        if (partitionColumnSet.containsAll(predicate.getUsedColumns())) {
            ScalarOperator left = predicate.getChild(0).accept(this, null);
            if (isShortCut(left)) {
                return ConstantOperator.createBoolean(true);
            }
            ScalarOperator right = predicate.getChild(1).accept(this, null);
            if (isShortCut(right)) {
                return ConstantOperator.createBoolean(true);
            }
            switch (predicate.getBinaryType()) {
                case EQ_FOR_NULL:
                    return predicate;
                default:
                    if (isConstantNull(right)) {
                        return ConstantOperator.createBoolean(false);
                    }
                    return predicate;
            }
        }
        return ConstantOperator.createBoolean(true);
    }

    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
        // EliminateNegationsRewriter has eliminated most not predicate scenes except not like clause.
        // For not like clause, replace it with true condition.
        if (predicate.isNot()) {
            return ConstantOperator.createBoolean(true);
        }
        ScalarOperator first = predicate.getChild(0).accept(this, null);
        if (predicate.isAnd()) {
            ScalarOperator second = predicate.getChild(1);
            if (first instanceof ConstantOperator) {
                boolean isTrue = ((ConstantOperator) first).getBoolean();
                if (isTrue) {
                    second = second.accept(this, null);
                    return second;
                } else {
                    return ConstantOperator.createBoolean(false);
                }

            } else {
                second = second.accept(this, null);
                if (second instanceof ConstantOperator) {
                    boolean isTrue = ((ConstantOperator) second).getBoolean();
                    if (isTrue) {
                        return first;
                    } else {
                        return ConstantOperator.createBoolean(false);
                    }
                } else {
                    return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, first, second);
                }
            }
        } else {
            ScalarOperator second = predicate.getChild(1).accept(this, null);
            if (first instanceof ConstantOperator) {
                return ((ConstantOperator) first).getBoolean() ? first : second;
            } else if (second instanceof ConstantOperator) {
                return ((ConstantOperator) second).getBoolean() ? second : first;
            } else {
                return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, first, second);
            }
        }
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
        ScalarOperator first = predicate.getChild(0).accept(this, null);
        if (isShortCut(first)) {
            return ConstantOperator.createBoolean(true);
        } else {
            if (predicate.allValuesMatch(ScalarOperator::isConstantRef)) {
                if (predicate.isNotIn()) {
                    // not in (null, value) always return false, otherwise, always return true
                    return ConstantOperator.createBoolean(!predicate.hasAnyNullValues());
                }
                return predicate;
            } else {
                return ConstantOperator.createBoolean(true);
            }
        }
    }

    @Override
    public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
        ScalarOperator first = predicate.getChild(0).accept(this, null);
        if (isShortCut(first) || predicate.isNotNull()) {
            return ConstantOperator.createBoolean(true);
        } else {
            return ConstantOperator.createBoolean(false);
        }
    }

    @Override
    public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
        return ConstantOperator.createBoolean(true);
    }

    private boolean isShortCut(ScalarOperator operator) {
        return operator == null;
    }

    private boolean isConstantNull(ScalarOperator operator) {
        if (operator instanceof ConstantOperator) {
            return ((ConstantOperator) operator).isNull();
        }
        return false;
    }

}
