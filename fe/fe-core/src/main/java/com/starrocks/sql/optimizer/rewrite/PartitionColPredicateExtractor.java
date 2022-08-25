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

import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.EQ_FOR_NULL;

/**
 * Extract predicate tree only with partition key or boolean constant
 * from original predicate tree.
 * For example, k1 is partition key and c1, c2 are normal column.
 * This predicate (k1 < 100 and c1 like 'a') or (k1 > 200 and func(k1) > c1)
 * will transform to
 * k1 < 100 or k1 > 200
 * Then we can use range info to test which ranges fit the transformed condition.
 */
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

    @Override
    public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
        return literal;
    }

    // only column is partition column will be preserved
    @Override
    public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
        if (partitionColumnSet.containsAll(variable.getUsedColumns())) {
            return variable;
        }
        return null;
    }

    @Override
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

            if (EQ_FOR_NULL == predicate.getBinaryType() || !isConstantNull(right)) {
                return predicate;
            } else {
                return ConstantOperator.createBoolean(false);
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
            if (first.isConstantRef()) {
                boolean isTrue = ((ConstantOperator) first).getBoolean();
                if (isTrue) {
                    second = second.accept(this, null);
                    return second;
                } else {
                    return ConstantOperator.createBoolean(false);
                }

            } else {
                second = second.accept(this, null);
                if (second.isConstantRef()) {
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
            if (first.isConstantRef()) {
                return ((ConstantOperator) first).getBoolean() ? first : second;
            } else if (second.isConstantRef()) {
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

    // any like predicateOperator does nothing to help partition prune.
    // we just transform it to true predicate.
    @Override
    public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
        return ConstantOperator.createBoolean(true);
    }

    // when operator == null, it means this predicate node isn't related to partition column
    // we return true to indicate that we can safely prune this predicate node.
    private boolean isShortCut(ScalarOperator operator) {
        return operator == null;
    }

    private boolean isConstantNull(ScalarOperator operator) {
        if (operator.isConstantRef()) {
            return ((ConstantOperator) operator).isNull();
        }
        return false;
    }

}
