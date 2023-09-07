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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.qe.ConnectContext;
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
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.analysis.BinaryType.EQ_FOR_NULL;

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

    private final ColumnRefSet partitionColumnSet;

    public PartitionColPredicateExtractor(RangePartitionInfo rangePartitionInfo,
                                          Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        List<ColumnRefOperator> columnRefOperators = Lists.newArrayList();
        Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        for (ColumnRefOperator columnRefOperator : columnMetaToColRefMap.values()) {
            if (partitionColumn.getName().equals(columnRefOperator.getName())) {
                columnRefOperators.add(columnRefOperator);
            }
        }

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
        if (ConnectContext.get().getSessionVariable().isEnableExprPrunePartition()
                && call.getColumnRefs().size() == 1 && partitionColumnSet.containsAll(call.getUsedColumns())) {
            BaseScalarOperatorShuttle replaceShuttle = new BaseScalarOperatorShuttle() {
                @Override
                public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                    return ConstantOperator.createExampleValueByType(variable.getType());
                }
            };

            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            try {
                ScalarOperator replaced = call.accept(replaceShuttle, null);
                ScalarOperator result = rewriter.rewrite(replaced,
                        Collections.singletonList(new FoldConstantsRule(true)));

                if (result instanceof ConstantOperator) {
                    return call;
                } else {
                    return null;
                }
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
        if (partitionColumnSet.containsAll(predicate.getUsedColumns())) {
            ScalarOperator left = predicate.getChild(0).accept(this, null);
            if (!isPartitionColExpr(left)) {
                return ConstantOperator.createBoolean(true);
            }
            ScalarOperator right = predicate.getChild(1).accept(this, null);
            if (!isConstantValue(right)) {
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
        if (first == null) {
            first = ConstantOperator.createBoolean(true);
        }

        if (predicate.isAnd()) {
            ScalarOperator second = predicate.getChild(1);
            if (first.isConstantRef()) {
                boolean isTrue = ((ConstantOperator) first).getBoolean();
                if (isTrue) {
                    second = second.accept(this, null);
                    return second == null ? ConstantOperator.createBoolean(true) : second;
                } else {
                    return ConstantOperator.createBoolean(false);
                }

            } else {
                second = second.accept(this, null);
                if (second == null) {
                    second = ConstantOperator.createBoolean(true);
                }
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
            if (second == null) {
                second = ConstantOperator.createBoolean(true);
            }
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
        if (!isPartitionColExpr(first)) {
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
        if (predicate.isNotNull() || !isPartitionColExpr(first)) {
            return ConstantOperator.createBoolean(true);
        } else {
            return predicate;
        }
    }

    // any like predicateOperator does nothing to help partition prune.
    // we just transform it to true predicate.
    @Override
    public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
        return ConstantOperator.createBoolean(true);
    }

    // For the operator in inExpr(first child), binaryExpr(first child), we need it's a partition col or
    // a callExpr just contains partition col and support constant fold.
    // For compoundExpr, its children may a bool type column or other expr.
    // when it == null or it's not a satisfied expr,
    // it means this predicate node cannot been used for further evaluator
    // we return a false flag to prune this predicate.
    private boolean isPartitionColExpr(ScalarOperator operator) {
        return operator != null && (operator instanceof ColumnRefOperator || operator instanceof CallOperator);
    }

    // For the second operator in binaryPredicateExpr, we need it's a constant value
    // when it == null, or it's not a constant,
    // it means this predicate node cannot been used for further evaluator
    // we return a false flag to prune this predicate.
    private boolean isConstantValue(ScalarOperator operator) {
        return operator != null && operator.isConstant();
    }

    private boolean isConstantNull(ScalarOperator operator) {
        if (operator.isConstantRef()) {
            return ((ConstantOperator) operator).isNull();
        }
        return false;
    }

}
