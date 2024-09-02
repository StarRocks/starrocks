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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Project                                  Join
 * |                                     /    \
 * Join               --->        LEFT_CHILD   Project
 * /     \                                        \
 * LEFT_CHILD  RIGHT_CHILD                      RIGHT_CHILD
 * <p>
 * Conditionally push down Project under Join operator:
 * 1. For profitable expression: input is complex type, output is short primitive type
 *
 * TODO: in theory it should be determined by cost and cover more cases
 *
 */
public class PushDownProjectToJoinRule extends TransformationRule {

    private static final PushDownProjectToJoinRule INSTANCE = new PushDownProjectToJoinRule();

    protected PushDownProjectToJoinRule() {
        super(RuleType.TF_PUSH_DOWN_PROJECT_TO_JOIN,
                Pattern.create(OperatorType.LOGICAL_PROJECT).addChildren(
                        Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_LEAF,
                                OperatorType.PATTERN_LEAF)
                ));
    }

    public static TransformationRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnablePushDownJoinProjection()) {
            return false;
        }
        LogicalProjectOperator projectOp = input.getOp().cast();
        for (var entry : projectOp.getColumnRefMap().entrySet()) {
            ScalarOperator expr = entry.getValue();
            if (isProfitableExpr(expr)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Expression whose input is complex type, and output is short primitive type.
     * If it's pushed down, the expression cost can be reduced
     */
    private boolean isProfitableExpr(ScalarOperator expr) {
        ScalarOperatorVisitor<Boolean, Void> visitor = new ScalarOperatorVisitor<>() {

            @Override
            public Boolean visit(ScalarOperator scalarOperator, Void context) {
                return false;
            }

            @Override
            public Boolean visitCall(CallOperator call, Void context) {
                Type retType = call.getType();
                List<ColumnRefOperator> argumentColumns =
                        call.getArguments().stream()
                                .flatMap(x -> x.getColumnRefs().stream())
                                .collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(argumentColumns) &&
                        !retType.isComplexType() &&
                        argumentColumns.stream().anyMatch(x -> x.getType().isComplexType())) {
                    return true;
                }
                return false;
            }
        };
        return expr.accept(visitor, null);
    }

    private void pushdownProject(Map<ColumnRefOperator, ScalarOperator> projectMap,
                                 ColumnRefSet leftColumns,
                                 ColumnRefSet rightColumns,
                                 Map<ColumnRefOperator, ScalarOperator> leftMap,
                                 Map<ColumnRefOperator, ScalarOperator> rightMap,
                                 OptimizerContext context) {
        // push down intersected columns
        ColumnRefFactory factory = context.getColumnRefFactory();
        Map<ColumnRefOperator, ColumnRefOperator> replacingMap = Maps.newHashMap();
        for (var entry : projectMap.entrySet()) {
            ScalarOperator expr = entry.getValue();
            if (!expr.isColumnRef()) {
                if (leftColumns.containsAll(expr.getUsedColumns())) {
                    ColumnRefOperator newRef = factory.create(expr, expr.getType(), expr.isNullable());
                    leftMap.put(newRef, expr);
                    replacingMap.put(entry.getKey(), newRef);
                } else if (rightColumns.containsAll(expr.getUsedColumns())) {
                    ColumnRefOperator newRef = factory.create(expr, expr.getType(), expr.isNullable());
                    rightMap.put(newRef, expr);
                    replacingMap.put(entry.getKey(), newRef);
                }
            }
        }

        // replace existing expression
        projectMap.putAll(replacingMap);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator projectOp = input.getOp().cast();
        OptExpression joinExpr = input.inputAt(0);
        LogicalJoinOperator joinOp = joinExpr.getOp().cast();
        OptExpression joinLeft = joinExpr.inputAt(0);
        OptExpression joinRight = joinExpr.inputAt(1);

        Map<ColumnRefOperator, ScalarOperator> topMap = Maps.newHashMap(projectOp.getColumnRefMap());
        Map<ColumnRefOperator, ScalarOperator> leftMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> rightMap = Maps.newHashMap();
        pushdownProject(topMap, joinLeft.getOutputColumns(), joinRight.getOutputColumns(), leftMap, rightMap, context);

        if (leftMap.isEmpty() && rightMap.isEmpty()) {
            return Lists.newArrayList();
        }
        if (!leftMap.isEmpty()) {
            leftMap.putAll(((LogicalProjectOperator) joinLeft.getOp()).getColumnRefMap());
            joinLeft = OptExpression.create(new LogicalProjectOperator.Builder()
                    .withOperator(projectOp)
                    .setColumnRefMap(leftMap)
                    .build(), joinLeft);
        }

        if (!rightMap.isEmpty()) {
            rightMap.putAll(((LogicalProjectOperator) joinRight.getOp()).getColumnRefMap());
            joinRight = OptExpression.create(
                    new LogicalProjectOperator.Builder()
                            .withOperator(projectOp)
                            .setColumnRefMap(rightMap)
                            .build(), joinRight);
        }

        OptExpression newJoin = OptExpression.create(
                new LogicalJoinOperator.Builder()
                        .withOperator(joinOp)
                        .build(), joinLeft, joinRight);

        LogicalProjectOperator newProjectOp =
                new LogicalProjectOperator.Builder()
                        .withOperator(projectOp)
                        .setColumnRefMap(topMap)
                        .build();
        return Lists.newArrayList(OptExpression.create(newProjectOp, newJoin));
    }
}
