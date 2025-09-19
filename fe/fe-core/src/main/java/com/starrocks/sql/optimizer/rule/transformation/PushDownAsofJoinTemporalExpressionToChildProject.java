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
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Push down ASOF temporal expressions to child Projects for ASOF Join optimization.
 *
 * This transformation rule is specifically designed for ASOF Join operations to optimize
 * temporal expression evaluation by pushing complex expressions down to child Project nodes.
 *
 * ASOF Join is a time-series join that matches records based on temporal proximity, typically
 * using expressions like date_trunc(), date_format(), or other time manipulation functions
 * in the temporal predicate.
 *
 * Transformation Pattern:
 *           ASOF Join                                     ASOF Join
 *         /           \                                 /           \
 *     ScanNode     ScanNode        ==>              Project      Project
 *                                                     |             |
 *                                                  ScanNode      ScanNode
 *
 * Example:
 *   Original ASOF join temporal predicate:
 *     date_trunc('hour', left.dt) >= date_trunc('hour', right.dt)
 *
 *   After transformation:
 *     left_projected_col >= right_projected_col
 *
 *   Where:
 *     - Left Project:  left_projected_col = date_trunc('hour', left.dt)
 *     - Right Project: right_projected_col = date_trunc('hour', right.dt)
 *
 * Note: If the ASOF join condition is invalid or malformed, this rule will skip processing
 * and leave the validation to subsequent optimization phases for proper error handling.
 */
public class PushDownAsofJoinTemporalExpressionToChildProject extends TransformationRule {
    public PushDownAsofJoinTemporalExpressionToChildProject() {
        super(RuleType.TF_PUSH_DOWN_ASOF_JOIN_TEMPORAL_EXPRESSION_TO_CHILD_PROJECT,
                Pattern.create(OperatorType.LOGICAL_JOIN)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF),
                                Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = (LogicalJoinOperator) input.getOp();
        if (!join.getJoinType().isAsofJoin()) {
            return false;
        }
        ScalarOperator on = join.getOnPredicate();
        if (on == null) {
            return false;
        }

        ColumnRefSet leftCols = input.inputAt(0).getOutputColumns();
        ColumnRefSet rightCols = input.inputAt(1).getOutputColumns();
        ScalarOperator temporal = findSingleAsofTemporalPredicate(Utils.extractConjuncts(on), leftCols, rightCols);
        return temporal != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = (LogicalJoinOperator) input.getOp();
        ScalarOperator on = join.getOnPredicate();
        ColumnRefSet leftCols = input.inputAt(0).getOutputColumns();
        ColumnRefSet rightCols = input.inputAt(1).getOutputColumns();

        List<ScalarOperator> conjuncts = Utils.extractConjuncts(on);
        ScalarOperator temporal = findSingleAsofTemporalPredicate(conjuncts, leftCols, rightCols);
        if (temporal == null) {
            return Collections.emptyList();
        }

        temporal = JoinHelper.applyCommutativeToPredicates(temporal, leftCols, rightCols);

        ScalarOperator lhs = temporal.getChild(0);
        ScalarOperator rhs = temporal.getChild(1);

        Map<ColumnRefOperator, ScalarOperator> leftProjectMap = new HashMap<>();
        Map<ColumnRefOperator, ScalarOperator> rightProjectMap = new HashMap<>();

        boolean lhsOnLeft = leftCols.containsAll(lhs.getUsedColumns());
        boolean lhsOnRight = rightCols.containsAll(lhs.getUsedColumns());
        boolean rhsOnRight = rightCols.containsAll(rhs.getUsedColumns());
        boolean rhsOnLeft = leftCols.containsAll(rhs.getUsedColumns());

        if ((lhsOnLeft && lhsOnRight) || (rhsOnLeft && rhsOnRight)) {
            return Collections.emptyList();
        }

        if (lhsOnLeft && !lhs.isColumnRef()) {
            leftProjectMap.put(context.getColumnRefFactory().create(lhs, lhs.getType(), lhs.isNullable()), lhs);
        } else if (lhsOnRight && !lhs.isColumnRef()) {
            rightProjectMap.put(context.getColumnRefFactory().create(lhs, lhs.getType(), lhs.isNullable()), lhs);
        }

        if (rhsOnRight && !rhs.isColumnRef()) {
            rightProjectMap.put(context.getColumnRefFactory().create(rhs, rhs.getType(), rhs.isNullable()), rhs);
        } else if (rhsOnLeft && !rhs.isColumnRef()) {
            leftProjectMap.put(context.getColumnRefFactory().create(rhs, rhs.getType(), rhs.isNullable()), rhs);
        }

        if (leftProjectMap.isEmpty() && rightProjectMap.isEmpty()) {
            return Collections.emptyList();
        }

        Rewriter leftRewriter = new Rewriter(leftProjectMap);
        Rewriter rightRewriter = new Rewriter(rightProjectMap);
        ScalarOperator newOn = on.clone().accept(leftRewriter, null).accept(rightRewriter, null);

        OptExpression newJoin = OptExpression.create(new LogicalJoinOperator.Builder().withOperator(join)
                .setOnPredicate(newOn)
                .build(), input.getInputs());

        if (!leftProjectMap.isEmpty()) {
            leftProjectMap.putAll(leftCols.getStream()
                    .map(id -> context.getColumnRefFactory().getColumnRef(id))
                    .collect(Collectors.toMap(Function.identity(), Function.identity())));
            LogicalProjectOperator leftProject = new LogicalProjectOperator(leftProjectMap);
            OptExpression leftProjOpt = OptExpression.create(leftProject, input.inputAt(0));
            newJoin.setChild(0, leftProjOpt);
        }

        if (!rightProjectMap.isEmpty()) {
            rightProjectMap.putAll(rightCols.getStream()
                    .map(id -> context.getColumnRefFactory().getColumnRef(id))
                    .collect(Collectors.toMap(Function.identity(), Function.identity())));
            LogicalProjectOperator rightProject = new LogicalProjectOperator(rightProjectMap);
            OptExpression rightProjOpt = OptExpression.create(rightProject, input.inputAt(1));
            newJoin.setChild(1, rightProjOpt);
        }

        return Lists.newArrayList(newJoin);
    }

    private static ScalarOperator findSingleAsofTemporalPredicate(List<ScalarOperator> otherJoin,
                                                                  ColumnRefSet leftColumns,
                                                                  ColumnRefSet rightColumns) {
        List<ScalarOperator> candidates = Lists.newArrayList();
        for (ScalarOperator p : otherJoin) {
            if (isValidAsofTemporalPredicate(p, leftColumns, rightColumns)) {
                candidates.add(p);
            }
        }
        if (candidates.size() != 1) {
            return null;
        }
        return candidates.get(0);
    }

    private static boolean isValidAsofTemporalPredicate(ScalarOperator predicate,
                                                        ColumnRefSet leftColumns,
                                                        ColumnRefSet rightColumns) {
        if (!(predicate instanceof BinaryPredicateOperator binary)) {
            return false;
        }
        if (!binary.getBinaryType().isRange()) {
            return false;
        }
        ColumnRefSet lCols = binary.getChild(0).getUsedColumns();
        ColumnRefSet rCols = binary.getChild(1).getUsedColumns();
        if (lCols.isIntersect(leftColumns) && lCols.isIntersect(rightColumns)) {
            return false;
        }
        if (rCols.isIntersect(leftColumns) && rCols.isIntersect(rightColumns)) {
            return false;
        }
        boolean cross = (leftColumns.containsAll(lCols) && rightColumns.containsAll(rCols)) ||
                (rightColumns.containsAll(lCols) && leftColumns.containsAll(rCols));
        return cross;
    }

    static class Rewriter extends ScalarOperatorVisitor<ScalarOperator, Void> {
        private final Map<ScalarOperator, ColumnRefOperator> operatorMap = new HashMap<>();

        public Rewriter(Map<ColumnRefOperator, ScalarOperator> map) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : map.entrySet()) {
                operatorMap.put(e.getValue(), e.getKey());
            }
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            if (operatorMap.containsKey(scalarOperator)) {
                return operatorMap.get(scalarOperator);
            }
            for (int i = 0; i < scalarOperator.getChildren().size(); ++i) {
                scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, null));
            }
            return scalarOperator;
        }
    }
}


