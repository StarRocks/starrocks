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

package com.starrocks.sql.optimizer.validate;

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Map;

/**
 * Validates that the value list of every IN-predicate references no column.
 *
 * <p>The BE builds every IN-predicate as a {@code VectorizedInConstPredicate}, which evaluates the
 * value list once in {@code open()} without a chunk. {@code NormalizePredicateRule} splits any
 * non-constant value out into an {@code OR} of equalities, so a column reference that shows up in the
 * value list of the final plan was put there by a later rewrite, and the BE can only fail on it.
 */
public class ConstInPredicateChecker implements PlanValidator.Checker {

    private static final String PREFIX = "Const IN-predicate check failed.";

    private static final ConstInPredicateChecker INSTANCE = new ConstInPredicateChecker();

    private ConstInPredicateChecker() {}

    public static ConstInPredicateChecker getInstance() {
        return INSTANCE;
    }

    @Override
    public void validate(OptExpression physicalPlan, TaskContext taskContext) {
        physicalPlan.getOp().accept(new OptVisitor(), physicalPlan, null);
    }

    private static final class OptVisitor extends OptExpressionVisitor<Void, Void> {

        private final ScalarVisitor scalarVisitor = new ScalarVisitor();

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            Operator op = optExpression.getOp();
            walk(op.getPredicate());
            Projection projection = op.getProjection();
            if (projection != null) {
                walk(projection.getColumnRefMap());
                walk(projection.getCommonSubOperatorMap());
            }
            if (op instanceof PhysicalProjectOperator) {
                walk(((PhysicalProjectOperator) op).getColumnRefMap());
                walk(((PhysicalProjectOperator) op).getCommonSubOperatorMap());
            } else if (op instanceof PhysicalJoinOperator) {
                walk(((PhysicalJoinOperator) op).getOnPredicate());
            } else if (op instanceof PhysicalHashAggregateOperator) {
                walk(((PhysicalHashAggregateOperator) op).getAggregations());
            } else if (op instanceof PhysicalWindowOperator) {
                walk(((PhysicalWindowOperator) op).getAnalyticCall());
            }
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, null);
            }
            return null;
        }

        private void walk(Map<ColumnRefOperator, ? extends ScalarOperator> map) {
            if (map == null) {
                return;
            }
            for (ScalarOperator scalar : map.values()) {
                walk(scalar);
            }
        }

        private void walk(ScalarOperator scalar) {
            if (scalar != null) {
                scalar.accept(scalarVisitor, null);
            }
        }
    }

    private static final class ScalarVisitor extends ScalarOperatorVisitor<Void, Void> {

        @Override
        public Void visit(ScalarOperator scalarOperator, Void context) {
            for (ScalarOperator child : scalarOperator.getChildren()) {
                child.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitInPredicate(InPredicateOperator predicate, Void context) {
            if (!predicate.isSubquery()) {
                for (ScalarOperator value : predicate.getListChildren()) {
                    if (!value.getUsedColumns().isEmpty()) {
                        throw new StarRocksPlannerException(
                                String.format("%s value '%s' of IN-predicate '%s' references a column",
                                        PREFIX, value, predicate),
                                ErrorType.INTERNAL_ERROR);
                    }
                }
            }
            return visit(predicate, context);
        }
    }
}
