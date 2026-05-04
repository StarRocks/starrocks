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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;

/**
 * Validates that the value-producing children of conditional expressions
 * ({@code IF}, {@code IFNULL}, {@code COALESCE}, and {@code CASE WHEN})
 * have a type compatible with the expression's declared result type.
 *
 * <p>Each of these expressions is supposed to be lowered with explicit
 * {@code CAST}s so that every branch yields the same type as the
 * surrounding expression. Plans where a branch leaks a different type
 * (e.g. a {@code DATETIME} child under a {@code coalesce(VARCHAR)}
 * signature) are malformed and silently produce wrong results or BE
 * crashes downstream. This checker rejects them up-front.
 */
public class ConditionalTypeChecker implements PlanValidator.Checker {

    private static final String PREFIX = "Conditional expression type check failed.";

    private static final ConditionalTypeChecker INSTANCE = new ConditionalTypeChecker();

    private ConditionalTypeChecker() {}

    public static ConditionalTypeChecker getInstance() {
        return INSTANCE;
    }

    @Override
    public void validate(OptExpression physicalPlan, TaskContext taskContext) {
        OptVisitor visitor = new OptVisitor();
        physicalPlan.getOp().accept(visitor, physicalPlan, null);
    }

    private static final class OptVisitor extends OptExpressionVisitor<Void, Void> {

        private final ScalarVisitor scalarVisitor = new ScalarVisitor();

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            checkOperator(optExpression.getOp());
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, null);
            }
            return null;
        }

        @Override
        public Void visitLogicalProject(OptExpression optExpression, Void context) {
            LogicalProjectOperator op = optExpression.getOp().cast();
            walkScalars(op.getColumnRefMap());
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalProject(OptExpression optExpression, Void context) {
            PhysicalProjectOperator op = optExpression.getOp().cast();
            walkScalars(op.getColumnRefMap());
            walkScalars(op.getCommonSubOperatorMap());
            return visit(optExpression, context);
        }

        private void checkOperator(Operator op) {
            ScalarOperator predicate = op.getPredicate();
            if (predicate != null) {
                predicate.accept(scalarVisitor, null);
            }
            Projection projection = op.getProjection();
            if (projection != null) {
                walkScalars(projection.getColumnRefMap());
                walkScalars(projection.getCommonSubOperatorMap());
            }
        }

        private void walkScalars(Map<ColumnRefOperator, ScalarOperator> map) {
            if (map == null) {
                return;
            }
            for (ScalarOperator scalar : map.values()) {
                if (scalar != null) {
                    scalar.accept(scalarVisitor, null);
                }
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
        public Void visitCall(CallOperator call, Void context) {
            String name = call.getFnName();
            if (FunctionSet.COALESCE.equalsIgnoreCase(name)
                    || FunctionSet.IFNULL.equalsIgnoreCase(name)) {
                checkAllArgs(call, call.getArguments(), call.getType());
            } else if (FunctionSet.IF.equalsIgnoreCase(name)) {
                List<ScalarOperator> args = call.getArguments();
                // if(cond, then, else): cond is BOOLEAN; then/else must match the result type.
                if (args.size() == 3) {
                    checkOneArg(call, args.get(1), call.getType());
                    checkOneArg(call, args.get(2), call.getType());
                }
            }
            return visit(call, context);
        }

        @Override
        public Void visitCaseWhenOperator(CaseWhenOperator caseWhen, Void context) {
            Type result = caseWhen.getType();
            for (int i = 0; i < caseWhen.getWhenClauseSize(); i++) {
                checkOneArg(caseWhen, caseWhen.getThenClause(i), result);
            }
            if (caseWhen.hasElse()) {
                checkOneArg(caseWhen, caseWhen.getElseClause(), result);
            }
            return visit(caseWhen, context);
        }

        private static void checkAllArgs(ScalarOperator expr, List<ScalarOperator> args, Type expected) {
            for (ScalarOperator arg : args) {
                checkOneArg(expr, arg, expected);
            }
        }

        private static void checkOneArg(ScalarOperator expr, ScalarOperator arg, Type expected) {
            if (arg == null) {
                return;
            }
            Type actual = arg.getType();
            if (actual == null || expected == null) {
                return;
            }
            if (!actual.matchesType(expected)) {
                throw new StarRocksPlannerException(
                        String.format(
                                "%s child '%s' has type %s, expected %s to match the result type of expr '%s'",
                                PREFIX, arg, actual, expected, expr),
                        ErrorType.INTERNAL_ERROR);
            }
        }
    }
}
