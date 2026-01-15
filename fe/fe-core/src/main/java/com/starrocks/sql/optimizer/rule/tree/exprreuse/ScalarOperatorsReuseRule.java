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


package com.starrocks.sql.optimizer.rule.tree.exprreuse;

import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.HashMap;
import java.util.Map;


public class ScalarOperatorsReuseRule implements TreeRewriteRule {
    private static final ReuseVisitor HANDLER = new ReuseVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(HANDLER, root, taskContext);
        return root;
    }

    private static class ReuseVisitor extends OptExpressionVisitor<Void, TaskContext> {
        @Override
        public Void visit(OptExpression opt, TaskContext context) {
            if (opt.getOp().getProjection() != null) {
                opt.getOp().setProjection(rewriteProject(opt, context));
            }

            if (shouldRewritePredicate(opt, context)) {
                ScalarOperator predicate = opt.getOp().getPredicate();
                if (predicate != null) {
                    Projection result = rewritePredicate(context, predicate);
                    if (!result.getCommonSubOperatorMap().isEmpty()) {
                        PhysicalOperator op = (PhysicalOperator) opt.getOp();
                        ScalarOperator newPredicate = result.getColumnRefMap().values().iterator().next();
                        op.setPredicate(newPredicate);
                        op.setPredicateCommonOperators(result.getCommonSubOperatorMap());
                    }
                } else {
                    if (!JoinHelper.canTreatOnPredicateAsPredicate(opt)) {
                        return null;
                    }
                    PhysicalJoinOperator join = (PhysicalJoinOperator) opt.getOp();
                    ColumnRefSet leftChildColumns = opt.inputAt(0).getOutputColumns();
                    ColumnRefSet rightChildColumns = opt.inputAt(1).getOutputColumns();
                    JoinHelper.JoinOnSplitPredicates split = JoinHelper.splitJoinOnPredicate(join.getJoinType(),
                            join.getOnPredicate(), leftChildColumns, rightChildColumns);
                    ScalarOperator joinOtherPredicate = split.otherOnPredicate();
                    if (joinOtherPredicate != null) {
                        Projection result = rewritePredicate(context, joinOtherPredicate);
                        if (!result.getCommonSubOperatorMap().isEmpty()) {
                            ScalarOperator newPredicate = result.getColumnRefMap().values().iterator().next();
                            join.setPredicate(newPredicate);
                            join.setPredicateCommonOperators(result.getCommonSubOperatorMap());
                            join.setOnPredicate(split.eqOnPredicate());
                        }
                    }
                }
            }

            for (OptExpression input : opt.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }

        // lambda functions include lambda arguments and captured column, the former should be evaluate with lambda
        // local context with arguments, the latter may not, called lambda argument non-related expressions which can
        // be evaluated in the projection's context. These two classes expressions reuse common sub expressions in
        // different evaluation context. Lambda argument non-related expressions can be reused as non-lambda
        // expressions, ignoring lambda argument related expressions. After theses, the lambda argument related
        // expressions are processed in another process.
        // NOTE: lambda functions cannot reuse common sub expressions across each other.
        Projection rewriteProject(OptExpression input, TaskContext context) {
            Projection projection = input.getOp().getProjection();

            projection = ScalarOperatorsReuse.rewriteProjectionOrLambdaExpr(projection,
                    context.getOptimizerContext().getColumnRefFactory());

            if (projection.needReuseLambdaDependentExpr()) {
                // rewrite lambda functions with lambda arguments
                rewriteLambdaFunction(projection.getCommonSubOperatorMap(),
                        context.getOptimizerContext().getColumnRefFactory());
                rewriteLambdaFunction(projection.getColumnRefMap(), context.getOptimizerContext().getColumnRefFactory());
            }
            return projection;
        }

        private boolean shouldRewritePredicate(OptExpression input, TaskContext context) {
            if (!context.getOptimizerContext().getSessionVariable().isEnablePredicateExprReuse()) {
                return false;
            }
            if (input.getOp().getOpType() == OperatorType.PHYSICAL_FILTER ||
                    input.getOp().getOpType() == OperatorType.PHYSICAL_HASH_JOIN ||
                    input.getOp().getOpType() == OperatorType.PHYSICAL_NESTLOOP_JOIN) {
                return true;
            }
            return false;
        }

        Projection rewritePredicate(TaskContext context, ScalarOperator predicate) {
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
            ColumnRefFactory columnRefFactory = context.getOptimizerContext().getColumnRefFactory();
            columnRefMap.put(new ColumnRefOperator(
                    columnRefFactory.getNextUniqueId(), predicate.getType(), "predicate", predicate.isNullable()),
                    predicate);
            Projection result = ScalarOperatorsReuse.rewriteProjectionOrLambdaExpr(
                    new Projection(columnRefMap), columnRefFactory);
            return result;
        }

        void rewriteLambdaFunction(Map<ColumnRefOperator, ScalarOperator> operatorMap, ColumnRefFactory factory) {
            if (operatorMap.isEmpty()) {
                return;
            }
            ScalarOperatorsReuse.LambdaFunctionOperatorRewriter rewriter =
                    new ScalarOperatorsReuse.LambdaFunctionOperatorRewriter(factory);

            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : operatorMap.entrySet()) {
                kv.getValue().accept(rewriter, null);
            }
        }
    }
}
