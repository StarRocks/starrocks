// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.PhysicalOperatorTreeRewriteRule;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorsReuse;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class ScalarOperatorReuseRule implements PhysicalOperatorTreeRewriteRule {

    private static class ScalarOperatorReuseVisitor extends OptExpressionVisitor<Void, Void> {
        private final OptimizerContext context;

        public ScalarOperatorReuseVisitor(OptimizerContext context) {
            this.context = context;
        }

        @Override
        public Void visit(OptExpression optExpression, Void v) {
            Operator operator = optExpression.getOp();
            if (operator.getProjection() != null) {
                Projection projection = operator.getProjection();

                Map<ColumnRefOperator, ScalarOperator> columnRefMap = projection.getColumnRefMap();
                List<ScalarOperator> scalarOperators = Lists.newArrayList(columnRefMap.values());

                Map<Integer, Map<ScalarOperator, ColumnRefOperator>>
                        commonSubOperatorsByDepth = ScalarOperatorsReuse.
                        collectCommonSubScalarOperators(scalarOperators,
                                context.getColumnRefFactory());

                Map<ScalarOperator, ColumnRefOperator> commonSubOperators =
                        commonSubOperatorsByDepth.values().stream()
                                .flatMap(m -> m.entrySet().stream())
                                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

                if (!commonSubOperators.isEmpty()) {
                    /*
                     * If no rewritted, we should return empty list
                     */
                    boolean hasRewritted = false;
                    for (ScalarOperator scalarOperator : columnRefMap.values()) {
                        ScalarOperator rewrittedOperator = ScalarOperatorsReuse.
                                rewriteOperatorWithCommonOperator(scalarOperator, commonSubOperators);
                        if (!rewrittedOperator.equals(scalarOperator)) {
                            hasRewritted = true;
                            break;
                        }
                    }

                    /*
                     * 1. Rewrite the operator with the common sub operators
                     * 2. Put the common sub operators to project node, we need to compute
                     * common sub operators firstly in BE
                     */
                    if (hasRewritted) {
                        Map<ColumnRefOperator, ScalarOperator> newMap = Maps.newHashMap();
                        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : columnRefMap.entrySet()) {
                            newMap.put(kv.getKey(), ScalarOperatorsReuse.
                                    rewriteOperatorWithCommonOperator(kv.getValue(), commonSubOperators));
                        }

                        Map<ColumnRefOperator, ScalarOperator> newCommonMap = Maps.newHashMap();
                        for (Map.Entry<ScalarOperator, ColumnRefOperator> kv : commonSubOperators.entrySet()) {
                            Preconditions.checkState(!newMap.containsKey(kv.getValue()));
                            newCommonMap.put(kv.getValue(), kv.getKey());
                        }

                        operator.setProjection(new Projection(newMap, newCommonMap));
                    }
                }
            }
            optExpression.getInputs().forEach(c -> this.visit(c, v));
            return null;
        }
    }

    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        new ScalarOperatorReuseVisitor(taskContext.getOptimizerContext()).visit(root, null);
        return root;
    }
}
