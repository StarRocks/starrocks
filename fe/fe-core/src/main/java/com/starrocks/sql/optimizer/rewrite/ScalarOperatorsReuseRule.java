// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class ScalarOperatorsReuseRule implements PhysicalOperatorTreeRewriteRule {
    public static final ReuseVisitor handler = new ReuseVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(handler, root, taskContext);
        return root;
    }

    private static class ReuseVisitor extends OptExpressionVisitor<Void, TaskContext> {
        @Override
        public Void visit(OptExpression opt, TaskContext context) {
            if (opt.getOp().getProjection() != null) {
                opt.getOp().setProjection(rewriteProject(opt, context));
            }

            for (OptExpression input : opt.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }

        Projection rewriteProject(OptExpression input, TaskContext context) {
            Projection projection = input.getOp().getProjection();

            Map<ColumnRefOperator, ScalarOperator> columnRefMap = projection.getColumnRefMap();
            List<ScalarOperator> scalarOperators = Lists.newArrayList(columnRefMap.values());

            Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubOperatorsByDepth = ScalarOperatorsReuse
                    .collectCommonSubScalarOperators(scalarOperators,
                            context.getOptimizerContext().getColumnRefFactory());

            Map<ScalarOperator, ColumnRefOperator> commonSubOperators =
                    commonSubOperatorsByDepth.values().stream()
                            .flatMap(m -> m.entrySet().stream())
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            if (commonSubOperators.isEmpty()) {
                // no rewrite
                return projection;
            }

            boolean hasRewritted = false;
            for (ScalarOperator operator : columnRefMap.values()) {
                ScalarOperator rewriteOperator =
                        ScalarOperatorsReuse.rewriteOperatorWithCommonOperator(operator, commonSubOperators);
                if (!rewriteOperator.equals(operator)) {
                    hasRewritted = true;
                    break;
                }
            }

            /*
             * 1. Rewrite the operator with the common sub operators
             * 2. Put the common sub operators to projection, we need to compute
             * common sub operators firstly in BE
             */
            if (hasRewritted) {
                Map<ColumnRefOperator, ScalarOperator> newMap =
                        Maps.newTreeMap(Comparator.comparingInt(ColumnRefOperator::getId));
                for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : columnRefMap.entrySet()) {
                    newMap.put(kv.getKey(), ScalarOperatorsReuse.
                            rewriteOperatorWithCommonOperator(kv.getValue(), commonSubOperators));
                }

                Map<ColumnRefOperator, ScalarOperator> newCommonMap =
                        Maps.newTreeMap(Comparator.comparingInt(ColumnRefOperator::getId));
                for (Map.Entry<ScalarOperator, ColumnRefOperator> kv : commonSubOperators.entrySet()) {
                    Preconditions.checkState(!newMap.containsKey(kv.getValue()));
                    newCommonMap.put(kv.getValue(), kv.getKey());
                }

                return new Projection(newMap, newCommonMap);
            }
            return projection;
        }
    }
}
