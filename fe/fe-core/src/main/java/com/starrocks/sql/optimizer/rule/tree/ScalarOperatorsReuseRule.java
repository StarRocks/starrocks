// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class ScalarOperatorsReuseRule implements TreeRewriteRule {
    public static final ReuseVisitor HANDLER = new ReuseVisitor();

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

            boolean hasRewritten = false;
            for (ScalarOperator operator : columnRefMap.values()) {
                ScalarOperator rewriteOperator =
                        ScalarOperatorsReuse.rewriteOperatorWithCommonOperator(operator, commonSubOperators);
                if (!rewriteOperator.equals(operator)) {
                    hasRewritten = true;
                    break;
                }
            }

            /*
             * 1. Rewrite the operator with the common sub operators
             * 2. Put the common sub operators to projection, we need to compute
             * common sub operators firstly in BE
             */
            if (hasRewritten) {
                Map<ColumnRefOperator, ScalarOperator> newMap =
                        Maps.newTreeMap(Comparator.comparingInt(ColumnRefOperator::getId));
                // Apply to normalize rule to eliminate invalid ColumnRef usage for in-predicate
                ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
                List<ScalarOperatorRewriteRule> rules = Collections.singletonList(new NormalizePredicateRule());
                for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : columnRefMap.entrySet()) {
                    ScalarOperator rewriteOperator =
                            ScalarOperatorsReuse.rewriteOperatorWithCommonOperator(kv.getValue(), commonSubOperators);
                    rewriteOperator = rewriter.rewrite(rewriteOperator, rules);

                    if (rewriteOperator.isColumnRef() && newMap.containsValue(rewriteOperator)) {
                        // must avoid multi columnRef: columnRef
                        //@TODO(hechenfeng): remove it if BE support COW column
                        newMap.put(kv.getKey(), kv.getValue());
                    } else {
                        newMap.put(kv.getKey(), rewriteOperator);
                    }
                }

                Map<ColumnRefOperator, ScalarOperator> newCommonMap =
                        Maps.newTreeMap(Comparator.comparingInt(ColumnRefOperator::getId));
                for (Map.Entry<ScalarOperator, ColumnRefOperator> kv : commonSubOperators.entrySet()) {
                    Preconditions.checkState(!newMap.containsKey(kv.getValue()));
                    ScalarOperator rewrittenOperator = rewriter.rewrite(kv.getKey(), rules);
                    newCommonMap.put(kv.getValue(), rewrittenOperator);
                }

                return new Projection(newMap, newCommonMap);
            }
            return projection;
        }
    }
}
