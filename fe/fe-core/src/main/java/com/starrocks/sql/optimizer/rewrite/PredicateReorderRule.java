// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;

/**
 * Predicate reorder
 * Evaluate the selectivity of child.
 */
public class PredicateReorderRule implements PhysicalOperatorTreeRewriteRule {
    public static final PredicateReorderVisitor handler = new PredicateReorderVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(handler, root, null);
        return root;
    }

    private static class PredicateReorderVisitor extends OptExpressionVisitor<OptExpression, Void> {
        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            for (int i = 0; i < optExpression.arity(); ++i) {
                optExpression.setChild(i, optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), null));
            }
            return predicateRewrite(optExpression);
        }

        private OptExpression predicateRewrite(OptExpression optExpression) {
            ScalarOperator predicate = optExpression.getOp().getPredicate();
            //check predicate type
            if (predicate == null || !(predicate instanceof CompoundPredicateOperator)) {
                return optExpression;
            }
            CompoundPredicateOperator compoundPredicateOperator = (CompoundPredicateOperator) predicate;
            //reorder predicate
            predicateReorder(compoundPredicateOperator, optExpression.getStatistics());
            return optExpression;
        }

        private void predicateReorder(ScalarOperator scalarOperator, Statistics statistics) {
            // get conjunctive predicate
            List<ScalarOperator> scalarOperators = Utils.extractConjuncts(scalarOperator);
            if (scalarOperators.size() == 0) {
                return;
            }
            for (ScalarOperator operator : scalarOperators) {
                predicateReorder(operator, statistics);
            }
            if (scalarOperators.size() > 1) {
                DefaultPredicateSelectivityEstimator heuristic = new DefaultPredicateSelectivityEstimator();
                scalarOperators.sort((o1, o2) -> {
                    if (heuristic.estimate(o1, statistics) > heuristic.estimate(o2, statistics)) {
                        return 1;
                    }
                    return 0;
                });
            }
        }
    }
}
