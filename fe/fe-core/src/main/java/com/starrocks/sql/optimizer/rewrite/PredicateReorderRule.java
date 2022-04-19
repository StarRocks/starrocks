// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.common.FeConstants;
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
        if (FeConstants.OPEN_PREDICATE_REORDER) {
            root.getOp().accept(handler, root, null);
        }
        return root;
    }

    private static class PredicateReorderVisitor extends OptExpressionVisitor<OptExpression, Void> {
        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            for (int i = 0; i < optExpression.arity(); i++) {
                OptExpression inputOptExpression = optExpression.inputAt(i);
                inputOptExpression.getOp().accept(this, inputOptExpression, null);
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
            optExpression.getOp().setPredicate(predicateReorder(compoundPredicateOperator, optExpression.getStatistics()));
            return optExpression;
        }

        private ScalarOperator predicateReorder(ScalarOperator scalarOperator, Statistics statistics) {
            // todo need to think partition columns & distribution columns & index
            // get conjunctive predicate
            List<ScalarOperator> conjunctiveScalarOperators = Utils.extractConjuncts(scalarOperator);
            if (conjunctiveScalarOperators.size() <= 1) {
                return scalarOperator;
            } else {
                DefaultPredicateSelectivityEstimator selectivityEstimator = new DefaultPredicateSelectivityEstimator();
                conjunctiveScalarOperators.sort((o1, o2) -> {
                    if (selectivityEstimator.estimate(o1, statistics) > selectivityEstimator.estimate(o2, statistics)) {
                        return 1;
                    } else if (selectivityEstimator.estimate(o1, statistics) < selectivityEstimator.estimate(o2, statistics)) {
                        return -1;
                    } else {
                        return 0;
                    }
                });
                return Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, conjunctiveScalarOperators);
            }
        }
    }
}
