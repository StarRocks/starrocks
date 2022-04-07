// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.ExpressionStatisticCalculator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
/**
 * Predicate reorder
 * Evaluate the selectivity of left child and right child , if selectivity of right child  < left child , swap it.
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
            if(predicate == null || !(predicate instanceof CompoundPredicateOperator)) {
                return optExpression;
            }
            CompoundPredicateOperator compoundPredicateOperator = (CompoundPredicateOperator) predicate;
            //reorder predicate
            predicateReorder(compoundPredicateOperator.getChildren(), optExpression.getStatistics());
            return optExpression;
        }

        private void predicateReorder(List<ScalarOperator> children, Statistics statistics) {
            if(children == null || children.size() == 0 ) {
                return;
            }
            if(children.size() == 2){
                ColumnStatistic leftColumnStatistics = ExpressionStatisticCalculator.calculate(children.get(0), statistics);
                ColumnStatistic rightColumnStatistics = ExpressionStatisticCalculator.calculate(children.get(1), statistics);
                predicateReorder(children.get(0).getChildren(), statistics);
                predicateReorder(children.get(1).getChildren(), statistics);
                //todo compare left and right, if left selectivity > right selectivity, swap it
                // only use averageRowSize now, need design algorithm
                if(leftColumnStatistics.getAverageRowSize() > rightColumnStatistics.getAverageRowSize()) {
                    ScalarOperator tempScalarOperator = children.get(0);
                    children.set(0,children.get(1));
                    children.set(1,tempScalarOperator);
                }
            } else if(children.size() == 1){
                predicateReorder(children.get(0).getChildren(), statistics);
            } else {
                // must not here
            }
        }
    }
}
