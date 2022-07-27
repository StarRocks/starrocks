// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite.physical;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.PredicateStatisticsCalculator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Predicate reorder
 * Evaluate the selectivity of child.
 */
public class PredicateReorderRule implements PhysicalOperatorTreeRewriteRule {
    public static final PredicateReorderVisitor handler = new PredicateReorderVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (taskContext.getOptimizerContext().getSessionVariable().isEnablePredicateReorder()) {
            root.getOp().accept(handler, root, null);
        }
        return root;
    }

    private static class PredicateReorderVisitor extends OptExpressionVisitor<OptExpression, Void> {
        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            for (int i = 0; i < optExpression.arity(); ++i) {
                OptExpression inputOptExpression = optExpression.inputAt(i);
                inputOptExpression.getOp().accept(this, inputOptExpression, null);
            }
            return predicateRewrite(optExpression);
        }

        private OptExpression predicateRewrite(OptExpression optExpression) {
            ScalarOperator predicate = optExpression.getOp().getPredicate();
            //check predicate type
            if (!(predicate instanceof CompoundPredicateOperator)) {
                return optExpression;
            }
            CompoundPredicateOperator compoundPredicateOperator = (CompoundPredicateOperator) predicate;
            if (!compoundPredicateOperator.isAnd()) {
                return optExpression;
            }

            // process statistics
            List<OptExpression> childOptExpressions = optExpression.getInputs();
            Statistics.Builder statisticsBuilder = Statistics.builder();
            Preconditions.checkState(childOptExpressions != null);
            if (childOptExpressions.size() > 0) {
                childOptExpressions.forEach(child -> {
                    statisticsBuilder.addColumnStatistics(child.getStatistics().getColumnStatistics());
                });
            } else {
                if (optExpression.getOp() instanceof PhysicalOlapScanOperator) {
                    PhysicalOlapScanOperator olapScanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
                    Table table = olapScanOperator.getTable();
                    Set<ColumnRefOperator> columnRefOperators =
                            optExpression.getStatistics().getColumnStatistics().keySet();
                    for (ColumnRefOperator column : columnRefOperators) {
                        ColumnStatistic columnStatistic = GlobalStateMgr.getCurrentStatisticStorage().
                                getColumnStatistic(table, column.getName());
                        statisticsBuilder.addColumnStatistic(column, columnStatistic);
                    }
                } else {
                    //other scan no support
                    return optExpression;
                }
            }
            statisticsBuilder.setOutputRowCount(optExpression.getStatistics().getOutputRowCount());
            Statistics statistics = statisticsBuilder.build();
            //reorder predicate
            optExpression.getOp().setPredicate(predicateReorder(compoundPredicateOperator, statistics));
            return optExpression;
        }

        private ScalarOperator predicateReorder(ScalarOperator scalarOperator, Statistics statistics) {
            // do not reorder predicate if statistics has UNKNOWN
            if (statistics.getColumnStatistics().values().stream().anyMatch(ColumnStatistic::isUnknown)) {
                return scalarOperator;
            }
            // todo need to think partition columns & distribution columns & index
            // get conjunctive predicate
            List<ScalarOperator> conjunctiveScalarOperators = Utils.extractConjuncts(scalarOperator);
            if (conjunctiveScalarOperators.size() <= 1) {
                return scalarOperator;
            } else {
                conjunctiveScalarOperators.sort(
                        Comparator.comparingDouble(predicate -> {
                            double predicateRowCount = PredicateStatisticsCalculator.
                                    statisticsCalculate(predicate, statistics).getOutputRowCount();
                            if (Double.isNaN(predicateRowCount)) {
                                return 0;
                            }
                            return predicateRowCount / Math.max(1.0, statistics.getOutputRowCount());
                        }));
                return Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, conjunctiveScalarOperators);
            }
        }
    }
}
