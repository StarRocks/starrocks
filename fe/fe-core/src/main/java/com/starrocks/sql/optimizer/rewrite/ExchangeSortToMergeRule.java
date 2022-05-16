// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;

/**
 * Rewrite PhysicalDistribute with child topN(FINAL) to
 * two phase topN (partial -> final)
 * TOP-N not split to two phase may be constructed by property enforce
 */
public class ExchangeSortToMergeRule extends OptExpressionVisitor<OptExpression, Void> {
    public OptExpression rewrite(OptExpression optExpression) {
        return optExpression.getOp().accept(this, optExpression, null);
    }

    @Override
    public OptExpression visit(OptExpression optExpr, Void context) {
        for (int idx = 0; idx < optExpr.arity(); ++idx) {
            optExpr.setChild(idx, rewrite(optExpr.inputAt(idx)));
        }
        return optExpr;
    }

    @Override
    public OptExpression visitPhysicalDistribution(OptExpression optExpr, Void context) {
        if (optExpr.arity() == 1 && optExpr.inputAt(0).getOp() instanceof PhysicalTopNOperator) {
            PhysicalTopNOperator topN = (PhysicalTopNOperator) optExpr.inputAt(0).getOp();

            if (topN.getSortPhase().isFinal() && !topN.isSplit() && topN.getLimit() == Operator.DEFAULT_LIMIT) {
                OptExpression child = OptExpression.create(new PhysicalTopNOperator(
                        topN.getOrderSpec(), topN.getLimit(), topN.getOffset(), topN.getPartitionByColumns(),
                        SortPhase.PARTIAL, false, false, null, null
                ), optExpr.inputAt(0).getInputs());
                child.setLogicalProperty(optExpr.inputAt(0).getLogicalProperty());
                child.setStatistics(optExpr.getStatistics());

                OptExpression newOpt = OptExpression.create(new PhysicalTopNOperator(
                                topN.getOrderSpec(), topN.getLimit(), topN.getOffset(), topN.getPartitionByColumns(),
                                SortPhase.FINAL, true, false, null, topN.getProjection()),
                        Lists.newArrayList(child));
                newOpt.setLogicalProperty(optExpr.getLogicalProperty());
                newOpt.setStatistics(optExpr.getStatistics());

                return visit(newOpt, null);
            } else {
                return visit(optExpr, null);
            }
        }
        return visit(optExpr, null);
    }
}
