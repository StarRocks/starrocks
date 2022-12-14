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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

/**
 * Rewrite PhysicalDistribute with child topN(FINAL) to
 * two phase topN (partial -> final)
 * TOP-N not split to two phase may be constructed by property enforce
 */
public class ExchangeSortToMergeRule extends OptExpressionVisitor<OptExpression, Void> implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression optExpression, TaskContext taskContext) {
        return optExpression.getOp().accept(this, optExpression, null);
    }

    @Override
    public OptExpression visit(OptExpression optExpr, Void context) {
        for (int idx = 0; idx < optExpr.arity(); ++idx) {
            optExpr.setChild(idx, rewrite(optExpr.inputAt(idx), null));
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
                        topN.getPartitionLimit(), SortPhase.PARTIAL, topN.getTopNType(), false, false, null, null
                ), optExpr.inputAt(0).getInputs());
                child.setLogicalProperty(optExpr.inputAt(0).getLogicalProperty());
                child.setStatistics(optExpr.getStatistics());

                OptExpression newOpt = OptExpression.create(new PhysicalTopNOperator(
                                topN.getOrderSpec(), topN.getLimit(), topN.getOffset(), topN.getPartitionByColumns(),
                                topN.getPartitionLimit(), SortPhase.FINAL, topN.getTopNType(), true, false, null,
                                topN.getProjection()),
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
