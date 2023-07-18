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

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

/*
 * Because of local property, we could generate three/four stage plan like:
 * three stage:
 *      Agg(Global)
 *           |
 *   Agg(Distinct Global)
 *           |
 *      Agg(Local)
 *
 * four stage:
 *      Agg(Global)
 *           |
 *      Distribution
 *           |
 *   Agg（Distinct Local)
 *           |
 *  Agg (Distinct Global)
 *           |
 *       Agg (Local)
 * Because of there is no shuffle between the Agg(Distinct Global) and Agg(Local), the update/merge procedure is not
 * really required here. We could optimize the two aggregate node(Agg(Distinct Global) - Agg(Local)) to one aggregate node.
 * This optimization avoids serialization and deserialization of data.
 * Optimized plan：
 * three stage:
 *      Agg(Global)
 *           |
 *      Agg(Local)

 * four stage:
 *      Agg(Global)
 *          |
 *     Distribution
 *          |
 *   Agg（Distinct Local)
 *          |
 *      Agg (Local)
 **/
public class PruneAggregateNodeRule implements TreeRewriteRule {
    private static final PruneAggVisitor PRUNE_AGG_VISITOR = new PruneAggVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        // respect user session variable
        // runningUnitTest used for UT
        if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() != 0 && !FeConstants.runningUnitTest) {
            return root;
        }
        return root.getOp().accept(PRUNE_AGG_VISITOR, root, null);
    }

    private static class PruneAggVisitor extends OptExpressionVisitor<OptExpression, Void> {
        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            for (int i = 0; i < optExpression.arity(); ++i) {
                optExpression.setChild(i,
                        optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), null));
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            PhysicalHashAggregateOperator parentOperator = (PhysicalHashAggregateOperator) optExpression.getOp();
            Operator childOperator = optExpression.inputAt(0).getOp();

            if (parentOperator.getType().isDistinctGlobal() && childOperator instanceof PhysicalHashAggregateOperator) {
                PhysicalHashAggregateOperator hashAggregateOperator = (PhysicalHashAggregateOperator) childOperator;
                hashAggregateOperator.setMergedLocalAgg(true);
                hashAggregateOperator.setProjection(parentOperator.getProjection());
                return optExpression.inputAt(0);
            } else {
                return visit(optExpression, context);
            }
        }
    }
}
