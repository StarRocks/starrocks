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

import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Objects;

public class SkewShuffleJoinEliminationRule implements TreeRewriteRule {

    private static final SkewShuffleJoinEliminationVisitor HANDLER = new SkewShuffleJoinEliminationVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        return root.getOp().accept(HANDLER, root, true);
    }

    private static class SkewShuffleJoinEliminationVisitor extends OptExpressionVisitor<OptExpression, Boolean> {
        @Override
        public OptExpression visit(OptExpression optExpr, Boolean parentRequireEmpty) {
            // default: parent doesn't require empty
            return visitChild(optExpr, false);
        }

        // visit all children of the given opt expression with the given parentRequireEmpty
        private OptExpression visitChild(OptExpression opt, Boolean parentRequireEmpty) {
            for (int idx = 0; idx < opt.arity(); ++idx) {
                OptExpression child = opt.inputAt(idx);
                opt.setChild(idx, child.getOp().accept(this, child, parentRequireEmpty));
            }
            return opt;
        }

        @Override
        public OptExpression visitPhysicalHashJoin(OptExpression opt, Boolean parentRequireEmpty) {
            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) opt.getOp();

            boolean requireEmptyForChild = isBroadCastJoin(opt);
            // doesn't support cross join and right join
            if (joinOperator.getJoinType().isCrossJoin() || joinOperator.getJoinType().isRightJoin()) {
                return visitChild(opt, requireEmptyForChild);
            }

            if (!isShuffleJoin(opt)) {
                return visitChild(opt, requireEmptyForChild);
            }

            if (!isSkew(opt)) {
                return visitChild(opt, requireEmptyForChild);
            }

            return null;
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression opt, Boolean parentRequireEmpty) {
            PhysicalHashAggregateOperator aggOperator = (PhysicalHashAggregateOperator) opt.getOp();
            boolean requireEmptyForChild = aggOperator.getType().isLocal();
            return visitChild(opt, requireEmptyForChild);
        }

        private boolean isShuffleJoin(OptExpression opt) {
            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) opt.getOp();
            // right now only support shuffle join
            if (!isExchangeWithDistributionType(opt.getInputs().get(0).getOp(),
                    DistributionSpec.DistributionType.SHUFFLE) ||
                    !isExchangeWithDistributionType(opt.getInputs().get(1).getOp(),
                            DistributionSpec.DistributionType.SHUFFLE)) {
                return false;
            }
            return true;
        }

        private boolean isBroadCastJoin(OptExpression opt) {
            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) opt.getOp();

            return isExchangeWithDistributionType(opt.getInputs().get(1).getOp(),
                    DistributionSpec.DistributionType.BROADCAST);
        }

        private boolean isSkew(OptExpression opt) {
            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) opt.getOp();
            // respect join hint
            if (joinOperator.getJoinHint().equals(JoinOperator.HINT_SKEW)) {
                return true;
            }
            // right now only support join hint
            return false;
        }

        private boolean isExchangeWithDistributionType(Operator child, DistributionSpec.DistributionType expectedType) {
            if (!(child.getOpType() == OperatorType.PHYSICAL_DISTRIBUTION)) {
                return false;
            }
            PhysicalDistributionOperator operator = (PhysicalDistributionOperator) child;
            return Objects.equals(operator.getDistributionSpec().getType(), expectedType);
        }
    }

}
