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

package com.starrocks.qe.feedback.guide;

import com.google.common.collect.Lists;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.qe.feedback.skeleton.JoinNode;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Optional;

public class LeftChildEstimationErrorTuningGuide extends JoinTuningGuide {

    public LeftChildEstimationErrorTuningGuide(JoinNode joinNode, EstimationErrorType type) {
        this.joinNode = joinNode;
        this.type = type;
    }

    @Override
    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("Reason: ");
        sb.append("left child statistics of JoinNode ").append(joinNode.getNodeId());
        sb.append(" had been ");
        if (type == EstimationErrorType.LEFT_INPUT_UNDERESTIMATED) {
            sb.append("underestimated.");
        } else if (type == EstimationErrorType.LEFT_INPUT_OVERESTIMATED) {
            sb.append("overestimated.");
        }
        return sb.toString();
    }

    @Override
    public String getAdvice() {
        return "Advice: Adjust the distribution join execution type and join plan to improve the performance.";
    }

    @Override
    public Optional<OptExpression> applyImpl(OptExpression optExpression) {
        if (optExpression.getOp().getOpType() != OperatorType.PHYSICAL_HASH_JOIN) {
            return Optional.empty();
        }

        PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) optExpression.getOp();
        SkeletonNode leftChildNode = joinNode.getChild(0);
        SkeletonNode rightChildNode = joinNode.getChild(1);

        NodeExecStats leftNodeExecStats = leftChildNode.getNodeExecStats();
        NodeExecStats rightNodeExecStats = rightChildNode.getNodeExecStats();

        Statistics leftStats = leftChildNode.getStatistics();
        Statistics rightStats = rightChildNode.getStatistics();

        double leftSize = (leftNodeExecStats.getPullRows() + leftNodeExecStats.getRfFilterRows()) * leftStats.getAvgRowSize();
        double rightSize = rightNodeExecStats.getPullRows() * rightStats.getAvgRowSize();

        OptExpression leftChild = optExpression.getInputs().get(0);
        OptExpression rightChild = optExpression.getInputs().get(1);

        JoinHelper originalHelper = JoinHelper.of(joinOperator, leftChild.getRowOutputInfo().getOutputColumnRefSet(),
                rightChild.getRowOutputInfo().getOutputColumnRefSet());
        JoinHelper commuteJoinHelper = JoinHelper.of(joinOperator, rightChild.getRowOutputInfo().getOutputColumnRefSet(),
                leftChild.getRowOutputInfo().getOutputColumnRefSet());

        if (isBroadcastJoin(rightChild) && !optExpression.isExistRequiredDistribution()) {
            if (leftNodeExecStats.getPushRows() < BROADCAST_THRESHOLD && leftSize < rightSize
                    && !commuteJoinHelper.onlyShuffle()) {
                // original plan: small table inner join large table(broadcast)
                // rewrite to: large table inner join small table(broadcast)
                PhysicalDistributionOperator broadcastOp = new PhysicalDistributionOperator(
                        DistributionSpec.createReplicatedDistributionSpec());
                OptExpression newRightChild = OptExpression.builder().with(leftChild)
                        .setOp(broadcastOp)
                        .setInputs(Lists.newArrayList(leftChild))
                        .build();

                return Optional.of(OptExpression.builder().with(optExpression)
                        .setOp(buildJoinOperator(joinOperator, true))
                        .setInputs(Lists.newArrayList(rightChild.getInputs().get(0), newRightChild))
                        .build());
            }
        }

        return Optional.empty();
    }
}
