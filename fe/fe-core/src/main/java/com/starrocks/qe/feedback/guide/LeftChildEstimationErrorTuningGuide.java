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
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;

import java.util.List;
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


    // The runtime filter may help reduce a lot of rows in left child which makes the pull rows of left child
    // is far less than the statistics estimated rows. So here we just change the broadcast join to shuffle
    // join when the right child output rows is larger than BROADCAST_THRESHOLD.
    @Override
    public Optional<OptExpression> applyImpl(OptExpression optExpression) {
        if (optExpression.getOp().getOpType() != OperatorType.PHYSICAL_HASH_JOIN) {
            return Optional.empty();
        }

        PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) optExpression.getOp();
        SkeletonNode rightChildNode = joinNode.getChild(1);

        NodeExecStats rightNodeExecStats = rightChildNode.getNodeExecStats();

        OptExpression leftChild = optExpression.getInputs().get(0);
        OptExpression rightChild = optExpression.getInputs().get(1);

        JoinHelper originalHelper = JoinHelper.of(joinOperator, leftChild.getRowOutputInfo().getOutputColumnRefSet(),
                rightChild.getRowOutputInfo().getOutputColumnRefSet());

        if (type == EstimationErrorType.LEFT_INPUT_OVERESTIMATED) {
            if (optExpression.isExistRequiredDistribution()) {
                return Optional.empty();
            }
            if (isBroadcastJoin(rightChild) && rightNodeExecStats.getPullRows() >= BROADCAST_THRESHOLD) {
                if (!originalHelper.onlyBroadcast()) {
                    // original plan: large table inner join large table(broadcast)
                    // rewrite to: large table(shuffle) inner large table(shuffle)
                    PhysicalDistributionOperator leftExchangeOp = new PhysicalDistributionOperator(
                            DistributionSpec.createHashDistributionSpec(
                                    new HashDistributionDesc(originalHelper.getLeftCols(),
                                            HashDistributionDesc.SourceType.SHUFFLE_JOIN)));

                    PhysicalDistributionOperator rightExchangeOp = new PhysicalDistributionOperator(
                            DistributionSpec.createHashDistributionSpec(
                                    new HashDistributionDesc(originalHelper.getRightCols(),
                                            HashDistributionDesc.SourceType.SHUFFLE_JOIN)));
                    OptExpression newLeftChild = OptExpression.builder().with(leftChild)
                            .setOp(leftExchangeOp)
                            .setRequiredProperties(List.of(PhysicalPropertySet.EMPTY))
                            .setInputs(List.of(leftChild))
                            .build();

                    OptExpression newRightChild = OptExpression.builder().with(rightChild)
                            .setOp(rightExchangeOp)
                            .setRequiredProperties(List.of(PhysicalPropertySet.EMPTY))
                            .setInputs(rightChild.getInputs())
                            .build();

                    return Optional.of(OptExpression.builder().with(optExpression)
                            .setOp(buildJoinOperator(joinOperator, false))
                            .setRequiredProperties(
                                    List.of(createShufflePropertySet(leftExchangeOp.getDistributionSpec()),
                                            createShufflePropertySet(rightExchangeOp.getDistributionSpec())))
                            .setInputs(Lists.newArrayList(newLeftChild, newRightChild))
                            .build());
                }
            }
        }

        return Optional.empty();
    }
}
