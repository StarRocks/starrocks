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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.List;
import java.util.Optional;

public class RightChildEstimationErrorTuningGuide extends JoinTuningGuide {

    public RightChildEstimationErrorTuningGuide(JoinNode joinNode, EstimationErrorType type) {
        this.joinNode = joinNode;
        this.type = type;
    }

    @Override
    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("Reason: ");
        sb.append("Right child statistics of JoinNode ").append(joinNode.getNodeId());
        sb.append(" had been ");
        if (type == EstimationErrorType.RIGHT_INPUT_UNDERESTIMATED) {
            sb.append("underestimated.");
        } else if (type == EstimationErrorType.RIGHT_INPUT_OVERESTIMATED) {
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


        if (type == EstimationErrorType.RIGHT_INPUT_UNDERESTIMATED) {
            if (optExpression.isExistRequiredDistribution()) {
                return Optional.empty();
            }
            if (isBroadcastJoin(rightChild)) {
                if (leftNodeExecStats.getPullRows() < BROADCAST_THRESHOLD && leftSize < rightSize
                        && !commuteJoinHelper.onlyShuffle()) {
                    // original plan: small table inner join large table(broadcast)
                    // rewrite to: large table inner join small table(broadcast)
                    PhysicalDistributionOperator broadcastOp = new PhysicalDistributionOperator(
                            DistributionSpec.createReplicatedDistributionSpec());
                    OptExpression newRightChild = OptExpression.builder().with(leftChild)
                            .setOp(broadcastOp)
                            .setInputs(List.of(leftChild))
                            .build();

                    return Optional.of(OptExpression.builder().with(optExpression)
                            .setRequiredProperties(List.of(rightChild.inputAt(0).getOutputProperty(),
                                    createBroadcastPropertySet()))
                            .setOp(buildJoinOperator(joinOperator, true))
                            .setInputs(List.of(rightChild.inputAt(0), newRightChild))
                            .build());
                } else if (leftSize < rightSize && !commuteJoinHelper.onlyBroadcast()) {
                    // original plan: medium table inner join large table(broadcast)
                    // rewrite to: large table(shuffle) inner join medium table(shuffle)
                    PhysicalDistributionOperator leftExchangeOp = new PhysicalDistributionOperator(
                            DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(commuteJoinHelper.getLeftCols(),
                                    HashDistributionDesc.SourceType.SHUFFLE_JOIN)));

                    PhysicalDistributionOperator rightExchangeOp = new PhysicalDistributionOperator(
                            DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(commuteJoinHelper.getRightCols(),
                                    HashDistributionDesc.SourceType.SHUFFLE_JOIN)));
                    OptExpression newLeftChild = OptExpression.builder().with(rightChild)
                            .setOp(leftExchangeOp)
                            .setInputs(rightChild.getInputs())
                            .build();

                    OptExpression newRightChild = OptExpression.builder().with(leftChild)
                            .setOp(rightExchangeOp)
                            .setInputs(List.of(leftChild))
                            .build();

                    return Optional.of(OptExpression.builder().with(optExpression)
                            .setOp(buildJoinOperator(joinOperator, true))
                            .setRequiredProperties(List.of(createShufflePropertySet(leftExchangeOp.getDistributionSpec()),
                                    createShufflePropertySet(rightExchangeOp.getDistributionSpec())))
                            .setInputs(Lists.newArrayList(newLeftChild, newRightChild))
                            .build());
                } else if (leftSize >= rightSize && !originalHelper.onlyBroadcast()) {
                    // original plan: large table1 inner join large table2(broadcast)
                    // rewrite to: large table1(shuffle) inner join large table2(shuffle)
                    PhysicalDistributionOperator leftExchangeOp = new PhysicalDistributionOperator(
                            DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(originalHelper.getLeftCols(),
                                    HashDistributionDesc.SourceType.SHUFFLE_JOIN)));

                    PhysicalDistributionOperator rightExchangeOp = new PhysicalDistributionOperator(
                            DistributionSpec.createHashDistributionSpec(new HashDistributionDesc(originalHelper.getRightCols(),
                                    HashDistributionDesc.SourceType.SHUFFLE_JOIN)));
                    OptExpression newLeftChild = OptExpression.builder().with(leftChild)
                            .setOp(leftExchangeOp)
                            .setInputs(List.of(leftChild))
                            .build();

                    OptExpression newRightChild = OptExpression.builder().with(rightChild)
                            .setOp(rightExchangeOp)
                            .setInputs(rightChild.getInputs())
                            .build();

                    return Optional.of(OptExpression.builder().with(optExpression)
                            .setOp(buildJoinOperator(joinOperator, false))
                            .setRequiredProperties(List.of(createShufflePropertySet(leftExchangeOp.getDistributionSpec()),
                                    createShufflePropertySet(rightExchangeOp.getDistributionSpec())))
                            .setInputs(Lists.newArrayList(newLeftChild, newRightChild))
                            .build());
                }
            } else if (isShuffleJoin(leftChild, rightChild)) {
                if (leftNodeExecStats.getPullRows() < BROADCAST_THRESHOLD &&
                        leftSize < rightSize && !commuteJoinHelper.onlyShuffle()) {
                    if (optExpression.isExistRequiredDistribution()) {
                        return Optional.empty();
                    }

                    // original plan: small table(shuffle) inner join large table(shuffle)
                    // rewrite to: large table inner join small table(broadcast)
                    PhysicalDistributionOperator broadcastOp = new PhysicalDistributionOperator(
                            DistributionSpec.createReplicatedDistributionSpec());
                    OptExpression newRightChild = OptExpression.builder().with(leftChild)
                            .setOp(broadcastOp)
                            .setInputs(leftChild.getInputs())
                            .build();
                    return Optional.of(OptExpression.builder().with(optExpression)
                            .setOp(buildJoinOperator(joinOperator, true))
                            .setRequiredProperties(List.of(rightChild.inputAt(0).getOutputProperty(),
                                    createBroadcastPropertySet()))
                            .setInputs(Lists.newArrayList(rightChild.inputAt(0), newRightChild))
                            .build());
                } else if (leftSize < rightSize && !commuteJoinHelper.onlyBroadcast()) {
                    // original plan: medium table(shuffle) inner join large table(shuffle)
                    // rewrite to: large table(shuffle) inner join medium table(shuffle)
                    return Optional.of(OptExpression.builder().with(optExpression)
                            .setOp(buildJoinOperator(joinOperator, true))
                            .setInputs(Lists.newArrayList(rightChild, leftChild))
                            .build());
                }
            } else if (isColocateJoin(optExpression)) {
                if (leftSize < rightSize && !commuteJoinHelper.onlyBroadcast()) {
                    // original plan: medium table colocate join large table
                    // rewrite to: large table colocate join medium table
                    return Optional.of(OptExpression.builder().with(optExpression)
                            .setOp(buildJoinOperator(joinOperator, true))
                            .setRequiredProperties(List.of(rightChild.getOutputProperty(), leftChild.getOutputProperty()))
                            .setInputs(List.of(rightChild, leftChild))
                            .build());
                }
            }
        } else if (type == EstimationErrorType.RIGHT_INPUT_OVERESTIMATED) {
            if (isShuffleJoin(leftChild, rightChild)) {
                if (rightNodeExecStats.getPushRows() < BROADCAST_THRESHOLD && !originalHelper.onlyShuffle()) {
                    if (optExpression.isExistRequiredDistribution()) {
                        return Optional.empty();
                    }
                    // original plan: large table(shuffle) inner join small table(shuffle)
                    // rewrite to: large table inner join small table(broadcast)
                    PhysicalDistributionOperator broadcastOp = new PhysicalDistributionOperator(
                            DistributionSpec.createReplicatedDistributionSpec());

                    OptExpression newRightChild = OptExpression.builder().with(rightChild)
                            .setOp(broadcastOp)
                            .setInputs(rightChild.getInputs())
                            .build();

                    return Optional.of(OptExpression.builder().with(optExpression)
                            .setOp(buildJoinOperator(joinOperator, false))
                            .setRequiredProperties(List.of(leftChild.inputAt(0).getOutputProperty(),
                                    createBroadcastPropertySet()))
                            .setInputs(Lists.newArrayList(leftChild.getInputs().get(0), newRightChild))
                            .build());
                }
            }
        }

        return Optional.empty();
    }
}
