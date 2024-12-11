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

package com.starrocks.qe.feedback.analyzer;

import com.google.common.base.Preconditions;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.qe.feedback.OperatorTuningGuides;
import com.starrocks.qe.feedback.guide.LeftChildEstimationErrorTuningGuide;
import com.starrocks.qe.feedback.guide.RightChildEstimationErrorTuningGuide;
import com.starrocks.qe.feedback.skeleton.JoinNode;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Map;

import static com.starrocks.qe.feedback.guide.JoinTuningGuide.EstimationErrorType.LEFT_INPUT_OVERESTIMATED;
import static com.starrocks.qe.feedback.guide.JoinTuningGuide.EstimationErrorType.RIGHT_INPUT_OVERESTIMATED;
import static com.starrocks.qe.feedback.guide.JoinTuningGuide.EstimationErrorType.RIGHT_INPUT_UNDERESTIMATED;

public class JoinTuningAnalyzer implements PlanTuningAnalyzer.Analyzer {

    private static final JoinTuningAnalyzer INSTANCE = new JoinTuningAnalyzer();

    private static final long UNDERESTIMATED_FACTOR = 1000;

    private static final long LARGE_TABLE_ROWS_THRESHOLD = 5000000;

    public static JoinTuningAnalyzer getInstance() {
        return INSTANCE;
    }

    @Override
    public void analyze(OptExpression root, Map<Integer, SkeletonNode> skeletonNodeMap,
                        OperatorTuningGuides tuningGuides) {
        Analyzer analyzer = new Analyzer(skeletonNodeMap, tuningGuides);
        root.getOp().accept(analyzer, root, null);
    }

    private static class Analyzer extends OptExpressionVisitor<Void, Void> {
        private final Map<Integer, SkeletonNode> skeletonNodeMap;

        private final OperatorTuningGuides tuningGuides;

        public Analyzer(Map<Integer, SkeletonNode> skeletonNodeMap, OperatorTuningGuides tuningGuides) {
            this.skeletonNodeMap = skeletonNodeMap;
            this.tuningGuides = tuningGuides;
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                Operator operator = input.getOp();
                operator.accept(this, input, null);
            }
            return null;
        }

        @Override
        public Void visitPhysicalHashJoin(OptExpression optExpression, Void context) {
            PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) optExpression.getOp();
            if (!joinOperator.getJoinHint().isEmpty()) {
                return null;
            }
            SkeletonNode skeletonNode = skeletonNodeMap.get(joinOperator.getPlanNodeId());
            Preconditions.checkState(skeletonNode != null && skeletonNode instanceof JoinNode);
            Preconditions.checkState(skeletonNode.getChildren().size() == 2);

            JoinNode joinNode = (JoinNode) skeletonNode;
            NodeExecStats leftExecStats = joinNode.getChild(0).getNodeExecStats();
            NodeExecStats rightExecStats = joinNode.getChild(1).getNodeExecStats();

            Statistics leftStats = optExpression.getInputs().get(0).getStatistics();
            Statistics rightStats = optExpression.getInputs().get(1).getStatistics();

            if (rightExecStats.getPullRows() > rightStats.getOutputRowCount() * UNDERESTIMATED_FACTOR
                    && rightExecStats.getPullRows() > LARGE_TABLE_ROWS_THRESHOLD) {
                tuningGuides.addTuningGuide(joinNode.getNodeId(),
                        new RightChildEstimationErrorTuningGuide(joinNode, RIGHT_INPUT_UNDERESTIMATED));
            } else if (rightStats.getOutputRowCount() > rightExecStats.getPullRows() * UNDERESTIMATED_FACTOR) {
                tuningGuides.addTuningGuide(joinNode.getNodeId(),
                        new RightChildEstimationErrorTuningGuide(joinNode, RIGHT_INPUT_OVERESTIMATED));
            } else if (rightExecStats.getPullRows() > LARGE_TABLE_ROWS_THRESHOLD &&
                    leftStats.getOutputRowCount() > leftExecStats.getPullRows() * UNDERESTIMATED_FACTOR) {
                tuningGuides.addTuningGuide(joinNode.getNodeId(),
                        new LeftChildEstimationErrorTuningGuide(joinNode, LEFT_INPUT_OVERESTIMATED));
            }
            visit(optExpression, context);
            return null;
        }
    }
}
