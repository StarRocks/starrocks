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
import com.starrocks.qe.feedback.guide.StreamingAggTuningGuide;
import com.starrocks.qe.feedback.skeleton.BlockingAggNode;
import com.starrocks.qe.feedback.skeleton.DistributionNode;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.qe.feedback.skeleton.StreamingAggNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;

import java.util.Map;

public class StreamingAggTuningAnalyzer implements PlanTuningAnalyzer.Analyzer {

    private static final StreamingAggTuningAnalyzer INSTANCE = new StreamingAggTuningAnalyzer();

    private StreamingAggTuningAnalyzer() {

    }

    public static StreamingAggTuningAnalyzer getInstance() {
        return INSTANCE;
    }

    @Override
    public void analyze(OptExpression root, Map<Integer, SkeletonNode> skeletonNodeMap, OperatorTuningGuides tuningGuides) {
        Analyzer analyzer = new Analyzer(skeletonNodeMap, tuningGuides);
        root.getOp().accept(analyzer, root, null);
    }

    private static class Analyzer extends OptExpressionVisitor<Void, Void> {

        private final Map<Integer, SkeletonNode> skeletonNodeMap;

        private final OperatorTuningGuides tuningGuides;

        private static final double STREAMING_AGGREGATION_THRESHOLD = 2.5;
        private static final double AGGREGATION_THRESHOLD = STREAMING_AGGREGATION_THRESHOLD * 10;

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
        public Void visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            PhysicalHashAggregateOperator operator = (PhysicalHashAggregateOperator) optExpression.getOp();
            int nodeId = operator.getPlanNodeId();
            SkeletonNode skeletonNode = skeletonNodeMap.get(nodeId);
            Preconditions.checkState(skeletonNode != null);
            if (skeletonNode instanceof StreamingAggNode) {
                double inputRows = skeletonNode.getNodeExecStats().getPushRows();
                double streamingOutputRows = skeletonNode.getNodeExecStats().getPullRows();
                BlockingAggNode blockingAggNode = findBlockingAggNode(skeletonNode);
                if (blockingAggNode != null) {
                    double blockingGroupCount = groupCountOf(blockingAggNode);
                    // If the streaming aggregation has poor aggregation effectiveness while the final blocking aggregation
                    // performs exceptionally well, switch the streaming aggregation to `force_preaggregation` mode.
                    if (blockingGroupCount > 0
                            && streamingOutputRows > 0
                            && blockingGroupCount < inputRows
                            && (inputRows / streamingOutputRows) < STREAMING_AGGREGATION_THRESHOLD
                            && (inputRows / blockingGroupCount) > AGGREGATION_THRESHOLD) {
                        tuningGuides.addTuningGuide(skeletonNode.getNodeId(), skeletonNode.getOperatorId(),
                                new StreamingAggTuningGuide((StreamingAggNode) skeletonNode));
                    }
                }
            }
            visit(optExpression, context);
            return null;
        }

        // Runtime filters, then HAVING, are applied to the completed hash map output, so a row dropped
        // by either is a group. The two counters partition that output because HAVING only sees what
        // the filter left, so the three values sum to the cardinality. NodeExecStats carries -1 only
        // as a whole via EMPTY, which the caller's positive check rejects.
        private static double groupCountOf(BlockingAggNode blockingAggNode) {
            NodeExecStats stats = blockingAggNode.getNodeExecStats();
            long pullRows = Math.max(0L, stats.getPullRows());
            long predFilterRows = Math.max(0L, stats.getPredFilterRows());
            long rfFilterRows = Math.max(0L, stats.getRfFilterRows());
            return (double) pullRows + predFilterRows + rfFilterRows;
        }

        private BlockingAggNode findBlockingAggNode(SkeletonNode streamingAggNode) {
            SkeletonNode parent = streamingAggNode.getParent();
            while (parent != null) {
                if (parent instanceof BlockingAggNode) {
                    return (BlockingAggNode) parent;
                } else if (parent instanceof DistributionNode) {
                    parent = parent.getParent();
                } else {
                    parent = null;
                }

            }
            return null;
        }
    }
}
