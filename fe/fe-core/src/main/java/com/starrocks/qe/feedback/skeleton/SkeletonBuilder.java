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

package com.starrocks.qe.feedback.skeleton;

import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.proto.NodeExecStatsItemPB;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.plan.ExecPlan;

import java.util.List;
import java.util.Map;

public class SkeletonBuilder extends OptExpressionVisitor<SkeletonNode, SkeletonNode> {

    private Map<Integer, NodeExecStats> nodeExecStatsMap = Maps.newHashMap();

    private final Map<Integer, SkeletonNode> skeletonNodeMap = Maps.newHashMap();

    public SkeletonBuilder() {

    }

    public SkeletonBuilder(List<NodeExecStatsItemPB> nodeExecStatsItems) {
        for (NodeExecStatsItemPB itemPB : nodeExecStatsItems) {
            nodeExecStatsMap.put(itemPB.getNodeId(), NodeExecStats.buildFromPB(itemPB));
        }
    }

    public SkeletonBuilder(Map<Integer, NodeExecStats> nodeExecStatsMap) {
        this.nodeExecStatsMap = nodeExecStatsMap;
    }

    public Pair<SkeletonNode, Map<Integer, SkeletonNode>> buildSkeleton(OptExpression root) {
        // buildSkeleton is invoked at two stages:
        // - Query execution completion phase: Analyze the query based on actual execution information to produce tuning guides.
        // - Rewrite phase: Rewrite the query according to the tuning guides. At this stage, operator IDs are not yet set,
        //   since they are assigned only when the FragmentBuilder decomposes the physical plan into fragments.
        // Therefore, assign operator ids temporally if it doesn't exist.
        if (root.getOp().getOperatorId() == Operator.ABSENT_OPERATOR_ID) {
            ExecPlan.assignOperatorIds(root);
        }
        SkeletonNode skeletonRoot = root.getOp().accept(this, root, null);
        return Pair.create(skeletonRoot, skeletonNodeMap);
    }

    private void visitChildren(SkeletonNode parent, List<OptExpression> optExpressions) {
        for (OptExpression optExpression : optExpressions) {
            SkeletonNode node = optExpression.getOp().accept(this, optExpression, parent);
            parent.addChild(node);
            skeletonNodeMap.putIfAbsent(optExpression.getOp().getPlanNodeId(), node);
        }
    }

    @Override
    public SkeletonNode visit(OptExpression optExpression, SkeletonNode parent) {
        int planNodeId = optExpression.getOp().getPlanNodeId();
        SkeletonNode node = new SkeletonNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
        visitChildren(node, optExpression.getInputs());
        skeletonNodeMap.putIfAbsent(planNodeId, node);
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalScan(OptExpression optExpression, SkeletonNode parent) {
        int planNodeId = optExpression.getOp().getPlanNodeId();
        ScanNode node = new ScanNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
        skeletonNodeMap.putIfAbsent(planNodeId, node);
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalJoin(OptExpression optExpression, SkeletonNode parent) {
        int planNodeId = optExpression.getOp().getPlanNodeId();
        JoinNode node = new JoinNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
        visitChildren(node, optExpression.getInputs());
        skeletonNodeMap.putIfAbsent(planNodeId, node);
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalHashAggregate(OptExpression optExpression, SkeletonNode parent) {
        int planNodeId = optExpression.getOp().getPlanNodeId();
        PhysicalHashAggregateOperator aggOperator = (PhysicalHashAggregateOperator) optExpression.getOp();
        if (aggOperator.getType().isAnyGlobal()) {
            BlockingAggNode node = new BlockingAggNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
            visitChildren(node, optExpression.getInputs());
            skeletonNodeMap.putIfAbsent(planNodeId, node);
            return node;
        } else {
            StreamingAggNode node = new StreamingAggNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
            visitChildren(node, optExpression.getInputs());
            skeletonNodeMap.putIfAbsent(planNodeId, node);
            return node;
        }
    }

    @Override
    public SkeletonNode visitPhysicalDistribution(OptExpression optExpression, SkeletonNode parent) {
        int planNodeId = optExpression.getOp().getPlanNodeId();
        DistributionNode node = new DistributionNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
        visitChildren(node, optExpression.getInputs());
        skeletonNodeMap.putIfAbsent(planNodeId, node);
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalCTEAnchor(OptExpression optExpression, SkeletonNode parent) {
        SkeletonNode node = new SkeletonNode(optExpression, null, parent);
        visitChildren(node, optExpression.getInputs());
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalCTEProduce(OptExpression optExpression, SkeletonNode parent) {
        SkeletonNode node = new SkeletonNode(optExpression, null, parent);
        visitChildren(node, optExpression.getInputs());
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalNoCTE(OptExpression optExpression, SkeletonNode parent) {
        SkeletonNode node = new SkeletonNode(optExpression, null, parent);
        visitChildren(node, optExpression.getInputs());
        return node;
    }
}
