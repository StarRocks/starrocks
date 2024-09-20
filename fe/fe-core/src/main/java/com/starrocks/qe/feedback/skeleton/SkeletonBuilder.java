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
import com.starrocks.common.Id;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.Pair;
import com.starrocks.proto.NodeExecStatsItemPB;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;

import java.util.List;
import java.util.Map;

public class SkeletonBuilder extends OptExpressionVisitor<SkeletonNode, SkeletonNode> {

    private Map<Integer, NodeExecStats> nodeExecStatsMap = Maps.newHashMap();

    private Map<Integer, SkeletonNode> skeletonNodeMap = Maps.newHashMap();

    private IdGenerator<SkeletonNodeId> idGenerator = SkeletonNodeId.createGenerator();


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
        fillNodeId(optExpression.getOp(), node);
        skeletonNodeMap.putIfAbsent(planNodeId, node);
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalScan(OptExpression optExpression, SkeletonNode parent) {
        int planNodeId = optExpression.getOp().getPlanNodeId();
        ScanNode node = new ScanNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
        fillNodeId(optExpression.getOp(), node);
        skeletonNodeMap.putIfAbsent(planNodeId, node);
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalJoin(OptExpression optExpression, SkeletonNode parent) {
        int planNodeId = optExpression.getOp().getPlanNodeId();
        JoinNode node = new JoinNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
        visitChildren(node, optExpression.getInputs());
        fillNodeId(optExpression.getOp(), node);
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
            fillNodeId(optExpression.getOp(), node);
            skeletonNodeMap.putIfAbsent(planNodeId, node);
            return node;
        } else {
            StreamingAggNode node = new StreamingAggNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
            visitChildren(node, optExpression.getInputs());
            fillNodeId(optExpression.getOp(), node);
            skeletonNodeMap.putIfAbsent(planNodeId, node);
            return node;
        }
    }

    @Override
    public SkeletonNode visitPhysicalDistribution(OptExpression optExpression, SkeletonNode parent) {
        int planNodeId = optExpression.getOp().getPlanNodeId();
        DistributionNode node = new DistributionNode(optExpression, nodeExecStatsMap.get(planNodeId), parent);
        visitChildren(node, optExpression.getInputs());
        fillNodeId(optExpression.getOp(), node);
        skeletonNodeMap.putIfAbsent(planNodeId, node);
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalCTEAnchor(OptExpression optExpression, SkeletonNode parent) {
        SkeletonNode node = new SkeletonNode(optExpression, null, parent);
        visitChildren(node, optExpression.getInputs());
        if (nodeExecStatsMap.isEmpty()) {
            node.setNodeId(node.getChild(1).getNodeId());
        }
        return node;
    }


    @Override
    public SkeletonNode visitPhysicalCTEProduce(OptExpression optExpression, SkeletonNode parent) {
        SkeletonNode node = new SkeletonNode(optExpression, null, parent);
        visitChildren(node, optExpression.getInputs());
        if (nodeExecStatsMap.isEmpty()) {
            node.setNodeId(node.getChild(0).getNodeId());
        }
        return node;
    }

    @Override
    public SkeletonNode visitPhysicalNoCTE(OptExpression optExpression, SkeletonNode parent) {
        SkeletonNode node = new SkeletonNode(optExpression, null, parent);
        visitChildren(node, optExpression.getInputs());
        if (nodeExecStatsMap.isEmpty()) {
            node.setNodeId(node.getChild(0).getNodeId());
        }
        return node;
    }

    private void fillNodeId(Operator operator, SkeletonNode node) {
        if (nodeExecStatsMap.isEmpty()) {
            node.setNodeId(idGenerator.getNextId().asInt());
            fillProjectionNodeId(operator);
        }
    }

    private void fillProjectionNodeId(Operator operator) {
        if (operator.getProjection() != null) {
            idGenerator.getNextId();
        }
    }

    public static class SkeletonNodeId extends Id<SkeletonNodeId> {
        public SkeletonNodeId(int id) {
            super(id);
        }

        public static IdGenerator<SkeletonNodeId> createGenerator() {
            return new IdGenerator<>() {
                @Override
                public SkeletonNodeId getNextId() {
                    return new SkeletonNodeId(nextId++);
                }

                @Override
                public SkeletonNodeId getMaxId() {
                    return new SkeletonNodeId(nextId - 1);
                }
            };
        }
    }
}
