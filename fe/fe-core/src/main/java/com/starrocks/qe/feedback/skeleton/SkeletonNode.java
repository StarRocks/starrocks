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

import com.starrocks.common.TreeNode;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Objects;

public class SkeletonNode extends TreeNode<SkeletonNode> {

    protected final int operatorId;

    // nodeId is not used in `hashCode` and `equals`, because rewrite phase has not set nodeIds to physical operators.
    protected final int nodeId;

    protected final OperatorType type;

    protected final long limit;

    protected final ScalarOperator predicate;

    protected final Statistics statistics;

    protected final SkeletonNode parent;

    protected final NodeExecStats nodeExecStats;

    public SkeletonNode(OptExpression optExpression, NodeExecStats nodeExecStats, SkeletonNode parent) {
        this.operatorId = optExpression.getOp().getOperatorId();
        this.nodeId = optExpression.getOp().getPlanNodeId();
        this.type = optExpression.getOp().getOpType();
        this.limit = optExpression.getOp().getLimit();
        this.predicate = optExpression.getOp().getPredicate();
        this.statistics = optExpression.getStatistics();
        this.parent = parent;
        if (nodeExecStats != null) {
            this.nodeExecStats = calibrateNodeExecStats(nodeExecStats, optExpression.getOp());
        } else {
            this.nodeExecStats = NodeExecStats.EMPTY;
        }
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getOperatorId() {
        return operatorId;
    }

    public SkeletonNode getParent() {
        return parent;
    }

    public NodeExecStats getNodeExecStats() {
        return nodeExecStats;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorId, type, limit, predicate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SkeletonNode that = (SkeletonNode) o;
        return operatorId == that.operatorId && limit == that.limit && type == that.type &&
                Objects.equals(predicate, that.predicate);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        explain(sb);
        return sb.toString();
    }

    public void explain(StringBuilder sb) {
        sb.append("Node ID: ").append(nodeId).append("\n");
        sb.append("Type: ").append(type).append("\n");
        sb.append("Child:").append("\n");
        for (SkeletonNode child : children) {
            child.explain(sb);
        }
    }

    private NodeExecStats calibrateNodeExecStats(NodeExecStats nodeExecStats, Operator operator) {
        NodeExecStats.Builder builder = NodeExecStats.Builder.buildFrom(nodeExecStats);

        // node may finish in advance because of this limit, the counter in BE may not been updated.
        // so we need to adjust the pull rows in FE.
        if (nodeExecStats.getPullRows() < limit) {
            builder.setPullRows(limit);
        }

        // broadcast distribution will accumulate all send rows to multiple BEs.
        // so we need to adjust the pull rows in FE.
        if (operator.getOpType() == OperatorType.PHYSICAL_DISTRIBUTION) {
            builder.setPullRows(nodeExecStats.getPushRows());
        }

        return nodeExecStats.getPullRows() < limit ?
                NodeExecStats.Builder.buildFrom(nodeExecStats).setPullRows(limit).build() : nodeExecStats;
    }
}
