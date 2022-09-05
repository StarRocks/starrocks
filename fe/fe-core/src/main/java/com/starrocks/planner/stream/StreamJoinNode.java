// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner.stream;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.TableRef;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.thrift.TPlanNode;
import org.apache.commons.collections.ListUtils;

public class StreamJoinNode extends JoinNode {

    public StreamJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef) {
        // TODO(mofei)
        super("STREAM JOIN", id, outer, inner, JoinOperator.INNER_JOIN, ListUtils.EMPTY_LIST, ListUtils.EMPTY_LIST);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    protected void toThrift(TPlanNode msg) {

    }

    @Override
    public boolean canPushDownRuntimeFilter() {
        return false;
    }
}
