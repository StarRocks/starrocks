// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.planner.stream;

import com.starrocks.analysis.TupleId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.thrift.TPlanNode;

import java.util.ArrayList;

public class StreamJoinNode extends PlanNode {

    public StreamJoinNode(PlanNodeId id, ArrayList<TupleId> tupleIds,
                          String planNodeName) {
        super(id, tupleIds, planNodeName);
    }

    @Override
    protected void toThrift(TPlanNode msg) {

    }
}
