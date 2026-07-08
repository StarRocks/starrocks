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
package com.starrocks.planner;

import com.starrocks.thrift.TMultiSinkDispatchNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.List;

// Option X (CK-compatible logical-sink MV JOIN/agg): the collector fragment's plan root. An N-ary NON-merging
// node whose children are the N ExchangeNodes (one per branch fragment). It produces no output tuple of its
// own; the BE MultiSinkDispatchNode decomposes each child into an ExchangeSource pipeline that the collector's
// MultiSink DataSink caps with its own OlapTableSink. See CH_REALTIME_MV_DESIGN.md sec 12/13.
public class MultiSinkDispatchNode extends PlanNode {

    public MultiSinkDispatchNode(PlanNodeId id, List<PlanNode> children) {
        super(id, "MULTI_SINK_DISPATCH");
        for (PlanNode child : children) {
            addChild(child);
        }
        // The node is non-merging and emits no tuple of its own, but TPlanNode.row_tuples is a
        // required thrift field and the BE ExecNode ctor builds its row descriptor from it. All
        // branches carry the same row shape, so mirror ExchangeNode.computeTupleIds() and adopt the
        // first child's tuple ids.
        if (!this.children.isEmpty()) {
            this.tupleIds.addAll(this.children.get(0).getTupleIds());
            this.nullableTupleIds.addAll(this.children.get(0).getNullableTupleIds());
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.MULTI_SINK_DISPATCH_NODE;
        msg.multi_sink_dispatch_node = new TMultiSinkDispatchNode();
    }
}
