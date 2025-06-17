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

import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Table;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TLookUpNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.sparkproject.guava.collect.Lists;
import software.amazon.awssdk.services.lexruntimev2.model.Slot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LookUpNode extends PlanNode {
    private List<TupleDescriptor> descs;
    private Map<TupleId, SlotId> rowidSlots;

    public LookUpNode(PlanNodeId id, List<TupleDescriptor> descs, Map<TupleId, SlotId> rowidSlots) {
        super(id, new ArrayList<>(descs.stream().map(desc -> desc.getId()).collect(Collectors.toList())), "LookUp");
        this.descs = descs;
        this.rowidSlots = rowidSlots;
    }

    public List<TupleDescriptor> getDescs() {
        return descs;
    }

    public Map<TupleId, SlotId> getRowidSlots() {
        return rowidSlots;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.LOOKUP_NODE;
        msg.look_up_node = new TLookUpNode();
        msg.look_up_node.row_id_slots = new HashMap<>();
        rowidSlots.forEach((tupleId, slotId) -> {
            msg.look_up_node.row_id_slots.put(tupleId.asInt(), slotId.asInt());
        });
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("LOOKUP\n");
        return output.toString();
    }
}
