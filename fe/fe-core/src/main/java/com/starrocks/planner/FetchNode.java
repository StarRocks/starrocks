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

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.RowPositionDescriptor;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TFetchNode;
import com.starrocks.thrift.TNodesInfo;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import software.amazon.awssdk.services.gamelift.model.Compute;
import software.amazon.awssdk.services.lexruntimev2.model.Slot;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FetchNode extends PlanNode {
    PlanNodeId targetNodeId;
    List<TupleDescriptor> descs;
    // row position desc for each table
    Map<TupleId, RowPositionDescriptor> rowPosDescs;
    private ComputeResource computeResource = WarehouseManager.DEFAULT_RESOURCE;

    public FetchNode(PlanNodeId id, PlanNode inputNode,
                     PlanNodeId targetNodeId, List<TupleDescriptor> descs,
                     Map<TupleId, RowPositionDescriptor> rowPosDescs, ComputeResource computeResource) {
        super(id, inputNode, "FETCH");
        addChild(inputNode);
        this.targetNodeId = targetNodeId;
        this.descs = descs;
        this.tupleIds.addAll(descs.stream().map(tupleDescriptor -> tupleDescriptor.getId()).collect(Collectors.toList()));
        this.rowPosDescs = rowPosDescs;
        this.computeResource = computeResource;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.FETCH_NODE;
        msg.fetch_node = new TFetchNode();
        msg.fetch_node.setTarget_node_id(targetNodeId.asInt());
        msg.fetch_node.row_pos_descs = new HashMap<>();
        rowPosDescs.forEach((tupleId, rowPosDescs) -> {
            msg.fetch_node.row_pos_descs.put(tupleId.asInt(), rowPosDescs.toThrift());
        });
        // @TODO refactor nodes info
        msg.fetch_node.nodes_info = GlobalStateMgr.getCurrentState().createNodesInfo(computeResource,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
    }

    @Override
    protected String debugString() {
        return "FetchNode";
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("lookup node: ").append(targetNodeId).append("\n");
        for (TupleDescriptor tupleDesc : descs) {
            Table table = tupleDesc.getTable();
            output.append(prefix).append("table: ").append(table.getName()).append("\n");
            Set<SlotId> lookupRefSlots = new HashSet<>();
            RowPositionDescriptor rowPositionDescriptor = rowPosDescs.get(tupleDesc.getId());
            lookupRefSlots.addAll(rowPositionDescriptor.getLookupRefSlots());

            if (detailLevel.equals(TExplainLevel.VERBOSE)) {
                // output row id slot
                output.append(prefix + " <slot ")
                        .append(rowPositionDescriptor.getRowSourceSlot().asInt()).append("> => ROW_SOURCE_ID").append("\n");
                output.append(prefix + "  <row position slots> => " +
                        rowPositionDescriptor.getFetchRefSlots().stream()
                                .map(slotId -> slotId.toString()).collect(
                                        Collectors.joining(",", "[", "]"))).append("\n");
            }
            List<SlotDescriptor> slotDescs = tupleDesc.getSlots();
            if (!detailLevel.equals(TExplainLevel.VERBOSE)) {
                slotDescs = slotDescs.stream().filter(slotDescriptor -> !lookupRefSlots.contains(slotDescriptor.getId()))
                        .collect(Collectors.toList());
            }
            for (SlotDescriptor slotDesc : slotDescs) {
                output.append(prefix + "  <slot ").
                        append(slotDesc.getId()).append("> => ");
                if (!detailLevel.equals(TExplainLevel.VERBOSE)) {
                    output.append(slotDesc.getColumn().getName()).append("\n");
                } else {
                    output.append("[" + slotDesc.getColumn().getName() +
                            ", " + slotDesc.getType() + ", " + slotDesc.getIsNullable() + "]\n");
                }
            }
        }
        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    @Override
    public List<SlotId> getOutputSlotIds(DescriptorTable descriptorTable) {
        return null;
    }

}

