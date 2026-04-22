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

import com.starrocks.thrift.TEnforceUniqueNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.List;

/**
 * EnforceUniqueNode passes all rows through while checking that key columns are unique.
 * Used for Iceberg MERGE INTO to ensure that each target row (_file, _pos) is matched
 * by at most one source row. If a duplicate key is detected, execution fails with an error.
 */
public class EnforceUniqueNode extends PlanNode {

    private final List<Integer> uniqueKeyColIndices;

    public EnforceUniqueNode(PlanNodeId id, PlanNode child, List<Integer> uniqueKeyColIndices) {
        super(id, "ENFORCE UNIQUE");
        this.uniqueKeyColIndices = uniqueKeyColIndices;
        this.children.add(child);
        this.tupleIds.addAll(child.getTupleIds());
        this.nullableTupleIds.addAll(child.getNullableTupleIds());
    }

    /** Physical chunk indices of the unique-key columns — exposed for unit-test inspection. */
    public List<Integer> getUniqueKeyColIndices() {
        return uniqueKeyColIndices;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ENFORCE_UNIQUE_NODE;
        TEnforceUniqueNode node = new TEnforceUniqueNode();
        node.setUnique_key_col_indices(uniqueKeyColIndices);
        msg.setEnforce_unique_node(node);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("unique key columns: ").append(uniqueKeyColIndices).append("\n");
        return sb.toString();
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }
}
