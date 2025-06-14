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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/SelectNode.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotId;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TNormalSelectNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TSelectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Node that applies conjuncts and a limit clause. Has exactly one child.
 */
public class SelectNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(SelectNode.class);
    private Map<SlotId, Expr> commonSlotMap;

    public SelectNode(PlanNodeId id, PlanNode child, List<Expr> conjuncts) {
        super(id, child.getTupleIds(), "SELECT");
        addChild(child);
        this.nullableTupleIds = child.nullableTupleIds;
        this.conjuncts.addAll(conjuncts);
    }

    public void setCommonSlotMap(Map<SlotId, Expr> commonSlotMap) {
        this.commonSlotMap = commonSlotMap;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.SELECT_NODE;
        msg.select_node = new TSelectNode();
        if (commonSlotMap != null) {
            commonSlotMap.forEach((key, value) -> msg.select_node.putToCommon_slot_map(key.asInt(), value.treeToThrift()));
        }
    }

    @Override
    public void init(Analyzer analyzer) throws StarRocksException {
    }

    @Override
    public void computeStats(Analyzer analyzer) {
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalSelectNode selectNode = new TNormalSelectNode();
        if (commonSlotMap != null) {
            Pair<List<Integer>, List<ByteBuffer>> slotIdsAndExprs = normalizer.normalizeSlotIdsAndExprs(commonSlotMap);
            selectNode.setCse_slot_ids(slotIdsAndExprs.first);
            selectNode.setCse_exprs(slotIdsAndExprs.second);
        }
        planNode.setSelect_node(selectNode);
        planNode.setNode_type(TPlanNodeType.SELECT_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!conjuncts.isEmpty()) {
            output.append(prefix + "predicates: " + getExplainString(conjuncts) + "\n");
            if (commonSlotMap != null && !commonSlotMap.isEmpty()) {
                output.append(prefix + "  common sub expr:" + "\n");
                for (Map.Entry<SlotId, Expr> entry : commonSlotMap.entrySet()) {
                    output.append(prefix + "  <slot " + entry.getKey().toString() + "> : "
                            + getExplainString(Arrays.asList(entry.getValue())) + "\n");
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
    public boolean needCollectExecStats() {
        return true;
    }
}
