// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/AdapterNode.java

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

import com.starrocks.analysis.TupleId;
import com.starrocks.thrift.TAdapterNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.List;

/**
 * Used to convert column to row, itself should not change the plan,
 * so most of the logic is consistent with the child node
 */
public class AdapterNode extends PlanNode {

    protected final TupleId tupleId;

    public AdapterNode(PlanNodeId id, PlanNode node) {
        super(id, node.tupleIds, "ADAPTER");
        this.tupleId = node.tupleIds.get(0);
        this.children.add(node);
        this.setFragment(node.getFragment());
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.adapter_node = new TAdapterNode(tupleId.asInt());
        msg.node_type = TPlanNodeType.ADAPTER_NODE;
    }

    @Override
    public boolean isVectorized() {
        return false;
    }

    public static void insertAdapterNodeToFragment(List<PlanFragment> fragments, PlannerContext plannerContext) {
        for (PlanFragment fragment : fragments) {
            PlanNode root = fragment.getPlanRoot();

            if (root.isVectorized()) {
                root.setUseVectorized(true);
            } else {
                insertAdapterNodeToPlan(fragment.getPlanRoot(), plannerContext);
            }
        }

        // If OUTPUT EXPRS don't support Vectorized, we need to insert a AdapterNode
        if (fragments.get(0).getPlanRoot().isVectorized()) {
            if (fragments.get(0).isOutPutExprsVectorized()) {
                fragments.get(0).setOutPutExprsUseVectorized();
            } else {
                fragments.get(0)
                        .setPlanRoot(new AdapterNode(plannerContext.getNextNodeId(), fragments.get(0).getPlanRoot()));
            }
        }

        // If dest exchange node don't support Vectorized, we need to insert a AdapterNode
        // otherwise, we need to set fragment partition exprs use Vectorized
        for (PlanFragment fragment : fragments) {
            if (fragment.getPlanRoot().isVectorized()) {
                if (!fragment.isDestExchangeNodeVectorized() || !fragment.isOutputPartitionVectorized()) {
                    fragment.setPlanRoot(new AdapterNode(plannerContext.getNextNodeId(), fragment.getPlanRoot()));
                } else {
                    fragment.setOutputPartitionUseVectorized(true);
                }
            }
        }
    }

    private static void insertAdapterNodeToPlan(PlanNode parent, PlannerContext plannerContext) {
        for (int i = 0; i < parent.getChildren().size(); i++) {
            PlanNode child = parent.getChild(i);
            if (child instanceof AdapterNode) {
                continue;
            }

            if (child.isVectorized()) {
                child.setUseVectorized(true);
                parent.setChild(i, new AdapterNode(plannerContext.getNextNodeId(), child));
            } else {
                insertAdapterNodeToPlan(child, plannerContext);
            }
        }
    }
}
