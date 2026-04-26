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

package com.starrocks.qe.scheduler.dag;

import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.starrocks.planner.LookUpNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.system.ComputeNode;

import java.util.List;
import java.util.Set;

public class PreExecutionFragmentBuilder {
    private final List<ExecutionFragment> fragments;

    public PreExecutionFragmentBuilder(List<ExecutionFragment> fragments) {
        this.fragments = fragments;
    }

    public void build(ExecutionDAG dag) {
        for (ExecutionFragment fragment : dag.getPreExecutedFragments()) {
            final PlanNode leftMostNode = fragment.getLeftMostNode();
            Set<ComputeNode> computeNodeSet = Sets.newHashSet();
            if (leftMostNode instanceof LookUpNode lookup) {
                lookup.getRowPosDescs().forEach((id, desc) -> {
                    final int scanNodeId = desc.getScanNodeId();
                    final ExecutionFragment scan = getScanFragment(scanNodeId);
                    for (FragmentInstance instance : scan.getInstances()) {
                        computeNodeSet.add(instance.getWorker());
                    }
                });
            }
            for (ComputeNode computeNode : computeNodeSet) {
                fragment.addInstance(new FragmentInstance(computeNode, fragment));
            }
        }
    }

    private ExecutionFragment getScanFragment(int scanNodeId) {
        for (ExecutionFragment fragment : fragments) {
            if (fragment.getScanNode(new PlanNodeId(scanNodeId)) != null) {
                return fragment;
            }
        }
        Preconditions.checkState(false, "can not found fragment");
        return null;
    }
}
