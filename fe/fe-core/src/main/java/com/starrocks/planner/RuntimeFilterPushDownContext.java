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

public class RuntimeFilterPushDownContext {
    private final RuntimeFilterDescription description;
    private final DescriptorTable descTbl;
    private final ExecGroupSets execGroups;

    // Depth of non-aggregation, deterministic pipeline-breaking nodes (blocking sort, analytic/window)
    // crossed on the current path from the runtime-filter builder down to the node being visited.
    // Aggregation is intentionally NOT counted: a near-distinct GROUP BY degrades to a streaming
    // pre-aggregation at runtime, which is exactly the case TopN-filter back-pressure is meant to help.
    // When this is > 0 at a scan, the TopN runtime filter cannot arrive while the scan is still
    // reading, so back-pressure would only stall the scan and is skipped. Mutated via enter/exit around
    // each breaker's child recursion (balanced with try/finally), mirroring the exchange-node counter.
    private int nonAggPipelineBreakerDepth = 0;

    RuntimeFilterPushDownContext(
            RuntimeFilterDescription description,
            DescriptorTable descTbl,
            ExecGroupSets execGroupSets) {
        this.description = description;
        this.descTbl = descTbl;
        this.execGroups = execGroupSets;
        // set description
        ExecGroup execGroup = this.execGroups.getExecGroup(description.getBuildPlanNodeId());
        this.description.setExecGroupInfo(execGroup.isColocateExecGroup(), execGroup.getGroupId().asInt());
    }

    public DescriptorTable getDescTbl() {
        return descTbl;
    }

    public RuntimeFilterDescription getDescription() {
        return description;
    }

    public ExecGroupId getExecGroupId(int planNodeId) {
        return this.execGroups.getExecGroupId(planNodeId);
    }

    public void enterNonAggPipelineBreaker() {
        this.nonAggPipelineBreakerDepth++;
    }

    public void exitNonAggPipelineBreaker() {
        this.nonAggPipelineBreakerDepth--;
    }

    // True when the current path from the RF builder has crossed at least one non-aggregation
    // deterministic pipeline breaker (so a TopN RF cannot reach a scan below in time).
    public boolean crossedNonAggPipelineBreaker() {
        return this.nonAggPipelineBreakerDepth > 0;
    }
}
