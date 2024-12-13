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

public class RuntimeFilterPushDownContext {
    private final RuntimeFilterDescription description;
    private final DescriptorTable descTbl;
    private final ExecGroupSets execGroups;

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

    public ExecGroup getExecGroup(int planNodeId) {
        return this.execGroups.getExecGroup(planNodeId);
    }
}
