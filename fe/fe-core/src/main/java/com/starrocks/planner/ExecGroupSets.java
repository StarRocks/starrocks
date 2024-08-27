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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.IdGenerator;

import java.util.List;

// A collection of ExecGroups.
public class ExecGroupSets {
    private final IdGenerator<ExecGroupId> execGroupIdIdGenerator = ExecGroupId.createGenerator();
    private final List<ExecGroup> execGroups = Lists.newArrayList();

    public List<ExecGroup> getExecGroups() {
        return execGroups;
    }

    public ExecGroup newExecGroup() {
        ExecGroup execGroup = new ExecGroup(execGroupIdIdGenerator.getNextId());
        execGroups.add(execGroup);
        return execGroup;
    }

    public void remove(ExecGroup execGroup) {
        execGroups.remove(execGroup);
    }

    public ExecGroup getExecGroup(int nodeId) {
        for (ExecGroup execGroup : execGroups) {
            if (execGroup.contains(nodeId)) {
                return execGroup;
            }
        }
        Preconditions.checkState(false, "not found exec group node: %d", nodeId);
        return null;
    }
}
