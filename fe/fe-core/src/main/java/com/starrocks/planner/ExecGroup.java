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

import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.starrocks.thrift.TExecGroup;

import java.util.Set;

// represents a set of plan nodes
public class ExecGroup {
    private enum Type {
        NORMAL,
        COLOCATE
    }

    public ExecGroup(ExecGroupId groupId) {
        this.groupId = groupId;
    }

    public void setColocateGroup() {
        type = Type.COLOCATE;
    }

    public boolean isColocateExecGroup() {
        return type == Type.COLOCATE && !disableColocateGroup;
    }

    public boolean isDisableColocateGroup() {
        return this.disableColocateGroup;
    }

    public void add(PlanNode node) {
        nodeIds.add(node.getId().asInt());
    }

    public void add(PlanNode node, boolean disableColocateGroup) {
        add(node);
        if (!this.disableColocateGroup && disableColocateGroup) {
            disableColocateGroup(node);
        }
    }

    public void disableColocateGroup(PlanNode root) {
        if (isColocateExecGroup()) {
            clearRuntimeFilterExecGroupInfo(root);
        }
        this.disableColocateGroup = true;
    }

    private void clearRuntimeFilterExecGroupInfo(PlanNode root) {
        if (!nodeIds.contains(root.getId().asInt())) {
            return;
        }

        root.getProbeRuntimeFilters().forEach(RuntimeFilterDescription::clearExecGroupInfo);
        if (root instanceof RuntimeFilterBuildNode) {
            RuntimeFilterBuildNode rfBuildNode = (RuntimeFilterBuildNode) root;
            rfBuildNode.getBuildRuntimeFilters().forEach(RuntimeFilterDescription::clearExecGroupInfo);
        }

        root.getChildren().forEach(this::clearRuntimeFilterExecGroupInfo);
    }

    public void merge(ExecGroup other) {
        if (this != other) {
            this.nodeIds.addAll(other.nodeIds);
        }
    }

    public boolean contains(PlanNode node) {
        return nodeIds.contains(node.getId().asInt());
    }

    public boolean contains(int nodeId) {
        return nodeIds.contains(nodeId);
    }

    public ExecGroupId getGroupId() {
        return groupId;
    }

    @Override
    public String toString() {
        return "ExecGroup{" +
                "groupId=" + groupId +
                ", nodeIds=" + nodeIds +
                '}';
    }

    public TExecGroup toThrift() {
        TExecGroup tExecGroup = new TExecGroup();
        Preconditions.checkState(isColocateExecGroup());
        tExecGroup.setGroup_id(groupId.asInt());
        for (Integer nodeId : nodeIds) {
            tExecGroup.addToPlan_node_ids(nodeId);
        }
        return tExecGroup;
    }

    private final ExecGroupId groupId;
    private boolean disableColocateGroup = false;
    private final Set<Integer> nodeIds = Sets.newHashSet();
    private Type type = Type.NORMAL;
}
