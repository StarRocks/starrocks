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

package com.starrocks.warehouse;

import com.google.gson.annotations.SerializedName;
import com.staros.client.StarClientException;
import com.staros.proto.ShardInfo;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;

import java.util.Set;

public class DefaultWarehouse extends Warehouse {
    @SerializedName(value = "cluster")
    Cluster cluster;

    public DefaultWarehouse(long id, String name, long clusterId) {
        super(id, name, "An internal warehouse init after FE is ready");
        cluster = new Cluster(clusterId);
    }

    @Override
    public Cluster getAnyAvailableCluster() {
        return cluster;
    }

    public Long getComputeNodeId(LakeTablet tablet) {
        try {
            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getShardInfo(tablet.getShardId(), cluster.getWorkerGroupId());

            Long nodeId;
            Set<Long> ids = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllBackendIdsByShard(shardInfo, true);
            if (!ids.isEmpty()) {
                nodeId = ids.iterator().next();
                return nodeId;
            } else {
                return null;
            }
        } catch (StarClientException e) {
            return null;
        }
    }

    public ComputeNode getComputeNode(LakeTablet tablet) {
        Long computeNodeId = getComputeNodeId(tablet);
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(computeNodeId);
    }
}
