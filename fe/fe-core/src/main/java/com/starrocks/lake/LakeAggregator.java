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

package com.starrocks.lake;

import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;


public class LakeAggregator {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    public LakeAggregator() {}

    // When file bundling is enabled, the aggregator is responsible for writing the bundled
    // tablet metadata / combined txn log on behalf of the whole batch. On the BE side the
    // file path is derived from "the first tablet id" of the batch, and that path lookup
    // eventually consults the staros worker cache (g_worker->get_shard_info). If the chosen
    // aggregator does not own ANY tablet in the batch, the worker cache will miss and the
    // BE has to issue an RPC to fetch the shard info, amplifying RPC pressure.
    //
    // Prefer picking an aggregator that already owns at least one tablet of the batch so
    // that the first tablet id can be resolved locally without an extra RPC. Fall back to
    // the original "random alive node" / "seq-chosen node" strategy only when no candidate
    // is available or alive.
    public static ComputeNode chooseAggregatorNode(ComputeResource computeResource,
                                                   Collection<ComputeNode> candidateNodes) {
        try {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            List<ComputeNode> aliveNodes = warehouseManager.getAliveComputeNodes(computeResource);
            if (aliveNodes != null && !aliveNodes.isEmpty()
                    && candidateNodes != null && !candidateNodes.isEmpty()) {
                Set<Long> aliveIds = aliveNodes.stream().map(ComputeNode::getId).collect(Collectors.toSet());
                List<ComputeNode> preferred = candidateNodes.stream()
                        .filter(n -> n != null && aliveIds.contains(n.getId()))
                        .distinct()
                        .collect(Collectors.toList());
                if (!preferred.isEmpty()) {
                    return preferred.get(ThreadLocalRandom.current().nextInt(preferred.size()));
                }
                LOG.debug("no candidate node is alive in warehouse {}, fall back to random alive node",
                        computeResource);
            }
            if (aliveNodes != null && !aliveNodes.isEmpty()) {
                return aliveNodes.get(ThreadLocalRandom.current().nextInt(aliveNodes.size()));
            }
            Long nodeId = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                            .getNodeSelector().seqChooseBackendOrComputeId();
            return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .getBackendOrComputeNode(nodeId);
        } catch (StarRocksException e) {
            return null;
        }
    }
}