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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Random;


public class LakeAggregator {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    public LakeAggregator() {}

    // TODO(zhangqiang)
    // Optimize the aggregator selection strategy
    public ComputeNode chooseAggregatorNode(long warehouseId) {
        try {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            List<ComputeNode> candidateNodes = warehouseManager.getAliveComputeNodes(warehouseId);
            if (candidateNodes != null && !candidateNodes.isEmpty()) {
                Random random = new Random();
                return candidateNodes.get(random.nextInt(candidateNodes.size()));
            } else {
                Long nodeId = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                                .getNodeSelector().seqChooseBackendOrComputeId();
                return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                        .getBackendOrComputeNode(nodeId); 
            }
        } catch (StarRocksException e) {
            return null;
        }
    }
}