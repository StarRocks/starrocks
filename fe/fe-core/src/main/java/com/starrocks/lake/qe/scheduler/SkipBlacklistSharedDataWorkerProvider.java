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

package com.starrocks.lake.qe.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * SkipBlacklistSharedDataWorkerProvider extends DefaultSharedDataWorkerProvider and skips backend blacklist verification.
 * This provider filters workers based only on availability (alive status), ignoring the blacklist.
 */
public class SkipBlacklistSharedDataWorkerProvider extends DefaultSharedDataWorkerProvider {
    private static final Logger LOG = LogManager.getLogger(SkipBlacklistSharedDataWorkerProvider.class);

    public static class Factory implements WorkerProvider.Factory {
        @Override
        public SkipBlacklistSharedDataWorkerProvider captureAvailableWorkers(
                SystemInfoService systemInfoService,
                boolean preferComputeNode,
                int numUsedComputeNodes,
                ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy,
                ComputeResource computeResource) {

            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            final ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
            final List<Long> computeNodeIds = warehouseManager.getAllComputeNodeIds(computeResource);
            computeNodeIds.forEach(nodeId -> builder.put(nodeId,
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId)));
            ImmutableMap<Long, ComputeNode> idToComputeNode = builder.build();
            if (LOG.isDebugEnabled()) {
                LOG.debug("SkipBlacklistSharedDataWorkerProvider - idToComputeNode: {}", idToComputeNode);
            }

            ImmutableMap<Long, ComputeNode> availableComputeNodes = filterAvailableWorkersSkipBlacklist(idToComputeNode);
            if (availableComputeNodes.isEmpty()) {
                Warehouse warehouse = warehouseManager.getWarehouse(computeResource.getWarehouseId());
                throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE, warehouse.getName());
            }

            return new SkipBlacklistSharedDataWorkerProvider(idToComputeNode, availableComputeNodes, computeResource);
        }

        private static ImmutableMap<Long, ComputeNode> filterAvailableWorkersSkipBlacklist(
                ImmutableMap<Long, ComputeNode> workers) {
            ImmutableMap.Builder<Long, ComputeNode> builder = new ImmutableMap.Builder<>();
            for (Map.Entry<Long, ComputeNode> entry : workers.entrySet()) {
                // Only check if worker is alive, skip blacklist verification
                if (entry.getValue().isAlive()) {
                    builder.put(entry);
                }
            }
            return builder.build();
        }
    }

    protected SkipBlacklistSharedDataWorkerProvider(ImmutableMap<Long, ComputeNode> id2ComputeNode,
                                             ImmutableMap<Long, ComputeNode> availableID2ComputeNode,
                                             ComputeResource computeResource) {
        super(id2ComputeNode, availableID2ComputeNode, computeResource);
    }

    /**
     * Override selectBackupWorker to skip blacklist verification.
     * This ensures consistent behavior with initial worker selection when skip_black_list is enabled.
     */
    @Override
    public long selectBackupWorker(long workerId) {
        if (availableID2ComputeNode.isEmpty() || !id2ComputeNode.containsKey(workerId)) {
            return -1;
        }
        if (allComputeNodeIds == null) {
            createAvailableIdList();
        }
        Preconditions.checkNotNull(allComputeNodeIds);
        Preconditions.checkState(allComputeNodeIds.contains(workerId));

        int startPos = allComputeNodeIds.indexOf(workerId);
        int attempts = allComputeNodeIds.size();
        while (attempts-- > 0) {
            startPos = (startPos + 1) % allComputeNodeIds.size();
            long buddyId = allComputeNodeIds.get(startPos);
            // Skip SimpleScheduler.isInBlocklist(buddyId) check - only verify buddyId != workerId and is in availableID2ComputeNode
            if (buddyId != workerId && availableID2ComputeNode.containsKey(buddyId)) {
                return buddyId;
            }
        }
        return -1;
    }
}
