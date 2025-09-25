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

package com.starrocks.warehouse.cngroup;

import com.google.common.collect.Lists;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@code WarehouseComputeResourceProvider} is responsible for providing warehouse compute node resources and
 * associated operations.
 */
public final class WarehouseComputeResourceProvider implements ComputeResourceProvider {
    private static final Logger LOG = LogManager.getLogger(WarehouseComputeResourceProvider.class);

    public WarehouseComputeResourceProvider() {
        // No-op
    }

    @Override
    public ComputeResource ofComputeResource(long warehouseId, long workerGroupId) {
        return WarehouseComputeResource.of(warehouseId);
    }

    @Override
    public List<ComputeResource> getComputeResources(Warehouse warehouse) {
        if (warehouse == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, "warehouse is null");
        }
        return Lists.newArrayList(WarehouseComputeResource.of(warehouse.getId()));
    }

    @Override
    public Optional<ComputeResource> acquireComputeResource(Warehouse warehouse, CRAcquireContext acquireContext) {
        final long warehouseId = acquireContext.getWarehouseId();
        if (warehouse == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", warehouseId));
        }
        WarehouseComputeResource computeResource = WarehouseComputeResource.of(warehouseId);
        if (!isResourceAvailable(computeResource)) {
            LOG.warn("failed to get alive compute nodes from warehouse {}", warehouse.getName());
            return Optional.empty();
        }
        return Optional.of(computeResource);
    }

    /**
     * TODO: Add a blacklist cache to avoid time-consuming alive check
     */
    @Override
    public boolean isResourceAvailable(ComputeResource computeResource) {
        try {
            final long availableWorkerGroupIdSize =
                    Optional.ofNullable(getAliveComputeNodes(computeResource)).map(List::size).orElse(0);
            return availableWorkerGroupIdSize > 0;
        } catch (Exception e) {
            LOG.warn("Failed to get alive compute nodes from starMgr : {}", e.getMessage());
            return false;
        }
    }

    @Override
    public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkersByWorkerGroup(computeResource.getWorkerGroupId());
        } catch (StarRocksException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    @Override
    public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
        List<Long> computeNodeIds = getAllComputeNodeIds(computeResource);
        if (CollectionUtils.isEmpty(computeNodeIds)) {
            return Lists.newArrayList();
        }
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<ComputeNode> nodes = computeNodeIds.stream()
                .map(id -> systemInfoService.getBackendOrComputeNode(id))
                .filter(ComputeNode::isAlive).collect(Collectors.toList());
        return nodes;
    }
}
