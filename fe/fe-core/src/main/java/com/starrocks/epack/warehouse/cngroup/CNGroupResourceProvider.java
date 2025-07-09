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

package com.starrocks.epack.warehouse.cngroup;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.CRAcquireStrategy;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.ComputeResourceProvider;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * CNGroupResourceProvider is responsible for providing compute node resources{@code CNGroupResource} and
 * associated operations.
 */
public class CNGroupResourceProvider implements ComputeResourceProvider  {
    private static final Logger LOG = LogManager.getLogger(CNGroupResourceProvider.class);

    public static CNGroupResourceProvider INSTANCE = new CNGroupResourceProvider();

    private static final AtomicLong NEXT_GENERATOR_ID = new AtomicLong(0);

    public CNGroupResourceProvider() {
        // No-op
    }

    @Override
    public ComputeResource ofComputeResource(long warehouseId, long workGroupId) {
        return CNGroupResource.of(warehouseId, workGroupId);
    }

    @Override
    public Optional<ComputeResource> acquireComputeResource(Warehouse warehouse, CRAcquireContext acquireContext) {
        final long warehouseId = acquireContext.getWarehouseId();
        if (warehouse == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id : %d", warehouseId));
        }
        final List<Long> ids = warehouse.getWorkerGroupIds();
        if (CollectionUtils.isEmpty(ids)) {
            LOG.warn("failed to get worker group id from warehouse {}", warehouse.getName());
            return Optional.empty();
        } else if (ids.size() == 1) {
            final long workerGroupId = ids.get(0);
            final CNGroupResource cnGroupResource = CNGroupResource.of(warehouseId, workerGroupId);
            if (!isResourceAvailable(cnGroupResource)) {
                LOG.warn("failed to get alive compute nodes from warehouse {}", warehouse.getName());
                return Optional.empty();
            }
            return Optional.of(cnGroupResource);
        } else {
            final CRAcquireStrategy strategy = acquireContext.getStrategy();
            // TODO: put it into a cache later
            final Set<Long> blackWorkerGroupIds = Sets.newHashSet();
            switch (strategy) {
                case LOCAL_FIRST: {
                    Optional<ComputeResource> result = acquireByLocalFirst(acquireContext, ids, blackWorkerGroupIds);
                    if (result.isPresent()) {
                        return result;
                    }
                    return acquireByRandom(acquireContext, ids, blackWorkerGroupIds);
                }
                case RANDOM:
                    return acquireByRandom(acquireContext, ids, blackWorkerGroupIds);
                default: {
                    // if the previous resource is not null, we will try to acquire the same group first
                    Optional<ComputeResource> result = acquireByLocalFirst(acquireContext, ids, blackWorkerGroupIds);
                    if (result.isPresent()) {
                        return result;
                    }
                    // only acquire by CNGroupUsage if the current node is leader
                    if (GlobalStateMgr.getCurrentState().isLeader()) {
                        result = acquireByCNGroupUsage(acquireContext, ids, blackWorkerGroupIds);
                        if (result.isPresent()) {
                            return result;
                        }
                    }
                    return acquireByRandom(acquireContext, ids, blackWorkerGroupIds);
                }
            }
        }
    }

    private Optional<ComputeResource> acquireByCNGroupUsage(CRAcquireContext acquireContext,
                                                            List<Long> ids,
                                                            Set<Long> blackWorkerGroupIds) {
        final int cnGroupSize = ids.size();
        final List<CNGroupResourceUsage> cnGroupResourceUsages = Lists.newArrayList();
        // To avoid the same worker group being selected multiple times in a row, initialize the index by next_generator_id.
        int start = Math.floorMod(NEXT_GENERATOR_ID.getAndIncrement(), cnGroupSize);
        int idx = 0;
        for (int i = 0; i < cnGroupSize; i++) {
            idx = Math.floorMod(start + i, cnGroupSize);
            final long workerGroupId = ids.get(idx);
            if (blackWorkerGroupIds.contains(workerGroupId)) {
                continue;
            }
            final CNGroupResource cnGroupResource = CNGroupResource.of(acquireContext.getWarehouseId(), workerGroupId);
            final List<ComputeNode> computeNodes = getAliveComputeNodes(cnGroupResource);
            if (computeNodes.isEmpty()) {
                blackWorkerGroupIds.add(workerGroupId);
                continue;
            }
            final CNGroupResourceUsage cnGroupResourceUsage =
                    CNGroupResourceUsage.of(cnGroupResource, computeNodes);
            // if the resource usage is not fresh, we skip it
            if (!cnGroupResourceUsage.isResourceUsageFresh()) {
                continue;
            }
            // if the resource group is under low watermark, we can return it directly
            if (cnGroupResourceUsage.isUnderLowWatermark()) {
                return Optional.of(cnGroupResource);
            }
            cnGroupResourceUsages.add(cnGroupResourceUsage);
        }
        if (cnGroupResourceUsages.isEmpty()) {
            return Optional.empty();
        }
        // sort by aliveComputeNodeCount, maxRunningQueries, avgCpuUsedPermille
        return CNGroupResourceUsage.findBestByUsage(cnGroupResourceUsages);
    }

    private Optional<ComputeResource> acquireByRandom(CRAcquireContext acquireContext,
                                                      List<Long> ids,
                                                      Set<Long> blackWorkerGroupIds) {
        final long warehouseId = acquireContext.getWarehouseId();
        // check whether the worker group contains alive compute nodes
        // select by random robin or some better strategy.
        final int cnGroupSize = ids.size();
        if (cnGroupSize == 0) {
            return Optional.empty();
        }
        int start = Math.floorMod(NEXT_GENERATOR_ID.getAndIncrement(), cnGroupSize);
        int idx = 0;
        for (int i = 0; i < cnGroupSize; i++) {
            idx = Math.floorMod(start + i, cnGroupSize);
            final long workerGroupId = ids.get(idx);
            if (blackWorkerGroupIds.contains(workerGroupId)) {
                continue;
            }
            CNGroupResource cnGroupResource = CNGroupResource.of(warehouseId, workerGroupId);
            if (isResourceAvailable(cnGroupResource)) {
                return Optional.of(cnGroupResource);
            }
        }
        return Optional.empty();
    }

    private Optional<ComputeResource> acquireByLocalFirst(CRAcquireContext acquireContext,
                                                          List<Long> ids,
                                                          Set<Long> blackWorkerGroupIds) {
        final ComputeResource prev = acquireContext.getPrevComputeResource();
        if (prev != null && prev instanceof CNGroupResource) {
            CNGroupResource prevGroup = (CNGroupResource) prev;
            final Set<Long> currentWorkerGroupIds = ids.stream().collect(Collectors.toUnmodifiableSet());
            final Optional<ComputeResource> result = tryAcquireByPrev(prevGroup, currentWorkerGroupIds);
            if (result.isPresent()) {
                return result;
            }
            blackWorkerGroupIds.add(prevGroup.getWorkerGroupId());
        }
        return Optional.empty();
    }

    private Optional<ComputeResource> tryAcquireByPrev(CNGroupResource prevGroup,
                                                       Set<Long> currentWorkerGroupIds) {
        if (prevGroup.getCreateTimeMs() <= 0) {
            return Optional.empty();
        }
        final long prevWorkerGroupId = prevGroup.getWorkerGroupId();
        // check whether the worker group contains living compute nodes
        if (!currentWorkerGroupIds.contains(prevWorkerGroupId)) {
            return Optional.empty();
        }
        if (isResourceAvailable(prevGroup)) {
            return Optional.of(CNGroupResource.of(prevGroup.getWarehouseId(), prevWorkerGroupId));
        }
        return Optional.empty();
    }

    @Override
    public boolean isResourceAvailable(ComputeResource cnResource) {
        try {
            final List<ComputeNode> computeNodes = getAliveComputeNodes(cnResource);
            final long availableWorkerGroupIdSize =
                    Optional.ofNullable(computeNodes).map(List::size).orElse(0);
            return availableWorkerGroupIdSize > 0;
        } catch (Exception e) {
            LOG.warn("Failed to get alive compute nodes from starMgr : {}", e.getMessage());
            return false;
        }
    }

    @Override
    public List<Long> getAllComputeNodeIds(ComputeResource cnResource) {
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        if (!warehouseManager.warehouseExists(cnResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", cnResource.getWarehouseId()));
        }
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getWorkersByWorkerGroup(cnResource.getWorkerGroupId());
        } catch (StarRocksException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    @Override
    public List<ComputeNode> getAliveComputeNodes(ComputeResource cnResource) {
        final List<Long> computeNodeIds = getAllComputeNodeIds(cnResource);
        if (CollectionUtils.isEmpty(computeNodeIds)) {
            return Lists.newArrayList();
        }
        final SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        return computeNodeIds.stream()
                .map(id -> systemInfoService.getBackendOrComputeNode(id))
                .filter(ComputeNode::isAlive).collect(Collectors.toList());
    }
}
