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

import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;

import java.util.List;
import java.util.Optional;

/**
 * {@code ComputeResourceProvider} is responsible to provide an available compute resource for a job scheduler
 * based on the current warehouse's load status or strategy.
 */
public interface ComputeResourceProvider {
    /**
     * Get a ComputeResource by warehouseId and workGroupId.
     * @param warehouseId the id of the warehouse
     * @param workGroupId the id of the worker group
     * @return a ComputeResource that can be used for compute
     */
    ComputeResource ofComputeResource(long warehouseId, long workGroupId);

    /**
     * Get all ComputeResources by warehouse.
     * @param warehouse the warehouse to get the ComputeResources from
     * @return a list of ComputeResources that can be used for compute
     */
    List<ComputeResource> getComputeResources(Warehouse warehouse);

    /**
     * NOTE: prefer to call this infrequently, as it can come to dominate the execution time of a query in the
     *  frontend if there are many calls per request (e.g. one per partition when there are many partitions).
     *
     * @param warehouse: the warehouse to get the worker group from
     * @param acquireContext: the context to acquire the worker group
     * @return: an available ComputeResource for the warehouse by the strategy, or Optional.empty() if no available worker group
     * @throws RuntimeException : if the warehouse is invalid or there is no available worker group
     */
    Optional<ComputeResource> acquireComputeResource(Warehouse warehouse, CRAcquireContext acquireContext);

    /**
     * Check the resource is available or not; this method will not throw exception.
     * @param computeResource: the ComputeResource to check
     * @return: true if the resource is available, false otherwise
     */
    boolean isResourceAvailable(ComputeResource computeResource);

    /**
     * Get all compute node ids in the ComputeResource without checking its alive status
     * @param computeResource: the ComputeResource to get the compute node ids from
     * @return: a list of compute node ids, empty if the ComputeResource is not available
     */
    List<Long> getAllComputeNodeIds(ComputeResource computeResource);

    /**
     * Get all alive compute nodes in the ComputeResource
     * @param computeResource: the ComputeResource to get the alive compute nodes from
     * @return: a list of alive compute nodes, empty if the ComputeResource is not available
     */
    List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource);
}
