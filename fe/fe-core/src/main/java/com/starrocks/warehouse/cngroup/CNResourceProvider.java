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
 * WorkerGroupProvider provides an available cngroup to a job scheduler based on the current warehouse's load status or strategy.
 */
public interface CNResourceProvider {
    /**
     * NOTE: prefer to call this infrequently, as it can come to dominate the execution time of a query in the
     *  frontend if there are many calls per request (e.g. one per partition when there are many partitions).
     * @param warehouse: the warehouse to get the worker group from
     * @param acquireContext: the context to acquire the worker group
     * @return: an available CNResource for the warehouse by the strategy
     * @throws RuntimeException : if the warehouse is invalid or there is no available worker group
     */
    Optional<CNResource> acquireCNResource(Warehouse warehouse, CNAcquireContext acquireContext);

    /**
     * Check Whether the resource is available, this method will not throw exception
     * @param cnResource: the CNResource to check
     * @return: true if the resource is available, false otherwise
     */
    boolean isResourceAvailable(CNResource cnResource);

    /**
     * Get all compute node ids in the CNResource without checking the alive status
     * @param cnResource: the CNResource to get the compute node ids from
     * @return: a list of compute node ids, empty if the CNResource is not available
     */
    List<Long> getAllComputeNodeIds(CNResource cnResource);

    /**
     * Get all alive compute nodes in the CNResource
     * @param cnResource: the CNResource to get the alive compute nodes from
     * @return: a list of alive compute nodes, empty if the CNResource is not available
     */
    List<ComputeNode> getAliveComputeNodes(CNResource cnResource);
}
