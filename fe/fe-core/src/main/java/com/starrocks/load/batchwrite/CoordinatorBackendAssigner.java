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

package com.starrocks.load.batchwrite;

import com.starrocks.system.ComputeNode;

import java.util.List;
import java.util.Optional;

/**
 * Interface for assigning coordinator backends to loads.
 */
public interface CoordinatorBackendAssigner {

    /**
     * Starts the backend assigner.
     */
    void start();

    /**
     * Registers a batch write with the specified parameters.
     *
     * @param id The ID of the batch write operation.
     * @param warehouseId The id of the warehouse.
     * @param tableId The identifier of the table.
     * @param expectParallel The expected parallelism.
     */
    void registerBatchWrite(long id, long warehouseId, TableId tableId, int expectParallel);

    /**
     * Unregisters a batch write.
     *
     * @param id The ID of the batch write to unregister.
     */
    void unregisterBatchWrite(long id);

    /**
     * Retrieves the list of compute nodes assigned to the batch write.
     *
     * @param id The ID of the batch write.
     * @return A list of compute nodes assigned to the batch write.
     */
    Optional<List<ComputeNode>> getBackends(long id);
}
