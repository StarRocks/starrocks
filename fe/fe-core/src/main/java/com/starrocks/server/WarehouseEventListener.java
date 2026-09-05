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

package com.starrocks.server;

import com.starrocks.warehouse.Warehouse;

/**
 * WarehouseEventListener is an interface that defines methods to listen for events related to warehouses.
 */
public interface WarehouseEventListener {

    /**
     * This method is called when a warehouse is created.
     * @param wh the warehouse that is created
     */
    void onCreateWarehouse(Warehouse wh);

    /**
     * This method is called when a warehouse is created.
     * @param wh the warehouse that is created
     */
    void onDropWarehouse(Warehouse wh);

    /**
     * This method is called when a Compute Node Group is created in a warehouse.
     * @param wh the warehouse where the CN group is created
     * @param workerGroupId the worker group ID associated with the CN group that is being created
     */
    void onCreateCNGroup(Warehouse wh, long workerGroupId);

    /**
     * This method is called when a Compute Node Group is dropped from a warehouse.
     * @param wh the warehouse from which the CN group is dropped
     * @param workerGroupId the worker group ID associated with the CN group that is being dropped
     */
    void onDropCNGroup(Warehouse wh, long workerGroupId);
}