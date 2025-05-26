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
// limitations under the License

package com.starrocks.warehouse.cngroup;

/**
 * 1. CNResource is a compute node resource acquired from a warehouse or some other resources.
 * 2. It can be a warehouse or a cngroup of warehouse which is to represent the resource that can be used for compute.
 */
public interface CNResource {

    /**
     * Get the id of the CNResource
     * @return: the id of the CNResource
     */
    long getWarehouseId();

    /**
     * Get the id of the worker group that this CNResource belongs to.
     * @return: the id of the worker group
     */
    long getWorkerGroupId();
}
