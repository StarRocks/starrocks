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

package com.starrocks.lake.compaction;

import com.starrocks.warehouse.cngroup.ComputeResource;

public class CompactionWarehouseInfo {
    public CompactionWarehouseInfo(String warehouseName, ComputeResource computeResource, int taskLimit,
            int taskRunning) {
        this.warehouseName = warehouseName;
        this.computeResource = computeResource;
        this.taskLimit = taskLimit;
        this.taskRunning = taskRunning;
        this.limitReached = false;
    }
    public final String warehouseName;
    public final ComputeResource computeResource;
    public final int taskLimit;
    public int taskRunning;
    public boolean limitReached;
}
