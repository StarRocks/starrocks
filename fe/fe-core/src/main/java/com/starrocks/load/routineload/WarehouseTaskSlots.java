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

package com.starrocks.load.routineload;

// Routine load task slots of one warehouse: a task can only take a slot in its own warehouse, so
// used reaching total blocks that warehouse's jobs even when other warehouses have room.
public class WarehouseTaskSlots {
    public final int used;
    public final int total;

    public WarehouseTaskSlots(int used, int total) {
        this.used = used;
        this.total = total;
    }

    public boolean isFull() {
        return used >= total;
    }
}
