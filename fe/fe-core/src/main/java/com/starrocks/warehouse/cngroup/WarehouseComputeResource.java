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

import com.google.gson.annotations.SerializedName;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@code WarehouseComputeResource} represents a compute node resource associated with a specific warehouse.
 */
public final class WarehouseComputeResource implements ComputeResource {
    private static final Logger LOG = LogManager.getLogger(WarehouseComputeResource.class);
    // The warehouseId is used to identify the warehouse.
    @SerializedName("warehouseId")
    private final long warehouseId;

    public static final ComputeResource DEFAULT = new WarehouseComputeResource(WarehouseManager.DEFAULT_WAREHOUSE_ID);

    public WarehouseComputeResource(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    public static WarehouseComputeResource of(long warehouseId) {
        return new WarehouseComputeResource(warehouseId);
    }

    @Override
    public long getWarehouseId() {
        return warehouseId;
    }

    @Override
    public long getWorkerGroupId() {
        if (!RunMode.isSharedDataMode()) {
            return StarOSAgent.DEFAULT_WORKER_GROUP_ID;
        }
        return selectWorkerGroupInternal(warehouseId)
                .orElse(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
    }

    private Optional<Long> selectWorkerGroupInternal(long warehouseId) {
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = warehouseManager.getWarehouse(warehouseId);
        List<Long> ids = warehouse.getWorkerGroupIds();
        if (CollectionUtils.isEmpty(ids)) {
            LOG.warn("failed to get worker group id from warehouse {}", warehouse);
            return Optional.empty();
        }
        return Optional.of(ids.get(0));
    }

    @Override
    public String toString() {
        return "{warehouseId=" + warehouseId + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(warehouseId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof WarehouseComputeResource)) {
            return false;
        }
        WarehouseComputeResource other = (WarehouseComputeResource) obj;
        return warehouseId == other.warehouseId;
    }
}
