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

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;

/**
 * {@code CRAcquireContext} is the context for acquiring ComputeResource from a warehouse.
 */
public class CRAcquireContext {
    // The id of the warehouse which must be specified.
    private final long warehouseId;

    // The strategy to get compute resource from the warehouse.
    private final CRAcquireStrategy strategy;

    // The previous CNResource which is used to get the compute resource from the warehouse.
    private final ComputeResource prevComputeResource;

    public CRAcquireContext(long warehouseId, CRAcquireStrategy strategy, ComputeResource prevComputeResource) {
        this.warehouseId = warehouseId;
        this.strategy = strategy;
        this.prevComputeResource = prevComputeResource;
    }

    public CRAcquireContext(long warehouseId, ComputeResource prevComputeResource) {
        this.warehouseId = warehouseId;
        this.strategy = CRAcquireStrategy.fromString(GlobalVariable.getCngroupScheduleMode());
        this.prevComputeResource = prevComputeResource;
    }

    public static CRAcquireContext of(long warehouseId, CRAcquireStrategy cnGroupStrategy, ComputeResource prevComputeResource) {
        return new CRAcquireContext(warehouseId, cnGroupStrategy, prevComputeResource);
    }

    public static CRAcquireContext of(long warehouseId, ComputeResource prevComputeResource) {
        return new CRAcquireContext(warehouseId, prevComputeResource);
    }

    public static CRAcquireContext of(long warehouseId, CRAcquireStrategy cnGroupStrategy) {
        return new CRAcquireContext(warehouseId, cnGroupStrategy, null);
    }

    public static CRAcquireContext of(long warehouseId) {
        return new CRAcquireContext(warehouseId, null);
    }

    public static CRAcquireContext of(String warehouseName) {
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        final Warehouse warehouse = warehouseManager.getWarehouse(warehouseName);
        if (warehouse == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("name: %s", warehouseName));
        }
        return new CRAcquireContext(warehouse.getId(), CRAcquireStrategy.STANDARD, null);
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public CRAcquireStrategy getStrategy() {
        return strategy;
    }

    public ComputeResource getPrevComputeResource() {
        return prevComputeResource;
    }

    @Override
    public String toString() {
        return "{" +
                "warehouseId=" + warehouseId +
                ", strategy=" + strategy +
                ", prevComputeResource=" + prevComputeResource +
                '}';
    }
}
