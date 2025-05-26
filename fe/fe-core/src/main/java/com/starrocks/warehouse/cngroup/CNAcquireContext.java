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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;

/**
 * CNAcquireContext is the context for acquiring CNResource from a warehouse.
 */
public class CNAcquireContext {
    // The id of the warehouse which is must be specified.
    private final long warehouseId;

    // The strategy to get the CNGroup from the warehouse.
    private final CNAcquireStrategy cnAcquireStrategy;

    // The previous CNResource which is used to get the CNGroup from the warehouse.
    private final CNResource prevCNResource;

    public CNAcquireContext(long warehouseId, CNAcquireStrategy cnAcquireStrategy, CNResource prevCNResource) {
        this.warehouseId = warehouseId;
        this.cnAcquireStrategy = cnAcquireStrategy;
        this.prevCNResource = prevCNResource;
    }

    public static CNAcquireContext of(long warehouseId, CNAcquireStrategy cnGroupStrategy, CNResource prevCNResource) {
        return new CNAcquireContext(warehouseId, cnGroupStrategy, prevCNResource);
    }

    public static CNAcquireContext of(long warehouseId, CNResource prevCNResource) {
        return new CNAcquireContext(warehouseId, CNAcquireStrategy.STANDARD, prevCNResource);
    }

    public static CNAcquireContext of(long warehouseId, CNAcquireStrategy cnGroupStrategy) {
        return new CNAcquireContext(warehouseId, cnGroupStrategy, null);
    }

    public static CNAcquireContext of(long warehouseId) {
        return new CNAcquireContext(warehouseId, CNAcquireStrategy.STANDARD, null);
    }

    public static CNAcquireContext of(String warehouseName) {
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        final Warehouse warehouse = warehouseManager.getWarehouse(warehouseName);
        if (warehouse == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("name: %d", warehouseName));
        }
        return new CNAcquireContext(warehouse.getId(), CNAcquireStrategy.STANDARD, null);
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public CNAcquireStrategy getCnAcquireStrategy() {
        return cnAcquireStrategy;
    }

    public CNResource getPrevCNResource() {
        return prevCNResource;
    }
}
