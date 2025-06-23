// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.transaction;

import com.google.gson.annotations.SerializedName;
import com.starrocks.server.WarehouseManager;

public class TransactionWarehouseInfo {
    @SerializedName(value = "wid")
    private long warehouseId = WarehouseManager.INVALID_WAREHOUSE_ID;

    public TransactionWarehouseInfo() {
        warehouseId = WarehouseManager.INVALID_WAREHOUSE_ID;
    }

    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    public long getWarehouseId() {
        return warehouseId;
    }
}
