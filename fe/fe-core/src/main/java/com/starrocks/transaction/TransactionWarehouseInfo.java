// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.transaction;

import com.google.gson.annotations.SerializedName;
import com.starrocks.epack.warehouse.cngroup.CNGroupResource;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.cngroup.ComputeResource;

public class TransactionWarehouseInfo {
    @SerializedName(value = "wid")
    private long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
    @SerializedName(value = "cgd")
    private long cnGroupId = StarOSAgent.DEFAULT_WORKER_GROUP_ID;

    public TransactionWarehouseInfo() {
        warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        cnGroupId = StarOSAgent.DEFAULT_WORKER_GROUP_ID;
    }

    public void setInfo(ComputeResource computeResource) {
        warehouseId = computeResource.getWarehouseId();
        cnGroupId = computeResource.getWorkerGroupId();
    }

    public ComputeResource getComputeResource() {
        return CNGroupResource.of(warehouseId, cnGroupId);
    }

    public long getWarehouseId() {
        return warehouseId;
    }
}
