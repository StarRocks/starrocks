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

import com.google.common.collect.ImmutableMap;
import com.staros.util.LockCloseable;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WarehouseManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    public static final String DEFAULT_WAREHOUSE_NAME = "default_warehouse";
    public static final long DEFAULT_WAREHOUSE_ID = 0L;
    public static final long DEFAULT_CLUSTER_ID = 0L;

    private final Map<Long, Warehouse> idToWh = new HashMap<>();
    private final Map<String, Warehouse> nameToWh = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public WarehouseManager() {
    }

    public void initDefaultWarehouse() {
        // gen a default warehouse
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Warehouse wh = new DefaultWarehouse(DEFAULT_WAREHOUSE_ID,
                    DEFAULT_WAREHOUSE_NAME, DEFAULT_CLUSTER_ID);
            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);
        }
    }

    public List<Warehouse> getAllWarehouses() {
        return new ArrayList<>(nameToWh.values());
    }

    public Warehouse getWarehouse(String warehouseName) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse warehouse = nameToWh.get(warehouseName);
            if (warehouse == null) {
                ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
            }
            return warehouse;
        }
    }

    public Warehouse getWarehouse(long warehouseId) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse warehouse = idToWh.get(warehouseId);
            if (warehouse == null) {
                ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseId);
            }
            return warehouse;
        }
    }

    public List<Long> getWarehouseIds() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return new ArrayList<>(idToWh.keySet());
        }
    }

    public boolean warehouseExists(String warehouseName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.containsKey(warehouseName);
        }
    }

    public ImmutableMap<Long, ComputeNode> getComputeNodesFromWarehouse(long warehouseId) {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        Warehouse warehouse = getWarehouse(warehouseId);
        warehouse.getAnyAvailableCluster().getComputeNodeIds().forEach(
                nodeId -> builder.put(nodeId,
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId)));
        return builder.build();
    }

    public ComputeNode getComputeNode(LakeTablet tablet) {
        DefaultWarehouse warehouse = (DefaultWarehouse) nameToWh.get(DEFAULT_WAREHOUSE_NAME);
        return warehouse.getComputeNode(tablet);
    }

    public ComputeNode getComputeNode(String warehouseName, LakeTablet tablet) {
        DefaultWarehouse warehouse = (DefaultWarehouse) nameToWh.get(warehouseName);
        return warehouse.getComputeNode(tablet);
    }

    public ComputeNode getComputeNode(Long warehouseId, LakeTablet tablet) {
        DefaultWarehouse warehouse = (DefaultWarehouse) idToWh.get(warehouseId);
        return warehouse.getComputeNode(tablet);
    }

    public AtomicInteger getNextComputeNodeIndexFromWarehouse(long warehouseId) {
        return getWarehouse(warehouseId).getAnyAvailableCluster().getNextComputeNodeHostId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
