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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.epack.warehouse.WarehouseUnavailableException;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WarehouseManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    public static final String DEFAULT_WAREHOUSE_NAME = "default_warehouse";

    public static final long DEFAULT_WAREHOUSE_ID = 0L;
    public static final long DEFAULT_CLUSTER_ID = 0L;

    protected Map<Long, Warehouse> idToWh = new HashMap<>();
    protected Map<String, Warehouse> nameToWh = new HashMap<>();

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public WarehouseManager() {
    }

    public static boolean isDefaultWarehouse(String name) {
        return name.equalsIgnoreCase(DEFAULT_WAREHOUSE_NAME);
    }

    public void initDefaultWarehouse() {
        // gen a default warehouse
        // NOTE: default warehouse use DEFAULT_WORKER_GROUP_ID, which is 0,
        // so it is unnecessary to create a worker group for it.
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Warehouse wh = new DefaultWarehouse(DEFAULT_WAREHOUSE_ID,
                    DEFAULT_WAREHOUSE_NAME, DEFAULT_CLUSTER_ID);
            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);
        }
    }

    // only for test, do not use it in your codes!!!
    public static Warehouse defaultWarehouse() {
        return new LocalWarehouse(DEFAULT_WAREHOUSE_ID,
                DEFAULT_WAREHOUSE_NAME, DEFAULT_CLUSTER_ID,
                "An internal warehouse init after FE is ready");
    }

    public Warehouse getAvailbleWarehouse(long warehouseId) throws WarehouseUnavailableException {
        return getWarehouse(DEFAULT_WAREHOUSE_NAME);
    }

    public Warehouse getAvailbleWarehouse(String warehouseName) throws WarehouseUnavailableException {
        return getWarehouse(DEFAULT_WAREHOUSE_NAME);
    }

    public AtomicInteger getNextComputeNodeIndexFromWarehouse(long warehouseId) {
        return getWarehouse(warehouseId).getAnyAvailableCluster().getNextComputeNodeHostId();
    }

    public Warehouse getDefaultWarehouse() {
        return getWarehouse(DEFAULT_WAREHOUSE_NAME);
    }

    public Warehouse getWarehouse(String warehouseName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.get(warehouseName);
        }
    }

    public Warehouse getWarehouse(long warehouseId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return idToWh.get(warehouseId);
        }
    }

    public List<Warehouse> getAllWarehouses() {
        return new ArrayList<>(nameToWh.values());
    }

    public List<Long> getWarehouseIds() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return new ArrayList<>(idToWh.keySet());
        }
    }

    public Set<String> getAllWarehouseNames() {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.keySet();
        }
    }

    public boolean warehouseExists(String warehouseName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.containsKey(warehouseName);
        }
    }

    // will check whether warehouse is available first
    public ImmutableMap<Long, ComputeNode> getComputeNodesFromAvailableWarehouse(long warehouseId)
            throws WarehouseUnavailableException {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        Warehouse warehouse = getAvailbleWarehouse(warehouseId);
        // check if warehouse available
        warehouse.getAnyAvailableCluster().getComputeNodeIds().forEach(
                nodeId -> builder.put(nodeId,
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId)));
        return builder.build();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
