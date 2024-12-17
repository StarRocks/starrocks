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
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.LocalWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseProcDir;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WarehouseManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    public static final String DEFAULT_WAREHOUSE_NAME = "default_warehouse";

    public static final long DEFAULT_WAREHOUSE_ID = 0L;

    private Map<Long, Warehouse> idToWh = new HashMap<>();
    private Map<String, Warehouse> nameToWh = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public WarehouseManager() {
    }

    public void initDefaultWarehouse() {
        // gen a default warehouse
<<<<<<< HEAD
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Warehouse wh = new LocalWarehouse(DEFAULT_WAREHOUSE_ID,
                    DEFAULT_WAREHOUSE_NAME);
=======
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Warehouse wh = new DefaultWarehouse(DEFAULT_WAREHOUSE_ID, DEFAULT_WAREHOUSE_NAME);
>>>>>>> 6cd9fbc95f ([Enhancement] Add cluster idle HTTP api (#53850))
            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);
            wh.setExist(true);
        }
    }

<<<<<<< HEAD
    public Warehouse getDefaultWarehouse() {
        return getWarehouse(DEFAULT_WAREHOUSE_NAME);
=======
    public List<Warehouse> getAllWarehouses() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return new ArrayList<>(nameToWh.values());
        }
    }

    public List<Long> getAllWarehouseIds() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return new ArrayList<>(idToWh.keySet());
        }
>>>>>>> 6cd9fbc95f ([Enhancement] Add cluster idle HTTP api (#53850))
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

    public ImmutableMap<Long, ComputeNode> getComputeNodesFromWarehouse() {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        Warehouse warehouse = getDefaultWarehouse();
        warehouse.getAnyAvailableCluster().getComputeNodeIds().forEach(
                nodeId -> builder.put(nodeId, GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeId)));
        return builder.build();
    }

<<<<<<< HEAD
    // not persist anything thereafter, so checksum ^= 0
    public long saveWarehouses(DataOutputStream out, long checksum) throws IOException {
        checksum ^= 0;
        write(out);
        return checksum;
    }

    public long loadWarehouses(DataInputStream dis, long checksum) throws IOException, DdlException {
        int warehouseCount = 0;
        try {
            String s = Text.readString(dis);
            WarehouseManager data = GsonUtils.GSON.fromJson(s, WarehouseManager.class);
            if (data != null && data.nameToWh != null) {
                warehouseCount = data.nameToWh.size();
=======
    public List<Long> getAllComputeNodeIds(String warehouseName) {
        Warehouse warehouse = getWarehouse(warehouseName);

        return getAllComputeNodeIds(warehouse.getId());
    }

    public List<Long> getAllComputeNodeIds(long warehouseId) {
        long workerGroupId = selectWorkerGroupInternal(warehouseId)
                .orElse(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
        return getAllComputeNodeIds(warehouseId, workerGroupId);
    }

    private List<Long> getAllComputeNodeIds(long warehouseId, long workerGroupId) {
        Warehouse warehouse = getWarehouse(warehouseId);

        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkersByWorkerGroup(workerGroupId);
        } catch (StarRocksException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    public List<ComputeNode> getAliveComputeNodes(long warehouseId) {
        Optional<Long> workerGroupId = selectWorkerGroupInternal(warehouseId);
        if (workerGroupId.isEmpty()) {
            return new ArrayList<>();
        }
        return getAliveComputeNodes(warehouseId, workerGroupId.get());
    }

    private List<ComputeNode> getAliveComputeNodes(long warehouseId, long workerGroupId) {
        List<Long> computeNodeIds = getAllComputeNodeIds(warehouseId, workerGroupId);
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<ComputeNode> nodes = computeNodeIds.stream()
                .map(id -> systemInfoService.getBackendOrComputeNode(id))
                .filter(ComputeNode::isAlive).collect(Collectors.toList());
        return nodes;
    }

    public Long getComputeNodeId(Long warehouseId, LakeTablet tablet) {
        try {
            long workerGroupId = selectWorkerGroupInternal(warehouseId)
                    .orElse(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getShardInfo(tablet.getShardId(), workerGroupId);

            Long nodeId;
            Set<Long> ids = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsByShard(shardInfo, true);
            if (!ids.isEmpty()) {
                nodeId = ids.iterator().next();
                return nodeId;
            } else {
                return null;
>>>>>>> 6cd9fbc95f ([Enhancement] Add cluster idle HTTP api (#53850))
            }
            checksum ^= warehouseCount;
            LOG.info("finished replaying WarehouseMgr from image");
        } catch (EOFException e) {
            LOG.info("no WarehouseMgr to replay.");
        }
        return checksum;
    }

<<<<<<< HEAD
    public List<List<String>> getWarehousesInfo() {
        return new WarehouseProcDir(this).fetchResult().getRows();
=======
    public Long getComputeNodeId(String warehouseName, LakeTablet tablet) {
        Warehouse warehouse = getWarehouse(warehouseName);

        try {
            long workerGroupId = selectWorkerGroupInternal(warehouse.getId()).orElse(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getShardInfo(tablet.getShardId(), workerGroupId);

            Long nodeId;
            Set<Long> ids = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsByShard(shardInfo, true);
            if (!ids.isEmpty()) {
                nodeId = ids.iterator().next();
                return nodeId;
            } else {
                return null;
            }
        } catch (StarClientException e) {
            return null;
        }
    }

    public Set<Long> getAllComputeNodeIdsAssignToTablet(Long warehouseId, LakeTablet tablet) {
        try {
            long workerGroupId = selectWorkerGroupInternal(warehouseId).orElse(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getShardInfo(tablet.getShardId(), workerGroupId);

            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsByShard(shardInfo, true);
        } catch (StarClientException e) {
            return null;
        }
    }

    public ComputeNode getComputeNodeAssignedToTablet(String warehouseName, LakeTablet tablet) {
        Warehouse warehouse = getWarehouse(warehouseName);
        return getComputeNodeAssignedToTablet(warehouse.getId(), tablet);
    }

    public ComputeNode getComputeNodeAssignedToTablet(Long warehouseId, LakeTablet tablet) {
        Long computeNodeId = getComputeNodeId(warehouseId, tablet);
        if (computeNodeId == null) {
            Warehouse warehouse = idToWh.get(warehouseId);
            throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE,
                    String.format("name: %s", warehouse.getName()));
        }
        Preconditions.checkNotNull(computeNodeId);
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(computeNodeId);
    }

    private final AtomicInteger nextComputeNodeIndex = new AtomicInteger(0);

    public AtomicInteger getNextComputeNodeIndexFromWarehouse(long warehouseId) {
        return nextComputeNodeIndex;
>>>>>>> 6cd9fbc95f ([Enhancement] Add cluster idle HTTP api (#53850))
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
