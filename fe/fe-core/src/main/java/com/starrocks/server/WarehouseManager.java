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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.DropWarehouseLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class WarehouseManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    public static final String DEFAULT_WAREHOUSE_NAME = "default_warehouse";
    public static final long DEFAULT_WAREHOUSE_ID = 0L;

    protected final Map<Long, Warehouse> idToWh = new HashMap<>();
    protected final Map<String, Warehouse> nameToWh = new HashMap<>();

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public WarehouseManager() {
    }

    public void initDefaultWarehouse() {
        // gen a default warehouse
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Warehouse wh = new DefaultWarehouse(DEFAULT_WAREHOUSE_ID, DEFAULT_WAREHOUSE_NAME);
            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);
        }
    }

    public List<Warehouse> getAllWarehouses() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return new ArrayList<>(nameToWh.values());
        }
    }

    public List<Long> getAllWarehouseIds() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return new ArrayList<>(idToWh.keySet());
        }
    }

    public Warehouse getWarehouse(String warehouseName) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse warehouse = nameToWh.get(warehouseName);
            if (warehouse == null) {
                throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("name: %s", warehouseName));
            }
            return warehouse;
        }
    }

    public Warehouse getWarehouse(long warehouseId) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse warehouse = idToWh.get(warehouseId);
            if (warehouse == null) {
                throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("id: %d", warehouseId));
            }
            return warehouse;
        }
    }

    public Warehouse getWarehouseAllowNull(String warehouseName) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return nameToWh.get(warehouseName);
        }
    }

    public Warehouse getWarehouseAllowNull(long warehouseId) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return idToWh.get(warehouseId);
        }
    }

    public boolean warehouseExists(String warehouseName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.containsKey(warehouseName);
        }
    }

    public boolean warehouseExists(long warehouseId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return idToWh.containsKey(warehouseId);
        }
    }

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
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getPrimaryComputeNodeIdByShard(tablet.getShardId(), workerGroupId);
        } catch (StarRocksException e) {
            return null;
        }
    }

    public List<Long> getAllComputeNodeIdsAssignToTablet(Long warehouseId, LakeTablet tablet) {
        try {
            long workerGroupId = selectWorkerGroupInternal(warehouseId).orElse(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsByShard(tablet.getShardId(), workerGroupId);
        } catch (StarRocksException e) {
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
    }

    public Warehouse getCompactionWarehouse() {
        return getWarehouse(DEFAULT_WAREHOUSE_ID);
    }

    public Warehouse getBackgroundWarehouse() {
        return getWarehouse(DEFAULT_WAREHOUSE_ID);
    }

    public Optional<Long> selectWorkerGroupByWarehouseId(long warehouseId) {
        Optional<Long> workerGroupId = selectWorkerGroupInternal(warehouseId);
        if (workerGroupId.isEmpty()) {
            return workerGroupId;
        }

        List<ComputeNode> aliveNodes = getAliveComputeNodes(warehouseId, workerGroupId.get());
        if (CollectionUtils.isEmpty(aliveNodes)) {
            Warehouse warehouse = getWarehouse(warehouseId);
            LOG.warn("there is no alive workers in warehouse: " + warehouse.getName());
            return Optional.empty();
        }

        return workerGroupId;
    }

    public long getWarehouseResumeTime(long warehouseId) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse warehouse = idToWh.get(warehouseId);
            if (warehouse == null) {
                return -1;
            } else {
                return warehouse.getResumeTime();
            }
        }
    }

    private Optional<Long> selectWorkerGroupInternal(long warehouseId) {
        Warehouse warehouse = getWarehouse(warehouseId);
        List<Long> ids = warehouse.getWorkerGroupIds();
        if (CollectionUtils.isEmpty(ids)) {
            LOG.warn("failed to get worker group id from warehouse {}", warehouse);
            return Optional.empty();
        }

        return Optional.of(ids.get(0));
    }

    public void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        throw new DdlException("Multi-Warehouse is not implemented");
    }

    public void dropWarehouse(DropWarehouseStmt stmt) throws DdlException {
        throw new DdlException("Multi-Warehouse is not implemented");
    }

    public void suspendWarehouse(SuspendWarehouseStmt stmt) throws DdlException {
        throw new DdlException("Multi-Warehouse is not implemented");
    }

    public void resumeWarehouse(ResumeWarehouseStmt stmt) throws DdlException {
        throw new DdlException("Multi-Warehouse is not implemented");
    }

    public void alterWarehouse(AlterWarehouseStmt stmt) throws DdlException {
        throw new DdlException("Multi-Warehouse is not implemented");
    }

    public Set<String> getAllWarehouseNames() {
        return Sets.newHashSet(DEFAULT_WAREHOUSE_NAME);
    }

    public void replayCreateWarehouse(Warehouse warehouse) {

    }

    public void replayDropWarehouse(DropWarehouseLog log) {

    }

    public void replayAlterWarehouse(Warehouse warehouse) {

    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
    }

    public void addWarehouse(Warehouse warehouse) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            nameToWh.put(warehouse.getName(), warehouse);
            idToWh.put(warehouse.getId(), warehouse);
        }
    }
}
