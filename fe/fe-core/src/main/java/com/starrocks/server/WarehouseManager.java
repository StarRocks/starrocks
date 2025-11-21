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

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.staros.util.LockCloseable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.DropWarehouseLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WarehouseInternalOpLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.MultipleWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.ComputeResourceProvider;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import org.apache.arrow.util.VisibleForTesting;
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

public class WarehouseManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    public static final String DEFAULT_WAREHOUSE_NAME = "default_warehouse";
    public static final long DEFAULT_WAREHOUSE_ID = 0L;
    public static final long INVALID_WAREHOUSE_ID = -1L;

    // default compute resource
    public static final ComputeResource DEFAULT_RESOURCE = WarehouseComputeResource.DEFAULT;
    // computeResourceProvider is used to acquire cngroup resource from warehouse
    protected final ComputeResourceProvider computeResourceProvider;

    protected final Map<Long, Warehouse> idToWh = new HashMap<>();
    protected final Map<String, Warehouse> nameToWh = new HashMap<>();

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Listeners for warehouse events
    protected final List<WarehouseEventListener> warehouseEventListeners;

    public WarehouseManager(ComputeResourceProvider computeResourceProvider,
                            List<WarehouseEventListener> warehouseEventListeners) {
        this.computeResourceProvider = computeResourceProvider;
        this.warehouseEventListeners = warehouseEventListeners;
    }

    public WarehouseManager() {
        this(new WarehouseComputeResourceProvider(), Lists.newArrayList());
    }

    public void initDefaultWarehouse() {
        // gen a default warehouse
        // Be sure to make this initialization idempotent, the upper level may invoke it multiple times without
        // knowing it is initialized or not.
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            if (!nameToWh.containsKey(DEFAULT_WAREHOUSE_NAME)) {
                Warehouse wh = new DefaultWarehouse(DEFAULT_WAREHOUSE_ID, DEFAULT_WAREHOUSE_NAME);
                nameToWh.put(wh.getName(), wh);
                idToWh.put(wh.getId(), wh);
            }
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
                if (Config.enable_rollback_default_warehouse) {
                    return new DefaultWarehouse(DEFAULT_WAREHOUSE_ID, DEFAULT_WAREHOUSE_NAME);
                }
                throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("name: %s", warehouseName));
            }
            return warehouse;
        }
    }

    public Warehouse getWarehouse(long warehouseId) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse warehouse = idToWh.get(warehouseId);
            if (warehouse == null) {
                if (Config.enable_rollback_default_warehouse) {
                    return getWarehouse(DEFAULT_WAREHOUSE_NAME);
                }
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

    /**
     * Acquire an available compute resource from the warehouse manager by warehouse id and previous compute resource.
     * @param warehouseId: the id of the warehouse to acquire compute resource from.
     * @param prev: the previous compute resource, which is used to get the next compute resource from the warehouse.
     * @return: the acquired compute resource
     */
    public ComputeResource acquireComputeResource(long warehouseId, ComputeResource prev) {
        if (!RunMode.isSharedDataMode()) {
            return WarehouseComputeResource.DEFAULT;
        }
        CRAcquireContext acquireContext = CRAcquireContext.of(warehouseId, prev);
        return acquireComputeResource(acquireContext);
    }

    /**
     * Acquire an available compute resource from the warehouse manager by warehouse id.
     * @param warehouseId: the id of the warehouse to acquire compute resource from.
     * @return: the acquired compute resource
     */
    public ComputeResource acquireComputeResource(long warehouseId) {
        if (!RunMode.isSharedDataMode()) {
            return WarehouseComputeResource.DEFAULT;
        }
        CRAcquireContext acquireContext = CRAcquireContext.of(warehouseId);
        return acquireComputeResource(acquireContext);
    }

    /**
     * Acquire an available compute resource from the warehouse manager, and the following execution unit will
     * use this compute resource unless a new one is acquired.
     * @param acquireContext the context for acquiring compute resource, which contains the warehouse id and other parameters.
     * @throws RuntimeException if there are no available cngroup in the warehouse.
     */
    public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
        if (!RunMode.isSharedDataMode()) {
            return WarehouseComputeResource.DEFAULT;
        }
        final long warehouseId = acquireContext.getWarehouseId();
        final Warehouse warehouse = getWarehouse(warehouseId);
        if (warehouse == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", warehouseId));
        }
        Optional<ComputeResource> result = computeResourceProvider.acquireComputeResource(warehouse, acquireContext);
        if (result.isEmpty()) {
            throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_UNAVAILABLE, warehouse.getName());
        }
        ComputeResource computeResource = result.get();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Acquired cngroup resource: {}", computeResource);
        }
        return computeResource;
    }

    /**
     * Get the warehouse and compute resource name for the given compute resource.
     * @param computeResource: the compute resource to get the warehouse name from
     * @return: the warehouse name, empty if the compute resource is not available
     */
    public String getWarehouseComputeResourceName(ComputeResource computeResource) {
        if (!RunMode.isSharedDataMode()) {
            return "";
        }
        try {
            final Warehouse warehouse = getWarehouse(computeResource.getWarehouseId());
            return String.format("%s", warehouse.getName());
        } catch (Exception e) {
            LOG.warn("Failed to get warehouse name for computeResource: {}", computeResource, e);
            return "";
        }
    }

    /**
     * Get the compute resource name for the given compute resource.
     * @param computeResource: the compute resource to get the name from
     * @return: the compute resource name, empty if the compute resource is not available
     */
    public String getComputeResourceName(ComputeResource computeResource) {
        return "";
    }

    /**
     * Check whether the resource is available, this method will not throw exception
     * @param computeResource: the compute resource to check
     * @return: true if the resource is available, false otherwise
     */
    public boolean isResourceAvailable(ComputeResource computeResource) {
        if (!RunMode.isSharedDataMode()) {
            return true;
        }
        return computeResourceProvider.isResourceAvailable(computeResource);
    }

    /**
     * Get all compute node ids in the warehouse.
     * @param warehouseId: the id of the warehouse
     * @return: a list of compute node ids in the warehouse, empty if the warehouse is not available
     */
    public List<Long> getAllComputeNodeIds(long warehouseId) {
        // check warehouse exists
        if (!warehouseExists(warehouseId)) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", warehouseId));
        }
        WarehouseComputeResource warehouseComputeResource = WarehouseComputeResource.of(warehouseId);
        return getAllComputeNodeIds(warehouseComputeResource);
    }

    /**
     * Get all compute node ids in the warehouse.
     * @param computeResource: the compute resource to get the compute node ids from
     * @return: a list of compute node ids in the warehouse, empty if the compute resource is not available
     */
    public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
        // check warehouse exists
        if (!warehouseExists(computeResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", computeResource.getWarehouseId()));
        }
        return computeResourceProvider.getAllComputeNodeIds(computeResource);
    }

    /**
     * Get all alive compute nodes in the warehouse.
     * @param computeResource: the compute resource to get the alive compute nodes from
     * @return: a list of alive compute nodes in the warehouse, empty if the compute resource is not available
     */
    public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
        // check warehouse exists
        if (!warehouseExists(computeResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", computeResource.getWarehouseId()));
        }
        return computeResourceProvider.getAliveComputeNodes(computeResource);
    }

    public Long getComputeNodeId(ComputeResource computeResource, long tabletId) {
        // check warehouse exists
        if (!warehouseExists(computeResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", computeResource.getWarehouseId()));
        }
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getPrimaryComputeNodeIdByShard(tabletId, computeResource.getWorkerGroupId());
        } catch (StarRocksException e) {
            return null;
        }
    }

    public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
        // check warehouse exists
        if (!warehouseExists(computeResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", computeResource.getWarehouseId()));
        }
        try {
            List<Long> nodeIds = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsByShard(tabletId, computeResource.getWorkerGroupId());
            Long nodeId = nodeIds
                    .stream()
                    .filter(id -> GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkBackendAlive(id) ||
                            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkComputeNodeAlive(id))
                    .findFirst()
                    .orElse(null);
            return nodeId;
        } catch (StarRocksException e) {
            LOG.warn("get alive compute node id to tablet {} fail {}.", tabletId, e.getMessage());
            return null;
        }
    }

    public List<Long> getAllComputeNodeIdsAssignToTablet(ComputeResource computeResource, long tabletId) {
        // check warehouse exists
        if (!warehouseExists(computeResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", computeResource.getWarehouseId()));
        }
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsByShard(tabletId, computeResource.getWorkerGroupId());
        } catch (StarRocksException e) {
            LOG.warn("get all compute node ids assign to tablet {} fail {}.", tabletId, e.getMessage());
            return null;
        }
    }

    public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId)
            throws ErrorReportException {
        // check warehouse exists
        if (!warehouseExists(computeResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", computeResource.getWarehouseId()));
        }
        Long computeNodeId = getAliveComputeNodeId(computeResource, tabletId);
        if (computeNodeId == null) {
            Warehouse warehouse = idToWh.get(computeResource.getWarehouseId());
            throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE,
                    String.format("name: %s", warehouse.getName()));
        }
        Preconditions.checkNotNull(computeNodeId);
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(computeNodeId);
    }

    private final AtomicInteger nextComputeNodeIndex = new AtomicInteger(0);

    public AtomicInteger getNextComputeNodeIndexFromWarehouse(ComputeResource computeResource) {
        return nextComputeNodeIndex;
    }

    public ComputeResource getCompactionComputeResource(long tableId) {
        return DEFAULT_RESOURCE;
    }

    public Warehouse getBackgroundWarehouse() {
        return getWarehouse(DEFAULT_WAREHOUSE_ID);
    }

    public Warehouse getBackgroundWarehouse(long tableId) {
        return getBackgroundWarehouse();
    }

    public ComputeResource getBackgroundComputeResource() {
        final Warehouse warehouse = getBackgroundWarehouse();
        return acquireComputeResource(CRAcquireContext.of(warehouse.getId()));
    }

    public ComputeResource getBackgroundComputeResource(long tableId) {
        final Warehouse warehouse = getBackgroundWarehouse(tableId);
        return acquireComputeResource(CRAcquireContext.of(warehouse.getId()));
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

    public void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        String warehouseName = stmt.getWarehouseName();

        // Check if warehouse already exists
        if (stmt.isSetIfNotExists()) {
            if (warehouseExists(warehouseName)) {
                LOG.info("Create warehouse[{}] which already exists", warehouseName);
                return;
            }
        } else {
            if (warehouseExists(warehouseName)) {
                throw new DdlException("Warehouse '" + warehouseName + "' already exists");
            }
        }

        // Generate new warehouse ID
        long warehouseId = GlobalStateMgr.getCurrentState().getNextId();

        long workerGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().createWorkerGroup("x1");
        // Create warehouse instance
        Warehouse warehouse = new MultipleWarehouse(warehouseId, warehouseName, workerGroupId);

        // Add to manager
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            nameToWh.put(warehouseName, warehouse);
            idToWh.put(warehouseId, warehouse);
        }

        // Log for replay
        GlobalStateMgr.getCurrentState().getEditLog().logEdit(OperationType.OP_CREATE_WAREHOUSE, warehouse);

        LOG.info("Successfully created warehouse: {}", warehouseName);
    }

    public void dropWarehouse(DropWarehouseStmt stmt) throws DdlException {
        String warehouseName = stmt.getWarehouseName();

        // Check if warehouse exists
        if (stmt.isSetIfExists()) {
            if (!warehouseExists(warehouseName)) {
                LOG.info("Drop warehouse[{}] which does not exist", warehouseName);
                return;
            }
        } else {
            if (!warehouseExists(warehouseName)) {
                throw new DdlException("Warehouse '" + warehouseName + "' does not exist");
            }
        }

        // Cannot drop default warehouse
        if (warehouseName.equals(DEFAULT_WAREHOUSE_NAME)) {
            throw new DdlException("Cannot drop default warehouse: " + DEFAULT_WAREHOUSE_NAME);
        }

        // Check if there are alive compute nodes in the warehouse
        Warehouse warehouse = getWarehouse(warehouseName);
        List<ComputeNode> aliveComputeNodes = this.getAliveComputeNodes(WarehouseComputeResource.of(warehouse.getId()));
        if (!aliveComputeNodes.isEmpty()) {
            throw new DdlException("Cannot drop warehouse '" + warehouseName + "' while there are alive compute nodes: "
                    + aliveComputeNodes);
        }

        // Clean up associated worker groups
        if (warehouse instanceof MultipleWarehouse multipleWarehouse) {
            GlobalStateMgr.getCurrentState().getStarOSAgent().deleteWorkerGroup(multipleWarehouse.getWorkerGroupId());
        }

        // Remove from manager
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            nameToWh.remove(warehouseName);
            idToWh.remove(warehouse.getId());
        }

        GlobalStateMgr.getCurrentState().getEditLog().logEdit(OperationType.OP_DROP_WAREHOUSE, warehouse);

        LOG.info("Successfully dropped warehouse: {}", warehouseName);
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

    public void createCnGroup(CreateCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    public void dropCnGroup(DropCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    public void enableCnGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    public void disableCnGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    public void alterCnGroup(AlterCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    public void recordWarehouseInfoForTable(long tableId, ComputeResource computeResource) {

    }

    public void removeTableWarehouseInfo(long tableId) {

    }

    public Set<String> getAllWarehouseNames() {
        return Sets.newHashSet(DEFAULT_WAREHOUSE_NAME);
    }

    public void replayCreateWarehouse(Warehouse warehouse) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            nameToWh.put(warehouse.getName(), warehouse);
            idToWh.put(warehouse.getId(), warehouse);
        }
        LOG.info("Replayed create warehouse: {}", warehouse.getName());
    }

    public void replayDropWarehouse(DropWarehouseLog log) {
        String warehouseName = log.getWarehouseName();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Warehouse warehouse = nameToWh.remove(warehouseName);
            if (warehouse != null) {
                idToWh.remove(warehouse.getId());
            }
        }
        LOG.info("Replayed drop warehouse: {}", warehouseName);
    }

    public void replayAlterWarehouse(Warehouse warehouse) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            // Update existing warehouse or add if not exists
            Warehouse existingWarehouse = nameToWh.get(warehouse.getName());
            if (existingWarehouse != null) {
                idToWh.remove(existingWarehouse.getId());
            }
            nameToWh.put(warehouse.getName(), warehouse);
            idToWh.put(warehouse.getId(), warehouse);
        }
        LOG.info("Replayed alter warehouse: {}", warehouse.getName());
    }

    public void replayInternalOpLog(WarehouseInternalOpLog log) {

    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        List<Warehouse> warehouses = getAllWarehouses();

        int numJson = 1 + warehouses.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.WAREHOUSE_MGR, numJson);
        writer.writeInt(warehouses.size());
        for (Warehouse warehouse : warehouses) {
            writer.writeJson(warehouse);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        // Create the default warehouse during the WarehouseManager::load() invoked by loadImage()
        // The default_warehouse is not persisted through WarehouseManager::save(), but some of the image loads and
        // postImageLoad actions may depend on default_warehouse to perform actions.
        // The default_warehouse must be ready before postImageLoad.
        initDefaultWarehouse();
        reader.readCollection(Warehouse.class, this::replayCreateWarehouse);
    }

    public void addWarehouse(Warehouse warehouse) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            nameToWh.put(warehouse.getName(), warehouse);
            idToWh.put(warehouse.getId(), warehouse);
        }
    }

    public List<WarehouseEventListener> getWarehouseListeners() {
        return warehouseEventListeners;
    }

    public ComputeResourceProvider getComputeResourceProvider() {
        return computeResourceProvider;
    }

    public int getAllWorkerGroupCount() {
        int cnt = 0;
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            for (Warehouse wh : idToWh.values()) {
                cnt += wh.getWorkerGroupIds().size();
            }
        }
        return cnt;
    }

    @VisibleForTesting
    public void clearWarehouse() {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            // only clean nameToWh where name is not default warehouse
            nameToWh.entrySet().removeIf(entry -> !entry.getKey().equals(DEFAULT_WAREHOUSE_NAME));
            idToWh.entrySet().removeIf(entry -> !entry.getKey().equals(DEFAULT_WAREHOUSE_ID));
        }
    }
}
