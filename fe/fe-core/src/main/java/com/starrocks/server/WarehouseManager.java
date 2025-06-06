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
import com.starrocks.persist.DropWarehouseLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.WarehouseInternalOpLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
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
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.ComputeResourceProvider;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
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

    // default compute resource
    public static final ComputeResource DEFAULT_RESOURCE = WarehouseComputeResource.DEFAULT;
    // computeResourceProvider is used to acquire cngroup resource from warehouse
    protected final ComputeResourceProvider computeResourceProvider;

    protected final Map<Long, Warehouse> idToWh = new HashMap<>();
    protected final Map<String, Warehouse> nameToWh = new HashMap<>();

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public WarehouseManager(ComputeResourceProvider computeResourceProvider) {
        this.computeResourceProvider = computeResourceProvider;
    }

    public WarehouseManager() {
        this.computeResourceProvider = new WarehouseComputeResourceProvider();
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

    public Long getComputeNodeId(ComputeResource computeResource, LakeTablet tablet) {
        // check warehouse exists
        if (!warehouseExists(computeResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", computeResource.getWarehouseId()));
        }
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getPrimaryComputeNodeIdByShard(tablet.getShardId(), computeResource.getWorkerGroupId());
        } catch (StarRocksException e) {
            return null;
        }
    }

    public List<Long> getAllComputeNodeIdsAssignToTablet(ComputeResource computeResource, LakeTablet tablet) {
        // check warehouse exists
        if (!warehouseExists(computeResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", computeResource.getWarehouseId()));
        }
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsByShard(tablet.getShardId(), computeResource.getWorkerGroupId());
        } catch (StarRocksException e) {
            return null;
        }
    }

    public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, LakeTablet tablet) {
        // check warehouse exists
        if (!warehouseExists(computeResource.getWarehouseId())) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", computeResource.getWarehouseId()));
        }
        Long computeNodeId = getComputeNodeId(computeResource, tablet);
        if (computeNodeId == null) {
            Warehouse warehouse = idToWh.get(computeResource.getWarehouseId());
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

    public ComputeResource getBackgroundComputeResource() {
        final Warehouse warehouse = getBackgroundWarehouse();
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

    public Set<String> getAllWarehouseNames() {
        return Sets.newHashSet(DEFAULT_WAREHOUSE_NAME);
    }

    public void replayCreateWarehouse(Warehouse warehouse) {

    }

    public void replayDropWarehouse(DropWarehouseLog log) {

    }

    public void replayAlterWarehouse(Warehouse warehouse) {

    }

    public void replayInternalOpLog(WarehouseInternalOpLog log) {

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
