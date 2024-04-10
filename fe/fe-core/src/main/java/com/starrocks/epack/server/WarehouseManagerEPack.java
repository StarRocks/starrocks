// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.server;

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.staros.client.StarClientException;
import com.staros.proto.ShardInfo;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.UserException;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.epack.persist.DropWarehouseLog;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.epack.warehouse.Cluster;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class WarehouseManagerEPack extends WarehouseManager {
    private static final Logger LOG = LogManager.getLogger(WarehouseManagerEPack.class);
    public static final long DEFAULT_CLUSTER_ID = 0L;

    @Override
    public void initDefaultWarehouse() {
        // gen a default warehouse
        // NOTE: default warehouse use DEFAULT_WORKER_GROUP_ID, which is 0,
        // so it is unnecessary to create a worker group for it.
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            // FE Leader or FE Follower both execute initDefaultWarehouse during startup that will generate
            // the default warehouse, and it's state is always AVAILABLE.
            // If the state of default warehouse is updated, e.g. SUSPENDED, we should not overwrite the state.
            if (!nameToWh.containsKey(DEFAULT_WAREHOUSE_NAME)) {
                Warehouse wh = new LocalWarehouse(DEFAULT_WAREHOUSE_ID,
                        DEFAULT_WAREHOUSE_NAME, DEFAULT_CLUSTER_ID,
                        "An internal warehouse init after FE is ready");
                nameToWh.put(wh.getName(), wh);
                idToWh.put(wh.getId(), wh);
            }
        }
    }

    public Set<String> getAllWarehouseNames() {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.keySet();
        }
    }

    private void checkWarehouseState(LocalWarehouse warehouse) {
        if (warehouse.getState() == LocalWarehouse.WarehouseState.SUSPENDED) {
            ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_SUSPENDED, warehouse.getName());
        }
    }

    @Override
    public List<Long> getAllComputeNodeIds(long warehouseId) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseId);
        checkWarehouseState(warehouse);
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getWorkersByWorkerGroup(warehouse.getAnyAvailableCluster().getWorkerGroupId());
        } catch (UserException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    @Override
    public List<Long> getAllComputeNodeIds(String warehouseName) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseName);
        checkWarehouseState(warehouse);
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getWorkersByWorkerGroup(warehouse.getAnyAvailableCluster().getWorkerGroupId());
        } catch (UserException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    @Override
    public Long getComputeNodeId(Long warehouseId, LakeTablet tablet) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseId);
        checkWarehouseState(warehouse);
        try {
            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getShardInfo(tablet.getShardId(), warehouse.getAnyAvailableCluster().getWorkerGroupId());

            Long nodeId;
            Set<Long> ids = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllBackendIdsByShard(shardInfo, true);
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

    @Override
    public Long getComputeNodeId(String warehouseName, LakeTablet tablet) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseName);
        checkWarehouseState(warehouse);
        try {
            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getShardInfo(tablet.getShardId(), warehouse.getAnyAvailableCluster().getWorkerGroupId());

            Long nodeId;
            Set<Long> ids = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllBackendIdsByShard(shardInfo, true);
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

    @Override
    public Set<Long> getAllComputeNodeIdsAssignToTablet(Long warehouseId, LakeTablet tablet) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseId);
        checkWarehouseState(warehouse);
        try {
            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getShardInfo(tablet.getShardId(), warehouse.getAnyAvailableCluster().getWorkerGroupId());

            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllBackendIdsByShard(shardInfo, true);
        } catch (StarClientException e) {
            return null;
        }
    }

    @Override
    public ComputeNode getComputeNodeAssignedToTablet(String warehouseName, LakeTablet tablet) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseName);
        checkWarehouseState(warehouse);
        return getComputeNodeAssignedToTablet(warehouse.getId(), tablet);
    }

    @Override
    public ComputeNode getComputeNodeAssignedToTablet(Long warehouseId, LakeTablet tablet) {
        Long computeNodeId = getComputeNodeId(warehouseId, tablet);
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(computeNodeId);
    }

    @Override
    public AtomicInteger getNextComputeNodeIndexFromWarehouse(long warehouseId) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseId);
        checkWarehouseState(warehouse);
        return warehouse.getAnyAvailableCluster().getNextComputeNodeHostId();
    }

    public void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }

        String warehouseName = stmt.getWarehouseName();

        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            if (nameToWh.containsKey(warehouseName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("Warehouse {} already exists", warehouseName);
                    return;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_EXISTS, warehouseName);
            }

            long id = GlobalStateMgr.getCurrentState().getNextId();
            long clusterId = GlobalStateMgr.getCurrentState().getNextId();
            String comment = stmt.getComment();
            LocalWarehouse wh = new LocalWarehouse(id, warehouseName, clusterId, comment);

            for (Cluster cluster : wh.getClusters().values()) {
                try {
                    StarOSAgentEpack starOSAgent = (StarOSAgentEpack) GlobalStateMgr.getCurrentState().getStarOSAgent();
                    cluster.setWorkerGroupId(starOSAgent.createWorkerGroup("x0"));
                } catch (DdlException e) {
                    LOG.warn(e);
                    throw new DdlException("create warehouse " + wh.getName() + " failed, reason: " + e);
                }
            }

            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);
            EditLogEPack editLog = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logCreateWarehouse(wh);
            LOG.info("createWarehouse whName = " + warehouseName + ", id = " + id + ", " +
                    "comment = " + comment);
        }
    }

    public void replayCreateWarehouse(Warehouse warehouse) {
        String whName = warehouse.getName();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(!nameToWh.containsKey(whName), "Warehouse '%s' already exists", whName);
            nameToWh.put(whName, warehouse);
            idToWh.put(warehouse.getId(), warehouse);
        }
    }

    public void dropWarehouse(DropWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }

        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            LocalWarehouse warehouse = (LocalWarehouse) nameToWh.get(warehouseName);
            if (warehouse == null) {
                if (stmt.isSetIfExists()) {
                    return;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
            }

            nameToWh.remove(warehouseName);
            idToWh.remove(warehouse.getId());
            warehouse.dropSelf();
            EditLogEPack editLog = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logDropWarehouse(new DropWarehouseLog(warehouseName));
        }
    }

    public void replayDropWarehouse(DropWarehouseLog log) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            String warehouseName = log.getWarehouseName();
            if (nameToWh.containsKey(warehouseName)) {
                Warehouse warehouse = nameToWh.remove(warehouseName);
                idToWh.remove(warehouse.getId());
            }
        }
    }

    public void suspendWarehouse(SuspendWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }

        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToWh.containsKey(warehouseName),
                    "Warehouse '%s' doesn't exist", warehouseName);

            LocalWarehouse warehouse = (LocalWarehouse) nameToWh.get(warehouseName);
            if (warehouse.getState() == LocalWarehouse.WarehouseState.SUSPENDED) {
                ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_SUSPENDED, warehouseName);
            }
            warehouse.suspendSelf();
            EditLogEPack editLog = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logAlterWarehouse(warehouse);
        }
    }

    public void replayAlterWarehouse(Warehouse warehouse) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            nameToWh.put(warehouse.getName(), warehouse);
            idToWh.put(warehouse.getId(), warehouse);
        }
    }

    public void resumeWarehouse(ResumeWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }

        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToWh.containsKey(warehouseName),
                    "Warehouse '%s' doesn't exist", warehouseName);
            LocalWarehouse warehouse = (LocalWarehouse) nameToWh.get(warehouseName);
            if (warehouse.getState() == LocalWarehouse.WarehouseState.AVAILABLE) {
                ErrorReport.reportDdlException("Can't resume an available warehouse");
            }
            warehouse.resumeSelf();
            EditLogEPack editLog = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logAlterWarehouse(warehouse);
        }
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockIDEPack.WAREHOUSE_MGR, nameToWh.size() + 1);
        writer.writeJson(nameToWh.size());
        for (Warehouse warehouse : nameToWh.values()) {
            writer.writeJson(warehouse);
        }
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        int nameToWhSize = reader.readJson(int.class);
        for (int i = 0; i != nameToWhSize; ++i) {
            Warehouse warehouse = reader.readJson(Warehouse.class);
            this.nameToWh.put(warehouse.getName(), warehouse);
            this.idToWh.put(warehouse.getId(), warehouse);
        }
    }
}

