// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.warehouse;

import com.google.common.base.Preconditions;
import com.staros.client.StarClientException;
import com.staros.proto.ShardInfo;
import com.staros.util.LockCloseable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.DropWarehouseLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
                        DEFAULT_WAREHOUSE_NAME, DEFAULT_CLUSTER_ID, new WarehouseProperty(),
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
            throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_SUSPENDED, String.format("name: %s", warehouse.getName()));
        }
    }

    /**
     * get all compute node from warehouse. Note: the warehouse should exist and be available, otherwise exception will be thrown.
     *
     * @param warehouseId
     * @return
     * @exceptions ERR_UNKNOWN_WAREHOUSE, ERR_WAREHOUSE_SUSPENDED
     */
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

    @Override
    public Long getComputeNodeId(String warehouseName, LakeTablet tablet) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseName);
        checkWarehouseState(warehouse);
        try {
            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getShardInfo(tablet.getShardId(), warehouse.getAnyAvailableCluster().getWorkerGroupId());

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

    @Override
    public Set<Long> getAllComputeNodeIdsAssignToTablet(Long warehouseId, LakeTablet tablet) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseId);
        checkWarehouseState(warehouse);
        try {
            ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getShardInfo(tablet.getShardId(), warehouse.getAnyAvailableCluster().getWorkerGroupId());

            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsByShard(shardInfo, true);
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
        if (computeNodeId == null) {
            throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE, String.format("id: %d", warehouseId));
        }
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(computeNodeId);
    }

    @Override
    public AtomicInteger getNextComputeNodeIndexFromWarehouse(long warehouseId) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseId);
        checkWarehouseState(warehouse);
        return warehouse.getAnyAvailableCluster().getNextComputeNodeHostId();
    }

    @Override
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
                ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_EXISTS, String.format("name: %s", warehouseName));
            }
            WarehouseProperty warehouseProperty = new WarehouseProperty();
            Map<String, String> properties = stmt.getProperties();
            if (properties != null) {
                int computeReplica = Integer.parseInt(properties.getOrDefault(WarehouseProperty.PROPERTY_COMPUTE_REPLICA,
                        String.valueOf(WarehouseProperty.DEFAULT_REPLICA_NUMBER)));
                if (computeReplica <= 0) {
                    throw new DdlException("warehouse compute replica can not be <= 0");
                }
                if (computeReplica > Config.lake_warehouse_max_compute_replica) {
                    throw new DdlException("warehouse compute replica can not be larger than " +
                            Config.lake_warehouse_max_compute_replica);
                }
                warehouseProperty.setComputeReplica(computeReplica);
            }

            long id = GlobalStateMgr.getCurrentState().getNextId();
            long clusterId = GlobalStateMgr.getCurrentState().getNextId();
            String comment = stmt.getComment();
            LocalWarehouse wh = new LocalWarehouse(id, warehouseName, clusterId, warehouseProperty, comment);

            for (Cluster cluster : wh.getClusters().values()) {
                try {
                    StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
                    cluster.setWorkerGroupId(starOSAgent.createWorkerGroup("x0", warehouseProperty.getComputeReplica()));
                } catch (DdlException e) {
                    LOG.warn(e);
                    throw new DdlException("create warehouse " + wh.getName() + " failed, reason: " + e);
                }
            }

            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logEdit(OperationType.OP_CREATE_WAREHOUSE, wh);
            LOG.info("createWarehouse whName = " + warehouseName + ", id = " + id + ", " +
                    "comment = " + comment);
        }
    }

    @Override
    public void replayCreateWarehouse(Warehouse warehouse) {
        String whName = warehouse.getName();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(!nameToWh.containsKey(whName), "Warehouse '%s' already exists", whName);
            nameToWh.put(whName, warehouse);
            idToWh.put(warehouse.getId(), warehouse);
        }
    }

    @Override
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
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("name: %s", warehouseName));
            }

            nameToWh.remove(warehouseName);
            idToWh.remove(warehouse.getId());
            warehouse.dropSelf();
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logEdit(OperationType.OP_DROP_WAREHOUSE, new DropWarehouseLog(warehouseName));
        }
    }

    @Override
    public void replayDropWarehouse(DropWarehouseLog log) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            String warehouseName = log.getWarehouseName();
            if (nameToWh.containsKey(warehouseName)) {
                Warehouse warehouse = nameToWh.remove(warehouseName);
                idToWh.remove(warehouse.getId());
            }
        }
    }

    @Override
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
                ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_SUSPENDED, String.format("name: %s", warehouseName));
            }
            warehouse.suspendSelf();
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logEdit(OperationType.OP_ALTER_WAREHOUSE, warehouse);
        }
    }

    @Override
    public void replayAlterWarehouse(Warehouse warehouse) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            nameToWh.put(warehouse.getName(), warehouse);
            idToWh.put(warehouse.getId(), warehouse);
        }
    }

    @Override
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
                throw new DdlException("Can't resume an available warehouse");
            }
            warehouse.resumeSelf();
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logEdit(OperationType.OP_ALTER_WAREHOUSE, warehouse);
        }
    }

    @Override
    public void alterWarehouse(AlterWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }

        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToWh.containsKey(warehouseName),
                    "Warehouse '%s' doesn't exist", warehouseName);

            Map<String, String> properties = stmt.getProperties();
            if (properties == null || properties.isEmpty()) {
                return;
            }
            WarehouseProperty warehouseProperty = new WarehouseProperty();
            if (properties.get(WarehouseProperty.PROPERTY_COMPUTE_REPLICA) != null) {
                int computeReplica = Integer.parseInt(properties.get(WarehouseProperty.PROPERTY_COMPUTE_REPLICA));
                if (computeReplica <= 0) {
                    throw new DdlException("warehouse compute replica can not be <= 0");
                }
                if (computeReplica > Config.lake_warehouse_max_compute_replica) {
                    throw new DdlException("warehouse compute replica can not be larger than " +
                            Config.lake_warehouse_max_compute_replica);
                }
                warehouseProperty.setComputeReplica(computeReplica);

                LocalWarehouse warehouse = (LocalWarehouse) nameToWh.get(warehouseName);
                // TODO: operation below is not atomic
                StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
                for (Cluster cluster : warehouse.getClusters().values()) {
                    try {
                        starOSAgent.updateWorkerGroup(cluster.getWorkerGroupId(), warehouseProperty.getComputeReplica());
                    } catch (DdlException e) {
                        LOG.warn(e);
                        throw new DdlException("alter warehouse " + warehouse.getName() + " failed, reason: " + e);
                    }
                }
                warehouse.setProperty(warehouseProperty);
                EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
                editLog.logEdit(OperationType.OP_ALTER_WAREHOUSE, warehouse);
            } else {
                throw new DdlException("unsupported warehouse property");
            }
        }
    }

    @Override
    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.WAREHOUSE_MGR, nameToWh.size() + 1);
        writer.writeInt(nameToWh.size());
        for (Warehouse warehouse : nameToWh.values()) {
            writer.writeJson(warehouse);
        }
        writer.close();
    }

    @Override
    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        reader.readCollection(Warehouse.class, warehouse -> {
            this.nameToWh.put(warehouse.getName(), warehouse);
            this.idToWh.put(warehouse.getId(), warehouse);
        });
    }

    @Override
    public Warehouse getCompactionWarehouse() {
        return getWarehouse(Config.lake_compaction_warehouse);
    }

    @Override
    public Warehouse getBackgroundWarehouse() {
        return getWarehouse(Config.lake_background_warehouse);
    }
}

