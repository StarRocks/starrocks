// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.warehouse;

import com.google.common.base.Preconditions;
import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.staros.util.LockCloseable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
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
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
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
import java.util.HashMap;
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
                onCreateWarehouse(wh);
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
        } catch (StarRocksException e) {
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
        } catch (StarRocksException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    @Override
    public Long getComputeNodeId(Long warehouseId, LakeTablet tablet) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseId);
        checkWarehouseState(warehouse);
        long workerGroupId = warehouse.getAnyAvailableCluster().getWorkerGroupId();
        try {
            Set<Long> ids = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsInWorkerGroupByShard(tablet.getShardId(), workerGroupId, true);
            if (!ids.isEmpty()) {
                return ids.iterator().next();
            } else {
                return null;
            }
        } catch (StarRocksException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return null;
        }
    }

    @Override
    public List<Long> getAllComputeNodeIdsAssignToTablet(Long warehouseId, LakeTablet tablet) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(warehouseId);
        checkWarehouseState(warehouse);
        try {
            long workerGroupId = warehouse.getAnyAvailableCluster().getWorkerGroupId();
            Set<Long> nodeIds = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsInWorkerGroupByShard(tablet.getShardId(), workerGroupId, true);
            return new ArrayList<>(nodeIds);
        } catch (StarRocksException e) {
            LOG.warn("Fail to get all compute node ids assigned to tablet {}, {}", tablet.getId(), e.getMessage());
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
            if (stmt.getProperties() != null && !stmt.getProperties().isEmpty()) {
                Map<String, String> properties = new HashMap<>(stmt.getProperties());

                int computeReplica =
                        Integer.parseInt(properties.getOrDefault(WarehouseProperty.PROPERTY_COMPUTE_REPLICA,
                                String.valueOf(WarehouseProperty.DEFAULT_REPLICA_NUMBER)));
                if (computeReplica <= 0) {
                    throw new DdlException("warehouse compute replica can not be <= 0");
                }
                if (computeReplica > Config.lake_warehouse_max_compute_replica) {
                    throw new DdlException("warehouse compute replica can not be larger than " +
                            Config.lake_warehouse_max_compute_replica);
                }
                warehouseProperty.setComputeReplica(computeReplica);
                properties.remove(WarehouseProperty.PROPERTY_COMPUTE_REPLICA);

                // handle 'replication_type': {none, sync, async}, default to: none
                String replicationType = properties.getOrDefault(WarehouseProperty.PROPERTY_REPLICATION_TYPE,
                        WarehouseProperty.ReplicationType.NONE.toString());
                warehouseProperty.setReplicationType(WarehouseProperty.replicationTypeFromString(replicationType));
                properties.remove(WarehouseProperty.PROPERTY_REPLICATION_TYPE);

                // handle 'warmup_level': {none, meta, index, all}, default to: none
                String warmupLevel = properties.getOrDefault(WarehouseProperty.PROPERTY_WARMUP_LEVEL,
                        WarehouseProperty.WarmupLevelType.NONE.toString());
                warehouseProperty.setWarmupLevel(WarehouseProperty.warmupLevelTypeFromString(warmupLevel));
                properties.remove(WarehouseProperty.PROPERTY_WARMUP_LEVEL);

                // enable_query_queue
                if (properties.containsKey(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE)) {
                    boolean enableQueryQueue = properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE)
                            .equalsIgnoreCase("true");
                    warehouseProperty.setEnableQueryQueue(enableQueryQueue);
                    properties.remove(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE);
                }
                // enable_query_queue_load
                if (properties.containsKey(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_LOAD)) {
                    boolean enableQueryQueueLoad = properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_LOAD)
                            .equalsIgnoreCase("true");
                    warehouseProperty.setEnableQueryQueueLoad(enableQueryQueueLoad);
                    properties.remove(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_LOAD);
                }
                // enable_query_queue_statistic
                if (properties.containsKey(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC)) {
                    boolean enableQueryQueueStatistic = properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC)
                            .equalsIgnoreCase("true");
                    warehouseProperty.setEnableQueryQueueStatistic(enableQueryQueueStatistic);
                    properties.remove(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC);
                }
                // query_queue_max_queued_queries
                if (properties.containsKey(WarehouseProperty.PROPERTY_QUERY_QUEUE_MAX_QUEUED_QUERIES)) {
                    int queryQueueMaxQueuedQueries =
                            Integer.parseInt(properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_MAX_QUEUED_QUERIES));
                    warehouseProperty.setQueryQueueMaxQueuedQueries(queryQueueMaxQueuedQueries);
                    properties.remove(WarehouseProperty.PROPERTY_QUERY_QUEUE_MAX_QUEUED_QUERIES);
                }
                // query_queue_max_queued_queries
                if (properties.containsKey(WarehouseProperty.PROPERTY_QUERY_QUEUE_PENDING_TIMEOUT_SECOND)) {
                    int queryQueuePendingTimeoutSecond =
                            Integer.parseInt(properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_PENDING_TIMEOUT_SECOND));
                    warehouseProperty.setQueryQueuePendingTimeoutSecond(queryQueuePendingTimeoutSecond);
                    properties.remove(WarehouseProperty.PROPERTY_QUERY_QUEUE_PENDING_TIMEOUT_SECOND);
                }
                // query_queue_max_queued_queries
                if (properties.containsKey(WarehouseProperty.PROPERTY_QUERY_QUEUE_CONCURRENCY_LIMIT)) {
                    int queryQueueConcurrencyLimit =
                            Integer.parseInt(properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_CONCURRENCY_LIMIT));
                    warehouseProperty.setQueryQueueConcurrencyLimit(queryQueueConcurrencyLimit);
                    properties.remove(WarehouseProperty.PROPERTY_QUERY_QUEUE_CONCURRENCY_LIMIT);
                }

                if (!properties.isEmpty()) {
                    throw new DdlException(String.format("Unknown warehouse properties: {%s}",
                            String.join(", ", properties.keySet())));
                }
            }
            long id = GlobalStateMgr.getCurrentState().getNextId();
            long clusterId = GlobalStateMgr.getCurrentState().getNextId();
            String comment = stmt.getComment();
            LocalWarehouse wh = new LocalWarehouse(id, warehouseName, clusterId, warehouseProperty, comment);

            for (Cluster cluster : wh.getClusters().values()) {
                try {
                    ReplicationType replicationType = toStarOSReplicationType(warehouseProperty.getReplicationType());
                    WarmupLevel warmupLevel = toStarOSWarmupLevel(warehouseProperty.getWarmupLevel());
                    StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
                    cluster.setWorkerGroupId(starOSAgent.createWorkerGroup("x0", warehouseProperty.getComputeReplica(),
                                             replicationType, warmupLevel));
                } catch (DdlException e) {
                    LOG.warn(e);
                    throw new DdlException("create warehouse " + wh.getName() + " failed, reason: " + e);
                }
            }

            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);

            onCreateWarehouse(wh);
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logEdit(OperationType.OP_CREATE_WAREHOUSE, wh);
            LOG.info("createWarehouse whName = {}, id = {}, comment = {}", warehouseName, id, comment);
        }
    }

    @Override
    public void replayCreateWarehouse(Warehouse warehouse) {
        String whName = warehouse.getName();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(!nameToWh.containsKey(whName), "Warehouse '%s' already exists", whName);
            nameToWh.put(whName, warehouse);
            idToWh.put(warehouse.getId(), warehouse);
            onCreateWarehouse(warehouse);
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
            if (warehouseName.equals(Config.lake_compaction_warehouse) ||
                    warehouseName.equals(Config.lake_background_warehouse)) {
                ErrorReport.reportDdlException(String.format("warehouse %s is used by compaction or background job, adjust " +
                        "lake_compaction_warehouse or lake_background_warehouse first", warehouseName),
                        ErrorCode.ERR_UNKNOWN_ERROR);
            }
            onDropWarehouse(warehouse);

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
                Warehouse warehouse = nameToWh.get(warehouseName);
                onDropWarehouse(warehouse);

                nameToWh.remove(warehouseName);
                idToWh.remove(warehouse.getId());
            }
        }
    }

    private void onCreateWarehouse(Warehouse wh) {
        if (wh == null) {
            return;
        }
        // register warehouse to slot manager
        try {
            BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
            if (slotManager == null || !(slotManager instanceof WarehouseSlotManager)) {
                return;
            }
            WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) slotManager;
            warehouseSlotManager.registerWarehouse(wh.getId());
        } catch (Exception e) {
            LOG.warn("register warehouse {} to slot manager failed", wh.getName(), e);
        }
    }

    private void onDropWarehouse(Warehouse wh) {
        if (wh == null) {
            return;
        }
        // unregister warehouse to slot manager
        try {
            BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
            if (slotManager == null || !(slotManager instanceof WarehouseSlotManager)) {
                return;
            }
            WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) slotManager;
            warehouseSlotManager.unregisterWarehouse(wh.getId());
        } catch (Exception e) {
            LOG.warn("unregister warehouse {} to slot manager failed", wh.getName(), e);
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
            onCreateWarehouse(warehouse);
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

            if (stmt.getProperties() == null || stmt.getProperties().isEmpty()) {
                return;
            }
            // make a copy of the properties, need to modify the map during the processing
            Map<String, String> properties = new HashMap<>(stmt.getProperties());
            LocalWarehouse warehouse = (LocalWarehouse) nameToWh.get(warehouseName);
            WarehouseProperty warehouseProperty = new WarehouseProperty(warehouse.getProperty());

            // handle update of 'compute_replica'
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
                properties.remove(WarehouseProperty.PROPERTY_COMPUTE_REPLICA);
            }
            // handle update of 'replication_type'
            if (properties.get(WarehouseProperty.PROPERTY_REPLICATION_TYPE) != null) {
                String replicationType = properties.get(WarehouseProperty.PROPERTY_REPLICATION_TYPE);
                warehouseProperty.setReplicationType(WarehouseProperty.replicationTypeFromString(replicationType));
                properties.remove(WarehouseProperty.PROPERTY_REPLICATION_TYPE);
            }
            // handle update of 'warmup_level'
            if (properties.get(WarehouseProperty.PROPERTY_WARMUP_LEVEL) != null) {
                String warmupLevel = properties.get(WarehouseProperty.PROPERTY_WARMUP_LEVEL);
                warehouseProperty.setWarmupLevel(WarehouseProperty.warmupLevelTypeFromString(warmupLevel));
                properties.remove(WarehouseProperty.PROPERTY_WARMUP_LEVEL);
            }
            // handle update of 'enable_query_queue'
            if (properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE) != null) {
                boolean enableQueryQueue =
                        properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE).equalsIgnoreCase("true");
                warehouseProperty.setEnableQueryQueue(enableQueryQueue);
                properties.remove(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE);
            }
            // handle update of 'enable_query_queue_load'
            if (properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_LOAD) != null) {
                boolean enableQueryQueueLoad =
                        properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_LOAD).equalsIgnoreCase("true");
                warehouseProperty.setEnableQueryQueueLoad(enableQueryQueueLoad);
                properties.remove(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_LOAD);
            }
            // handle update of 'enable_query_queue'
            if (properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC) != null) {
                boolean enableQueryQueueStatistic =
                        properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC).equalsIgnoreCase("true");
                warehouseProperty.setEnableQueryQueueStatistic(enableQueryQueueStatistic);
                properties.remove(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC);
            }
            // query_queue_max_queued_queries
            if (properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_MAX_QUEUED_QUERIES) != null) {
                int queryQueueMaxQueuedQueries =
                        Integer.parseInt(properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_MAX_QUEUED_QUERIES));
                if (queryQueueMaxQueuedQueries <= 0) {
                    throw new DdlException("warehouse query queue max queued queries can not be <= 0");
                }
                warehouseProperty.setQueryQueueMaxQueuedQueries(queryQueueMaxQueuedQueries);
                properties.remove(WarehouseProperty.PROPERTY_QUERY_QUEUE_MAX_QUEUED_QUERIES);
            }
            // query_queue_pending_timeout_second
            if (properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_PENDING_TIMEOUT_SECOND) != null) {
                int queryQueuePendingTimeoutSecond =
                        Integer.parseInt(properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_PENDING_TIMEOUT_SECOND));
                if (queryQueuePendingTimeoutSecond <= 0) {
                    throw new DdlException("warehouse query queue pending timeout second can not be <= 0");
                }
                warehouseProperty.setQueryQueuePendingTimeoutSecond(queryQueuePendingTimeoutSecond);
                properties.remove(WarehouseProperty.PROPERTY_QUERY_QUEUE_PENDING_TIMEOUT_SECOND);
            }
            // query_queue_concurrency_limit
            if (properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_CONCURRENCY_LIMIT) != null) {
                int queryQueueConcurrencyLimit =
                        Integer.parseInt(properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_CONCURRENCY_LIMIT));
                warehouseProperty.setQueryQueueConcurrencyLimit(queryQueueConcurrencyLimit);
                properties.remove(WarehouseProperty.PROPERTY_QUERY_QUEUE_CONCURRENCY_LIMIT);
            }

            if (!properties.isEmpty()) {
                throw new DdlException(
                        String.format("Unknown warehouse properties: {%s}", String.join(", ", properties.keySet())));
            }

            if (!warehouseProperty.equals(warehouse.getProperty())) { // some changes are made
                // TODO: operation below is not atomic
                StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
                for (Cluster cluster : warehouse.getClusters().values()) {
                    try {
                        ReplicationType replicationType = toStarOSReplicationType(warehouseProperty.getReplicationType());
                        WarmupLevel warmupLevel = toStarOSWarmupLevel(warehouseProperty.getWarmupLevel());
                        starOSAgent.updateWorkerGroup(cluster.getWorkerGroupId(), warehouseProperty.getComputeReplica(),
                                replicationType, warmupLevel);
                    } catch (DdlException e) {
                        LOG.warn(e);
                        throw new DdlException("alter warehouse " + warehouse.getName() + " failed, reason: " + e);
                    }
                }
                warehouse.setProperty(warehouseProperty);
                EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
                editLog.logEdit(OperationType.OP_ALTER_WAREHOUSE, warehouse);
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
            onCreateWarehouse(warehouse);
        });
    }

    @Override
    public Warehouse getCompactionWarehouse() {
        return getWarehouse(Config.lake_compaction_warehouse);
    }

    @Override
    public long getCompactionWarehouseID() {
        Warehouse warehouse = getWarehouse(Config.lake_compaction_warehouse);
        if (warehouse != null) {
            return warehouse.getId();
        }
        return DEFAULT_WAREHOUSE_ID;
    }

    public Warehouse getCompactionServiceWarehouse() {
        return getWarehouse(Config.lake_compaction_service_warehouse);
    }


    @Override
    public Warehouse getBackgroundWarehouse() {
        return getWarehouse(Config.lake_background_warehouse);
    }

    public static ReplicationType toStarOSReplicationType(
            WarehouseProperty.ReplicationType replicationType)
            throws DdlException {
        return switch (replicationType) {
            case NONE -> ReplicationType.NO_REPLICATION;
            case SYNC -> ReplicationType.SYNC;
            case ASYNC -> ReplicationType.ASYNC;
            default -> throw new DdlException("Unknown replication type " + replicationType);
        };
    }

    public static WarmupLevel toStarOSWarmupLevel(WarehouseProperty.WarmupLevelType warmupLevelType)
            throws DdlException {
        return switch (warmupLevelType) {
            case NONE -> WarmupLevel.WARMUP_NOTHING;
            case META -> WarmupLevel.WARMUP_META;
            case INDEX -> WarmupLevel.WARMUP_INDEX;
            case ALL -> WarmupLevel.WARMUP_ALL;
            default -> throw new DdlException("Unknown warmup level type " + warmupLevelType);
        };
    }
}

