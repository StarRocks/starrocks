// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.warehouse;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.staros.util.LockCloseable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.epack.warehouse.cngroup.CNGroupResource;
import com.starrocks.epack.warehouse.cngroup.CNGroupResourceProvider;
import com.starrocks.epack.warehouse.cngroup.CNGroupUtils;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.DropWarehouseLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WarehouseInternalOpLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.ComputeResourceProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class WarehouseManagerEPack extends WarehouseManager {
    private static final Logger LOG = LogManager.getLogger(WarehouseManagerEPack.class);

    @SerializedName(value = "tableLastTransactionWarehouseInfo")
    private ConcurrentHashMap<Long /* TableId */, TransactionWarehouseInfo> tableLastTransactionWarehouseInfo
            = new ConcurrentHashMap<>();

    public WarehouseManagerEPack(ComputeResourceProvider computeResourceProvider) {
        super(computeResourceProvider, ImmutableList.of(new WarehouseEPEventListener()));
    }

    public WarehouseManagerEPack() {
        this(new CNGroupResourceProvider());
    }

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
                Warehouse wh =
                        LocalWarehouse.createDefaultLocalWarehouse("An internal warehouse init after FE is ready");
                nameToWh.put(wh.getName(), wh);
                idToWh.put(wh.getId(), wh);
                warehouseEventListeners.stream()
                        .forEach(listener -> listener.onCreateWarehouse(wh));
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
            throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_SUSPENDED,
                    String.format("name: %s", warehouse.getName()));
        }
    }

    public String getWarehouseComputeResourceName(ComputeResource computeResource) {
        if (RunMode.isSharedNothingMode() || computeResource == null) {
            return "";
        }
        try {
            final LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(computeResource.getWarehouseId());
            return String.format("%s:%s", warehouse.getName(), getComputeResourceName(computeResource));
        } catch (Exception e) {
            LOG.warn("Failed to get warehouse name for computeResource: {}", computeResource, e);
            return "";
        }
    }

    public String getComputeResourceName(ComputeResource computeResource) {
        if (!RunMode.isSharedDataMode() || computeResource == null) {
            return "";
        }
        try {
            CNGroupResource cnGroupResource = CNGroupUtils.getAcquiredCNGroupResource(computeResource);
            if (cnGroupResource == null) {
                LOG.warn("Failed to get compute resource name for computeResource: {}, cnGroupResource is null",
                        computeResource);
                return "";
            }
            LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(cnGroupResource.getWarehouseId());
            checkWarehouseState(warehouse);
            Cluster cluster = warehouse.getClusterByWorkGroupId(cnGroupResource.getWorkerGroupId());
            if (cluster == null) {
                LOG.warn("Failed to get compute resource name for computeResource: {}, cluster is null",
                        computeResource);
                return "";
            }
            return cluster.getName();
        } catch (Exception e) {
            LOG.warn("Failed to get compute resource name for computeResource: {}", computeResource, e);
            return "";
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
        return warehouse.getClusters().values().stream()
                .filter(Cluster::isEnabled)
                .flatMap(cluster -> cluster.getComputeNodeIds().stream())
                .toList();
    }

    @Override
    public Long getComputeNodeId(ComputeResource computeResource, long tabletId) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(computeResource.getWarehouseId());
        checkWarehouseState(warehouse);
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getPrimaryComputeNodeIdByShard(tabletId, computeResource.getWorkerGroupId());
        } catch (StarRocksException e) {
            LOG.warn("get primary compute node id for tablet {} fail {}.", tabletId, e.getMessage());
            return null;
        }
    }

    @Override
    public List<Long> getAllComputeNodeIdsAssignToTablet(ComputeResource computeResource, long tabletId) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(computeResource.getWarehouseId());
        checkWarehouseState(warehouse);
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getAllNodeIdsByShard(tabletId, computeResource.getWorkerGroupId());
        } catch (StarRocksException e) {
            LOG.warn("get all compute node ids for tablet {} fail {}.", tabletId, e.getMessage());
            return null;
        }
    }

    @Override
    public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
        Long computeNodeId = getComputeNodeId(computeResource, tabletId);
        if (computeNodeId == null) {
            throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE,
                    String.format("id: %s", computeResource));
        }
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(computeNodeId);
    }

    @Override
    public AtomicInteger getNextComputeNodeIndexFromWarehouse(ComputeResource computeResource) {
        LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(computeResource.getWarehouseId());
        checkWarehouseState(warehouse);
        Cluster cluster = warehouse.getClusterByWorkGroupId(computeResource.getWorkerGroupId());
        if (cluster == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %s", computeResource));
        }
        return cluster.getNextComputeNodeHostId();
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
                ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_EXISTS,
                        String.format("name: %s", warehouseName));
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

                // handle 'warmup_timeout_secs': per-warehouse override of the global shard warmup timeout,
                // in seconds. 0 (the default when absent) means no override and falls back to
                // Config.lake_compute_replica_warmup_timeout_secs.
                if (properties.containsKey(WarehouseProperty.PROPERTY_WARMUP_TIMEOUT_SECS)) {
                    int warmupTimeoutSecs =
                            Integer.parseInt(properties.get(WarehouseProperty.PROPERTY_WARMUP_TIMEOUT_SECS));
                    if (warmupTimeoutSecs < 0) {
                        throw new DdlException("warehouse warmup timeout secs can not be < 0");
                    }
                    warehouseProperty.setWarmupTimeoutSecs(warmupTimeoutSecs);
                    properties.remove(WarehouseProperty.PROPERTY_WARMUP_TIMEOUT_SECS);
                }

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
                    boolean enableQueryQueueStatistic =
                            properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC)
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
                            Integer.parseInt(
                                    properties.get(WarehouseProperty.PROPERTY_QUERY_QUEUE_PENDING_TIMEOUT_SECOND));
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

            long warehouseId = GlobalStateMgr.getCurrentState().getNextId();
            String comment = stmt.getComment();
            LocalWarehouse wh = new LocalWarehouse(warehouseId, warehouseName, warehouseProperty, comment);
            wh.initializeBuiltinCNGroup();

            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);

            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logJsonObject(OperationType.OP_CREATE_WAREHOUSE, wh);
            LOG.info("createWarehouse whName = {}, id = {}, comment = {}", warehouseName, warehouseId, comment);
            warehouseEventListeners.stream()
                    .forEach(listener -> listener.onCreateWarehouse(wh));
        }
    }

    @Override
    public void replayCreateWarehouse(Warehouse warehouse) {
        String whName = warehouse.getName();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(!nameToWh.containsKey(whName), "Warehouse '%s' already exists", whName);
            nameToWh.put(whName, warehouse);
            idToWh.put(warehouse.getId(), warehouse);
            try {
                warehouseEventListeners.stream()
                        .forEach(listener -> listener.onCreateWarehouse(warehouse));
                if (warehouse instanceof LocalWarehouse) {
                    LocalWarehouse localWarehouse = (LocalWarehouse) warehouse;
                    localWarehouse.getClusters().values()
                            .stream()
                            .forEach(cluster -> {
                                warehouseEventListeners.stream().forEach(listener -> {
                                    listener.onCreateCNGroup(localWarehouse, cluster.getWorkerGroupId());
                                });
                            });
                }
            } catch (Exception e) {
                LOG.warn("Failed to notify warehouse listeners for warehouse: {}", whName, e);
            }
        }
    }

    @Override
    public void dropWarehouse(DropWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }

        String warehouseName = stmt.getWarehouseName();
        LocalWarehouse warehouse;
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            warehouse = (LocalWarehouse) nameToWh.get(warehouseName);
            if (warehouse == null) {
                if (stmt.isSetIfExists()) {
                    return;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                        String.format("name: %s", warehouseName));
            }
            if (warehouseName.equals(Config.lake_compaction_warehouse) ||
                    warehouseName.equals(Config.lake_background_warehouse)) {
                ErrorReport.reportDdlException(
                        String.format("warehouse %s is used by compaction or background job, adjust " +
                                "lake_compaction_warehouse or lake_background_warehouse first", warehouseName),
                        ErrorCode.ERR_UNKNOWN_ERROR);
            }
            warehouseEventListeners.stream()
                    .forEach(listener -> listener.onDropWarehouse(warehouse));

            warehouse.delete();

            nameToWh.remove(warehouseName);
            idToWh.remove(warehouse.getId());

            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logJsonObject(OperationType.OP_DROP_WAREHOUSE, new DropWarehouseLog(warehouseName));
        }

        if (!GlobalStateMgr.isCheckpointThread()) {
            BackendResourceStat.getInstance().removeWarehouse(warehouse.getId());
        }
    }

    @Override
    public void replayDropWarehouse(DropWarehouseLog log) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            String warehouseName = log.getWarehouseName();
            if (nameToWh.containsKey(warehouseName)) {
                LocalWarehouse warehouse = (LocalWarehouse) nameToWh.get(warehouseName);
                warehouse.replayDelete();
                warehouseEventListeners.stream()
                        .forEach(listener -> listener.onDropWarehouse(warehouse));
                nameToWh.remove(warehouseName);
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
                ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_SUSPENDED,
                        String.format("name: %s", warehouseName));
            }

            warehouse.suspendSelf();
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logJsonObject(OperationType.OP_ALTER_WAREHOUSE, warehouse);
        }
    }

    @Override
    public void replayAlterWarehouse(Warehouse warehouse) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            LocalWarehouse originWh = (LocalWarehouse) getWarehouse(warehouse.getId());
            originWh.replayAlterWarehouse((LocalWarehouse) warehouse);
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
            editLog.logJsonObject(OperationType.OP_ALTER_WAREHOUSE, warehouse);
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

            // analyze warehouse session variables
            final String sessionVariablePrefix = "session.";
            List<SetListItem> setListItems = Lists.newArrayList();
            Map<String, String> sessionVariables = Maps.newHashMap();
            properties.forEach((k, v) -> {
                if (!k.startsWith(sessionVariablePrefix)) {
                    return;
                }
                String varKey = k.substring(sessionVariablePrefix.length());
                SystemVariable variable = new SystemVariable(varKey, new StringLiteral(v));
                setListItems.add(variable);
                sessionVariables.put(varKey, v);
            });
            properties.entrySet().removeIf(entry -> entry.getKey().startsWith(sessionVariablePrefix));
            for (SetListItem item : setListItems) {
                SystemVariable variable = (SystemVariable) item;
                GlobalStateMgr.getCurrentState().getVariableMgr().checkSystemVariableExist(variable);
            }
            SetStmtAnalyzer.analyze(new SetStmt(setListItems), null);
            warehouseProperty.getSessionVariables().putAll(sessionVariables);

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
            // handle update of 'warmup_timeout_secs'
            if (properties.get(WarehouseProperty.PROPERTY_WARMUP_TIMEOUT_SECS) != null) {
                int warmupTimeoutSecs =
                        Integer.parseInt(properties.get(WarehouseProperty.PROPERTY_WARMUP_TIMEOUT_SECS));
                if (warmupTimeoutSecs < 0) {
                    throw new DdlException("warehouse warmup timeout secs can not be < 0");
                }
                warehouseProperty.setWarmupTimeoutSecs(warmupTimeoutSecs);
                properties.remove(WarehouseProperty.PROPERTY_WARMUP_TIMEOUT_SECS);
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
                        properties.get(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC)
                                .equalsIgnoreCase("true");
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
                        ReplicationType replicationType =
                                toStarOSReplicationType(warehouseProperty.getReplicationType());
                        WarmupLevel warmupLevel = toStarOSWarmupLevel(warehouseProperty.getWarmupLevel());
                        starOSAgent.updateWorkerGroup(cluster.getWorkerGroupId(), warehouseProperty.getComputeReplica(),
                                replicationType, warmupLevel,
                                OptionalInt.of(warehouseProperty.getWarmupTimeoutSecs()));
                    } catch (DdlException e) {
                        LOG.warn(e);
                        throw new DdlException("alter warehouse " + warehouse.getName() + " failed, reason: " + e);
                    }
                }
                warehouse.setProperty(warehouseProperty);
                EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
                editLog.logJsonObject(OperationType.OP_ALTER_WAREHOUSE, warehouse);
            }
        }
    }

    @Override
    public void createCnGroup(CreateCnGroupStmt stmt) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse wh = getWarehouse(stmt.getWarehouseName());
            wh.createCNGroup(stmt);
        }
    }

    @Override
    public void dropCnGroup(DropCnGroupStmt stmt) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse wh = getWarehouse(stmt.getWarehouseName());
            wh.dropCNGroup(stmt);
        }
    }

    @Override
    public void enableCnGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse wh = getWarehouse(stmt.getWarehouseName());
            wh.enableCNGroup(stmt);
        }
    }

    @Override
    public void disableCnGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse wh = getWarehouse(stmt.getWarehouseName());
            wh.disableCNGroup(stmt);
        }
    }

    @Override
    public void alterCnGroup(AlterCnGroupStmt stmt) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse wh = getWarehouse(stmt.getWarehouseName());
            wh.alterCNGroup(stmt);
        }
    }

    @Override
    public void replayInternalOpLog(WarehouseInternalOpLog log) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Warehouse wh = getWarehouse(log.getWarehouseName());
            wh.replayInternalOpLog(log.getPayload());
        }
    }

    @Override
    public void recordWarehouseInfoForTable(long tableId, ComputeResource computeResource) {
        TransactionWarehouseInfo info = tableLastTransactionWarehouseInfo.compute(tableId, (k, v) -> {
            if (v == null) {
                v = new TransactionWarehouseInfo();
            }
            v.setInfo(computeResource);
            return v;
        });
        LOG.debug("record warehouse {} cngroup {} for table {}",
                computeResource.getWarehouseId(), computeResource.getWorkerGroupId(), tableId);
    }

    @Override
    public void removeTableWarehouseInfo(long tableId) {
        tableLastTransactionWarehouseInfo.remove(tableId);
        LOG.debug("remove warehouse info for table {}", tableId);
    }

    public ComputeResource getLastTransactionWarehouseInfoForTable(long tableId) {
        TransactionWarehouseInfo last = tableLastTransactionWarehouseInfo.get(tableId);
        if (last == null) {
            return CNGroupResource.of(0, 0);
        } else {
            return last.getComputeResource();
        }
    }

    @Override
    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.WAREHOUSE_MGR,
                nameToWh.size() + 1 /* Int */ + 1 /* this */);
        writer.writeInt(nameToWh.size());
        for (Warehouse warehouse : nameToWh.values()) {
            writer.writeJson(warehouse);
        }
        writer.writeJson(this);
        writer.close();
    }

    @Override
    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        reader.readCollection(Warehouse.class, warehouse -> {
            this.nameToWh.put(warehouse.getName(), warehouse);
            this.idToWh.put(warehouse.getId(), warehouse);

            try {
                warehouseEventListeners.stream()
                        .forEach(listener -> listener.onCreateWarehouse(warehouse));
                if (warehouse instanceof LocalWarehouse) {
                    LocalWarehouse localWarehouse = (LocalWarehouse) warehouse;
                    localWarehouse.getClusters().values()
                            .stream()
                            .forEach(cluster -> {
                                warehouseEventListeners.stream().forEach(listener -> {
                                    listener.onCreateCNGroup(localWarehouse, cluster.getWorkerGroupId());
                                });
                            });
                }
            } catch (Exception e) {
                LOG.warn("Failed to notify warehouse listener on create warehouse: {}", warehouse.getName(), e);
            }
        });
        WarehouseManagerEPack warehouseManagerEPack = reader.readJson(WarehouseManagerEPack.class);
        tableLastTransactionWarehouseInfo = warehouseManagerEPack.tableLastTransactionWarehouseInfo;
    }

    private Warehouse getWarehouseForTable(long tableId, TransactionWarehouseInfo info, boolean isCompaction) {
        try {
            LocalWarehouse warehouse = (LocalWarehouse) getWarehouse(info.getWarehouseId());
            checkWarehouseState(warehouse);
            return warehouse;
        } catch (ErrorReportException e) {
            if (e.getErrorCode() == ErrorCode.ERR_UNKNOWN_WAREHOUSE) {
                removeTableWarehouseInfo(tableId);
                return getWarehouse(isCompaction ? Config.lake_compaction_warehouse : Config.lake_background_warehouse);
            } else if (e.getErrorCode() == ErrorCode.ERR_WAREHOUSE_SUSPENDED) {
                return getWarehouse(isCompaction ? Config.lake_compaction_warehouse : Config.lake_background_warehouse);
            } else {
                throw e;
            }
        }
    }

    @Override
    public ComputeResource getCompactionComputeResource(long tableId) {
        TransactionWarehouseInfo info = tableLastTransactionWarehouseInfo.get(tableId);
        if (info == null) { // warehouse might be dropped or upgraded from older version
            return acquireComputeResource(CRAcquireContext.of(getWarehouse(Config.lake_compaction_warehouse).getId()));
        }
        Warehouse warehouse = getWarehouseForTable(tableId, info, true /* isCompaction */);
        return acquireComputeResource(CRAcquireContext.of(warehouse.getId(), info.getComputeResource()));
    }

    @Override
    public ComputeResource getVectorIndexBuildComputeResource(long tableId) {
        TransactionWarehouseInfo info = tableLastTransactionWarehouseInfo.get(tableId);
        if (info == null) {
            return acquireComputeResource(
                    CRAcquireContext.of(getWarehouse(Config.lake_vector_index_build_warehouse).getId()));
        }
        Warehouse warehouse = getWarehouseForTable(tableId, info, false /* isCompaction */);
        return acquireComputeResource(CRAcquireContext.of(warehouse.getId(), info.getComputeResource()));
    }

    @Override
    public Warehouse getBackgroundWarehouse() {
        return getWarehouse(Config.lake_background_warehouse);
    }

    @Override
    public Warehouse getBackgroundWarehouse(long tableId) {
        TransactionWarehouseInfo info = tableLastTransactionWarehouseInfo.get(tableId);
        if (info == null) { // warehouse might be dropped or upgraded from older version
            return getWarehouse(Config.lake_background_warehouse);
        }
        return getWarehouseForTable(tableId, info, false /* isCompaction */);
    }

    @Override
    public ComputeResource getBackgroundComputeResource(long tableId) {
        TransactionWarehouseInfo info = tableLastTransactionWarehouseInfo.get(tableId);
        if (info == null) { // warehouse might be dropped or upgraded from older version
            return acquireComputeResource(CRAcquireContext.of(getWarehouse(Config.lake_background_warehouse).getId()));
        }
        Warehouse warehouse = getWarehouseForTable(tableId, info, false /* isCompaction */);
        return acquireComputeResource(CRAcquireContext.of(warehouse.getId(), info.getComputeResource()));
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

