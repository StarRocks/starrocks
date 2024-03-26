// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.server;

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.epack.persist.DropWarehouseLog;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Cluster;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;


public class WarehouseManagerEpack extends WarehouseManager {
    private static final Logger LOG = LogManager.getLogger(WarehouseManagerEpack.class);

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

    public void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }

        String warehouseName = stmt.getWarehouseName();

        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
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
            GlobalStateMgr.getCurrentState().getEditLog().logCreateWarehouse(wh);
            LOG.info("createWarehouse whName = " + warehouseName + ", id = " + id + ", " +
                    "comment = " + comment);
        }
    }

    public void replayCreateWarehouse(Warehouse warehouse) {
        String whName = warehouse.getName();
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
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
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {

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
            GlobalStateMgr.getCurrentState().getEditLog().
                    logDropWarehouse(new DropWarehouseLog(warehouseName));
        }
    }

    public void replayDropWarehouse(DropWarehouseLog log) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
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
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToWh.containsKey(warehouseName),
                    "Warehouse '%s' doesn't exist", warehouseName);

            LocalWarehouse warehouse = (LocalWarehouse) nameToWh.get(warehouseName);
            if (warehouse.getState() == LocalWarehouse.WarehouseState.SUSPENDED) {
                ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_SUSPENDED, warehouseName);
            }
            warehouse.suspendSelf();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterWarehouse(warehouse);
        }
    }

    public void replayAlterWarehouse(Warehouse warehouse) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            nameToWh.put(warehouse.getName(), warehouse);
            idToWh.put(warehouse.getId(), warehouse);
        }
    }

    public void resumeWarehouse(ResumeWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }

        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToWh.containsKey(warehouseName),
                    "Warehouse '%s' doesn't exist", warehouseName);
            LocalWarehouse warehouse = (LocalWarehouse) nameToWh.get(warehouseName);
            if (warehouse.getState() == LocalWarehouse.WarehouseState.AVAILABLE) {
                ErrorReport.reportDdlException("Can't resume an available warehouse");
            }
            warehouse.resumeSelf();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterWarehouse(warehouse);
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

