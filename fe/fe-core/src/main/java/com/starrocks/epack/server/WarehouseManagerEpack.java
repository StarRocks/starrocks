// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.server;

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.epack.persist.DropWarehouseLog;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.LocalWarehouse;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WarehouseManagerEpack extends WarehouseManager {
    private static final Logger LOG = LogManager.getLogger(WarehouseManagerEpack.class);

    public void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            throw new DdlException("unsupported statement in shared_nothing mode");
        }

        String warehouseName = stmt.getWarehouseName();

        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (nameToWh.containsKey(warehouseName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("Warehouse '%s' already exists", warehouseName);
                    return;
                }
                throw new DdlException("Warehouse " + warehouseName + " already exists");
            }

            long id = GlobalStateMgr.getCurrentState().getNextId();
            long clusterId = GlobalStateMgr.getCurrentState().getNextId();
            String comment = stmt.getComment();
            Warehouse wh = new LocalWarehouse(id, warehouseName, clusterId, comment);
            try {
                wh.initCluster();
            } catch (DdlException e) {
                LOG.warn(e);
                throw new DdlException("create warehouse " + wh.getName() + " failed, reason: " + e);
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
            throw new RuntimeException(new DdlException("unsupported statement in shared_nothing mode"));
        }

        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToWh.containsKey(warehouseName),
                    "Warehouse '%s' doesn't exist", warehouseName);
            Warehouse warehouse = nameToWh.get(warehouseName);
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
            throw new RuntimeException(new DdlException("unsupported statement in shared_nothing mode"));
        }

        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToWh.containsKey(warehouseName),
                    "Warehouse '%s' doesn't exist", warehouseName);

            Warehouse warehouse = nameToWh.get(warehouseName);
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
            throw new RuntimeException(new DdlException("unsupported statement in shared_nothing mode"));
        }

        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(nameToWh.containsKey(warehouseName),
                    "Warehouse '%s' doesn't exist", warehouseName);
            Warehouse warehouse = nameToWh.get(warehouseName);
            warehouse.resumeSelf();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterWarehouse(warehouse);
        }
    }

}

