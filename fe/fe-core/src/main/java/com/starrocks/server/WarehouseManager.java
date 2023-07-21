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

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.persist.DropWarehouseLog;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WarehouseManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    public static final String DEFAULT_WAREHOUSE_NAME = "default_warehouse";

    public static final long DEFAULT_WAREHOUSE_ID = 0L;
    public static final long DEFAULT_CLUSTER_ID = 0L;

    private Map<Long, Warehouse> idToWh = new HashMap<>();
    private Map<String, Warehouse> nameToWh = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public WarehouseManager() {
    }

    public void initDefaultWarehouse() {
        // gen a default warehouse
        // NOTE: default warehouse use DEFAULT_WORKER_GROUP_ID, which is 0,
        // so it is unnecessary to create a worker group for it.
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Warehouse wh = new LocalWarehouse(DEFAULT_WAREHOUSE_ID,
                    DEFAULT_WAREHOUSE_NAME, DEFAULT_CLUSTER_ID,
                    "An internal warehouse contains all compute nodes in this system");
            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);
        }
    }

    public AtomicInteger getNextComputeNodeIndexFromWarehouse(String warehouseName) {
        return getWarehouse(warehouseName).getAnyAvailableCluster().getNextComputeNodeHostId();
    }

    public Warehouse getDefaultWarehouse() {
        return getWarehouse(DEFAULT_WAREHOUSE_NAME);
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

    public Set<String> getAllWarehouseNames() {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.keySet();
        }
    }

    public boolean warehouseExists(String warehouseName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.containsKey(warehouseName);
        }
    }

    public ImmutableMap<Long, ComputeNode> getComputeNodesFromWarehouse(String warehouseName) {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        Warehouse warehouse = getWarehouse(warehouseName);
        warehouse.getAnyAvailableCluster().getComputeNodeIds().forEach(
                nodeId -> builder.put(nodeId, GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeId)));
        return builder.build();
    }



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

    // warehouse meta persistence api
    public long saveWarehouses(DataOutputStream out, long checksum) throws IOException {
        checksum ^= idToWh.size();
        write(out);
        return checksum;
    }

    public long loadWarehouses(DataInputStream dis, long checksum) throws IOException, DdlException {
        int warehouseCount = 0;
        try {
            String s = Text.readString(dis);
            WarehouseManager data = GsonUtils.GSON.fromJson(s, WarehouseManager.class);
            if (data != null && data.idToWh != null) {
                warehouseCount = data.idToWh.size();
            }
            checksum ^= warehouseCount;
            LOG.info("finished replaying WarehouseMgr from image");
        } catch (EOFException e) {
            LOG.info("no WarehouseMgr to replay.");
        }
        return checksum;
    }

    // new image persist func
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
        for (int i = 0; i  != nameToWhSize; ++i) {
            Warehouse warehouse = reader.readJson(Warehouse.class);
            this.nameToWh.put(warehouse.getName(), warehouse);
            this.idToWh.put(warehouse.getId(), warehouse);
        }
    }

    public List<List<String>> getWarehousesInfo() {
        return new WarehouseProcDir(this).fetchResult().getRows();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
