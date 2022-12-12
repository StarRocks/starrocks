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
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.DbsProcDir;
import com.starrocks.common.proc.ExternalDbsProcDir;
import com.starrocks.common.proc.ProcDirInterface;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.persist.AlterWhPropertyOplog;
import com.starrocks.persist.OpWarehouseLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.AlterWarehouseStmt;
import com.starrocks.sql.ast.CreateWarehouseStmt;
import com.starrocks.sql.ast.DropWarehouseStmt;
import com.starrocks.sql.ast.ResumeWarehouseStmt;
import com.starrocks.sql.ast.SuspendWarehouseStmt;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WarehouseManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    private Map<Long, Warehouse> idToWh = new HashMap<>();
    @SerializedName(value = "fullNameToWh")
    private Map<String, Warehouse> fullNameToWh = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final WarehouseProcNode procNode = new WarehouseProcNode();
    public static final ImmutableList<String> WAREHOUSE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Warehouse")
            .add("State")
            .add("Size")
            .add("min_cluster")
            .add("max_cluster")
            .add("cluster_count")
            .add("total_pending")
            .add("total_running")
            .build();

    private void readLock() {
        this.rwLock.readLock().lock();
    }
    private void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    private void writeLock() {
        this.rwLock.writeLock().lock();
    }

    private void writeUnLock() {
        this.rwLock.writeLock().unlock();
    }

    public WarehouseManager() {
    }

    public Warehouse getWarehouse(String warehouseName) {
        return fullNameToWh.get(warehouseName);
    }

    public boolean warehouseExists(String warehouseName) {
        readLock();
        try {
            return fullNameToWh.containsKey(warehouseName);
        } finally {
            readUnlock();
        }
    }

    // these apis need lock protection
    public void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        createWarehouse(stmt.getFullWhName(), stmt.getProperties());
    }

    public void createWarehouse(String whName, Map<String, String> properties) {
        readLock();
        try {
            Preconditions.checkState(!fullNameToWh.containsKey(whName), "Warehouse '%s' already exists", whName);
        } finally {
            readUnlock();
        }

        writeLock();
        try {
            Preconditions.checkState(!fullNameToWh.containsKey(whName), "Warehouse '%s' already exists", whName);
            long id = GlobalStateMgr.getCurrentState().getNextId();
            Warehouse wh = new Warehouse(id, whName, properties);
            fullNameToWh.put(wh.getFullName(), wh);
            idToWh.put(wh.getId(), wh);
            wh.setExist(true);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateWh(wh);
            LOG.info("createWarehouse whName = " + whName + ", id = " + id);
        } finally {
            writeUnLock();
        }
    }

    public void replayCreateWarehouse(Warehouse warehouse) {
        String whName = warehouse.getFullName();
        readLock();
        try {
            Preconditions.checkState(!fullNameToWh.containsKey(whName), "Warehouse '%s' already exists", whName);
        } finally {
            readUnlock();
        }

        writeLock();
        try {
            fullNameToWh.put(whName, warehouse);
            idToWh.put(warehouse.getId(), warehouse);
            warehouse.setExist(true);
        } finally {
            writeUnLock();
        }
    }


    public void suspendWarehouse(SuspendWarehouseStmt stmt) {
        String whName = stmt.getFullWhName();
        readLock();
        Warehouse warehouse = fullNameToWh.get(whName);
        readUnlock();
        warehouse.suspendSelf(false);
        OpWarehouseLog log = new OpWarehouseLog(whName);
        GlobalStateMgr.getCurrentState().getEditLog().logSuspendWh(log);
    }

    public void replaySuspendWarehouse(String whName) {
        readLock();
        Warehouse wh = fullNameToWh.get(whName);
        readUnlock();
        wh.suspendSelf(true);
    }

    public void resumeWarehouse(ResumeWarehouseStmt stmt) {
        String whName = stmt.getFullWhName();
        readLock();
        Warehouse warehouse = fullNameToWh.get(whName);
        readUnlock();
        warehouse.resumeSelf(false);
        OpWarehouseLog log = new OpWarehouseLog(whName);
        GlobalStateMgr.getCurrentState().getEditLog().logResumeWh(log);
    }

    public void replayResumeWarehouse(String whName) {
        readLock();
        Warehouse wh = fullNameToWh.get(whName);
        readUnlock();
        wh.resumeSelf(true);
    }

    public void dropWarehouse(DropWarehouseStmt stmt) {
        String whName = stmt.getFullWhName();
        writeLock();
        try {
            Warehouse warehouse = fullNameToWh.get(whName);
            fullNameToWh.remove(whName);
            idToWh.remove(warehouse.getId());
            warehouse.dropSelf();

            OpWarehouseLog log = new OpWarehouseLog(whName);
            GlobalStateMgr.getCurrentState().getEditLog().logDropWh(log);
        } finally {
            writeUnLock();
        }
    }

    public void replayDropWarehouse(String whName) {
        writeLock();
        try {
            Warehouse warehouse = fullNameToWh.get(whName);
            fullNameToWh.remove(whName);
            idToWh.remove(warehouse.getId());
        } finally {
            writeUnLock();
        }
    }

    public void alterWarehouse(AlterWarehouseStmt stmt) throws DdlException {
        if (stmt.getOpType() == AlterWarehouseStmt.OpType.ADD_CLUSTER) {
            // add cluster
            addCluster(stmt.getFullWhName());
        } else if (stmt.getOpType() == AlterWarehouseStmt.OpType.REMOVE_CLUSTER) {
            // remove cluster
            removeCluster(stmt.getFullWhName());
        } else {
            // modify some property
            Map<String, String> properties = stmt.getProperties();
            modifyProperty(stmt.getFullWhName(), properties);
        }
    }

    private void addCluster(String warehouseName) throws DdlException {
        if (fullNameToWh.containsKey(warehouseName)) {
            Warehouse wh = fullNameToWh.get(warehouseName);
            wh.addCluster();
        }
    }

    private void removeCluster(String warehouseName) throws DdlException {
        if (fullNameToWh.containsKey(warehouseName)) {
            Warehouse wh = fullNameToWh.get(warehouseName);
            wh.removeCluster();
        }
    }

    private void modifyProperty(String warehouseName, Map<String, String> properties) throws DdlException {
        boolean isChanged = false;
        if (fullNameToWh.containsKey(warehouseName)) {
            Warehouse wh = fullNameToWh.get(warehouseName);
            if (properties != null) {
                if (properties.get("size") != null) {
                    wh.setSize(properties.get("size"));
                    // 调用modify worker group进行资源调整
                    wh.modifyCluterSize();
                    isChanged = true;
                }

                if (properties.get("min_cluster") != null) {
                    wh.setMinCluster(Integer.valueOf(properties.get("min_cluster")));
                    isChanged = true;
                }

                if (properties.get("max_cluster") != null) {
                    wh.setMaxCluster(Integer.valueOf(properties.get("max_cluster")));
                    isChanged = true;
                }
            }
        }

        if (isChanged) {
            AlterWhPropertyOplog log = new AlterWhPropertyOplog(warehouseName, properties);
            GlobalStateMgr.getCurrentState().getEditLog().logModifyWhProperty(log);
        }

        LOG.info("alter warehouse properties {}, warehouse name: {}", properties, warehouseName);
    }

    public void replayModifyProperty(String warehouseName, Map<String, String> properties) {
        Warehouse wh = fullNameToWh.get(warehouseName);
        if (properties != null) {
            if (properties.get("size") != null) {
                wh.setSize(properties.get("size"));
            }

            if (properties.get("min_cluster") != null) {
                wh.setMinCluster(Integer.valueOf(properties.get("min_cluster")));
            }

            if (properties.get("max_cluster") != null) {
                wh.setMaxCluster(Integer.valueOf(properties.get("max_cluster")));
            }
        }
        LOG.info("replay alter warehouse properties {}, warehouse name: {}", properties, warehouseName);
    }

    // warehouse meta persistence api
    public long saveWarehouses(DataOutputStream out, long checksum) throws IOException {
        checksum ^= fullNameToWh.size();
        write(out);
        return checksum;
    }

    public long loadWarehouses(DataInputStream dis, long checksum) throws IOException, DdlException {
        int warehouseCount = 0;
        try {
            String s = Text.readString(dis);
            WarehouseManager data = GsonUtils.GSON.fromJson(s, WarehouseManager.class);
            if (data != null) {
                if (data.fullNameToWh != null && data.idToWh != null) {
                    this.fullNameToWh = data.fullNameToWh;
                    this.idToWh = data.idToWh;
                }
                warehouseCount = data.fullNameToWh.size();
            }
            checksum ^= warehouseCount;
            LOG.info("finished replaying WarehouseMgr from image");
        } catch (EOFException e) {
            LOG.info("no WarehouseMgr to replay.");
        }
        return checksum;
    }

    public void replayDropWarehouse() {}
    public void replayAlterWarehouse() {}

    public List<List<String>> getWarehousesInfo() {
        return procNode.fetchResult().getRows();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public class WarehouseProcNode implements ProcDirInterface {

        @Override
        public boolean register(String name, ProcNodeInterface node) {
            return false;
        }

        @Override
        public ProcNodeInterface lookup(String catalogName) throws AnalysisException {
            if (CatalogMgr.isInternalCatalog(catalogName)) {
                return new DbsProcDir(GlobalStateMgr.getCurrentState());
            }
            return new ExternalDbsProcDir(catalogName);
        }

        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(WAREHOUSE_PROC_NODE_TITLE_NAMES);
            readLock();
            try {
                for (Map.Entry<String, Warehouse> entry : fullNameToWh.entrySet()) {
                    Warehouse warehouse = entry.getValue();
                    if (warehouse == null) {
                        continue;
                    }
                    warehouse.getProcNodeData(result);
                }
            } finally {
                readUnlock();
            }
            return result;
        }
    }

}
