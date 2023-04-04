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
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.CreateWarehouseStmt;
import com.starrocks.warehouse.LocalWarehouse;
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

    public static final String DEFAULT_WAREHOUSE_NAME = "default_warehouse";

    private Map<Long, Warehouse> idToWh = new HashMap<>();
    @SerializedName(value = "fullNameToWh")
    private Map<String, Warehouse> fullNameToWh = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final WarehouseProcNode procNode = new WarehouseProcNode();
    public static final ImmutableList<String> WAREHOUSE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Warehouse")
            .add("State")
            .add("ClusterCount")
            .build();

    public WarehouseManager() {
    }

    public void initDefaultWarehouse() {
        createWarehouse(DEFAULT_WAREHOUSE_NAME, null);
    }

    public Warehouse getWarehouse(String warehouseName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return fullNameToWh.get(warehouseName);
        }
    }

    public boolean warehouseExists(String warehouseName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return fullNameToWh.containsKey(warehouseName);
        }
    }

    // these apis need lock protection
    public void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        createWarehouse(stmt.getFullWhName(), stmt.getProperties());
    }

    public void createWarehouse(String warehouseName, Map<String, String> properties) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(!fullNameToWh.containsKey(warehouseName),
                    "Warehouse '%s' already exists", warehouseName);

            long id = GlobalStateMgr.getCurrentState().getNextId();
            Warehouse wh = new LocalWarehouse(id, warehouseName);
            fullNameToWh.put(wh.getFullName(), wh);
            idToWh.put(wh.getId(), wh);
            wh.setExist(true);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateWarehouse(wh);
            LOG.info("createWarehouse whName = " + warehouseName + ", id = " + id);
        }
    }

    public void replayCreateWarehouse(Warehouse warehouse) {
        String whName = warehouse.getFullName();
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(!fullNameToWh.containsKey(whName), "Warehouse '%s' already exists", whName);
            fullNameToWh.put(whName, warehouse);
            idToWh.put(warehouse.getId(), warehouse);
            warehouse.setExist(true);
        }
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
                if (data.fullNameToWh != null) {
                    for (Warehouse warehouse : data.fullNameToWh.values()) {
                        replayCreateWarehouse(warehouse);
                    }
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

    public List<List<String>> getWarehousesInfo() {
        return procNode.fetchResult().getRows();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public class WarehouseProcNode implements ProcNodeInterface {

        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(WAREHOUSE_PROC_NODE_TITLE_NAMES);
            try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
                for (Map.Entry<String, Warehouse> entry : fullNameToWh.entrySet()) {
                    Warehouse warehouse = entry.getValue();
                    if (warehouse == null) {
                        continue;
                    }
                    warehouse.getProcNodeData(result);
                }
            }
            return result;
        }
    }
}
