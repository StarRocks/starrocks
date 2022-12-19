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
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.CreateWarehouseStmt;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class WarehouseManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    @SerializedName(value = "idToWh")
    private final ConcurrentHashMap<Long, Warehouse> idToWh = new ConcurrentHashMap<>();
    @SerializedName(value = "fullNameToWh")
    private final ConcurrentHashMap<String, Warehouse> fullNameToWh = new ConcurrentHashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

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

    // these apis need lock protection
    public void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        createWarehouse(stmt.getFullWhName(), stmt.getProperties());
    }

    public void createWarehouse(String whName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
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
            Warehouse wh = new Warehouse(id, whName);
            idToWh.put(wh.getId(), wh);
            fullNameToWh.put(wh.getFullName(), wh);
            wh.setExist(true);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateWh(wh);
            LOG.info("createWarehouse whName = " + whName + ", id = " + id);
        } finally {
            writeUnLock();
        }
    }


    public void dropWarehouse(String name) {}
    public void alterWarehouse(String name) {}

    // warehouse meta persistence api
    public long saveWarehouses(DataOutputStream out, long checksum) throws IOException {
        write(out);
        return checksum;
    }

    public void replayCreateWarehouse(Warehouse wh) {

    }

    public void replayDropWarehouse() {}
    public void replayAlterWarehouse() {}

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static WarehouseManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WarehouseManager.class);
    }

}
