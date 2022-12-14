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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WarehouseManager implements Writable, ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    private final GlobalStateMgr stateMgr;
    private EditLog editLog;

    @SerializedName(value = "idToWh")
    private final ConcurrentHashMap<Long, Warehouse> idToWh = new ConcurrentHashMap<>();
    @SerializedName(value = "fullNameToWh")
    private final ConcurrentHashMap<String, Warehouse> fullNameToWh = new ConcurrentHashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public WarehouseManager(GlobalStateMgr globalStateMgr) {
        this.stateMgr = globalStateMgr;
    }

    private boolean tryLock(boolean mustLock) {
        return stateMgr.tryLock(mustLock);
    }

    private void unlock() {
        stateMgr.unlock();
    }

    private long getNextId() {
        return stateMgr.getNextId();
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    @Override
    public List<String> listWhNames() {
        return Lists.newArrayList(fullNameToWh.keySet());
    }

    // these apis need lock protection
    @Override
    public void createWarehouse(String whName) throws DdlException, AlreadyExistsException {
        long id = 0L;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }

        try {
            if (fullNameToWh.containsKey(whName)) {
                throw new AlreadyExistsException("Warehouse Already Exists");
            } else {
                id = getNextId();
                Warehouse wh = new Warehouse(id, whName);
                unprotectCreateWarehouse(wh);
                editLog.logCreateWh(wh);
            }
        } finally {
            unlock();
        }
        LOG.info("createWarehouse whName = " + whName + ", id = " + id);
    }

    public void unprotectCreateWarehouse(Warehouse wh) {
        idToWh.put(wh.getId(), wh);
        fullNameToWh.put(wh.getFullName(), wh);
        wh.writeLock();
        wh.setExist(true);
        wh.writeUnlock();
    }


    public void dropWarehouse(String name) {}
    public void alterWarehouse(String name) {}

    // warehouse meta persistence api
    public long saveWarehouses(DataOutputStream out, long checksum) throws IOException {
        write(out);
        return checksum;
    }

    public void replayCreateWarehouse(Warehouse wh) {
        tryLock(true);
        try {
            unprotectCreateWarehouse(wh);
            LOG.info("finish replay create warehouse, name: {}, id: {}", wh.getFullName(), wh.getId());
        } finally {
            unlock();
        }
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
