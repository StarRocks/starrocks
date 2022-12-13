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

package com.starrocks.warehouse;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.QueryableReentrantReadWriteLock;
import com.starrocks.common.util.Util;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Warehouse implements Writable {
    private static final Logger LOG = LogManager.getLogger(Warehouse.class);

    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "id")
    private long id;

    private QueryableReentrantReadWriteLock rwLock;

    private enum WarehouseState {
        INITIALIZING,
        RUNNING,
        SUSPENDED,
        SCALING
    }

    @SerializedName(value = "state")
    private WarehouseState state;

    // cluster id -> cluster obj
    @SerializedName(value = "clusters")
    private Map<Long, Cluster> clusters;

    // properties
    @SerializedName(value = "size")
    private String size = "${default_size}";
    @SerializedName(value = "minCluster")
    private int minCluster = 1;
    @SerializedName(value = "maxCluster")
    private int maxCluster = 5;
    // and others ...

    private volatile boolean exist = true;

    private long lastSlowLockLogTime = 0;

    private AtomicInteger numPendingSqls;

    public Warehouse(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public String getFullName() {
        return name;
    }

    private String getOwnerInfo(Thread owner) {
        if (owner == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("owner id: ").append(owner.getId()).append(", owner name: ")
                .append(owner.getName()).append(", owner stack: ").append(Util.dumpThread(owner, 50));
        return sb.toString();
    }

    private void logSlowLockEventIfNeeded(long startMs, String type, Thread formerOwner) {
        long endMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        if (endMs - startMs > Config.slow_lock_threshold_ms &&
                endMs > lastSlowLockLogTime + Config.slow_lock_log_every_ms) {
            lastSlowLockLogTime = endMs;
            LOG.warn("slow db lock. type: {}, db id: {}, db name: {}, wait time: {}ms, " +
                            "former {}, current stack trace: ", type, id, name, endMs - startMs,
                    getOwnerInfo(formerOwner), new Exception());
        }
    }

    public void writeLock() {
        long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        Thread formerOwner = rwLock.getOwner();
        this.rwLock.writeLock().lock();
        logSlowLockEventIfNeeded(startMs, "writeLock", formerOwner);
    }

    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    // the invoker should hold db's writeLock
    public void setExist(boolean exist) {
        this.exist = exist;
    }


    public void addCluster() {}
    public void removeCluster() {}
    public void suspend() {}
    public void resume() {}
    public void showClusters() {}

    // property setters and getters
    public void setSize(String size) {}
    public void getSize() {}
    // and others...

    public long numTotalRunningSqls() {
        return 1L;
    }

    public int getPendingSqls() {
        return 1;
    }

    public int setPendingSqls(int val) {
        return 1;
    }

    public int addAndGetPendingSqls(int delta) {
        return 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Warehouse read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Warehouse.class);
    }
}
