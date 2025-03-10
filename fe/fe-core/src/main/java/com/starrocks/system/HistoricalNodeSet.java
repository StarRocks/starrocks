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

package com.starrocks.system;

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import com.staros.util.LockCloseable;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class keeps a historical backend or compute node set with the update time.
 */
public class HistoricalNodeSet implements Writable {
    @SerializedName(value = "idToBackend")
    private ImmutableMap<Long, Backend> idToBackend;

    @SerializedName(value = "idToComputeNode")
    private ImmutableMap<Long, ComputeNode> idToComputeNode;

    @SerializedName(value = "lastUpdateTime")
    private long lastUpdateTime;
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    HistoricalNodeSet() {
        idToBackend = ImmutableMap.of();
        idToComputeNode = ImmutableMap.of();
        lastUpdateTime = System.currentTimeMillis();
    }

    public void updateHistoricalBackends(Map<Long, Backend> idToBackend, long currentTime) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            this.idToBackend = ImmutableMap.copyOf(idToBackend);
            this.lastUpdateTime = currentTime;
        }
    }

    public void updateHistoricalComputeNodes(Map<Long, ComputeNode> idToComputeNode, long currentTime) {
        // long currentTime = System.currentTimeMillis();
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            this.idToComputeNode = ImmutableMap.copyOf(idToComputeNode);
            this.lastUpdateTime = currentTime;
        }
    }

    public ImmutableMap<Long, Backend> getHistoricalBackends() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return idToBackend;
        }
    }

    public ImmutableMap<Long, ComputeNode> getHistoricalComputeNodes() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return idToComputeNode;
        }
    }

    public long getLastUpdateTime() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return lastUpdateTime;
        }
    }

    public static HistoricalNodeSet read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), HistoricalNodeSet.class);
    }
}
