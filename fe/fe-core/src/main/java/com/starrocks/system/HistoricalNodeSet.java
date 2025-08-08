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

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import com.staros.util.LockCloseable;
import com.starrocks.common.io.Writable;

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class keeps a historical backend or compute node IDs with the update time.
 */
public class HistoricalNodeSet implements Writable {
    @SerializedName(value = "backendIds")
    private List<Long> backendIds;

    @SerializedName(value = "computeNodeIds")
    private List<Long> computeNodeIds;

    @SerializedName(value = "lastUpdateTime")
    private long lastUpdateTime;
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    HistoricalNodeSet() {
        backendIds = ImmutableList.of();
        computeNodeIds = ImmutableList.of();
        lastUpdateTime = System.currentTimeMillis();
    }

    public void updateHistoricalBackendIds(List<Long> backendIds, long currentTime) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            this.backendIds = ImmutableList.copyOf(backendIds);
            this.lastUpdateTime = currentTime;
        }
    }

    public void updateHistoricalComputeNodeIds(List<Long> computeNodeIds, long currentTime) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            this.computeNodeIds = ImmutableList.copyOf(computeNodeIds);
            this.lastUpdateTime = currentTime;
        }
    }

    public ImmutableList<Long> getHistoricalBackendIds() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return ImmutableList.copyOf(backendIds);
        }
    }

    public ImmutableList<Long> getHistoricalComputeNodeIds() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return ImmutableList.copyOf(computeNodeIds);
        }
    }

    public long getLastUpdateTime() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return lastUpdateTime;
        }
    }

}
