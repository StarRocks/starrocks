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
import com.staros.util.LockCloseable;

import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class keeps a historical backend or compute node set with the update time.
 */
public class HistoricalNodeSet {
    private ImmutableMap<Long, Backend> idToBackend;
    private ImmutableMap<Long, ComputeNode> idToComputeNode;
    private long lastUpdateTime;
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    HistoricalNodeSet() {
        idToBackend = ImmutableMap.of();
        idToComputeNode = ImmutableMap.of();
        lastUpdateTime = System.currentTimeMillis();
    }

    public boolean updateHistoricalBackends(Map<Long, Backend> idToBackend, long minUpdateInterval) {
        long currentTime = System.currentTimeMillis();

        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            if (currentTime - lastUpdateTime < minUpdateInterval) {
                return false;
            }

            this.idToBackend = ImmutableMap.copyOf(idToBackend);
            this.lastUpdateTime = currentTime;
            return true;
        }
    }

    public boolean updateHistoricalComputeNodes(Map<Long, ComputeNode> idToComputeNode, long minUpdateInterval) {
        long currentTime = System.currentTimeMillis();

        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            if (currentTime - lastUpdateTime < minUpdateInterval) {
                return false;
            }

            this.idToComputeNode = ImmutableMap.copyOf(idToComputeNode);
            this.lastUpdateTime = currentTime;
            return true;
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
        return lastUpdateTime;
    }

    /*
    public boolean setHistoricalNodeIds(List<Long> nodeIds, long minUpdateInterval) {
        long currentTime = System.currentTimeMillis();

        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            if (currentTime - lastUpdateTime < minUpdateInterval) {
                return false;
            }

            //this.nodeIds = nodeIds;
            this.lastUpdateTime = currentTime;
            return true;
        }
    }

    public List<Long> getNodeIds() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return this.nodeIds;
        }
    }
     */
}
