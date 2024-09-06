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
import com.starrocks.common.CloseableLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cache the resource information of all backends and compute nodes, and provide the average value of the resource information.
 * The resource information includes the number of CPU cores and the memory limit for now.
 *
 * <p> MUST update the resource information of the backend when the backend is added, removed, or updated.
 *
 * <p> All the methods are thread-safe.
 */
public class BackendResourceStat {

    private static final Logger LOG = LogManager.getLogger(BackendResourceStat.class);

    private static final int DEFAULT_CORES_OF_BE = 1;
    private static final long DEFAULT_MEM_LIMIT_BYTES = 0L;
    private static final int ABSENT_CACHE_VALUE = -1;

    private final ReentrantLock lock = new ReentrantLock();

    private final ConcurrentHashMap<Long, Integer> numHardwareCoresPerBe = new ConcurrentHashMap<>();
    private final AtomicInteger cachedAvgNumHardwareCores = new AtomicInteger(ABSENT_CACHE_VALUE);
    private final AtomicInteger cachedMinNumCores = new AtomicInteger(ABSENT_CACHE_VALUE);

    private final ConcurrentHashMap<Long, Long> memLimitBytesPerBe = new ConcurrentHashMap<>();
    private final AtomicLong cachedAvgMemLimitBytes = new AtomicLong(ABSENT_CACHE_VALUE);

    private static class SingletonHolder {
        private static final BackendResourceStat INSTANCE = new BackendResourceStat();
    }

    public static BackendResourceStat getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private BackendResourceStat() {
    }

    public void setNumHardwareCoresOfBe(long be, int numCores) {
        Integer previous = numHardwareCoresPerBe.put(be, numCores);
        LOG.info("set numHardwareCores [{}] of be [{}], current cpuCores stats: {}", numCores, be, numHardwareCoresPerBe);
        if (previous == null || previous != numCores) {
            cachedAvgNumHardwareCores.set(ABSENT_CACHE_VALUE);
            cachedMinNumCores.set(ABSENT_CACHE_VALUE);
        }
    }

    public void setMemLimitBytesOfBe(long be, long memLimitBytes) {
        Long previous = memLimitBytesPerBe.put(be, memLimitBytes);
        if (previous == null || previous != memLimitBytes) {
            cachedAvgMemLimitBytes.set(-1L);
        }
    }

    public int getNumBes() {
        return Math.max(numHardwareCoresPerBe.size(), memLimitBytesPerBe.size());
    }

    public void removeBe(long be) {
        if (numHardwareCoresPerBe.remove(be) != null) {
            cachedAvgNumHardwareCores.set(ABSENT_CACHE_VALUE);
            cachedMinNumCores.set(ABSENT_CACHE_VALUE);
        }
        LOG.info("remove numHardwareCores of be [{}], current cpuCores stats: {}", be, numHardwareCoresPerBe);

        if (memLimitBytesPerBe.remove(be) != null) {
            cachedAvgMemLimitBytes.set(ABSENT_CACHE_VALUE);
        }
    }

    public void reset() {
        numHardwareCoresPerBe.clear();
        cachedAvgNumHardwareCores.set(ABSENT_CACHE_VALUE);
        cachedMinNumCores.set(ABSENT_CACHE_VALUE);

        memLimitBytesPerBe.clear();
        cachedAvgMemLimitBytes.set(ABSENT_CACHE_VALUE);
    }

    public int getAvgNumHardwareCoresOfBe() {
        int snapshotAvg = cachedAvgNumHardwareCores.get();
        if (snapshotAvg > 0) {
            return snapshotAvg;
        }

        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            if (numHardwareCoresPerBe.isEmpty()) {
                return DEFAULT_CORES_OF_BE;
            }

            int sum = numHardwareCoresPerBe.values().stream().reduce(Integer::sum).orElse(DEFAULT_CORES_OF_BE);
            int avg = sum / numHardwareCoresPerBe.size();
            cachedAvgNumHardwareCores.set(avg);
            LOG.info("update avgNumHardwareCoresOfBe to {}, current cpuCores stats: {}", avg, numHardwareCoresPerBe);

            return avg;
        }
    }

    public int getMinNumHardwareCoresOfBe() {
        int snapshot = cachedMinNumCores.get();
        if (snapshot > 0) {
            return snapshot;
        }

        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            if (numHardwareCoresPerBe.isEmpty()) {
                return DEFAULT_CORES_OF_BE;
            }

            int min = numHardwareCoresPerBe.values().stream().reduce(Integer::min).orElse(DEFAULT_CORES_OF_BE);
            min = Math.max(min, 1);
            cachedMinNumCores.set(min);
            LOG.info("update minNumHardwareCoresOfBe to {}, current cpuCores stats: {}", min, numHardwareCoresPerBe);

            return min;
        }
    }

    public long getAvgMemLimitBytes() {
        long snapshotAvg = cachedAvgMemLimitBytes.get();
        if (snapshotAvg > 0) {
            return snapshotAvg;
        }

        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            if (memLimitBytesPerBe.isEmpty()) {
                return DEFAULT_MEM_LIMIT_BYTES;
            }

            long sum = memLimitBytesPerBe.values().stream().reduce(Long::sum).orElse(DEFAULT_MEM_LIMIT_BYTES);
            long avg = sum / memLimitBytesPerBe.size();
            cachedAvgMemLimitBytes.set(avg);

            return avg;
        }
    }

    public int getDefaultDOP() {
        int avgNumOfCores = getAvgNumHardwareCoresOfBe();
        return Math.max(1, avgNumOfCores / 2);
    }

    public int getSinkDefaultDOP() {
        // load includes query engine execution and storage engine execution
        // so we can't let query engine use up resources
        // At the same time, the improvement of performance and concurrency is not linear
        // but the memory usage increases linearly, so we control the slope of concurrent growth
        int avgCoreNum = getAvgNumHardwareCoresOfBe();
        if (avgCoreNum <= 24) {
            return Math.max(1, avgCoreNum / 3);
        } else {
            return Math.min(32, avgCoreNum / 4);
        }
    }

    /**
     * Used to dump and replay the state of the BackendResourceStat.
     */
    public ImmutableMap<Long, Integer> getNumHardwareCoresPerBe() {
        return ImmutableMap.copyOf(numHardwareCoresPerBe);
    }

    /**
     * Used to dump and replay the state of the BackendResourceStat.
     */
    public Integer getCachedAvgNumHardwareCores() {
        return cachedAvgNumHardwareCores.get();
    }

    /**
     * Used to dump and replay the state of the BackendResourceStat.
     */
    public void setCachedAvgNumHardwareCores(int cores) {
        cachedAvgNumHardwareCores.set(cores);
    }
}