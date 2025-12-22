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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.common.CloseableLock;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.WarehouseManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Cache the resource information of all backends and compute nodes, and provide the average value of the resource information.
 * The resource information includes the number of CPU cores and the memory limit for now.
 *
 * <p> MUST update the resource information of the backend when the backend is added, removed, or updated.
 *
 * <p> All the methods are thread-safe.
 * - NOTE that all methods that modify fields must be protected by `lock`.
 * - Read methods do not need to acquire the lock, because we are using `volatile` and concurrent containers.
 */
public class BackendResourceStat {

    private static final Logger LOG = LogManager.getLogger(BackendResourceStat.class);

    private static final int DEFAULT_CORES_OF_BE = 1;
    private static final long DEFAULT_MEM_LIMIT_BYTES = 0L;
    private static final int ABSENT_CACHE_VALUE = -1;

    private final ReentrantLock lock = new ReentrantLock();
    private final ConcurrentHashMap<Long, WarehouseResourceStat> warehouseIdToStat = new ConcurrentHashMap<>();

    private volatile int cachedAvgNumCores = ABSENT_CACHE_VALUE;
    private volatile int cachedMinNumCores = ABSENT_CACHE_VALUE;

    private static class SingletonHolder {
        private static final BackendResourceStat INSTANCE = new BackendResourceStat();
    }

    public static BackendResourceStat getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private BackendResourceStat() {
    }

    public void setNumCoresOfBe(long warehouseId, long be, int numCores) {
        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            boolean isChanged = warehouseIdToStat
                    .computeIfAbsent(warehouseId, whId -> new WarehouseResourceStat(whId, lock))
                    .setNumCoresOfBe(be, numCores);
            if (isChanged) {
                cachedAvgNumCores = ABSENT_CACHE_VALUE;
                cachedMinNumCores = ABSENT_CACHE_VALUE;
            }
        }
    }

    public void setMemLimitBytesOfBe(long warehouseId, long be, long memLimitBytes) {
        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            warehouseIdToStat
                    .computeIfAbsent(warehouseId, whId -> new WarehouseResourceStat(whId, lock))
                    .setMemLimitBytesOfBe(be, memLimitBytes);
        }
    }

    public void removeBe(long warehouseId, long beId) {
        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            WarehouseResourceStat stat = warehouseIdToStat.get(warehouseId);
            if (stat != null) {
                stat.removeBe(beId);
                cachedAvgNumCores = ABSENT_CACHE_VALUE;
                cachedMinNumCores = ABSENT_CACHE_VALUE;
            }
        }
    }

    public void removeWarehouse(long warehouseId) {
        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            WarehouseResourceStat stat = warehouseIdToStat.remove(warehouseId);
            if (stat != null) {
                cachedAvgNumCores = ABSENT_CACHE_VALUE;
                cachedMinNumCores = ABSENT_CACHE_VALUE;
            }
        }
    }

    public int getNumBes(long warehouseId) {
        WarehouseResourceStat stat = warehouseIdToStat.get(warehouseId);
        if (stat == null) {
            return 0;
        }
        return stat.getNumBes();
    }

    public int getAvgNumCoresOfBe() {
        int snapshotAvg = cachedAvgNumCores;
        if (snapshotAvg > 0) {
            return snapshotAvg;
        }

        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            List<Integer> numCoresPerBe = warehouseIdToStat.values().stream()
                    .flatMap(warehouseStat -> warehouseStat.numCoresPerBe.values().stream())
                    .toList();
            if (numCoresPerBe.isEmpty()) {
                return DEFAULT_CORES_OF_BE;
            }

            int totalCores = numCoresPerBe.stream().reduce(Integer::sum).orElse(DEFAULT_CORES_OF_BE);
            int avg = totalCores / numCoresPerBe.size();
            cachedAvgNumCores = avg;
            LOG.info("update avgNumHardwareCoresOfBe to {}, current stats: {}", avg, warehouseIdToStat);
            return avg;
        }
    }

    public int getAvgNumCoresOfBe(long warehouseId) {
        WarehouseResourceStat stat = warehouseIdToStat.get(warehouseId);
        if (stat != null) {
            return stat.getAvgNumCoresOfBe();
        }
        return DEFAULT_CORES_OF_BE;
    }

    public int getMinNumHardwareCoresOfBe() {
        int snapshot = cachedMinNumCores;
        if (snapshot > 0) {
            return snapshot;
        }

        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            List<Integer> numCoresPerBe = warehouseIdToStat.values().stream()
                    .flatMap(warehouseStat -> warehouseStat.numCoresPerBe.values().stream())
                    .toList();
            if (numCoresPerBe.isEmpty()) {
                return DEFAULT_CORES_OF_BE;
            }

            int min = numCoresPerBe.stream().reduce(Integer::min).orElse(DEFAULT_CORES_OF_BE);
            min = Math.max(min, 1);
            cachedMinNumCores = min;
            LOG.info("update minNumHardwareCoresOfBe to {}, current stats: {}", min, warehouseIdToStat);

            return min;
        }
    }

    public long getAvgMemLimitBytes(long warehouseId) {
        WarehouseResourceStat stat = warehouseIdToStat.get(warehouseId);
        if (stat != null) {
            return stat.getAvgMemLimitBytes();
        }
        return DEFAULT_MEM_LIMIT_BYTES;
    }

    public int getDefaultDOP(long warehouseId) {
        int avgNumOfCores = getAvgNumCoresOfBe(warehouseId);
        return Math.max(1, avgNumOfCores / 2);
    }

    public int getSinkDefaultDOP(long warehouseId) {
        // load includes query engine execution and storage engine execution
        // so we can't let query engine use up resources
        // At the same time, the improvement of performance and concurrency is not linear
        // but the memory usage increases linearly, so we control the slope of concurrent growth
        int avgCoreNum = getAvgNumCoresOfBe(warehouseId);
        if (avgCoreNum <= 24) {
            return Math.max(1, avgCoreNum / 3);
        } else {
            return Math.min(32, avgCoreNum / 4);
        }
    }

    public void dump(JsonObject dumpJson, ConnectContext connectContext) {
        // backend core stat
        JsonObject backendCoreStat = new JsonObject();
        backendCoreStat.addProperty("cachedAvgNumOfHardwareCores", cachedAvgNumCores);
        Map<Long, Integer> numCoresPerBe = warehouseIdToStat.values().stream()
                .flatMap(warehouseStat -> warehouseStat.numCoresPerBe.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        backendCoreStat.addProperty("numOfHardwareCoresPerBe", GsonUtils.GSON.toJson(numCoresPerBe));
        dumpJson.add("be_core_stat", backendCoreStat);

        // backend core stat v2
        JsonObject backendCoreStatV2 = new JsonObject();
        backendCoreStatV2.addProperty("cachedAvgNumOfHardwareCores", cachedAvgNumCores);
        JsonArray warehouseStats = new JsonArray();
        warehouseIdToStat.forEach((whId, stat) -> {
            JsonObject warehouseStat = new JsonObject();
            warehouseStat.addProperty("warehouseId", whId);
            warehouseStat.addProperty("cachedAvgNumOfHardwareCores", stat.cachedAvgNumCores);
            warehouseStat.addProperty("numOfHardwareCoresPerBe", GsonUtils.GSON.toJson(stat.numCoresPerBe));
            warehouseStats.add(warehouseStat);
        });
        backendCoreStatV2.add("warehouses", warehouseStats);
        Long currentWarehouseId = connectContext.getCurrentWarehouseIdAllowNull();
        backendCoreStatV2.addProperty("currentWarehouseId",
                currentWarehouseId == null ? WarehouseManager.DEFAULT_WAREHOUSE_ID : currentWarehouseId);
        dumpJson.add("be_core_stat_v2", backendCoreStatV2);
    }

    /**
     * Used to replay the state of the BackendResourceStat.
     */
    public void setCachedAvgNumCores(int cores) {
        cachedAvgNumCores = cores;
    }

    /**
     * Used to replay the state of the BackendResourceStat.
     */
    public void setCachedAvgNumCores(long warehouseId, int cores) {
        WarehouseResourceStat stat =
                warehouseIdToStat.computeIfAbsent(warehouseId, whId -> new WarehouseResourceStat(whId, lock));
        stat.cachedAvgNumCores = cores;
    }

    @VisibleForTesting
    public void reset() {
        try (CloseableLock ignored = CloseableLock.lock(lock)) {
            warehouseIdToStat.values().forEach(WarehouseResourceStat::reset);
            cachedAvgNumCores = ABSENT_CACHE_VALUE;
            cachedMinNumCores = ABSENT_CACHE_VALUE;
        }
    }

    @VisibleForTesting
    int getCachedAvgNumCores() {
        return cachedAvgNumCores;
    }

    private static class WarehouseResourceStat {
        private final long warehouseId;

        private final ReentrantLock lock;

        private final ConcurrentHashMap<Long, Integer> numCoresPerBe = new ConcurrentHashMap<>();
        private volatile int cachedAvgNumCores = ABSENT_CACHE_VALUE;
        private volatile int cachedMinNumCores = ABSENT_CACHE_VALUE;

        private final ConcurrentHashMap<Long, Long> memLimitBytesPerBe = new ConcurrentHashMap<>();
        private volatile long cachedAvgMemLimitBytes = ABSENT_CACHE_VALUE;

        private WarehouseResourceStat(long warehouseId, ReentrantLock lock) {
            this.warehouseId = warehouseId;
            this.lock = lock;
        }

        public boolean setNumCoresOfBe(long be, int numCores) {
            Integer previous = numCoresPerBe.put(be, numCores);
            LOG.info("set setNumCoresOfBe [{}] of be [{}] in warehouse [{}], current cpuCores stats: {}",
                    numCores, be, warehouseId, numCoresPerBe);
            boolean isChanged = previous == null || previous != numCores;
            if (isChanged) {
                cachedAvgNumCores = ABSENT_CACHE_VALUE;
                cachedMinNumCores = ABSENT_CACHE_VALUE;
            }
            return isChanged;
        }

        public boolean setMemLimitBytesOfBe(long be, long memLimitBytes) {
            Long previous = memLimitBytesPerBe.put(be, memLimitBytes);
            boolean isChanged = previous == null || previous != memLimitBytes;
            if (isChanged) {
                cachedAvgMemLimitBytes = ABSENT_CACHE_VALUE;
            }
            return isChanged;
        }

        public int getNumBes() {
            return Math.max(numCoresPerBe.size(), memLimitBytesPerBe.size());
        }

        public void removeBe(long beId) {
            if (numCoresPerBe.remove(beId) != null) {
                cachedAvgNumCores = ABSENT_CACHE_VALUE;
                cachedMinNumCores = ABSENT_CACHE_VALUE;
            }
            LOG.info("remove numHardwareCores of be [{}], current cpuCores stats: {}", beId, numCoresPerBe);

            if (memLimitBytesPerBe.remove(beId) != null) {
                cachedAvgMemLimitBytes = ABSENT_CACHE_VALUE;
            }
        }

        public void reset() {
            numCoresPerBe.clear();
            cachedAvgNumCores = ABSENT_CACHE_VALUE;
            cachedMinNumCores = ABSENT_CACHE_VALUE;

            memLimitBytesPerBe.clear();
            cachedAvgMemLimitBytes = ABSENT_CACHE_VALUE;
        }

        public int getAvgNumCoresOfBe() {
            int snapshotAvg = cachedAvgNumCores;
            if (snapshotAvg > 0) {
                return snapshotAvg;
            }

            try (CloseableLock ignored = CloseableLock.lock(lock)) {
                if (numCoresPerBe.isEmpty()) {
                    return DEFAULT_CORES_OF_BE;
                }

                List<Integer> snapshotNumCoresPerBe = new ArrayList<>(numCoresPerBe.values());
                int sum = snapshotNumCoresPerBe.stream().reduce(Integer::sum).orElse(DEFAULT_CORES_OF_BE);
                int avg = sum / snapshotNumCoresPerBe.size();
                cachedAvgNumCores = avg;
                return avg;
            }
        }

        public long getAvgMemLimitBytes() {
            long snapshotAvg = cachedAvgMemLimitBytes;
            if (snapshotAvg > 0) {
                return snapshotAvg;
            }

            try (CloseableLock ignored = CloseableLock.lock(lock)) {
                if (memLimitBytesPerBe.isEmpty()) {
                    return DEFAULT_MEM_LIMIT_BYTES;
                }

                List<Long> snapshotMemLimitBytesPerBe = new ArrayList<>(memLimitBytesPerBe.values());
                long sum = snapshotMemLimitBytesPerBe.stream().reduce(Long::sum).orElse(DEFAULT_MEM_LIMIT_BYTES);
                long avg = sum / snapshotMemLimitBytesPerBe.size();
                cachedAvgMemLimitBytes = avg;
                return avg;
            }
        }

        @Override
        public String toString() {
            return "WarehouseResourceStat{" +
                    "warehouseId=" + warehouseId +
                    ", lock=" + lock +
                    ", numCoresPerBe=" + numCoresPerBe +
                    ", cachedAvgNumCores=" + cachedAvgNumCores +
                    ", cachedMinNumCores=" + cachedMinNumCores +
                    ", memLimitBytesPerBe=" + memLimitBytesPerBe +
                    ", cachedAvgMemLimitBytes=" + cachedAvgMemLimitBytes +
                    '}';
        }
    }
}
