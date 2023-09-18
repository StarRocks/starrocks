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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class BackendCoreStat {

    private static final Logger LOG = LogManager.getLogger(BackendCoreStat.class);

    private static int DEFAULT_CORES_OF_BE = 1;

    private static ConcurrentHashMap<Long, Integer> numOfHardwareCoresPerBe = new ConcurrentHashMap<>();
    private static AtomicInteger cachedAvgNumOfHardwareCores = new AtomicInteger(-1);
    private static ReentrantLock lock = new ReentrantLock();

    public static void setNumOfHardwareCoresOfBe(long be, int numOfCores) {
        Integer previous = numOfHardwareCoresPerBe.put(be, numOfCores);
        LOG.info("set numOfHardwareCores of be({}) to {}, current cpuCores stats: {}", be, numOfCores,
                numOfHardwareCoresPerBe);
        if (previous == null || previous != numOfCores) {
            cachedAvgNumOfHardwareCores.set(-1);
        }
    }

    public static void removeNumOfHardwareCoresOfBe(long be) {
        Integer previous = numOfHardwareCoresPerBe.remove(be);
        LOG.info("remove numOfHardwareCores of be({}), current cpuCores stats: {}", be, numOfHardwareCoresPerBe);
        if (previous != null) {
            cachedAvgNumOfHardwareCores.set(-1);
        }
    }

    public static void reset() {
        numOfHardwareCoresPerBe.clear();
        cachedAvgNumOfHardwareCores.set(-1);
    }

    public static ImmutableMap<Long, Integer> getNumOfHardwareCoresPerBe() {
        return ImmutableMap.copyOf(numOfHardwareCoresPerBe);
    }

    public static Integer getCachedAvgNumOfHardwareCores() {
        return cachedAvgNumOfHardwareCores.get();
    }

    // used for mock env
    public static void setCachedAvgNumOfHardwareCores(int cores) {
        cachedAvgNumOfHardwareCores = new AtomicInteger(cores);
    }

    public static void setDefaultCoresOfBe(int cores) {
        DEFAULT_CORES_OF_BE = cores;
    }

    public static int getCoresOfBe(long beId) {
        return numOfHardwareCoresPerBe.getOrDefault(beId, 0);
    }

    public static int getAvgNumOfHardwareCoresOfBe() {
        // optimistic path
        int snapshotAvg = cachedAvgNumOfHardwareCores.get();
        if (snapshotAvg > 0) {
            return snapshotAvg;
        }

        // pessimistic path
        try {
            lock.lock();
            if (numOfHardwareCoresPerBe.isEmpty()) {
                return DEFAULT_CORES_OF_BE;
            }
            int sum = numOfHardwareCoresPerBe.values().stream().reduce(Integer::sum).get();
            int avg = sum / numOfHardwareCoresPerBe.size();
            cachedAvgNumOfHardwareCores.set(avg);
            LOG.info("update avgNumOfHardwareCoresOfBe to {}, current cpuCores stats: {}", avg,
                    numOfHardwareCoresPerBe);
            return avg;
        } finally {
            lock.unlock();
        }
    }

    public static int getDefaultDOP() {
        int avgNumOfCores = BackendCoreStat.getAvgNumOfHardwareCoresOfBe();
        return Math.max(1, avgNumOfCores / 2);
    }

    public static int getSinkDefaultDOP() {
        // load includes query engine execution and storage engine execution
        // so we can't let query engine use up resources
        // At the same time, the improvement of performance and concurrency is not linear
        // but the memory usage increases linearly, so we control the slope of concurrent growth
        int avgCoreNum = BackendCoreStat.getAvgNumOfHardwareCoresOfBe();
        if (avgCoreNum <= 24) {
            return Math.max(1, avgCoreNum / 3);
        } else {
            return Math.min(32, avgCoreNum / 4);
        }
    }
}