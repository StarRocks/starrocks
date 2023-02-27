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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BackendCoreStat {
    private static int DEFAULT_CORES_OF_BE = 1;

    private static ConcurrentHashMap<Long, Integer> numOfHardwareCoresPerBe = new ConcurrentHashMap<>();
    private static AtomicInteger cachedAvgNumOfHardwareCores = new AtomicInteger(-1);

    public static void setNumOfHardwareCoresOfBe(long be, int numOfCores) {
        Integer previous = numOfHardwareCoresPerBe.put(be, numOfCores);
        if (previous == null || previous != numOfCores) {
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
        int snapshotAvg = cachedAvgNumOfHardwareCores.get();
        if (snapshotAvg > 0) {
            return snapshotAvg;
        }

        cachedAvgNumOfHardwareCores.compareAndSet(snapshotAvg, 0);
        Integer[] numCoresArray = new Integer[0];
        numCoresArray = numOfHardwareCoresPerBe.values().toArray(numCoresArray);
        if (numCoresArray.length == 0) {
            return DEFAULT_CORES_OF_BE;
        }
        int sum = 0;
        for (Integer v : numCoresArray) {
            sum += v;
        }
        int newAvg = sum / numCoresArray.length;
        snapshotAvg = 0;
        // Update the cached value if numOfHardwareCoresPerBe is changed(cachedAvgNumOfHardwareCores = -1)
        cachedAvgNumOfHardwareCores.compareAndSet(snapshotAvg, newAvg);
        return newAvg;
    }

    public static int getDefaultDOP() {
        return Math.max(1, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
    }
}