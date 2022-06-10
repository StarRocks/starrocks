// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.system;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BackendCoreStat {
    private static int DEFAULT_CORES_OF_BE = 1;

    private static ConcurrentHashMap<Long, Integer> numOfHardwareCoresPerBe = new ConcurrentHashMap<>();
    private static AtomicInteger cachedAvgNumOfHardwareCores = new AtomicInteger(-1);

    public static void setNumOfHardwareCoresOfBe(long be, int numOfCores) {
        if (numOfHardwareCoresPerBe.putIfAbsent(be, numOfCores) == null) {
            cachedAvgNumOfHardwareCores.set(-1);
        }
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
        int avgNumOfCores = BackendCoreStat.getAvgNumOfHardwareCoresOfBe();
        return Math.max(1, avgNumOfCores / 2);
    }
}