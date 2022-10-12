// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.cost;

import static java.lang.Double.POSITIVE_INFINITY;

public class CostEstimate {
    private static final CostEstimate ZERO = new CostEstimate(0, 0, 0);
    private static final CostEstimate INFINITE =
            new CostEstimate(POSITIVE_INFINITY, POSITIVE_INFINITY, POSITIVE_INFINITY);

    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;

    public CostEstimate(double cpuCost, double memoryCost, double networkCost) {
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
    }

    public double getCpuCost() {
        return cpuCost;
    }

    public double getMemoryCost() {
        return memoryCost;
    }

    public double getNetworkCost() {
        return networkCost;
    }

    public static CostEstimate addCost(CostEstimate left, CostEstimate right) {
        return new CostEstimate(left.cpuCost + right.cpuCost,
                left.memoryCost + right.memoryCost,
                left.networkCost + right.networkCost);
    }

    public static boolean isZero(CostEstimate costEstimate) {
        return costEstimate.cpuCost == 0 && costEstimate.memoryCost == 0 && costEstimate.networkCost == 0;
    }

    public static CostEstimate zero() {
        return ZERO;
    }

    public static CostEstimate infinite() {
        return INFINITE;
    }

    public static CostEstimate ofCpu(double cpuCost) {
        return of(cpuCost, 0, 0);
    }

    public static CostEstimate ofMemory(double memoryCost) {
        return of(0, memoryCost, 0);
    }

    public static CostEstimate of(double cpuCost, double memoryCost, double networkCost) {
        return new CostEstimate(cpuCost, memoryCost, networkCost);
    }
}
