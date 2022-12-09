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

    public CostEstimate multiplyBy(double factor) {
        return new CostEstimate(cpuCost * factor,
                memoryCost * factor,
                networkCost * factor);
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

    @Override
    public String toString() {
        return String.format("[cpuCost: %f. memoryCost: %f. networkCost: %f.]", cpuCost, memoryCost, networkCost);
    }
}
