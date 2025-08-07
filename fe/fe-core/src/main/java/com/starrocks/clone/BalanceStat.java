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

package com.starrocks.clone;

import com.google.gson.Gson;

public abstract class BalanceStat {
    public enum BalanceType {
        CLUSTER_DISK("cluster disk"),
        CLUSTER_TABLET("cluster tablet"),
        BACKEND_DISK("backend disk"),
        BACKEND_TABLET("backend tablet");

        private final String label;

        BalanceType(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    private static final Gson GSON = new Gson();

    // Singleton instance indicating that everything is balanced
    public static final BalanceStat BALANCED_STAT = new BalancedStat();

    private boolean balanced;

    public BalanceStat(boolean balanced) {
        this.balanced = balanced;
    }

    public boolean isBalanced() {
        return balanced;
    }

    public abstract BalanceType getBalanceType();

    @Override
    public String toString() {
        return GSON.toJson(this);
    }

    // Factory methods for different balance stat types
    public static BalanceStat createClusterDiskBalanceStat(long maxBeId, long minBeId, double maxDiskUsage, double minDiskUsage) {
        return new ClusterDiskBalanceStat(maxBeId, minBeId, maxDiskUsage, minDiskUsage);
    }

    public static BalanceStat createClusterTabletBalanceStat(long maxBeId, long minBeId, long maxTabletCount,
                                                             long minTabletCount) {
        return new ClusterTabletBalanceStat(maxBeId, minBeId, maxTabletCount, minTabletCount);
    }

    public static BalanceStat createBackendDiskBalanceStat(long beId, String maxPath, String minPath, double maxDiskUsage,
                                                           double minDiskUsage) {
        return new BackendDiskBalanceStat(beId, maxPath, minPath, maxDiskUsage, minDiskUsage);
    }

    public static BalanceStat createBackendTabletBalanceStat(long beId, String maxPath, String minPath, long maxTabletCount,
                                                             long minTabletCount) {
        return new BackendTabletBalanceStat(beId, maxPath, minPath, maxTabletCount, minTabletCount);
    }

    /**
     * Represents balanced stat
     */
    private static class BalancedStat extends BalanceStat {
        public BalancedStat() {
            super(true);
        }

        public BalanceType getBalanceType() {
            return null;
        }
    }

    /**
     * Base abstract class for all unbalanced stats
     */
    private abstract static class UnbalancedStat extends BalanceStat {
        private BalanceType type;

        public UnbalancedStat(BalanceType type) {
            super(false);
            this.type = type;
        }

        @Override
        public BalanceType getBalanceType() {
            return type;
        }
    }

    /**
     * Base abstract class for cluster unbalanced stats
     */
    private abstract static class ClusterBalanceStat extends UnbalancedStat {
        private long maxBeId;
        private long minBeId;

        public ClusterBalanceStat(BalanceType type, long maxBeId, long minBeId) {
            super(type);
            this.maxBeId = maxBeId;
            this.minBeId = minBeId;
        }
    }

    /**
     * Balance stat for cluster disk usage
     */
    private static class ClusterDiskBalanceStat extends ClusterBalanceStat {
        private double maxUsedPercent;
        private double minUsedPercent;

        public ClusterDiskBalanceStat(long maxBeId, long minBeId, double maxUsedPercent, double minUsedPercent) {
            super(BalanceType.CLUSTER_DISK, maxBeId, minBeId);
            this.maxUsedPercent = maxUsedPercent;
            this.minUsedPercent = minUsedPercent;
        }
    }

    /**
     * Balance stat for cluster tablet distribution
     */
    private static class ClusterTabletBalanceStat extends ClusterBalanceStat {
        private long maxTabletNum;
        private long minTabletNum;

        public ClusterTabletBalanceStat(long maxBeId, long minBeId, long maxTabletNum, long minTabletNum) {
            super(BalanceType.CLUSTER_TABLET, maxBeId, minBeId);
            this.maxTabletNum = maxTabletNum;
            this.minTabletNum = minTabletNum;
        }
    }

    /**
     * Base abstract class for backend unbalanced stats
     */
    private abstract static class BackendBalanceStat extends UnbalancedStat {
        private long beId;
        private String maxPath;
        private String minPath;

        public BackendBalanceStat(BalanceType type, long beId, String maxPath, String minPath) {
            super(type);
            this.beId = beId;
            this.maxPath = maxPath;
            this.minPath = minPath;
        }
    }

    /**
     * Balance stat for backend disk usage
     */
    private static class BackendDiskBalanceStat extends BackendBalanceStat {
        private double maxUsedPercent;
        private double minUsedPercent;

        public BackendDiskBalanceStat(long beId, String maxPath, String minPath, double maxUsedPercent, double minUsedPercent) {
            super(BalanceType.BACKEND_DISK, beId, maxPath, minPath);
            this.maxUsedPercent = maxUsedPercent;
            this.minUsedPercent = minUsedPercent;
        }
    }

    /**
     * Balance stat for backend tablet distribution
     */
    private static class BackendTabletBalanceStat extends BackendBalanceStat {
        private long maxTabletNum;
        private long minTabletNum;

        public BackendTabletBalanceStat(long beId, String maxPath, String minPath, long maxTabletNum, long minTabletNum) {
            super(BalanceType.BACKEND_TABLET, beId, maxPath, minPath);
            this.maxTabletNum = maxTabletNum;
            this.minTabletNum = minTabletNum;
        }
    }
}
