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

package com.starrocks.qe.scheduler.slot;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.system.BackendResourceStat;

import java.util.Arrays;
import java.util.Objects;

public class QueryQueueOptions {
    private final boolean enableQueryQueueV2;
    private final V2 v2;

    public static QueryQueueOptions createFromEnvAndQuery(DefaultCoordinator coord) {
        if (!coord.getJobSpec().isEnableQueue() || !coord.getJobSpec().isNeedQueued()) {
            return new QueryQueueOptions(false, V2.DEFAULT);
        }

        return createFromEnv();
    }

    public static QueryQueueOptions createFromEnv() {
        if (!Config.enable_query_queue_v2) {
            return new QueryQueueOptions(false, V2.DEFAULT);
        }

        V2 v2 = new V2(Config.query_queue_v2_concurrency_level,
                BackendResourceStat.getInstance().getNumBes(),
                BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe(),
                BackendResourceStat.getInstance().getAvgMemLimitBytes(),
                Config.query_queue_v2_num_rows_per_slot,
                Config.query_queue_v2_cpu_costs_per_slot);
        return new QueryQueueOptions(true, v2);
    }

    @VisibleForTesting
    QueryQueueOptions(boolean enableQueryQueueV2, V2 v2) {
        this.enableQueryQueueV2 = enableQueryQueueV2 && v2 != null;
        this.v2 = v2;
    }

    public boolean isEnableQueryQueueV2() {
        return enableQueryQueueV2;
    }

    public V2 v2() {
        return v2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryQueueOptions that = (QueryQueueOptions) o;
        return enableQueryQueueV2 == that.enableQueryQueueV2 && Objects.equals(v2, that.v2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enableQueryQueueV2, v2);
    }

    public static class V2 {
        private static final V2 DEFAULT = new V2();

        private final int numWorkers;
        private final int numRowsPerSlot;

        private final int totalSlots;
        private final long memBytesPerSlot;
        private final long cpuCostsPerSlot;
        private final int totalSmallSlots;

        @VisibleForTesting
        V2() {
            this(1, 1, 1, 1, 1, 1);
        }

        @VisibleForTesting
        V2(int concurrencyLevel, int numWorkers, int numCoresPerWorker, long memLimitBytesPerWorker, int numRowsPerSlot,
                long cpuCostsPerSlot) {
            if (concurrencyLevel <= 0) {
                concurrencyLevel = 4;
            }
            int normNumWorkers = Math.max(1, numWorkers);
            int normNumCoresPerWorker = Math.max(1, numCoresPerWorker);
            int normNumRowsPerSlot = Math.max(1, numRowsPerSlot);
            long normCpuCostsPerSlot = Math.max(1, cpuCostsPerSlot);

            this.numWorkers = normNumWorkers;
            this.numRowsPerSlot = normNumRowsPerSlot;

            this.totalSlots = normNumCoresPerWorker * concurrencyLevel * normNumWorkers;
            int totalSlotsPerWorker = normNumCoresPerWorker * concurrencyLevel;
            this.totalSmallSlots = normNumCoresPerWorker;
            this.memBytesPerSlot = isAnyZero(memLimitBytesPerWorker, numCoresPerWorker) ? Long.MAX_VALUE :
                    memLimitBytesPerWorker / totalSlotsPerWorker;
            this.cpuCostsPerSlot = normCpuCostsPerSlot;
        }

        public int getNumWorkers() {
            return numWorkers;
        }

        public int getNumRowsPerSlot() {
            return numRowsPerSlot;
        }

        public int getTotalSlots() {
            return totalSlots;
        }

        public long getMemBytesPerSlot() {
            return memBytesPerSlot;
        }

        public int getTotalSmallSlots() {
            return totalSmallSlots;
        }

        public long getCpuCostsPerSlot() {
            return cpuCostsPerSlot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            V2 v2 = (V2) o;
            return numWorkers == v2.numWorkers && numRowsPerSlot == v2.numRowsPerSlot && totalSlots == v2.totalSlots &&
                    memBytesPerSlot == v2.memBytesPerSlot && totalSmallSlots == v2.totalSmallSlots &&
                    cpuCostsPerSlot == v2.cpuCostsPerSlot;
        }

        @Override
        public int hashCode() {
            return Objects.hash(numWorkers, numRowsPerSlot, totalSlots, memBytesPerSlot, totalSmallSlots, cpuCostsPerSlot);
        }
    }

    private static boolean isAnyZero(long... values) {
        return Arrays.stream(values).anyMatch(val -> val == 0);
    }
}
