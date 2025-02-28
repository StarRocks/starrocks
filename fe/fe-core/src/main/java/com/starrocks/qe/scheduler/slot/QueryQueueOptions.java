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
import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Objects;

public class QueryQueueOptions {

    private static final Logger LOG = LogManager.getLogger(QueryQueueOptions.class);

    private final boolean enableQueryQueueV2;
    private final V2 v2;
    private final SchedulePolicy policy;

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
        SchedulePolicy policy = SchedulePolicy.create(Config.query_queue_v2_schedule_strategy);
        if (policy == null) {
            LOG.error("unknown query_queue_v2_schedule_policy: {}", Config.query_queue_v2_schedule_strategy);
            policy = SchedulePolicy.createDefault();
        }
        return new QueryQueueOptions(true, v2, policy);
    }

    @VisibleForTesting
    QueryQueueOptions(boolean enableQueryQueueV2, V2 v2) {
        this.enableQueryQueueV2 = enableQueryQueueV2 && v2 != null;
        this.v2 = v2;
        this.policy = SchedulePolicy.createDefault();
    }

    QueryQueueOptions(boolean enableQueryQueueV2, V2 v2, SchedulePolicy policy) {
        this.enableQueryQueueV2 = enableQueryQueueV2 && v2 != null;
        this.v2 = v2;
        this.policy = policy;
    }

    public boolean isEnableQueryQueueV2() {
        return enableQueryQueueV2;
    }

    public V2 v2() {
        return v2;
    }

    public SchedulePolicy getPolicy() {
        return policy;
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
        return enableQueryQueueV2 == that.enableQueryQueueV2
                && Objects.equals(v2, that.v2)
                && policy.equals(that.policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enableQueryQueueV2, v2, policy);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("QueryQueueOptions{");
        sb.append("enableQueryQueueV2=").append(enableQueryQueueV2);
        sb.append(", v2=").append(v2);
        sb.append(", policy=").append(policy);
        sb.append('}');
        return sb.toString();
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

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("V2{");
            sb.append("numWorkers=").append(numWorkers);
            sb.append(", numRowsPerSlot=").append(numRowsPerSlot);
            sb.append(", totalSlots=").append(totalSlots);
            sb.append(", memBytesPerSlot=").append(memBytesPerSlot);
            sb.append(", cpuCostsPerSlot=").append(cpuCostsPerSlot);
            sb.append(", totalSmallSlots=").append(totalSmallSlots);
            sb.append('}');
            return sb.toString();
        }
    }

    public enum SchedulePolicy {
        SWRR, // Smooth Weighted Round Robin, which is suitable for hybrid workload without significant priority
        SJF; // Short Job First + Aging, which is suitable for workload needs significant priority

        public static SchedulePolicy createDefault() {
            return SWRR;
        }

        public static SchedulePolicy create(String value) {
            return EnumUtils.getEnumIgnoreCase(SchedulePolicy.class, value);
        }
    }
    private static boolean isAnyZero(long... values) {
        return Arrays.stream(values).anyMatch(val -> val == 0);
    }
}
