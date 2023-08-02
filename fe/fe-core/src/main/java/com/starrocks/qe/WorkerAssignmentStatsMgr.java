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

package com.starrocks.qe;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class WorkerAssignmentStatsMgr {
    private final ConcurrentMap<Long, WorkerStats> workerToStats = Maps.newConcurrentMap();

    public WorkerStatsTracker createGlobalWorkerStatsTracker() {
        return new GlobalWorkerStatsTracker(workerToStats);
    }

    public WorkerStatsTracker createLocalWorkerStatsTracker() {
        return new LocalWorkerStatsTracker(workerToStats);
    }

    public interface WorkerStatsTracker {
        Long getNumRunningTablets(Long workerId);

        Long getNumTotalTablets(Long workerId);

        Long getNumRunningTabletRows(Long workerId);

        boolean tryConsume(Long workerId, Long expectedNumRunningTablets, Long numRunningTablets, Long numRunningTabletRows);

        void consume(Long workerId, Long numRunningTablets, Long numRunningTabletRows);

        void release();
    }

    private abstract static class BaseWorkerStatsTracker implements WorkerStatsTracker {
        protected final Map<Long, WorkerStats> localWorkerToStats = Maps.newHashMap();
        protected final ConcurrentMap<Long, WorkerStats> globalWorkerToStats;

        public BaseWorkerStatsTracker(ConcurrentMap<Long, WorkerStats> globalWorkerToStats) {
            this.globalWorkerToStats = globalWorkerToStats;
        }

        protected abstract Map<Long, WorkerStats> getWorkerToStats();

        @Override
        public Long getNumRunningTablets(Long workerId) {
            WorkerStats stats = getWorkerToStats().get(workerId);
            if (stats == null) {
                return 0L;
            }
            return stats.numRunningTablets.get();
        }

        @Override
        public Long getNumTotalTablets(Long workerId) {
            WorkerStats stats = getWorkerToStats().get(workerId);
            if (stats == null) {
                return 0L;
            }
            return stats.numTotalTablets.get();
        }

        @Override
        public Long getNumRunningTabletRows(Long workerId) {
            WorkerStats stats = getWorkerToStats().get(workerId);
            if (stats == null) {
                return 0L;
            }
            return stats.numRunningTabletRows.get();
        }

        @Override
        public void consume(Long workerId, Long numRunningTablets, Long numRunningTabletRows) {
            localWorkerToStats.computeIfAbsent(workerId, k -> new WorkerStats())
                    .consume(numRunningTablets, numRunningTabletRows);

            globalWorkerToStats.computeIfAbsent(workerId, k -> new WorkerStats())
                    .consume(numRunningTablets, numRunningTabletRows);
        }

        @Override
        public void release() {
            localWorkerToStats.forEach((workerId, stats) ->
                    globalWorkerToStats.get(workerId).consume(-stats.numRunningTablets.get(), -stats.numRunningTabletRows.get()));
        }

        @Override
        public String toString() {
            return "WorkerStatsTracker{" +
                    "localWorkerToStats=" + localWorkerToStats +
                    ", globalWorkerToStats=" + globalWorkerToStats +
                    '}';
        }
    }

    public static class GlobalWorkerStatsTracker extends BaseWorkerStatsTracker {
        public GlobalWorkerStatsTracker(ConcurrentMap<Long, WorkerStats> globalWorkerToStats) {
            super(globalWorkerToStats);
        }

        @Override
        protected Map<Long, WorkerStats> getWorkerToStats() {
            return globalWorkerToStats;
        }

        @Override
        public boolean tryConsume(Long workerId, Long expectedNumRunningTablets, Long numRunningTablets,
                                  Long numRunningTabletRows) {
            WorkerStats stats = globalWorkerToStats.computeIfAbsent(workerId, k -> new WorkerStats());
            boolean ok = stats.tryConsume(expectedNumRunningTablets, numRunningTablets, numRunningTabletRows);
            if (!ok) {
                return false;
            }

            localWorkerToStats.computeIfAbsent(workerId, k -> new WorkerStats())
                    .consume(numRunningTablets, numRunningTabletRows);

            return true;
        }
    }

    public static class LocalWorkerStatsTracker extends BaseWorkerStatsTracker {
        public LocalWorkerStatsTracker(ConcurrentMap<Long, WorkerStats> globalWorkerToStats) {
            super(globalWorkerToStats);
        }

        @Override
        protected Map<Long, WorkerStats> getWorkerToStats() {
            return localWorkerToStats;
        }

        @Override
        public boolean tryConsume(Long workerId, Long expectedNumRunningTablets, Long numRunningTablets,
                                  Long numRunningTabletRows) {
            consume(workerId, numRunningTablets, numRunningTabletRows);
            return true;
        }
    }

    private static class WorkerStats {
        private final AtomicLong numRunningTablets = new AtomicLong();
        private final AtomicLong numTotalTablets = new AtomicLong();
        private final AtomicLong numRunningTabletRows = new AtomicLong();

        private void consume(Long numRunningTablets, Long numRunningTabletRows) {
            if (numRunningTablets > 0) {
                this.numTotalTablets.addAndGet(numRunningTablets);
            }
            this.numRunningTablets.addAndGet(numRunningTablets);
            this.numRunningTabletRows.addAndGet(numRunningTabletRows);
        }

        private boolean tryConsume(Long expectedNumRunningTablets, Long numRunningTablets, Long numRunningTabletRows) {
            if (numRunningTablets <= 0) {
                consume(numRunningTablets, numRunningTabletRows);
                return true;
            }

            boolean ok = this.numRunningTablets.compareAndSet(expectedNumRunningTablets,
                    expectedNumRunningTablets + numRunningTablets);
            if (!ok) {
                return false;
            }

            this.numTotalTablets.addAndGet(numRunningTablets);
            this.numRunningTabletRows.addAndGet(numRunningTabletRows);

            return true;
        }

        @Override
        public String toString() {
            return "WorkerStats{" +
                    "numRunningTablets=" + numRunningTablets +
                    ", numTotalTablets=" + numTotalTablets +
                    ", numRunningTabletRows=" + numRunningTabletRows +
                    '}';
        }
    }
}
