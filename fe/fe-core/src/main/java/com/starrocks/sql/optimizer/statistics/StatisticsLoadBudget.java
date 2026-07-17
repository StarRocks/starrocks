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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;

import java.util.concurrent.TimeUnit;

/**
 * Models the time budget we have per query to wait for stats to be read for query planning.
 */
public class StatisticsLoadBudget {
    private final long totalBudgetMs;
    private long consumedBudgetMs;

    public StatisticsLoadBudget(long totalBudgetMs) {
        this.totalBudgetMs = Math.max(totalBudgetMs, 0);
    }

    public static StatisticsLoadBudget fromConfig() {
        long configuredBudgetMs = Config.sync_statistics_load_per_query_budget_ms;
        long totalBudgetMs = configuredBudgetMs < 0 ? Config.sync_statistics_load_timeout_ms : configuredBudgetMs;
        return new StatisticsLoadBudget(totalBudgetMs);
    }

    public static Scope openScope(ConnectContext connectContext) {
        return new Scope(connectContext);
    }

    public synchronized long getRemainingTimeoutMs(long desiredTimeoutMs) {
        if (desiredTimeoutMs <= 0 || totalBudgetMs <= 0) {
            return 0;
        }
        long remainingBudgetMs = totalBudgetMs - consumedBudgetMs;
        if (remainingBudgetMs <= 0) {
            return 0;
        }
        return Math.min(desiredTimeoutMs, remainingBudgetMs);
    }

    public synchronized void recordWait(long elapsedNanos) {
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(Math.max(elapsedNanos, 0));
        consumedBudgetMs += elapsedMs;
    }

    public synchronized long getTotalBudgetMs() {
        return totalBudgetMs;
    }

    public synchronized long getConsumedBudgetMs() {
        return consumedBudgetMs;
    }

    public synchronized long getRemainingBudgetMs() {
        return Math.max(totalBudgetMs - consumedBudgetMs, 0);
    }

    public static class Scope implements AutoCloseable {
        private final ConnectContext connectContext;
        private final StatisticsLoadBudget previousBudget;
        private final boolean owner;

        private Scope(ConnectContext connectContext) {
            this.connectContext = connectContext;
            this.previousBudget = connectContext == null ? null : connectContext.getStatisticsLoadBudget();
            this.owner = connectContext != null && previousBudget == null;
            if (owner) {
                connectContext.setStatisticsLoadBudget(StatisticsLoadBudget.fromConfig());
            }
        }

        @Override
        public void close() {
            if (owner) {
                connectContext.setStatisticsLoadBudget(previousBudget);
            }
        }
    }
}
