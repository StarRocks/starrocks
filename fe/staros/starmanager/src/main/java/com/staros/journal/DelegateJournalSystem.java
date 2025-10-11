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

package com.staros.journal;

import com.staros.exception.StarException;
import com.staros.metrics.MetricsSystem;
import io.prometheus.metrics.core.metrics.Counter;

import java.util.concurrent.Future;

/**
 * Delegate the JournalSystem, so that a few metrics can be collected.
 */
public class DelegateJournalSystem implements JournalSystem {
    private static final Counter METRIC_JOURNAL_WRITE_COUNT =
            MetricsSystem.registerCounter("starmgr_journal_write_ops", "counter write iops of starmgr journal records");

    private static final Counter METRIC_JOURNAL_WRITE_BYTES =
            MetricsSystem.registerCounter("starmgr_journal_write_bytes",
                    "number of numbers written to starmgr journal, unit: byte");

    private static final Counter METRIC_JOURNAL_WRITE_ASYNC_COUNT =
            MetricsSystem.registerCounter("starmgr_journal_write_async_ops",
                    "counter async write iops of starmgr journal records");

    private static final Counter METRIC_JOURNAL_WRITE_ASYNC_BYTES =
            MetricsSystem.registerCounter("starmgr_journal_write_async_bytes",
                    "number of numbers written to starmgr journal in async mode, unit: byte");
    private final JournalSystem innerJournalSystem;

    public DelegateJournalSystem(JournalSystem journalSystem) {
        this.innerJournalSystem = journalSystem;
    }

    @Override
    public void write(Journal journal) throws StarException {
        innerJournalSystem.write(journal);
        METRIC_JOURNAL_WRITE_COUNT.inc();
        METRIC_JOURNAL_WRITE_BYTES.inc(journal.size());
    }

    @Override
    public Future<Boolean> writeAsync(Journal journal) throws StarException {
        Future<Boolean> future = innerJournalSystem.writeAsync(journal);
        METRIC_JOURNAL_WRITE_ASYNC_COUNT.inc();
        METRIC_JOURNAL_WRITE_ASYNC_BYTES.inc(journal.size());
        return future;
    }

    @Override
    public void replayTo(long journalId) throws StarException {
        innerJournalSystem.replayTo(journalId);
    }

    @Override
    public void setReplayId(long replayId) {
        innerJournalSystem.setReplayId(replayId);
    }

    @Override
    public long getReplayId() {
        return innerJournalSystem.getReplayId();
    }

    @Override
    public void onBecomeLeader() {
        innerJournalSystem.onBecomeLeader();
    }

    @Override
    public void onBecomeFollower() {
        innerJournalSystem.onBecomeFollower();
    }
}
