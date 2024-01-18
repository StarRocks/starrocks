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

package com.starrocks.cloudnative;

import com.google.common.collect.Lists;
import com.starrocks.cloudnative.compaction.CompactionMgr;
import com.starrocks.cloudnative.compaction.PartitionIdentifier;
import com.starrocks.cloudnative.compaction.Quantiles;
import com.starrocks.common.conf.Config;
import com.starrocks.transaction.CommitRateExceededException;
import com.starrocks.transaction.TransactionState;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CommitRateLimiterTest {
    private final long dbId = 1;
    private final long tableId = 2;
    private final long timeoutMs = 60_000;
    private CompactionMgr compactionMgr;
    private TransactionState transactionState;
    private CommitRateLimiter limiter;
    private final double ratio;
    private final double threshold;

    public CommitRateLimiterTest() {
        ratio = Config.lake_ingest_slowdown_ratio;
        threshold = Config.lake_ingest_slowdown_threshold;
    }

    @Before
    public void before() {
        compactionMgr = new CompactionMgr();
        transactionState = new TransactionState(dbId, Lists.newArrayList(tableId), 123456L, "label", null,
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, null, 0, timeoutMs);
        limiter = new CommitRateLimiter(compactionMgr, transactionState, tableId);
    }

    @Test
    public void testEmptyPartitionList() throws CommitRateExceededException {
        long currentTimeMs = System.currentTimeMillis();
        transactionState.setPrepareTime(currentTimeMs - 10_000);
        transactionState.setWriteEndTimeMs(currentTimeMs);

        limiter.check(Collections.emptySet(), currentTimeMs);
        Assert.assertEquals(transactionState.getWriteEndTimeMs(), transactionState.getAllowCommitTimeMs());
    }

    @Test
    public void testNotThrottled() throws CommitRateExceededException {
        long partitionId = 54321;
        Set<Long> partitions = new HashSet<>(Collections.singletonList(partitionId));

        long currentTimeMs = System.currentTimeMillis();
        transactionState.setPrepareTime(currentTimeMs - 100);
        transactionState.setWriteEndTimeMs(currentTimeMs);

        Assert.assertTrue(ratio > 0.01);
        Assert.assertTrue(threshold > 0);

        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(threshold)));

        limiter.check(partitions, currentTimeMs);
        Assert.assertEquals(transactionState.getWriteEndTimeMs(), transactionState.getAllowCommitTimeMs());
    }

    @Test
    public void testThrottled() {
        long partitionId = 54321;
        Set<Long> partitions = new HashSet<>(Collections.singletonList(partitionId));

        long currentTimeMs = System.currentTimeMillis();
        transactionState.setPrepareTime(currentTimeMs - 100);
        transactionState.setWriteEndTimeMs(currentTimeMs);

        Assert.assertTrue(ratio > 0.01);
        Assert.assertTrue(threshold > 0);

        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(threshold + 1.0)));

        CommitRateExceededException e1 =
                Assert.assertThrows(CommitRateExceededException.class, () -> limiter.check(partitions, currentTimeMs));
        Assert.assertTrue(e1.getAllowCommitTime() > currentTimeMs);

        // test retry after compaction score changed
        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 4, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(threshold + 10.0)));

        CommitRateExceededException e2 =
                Assert.assertThrows(CommitRateExceededException.class, () -> limiter.check(partitions, currentTimeMs));
        // allowCommitTime will not be affected by new compaction score
        Assert.assertEquals(e1.getAllowCommitTime(), e2.getAllowCommitTime());
    }

    @Test
    public void testDelayTime() {
        long partitionId = 54321;
        Set<Long> partitions = new HashSet<>(Collections.singletonList(partitionId));

        long writeDurationMs = 10000;
        long currentTimeMs = System.currentTimeMillis();
        transactionState.setPrepareTime(currentTimeMs - 100);
        transactionState.setWriteEndTimeMs(currentTimeMs);
        transactionState.setWriteDurationMs(writeDurationMs);

        Assert.assertTrue(ratio > 0.01);
        Assert.assertTrue(threshold > 0);

        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(threshold + 1.0)));

        CommitRateExceededException e1 =
                Assert.assertThrows(CommitRateExceededException.class, () -> limiter.check(partitions, currentTimeMs));
        Assert.assertTrue(e1.getAllowCommitTime() > currentTimeMs);
        Assert.assertEquals(currentTimeMs + writeDurationMs * 1.0 * ratio, e1.getAllowCommitTime(), 0.1);

        // test retry after compaction score changed
        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 4, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(threshold + 10.0)));

        CommitRateExceededException e2 =
                Assert.assertThrows(CommitRateExceededException.class, () -> limiter.check(partitions, currentTimeMs));
        // allowCommitTime will be affected by new compaction score
        Assert.assertEquals(e1.getAllowCommitTime(), e2.getAllowCommitTime());
    }

    @Test
    public void testAborted() {
        long partitionId = 54321;
        Set<Long> partitions = new HashSet<>(Collections.singletonList(partitionId));

        long currentTimeMs = System.currentTimeMillis();
        transactionState.setPrepareTime(currentTimeMs - timeoutMs);
        transactionState.setWriteEndTimeMs(currentTimeMs);

        Assert.assertTrue(ratio > 0.01);
        Assert.assertTrue(threshold > 0);

        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(threshold + 10.0)));

        CommitFailedException e1 =
                Assert.assertThrows(CommitFailedException.class, () -> limiter.check(partitions, currentTimeMs));
        Assert.assertTrue(e1.getMessage().contains("timed out"));
    }

    @Test
    public void testPartitionHasNoStatistics() throws CommitRateExceededException {
        long partitionId = 54321;
        Set<Long> partitions = new HashSet<>(Collections.singletonList(partitionId));

        long currentTimeMs = System.currentTimeMillis();
        transactionState.setPrepareTime(currentTimeMs - 100);
        transactionState.setWriteEndTimeMs(currentTimeMs);

        Assert.assertTrue(ratio > 0.01);
        Assert.assertTrue(threshold > 0);

        limiter.check(partitions, currentTimeMs);
        Assert.assertEquals(transactionState.getWriteEndTimeMs(), transactionState.getAllowCommitTimeMs());
    }

    @Test
    public void testCompactionTxn() throws CommitRateExceededException {
        long partitionId = 54321;
        long currentTimeMs = System.currentTimeMillis();
        Set<Long> partitions = new HashSet<>(Collections.singletonList(partitionId));

        transactionState = new TransactionState(dbId, Lists.newArrayList(tableId), 123456L, "label", null,
                TransactionState.LoadJobSourceType.LAKE_COMPACTION, null, 0, timeoutMs);

        transactionState.setPrepareTime(currentTimeMs - 100);
        transactionState.setWriteEndTimeMs(currentTimeMs);

        limiter = new CommitRateLimiter(compactionMgr, transactionState, tableId);

        Assert.assertTrue(ratio > 0.01);
        Assert.assertTrue(threshold > 0);

        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(threshold + 100)));

        limiter.check(partitions, currentTimeMs);
    }

    @Test
    public void testCompactionUpperBoundLimit01() {
        long partitionId = 54321;
        Set<Long> partitions = new HashSet<>(Collections.singletonList(partitionId));

        long currentTimeMs = System.currentTimeMillis();
        transactionState.setPrepareTime(currentTimeMs - 100);
        transactionState.setWriteEndTimeMs(currentTimeMs);

        Assert.assertTrue(ratio > 0.01);
        Assert.assertTrue(threshold > 0);

        new MockUp<CommitRateLimiter>() {
            @Mock
            long compactionScoreUpperBound() {
                return (long) (threshold - 10);
            }
        };

        // lake_compaction_score_upper_bound < compactionScore < lake_ingest_slowdown_threshold
        double compactionScore = threshold - 5;
        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(compactionScore)));

        CommitRateExceededException e =
                Assert.assertThrows(CommitRateExceededException.class, () -> limiter.check(partitions, currentTimeMs));
        Assert.assertEquals(currentTimeMs + 1000, e.getAllowCommitTime());

        CommitRateExceededException e2 =
                Assert.assertThrows(CommitRateExceededException.class, () -> limiter.check(partitions, currentTimeMs));
        Assert.assertEquals(currentTimeMs + 1000, e2.getAllowCommitTime());
    }

    @Test
    public void testCompactionUpperBoundLimit02() {
        long partitionId = 54321;
        Set<Long> partitions = new HashSet<>(Collections.singletonList(partitionId));

        long currentTimeMs = System.currentTimeMillis();
        transactionState.setPrepareTime(currentTimeMs - 100);
        transactionState.setWriteEndTimeMs(currentTimeMs);

        Assert.assertTrue(ratio > 0.01);
        Assert.assertTrue(threshold > 0);

        new MockUp<CommitRateLimiter>() {
            @Mock
            long compactionScoreUpperBound() {
                return (long) (threshold + 10);
            }
        };

        // lake_ingest_slowdown_threshold < compactionScore < lake_compaction_score_upper_bound
        double compactionScore = threshold + 5;
        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 3, currentTimeMs,
                Quantiles.compute(Lists.newArrayList(compactionScore)));

        // Commit should be denied by lake_ingest_slowdown_threshold
        CommitRateExceededException e =
                Assert.assertThrows(CommitRateExceededException.class, () -> limiter.check(partitions, currentTimeMs));

        ThreadUtil.sleepAtLeastIgnoreInterrupts(e.getAllowCommitTime() - currentTimeMs);
        long newCurrentTimeMs = System.currentTimeMillis();

        // Update the compaction score, make it greater than the lake_compaction_score_upper_bound
        compactionScore = threshold + 15;
        compactionMgr.handleLoadingFinished(new PartitionIdentifier(dbId, tableId, partitionId), 4, newCurrentTimeMs,
                Quantiles.compute(Lists.newArrayList(compactionScore)));

        // This time commit should be denied by the lake_compaction_score_upper_bound
        CommitRateExceededException e2 = Assert.assertThrows(CommitRateExceededException.class,
                () -> limiter.check(partitions, newCurrentTimeMs));
        Assert.assertEquals(newCurrentTimeMs + 1000, e2.getAllowCommitTime());
    }
}
