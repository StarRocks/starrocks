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

package com.starrocks.alter.reshard;

import com.starrocks.alter.reshard.ReshardingPhysicalPartition.PublishState;
import com.starrocks.catalog.TabletRange;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricRepo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Publish-retry pacing in ReshardingPhysicalPartition. A reshard publish has no rollback path once its
 * transaction is committed, so it is retried until it succeeds; these tests pin the pacing that keeps a
 * deterministic BE-side failure from becoming an unbounded hot loop.
 */
public class ReshardingPhysicalPartitionTest {

    /** Mirrors PUBLISH_RETRY_MAX_INTERVAL_MS, which is private to the class under test. */
    private static final long PUBLISH_RETRY_CAP_MS = 30000L;

    private static ReshardingPhysicalPartition newPartition() {
        return new ReshardingPhysicalPartition(1L, new HashMap<>());
    }

    private static CompletableFuture<Map<Long, TabletRange>> failedPublish(String message) {
        CompletableFuture<Map<Long, TabletRange>> future = new CompletableFuture<>();
        future.completeExceptionally(new TabletReshardException(message));
        return future;
    }

    @Test
    public void testNoAttemptYetIsNotStartedAndImmediatelyDue() {
        ReshardingPhysicalPartition partition = newPartition();

        Assertions.assertEquals(PublishState.NOT_STARTED, partition.getPublishResult().publishState());
        // Nothing to back off from, so the first attempt must be allowed right away.
        Assertions.assertTrue(partition.isPublishRetryDue());
    }

    @Test
    public void testInProgressWhileFutureIsPending() {
        ReshardingPhysicalPartition partition = newPartition();
        partition.setPublishFuture(new CompletableFuture<>());

        Assertions.assertEquals(PublishState.IN_PROGRESS, partition.getPublishResult().publishState());
        Assertions.assertTrue(partition.isPublishRetryDue());
    }

    @Test
    public void testFailureDefersTheNextAttemptAndCarriesItsReason() {
        ReshardingPhysicalPartition partition = newPartition();
        partition.setPublishFuture(failedPublish("Segment id overflow during tablet merge"));

        ReshardingPhysicalPartition.PublishResult result = partition.getPublishResult();
        Assertions.assertEquals(PublishState.FAILED, result.publishState());
        // The reason must be retrievable, not just logged: the job reports it as its ERROR_MESSAGE.
        Assertions.assertEquals("Segment id overflow during tablet merge", result.failureReason());
        // A resubmit is only allowed once the backoff elapses; without this the job resubmits the same
        // doomed publish on every daemon tick.
        Assertions.assertFalse(partition.isPublishRetryDue());
    }

    @Test
    public void testPollingAFailedAttemptDoesNotPushTheDeadlineOut() {
        ReshardingPhysicalPartition partition = newPartition();
        partition.setPublishFuture(failedPublish("boom"));

        Assertions.assertEquals(PublishState.FAILED, partition.getPublishResult().publishState());
        long deadlineMs = partition.nextPublishRetryTimeMs;

        // The job polls the same failed attempt once per daemon tick. Re-stamping the deadline on every
        // poll would push the retry forever out of reach, turning the backoff into a permanent block --
        // and re-counting the attempt would make the failure metric track ticks instead of attempts.
        for (int i = 0; i < 10; ++i) {
            ReshardingPhysicalPartition.PublishResult result = partition.getPublishResult();
            Assertions.assertEquals(PublishState.FAILED, result.publishState());
            Assertions.assertEquals("boom", result.failureReason());
        }
        Assertions.assertEquals(deadlineMs, partition.nextPublishRetryTimeMs);
        Assertions.assertEquals(1, partition.consecutivePublishFailures);
    }

    @Test
    public void testBackoffGrowsWithConsecutiveFailures() {
        ReshardingPhysicalPartition partition = newPartition();

        long previousDelayMs = -1;
        for (int attempt = 1; attempt <= 4; ++attempt) {
            partition.setPublishFuture(failedPublish("boom " + attempt));
            long before = System.currentTimeMillis();
            Assertions.assertEquals(PublishState.FAILED, partition.getPublishResult().publishState());
            long delayMs = partition.nextPublishRetryTimeMs - before;
            Assertions.assertTrue(delayMs > previousDelayMs,
                    "attempt " + attempt + " delay " + delayMs + " should exceed previous " + previousDelayMs);
            previousDelayMs = delayMs;
        }
    }

    @Test
    public void testBackoffIsCappedAtThirtySeconds() {
        ReshardingPhysicalPartition partition = newPartition();

        // Doubling from 1s would pass 30s at the sixth attempt, so run well past it: a deterministic
        // failure must settle at one attempt per 30s, not drift towards minutes between attempts.
        for (int attempt = 1; attempt <= 12; ++attempt) {
            partition.setPublishFuture(failedPublish("boom " + attempt));
            long before = System.currentTimeMillis();
            Assertions.assertEquals(PublishState.FAILED, partition.getPublishResult().publishState());
            long after = System.currentTimeMillis();
            // The deadline is stamped somewhere in [before, after], so the interval it encodes is at
            // most (deadline - before) and at least (deadline - after) regardless of scheduling noise.
            Assertions.assertTrue(partition.nextPublishRetryTimeMs - after <= PUBLISH_RETRY_CAP_MS,
                    "attempt " + attempt + " must not back off beyond " + PUBLISH_RETRY_CAP_MS + " ms");
            if (attempt >= 6) {
                Assertions.assertTrue(partition.nextPublishRetryTimeMs - before >= PUBLISH_RETRY_CAP_MS,
                        "attempt " + attempt + " must have reached the " + PUBLISH_RETRY_CAP_MS + " ms cap");
            }
        }
    }

    @Test
    public void testFailureBumpsThePublishFailedCounter() {
        boolean savedHasInit = MetricRepo.hasInit;
        LongCounterMetric savedCounter = MetricRepo.COUNTER_TABLET_RESHARD_PUBLISH_FAILED;
        // The counter is only assigned by MetricRepo.init(), which a plain unit test does not run.
        MetricRepo.COUNTER_TABLET_RESHARD_PUBLISH_FAILED = new LongCounterMetric(
                "tablet_reshard_publish_failed", MetricUnit.REQUESTS, "test");
        MetricRepo.hasInit = true;
        try {
            ReshardingPhysicalPartition partition = newPartition();
            partition.setPublishFuture(failedPublish("boom"));
            Assertions.assertEquals(PublishState.FAILED, partition.getPublishResult().publishState());
            Assertions.assertEquals(1L,
                    MetricRepo.COUNTER_TABLET_RESHARD_PUBLISH_FAILED.getValue().longValue());

            // Polling an already-counted failure must not inflate the counter: the rate has to track
            // attempts, otherwise it reads as an escalating failure while the partition is merely
            // waiting out its backoff.
            for (int i = 0; i < 5; ++i) {
                Assertions.assertEquals(PublishState.FAILED, partition.getPublishResult().publishState());
            }
            Assertions.assertEquals(1L,
                    MetricRepo.COUNTER_TABLET_RESHARD_PUBLISH_FAILED.getValue().longValue());

            // A genuinely new attempt is counted again.
            partition.setPublishFuture(failedPublish("boom again"));
            Assertions.assertEquals(PublishState.FAILED, partition.getPublishResult().publishState());
            Assertions.assertEquals(2L,
                    MetricRepo.COUNTER_TABLET_RESHARD_PUBLISH_FAILED.getValue().longValue());
        } finally {
            MetricRepo.hasInit = savedHasInit;
            MetricRepo.COUNTER_TABLET_RESHARD_PUBLISH_FAILED = savedCounter;
        }
    }

    @Test
    public void testSuccessResetsThePacing() {
        ReshardingPhysicalPartition partition = newPartition();
        partition.setPublishFuture(failedPublish("boom"));
        Assertions.assertEquals(PublishState.FAILED, partition.getPublishResult().publishState());
        Assertions.assertFalse(partition.isPublishRetryDue());

        Map<Long, TabletRange> ranges = new HashMap<>();
        partition.setPublishFuture(CompletableFuture.completedFuture(ranges));

        Assertions.assertEquals(PublishState.SUCCESS, partition.getPublishResult().publishState());
        // The failures before a success were not consecutive after all, and no deadline may outlive the
        // attempt it was meant to defer.
        Assertions.assertEquals(0, partition.consecutivePublishFailures);
        Assertions.assertTrue(partition.isPublishRetryDue());
        // Success stays readable across polls: the job re-reads tabletRanges while other partitions run.
        Assertions.assertEquals(PublishState.SUCCESS, partition.getPublishResult().publishState());
    }
}
