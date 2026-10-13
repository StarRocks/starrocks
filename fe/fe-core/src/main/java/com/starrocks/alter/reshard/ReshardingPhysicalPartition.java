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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.TabletRange;
import com.starrocks.metric.MetricRepo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.Future;

/*
 * ReshardingPhysicalPartition saves the context during tablet splitting or merging for a physical partition
 */
public class ReshardingPhysicalPartition {
    private static final Logger LOG = LogManager.getLogger(ReshardingPhysicalPartition.class);

    // Exponential backoff between publish attempts: 1s, 2s, 4s ... capped at 30s. A transient failure
    // (restarting node, brief RPC error) still recovers within seconds, while a deterministic one
    // settles at one attempt per PUBLISH_RETRY_MAX_INTERVAL_MS instead of tens per second. The cap is
    // kept at half a minute rather than a full one so a stuck publish keeps ticking
    // COUNTER_TABLET_RESHARD_PUBLISH_FAILED often enough for a rate-based alert to see it promptly.
    private static final long PUBLISH_RETRY_BASE_INTERVAL_MS = 1000L;
    private static final long PUBLISH_RETRY_MAX_INTERVAL_MS = 30000L;

    @SerializedName(value = "physicalPartitionId")
    protected final long physicalPartitionId;

    @SerializedName(value = "reshardingIndexes")
    protected final Map<Long, ReshardingMaterializedIndex> reshardingIndexes;

    @SerializedName(value = "commitVersion")
    protected long commitVersion;

    protected Future<Map<Long, TabletRange>> publishFuture;

    // Reason this partition's last publish attempt failed, cleared once it publishes or the partition
    // is dropped from the table (runRunningJob then skips it, so no publish result would). Scoped to
    // the partition (not the job) so a partition that recovers stops reporting even while a sibling
    // partition is still retrying. Deliberately NOT serialized: a publish failure is always retried
    // and never terminal, so it must not reach the journal. volatile because runRunningJob writes it
    // from the reshard daemon while getInfo() reads it on an RPC thread, and the job stays in
    // RUNNING throughout, so there is no other happens-before edge to publish the write.
    protected transient volatile String publishFailureReason;

    // Publish-retry pacing. A reshard publish is retried until it succeeds, because a merge / split
    // whose transaction is already committed has no rollback path. The retry therefore has to be paced:
    // resubmitting a deterministically failing publish on every daemon tick is an unbounded hot loop,
    // observed in the field at ~95 identical warn lines per second per job.
    // Not serialized, like publishFailureReason: a retry a leader change interrupts simply starts over
    // from the first interval.
    protected transient int consecutivePublishFailures;
    protected transient long nextPublishRetryTimeMs;
    // Whether the failure of the future currently held has already been counted, logged and turned into
    // a retry deadline. getPublishResult() is polled once per daemon tick and keeps reporting FAILED
    // for the same failed attempt, so without this the deadline would be pushed further out on every
    // poll and the retry would never come due -- the pacing would turn into a permanent block.
    protected transient boolean publishFailureAccounted;

    public ReshardingPhysicalPartition(long physicalPartitionId,
            Map<Long, ReshardingMaterializedIndex> reshardingIndexes) {
        this.physicalPartitionId = physicalPartitionId;
        this.reshardingIndexes = reshardingIndexes;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    public Map<Long, ReshardingMaterializedIndex> getReshardingIndexes() {
        return reshardingIndexes;
    }

    public void setCommitVersion(long commitVersion) {
        this.commitVersion = commitVersion;
    }

    public long getCommitVersion() {
        return commitVersion;
    }

    public void setPublishFuture(Future<Map<Long, TabletRange>> publishFuture) {
        this.publishFuture = publishFuture;
        // A new attempt: its failure, if it fails, is a new one to count and to back off from.
        this.publishFailureAccounted = false;
    }

    public void setPublishFailureReason(String publishFailureReason) {
        this.publishFailureReason = publishFailureReason;
    }

    public String getPublishFailureReason() {
        return publishFailureReason;
    }

    public enum PublishState {
        NOT_STARTED, // Publish not started
        IN_PROGRESS, // Publish in progress
        SUCCESS, // Publish success
        FAILED, // Publish failed
    }

    /**
     * {@code failureReason} is set only for {@link PublishState#FAILED} and carries the publish
     * error text, so a job that keeps retrying can surface why in its errorMessage instead of
     * sitting in RUNNING with nothing but a log line.
     */
    public static record PublishResult(
            PublishState publishState,
            Map<Long, TabletRange> tabletRanges,
            String failureReason) {
        public PublishResult(PublishState publishState, Map<Long, TabletRange> tabletRanges) {
            this(publishState, tabletRanges, null);
        }
    }

    public PublishResult getPublishResult() {
        if (publishFuture == null) {
            return new PublishResult(PublishState.NOT_STARTED, null);
        }

        if (!publishFuture.isDone()) {
            return new PublishResult(PublishState.IN_PROGRESS, null);
        }

        try {
            Map<Long, TabletRange> tabletRanges = publishFuture.get();
            // This attempt succeeded, so the failures before it were not consecutive after all: should
            // this partition ever be published again, it starts from the first interval.
            consecutivePublishFailures = 0;
            nextPublishRetryTimeMs = 0;
            return new PublishResult(PublishState.SUCCESS, tabletRanges);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted to future get. ", e);
            Thread.currentThread().interrupt();
            return new PublishResult(PublishState.IN_PROGRESS, null);
        } catch (Exception e) {
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            if (!publishFailureAccounted) {
                publishFailureAccounted = true;
                ++consecutivePublishFailures;
                long retryIntervalMs = publishRetryIntervalMs();
                nextPublishRetryTimeMs = System.currentTimeMillis() + retryIntervalMs;
                // One increment per failed attempt, so a stuck reshard is alertable without reading
                // fe.warn.log or polling information_schema.tablet_reshard_jobs. Counting it here rather
                // than per poll keeps the rate tracking attempts instead of daemon ticks.
                if (MetricRepo.hasInit) {
                    MetricRepo.COUNTER_TABLET_RESHARD_PUBLISH_FAILED.increase(1L);
                }
                // Logged once per attempt for the same reason: re-logging the same completed-exceptionally
                // future on every poll is what grew fe.warn.log to hundreds of MB in the field.
                LOG.warn("Failed to publish future get. Partition {} publish attempt {} failed, retry in {} ms. ",
                        physicalPartitionId, consecutivePublishFailures, retryIntervalMs, e);
            }
            return new PublishResult(PublishState.FAILED, null, cause.getMessage());
        }
    }

    private long publishRetryIntervalMs() {
        // Shift by at most 5: 1s << 5 is already past the cap, so a higher shift only risks overflow.
        int shift = Math.min(Math.max(consecutivePublishFailures - 1, 0), 5);
        return Math.min(PUBLISH_RETRY_BASE_INTERVAL_MS << shift, PUBLISH_RETRY_MAX_INTERVAL_MS);
    }

    /**
     * Whether a new publish attempt may be submitted now. Always true before the first attempt and
     * after a success; after a failure it stays false until the backoff computed above has elapsed.
     */
    public boolean isPublishRetryDue() {
        return System.currentTimeMillis() >= nextPublishRetryTimeMs;
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (ReshardingMaterializedIndex reshardingMaterializedIndex : reshardingIndexes.values()) {
            parallelTablets += reshardingMaterializedIndex.getParallelTablets();
        }
        return parallelTablets;
    }
}
