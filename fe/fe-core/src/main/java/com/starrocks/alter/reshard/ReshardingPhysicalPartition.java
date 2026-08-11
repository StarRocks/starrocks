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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.Future;

/*
 * ReshardingPhysicalPartition saves the context during tablet splitting or merging for a physical partition
 */
public class ReshardingPhysicalPartition {
    private static final Logger LOG = LogManager.getLogger(ReshardingPhysicalPartition.class);

    @SerializedName(value = "physicalPartitionId")
    protected final long physicalPartitionId;

    @SerializedName(value = "reshardingIndexes")
    protected final Map<Long, ReshardingMaterializedIndex> reshardingIndexes;

    @SerializedName(value = "commitVersion")
    protected long commitVersion;

    protected Future<Map<Long, TabletRange>> publishFuture;

    // Reason this partition's last publish attempt failed, or null while it is healthy. Scoped to
    // the partition (not the job) so a partition that recovers stops reporting even while a sibling
    // partition is still retrying. Deliberately NOT serialized: a publish failure is always retried
    // and never terminal, so it must not reach the journal. volatile because runRunningJob writes it
    // from the reshard daemon while getInfo() reads it on an RPC thread, and the job stays in
    // RUNNING throughout, so there is no other happens-before edge to publish the write.
    protected transient volatile String publishFailureReason;

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
            return new PublishResult(PublishState.SUCCESS, tabletRanges);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted to future get. ", e);
            Thread.currentThread().interrupt();
            return new PublishResult(PublishState.IN_PROGRESS, null);
        } catch (Exception e) {
            LOG.warn("Failed to publish future get. ", e);
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            return new PublishResult(PublishState.FAILED, null, cause.getMessage());
        }
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (ReshardingMaterializedIndex reshardingMaterializedIndex : reshardingIndexes.values()) {
            parallelTablets += reshardingMaterializedIndex.getParallelTablets();
        }
        return parallelTablets;
    }
}
