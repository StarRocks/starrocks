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

    public enum PublishState {
        NOT_STARTED, // Publish not started
        IN_PROGRESS, // Publish in progress
        SUCCESS, // Publish success
        FAILED, // Publish failed
    }

    public static record PublishResult(
            PublishState publishState,
            Map<Long, TabletRange> tabletRanges) {
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
            return new PublishResult(PublishState.FAILED, null);
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
