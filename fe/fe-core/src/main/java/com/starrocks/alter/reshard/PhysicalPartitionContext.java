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
import com.starrocks.catalog.PhysicalPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.Future;

/*
 * PhysicalPartitionContext saves the context during tablet splitting or merging for a physical partition
 */
public class PhysicalPartitionContext {
    private static final Logger LOG = LogManager.getLogger(PhysicalPartitionContext.class);

    @SerializedName(value = "physicalPartition")
    protected final PhysicalPartition physicalPartition;

    @SerializedName(value = "reshardingTabletses")
    protected final Map<Long, ReshardingTablets> reshardingTabletses;

    @SerializedName(value = "commitVersion")
    protected long commitVersion;

    protected Future<Boolean> publishFuture;

    public PhysicalPartitionContext(PhysicalPartition physicalPartition,
            Map<Long, ReshardingTablets> reshardingTabletses) {
        this.physicalPartition = physicalPartition;
        this.reshardingTabletses = reshardingTabletses;
    }

    public PhysicalPartition getPhysicalPartition() {
        return physicalPartition;
    }

    public Map<Long, ReshardingTablets> getReshardingTabletses() {
        return reshardingTabletses;
    }

    public void setCommitVersion(long commitVersion) {
        this.commitVersion = commitVersion;
    }

    public long getCommitVersion() {
        return commitVersion;
    }

    public void setPublishFuture(Future<Boolean> publishFuture) {
        this.publishFuture = publishFuture;
    }

    public static enum PublishState {
        PUBLISH_NOT_STARTED, // Publish not started
        PUBLISH_IN_PROGRESS, // Publish in progress
        PUBLISH_SUCCESS, // Publish success
        PUBLISH_FAILED, // Publish failed
    }

    public PublishState getPublishState() {
        if (publishFuture == null) {
            return PublishState.PUBLISH_NOT_STARTED;
        }

        if (!publishFuture.isDone()) {
            return PublishState.PUBLISH_IN_PROGRESS;
        }

        try {
            return publishFuture.get() ? PublishState.PUBLISH_SUCCESS : PublishState.PUBLISH_FAILED;
        } catch (InterruptedException e) {
            LOG.warn("Interrupted to publish future get. ", e);
            Thread.currentThread().interrupt();
            return PublishState.PUBLISH_IN_PROGRESS;
        } catch (Exception e) {
            LOG.warn("Failed to publish future get. ", e);
            return PublishState.PUBLISH_FAILED;
        }
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (ReshardingTablets reshardingTablets : reshardingTabletses.values()) {
            parallelTablets += reshardingTablets.getParallelTablets();
        }
        return parallelTablets;
    }
}
