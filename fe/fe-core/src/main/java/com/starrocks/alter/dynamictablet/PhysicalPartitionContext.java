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

package com.starrocks.alter.dynamictablet;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.Future;

/*
 * PhysicalPartitionContext saves the context during tablet splitting or merging for a physical partition
 */
public class PhysicalPartitionContext {
    private static final Logger LOG = LogManager.getLogger(PhysicalPartitionContext.class);

    @SerializedName(value = "newPhysicalPartitionId")
    protected final long newPhysicalPartitionId;

    @SerializedName(value = "commitVersion")
    protected long commitVersion;

    @SerializedName(value = "indexContexts")
    protected final Map<Long, MaterializedIndexContext> indexContexts;

    protected Future<Boolean> publishFuture;

    public PhysicalPartitionContext(long newPhysicalPartitionId, Map<Long, MaterializedIndexContext> indexContexts) {
        this.newPhysicalPartitionId = newPhysicalPartitionId;
        this.indexContexts = indexContexts;
    }

    public long getNewPhysicalPartitionId() {
        return newPhysicalPartitionId;
    }

    public long getCommitVersion() {
        return commitVersion;
    }

    public void setCommitVersion(long commitVersion) {
        this.commitVersion = commitVersion;
    }

    public Map<Long, MaterializedIndexContext> getIndexContexts() {
        return indexContexts;
    }

    public void setPublishFuture(Future<Boolean> publishFuture) {
        this.publishFuture = publishFuture;
    }

    /*
     * < 0: Publish not start or failed
     * > 0: Publish success
     * = 0: Publish in progress
     */
    public int getPublishState() {
        if (publishFuture == null) {
            return -1;
        }

        if (!publishFuture.isDone()) {
            return 0;
        }

        try {
            return publishFuture.get() ? 1 : -1;
        } catch (InterruptedException e) {
            LOG.warn("Interrupted to publish future get. ", e);
            Thread.currentThread().interrupt();
            return 0;
        } catch (Exception e) {
            LOG.warn("Failed to publish future get. ", e);
            return -1;
        }
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (MaterializedIndexContext indexContext : indexContexts.values()) {
            parallelTablets += indexContext.getParallelTablets();
        }
        return parallelTablets;
    }
}
