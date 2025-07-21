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

import java.util.Map;

/*
 * PhysicalPartitionContext saves the context during tablet splitting or merging for a physical partition
 */
public class PhysicalPartitionContext {

    @SerializedName(value = "commitVersion")
    protected long commitVersion;

    @SerializedName(value = "indexContexts")
    protected final Map<Long, MaterializedIndexContext> indexContexts;

    public PhysicalPartitionContext(Map<Long, MaterializedIndexContext> indexContexts) {
        this.indexContexts = indexContexts;
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

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (MaterializedIndexContext indexContext : indexContexts.values()) {
            parallelTablets += indexContext.getParallelTablets();
        }
        return parallelTablets;
    }
}
