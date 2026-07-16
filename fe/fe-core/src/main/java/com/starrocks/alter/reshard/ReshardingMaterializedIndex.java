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
import com.starrocks.catalog.MaterializedIndex;

import java.util.List;

/*
 * ReshardingMaterializedIndex saves the context during tablet splitting or merging for a materialized index
 */
public class ReshardingMaterializedIndex {

    @SerializedName(value = "materializedIndexId")
    protected final long materializedIndexId;

    @SerializedName(value = "materializedIndex")
    protected final MaterializedIndex materializedIndex;

    @SerializedName(value = "reshardingTablets")
    protected final List<ReshardingTablet> reshardingTablets;

    // Synthetic ids for the "virtual partition" that parks this superseded (old) materialized index in
    // the CatalogRecycleBin during reshard cleanup, so an in-flight query planned against it can keep
    // reading its tablets' metadata until the recycle retention expires (issue #75993 / split-read
    // race). Allocated once on the leader in removeOldMaterializedIndexes() and persisted here, so the
    // replay path reuses the same ids (never re-allocates) and rebuilds an identical recycle-bin entry.
    // -1 means "not yet recycled".
    @SerializedName(value = "recycledVirtualPartitionId")
    protected long recycledVirtualPartitionId = -1L;

    @SerializedName(value = "recycledVirtualPhysicalPartitionId")
    protected long recycledVirtualPhysicalPartitionId = -1L;

    public ReshardingMaterializedIndex(long materializedIndexId, MaterializedIndex materializedIndex,
            List<ReshardingTablet> reshardingTablets) {
        this.materializedIndexId = materializedIndexId;
        this.materializedIndex = materializedIndex;
        this.reshardingTablets = reshardingTablets;
    }

    public long getMaterializedIndexId() {
        return materializedIndexId;
    }

    public MaterializedIndex getMaterializedIndex() {
        return materializedIndex;
    }

    public List<ReshardingTablet> getReshardingTablets() {
        return reshardingTablets;
    }

    public long getRecycledVirtualPartitionId() {
        return recycledVirtualPartitionId;
    }

    public long getRecycledVirtualPhysicalPartitionId() {
        return recycledVirtualPhysicalPartitionId;
    }

    public void setRecycledVirtualPartitionIds(long partitionId, long physicalPartitionId) {
        this.recycledVirtualPartitionId = partitionId;
        this.recycledVirtualPhysicalPartitionId = physicalPartitionId;
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (ReshardingTablet reshardingTablet : reshardingTablets) {
            parallelTablets += reshardingTablet.getParallelTablets();
        }
        return parallelTablets;
    }
}
