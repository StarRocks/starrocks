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

package com.starrocks.catalog;

import com.starrocks.catalog.MaterializedIndex.IndexExtState;

import java.util.List;

/* 
 * PhysicalPartition is the interface that describes the physical storage of a partition.
 * It includes version information and one or more MaterializedIndexes.
 * Each MaterializedIndex contains multiple tablets.
 */
public interface PhysicalPartition {

    // partition id which contains this physical partition
    public long getParentId();

    // physical partition id
    public long getId();

    // version interface

    public void updateVersionForRestore(long visibleVersion);
    public void updateVisibleVersion(long visibleVersion);
    public void updateVisibleVersion(long visibleVersion, long visibleVersionTime);
    public long getVisibleVersion();
    public long getVisibleVersionTime();
    public void setVisibleVersion(long visibleVersion, long visibleVersionTime);
    public long getNextVersion();
    public void setNextVersion(long nextVersion);
    public long getCommittedVersion();

    // materialized index interface

    public void createRollupIndex(MaterializedIndex mIndex);
    public MaterializedIndex deleteRollupIndex(long indexId);
    public void setBaseIndex(MaterializedIndex baseIndex);
    public MaterializedIndex getBaseIndex();
    public MaterializedIndex getIndex(long indexId);
    public List<MaterializedIndex> getMaterializedIndices(IndexExtState extState);
    /*
     * Change the index' state from SHADOW to NORMAL
     */
    public boolean visualiseShadowIndex(long shadowIndexId, boolean isBaseIndex);

    // statistic interface

    // partition data size reported by be, but may be not accurate
    public long storageDataSize();
    // partition row count reported by be, but may be not accurate
    public long storageRowCount();
    // partition replica count, it's accurate 
    public long storageReplicaCount();
    // has data judge by fe version, it's accurate
    public boolean hasStorageData();
    public boolean hasMaterializedView();
    public boolean isFirstLoad();
}
