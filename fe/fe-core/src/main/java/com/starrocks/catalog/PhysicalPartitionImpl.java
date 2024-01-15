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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.FeConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Physical Partition implementation
 */
public class PhysicalPartitionImpl extends MetaObject implements PhysicalPartition {
    private static final Logger LOG = LogManager.getLogger(PhysicalPartitionImpl.class);

    public static final long PARTITION_INIT_VERSION = 1L;

    @SerializedName(value = "id")
    private long id;

    private long beforeRestoreId;

    @SerializedName(value = "parentId")
    private long parentId;

    @SerializedName(value = "shardGroupId")
    private long shardGroupId;

    /* Physical Partition Member */
    @SerializedName(value = "isImmutable")
    private AtomicBoolean isImmutable = new AtomicBoolean(false);

    @SerializedName(value = "baseIndex")
    private MaterializedIndex baseIndex;
    /**
     * Visible rollup indexes are indexes which are visible to user.
     * User can do query on them, show them in related 'show' stmt.
     */
    @SerializedName(value = "idToVisibleRollupIndex")
    private Map<Long, MaterializedIndex> idToVisibleRollupIndex = Maps.newHashMap();
    /**
     * Shadow indexes are indexes which are not visible to user.
     * Query will not run on these shadow indexes, and user can not see them neither.
     * But load process will load data into these shadow indexes.
     */
    @SerializedName(value = "idToShadowIndex")
    private Map<Long, MaterializedIndex> idToShadowIndex = Maps.newHashMap();

    /**
     * committed version(hash): after txn is committed, set committed version(hash)
     * visible version(hash): after txn is published, set visible version
     * next version(hash): next version is set after finished committing, it should equal to committed version + 1
     */

    // not have committedVersion because committedVersion = nextVersion - 1
    @SerializedName(value = "visibleVersion")
    private long visibleVersion;
    @SerializedName(value = "visibleVersionTime")
    private long visibleVersionTime;
    @SerializedName(value = "nextVersion")
    private long nextVersion;
    /**
     * ID of the transaction that has committed current visible version.
     * Just for tracing the txn log, no need to persist.
     */
    private long visibleTxnId = -1;

    private volatile long lastVacuumTime = 0;

    private volatile long minRetainVersion = 0;

    public PhysicalPartitionImpl(long id, long parentId, long sharedGroupId, MaterializedIndex baseIndex) {
        this.id = id;
        this.parentId = parentId;
        this.baseIndex = baseIndex;
        this.visibleVersion = PARTITION_INIT_VERSION;
        this.visibleVersionTime = System.currentTimeMillis();
        this.nextVersion = PARTITION_INIT_VERSION + 1;
        this.shardGroupId = sharedGroupId;
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public void setIdForRestore(long id) {
        this.beforeRestoreId = this.id;
        this.id = id;
    }

    @Override
    public long getBeforeRestoreId() {
        return this.beforeRestoreId;
    }

    @Override
    public long getParentId() {
        return this.parentId;
    }

    @Override
    public void setParentId(long parentId) {
        this.parentId = parentId;
    }
 
    @Override
    public long getShardGroupId() {
        return this.shardGroupId;
    }

    @Override
    public void setImmutable(boolean isImmutable) {
        this.isImmutable.set(isImmutable);
    }

    @Override
    public boolean isImmutable() {
        return this.isImmutable.get();
    }

    @Override
    public long getLastVacuumTime() {
        return lastVacuumTime;
    }

    @Override
    public void setLastVacuumTime(long lastVacuumTime) {
        this.lastVacuumTime = lastVacuumTime;
    }

    @Override
    public long getMinRetainVersion() {
        return minRetainVersion;
    }

    @Override
    public void setMinRetainVersion(long minRetainVersion) {
        this.minRetainVersion = minRetainVersion;
    }

    /*
     * If a partition is overwritten by a restore job, we need to reset all version info to
     * the restored partition version info)
     */
    @Override
    public void updateVersionForRestore(long visibleVersion) {
        this.setVisibleVersion(visibleVersion, System.currentTimeMillis());
        this.nextVersion = this.visibleVersion + 1;
        LOG.info("update partition {} version for restore: visible: {}, next: {}",
                id, visibleVersion, nextVersion);
    }

    @Override
    public void updateVisibleVersion(long visibleVersion) {
        updateVisibleVersion(visibleVersion, System.currentTimeMillis());
    }

    @Override
    public void updateVisibleVersion(long visibleVersion, long visibleVersionTime) {
        this.setVisibleVersion(visibleVersion, visibleVersionTime);
    }

    @Override
    public void updateVisibleVersion(long visibleVersion, long visibleVersionTime, long visibleTxnId) {
        setVisibleVersion(visibleVersion, visibleVersionTime);
        this.visibleTxnId = visibleTxnId;
    }

    @Override
    public long getVisibleTxnId() {
        return visibleTxnId;
    }

    @Override
    public long getVisibleVersion() {
        return visibleVersion;
    }

    @Override
    public long getVisibleVersionTime() {
        return visibleVersionTime;
    }

    @Override
    public void setVisibleVersion(long visibleVersion, long visibleVersionTime) {
        this.visibleVersion = visibleVersion;
        this.visibleVersionTime = visibleVersionTime;
    }

    @Override
    public void createRollupIndex(MaterializedIndex mIndex) {
        if (mIndex.getState().isVisible()) {
            this.idToVisibleRollupIndex.put(mIndex.getId(), mIndex);
        } else {
            this.idToShadowIndex.put(mIndex.getId(), mIndex);
        }
    }

    @Override
    public MaterializedIndex deleteRollupIndex(long indexId) {
        if (this.idToVisibleRollupIndex.containsKey(indexId)) {
            return idToVisibleRollupIndex.remove(indexId);
        } else {
            return idToShadowIndex.remove(indexId);
        }
    }

    @Override
    public void setBaseIndex(MaterializedIndex baseIndex) {
        this.baseIndex = baseIndex;
    }

    @Override
    public MaterializedIndex getBaseIndex() {
        return baseIndex;
    }

    @Override
    public long getNextVersion() {
        return nextVersion;
    }

    @Override
    public void setNextVersion(long nextVersion) {
        this.nextVersion = nextVersion;
    }

    @Override
    public long getCommittedVersion() {
        return this.nextVersion - 1;
    }

    @Override
    public MaterializedIndex getIndex(long indexId) {
        if (baseIndex.getId() == indexId) {
            return baseIndex;
        }
        if (idToVisibleRollupIndex.containsKey(indexId)) {
            return idToVisibleRollupIndex.get(indexId);
        } else {
            return idToShadowIndex.get(indexId);
        }
    }

    @Override
    public List<MaterializedIndex> getMaterializedIndices(IndexExtState extState) {
        List<MaterializedIndex> indices = Lists.newArrayList();
        switch (extState) {
            case ALL:
                indices.add(baseIndex);
                indices.addAll(idToVisibleRollupIndex.values());
                indices.addAll(idToShadowIndex.values());
                break;
            case VISIBLE:
                indices.add(baseIndex);
                indices.addAll(idToVisibleRollupIndex.values());
                break;
            case SHADOW:
                indices.addAll(idToShadowIndex.values());
            default:
                break;
        }
        return indices;
    }

    @Override
    public long getTabletMaxDataSize() {
        long maxDataSize = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            maxDataSize = Math.max(maxDataSize, mIndex.getTabletMaxDataSize());
        }
        return maxDataSize;
    }

    @Override
    public long storageDataSize() {
        long dataSize = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            dataSize += mIndex.getDataSize();
        }
        return dataSize;
    }

    @Override
    public long storageRowCount() {
        long rowCount = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            rowCount += mIndex.getRowCount();
        }
        return rowCount;
    }

    @Override
    public long storageReplicaCount() {
        long replicaCount = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            replicaCount += mIndex.getReplicaCount();
        }
        return replicaCount;
    }

    @Override
    public boolean hasMaterializedView() {
        return !idToVisibleRollupIndex.isEmpty();
    }

    @Override
    public boolean hasStorageData() {
        // The fe unit test need to check the selected index id without any data.
        // So if set FeConstants.runningUnitTest, we can ensure that the number of partitions is not empty,
        // And the test case can continue to execute the logic of 'select best roll up'
        return ((visibleVersion != PARTITION_INIT_VERSION)
                || FeConstants.runningUnitTest);
    }

    @Override
    public boolean isFirstLoad() {
        return visibleVersion == PARTITION_INIT_VERSION + 1;
    }

    /*
     * Change the index' state from SHADOW to NORMAL
     * Also move it to idToVisibleRollupIndex if it is not the base index.
     */
    @Override
    public boolean visualiseShadowIndex(long shadowIndexId, boolean isBaseIndex) {
        MaterializedIndex shadowIdx = idToShadowIndex.remove(shadowIndexId);
        if (shadowIdx == null) {
            return false;
        }
        Preconditions.checkState(!idToVisibleRollupIndex.containsKey(shadowIndexId), shadowIndexId);
        shadowIdx.setState(IndexState.NORMAL);
        if (isBaseIndex) {
            baseIndex = shadowIdx;
        } else {
            idToVisibleRollupIndex.put(shadowIndexId, shadowIdx);
        }
        LOG.info("visualise the shadow index: {}", shadowIndexId);
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(visibleVersion, baseIndex);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PhysicalPartitionImpl)) {
            return false;
        }

        PhysicalPartitionImpl partition = (PhysicalPartitionImpl) obj;
        if (idToVisibleRollupIndex != partition.idToVisibleRollupIndex) {
            if (idToVisibleRollupIndex.size() != partition.idToVisibleRollupIndex.size()) {
                return false;
            }
            for (Entry<Long, MaterializedIndex> entry : idToVisibleRollupIndex.entrySet()) {
                long key = entry.getKey();
                if (!partition.idToVisibleRollupIndex.containsKey(key)) {
                    return false;
                }
                if (!entry.getValue().equals(partition.idToVisibleRollupIndex.get(key))) {
                    return false;
                }
            }
        }

        return (visibleVersion == partition.visibleVersion)
                && (baseIndex.equals(partition.baseIndex));
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("partitionId: ").append(id).append("; ");
        buffer.append("parentPartitionId: ").append(parentId).append("; ");
        buffer.append("shardGroupId: ").append(shardGroupId).append("; ");
        buffer.append("isImmutable: ").append(isImmutable()).append("; ");

        buffer.append("baseIndex: ").append(baseIndex.toString()).append("; ");

        int rollupCount = (idToVisibleRollupIndex != null) ? idToVisibleRollupIndex.size() : 0;
        buffer.append("rollupCount: ").append(rollupCount).append("; ");

        if (idToVisibleRollupIndex != null) {
            for (Map.Entry<Long, MaterializedIndex> entry : idToVisibleRollupIndex.entrySet()) {
                buffer.append("rollupIndex: ").append(entry.getValue().toString()).append("; ");
            }
        }

        buffer.append("visibleVersion: ").append(visibleVersion).append("; ");
        buffer.append("visibleVersionTime: ").append(visibleVersionTime).append("; ");
        buffer.append("committedVersion: ").append(getCommittedVersion()).append("; ");

        buffer.append("storageDataSize: ").append(storageDataSize()).append("; ");
        buffer.append("storageRowCount: ").append(storageRowCount()).append("; ");
        buffer.append("storageReplicaCount: ").append(storageReplicaCount()).append("; ");

        return buffer.toString();
    }
}
