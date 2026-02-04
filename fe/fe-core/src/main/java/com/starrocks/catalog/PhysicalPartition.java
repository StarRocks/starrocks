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
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.TransactionType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Physical Partition implementation
 */
public class PhysicalPartition extends MetaObject implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(PhysicalPartition.class);

    public static final long PARTITION_INIT_VERSION = 1L;

    public static final long INVALID_SHARD_GROUP_ID = -1L;

    @SerializedName(value = "id")
    private long id;

    private long beforeRestoreId;

    @SerializedName(value = "parentId")
    private long parentId;

    @SerializedName(value = "shardGroupId")
    private long shardGroupId = INVALID_SHARD_GROUP_ID;

    /* Physical Partition Member */
    @SerializedName(value = "isImmutable")
    private AtomicBoolean isImmutable = new AtomicBoolean(false);

    /**
     * Deprecated, use baseIndexMetaId and indexMetaIdToIndexIds instead.
     *
     * indexMetaIdToIndexIds.get(baseIndexMetaId).getLast() is the latest version of base index.
     */
    @Deprecated
    @SerializedName(value = "baseIndex")
    private MaterializedIndex baseIndex;

    @SerializedName(value = "baseIndexMetaId")
    private long baseIndexMetaId = -1L;

    /**
     * Support multi-version materialized indexes for tablet split.
     * A single index meta may correspond to multiple materialized indexes in tablet split process.
     *
     * index meta id -> List<index id>
     */
    @SerializedName(value = "indexMetaIdToIndexIds")
    private Map<Long, List<Long>> indexMetaIdToIndexIds = Maps.newHashMap();

    /**
     * Visible indexes are indexes which are visible to user.
     * User can do query on them, show them in related 'show' stmt.
     *
     * Visible indexes = base index + rollup indexes
     *
     * Move baseIndex to idToVisibleIndex for better management.
     * Not change the SerializedName for compatibility.
     */
    @SerializedName(value = "idToVisibleRollupIndex")
    private Map<Long, MaterializedIndex> idToVisibleIndex = Maps.newHashMap();
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

    @SerializedName(value = "dataVersion")
    private long dataVersion;
    @SerializedName(value = "nextDataVersion")
    private long nextDataVersion;

    @SerializedName(value = "versionEpoch")
    private long versionEpoch;
    @SerializedName(value = "versionTxnType")
    private TransactionType versionTxnType;

    /**
     * metadataSwitchVersion is non-zero: The metadata format differs before and after this version.
     * Therefore, vacuum operations cannot be executed across `metadataSwitchVersion`
     * e.g.
     *  the tablet under this partition has five versions: 1, 2, 3, 4, 5 and the metadataSwitchVersion is 3
     *  the vacuum is run as the following:
     *      1. Set minRetainVersion to 3 in the vacuumRequest.
     *      2. The BE will delete tablet metadata where the version is less than 3.
     *      3. If all tablets execute successfully and there are no metadata entries in two different formats, 
     *      set metadataSwitchVersion to 0.
     */
    @SerializedName(value = "metadataSwitchVersion")
    private long metadataSwitchVersion;
    /**
     * ID of the transaction that has committed current visible version.
     * Just for tracing the txn log, no need to persist.
     */
    private long visibleTxnId = -1;

    // Autovacuum
    private final AtomicLong lastVacuumTime = new AtomicLong(0);

    // Full vacuum (orphan data files and redundant db/table/partition)
    private volatile long lastFullVacuumTime;

    private final AtomicLong minRetainVersion = new AtomicLong(0);

    private final AtomicLong lastSuccVacuumVersion = new AtomicLong(0);

    @SerializedName(value = "bucketNum")
    private int bucketNum = 0;
    
    private final AtomicLong extraFileSize = new AtomicLong(0);

    private PhysicalPartition() {

    }

    public PhysicalPartition(long id, long parentId, MaterializedIndex baseIndex) {
        this.id = id;
        this.parentId = parentId;
        this.baseIndexMetaId = baseIndex.getMetaId();
        this.indexMetaIdToIndexIds.put(baseIndex.getMetaId(), Lists.newArrayList(baseIndex.getId()));
        this.idToVisibleIndex.put(baseIndex.getId(), baseIndex);
        this.visibleVersion = PARTITION_INIT_VERSION;
        this.visibleVersionTime = System.currentTimeMillis();
        this.nextVersion = this.visibleVersion + 1;
        this.dataVersion = this.visibleVersion;
        this.nextDataVersion = this.nextVersion;
        this.versionEpoch = this.nextVersionEpoch();
        this.versionTxnType = TransactionType.TXN_NORMAL;
    }

    // for external olap table
    public PhysicalPartition(long id, long parentId) {
        this.id = id;
        this.parentId = parentId;
        this.visibleVersion = PARTITION_INIT_VERSION;
        this.visibleVersionTime = System.currentTimeMillis();
        this.nextVersion = this.visibleVersion + 1;
        this.dataVersion = this.visibleVersion;
        this.nextDataVersion = this.nextVersion;
        this.versionEpoch = this.nextVersionEpoch();
        this.versionTxnType = TransactionType.TXN_NORMAL;
    }

    public long getId() {
        return this.id;
    }

    public void setIdForRestore(long id) {
        this.beforeRestoreId = this.id;
        this.id = id;
    }

    public long getBeforeRestoreId() {
        return this.beforeRestoreId;
    }

    public long getParentId() {
        return this.parentId;
    }

    public void setParentId(long parentId) {
        this.parentId = parentId;
    }

    public long getShardGroupId() {
        return this.shardGroupId;
    }

    public List<Long> getShardGroupIds() {
        Set<Long> result = Sets.newHashSet();
        for (List<Long> indexIds : indexMetaIdToIndexIds.values()) {
            for (long indexId : indexIds) {
                MaterializedIndex index = idToVisibleIndex.get(indexId);
                if (index != null) {
                    result.add(index.getShardGroupId());
                    continue;
                }

                index = idToShadowIndex.get(indexId);
                if (index != null) {
                    result.add(index.getShardGroupId());
                }
            }
        }
        return Lists.newArrayList(result);
    }

    public void setShardGroupId(Long shardGroupId) {
        this.shardGroupId = shardGroupId;
    }

    public void setImmutable(boolean isImmutable) {
        this.isImmutable.set(isImmutable);
    }

    public boolean isImmutable() {
        return this.isImmutable.get();
    }

    public long getLastVacuumTime() {
        return lastVacuumTime.get();
    }

    public void setLastVacuumTime(long lastVacuumTime) {
        this.lastVacuumTime.set(lastVacuumTime);
    }

    public long getLastFullVacuumTime() {
        return lastFullVacuumTime;
    }

    public void setLastFullVacuumTime(long lastVacuumTime) {
        this.lastFullVacuumTime = lastVacuumTime;
    }

    public long getMinRetainVersion() {
        long retainVersion = minRetainVersion.get();
        if (metadataSwitchVersion != 0) {
            if (retainVersion != 0) {
                retainVersion = Math.min(retainVersion, metadataSwitchVersion);
            } else {
                retainVersion = metadataSwitchVersion;
            }
        }
        return retainVersion;
    }

    public void setMinRetainVersion(long minRetainVersion) {
        this.minRetainVersion.set(minRetainVersion);
    }

    public long getLastSuccVacuumVersion() {
        return lastSuccVacuumVersion.get();
    }

    public void setLastSuccVacuumVersion(long lastSuccVacuumVersion) {
        this.lastSuccVacuumVersion.set(lastSuccVacuumVersion);
    }

    public long getExtraFileSize() {
        return extraFileSize.get();
    }

    public void setExtraFileSize(long extraFileSize) {
        this.extraFileSize.set(extraFileSize);
    }

    public void incExtraFileSize(long addFileSize) {
        this.extraFileSize.addAndGet(addFileSize);
    }

    /*
     * If a partition is overwritten by a restore job, we need to reset all version info to
     * the restored partition version info)
     */

    public void updateVersionForRestore(long visibleVersion) {
        this.setVisibleVersion(visibleVersion, System.currentTimeMillis());
        this.nextVersion = this.visibleVersion + 1;
        LOG.info("update partition {} version for restore: visible: {}, next: {}",
                id, visibleVersion, nextVersion);
    }

    public void updateVisibleVersion(long visibleVersion) {
        updateVisibleVersion(visibleVersion, System.currentTimeMillis());
    }

    public void updateVisibleVersion(long visibleVersion, long visibleVersionTime) {
        this.setVisibleVersion(visibleVersion, visibleVersionTime);
    }

    public void updateVisibleVersion(long visibleVersion, long visibleVersionTime, long visibleTxnId) {
        setVisibleVersion(visibleVersion, visibleVersionTime);
        this.visibleTxnId = visibleTxnId;
    }

    public long getVisibleTxnId() {
        return visibleTxnId;
    }

    public long getVisibleVersion() {
        return visibleVersion;
    }

    public long getVisibleVersionTime() {
        return visibleVersionTime;
    }

    public void setVisibleVersion(long visibleVersion, long visibleVersionTime) {
        this.visibleVersion = visibleVersion;
        this.visibleVersionTime = visibleVersionTime;
    }

    public void setBaseIndex(MaterializedIndex baseIndex) {
        Preconditions.checkState(!indexMetaIdToIndexIds.containsKey(baseIndex.getMetaId()),
                String.format("base index meta id %d already exists", baseIndex.getMetaId()));
        Preconditions.checkState(!idToVisibleIndex.containsKey(baseIndex.getId()),
                String.format("base index id %d already exists", baseIndex.getId()));

        baseIndexMetaId = baseIndex.getMetaId();
        indexMetaIdToIndexIds.put(baseIndex.getMetaId(), Lists.newArrayList(baseIndex.getId()));
        idToVisibleIndex.put(baseIndex.getId(), baseIndex);
    }

    public MaterializedIndex getLatestBaseIndex() {
        List<Long> indexIds = indexMetaIdToIndexIds.get(baseIndexMetaId);
        Preconditions.checkState(indexIds != null && !indexIds.isEmpty(),
                String.format("base index meta id %d not exist or index list is empty", baseIndexMetaId));
        return idToVisibleIndex.get(indexIds.get(indexIds.size() - 1));
    }

    public List<MaterializedIndex> getBaseIndices() {
        List<Long> indexIds = indexMetaIdToIndexIds.get(baseIndexMetaId);
        Preconditions.checkState(indexIds != null && !indexIds.isEmpty(),
                String.format("base index meta id %d not exist or index list is empty", baseIndexMetaId));
        List<MaterializedIndex> indices = Lists.newArrayList();
        for (Long indexId : indexIds) {
            MaterializedIndex index = idToVisibleIndex.get(indexId);
            Preconditions.checkState(index != null, String.format("base index id %d not exist", indexId));
            indices.add(index);
        }
        return indices;
    }

    // Create new rollup index.
    // 1. indexMetaIdToIndexIds currently does not contain mIndex.metaId.
    public void createRollupIndex(MaterializedIndex mIndex) {
        Preconditions.checkState(!indexMetaIdToIndexIds.containsKey(mIndex.getMetaId()),
                String.format("index meta id %d already exists", mIndex.getMetaId()));
        Preconditions.checkState(!idToVisibleIndex.containsKey(mIndex.getId()) && !idToShadowIndex.containsKey(mIndex.getId()),
                String.format("index id %d already exists", mIndex.getId()));

        indexMetaIdToIndexIds.put(mIndex.getMetaId(), Lists.newArrayList(mIndex.getId()));
        if (mIndex.getState().isVisible()) {
            idToVisibleIndex.put(mIndex.getId(), mIndex);
        } else {
            idToShadowIndex.put(mIndex.getId(), mIndex);
        }
    }

    // Add new version base or rollup materialized index.
    // 1. mIndex.state is NORMAL.
    // 2. indexMetaIdToIndexIds currently contains mIndex.metaId.
    public void addMaterializedIndex(MaterializedIndex mIndex, boolean isBaseIndex) {
        Preconditions.checkState(indexMetaIdToIndexIds.containsKey(mIndex.getMetaId()),
                String.format("index meta id %d not exist", mIndex.getMetaId()));
        Preconditions.checkState(!idToVisibleIndex.containsKey(mIndex.getId()) && !idToShadowIndex.containsKey(mIndex.getId()),
                String.format("index id %d already exists", mIndex.getId()));
        Preconditions.checkState(!isBaseIndex || mIndex.getMetaId() == baseIndexMetaId,
                String.format("index meta id %d not match baseIndexMetaId %d", mIndex.getMetaId(), baseIndexMetaId));
        Preconditions.checkState(mIndex.getState() == IndexState.NORMAL,
                String.format("index state %s is not NORMAL", mIndex.getState()));

        indexMetaIdToIndexIds.get(mIndex.getMetaId()).add(mIndex.getId());
        idToVisibleIndex.put(mIndex.getId(), mIndex);
    }

    public MaterializedIndex deleteMaterializedIndexByIndexId(long indexId) {
        MaterializedIndex index = idToVisibleIndex.remove(indexId);
        if (index == null) {
            index = idToShadowIndex.remove(indexId);
        }

        if (index != null) {
            List<Long> indexIds = indexMetaIdToIndexIds.get(index.getMetaId());
            Preconditions.checkState(indexIds != null && indexIds.remove(indexId),
                    String.format("index id %d not found in indexMetaIdToIndexIds", indexId));

            if (indexIds.isEmpty()) {
                indexMetaIdToIndexIds.remove(index.getMetaId());
            }
        }

        return index;
    }

    public List<MaterializedIndex> deleteMaterializedIndexByMetaId(long indexMetaId) {
        List<MaterializedIndex> indices = Lists.newArrayList();
        List<Long> indexIds = indexMetaIdToIndexIds.remove(indexMetaId);
        if (indexIds != null) {
            for (long indexId : indexIds) {
                MaterializedIndex index = idToVisibleIndex.remove(indexId);
                if (index != null) {
                    indices.add(index);
                    continue;
                }

                index = idToShadowIndex.remove(indexId);
                if (index != null) {
                    indices.add(index);
                }
            }
        }
        return indices;
    }

    public long getNextVersion() {
        return nextVersion;
    }

    public void setNextVersion(long nextVersion) {
        this.nextVersion = nextVersion;
    }

    public long getCommittedVersion() {
        return this.nextVersion - 1;
    }

    public long getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(long dataVersion) {
        this.dataVersion = dataVersion;
    }

    public long getNextDataVersion() {
        return nextDataVersion;
    }

    public void setNextDataVersion(long nextDataVersion) {
        this.nextDataVersion = nextDataVersion;
    }

    public long getCommittedDataVersion() {
        return this.nextDataVersion - 1;
    }

    public long getVersionEpoch() {
        return versionEpoch;
    }

    public void setVersionEpoch(long versionEpoch) {
        this.versionEpoch = versionEpoch;
    }

    public long nextVersionEpoch() {
        return GlobalStateMgr.getCurrentState().getGtidGenerator().nextGtid();
    }

    public TransactionType getVersionTxnType() {
        return versionTxnType;
    }

    public void setVersionTxnType(TransactionType versionTxnType) {
        this.versionTxnType = versionTxnType;
    }

    public long getMetadataSwitchVersion() {
        return metadataSwitchVersion;
    }

    public void setMetadataSwitchVersion(long metadataSwitchVersion) {
        this.metadataSwitchVersion = metadataSwitchVersion;
    }

    public boolean isTabletBalanced() {
        for (MaterializedIndex index : getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
            if (!index.isTabletBalanced()) {
                return false;
            }
        }
        return true;
    }

    public MaterializedIndex getLatestIndex(long indexMetaId) {
        List<Long> indexIds = indexMetaIdToIndexIds.get(indexMetaId);
        if (indexIds == null || indexIds.isEmpty()) {
            return null;
        }
        return getIndex(indexIds.get(indexIds.size() - 1));
    }

    public MaterializedIndex getIndex(long indexId) {
        MaterializedIndex index = idToVisibleIndex.get(indexId);
        if (index != null) {
            return index;
        } else {
            return idToShadowIndex.get(indexId);
        }
    }

    private List<MaterializedIndex> getLatestVisibleIndices() {
        List<MaterializedIndex> indices = Lists.newArrayList();
        for (Map.Entry<Long, List<Long>> entry : indexMetaIdToIndexIds.entrySet()) {
            List<Long> indexIds = entry.getValue();
            Preconditions.checkState(!indexIds.isEmpty(), String.format("index list is empty. meta id: %d", entry.getKey()));
            long indexId = indexIds.get(indexIds.size() - 1);
            MaterializedIndex index = idToVisibleIndex.get(indexId);
            if (index != null) {
                indices.add(index);
            }
        }
        return indices;
    }

    private List<MaterializedIndex> getLatestShadowIndices() {
        List<MaterializedIndex> indices = Lists.newArrayList();
        for (Map.Entry<Long, List<Long>> entry : indexMetaIdToIndexIds.entrySet()) {
            List<Long> indexIds = entry.getValue();
            Preconditions.checkState(!indexIds.isEmpty(), String.format("index list is empty. meta id: %d", entry.getKey()));
            long indexId = indexIds.get(indexIds.size() - 1);
            MaterializedIndex index = idToShadowIndex.get(indexId);
            if (index != null) {
                indices.add(index);
            }
        }
        return indices;
    }

    public List<MaterializedIndex> getLatestMaterializedIndices(IndexExtState extState) {
        List<MaterializedIndex> indices = Lists.newArrayList();
        switch (extState) {
            case ALL:
                indices.addAll(getLatestVisibleIndices());
                indices.addAll(getLatestShadowIndices());
                break;
            case VISIBLE:
                indices.addAll(getLatestVisibleIndices());
                break;
            case SHADOW:
                indices.addAll(getLatestShadowIndices());
                break;
            default:
                break;
        }
        return indices;
    }

    public List<MaterializedIndex> getAllMaterializedIndices(IndexExtState extState) {
        List<MaterializedIndex> indices = Lists.newArrayList();
        switch (extState) {
            case ALL:
                indices.addAll(idToVisibleIndex.values());
                indices.addAll(idToShadowIndex.values());
                break;
            case VISIBLE:
                indices.addAll(idToVisibleIndex.values());
                break;
            case SHADOW:
                indices.addAll(idToShadowIndex.values());
                break;
            default:
                break;
        }
        return indices;
    }

    public long getTabletMaxDataSize() {
        long maxDataSize = 0;
        for (MaterializedIndex mIndex : getLatestVisibleIndices()) {
            maxDataSize = Math.max(maxDataSize, mIndex.getTabletMaxDataSize());
        }
        return maxDataSize;
    }

    public long storageDataSize() {
        long dataSize = 0;
        for (MaterializedIndex mIndex : getLatestVisibleIndices()) {
            dataSize += mIndex.getDataSize();
        }
        return dataSize;
    }

    public long storageRowCount() {
        long rowCount = 0;
        for (MaterializedIndex mIndex : getLatestVisibleIndices()) {
            rowCount += mIndex.getRowCount();
        }
        return rowCount;
    }

    public long storageReplicaCount() {
        long replicaCount = 0;
        for (MaterializedIndex mIndex : getLatestVisibleIndices()) {
            replicaCount += mIndex.getReplicaCount();
        }
        return replicaCount;
    }

    public boolean hasMaterializedView() {
        List<Long> baseIndexIds = indexMetaIdToIndexIds.get(baseIndexMetaId);
        Preconditions.checkState(baseIndexIds != null && !baseIndexIds.isEmpty(),
                String.format("base index meta id %d not exist or index list is empty", baseIndexMetaId));
        Set<Long> visibleIndexIds = Sets.newHashSet(idToVisibleIndex.keySet());
        visibleIndexIds.removeAll(baseIndexIds);
        return !visibleIndexIds.isEmpty();
    }

    public boolean hasStorageData() {
        // The fe unit test need to check the selected index id without any data.
        // So if set FeConstants.runningUnitTest, we can ensure that the number of partitions is not empty,
        // And the test case can continue to execute the logic of 'select best roll up'
        return ((visibleVersion != PARTITION_INIT_VERSION)
                || FeConstants.runningUnitTest);
    }

    public boolean isFirstLoad() {
        return visibleVersion == PARTITION_INIT_VERSION + 1;
    }

    /*
     * Change the index' state from SHADOW to NORMAL
     * Also move it to idToVisibleRollupIndex if it is not the base index.
     */

    public boolean visualiseShadowIndex(long shadowIndexId, boolean isBaseIndex) {
        MaterializedIndex shadowIdx = idToShadowIndex.remove(shadowIndexId);
        if (shadowIdx == null) {
            return false;
        }
        Preconditions.checkState(!idToVisibleIndex.containsKey(shadowIndexId),
                String.format("index id %d already exists", shadowIndexId));
        shadowIdx.setState(IndexState.NORMAL);
        if (isBaseIndex) {
            // in shared-data cluster, if upgraded from 3.3 or older version, `shardGroupId` will not
            // be set, so must set it here
            if (shadowIdx.getShardGroupId() == PhysicalPartition.INVALID_SHARD_GROUP_ID) {
                shadowIdx.setShardGroupId(shardGroupId);
            }
            baseIndexMetaId = shadowIdx.getMetaId();
        }
        indexMetaIdToIndexIds.put(shadowIdx.getMetaId(), Lists.newArrayList(shadowIndexId));
        idToVisibleIndex.put(shadowIndexId, shadowIdx);
        LOG.info("visualise the shadow index: {}", shadowIndexId);
        return true;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, parentId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PhysicalPartition)) {
            return false;
        }

        PhysicalPartition partition = (PhysicalPartition) obj;
        return id == partition.id && parentId == partition.parentId;
    }

    @Override
    public String toString() {
        List<MaterializedIndex> baseIndices = Lists.newArrayList();
        List<MaterializedIndex> rollupIndices = Lists.newArrayList();
        for (Map.Entry<Long, List<Long>> entry : indexMetaIdToIndexIds.entrySet()) {
            long indexMetaId = entry.getKey();
            List<Long> indexIds = entry.getValue();
            List<MaterializedIndex> indices = indexMetaId == baseIndexMetaId ? baseIndices : rollupIndices;
            for (Long indexId : indexIds) {
                MaterializedIndex index = idToVisibleIndex.get(indexId);
                if (index != null) {
                    indices.add(index);
                }
            }
        }

        StringBuilder buffer = new StringBuilder();
        buffer.append("partitionId: ").append(id).append("; ");
        buffer.append("parentPartitionId: ").append(parentId).append("; ");
        buffer.append("shardGroupId: ").append(shardGroupId).append("; ");
        buffer.append("isImmutable: ").append(isImmutable()).append("; ");

        buffer.append("baseIndex: ").append(baseIndices).append("; ");

        buffer.append("rollupCount: ").append(rollupIndices.size()).append("; ");

        if (!rollupIndices.isEmpty()) {
            for (MaterializedIndex index : rollupIndices) {
                buffer.append("rollupIndex: ").append(index.toString()).append("; ");
            }
        }

        buffer.append("visibleVersion: ").append(visibleVersion).append("; ");
        buffer.append("visibleVersionTime: ").append(visibleVersionTime).append("; ");
        buffer.append("committedVersion: ").append(getCommittedVersion()).append("; ");

        buffer.append("dataVersion: ").append(dataVersion).append("; ");
        buffer.append("committedDataVersion: ").append(getCommittedDataVersion()).append("; ");

        buffer.append("versionEpoch: ").append(versionEpoch).append("; ");
        buffer.append("versionTxnType: ").append(versionTxnType).append("; ");

        buffer.append("storageDataSize: ").append(storageDataSize()).append("; ");
        buffer.append("storageRowCount: ").append(storageRowCount()).append("; ");
        buffer.append("storageReplicaCount: ").append(storageReplicaCount()).append("; ");
        buffer.append("bucketNum: ").append(bucketNum).append("; ");

        return buffer.toString();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (dataVersion == 0) {
            dataVersion = visibleVersion;
        }
        if (nextDataVersion == 0) {
            nextDataVersion = nextVersion;
        }
        if (versionEpoch == 0) {
            versionEpoch = nextVersionEpoch();
        }
        if (versionTxnType == null) {
            versionTxnType = TransactionType.TXN_NORMAL;
        }

        if (baseIndexMetaId == -1L) {
            Preconditions.checkState(indexMetaIdToIndexIds.isEmpty());
            Preconditions.checkNotNull(baseIndex);

            // add base index into idToVisibleIndex
            idToVisibleIndex.put(baseIndex.getId(), baseIndex);

            // fill indexMetaIdToIndexIds
            for (MaterializedIndex index : idToVisibleIndex.values()) {
                indexMetaIdToIndexIds.put(index.getMetaId(), Lists.newArrayList(index.getId()));
            }
            for (MaterializedIndex index : idToShadowIndex.values()) {
                indexMetaIdToIndexIds.put(index.getMetaId(), Lists.newArrayList(index.getId()));
            }

            // set baseIndexMetaId
            baseIndexMetaId = baseIndex.getMetaId();

            // reset baseIndex to null
            baseIndex = null;
        }
    }
}
