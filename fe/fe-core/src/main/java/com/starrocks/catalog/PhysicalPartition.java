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
import com.starrocks.transaction.TransactionType;

import java.util.List;

/* 
 * PhysicalPartition is the interface that describes the physical storage of a partition.
 * It includes version information and one or more MaterializedIndexes.
 * Each MaterializedIndex contains multiple tablets.
 */
<<<<<<< HEAD
public interface PhysicalPartition {

    // partition id which contains this physical partition
    public long getParentId();
    public void setParentId(long parentId);

    // physical partition id
    public long getId();
    public String getName();
    public void setName(String name);
    public void setIdForRestore(long id);
    public long getBeforeRestoreId();

    public long getShardGroupId();

    public void setImmutable(boolean isImmutable);
    public boolean isImmutable();

    // version interface

    public void updateVersionForRestore(long visibleVersion);

    public void updateVisibleVersion(long visibleVersion);

    public void updateVisibleVersion(long visibleVersion, long visibleVersionTime);
    public void updateVisibleVersion(long visibleVersion, long visibleVersionTime, long visibleTxnId);
    public long getVisibleVersion();
    public long getVisibleVersionTime();
    public void setVisibleVersion(long visibleVersion, long visibleVersionTime);
    public long getNextVersion();
    public void setNextVersion(long nextVersion);
    public long getCommittedVersion();
    public long getDataVersion();
    public void setDataVersion(long dataVersion);
    public long getNextDataVersion();
    public void setNextDataVersion(long nextDataVersion);
    public long getCommittedDataVersion();
    public long getVersionEpoch();
    public void setVersionEpoch(long versionEpoch);
    public long nextVersionEpoch();
    public TransactionType getVersionTxnType();
    public void setVersionTxnType(TransactionType versionTxnType);
    public long getVisibleTxnId();

    // materialized index interface

    public void createRollupIndex(MaterializedIndex mIndex);
    public MaterializedIndex deleteRollupIndex(long indexId);
    public void setBaseIndex(MaterializedIndex baseIndex);
    public MaterializedIndex getBaseIndex();
    public MaterializedIndex getIndex(long indexId);
    public List<MaterializedIndex> getMaterializedIndices(IndexExtState extState);
=======
public class PhysicalPartition extends MetaObject implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(PhysicalPartition.class);

    public static final long PARTITION_INIT_VERSION = 1L;

    public static final long INVALID_SHARD_GROUP_ID = -1L;

    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "name")
    private String name;

    private long beforeRestoreId;

    @SerializedName(value = "parentId")
    private long parentId;

    @SerializedName(value = "shardGroupId")
    private long shardGroupId = INVALID_SHARD_GROUP_ID;

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

    @SerializedName(value = "dataVersion")
    private long dataVersion;
    @SerializedName(value = "nextDataVersion")
    private long nextDataVersion;

    @SerializedName(value = "versionEpoch")
    private long versionEpoch;
    @SerializedName(value = "versionTxnType")
    private TransactionType versionTxnType;
    /**
     * ID of the transaction that has committed current visible version.
     * Just for tracing the txn log, no need to persist.
     */
    private long visibleTxnId = -1;

    private volatile long lastVacuumTime = 0;

    private volatile long minRetainVersion = 0;

    private volatile long lastSuccVacuumVersion = 0;

    private PhysicalPartition() {

    }

    public PhysicalPartition(long id, String name, long parentId, MaterializedIndex baseIndex) {
        this.id = id;
        this.name = name;
        this.parentId = parentId;
        this.baseIndex = baseIndex;
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

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
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
        List<Long> result = new ArrayList<>();
        idToVisibleRollupIndex.values().stream().map(MaterializedIndex::getShardGroupId).forEach(result::add);
        idToShadowIndex.values().stream().map(MaterializedIndex::getShardGroupId).forEach(result::add);
        result.add(baseIndex.getShardGroupId());
        return result;
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
        return lastVacuumTime;
    }

    public void setLastVacuumTime(long lastVacuumTime) {
        this.lastVacuumTime = lastVacuumTime;
    }

    public long getMinRetainVersion() {
        return minRetainVersion;
    }

    public void setMinRetainVersion(long minRetainVersion) {
        this.minRetainVersion = minRetainVersion;
    }

    public long getLastSuccVacuumVersion() {
        return lastSuccVacuumVersion;
    }

    public void setLastSuccVacuumVersion(long lastSuccVacuumVersion) {
        this.lastSuccVacuumVersion = lastSuccVacuumVersion;
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

    public void createRollupIndex(MaterializedIndex mIndex) {
        if (mIndex.getState().isVisible()) {
            this.idToVisibleRollupIndex.put(mIndex.getId(), mIndex);
        } else {
            this.idToShadowIndex.put(mIndex.getId(), mIndex);
        }
    }

    public MaterializedIndex deleteRollupIndex(long indexId) {
        if (this.idToVisibleRollupIndex.containsKey(indexId)) {
            return idToVisibleRollupIndex.remove(indexId);
        } else {
            return idToShadowIndex.remove(indexId);
        }
    }

    public void setBaseIndex(MaterializedIndex baseIndex) {
        this.baseIndex = baseIndex;
    }

    public MaterializedIndex getBaseIndex() {
        return baseIndex;
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

    public List<MaterializedIndex> getMaterializedIndices(IndexExtState extState) {
        int expectedSize = 1 + idToVisibleRollupIndex.size() + idToShadowIndex.size();
        List<MaterializedIndex> indices = Lists.newArrayListWithExpectedSize(expectedSize);
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

    public long getTabletMaxDataSize() {
        long maxDataSize = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            maxDataSize = Math.max(maxDataSize, mIndex.getTabletMaxDataSize());
        }
        return maxDataSize;
    }

    public long storageDataSize() {
        long dataSize = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            dataSize += mIndex.getDataSize();
        }
        return dataSize;
    }

    public long storageRowCount() {
        long rowCount = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            rowCount += mIndex.getRowCount();
        }
        return rowCount;
    }

    public long storageReplicaCount() {
        long replicaCount = 0;
        for (MaterializedIndex mIndex : getMaterializedIndices(IndexExtState.VISIBLE)) {
            replicaCount += mIndex.getReplicaCount();
        }
        return replicaCount;
    }

    public boolean hasMaterializedView() {
        return !idToVisibleRollupIndex.isEmpty();
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

>>>>>>> 1e54252a06 ([Enhancement] Optimize vacuum logic by adding vacuum version. (#55764))
    /*
     * Change the index' state from SHADOW to NORMAL
     */
    public boolean visualiseShadowIndex(long shadowIndexId, boolean isBaseIndex);

    // statistic interface

    // max data size of one tablet in this physical partition
    public long getTabletMaxDataSize();
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

    // for lake partition
    public long getMinRetainVersion();
    public void setMinRetainVersion(long minRetainVersion);
    public long getLastVacuumTime();
    public void setLastVacuumTime(long lastVacuumTime);

}
