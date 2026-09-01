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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.starrocks.catalog.Replica.ReplicaState.NORMAL;

/**
 * This class represents the StarRocks lake tablet related metadata.
 * LakeTablet is based on cloud object storage, such as S3, OSS.
 * Data replicas are managed by object storage and compute replicas are managed by StarOS through Shard.
 * Tablet id is same as StarOS Shard id.
 */
public class LakeTablet extends Tablet {
    public static final String PROPERTY_KEY_TABLE_ID = "tableId";
    public static final String PROPERTY_KEY_INDEX_ID = "indexId";
    public static final String PROPERTY_KEY_PARTITION_ID = "partitionId";

    private static final Logger LOG = LogManager.getLogger(LakeTablet.class);

    private static final String JSON_KEY_DATA_SIZE = "dataSize";
    private static final String JSON_KEY_ROW_COUNT = "rowCount";
    private static final String JSON_KEY_DATA_SIZE_UPDATE_TIME = "dataSizeUpdateTime";

    @SerializedName(value = JSON_KEY_DATA_SIZE)
    private volatile long dataSize = 0L;
    @SerializedName(value = JSON_KEY_ROW_COUNT)
    private volatile long rowCount = 0L;
    @SerializedName(value = JSON_KEY_DATA_SIZE_UPDATE_TIME)
    private volatile long dataSizeUpdateTime = 0L;

    // Which tablet version rowCount was computed from; 0 = unknown. Not persisted and not
    // journal-replicated: it describes what THIS FE managed to collect. Only ever written together
    // with the count it describes and read back the same way, see getRowCountAtVersion.
    private volatile long rowCountVersion = 0L;

    // The vacuum metadata floor the BE last proved for this tablet: no tablet metadata exists at or
    // below this version, so the next vacuum round starts its prev_garbage_version walk here instead
    // of descending into versions an earlier round already deleted. Sent to the BE as
    // TabletInfoPB.min_version and adopted back from the vacuum response (AutovacuumDaemon); also the
    // lower bound of a repair metadata scan (TabletRepairHelper).
    //
    // Known issue, working as designed for now: no @SerializedName, so it is neither persisted in the
    // image nor journal-replicated, and every FE restart or leader failover resets it to 0 for every
    // tablet. Acceptable because it is a hint, never a correctness input -- the BE clamps it with
    // max(1, min_version), treats a NotFound during the walk as the chain bottom rather than an error,
    // and re-reports a fresh floor on every round that proves one, so a stale-low value costs only
    // extra metadata reads (one NotFound per tablet per round) until a later round restores it.
    // Persisting it would be an image/journal format change. Do not start relying on this value for
    // anything that must survive a restart.
    private volatile long minVersion = 0L;

    // Written by the ALTER ... DROP PERSISTENT INDEX path and read lock-free by the lake publish
    // thread (Utils.processTablets); must be volatile so the publish thread observes the update.
    private volatile long rebuildPindexVersion = 0L;

    public LakeTablet() {
        super();
    }

    public LakeTablet(long id) {
        super(id);
    }

    public LakeTablet(long id, TabletRange range) {
        super(id, range);
    }

    public long getShardId() {
        return getId();
    }

    // singleReplica is not used
    @Override
    public long getDataSize(boolean singleReplica) {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public void setDataSizeUpdateTime(long dataSizeUpdateTime) {
        this.dataSizeUpdateTime = dataSizeUpdateTime;
    }

    public long getDataSizeUpdateTime() {
        return dataSizeUpdateTime;
    }

    /**
     * The CN computes get_tablet_stats strictly from the version the FE asked for
     * (LakeServiceImpl::get_tablet_stats -> get_tablet_metadata(tablet_id, version)), so the
     * version we requested is exactly the version the returned rowCount describes. The publish-time
     * shortcut in LakeTableTxnLogApplier likewise knows the version it is applying.
     */
    @Override
    public synchronized long getRowCountAtVersion(long version) {
        return rowCountVersion > 0 && rowCountVersion == version ? rowCount : -1L;
    }

    public long getMinVersion() {
        return minVersion;
    }

    public void setMinVersion(long minVersion) {
        this.minVersion = minVersion;
    }

    // version is not used
    @Override
    public long getRowCount(long version) {
        return rowCount;
    }

    @Override
    public long getFuzzyRowCount() {
        return rowCount;
    }

    /**
     * For a caller that knows which version the count was computed from. Written as one pair with
     * the version, so a reader can never pick up a count next to a version that does not describe
     * it; see getRowCountAtVersion.
     */
    public synchronized void setRowCount(long rowCount, long version) {
        this.rowCount = rowCount;
        this.rowCountVersion = version;
    }

    /**
     * For a caller that cannot say which version the count covers. It drops any previous proof
     * rather than leaving it to vouch for a number it never saw.
     */
    public synchronized void setRowCount(long rowCount) {
        this.rowCount = rowCount;
        this.rowCountVersion = 0L;
    }

    @Override
    public Set<Long> getBackendIds() {
        return getBackendIds(WarehouseManager.DEFAULT_RESOURCE);
    }

    public Set<Long> getBackendIds(ComputeResource computeResource) {
        if (GlobalStateMgr.isCheckpointThread()) {
            // NOTE: defensive code: don't touch any backend RPC if in checkpoint thread
            return Collections.emptySet();
        }

        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        try {
            List<Long> ids = warehouseManager.getAllComputeNodeIdsAssignToTablet(computeResource, getId());
            if (ids == null) {
                return Sets.newHashSet();
            } else {
                return new HashSet<Long>(ids);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get backends by shard id: {}", getId(), e);
            return Sets.newHashSet();
        }
    }

    @Override
    public List<Replica> getAllReplicas() {
        List<Replica> replicas = Lists.newArrayList();
        getQueryableReplicas(replicas, null, 0, -1, 0,
                WarehouseManager.DEFAULT_RESOURCE, null);
        return replicas;
    }

    // visibleVersion and schemaHash is not used
    @Override
    public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                     long visibleVersion, long localBeId, int schemaHash) {
        getQueryableReplicas(allQuerableReplicas, localReplicas, visibleVersion, localBeId,
                schemaHash, WarehouseManager.DEFAULT_RESOURCE, null);
    }

    @Override
    public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                     long visibleVersion, long localBeId, int schemaHash,
                                     ComputeResource computeResource, List<Long> locations) {
        List<Long> computeNodeIds = locations;
        if (computeNodeIds == null) { // initial location hint is null, grab the info from warehouse manager.
            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            computeNodeIds = warehouseManager.getAllComputeNodeIdsAssignToTablet(computeResource, getId());
        }
        if (computeNodeIds == null) {
            return;
        }
        for (long backendId : computeNodeIds) {
            Replica replica = new Replica(getId(), backendId, visibleVersion, schemaHash, getDataSize(true),
                    getRowCount(visibleVersion), NORMAL, -1, visibleVersion);
            allQuerableReplicas.add(replica);
            if (localBeId != -1 && backendId == localBeId) {
                localReplicas.add(replica);
            }
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LakeTablet)) {
            return false;
        }

        LakeTablet tablet = (LakeTablet) obj;
        return (id == tablet.id && dataSize == tablet.dataSize && rowCount == tablet.rowCount);
    }

    public void setRebuildPindexVersion(long rebuildPindexVersion) {
        if (rebuildPindexVersion > this.rebuildPindexVersion) {
            this.rebuildPindexVersion = rebuildPindexVersion;
        }
    }

    public long rebuildPindexVersion() {
        return rebuildPindexVersion;
    }
}
