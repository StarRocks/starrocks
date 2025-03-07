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
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
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

    private volatile long minVersion = 0L;

    public long rebuildPindexVersion = 0L;

    public LakeTablet(long id) {
        super(id);
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

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public Set<Long> getBackendIds() {
        return getBackendIds(WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    public Set<Long> getBackendIds(long warehouseId) {
        if (GlobalStateMgr.isCheckpointThread()) {
            // NOTE: defensive code: don't touch any backend RPC if in checkpoint thread
            return Collections.emptySet();
        }
        try {
            List<Long> ids = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getAllComputeNodeIdsAssignToTablet(warehouseId, this);
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
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
        return replicas;
    }

    // visibleVersion and schemaHash is not used
    @Override
    public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                     long visibleVersion, long localBeId, int schemaHash) {
        getQueryableReplicas(allQuerableReplicas, localReplicas, visibleVersion, localBeId,
                schemaHash, WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    @Override
    public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                     long visibleVersion, long localBeId, int schemaHash, long warehouseId) {
        List<Long> computeNodeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getAllComputeNodeIdsAssignToTablet(warehouseId, this);
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



    public static LakeTablet read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LakeTablet.class);
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
