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
import com.staros.client.StarClientException;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.validation.constraints.NotNull;

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

    // version is not used
    @Override
    public long getRowCount(long version) {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getPrimaryComputeNodeId() throws UserException {
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getDefaultWarehouse();
        long workerGroupId = warehouse.getAnyAvailableCluster().getWorkerGroupId();
        return getPrimaryComputeNodeId(workerGroupId);
    }

    public long getPrimaryComputeNodeId(long clusterId) throws UserException {
        return GlobalStateMgr.getCurrentState().getStarOSAgent().
                getPrimaryComputeNodeIdByShard(getShardId(), clusterId);
    }

    @Override
    public Set<Long> getBackendIds() {
        if (GlobalStateMgr.isCheckpointThread()) {
            // NOTE: defensive code: don't touch any backend RPC if in checkpoint thread
            return Collections.emptySet();
        }
        try {
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getDefaultWarehouse();
            long workerGroupId = warehouse.getAnyAvailableCluster().getWorkerGroupId();
            return GlobalStateMgr.getCurrentState().getStarOSAgent().getBackendIdsByShard(getShardId(), workerGroupId);
        } catch (UserException e) {
            LOG.warn("Failed to get backends by shard. tablet id: {}", getId(), e);
            return Sets.newHashSet();
        }
    }

    @Override
    public List<Replica> getAllReplicas() {
        List<Replica> replicas = Lists.newArrayList();
        getQueryableReplicas(replicas, null, 0, -1, 0);
        return replicas;
    }

    // visibleVersion and schemaHash is not used
    @Override
    public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                     long visibleVersion, long localBeId, int schemaHash) {
        for (long backendId : getBackendIds()) {
            Replica replica = new Replica(getId(), backendId, visibleVersion, schemaHash, getDataSize(true),
                    getRowCount(visibleVersion), NORMAL, -1, visibleVersion);
            allQuerableReplicas.add(replica);
            if (localBeId != -1 && backendId == localBeId) {
                localReplicas.add(replica);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
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

    @NotNull
    public ShardInfo getShardInfo() throws StarClientException {
        if (GlobalStateMgr.isCheckpointThread()) {
            throw new RuntimeException("Cannot call getShardInfo in checkpoint thread");
        }
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getDefaultWarehouse();
        long workerGroupId = warehouse.getAnyAvailableCluster().getWorkerGroupId();
        return GlobalStateMgr.getCurrentState().getStarOSAgent().getShardInfo(getShardId(), workerGroupId);
    }
}
