// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * This class represents the cloud olap tablet related metadata.
 * StarOSTablet is based on cloud object storage.
 * Data replicas are managed by object storage and compute replicas are managed by StarOS through Shard.
 */
public class StarOSTablet extends Tablet {
    private static final String JSON_KEY_SHARD_ID = "shardId";
    private static final String JSON_KEY_DATA_SIZE = "dataSize";
    private static final String JSON_KEY_ROW_COUNT = "rowCount";

    @SerializedName(value = JSON_KEY_SHARD_ID)
    private long shardId;
    @SerializedName(value = JSON_KEY_DATA_SIZE)
    private long dataSize = 0L;
    @SerializedName(value = JSON_KEY_ROW_COUNT)
    private long rowCount = 0L;

    public StarOSTablet(long id, long shardId) {
        super(id);
        this.shardId = shardId;
    }

    public long getShardId() {
        return shardId;
    }

    // singleReplica is not used
    @Override
    public long getDataSize(boolean singleReplica) {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    // version is not used
    @Override
    public long getRowCount(long version) {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getPrimaryBackendId() {
        return Catalog.getCurrentCatalog().getStarOSAgent().getPrimaryBackendIdByShard(shardId);
    }

    @Override
    public Set<Long> getBackendIds() {
        return Catalog.getCurrentCatalog().getStarOSAgent().getBackendIdsByShard(shardId);
    }

    // visibleVersion and schemaHash is not used
    @Override
    public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas,
                                     long visibleVersion, long localBeId, int schemaHash) {
        for (long backendId : getBackendIds()) {
            Replica replica = new Replica(-1, backendId, -1, null);
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

    public static StarOSTablet read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, StarOSTablet.class);
    }
}
