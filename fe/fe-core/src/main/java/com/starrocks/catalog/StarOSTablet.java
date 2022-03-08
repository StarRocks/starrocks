// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

/**
 * This class represents the cloud olap tablet related metadata.
 * StarOSTablet is based on cloud object storage.
 * Data replicas are managed by object storage and compute replicas are managed by StarOS through Shard.
 */
public class StarOSTablet extends Tablet {
    @SerializedName(value = "shardId")
    private long shardId;
    @SerializedName(value = "dataSize")
    private long dataSize = 0L;

    public StarOSTablet(long id, long shardId) {
        super(id);
        this.shardId = shardId;
    }

    @Override
    public long getDataSize() {
        return dataSize;
    }
}
