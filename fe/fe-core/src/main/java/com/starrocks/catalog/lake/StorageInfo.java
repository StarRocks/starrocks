// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog.lake;

import com.google.gson.annotations.SerializedName;
import com.staros.proto.ShardStorageInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;

import java.io.IOException;

public class StorageInfo implements GsonPreProcessable, GsonPostProcessable {

    // "shardStorageInfoBytes" is used for serialization of "shardStorageInfo".
    @SerializedName(value = "shardStorageInfoBytes")
    private byte[] shardStorageInfoBytes;
    // Currently, storage group is table level, so "shardStorageInfo" is null in partition property.
    private ShardStorageInfo shardStorageInfo;

    @SerializedName(value = "enableStorageCache")
    private boolean enableStorageCache = false;

    @SerializedName(value = "storageCacheTtlS")
    private long storageCacheTtlS = 0;

    public StorageInfo(ShardStorageInfo shardStorageInfo, boolean enableStorageCache, long storageCacheTtlS) {
        this.shardStorageInfo = shardStorageInfo;
        this.enableStorageCache = enableStorageCache;
        this.storageCacheTtlS = storageCacheTtlS;
    }

    public boolean isEnableStorageCache() {
        return enableStorageCache;
    }

    public long getStorageCacheTtlS() {
        return storageCacheTtlS;
    }

    public ShardStorageInfo getShardStorageInfo() {
        return shardStorageInfo;
    }

    @Override
    public void gsonPreProcess() throws IOException {
        if (shardStorageInfo != null) {
            shardStorageInfoBytes = shardStorageInfo.toByteArray();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (shardStorageInfoBytes != null) {
            shardStorageInfo = ShardStorageInfo.parseFrom(shardStorageInfoBytes);
        }
    }
}
