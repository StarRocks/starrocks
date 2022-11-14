// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.google.gson.annotations.SerializedName;
import com.staros.proto.CacheInfo;
import com.staros.proto.ShardStorageInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;

import java.io.IOException;

// Storage info for lake table, include object storage info and table default cache info.
// Currently, storage group is table level.
public class StorageInfo implements GsonPreProcessable, GsonPostProcessable {
    // "shardStorageInfoBytes" is used for serialization of "shardStorageInfo".
    @SerializedName(value = "shardStorageInfoBytes")
    private byte[] shardStorageInfoBytes;
    private ShardStorageInfo shardStorageInfo;

    public StorageInfo(ShardStorageInfo shardStorageInfo) {
        this.shardStorageInfo = shardStorageInfo;
    }

    public boolean isEnableStorageCache() {
        return getCacheInfo().getEnableCache();
    }

    public long getStorageCacheTtlS() {
        return getCacheInfo().getTtlSeconds();
    }

    public boolean isAllowAsyncWriteBack() {
        return getCacheInfo().getAllowAsyncWriteBack();
    }

    public ShardStorageInfo getShardStorageInfo() {
        return shardStorageInfo;
    }

    public CacheInfo getCacheInfo() {
        return shardStorageInfo.getCacheInfo();
    }

    public StorageCacheInfo getStorageCacheInfo() {
        return new StorageCacheInfo(getCacheInfo());
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
