// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

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

    // TODO: replace StorageCacheInfo with com.staros.proto.CacheInfo.
    @SerializedName(value = "storageCacheInfo")
    private StorageCacheInfo storageCacheInfo;

    public StorageInfo(ShardStorageInfo shardStorageInfo, StorageCacheInfo storageCacheInfo) {
        this.shardStorageInfo = shardStorageInfo;
        this.storageCacheInfo = storageCacheInfo;
    }

    public boolean isEnableStorageCache() {
        return storageCacheInfo.isEnableCache();
    }

    public long getStorageCacheTtlS() {
        return storageCacheInfo.getCacheTtlS();
    }

    public boolean isAllowAsyncWriteBack() {
        return storageCacheInfo.isAllowAsyncWriteBack();
    }

    public ShardStorageInfo getShardStorageInfo() {
        return shardStorageInfo;
    }

    public StorageCacheInfo getStorageCacheInfo() { return storageCacheInfo; }

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
