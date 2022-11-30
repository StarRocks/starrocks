// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.google.gson.annotations.SerializedName;
import com.staros.proto.FileCacheInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;

import java.io.IOException;

public class StorageCacheInfo implements GsonPreProcessable, GsonPostProcessable {
    // cache ttl:
    // -1 indicates "cache forever"
    // 0 indicates "disable cache"
    @SerializedName(value = "cacheInfoBytes")
    private byte[] cacheInfoBytes;
    private FileCacheInfo cacheInfo;

    public StorageCacheInfo(FileCacheInfo cacheInfo) {
        this.cacheInfo = cacheInfo;
    }

    public StorageCacheInfo(boolean enableCache, long cacheTtlS, boolean asyncWriteBack) {
        this.cacheInfo = FileCacheInfo.newBuilder().setEnableCache(enableCache).setTtlSeconds(cacheTtlS)
                .setAsyncWriteBack(asyncWriteBack).build();
    }

    public FileCacheInfo getCacheInfo() {
        return cacheInfo;
    }

    public boolean isEnableStorageCache() {
        return cacheInfo.getEnableCache();
    }

    public long getStorageCacheTtlS() {
        return cacheInfo.getTtlSeconds();
    }

    public boolean isAllowAsyncWriteBack() {
        return cacheInfo.getAsyncWriteBack();
    }

    @Override
    public void gsonPreProcess() throws IOException {
        if (cacheInfo != null) {
            cacheInfoBytes = cacheInfo.toByteArray();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (cacheInfoBytes != null) {
            cacheInfo = FileCacheInfo.parseFrom(cacheInfoBytes);
        }
    }
}
