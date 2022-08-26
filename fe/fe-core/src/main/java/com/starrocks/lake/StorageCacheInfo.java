// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.google.gson.annotations.SerializedName;

public class StorageCacheInfo {
    @SerializedName(value = "enableCache")
    private boolean enableCache = false;

    // -1 indicates "cache forever"
    // 0 indicates "disable cache"
    @SerializedName(value = "cacheTtlS")
    private long cacheTtlS = 0;

    @SerializedName(value = "allowAsyncWriteBack")
    private boolean allowAsyncWriteBack = false;

    public StorageCacheInfo(boolean enableCache, long cacheTtlS, boolean allowAsyncWriteBack) {
        this.enableCache = enableCache;
        this.cacheTtlS = cacheTtlS;
        this.allowAsyncWriteBack = allowAsyncWriteBack;
    }

    public boolean isEnableCache() {
        return enableCache;
    }

    public long getCacheTtlS() {
        return cacheTtlS;
    }

    public boolean isAllowAsyncWriteBack() {
        return allowAsyncWriteBack;
    }
}
