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

    public StorageCacheInfo(boolean enableCache, long cacheTtlS) {
        this.enableCache = enableCache;
        this.cacheTtlS = cacheTtlS;
    }

    public boolean isEnableCache() {
        return enableCache;
    }

    public long getCacheTtlS() {
        return cacheTtlS;
    }
}
