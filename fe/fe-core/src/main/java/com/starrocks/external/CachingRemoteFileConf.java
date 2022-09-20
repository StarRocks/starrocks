// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.starrocks.common.Config;

import java.util.Map;

public class CachingRemoteFileConf {
    private final long cacheTtlSec;
    private final long cacheRefreshIntervalSec;
    private final long loadRemoteFileMetadataThreadNum;
    private final long cacheMaxSize = 1000000L;
    private final long perQueryCacheMaxSize = 10000;

    public CachingRemoteFileConf(Map<String, String> conf) {
        this.cacheTtlSec = Long.parseLong(conf.getOrDefault("remote_file_cache_ttl_sec",
                String.valueOf(Config.remote_file_cache_ttl_s)));
        this.cacheRefreshIntervalSec = Long.parseLong(conf.getOrDefault("remote_file_cache_refresh_interval_sec",
                String.valueOf(Config.remote_file_cache_refresh_interval_s)));
        this.loadRemoteFileMetadataThreadNum = Long.parseLong(conf.getOrDefault("remote_file_load_thread_num",
                String.valueOf(Config.remote_file_metadata_load_concurrency)));
    }

    public long getCacheTtlSec() {
        return cacheTtlSec;
    }

    public long getCacheRefreshIntervalSec() {
        return cacheRefreshIntervalSec;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getPerQueryCacheMaxSize() {
        return perQueryCacheMaxSize;
    }

    public long getLoadRemoteFileMetadataThreadNum() {
        return loadRemoteFileMetadataThreadNum;
    }
}
