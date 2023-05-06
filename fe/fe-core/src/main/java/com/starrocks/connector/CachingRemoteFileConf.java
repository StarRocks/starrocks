// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.starrocks.common.Config;

import java.util.Map;

public class CachingRemoteFileConf {
    private final long cacheTtlSec;
    private final long cacheRefreshIntervalSec;
    private long cacheMaxSize = 1000000L;
    private final int perQueryCacheMaxSize = 10000;
    private final int refreshMaxThreadNum;

    public CachingRemoteFileConf(Map<String, String> conf) {
        this.cacheTtlSec = Long.parseLong(conf.getOrDefault("remote_file_cache_ttl_sec",
                String.valueOf(Config.remote_file_cache_ttl_s)));
        this.cacheRefreshIntervalSec = Long.parseLong(conf.getOrDefault("remote_file_cache_refresh_interval_sec",
                String.valueOf(Config.remote_file_cache_refresh_interval_s)));
        this.cacheMaxSize = Long.parseLong(conf.getOrDefault("remote_file_cache_max_num", String.valueOf(cacheMaxSize)));
        this.refreshMaxThreadNum = Integer.parseInt(conf.getOrDefault("async_refresh_max_thread_num", "32"));
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

    public int getPerQueryCacheMaxSize() {
        return perQueryCacheMaxSize;
    }

    public int getRefreshMaxThreadNum() {
        return refreshMaxThreadNum;
    }
}
