package com.starrocks.external.hive;

import com.starrocks.common.Config;

import java.util.Map;

public class CachingRemoteFileConf {
    private final long cacheTtlSec;
    private final long cacheRefreshIntervalSec;
    private final long cacheMaxSize = 1000000L;
    private final long perQueryMaxSize = 10000;

    public CachingRemoteFileConf(Map<String, String> conf) {
        this.cacheTtlSec = Long.parseLong(conf.getOrDefault("remote_file_cache_ttl_sec",
                String.valueOf(Config.remote_file_cache_ttl_s)));
        this.cacheRefreshIntervalSec = Long.parseLong(conf.getOrDefault("remote_file_cache_refresh_interval_sec",
                String.valueOf(Config.remote_file_cache_refresh_s)));
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

    public long getPerQueryMaxSize() {
        return perQueryMaxSize;
    }
}
