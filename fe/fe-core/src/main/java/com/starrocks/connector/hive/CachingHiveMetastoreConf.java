// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.starrocks.common.Config;

import java.util.Map;

public class CachingHiveMetastoreConf {
    private final long cacheTtlSec;
    private final long cacheRefreshIntervalSec;
    private long cacheMaxNum = 1000000;
    private final int perQueryCacheMaxNum = 10000;
    private final int cacheRefreshThreadMaxNum = 20;

    private final boolean enableListNamesCache;

    public CachingHiveMetastoreConf(Map<String, String> conf, String catalogType) {
        this.cacheTtlSec = Long.parseLong(conf.getOrDefault("metastore_cache_ttl_sec",
                String.valueOf(Config.hive_meta_cache_ttl_s)));
        this.cacheRefreshIntervalSec = Long.parseLong(conf.getOrDefault("metastore_cache_refresh_interval_sec",
                String.valueOf(Config.hive_meta_cache_refresh_interval_s)));
        String enableListNamesCacheDefaultValue = catalogType.equalsIgnoreCase("hive") ? "true" : "false";
        this.enableListNamesCache = Boolean.parseBoolean(conf.getOrDefault("enable_cache_list_names",
                "false"));
        this.cacheMaxNum = Long.parseLong(conf.getOrDefault("metastore_cache_max_num", String.valueOf(cacheMaxNum)));
    }

    public long getCacheTtlSec() {
        return cacheTtlSec;
    }

    public long getCacheRefreshIntervalSec() {
        return cacheRefreshIntervalSec;
    }

    public long getCacheMaxNum() {
        return cacheMaxNum;
    }

    public int getCacheRefreshThreadMaxNum() {
        return cacheRefreshThreadMaxNum;
    }

    public int getPerQueryCacheMaxNum() {
        return perQueryCacheMaxNum;
    }

    public boolean enableListNamesCache() {
        return enableListNamesCache;
    }
}
