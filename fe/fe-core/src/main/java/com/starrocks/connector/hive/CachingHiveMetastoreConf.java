// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.starrocks.common.config.ExternalCatlogConfig;
import com.starrocks.common.config.PropertyUtil;

import java.util.Map;

public class CachingHiveMetastoreConf {
    private final long cacheTtlSec;
    private final long cacheRefreshIntervalSec;
    private final long cacheMaxNum = 100000;
    private final int perQueryCacheMaxNum = 10000;
    private final int cacheRefreshThreadMaxNum = 20;

    private final boolean enableListNamesCache;

    public CachingHiveMetastoreConf(Map<String, String> conf) {
        this.cacheTtlSec = PropertyUtil.propertyAsLong(conf, ExternalCatlogConfig.HIVE_META_CACHE_TTL_S.key(),
                ExternalCatlogConfig.HIVE_META_CACHE_TTL_S.defaultValue());
        this.cacheRefreshIntervalSec = PropertyUtil.propertyAsLong(conf,
                ExternalCatlogConfig.METASTORE_CACHE_REFRESH_INTERVAL_SEC.key(),
                ExternalCatlogConfig.METASTORE_CACHE_REFRESH_INTERVAL_SEC.defaultValue());
        this.enableListNamesCache = PropertyUtil.propertyAsBoolean(conf, ExternalCatlogConfig.ENABLE_CACHE_LIST_NAMES.key(),
                ExternalCatlogConfig.ENABLE_CACHE_LIST_NAMES.defaultValue());
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
