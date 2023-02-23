// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
