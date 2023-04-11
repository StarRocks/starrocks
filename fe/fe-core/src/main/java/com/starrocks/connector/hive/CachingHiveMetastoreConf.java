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
                enableListNamesCacheDefaultValue));
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
