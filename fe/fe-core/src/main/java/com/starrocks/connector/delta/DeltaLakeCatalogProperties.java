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

package com.starrocks.connector.delta;

import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;

public class DeltaLakeCatalogProperties {
    public static final String ENABLE_DELTA_LAKE_TABLE_CACHE = "enable_deltalake_table_cache";
    public static final String ENABLE_DELTA_LAKE_JSON_META_CACHE = "enable_deltalake_json_meta_cache";
    public static final String DELTA_LAKE_JSON_META_CACHE_TTL = "deltalake_json_meta_cache_ttl_sec";
    public static final String DELTA_LAKE_JSON_META_CACHE_MAX_NUM = "deltalake_json_meta_cache_max_num";
    public static final String ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE = "enable_deltalake_checkpoint_meta_cache";
    public static final String DELTA_LAKE_CHECKPOINT_META_CACHE_TTL = "deltalake_checkpoint_meta_cache_ttl_sec";
    public static final String DELTA_LAKE_CHECKPOINT_META_CACHE_MAX_NUM = "deltalake_checkpoint_meta_cache_max_num";

    private final Map<String, String> properties;
    private boolean enableDeltaLakeTableCache;
    private boolean enableDeltaLakeJsonMetaCache;
    private boolean enableDeltaLakeCheckpointMetaCache;
    private long deltaLakeJsonMetaCacheTtlSec;
    private long deltaLakeJsonMetaCacheMaxNum;
    private long deltaLakeCheckpointMetaCacheTtlSec;
    private long deltaLakeCheckpointMetaCacheMaxNum;

    public DeltaLakeCatalogProperties(Map<String, String> properties) {
        this.properties = properties;
        init();
    }

    private void init() {
        this.enableDeltaLakeTableCache =
                PropertyUtil.propertyAsBoolean(properties, ENABLE_DELTA_LAKE_TABLE_CACHE, true);
        this.enableDeltaLakeJsonMetaCache =
                PropertyUtil.propertyAsBoolean(properties, ENABLE_DELTA_LAKE_JSON_META_CACHE, true);
        this.enableDeltaLakeCheckpointMetaCache =
                PropertyUtil.propertyAsBoolean(properties, ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE, false);
        this.deltaLakeJsonMetaCacheTtlSec =
                PropertyUtil.propertyAsLong(properties, DELTA_LAKE_JSON_META_CACHE_TTL, 48 * 60 * 60);
        this.deltaLakeJsonMetaCacheMaxNum =
                PropertyUtil.propertyAsLong(properties, DELTA_LAKE_JSON_META_CACHE_MAX_NUM, 1000);
        this.deltaLakeCheckpointMetaCacheTtlSec =
                PropertyUtil.propertyAsLong(properties, DELTA_LAKE_CHECKPOINT_META_CACHE_TTL, 48 * 60 * 60);
        this.deltaLakeCheckpointMetaCacheMaxNum =
                PropertyUtil.propertyAsLong(properties, DELTA_LAKE_CHECKPOINT_META_CACHE_MAX_NUM, 100);
    }

    public boolean isEnableDeltaLakeTableCache() {
        return enableDeltaLakeTableCache;
    }

    public boolean isEnableDeltaLakeJsonMetaCache() {
        return enableDeltaLakeJsonMetaCache;
    }

    public boolean isEnableDeltaLakeCheckpointMetaCache() {
        return enableDeltaLakeCheckpointMetaCache;
    }

    public long getDeltaLakeJsonMetaCacheTtlSec() {
        return deltaLakeJsonMetaCacheTtlSec;
    }

    public long getDeltaLakeJsonMetaCacheMaxNum() {
        return deltaLakeJsonMetaCacheMaxNum;
    }

    public long getDeltaLakeCheckpointMetaCacheTtlSec() {
        return deltaLakeCheckpointMetaCacheTtlSec;
    }

    public long getDeltaLakeCheckpointMetaCacheMaxNum() {
        return deltaLakeCheckpointMetaCacheMaxNum;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}

