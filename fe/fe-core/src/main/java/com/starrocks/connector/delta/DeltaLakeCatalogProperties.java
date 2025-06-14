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
    public static final String DELTA_LAKE_JSON_META_CACHE_MEMORY_USAGE_RATIO = "deltalake_json_meta_cache_memory_usage_ratio";
    public static final String ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE = "enable_deltalake_checkpoint_meta_cache";
    public static final String DELTA_LAKE_CHECKPOINT_META_CACHE_TTL = "deltalake_checkpoint_meta_cache_ttl_sec";
    public static final String DELTA_LAKE_CHECKPOINT_META_CACHE_MEMORY_USAGE_RATIO =
            "deltalake_checkpoint_meta_cache_memory_usage_ratio";

    private final Map<String, String> properties;
    private boolean enableDeltaLakeTableCache;
    private boolean enableDeltaLakeJsonMetaCache;
    private boolean enableDeltaLakeCheckpointMetaCache;
    private long deltaLakeJsonMetaCacheTtlSec;
    private double deltaLakeJsonMetaCacheMemoryUsageRatio;
    private long deltaLakeCheckpointMetaCacheTtlSec;
    private double deltaLakeCheckpointMetaCacheMemoryUsageRatio;

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
                PropertyUtil.propertyAsBoolean(properties, ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE, true);
        this.deltaLakeJsonMetaCacheTtlSec =
                PropertyUtil.propertyAsLong(properties, DELTA_LAKE_JSON_META_CACHE_TTL, 48 * 60 * 60);
        this.deltaLakeJsonMetaCacheMemoryUsageRatio =
                PropertyUtil.propertyAsDouble(properties, DELTA_LAKE_JSON_META_CACHE_MEMORY_USAGE_RATIO, 0.1);
        this.deltaLakeCheckpointMetaCacheTtlSec =
                PropertyUtil.propertyAsLong(properties, DELTA_LAKE_CHECKPOINT_META_CACHE_TTL, 48 * 60 * 60);
        this.deltaLakeCheckpointMetaCacheMemoryUsageRatio =
                PropertyUtil.propertyAsDouble(properties, DELTA_LAKE_CHECKPOINT_META_CACHE_MEMORY_USAGE_RATIO, 0.1);
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

    public double getDeltaLakeJsonMetaCacheMemoryUsageRatio() {
        return deltaLakeJsonMetaCacheMemoryUsageRatio;
    }

    public long getDeltaLakeCheckpointMetaCacheTtlSec() {
        return deltaLakeCheckpointMetaCacheTtlSec;
    }

    public double getDeltaLakeCheckpointMetaCacheMemoryUsageRatio() {
        return deltaLakeCheckpointMetaCacheMemoryUsageRatio;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}

