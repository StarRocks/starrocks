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

package com.starrocks.connector.iceberg;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;

public class IcebergCatalogProperties {
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    @Deprecated
    public static final String ICEBERG_CATALOG_LEGACY = "starrocks.catalog-type";
    @Deprecated
    public static final String ICEBERG_METASTORE_URIS = "iceberg.catalog.hive.metastore.uris";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_METASTORE_TIMEOUT = "hive.metastore.timeout";
    public static final String ICEBERG_CUSTOM_PROPERTIES_PREFIX = "iceberg.catalog.";
    public static final String ENABLE_ICEBERG_METADATA_CACHE = "enable_iceberg_metadata_cache";
    public static final String ICEBERG_META_CACHE_TTL = "iceberg_meta_cache_ttl_sec";
    public static final String ICEBERG_JOB_PLANNING_THREAD_NUM = "iceberg_job_planning_thread_num";
    public static final String REFRESH_OTHER_FE_ICEBERG_CACHE_THREAD_NUM = "refresh_other_fe_iceberg_cache_thread_num";
    public static final String BACKGROUND_ICEBERG_JOB_PLANNING_THREAD_NUM = "background_iceberg_job_planning_thread_num";
    public static final String ICEBERG_MANIFEST_CACHE_WITH_METRICS = "iceberg_manifest_cache_with_column_statistics";
    public static final String ICEBERG_MANIFEST_CACHE_MAX_NUM = "iceberg_manifest_cache_max_num";

    // internal config
    public static final String ICEBERG_TABLE_CACHE_TTL = "iceberg_table_cache_ttl_sec";

    private final Map<String, String> properties;
    private IcebergCatalogType catalogType;
    private boolean enableIcebergMetadataCache;
    private long icebergMetaCacheTtlSec;
    private int icebergJobPlanningThreadNum;
    private int backgroundIcebergJobPlanningThreadNum;
    private int refreshOtherFeIcebergCacheThreadNum;
    private boolean icebergManifestCacheWithColumnStatistics;
    private long icebergTableCacheTtlSec;
    private long icebergManifestCacheMaxNum;

    public IcebergCatalogProperties(Map<String, String> catalogProperties) {
        this.properties = catalogProperties;
        init();
    }

    private void init() {
        initCatalogType();
        initIcebergMetadataCache();
        initThreadPoolNum();
    }

    private void initCatalogType() {
        String nativeCatalogTypeStr = properties.get(ICEBERG_CATALOG_TYPE);
        if (Strings.isNullOrEmpty(nativeCatalogTypeStr)) {
            nativeCatalogTypeStr = properties.get(ICEBERG_CATALOG_LEGACY);
        }
        if (Strings.isNullOrEmpty(nativeCatalogTypeStr)) {
            throw new StarRocksConnectorException("Can't find iceberg native catalog type. You must specify the" +
                    " 'iceberg.catalog.type' property when creating an iceberg catalog in the catalog properties");
        }
        this.catalogType = IcebergCatalogType.fromString(nativeCatalogTypeStr);
    }

    private void initIcebergMetadataCache() {
        this.enableIcebergMetadataCache = PropertyUtil.propertyAsBoolean(properties, ENABLE_ICEBERG_METADATA_CACHE, true);

        this.icebergMetaCacheTtlSec = PropertyUtil.propertyAsLong(properties, ICEBERG_META_CACHE_TTL, 48 * 60 * 60);
        this.icebergTableCacheTtlSec = PropertyUtil.propertyAsLong(properties, ICEBERG_TABLE_CACHE_TTL, 1800L);
        this.icebergManifestCacheMaxNum = PropertyUtil.propertyAsLong(properties, ICEBERG_MANIFEST_CACHE_MAX_NUM, 100000);
        this.icebergManifestCacheWithColumnStatistics = PropertyUtil.propertyAsBoolean(
                properties, ICEBERG_MANIFEST_CACHE_WITH_METRICS, false);
    }

    private void initThreadPoolNum() {
        this.icebergJobPlanningThreadNum = Math.max(2,
                PropertyUtil.propertyAsInt(properties, ICEBERG_JOB_PLANNING_THREAD_NUM, Config.iceberg_worker_num_threads));
        this.refreshOtherFeIcebergCacheThreadNum = Math.max(2,
                PropertyUtil.propertyAsInt(properties, REFRESH_OTHER_FE_ICEBERG_CACHE_THREAD_NUM, 4));
        this.backgroundIcebergJobPlanningThreadNum =
                PropertyUtil.propertyAsInt(properties, BACKGROUND_ICEBERG_JOB_PLANNING_THREAD_NUM,
                        Math.max(2, Runtime.getRuntime().availableProcessors() / 8));
    }

    public IcebergCatalogType getCatalogType() {
        return catalogType;
    }

    public boolean enableIcebergMetadataCache() {
        return enableIcebergMetadataCache;
    }

    public long getIcebergMetaCacheTtlSec() {
        return icebergMetaCacheTtlSec;
    }


    public int getIcebergJobPlanningThreadNum() {
        return icebergJobPlanningThreadNum;
    }

    public int getRefreshOtherFeIcebergCacheThreadNum() {
        return refreshOtherFeIcebergCacheThreadNum;
    }

    public int getBackgroundIcebergJobPlanningThreadNum() {
        return backgroundIcebergJobPlanningThreadNum;
    }

    public boolean isIcebergManifestCacheWithColumnStatistics() {
        return icebergManifestCacheWithColumnStatistics;
    }

    public long getIcebergTableCacheTtlSec() {
        return icebergTableCacheTtlSec;
    }

    public boolean isEnableIcebergMetadataCache() {
        return enableIcebergMetadataCache;
    }

    public long getIcebergManifestCacheMaxNum() {
        return icebergManifestCacheMaxNum;
    }
}
