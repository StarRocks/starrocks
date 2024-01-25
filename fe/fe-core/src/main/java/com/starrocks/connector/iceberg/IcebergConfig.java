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

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;

public class IcebergConfig {
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
    public static final String ICEBERG_MANIFEST_CACHE_TTL = "iceberg_manifest_cache_ttl_sec";
    public static final String ICEBERG_JOB_PLANNING_THREAD_NUM = "iceberg_job_planning_thread_num";
    public static final String REFRESH_OTHER_FE_ICEBERG_CACHE_THREAD_NUM = "refresh_other_fe_iceberg_cache_thread_num";
    public static final String BACKGROUND_ICEBERG_JOB_PLANNING_THREAD_NUM = "background_iceberg_job_planning_thread_num";
    public static final String ENABLE_ICEBERG_MANIFEST_CACHE = "enable_iceberg_manifest_cache";
    public static final String ICEBERG_MANIFEST_CACHE_WITH_METRICS = "iceberg_manifest_cache_with_metrics";
    public static final String ICEBERG_MANIFEST_CACHE_MAX_SIZE = "iceberg_manifest_cache_max_size";
    public static final String ICEBERG_LOCAL_PLANNING_MAX_SLOT_BYTES = "iceberg_local_planning_max_slot_bytes";
    public static final String FORCE_ENABLE_MANIFEST_CACHE_WITHOUT_METRICS_WITH_DELETE_FILE =
            "force_enable_manifest_cache_without_metrics_with_delete_file";
    public static final String REFRESH_ICEBERG_MANIFEST_MIN_LENGTH = "refresh_iceberg_manifest_min_length";

    private final Map<String, String> properties;
    private IcebergCatalogType catalogType;
    private boolean enableIcebergMetadataCache = false;
    private long icebergMetaCacheTtlSec;
    private long icebergManifestCacheTtlSec;
    private int icebergJobPlanningThreadNum;
    private int refreshOtherFeIcebergCacheThreadNum;
    private int backgroundIcebergJobPlanningThreadNum;
    private boolean enableIcebergManifestCache;
    private boolean icebergManifestCacheWithMetrics;
    private long icebergManifestCacheMaxSize;
    private long localPlanningMaxSlotBytes;
    private boolean forceEnableManifestCacheWithoutMetricsWithDeleteFile;
    private long refreshIcebergManifestMinLength;

    public IcebergConfig(Map<String, String> catalogProperties) {
        this.properties = catalogProperties;
        init();
    }

    private void init() {
        initCatalogType();
        initIcebergMetadataCache();
        initIcebergJobPlanningThreadNum();
        initRefreshOtherFeThreadNum();
        initBackgroundIcebergJobPlanningThreadNum();
        initIcebergCacheTtl();
        initManifestCache();
        initLocalPlanningMaxSlotSize();
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
        if (properties.containsKey(ENABLE_ICEBERG_METADATA_CACHE)) {
            enableIcebergMetadataCache = Boolean.parseBoolean(properties.get(ENABLE_ICEBERG_METADATA_CACHE));
        } else {
            enableIcebergMetadataCache = catalogType == IcebergCatalogType.GLUE_CATALOG;
        }
    }

    private void initIcebergJobPlanningThreadNum() {
        this.icebergJobPlanningThreadNum = Math.max(2,
                PropertyUtil.propertyAsInt(properties, ICEBERG_JOB_PLANNING_THREAD_NUM, Config.iceberg_worker_num_threads));
    }

    private void initRefreshOtherFeThreadNum() {
        this.refreshOtherFeIcebergCacheThreadNum = Math.max(2,
                PropertyUtil.propertyAsInt(properties, REFRESH_OTHER_FE_ICEBERG_CACHE_THREAD_NUM, 4));
    }

    private void initBackgroundIcebergJobPlanningThreadNum() {
        int defaultPoolSize = Math.max(2, Runtime.getRuntime().availableProcessors() / 8);
        this.backgroundIcebergJobPlanningThreadNum =
                PropertyUtil.propertyAsInt(properties, BACKGROUND_ICEBERG_JOB_PLANNING_THREAD_NUM, defaultPoolSize);
    }

    private void initIcebergCacheTtl() {
        this.icebergMetaCacheTtlSec = PropertyUtil.propertyAsLong(properties, ICEBERG_META_CACHE_TTL, 1800);
        this.icebergManifestCacheTtlSec = PropertyUtil.propertyAsLong(properties, ICEBERG_MANIFEST_CACHE_TTL, 48 * 60 * 60);
    }

    private void initManifestCache() {
        this.enableIcebergManifestCache = PropertyUtil.propertyAsBoolean(properties, ENABLE_ICEBERG_MANIFEST_CACHE, true);
        this.icebergManifestCacheWithMetrics = PropertyUtil.propertyAsBoolean(
                properties, ICEBERG_MANIFEST_CACHE_WITH_METRICS, false);
        this.icebergManifestCacheMaxSize = PropertyUtil.propertyAsLong(properties, ICEBERG_MANIFEST_CACHE_MAX_SIZE, 100000);
        this.forceEnableManifestCacheWithoutMetricsWithDeleteFile =
                PropertyUtil.propertyAsBoolean(properties, FORCE_ENABLE_MANIFEST_CACHE_WITHOUT_METRICS_WITH_DELETE_FILE, false);
        this.refreshIcebergManifestMinLength = PropertyUtil.propertyAsLong(properties, REFRESH_ICEBERG_MANIFEST_MIN_LENGTH,
                2 * 1024 * 1024);
    }

    private void initLocalPlanningMaxSlotSize() {
        this.localPlanningMaxSlotBytes = PropertyUtil.propertyAsLong(
                properties, ICEBERG_LOCAL_PLANNING_MAX_SLOT_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
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

    public long getIcebergManifestCacheTtlSec() {
        return icebergManifestCacheTtlSec;
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

    public boolean enableIcebergManifestCache() {
        return enableIcebergManifestCache;
    }

    public boolean isIcebergManifestCacheWithMetrics() {
        return icebergManifestCacheWithMetrics;
    }

    public long getIcebergManifestCacheMaxSize() {
        return icebergManifestCacheMaxSize;
    }

    public long getLocalPlanningMaxSlotBytes() {
        return localPlanningMaxSlotBytes;
    }

    public boolean isForceEnableManifestCacheWithoutMetricsWithDeleteFile() {
        return forceEnableManifestCacheWithoutMetricsWithDeleteFile;
    }

    public long getRefreshIcebergManifestMinLength() {
        return refreshIcebergManifestMinLength;
    }
}
