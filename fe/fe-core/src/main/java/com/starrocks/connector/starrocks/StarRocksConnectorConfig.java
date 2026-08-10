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

package com.starrocks.connector.starrocks;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.starrocks.connector.config.Config;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.Locale;
import java.util.Map;

public class StarRocksConnectorConfig extends ConnectorConfig {
    public static final String FE_HTTP_URL = "starrocks.fe.http.url";
    public static final String USER = "starrocks.user";
    public static final String PASSWORD = "starrocks.password";
    public static final String SCAN_TRANSPORT = "starrocks.scan.transport";
    public static final String HTTP_TIMEOUT_MS = "starrocks.fe.http.timeout.ms";
    public static final String HTTP_RETRY_TIMES = "starrocks.fe.http.retry.times";
    public static final String CACHE_ENABLE = "starrocks.cache.enable";
    public static final String CACHE_REFRESH_SEC = "starrocks.cache.refresh.sec";
    public static final String CACHE_TTL_SEC = "starrocks.cache.ttl.sec";
    public static final String CACHE_TABLE_MAX_NUM = "starrocks.cache.table.max.num";
    public static final String CACHE_PARTITION_MAX_NUM = "starrocks.cache.partition.max.num";
    public static final String CACHE_REFRESH_THREAD_NUM = "starrocks.cache.refresh.thread.num";
    // Framework-level properties that are not declared as @Config fields here: "type" selects
    // this connector, and "catalog.access.control" picks the catalog's authorization engine and
    // is consumed generically by LazyConnector.
    public static final String CATALOG_TYPE = "type";
    public static final String CATALOG_ACCESS_CONTROL = "catalog.access.control";

    /**
     * Every property a StarRocks external catalog accepts: the @Config-backed ones below plus the
     * two framework keys above. Anything else fails the DDL — see rejectUnknownProperties. Keep
     * this in sync when adding a @Config field; StarRocksConnectorConfigTest asserts the two stay
     * aligned so the omission cannot ship.
     */
    public static final ImmutableSet<String> SUPPORTED_PROPERTIES = ImmutableSet.of(
            CATALOG_TYPE,
            CATALOG_ACCESS_CONTROL,
            FE_HTTP_URL,
            USER,
            PASSWORD,
            SCAN_TRANSPORT,
            HTTP_TIMEOUT_MS,
            HTTP_RETRY_TIMES,
            CACHE_ENABLE,
            CACHE_REFRESH_SEC,
            CACHE_TTL_SEC,
            CACHE_TABLE_MAX_NUM,
            CACHE_PARTITION_MAX_NUM,
            CACHE_REFRESH_THREAD_NUM);
    // Suffix of internal markers the persistence layer appends when obfuscating
    // credentials for the checkpoint image (see CatalogMgr.encryptCatalogForImage).
    // A user-supplied marker would make the image writer skip the obfuscation and
    // the image loader then fail to de-obfuscate the plaintext on restart, silently
    // dropping the catalog — reject it at DDL time.
    private static final String INTERNAL_MARKER_PROPERTY_SUFFIX = ".__encrypted";

    public static final String TRANSPORT_ARROW_FLIGHT = StarRocksRemoteScanWire.TRANSPORT_ARROW_FLIGHT;
    public static final String TRANSPORT_BRPC_CHUNK = StarRocksRemoteScanWire.TRANSPORT_BRPC_CHUNK;

    @Config(key = FE_HTTP_URL, desc = "Remote StarRocks FE http endpoints", defaultValue = "", nullable = false)
    private String feHttpUrl;

    @Config(key = USER, desc = "Remote StarRocks user", defaultValue = "", nullable = false)
    private String feUser;

    @Config(key = PASSWORD, desc = "Remote StarRocks password", defaultValue = "", nullable = true)
    private String fePassword;

    @Config(key = SCAN_TRANSPORT, desc = "Remote scan transport", defaultValue = TRANSPORT_BRPC_CHUNK,
            nullable = false)
    private String scanTransport;

    @Config(key = HTTP_TIMEOUT_MS, desc = "Remote FE http request timeout in milliseconds", defaultValue = "10000",
            nullable = false)
    private int httpTimeoutMs;

    @Config(key = HTTP_RETRY_TIMES, desc = "Remote FE http request retry times", defaultValue = "3", nullable = false)
    private int httpRetryTimes;

    // Primitive int: ConnectorConfig.loadConfig injects int via Field.setInt;
    // primitive long silently stays 0 and boxed Long fails Field.setLong.
    @Config(key = CACHE_ENABLE,
            desc = "Master switch of all caching in this catalog (metadata and statistics); "
                    + "when false every access goes straight to the remote FE",
            defaultValue = "true", nullable = false)
    private boolean cacheEnable;

    @Config(key = CACHE_REFRESH_SEC, desc = "Background refresh interval of the statistics snapshot cache in seconds",
            defaultValue = "300", nullable = false)
    private int cacheRefreshSec;

    @Config(key = CACHE_TTL_SEC, desc = "Hard expiry of all caches in seconds",
            defaultValue = "3600", nullable = false)
    private int cacheTtlSec;

    @Config(key = CACHE_TABLE_MAX_NUM, desc = "Max number of tables kept in the per-table caches "
            + "(table schema, table statistics snapshot)", defaultValue = "10000", nullable = false)
    private int cacheTableMaxNum;

    @Config(key = CACHE_PARTITION_MAX_NUM, desc = "Max number of (table, partition) entries kept in the "
            + "partition statistics cache", defaultValue = "100000", nullable = false)
    private int cachePartitionMaxNum;

    @Config(key = CACHE_REFRESH_THREAD_NUM, desc = "Thread pool size of this catalog's background statistics "
            + "cache refresh", defaultValue = "4", nullable = false)
    private int cacheRefreshThreadNum;

    @Override
    public void loadConfig(Map<String, String> properties) {
        rejectInternalMarkerProperties(properties);
        rejectUnknownProperties(properties);
        super.loadConfig(properties);
        // Validate eagerly at CREATE CATALOG time rather than deferring to first query.
        // ConnectorConfig.loadConfig swallows missing-required-field AnalysisExceptions and
        // just logs them, which silently produces an unusable catalog. Re-check the fields
        // that must be present for any meaningful operation against the remote cluster.
        if (Strings.isNullOrEmpty(feHttpUrl)) {
            throw new StarRocksConnectorException(
                    FE_HTTP_URL + " is required to create a StarRocks external catalog");
        }
        if (Strings.isNullOrEmpty(feUser)) {
            throw new StarRocksConnectorException(
                    USER + " is required to create a StarRocks external catalog");
        }
        // The URL may carry multiple endpoints; parsing them up front catches typos like
        // non-numeric ports before the catalog goes live and starts queries.
        StarRocksFeClient.parseFeAddresses(feHttpUrl);
    }

    /**
     * Rejects any property this catalog does not understand, so that a typo cannot be silently
     * dropped. {@link com.starrocks.connector.config.ConnectorConfig#loadConfig} only reads the
     * keys it recognizes and ignores the rest, which would leave a misspelled
     * {@code starrocks.cache.ttl} quietly running on the default while SHOW CREATE CATALOG still
     * echoes it back as if it took effect.
     *
     * <p>{@code ranger.plugin.hive.service.name} is deliberately absent from
     * {@link #SUPPORTED_PROPERTIES}: it binds the catalog to a Ranger service of type hive, whose
     * resource model only has database and table and therefore cannot express the catalog
     * dimension. It also silently takes precedence over {@code catalog.access.control}. Use
     * {@code catalog.access.control = ranger} instead, which goes through the StarRocks service
     * definition.
     */
    private static void rejectUnknownProperties(Map<String, String> properties) {
        if (properties == null) {
            return;
        }
        for (String key : properties.keySet()) {
            if (key != null && !SUPPORTED_PROPERTIES.contains(key)) {
                throw new StarRocksConnectorException(
                        "%s is not a supported property of a StarRocks external catalog. Supported properties: %s",
                        key, String.join(", ", SUPPORTED_PROPERTIES));
            }
        }
    }

    private static void rejectInternalMarkerProperties(Map<String, String> properties) {
        if (properties == null) {
            return;
        }
        for (String key : properties.keySet()) {
            if (key != null && key.endsWith(INTERNAL_MARKER_PROPERTY_SUFFIX)) {
                throw new StarRocksConnectorException(
                        key + " is an internal property and cannot be set");
            }
        }
    }

    public String getFeHttpUrl() {
        if (feHttpUrl.endsWith("/")) {
            return feHttpUrl.substring(0, feHttpUrl.length() - 1);
        }
        return feHttpUrl;
    }

    public String getFeUser() {
        return feUser;
    }

    public String getFePassword() {
        return fePassword;
    }

    public String getScanTransport() {
        String transport = scanTransport.toLowerCase(Locale.ROOT);
        if (!TRANSPORT_ARROW_FLIGHT.equals(transport) && !TRANSPORT_BRPC_CHUNK.equals(transport)) {
            throw new StarRocksConnectorException("unsupported starrocks scan transport: " + scanTransport);
        }
        return transport;
    }

    public long getFeHttpTimeoutMs() {
        return httpTimeoutMs;
    }

    public int getFeHttpRetryTimes() {
        return httpRetryTimes;
    }

    public boolean isCacheEnabled() {
        return cacheEnable;
    }

    public long getCacheRefreshSec() {
        return cacheRefreshSec <= 0 ? 300 : cacheRefreshSec;
    }

    public long getCacheTtlSec() {
        return cacheTtlSec <= 0 ? 3600 : cacheTtlSec;
    }

    public long getCacheTableMaxNum() {
        return cacheTableMaxNum <= 0 ? 10000 : cacheTableMaxNum;
    }

    public long getCachePartitionMaxNum() {
        return cachePartitionMaxNum <= 0 ? 100000 : cachePartitionMaxNum;
    }

    public int getCacheRefreshThreadNum() {
        return cacheRefreshThreadNum <= 0 ? 4 : cacheRefreshThreadNum;
    }

    public StarRocksMetadataCache.Options toCacheOptions() {
        return new StarRocksMetadataCache.Options(getCacheTtlSec(), getCacheRefreshSec(),
                getCacheTableMaxNum(), getCachePartitionMaxNum());
    }
}
