// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.config;

public class ExternalCatlogConfig {
    public static final ConfigProperty<Long> HIVE_META_CACHE_TTL_S = ConfigProperty
            .key("hive_meta_cache_ttl_s")
            .defaultValue(3600L * 24L)
            .sinceVersion("2.5")
            .withDocumentation("Hive metastore cache ttl.");

    public static final ConfigProperty<Long> METASTORE_CACHE_REFRESH_INTERVAL_SEC = ConfigProperty
            .key("metastore_cache_refresh_interval_sec")
            .defaultValue(3600L * 2L)
            .sinceVersion("2.5")
            .withDocumentation("Refresh hive meta cache interval.");

    public static final ConfigProperty<Boolean> ENABLE_CACHE_LIST_NAMES = ConfigProperty
            .key("enable_cache_list_names")
            .defaultValue(false)
            .withValidValues(true, false)
            .sinceVersion("2.5")
            .withDocumentation("Whether hive cache list names.");
}
