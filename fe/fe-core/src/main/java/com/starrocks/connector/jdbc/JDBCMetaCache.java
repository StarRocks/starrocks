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


package com.starrocks.connector.jdbc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.common.conf.Config;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class JDBCMetaCache<K, V> {

    private boolean enableCache = false;
    private Cache<K, V> metaCache;
    private long currentExpireSec;

    private final String jdbcMetaCacheEnable = "jdbc_meta_cache_enable";
    private final String jdbcMetaCacheExpireSec = "jdbc_meta_cache_expire_sec";


    public JDBCMetaCache(Map<String, String> properties, Boolean permanent) {

        if (permanent) {
            this.metaCache = Caffeine.newBuilder().build();
        } else if (checkEnableCache(properties)) {
            initializeExpireSec(properties);
            this.metaCache = Caffeine.newBuilder()
                    .expireAfterWrite(currentExpireSec, TimeUnit.SECONDS)
                    .build();
        }
    }

    private boolean checkEnableCache(Map<String, String> properties) {
        // The priority of jdbc_meta_cache_enable from properties is higher than that from Config
        String enableFromProperties = properties.get(jdbcMetaCacheEnable);
        if (enableFromProperties != null) {
            return this.enableCache = Boolean.parseBoolean(enableFromProperties);
        } else {
            return this.enableCache = Config.jdbc_meta_default_cache_enable;
        }
    }

    private void initializeExpireSec(Map<String, String> properties) {
        String expireSec = properties.get(jdbcMetaCacheExpireSec);
        if (expireSec != null) {
            this.currentExpireSec = Long.parseLong(expireSec);
        } else {
            currentExpireSec = Config.jdbc_meta_default_cache_expire_sec;
        }
    }

    public @NonNull V get(@NonNull K key, @NonNull Function<K, V> function) {
        if (this.enableCache) {
            return Objects.requireNonNull(this.metaCache.get(key, function));
        } else {
            return function.apply(key);
        }
    }

    public @NonNull V getPersistentCache(@NonNull K key, @NonNull Function<K, V> function) {
        return Objects.requireNonNull(this.metaCache.get(key, function));
    }

    public void invalidate(@NonNull K key) {
        if (this.metaCache != null) {
            this.metaCache.invalidate(key);
        }
    }

    public boolean isEnableCache() {
        return enableCache;
    }

    public long getCurrentExpireSec() {
        return currentExpireSec;
    }
}
