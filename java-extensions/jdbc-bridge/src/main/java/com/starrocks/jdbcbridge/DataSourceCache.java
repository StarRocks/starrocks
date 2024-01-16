// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariDataSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

// a cache for get datasource
public class DataSourceCache {
    public static final class DataSourceCacheItem {
        private final HikariDataSource hikariDataSource;
        private final ClassLoader classLoader;

        public DataSourceCacheItem(HikariDataSource hikariDataSource, ClassLoader classLoader) {
            this.hikariDataSource = hikariDataSource;
            this.classLoader = classLoader;
        }

        public HikariDataSource getHikariDataSource() {
            return hikariDataSource;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }

    private final Map<String, DataSourceCacheItem> sources = new ConcurrentHashMap<>();
    private static final DataSourceCache INSTANCE = new DataSourceCache();

    // presumed existing private constructor
    private DataSourceCache(){}

    public static DataSourceCache getInstance() {
        return INSTANCE;
    }

    public DataSourceCacheItem getSource(String driverId, Supplier<DataSourceCacheItem> provider) {
        if (driverId == null || driverId.isEmpty()) {
            throw new IllegalArgumentException("Driver ID cannot be null or empty");
        }

        return sources.computeIfAbsent(driverId, k -> {
            DataSourceCacheItem item = provider.get();
            if (item == null) {
                throw new NullPointerException("DataSourceCacheItem supplier returned null");
            }
            return item;
        });
    }
}