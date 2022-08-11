// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariDataSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

// a cache for get datasource
public class DataSourceCache {
    private final Map<String, HikariDataSource> sources = new ConcurrentHashMap<>();
    private static final DataSourceCache instance = new DataSourceCache();

    public static DataSourceCache getInstance() {
        return instance;
    }

    public synchronized HikariDataSource getSource(String driverId) {
        return sources.get(driverId);
    }

    public HikariDataSource getSource(String driverId, Supplier<HikariDataSource> provider) {
        HikariDataSource targetSource = sources.get(driverId);
        if (targetSource == null) {
            synchronized (this) {
                if (targetSource == null) {
                    sources.put(driverId, provider.get());
                }
            }
            targetSource = sources.get(driverId);
        }
        return targetSource;
    }
}
