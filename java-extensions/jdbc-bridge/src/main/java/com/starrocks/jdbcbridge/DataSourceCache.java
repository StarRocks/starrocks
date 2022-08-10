// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariDataSource;

import java.util.HashMap;
import java.util.Map;

// a cache for get datasource
public class DataSourceCache {
    private final Map<String, HikariDataSource> sources = new HashMap<>();
    private static final DataSourceCache instance = new DataSourceCache();

    public static DataSourceCache getInstance() {
        return instance;
    }

    public synchronized HikariDataSource getSource(String driverId) {
        return sources.get(driverId);
    }

    public synchronized void putIfAbsent(String driverId, HikariDataSource source) {
        sources.putIfAbsent(driverId, source);
    }

    public synchronized void release(String driverId, HikariDataSource source) {
        final HikariDataSource cached = sources.get(driverId);
        if (cached != source) {
            source.close();
        }
    }
}
