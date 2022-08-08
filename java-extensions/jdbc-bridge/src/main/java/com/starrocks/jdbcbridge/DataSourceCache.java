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

    public synchronized HikariDataSource getSource(String driver, String user, String passwd, String url) {
        return sources.get(driver + user + passwd + url);
    }

    public synchronized void put(String driver, String user, String passwd, String url, HikariDataSource source) {
        sources.put(driver + user + passwd + url, source);
    }

}
