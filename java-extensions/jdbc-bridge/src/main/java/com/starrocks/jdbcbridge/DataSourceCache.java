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

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariDataSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

// a cache for get datasource
public class DataSourceCache {
    public static class DataSourceCacheItem {
        public HikariDataSource hikariDataSource;
        public ClassLoader classLoader;
        public DataSourceCacheItem(HikariDataSource hikariDataSource, ClassLoader classLoader) {
            this.hikariDataSource = hikariDataSource;
            this.classLoader = classLoader;
        }
    }
    private final Map<String, DataSourceCacheItem> sources = new ConcurrentHashMap<>();
    private static final DataSourceCache INSTANCE = new DataSourceCache();

    public static DataSourceCache getInstance() {
        return INSTANCE;
    }

    public DataSourceCacheItem getSource(String driverId, Supplier<DataSourceCacheItem> provider) {
        return sources.computeIfAbsent(driverId, k -> provider.get());
    }
}
