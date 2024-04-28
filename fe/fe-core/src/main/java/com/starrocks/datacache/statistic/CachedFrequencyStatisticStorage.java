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

package com.starrocks.datacache.statistic;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

public class CachedFrequencyStatisticStorage {
    private static final Cache<CacheKey, Long>
            CACHED_MAP = Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).build();

    public static void addAccessItems(List<AccessItem> accessItems) {
        for (AccessItem accessItem : accessItems) {
            addAccessItem(accessItem);
        }
    }

    public static void addAccessItem(AccessItem accessItem) {
        CacheKey cacheKey = new CacheKey(accessItem.getPartitionName().orElse(null), accessItem.getCatalogName(),
                accessItem.getDbName(),
                accessItem.getTableName(), accessItem.getColumnName());
        Long previousFrequency = CACHED_MAP.getIfPresent(cacheKey);
        if (previousFrequency == null) {
            previousFrequency = 0L;
        }
        CACHED_MAP.put(cacheKey, previousFrequency + 1);
    }

    public static List<AccessItem> exportMostAccessedTables(int limit) {
        PriorityQueue<Map.Entry<CacheKey, Long>> queue = new PriorityQueue<>(
                new Comparator<Map.Entry<CacheKey, Long>>() {
                    @Override
                    public int compare(Map.Entry<CacheKey, Long> o1, Map.Entry<CacheKey, Long> o2) {
                        return o2.getValue().compareTo(o1.getValue());
                    }
                });

        queue.addAll(CACHED_MAP.asMap().entrySet());

        List<AccessItem> result = new LinkedList<>();
        for (int i = 0; i < limit && !queue.isEmpty(); ++i) {
            Map.Entry<CacheKey, Long> queueItem = queue.poll();
            CacheKey cacheKey = queueItem.getKey();
            AccessItem item = new AccessItem(Optional.ofNullable(cacheKey.partition), cacheKey.catalogName, cacheKey.dbName,
                    cacheKey.tableName, cacheKey.columnName, 0L);
            item.setAccessFrequency(queueItem.getValue());
            result.add(item);
        }
        return result;
    }

    public static long size() {
        return CACHED_MAP.asMap().size();
    }

    private static class CacheKey {
        private final String partition;
        private final String catalogName;
        private final String dbName;
        private final String tableName;
        private final String columnName;

        public CacheKey(String partition, String catalogName, String dbName, String tableName, String columnName) {
            this.partition = partition;
            this.catalogName = catalogName;
            this.dbName = dbName;
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(catalogName, cacheKey.catalogName) &&
                    Objects.equals(dbName, cacheKey.dbName) &&
                    Objects.equals(tableName, cacheKey.tableName) &&
                    Objects.equals(columnName, cacheKey.columnName) &&
                    Objects.equals(partition, cacheKey.partition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, dbName, tableName, columnName, partition);
        }
    }
}
