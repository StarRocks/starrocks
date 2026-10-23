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

package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.statistic.StatisticUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

public class CachedStatisticStorageTableStatsTest {
    private static final long TABLE_ID = 20001L;

    private List<Partition> partitions;

    @BeforeEach
    public void setUp() {
        new MockUp<StatisticUtils>() {
            @Mock
            public boolean statisticTableBlackListCheck(long tableId) {
                return false;
            }
        };
        partitions = Lists.newArrayList();
        for (long id = 1; id <= 4; id++) {
            partitions.add(new Partition(id, "p" + id, new RandomDistributionInfo(1)));
        }
    }

    // Records the partitions it is asked to load and keeps each load pending until the test completes it, loading
    // the row count partitionId * 100 for every partition.
    private static class PendingLoader implements AsyncCacheLoader<TableStatsCacheKey, Optional<Long>> {
        private final List<Set<Long>> requests = new CopyOnWriteArrayList<>();
        private final Map<CompletableFuture<Map<TableStatsCacheKey, Optional<Long>>>, List<TableStatsCacheKey>> pending =
                new HashMap<>();

        @Override
        public CompletableFuture<Optional<Long>> asyncLoad(TableStatsCacheKey key, Executor executor) {
            return asyncLoadAll(List.of(key), executor).thenApply(result -> result.get(key));
        }

        @Override
        public synchronized CompletableFuture<Map<TableStatsCacheKey, Optional<Long>>> asyncLoadAll(
                Iterable<? extends TableStatsCacheKey> keys, Executor executor) {
            List<TableStatsCacheKey> keyList = Lists.newArrayList(keys);
            Set<Long> partitionIds = new HashSet<>();
            keyList.forEach(key -> partitionIds.add(key.getPartitionId()));
            requests.add(partitionIds);
            CompletableFuture<Map<TableStatsCacheKey, Optional<Long>>> future = new CompletableFuture<>();
            pending.put(future, keyList);
            return future;
        }

        synchronized void completeAll() {
            pending.forEach((future, keys) -> {
                Map<TableStatsCacheKey, Optional<Long>> rowCounts = new HashMap<>();
                keys.forEach(key -> rowCounts.put(key, Optional.of(key.getPartitionId() * 100)));
                future.complete(rowCounts);
            });
            pending.clear();
        }
    }

    private static void putLoaded(CachedStatisticStorage storage, long partitionId, Optional<Long> rowCount) {
        storage.tableStatsCache.put(new TableStatsCacheKey(TABLE_ID, partitionId),
                CompletableFuture.completedFuture(rowCount));
    }

    @Test
    public void testGetTableStatisticsKeepsLoadedPartitionsWhileOthersLoad() {
        PendingLoader loader = new PendingLoader();
        CachedStatisticStorage storage = new CachedStatisticStorage();
        storage.tableStatsCache = Caffeine.newBuilder().buildAsync(loader);
        // p1 is loaded, p2 is loaded but has no statistics, p3 and p4 are not cached
        putLoaded(storage, 1L, Optional.of(100L));
        putLoaded(storage, 2L, Optional.empty());

        // Before, the row count of every partition was discarded while p3 and p4 were loading, p1's as well.
        Map<Long, Optional<Long>> rowCounts = storage.getTableStatistics(TABLE_ID, partitions);
        Assertions.assertEquals(ImmutableMap.of(1L, Optional.of(100L), 2L, Optional.empty()), rowCounts);
        // the partitions not cached load in one batch
        Assertions.assertEquals(List.of(Set.of(3L, 4L)), loader.requests);

        // Still loading: neither loaded again nor reported.
        rowCounts = storage.getTableStatistics(TABLE_ID, partitions);
        Assertions.assertEquals(ImmutableMap.of(1L, Optional.of(100L), 2L, Optional.empty()), rowCounts);
        Assertions.assertEquals(1, loader.requests.size());

        // Once every partition is loaded, all of them are returned, as before.
        loader.completeAll();
        rowCounts = storage.getTableStatistics(TABLE_ID, partitions);
        Assertions.assertEquals(ImmutableMap.of(1L, Optional.of(100L), 2L, Optional.empty(),
                3L, Optional.of(300L), 4L, Optional.of(400L)), rowCounts);
        Assertions.assertEquals(1, loader.requests.size());
    }

    @Test
    public void testReadyPartitionProbesDoNotDoubleCountCacheMetrics() {
        PendingLoader loader = new PendingLoader();
        CachedStatisticStorage storage = new CachedStatisticStorage();
        storage.tableStatsCache = Caffeine.newBuilder().recordStats().buildAsync(loader);
        putLoaded(storage, 1L, Optional.of(100L));
        putLoaded(storage, 2L, Optional.empty());

        Map<Long, Optional<Long>> rowCounts = storage.getTableStatistics(TABLE_ID, partitions);
        Assertions.assertEquals(ImmutableMap.of(1L, Optional.of(100L), 2L, Optional.empty()), rowCounts);
        // Only the bulk request counts: probing ready entries must not inflate the cache hit/miss metrics.
        Assertions.assertEquals(2, storage.tableStatsCache.synchronous().stats().hitCount());
        Assertions.assertEquals(2, storage.tableStatsCache.synchronous().stats().missCount());
        Assertions.assertEquals(List.of(Set.of(3L, 4L)), loader.requests);
    }

    @Test
    public void testGetTableStatisticsOfOnePartitionStillLoading() {
        PendingLoader loader = new PendingLoader();
        CachedStatisticStorage storage = new CachedStatisticStorage();
        storage.tableStatsCache = Caffeine.newBuilder().buildAsync(loader);
        putLoaded(storage, 1L, Optional.of(100L));
        putLoaded(storage, 2L, Optional.of(200L));
        putLoaded(storage, 3L, Optional.of(300L));

        // Only p4 is not cached: the three loaded partitions are kept and only p4 is loaded.
        Map<Long, Optional<Long>> rowCounts = storage.getTableStatistics(TABLE_ID, partitions);
        Assertions.assertEquals(ImmutableMap.of(1L, Optional.of(100L), 2L, Optional.of(200L), 3L, Optional.of(300L)),
                rowCounts);
        Assertions.assertFalse(rowCounts.containsKey(4L));
        Assertions.assertEquals(List.of(Set.of(4L)), loader.requests);
    }
}
