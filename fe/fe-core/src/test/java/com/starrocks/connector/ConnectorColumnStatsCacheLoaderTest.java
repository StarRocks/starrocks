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

package com.starrocks.connector;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.statistics.ConnectorColumnStatsCacheLoader;
import com.starrocks.connector.statistics.ConnectorHistogramColumnStatsCacheLoader;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectorColumnStatsCacheLoaderTest {
    public static ConnectContext connectContext;
    private AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>>
            connectorTableCachedStatistics =
            Caffeine.newBuilder().expireAfterWrite(Config.statistic_update_interval_sec * 2, TimeUnit.SECONDS)
                    .refreshAfterWrite(Config.statistic_update_interval_sec, TimeUnit.SECONDS)
                    .maximumSize(Config.statistic_cache_columns)
                    .buildAsync(new ConnectorColumnStatsCacheLoader());

    @BeforeAll
    public static void beforeClass() throws DdlException {
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testAsyncLoad() throws ExecutionException, InterruptedException {
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID, String column) {
                TStatisticData data1 = new TStatisticData();
                data1.setColumnName("r_regionkey");
                data1.setRowCount(5);
                data1.setDataSize(100);
                data1.setCountDistinct(5);
                data1.setNullCount(0);
                data1.setMin("0");
                data1.setMax("4");

                return ImmutableList.of(data1);
            }
        };

        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String tableUUID) {
                return connectContext.getGlobalStateMgr().getMetadataMgr().
                        getTable(connectContext, "hive0", "tpch", "region");
            }

        };
        CompletableFuture<Optional<ConnectorTableColumnStats>> future =
                connectorTableCachedStatistics.get(
                        new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey"));
        Optional<ConnectorTableColumnStats> result = future.get();

        Assertions.assertEquals(5, result.get().getRowCount());
        Assertions.assertEquals(20, result.get().getColumnStatistic().getAverageRowSize(), 0.0001);
        Assertions.assertEquals(4, result.get().getColumnStatistic().getMaxValue(), 0.0001);
        Assertions.assertEquals(0, result.get().getColumnStatistic().getMinValue(), 0.0001);
        Assertions.assertEquals(5, result.get().getColumnStatistic().getDistinctValuesCount(), 0.0001);
        Assertions.assertEquals(0, result.get().getColumnStatistic().getNullsFraction(), 0.0001);
    }

    @Test
    public void testAsyncLoadAll() throws ExecutionException, InterruptedException {
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID,
                                                            List<String> columns) {
                TStatisticData data1 = new TStatisticData();
                data1.setColumnName("r_regionkey");
                data1.setRowCount(5);
                data1.setDataSize(100);
                data1.setCountDistinct(5);
                data1.setNullCount(0);
                data1.setMin("0");
                data1.setMax("4");

                TStatisticData data2 = new TStatisticData();
                data2.setColumnName("r_name");
                data2.setRowCount(5);
                data2.setDataSize(100);
                data2.setCountDistinct(5);
                data2.setNullCount(0);
                data2.setMin("a");
                data2.setMax("z");

                TStatisticData data3 = new TStatisticData();
                data3.setColumnName("r_comment");
                data3.setRowCount(5);
                data3.setDataSize(100);
                data3.setCountDistinct(5);
                data3.setNullCount(0);
                data3.setMin("x");
                data3.setMax("y");

                return ImmutableList.of(data1, data2, data3);
            }
        };

        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String tableUUID) {
                return connectContext.getGlobalStateMgr().getMetadataMgr().
                        getTable(connectContext, "hive0", "tpch", "region");
            }

        };
        List<ConnectorTableColumnKey> cacheKeys = ImmutableList.of(
                new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey"),
                new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_name"),
                new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_comment"));
        CompletableFuture<Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>>> future =
                connectorTableCachedStatistics.getAll(cacheKeys);
        Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> result = future.get();
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(5, result.get(
                new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey")).get().getRowCount());
        Assertions.assertEquals(20, result.get(
                        new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey")).get().getColumnStatistic()
                .getAverageRowSize(), 0.0001);
        Assertions.assertEquals(4, result.get(
                        new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey")).get().getColumnStatistic()
                .getMaxValue(), 0.0001);
        Assertions.assertEquals(0, result.get(
                        new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey")).get().getColumnStatistic()
                .getMinValue(), 0.0001);
        Assertions.assertEquals(5, result.get(
                        new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey")).get().getColumnStatistic()
                .getDistinctValuesCount(), 0.0001);
        Assertions.assertEquals(0, result.get(
                        new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey")).get().getColumnStatistic()
                .getNullsFraction(), 0.0001);
    }

    @Test
    public void testGetConnectorHistogramStatistics() throws ExecutionException, InterruptedException {
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<Histogram>>
                connectorHistogramCache =
                Caffeine.newBuilder().expireAfterWrite(Config.statistic_update_interval_sec * 2, TimeUnit.SECONDS)
                        .refreshAfterWrite(Config.statistic_update_interval_sec, TimeUnit.SECONDS)
                        .maximumSize(Config.statistic_cache_columns)
                        .buildAsync(new ConnectorHistogramColumnStatsCacheLoader());

        new MockUp<ConnectorHistogramColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryHistogramStatistics(ConnectContext context, String tableUUID, List<String> column) {
                TStatisticData data1 = new TStatisticData();
                data1.setColumnName("c1");
                data1.setHistogram("{ \"buckets\" : [[\"2\",\"7\",\"6\",\"1\"],[\"8\",\"13\",\"12\",\"1\"]," +
                        "[\"14\",\"21\",\"18\",\"1\"],[\"22\",\"28\",\"24\",\"1\"],[\"29\",\"35\",\"30\",\"1\"]], " +
                        "\"mcv\" : [[\"27\",\"8\"],[\"19\",\"5\"],[\"20\",\"4\"]] }");

                TStatisticData data2 = new TStatisticData();
                data2.setColumnName("par_date");
                data2.setHistogram("{ \"buckets\" : [[\"2022-01-02\",\"2022-01-07\",\"6\",\"1\"]," +
                        "[\"2022-01-08\",\"2022-01-13\",\"12\",\"1\"]," +
                        "[\"2022-01-14\",\"2022-01-21\",\"18\",\"1\"]," +
                        "[\"2022-01-22\",\"2022-01-28\",\"24\",\"1\"]," +
                        "[\"2022-01-29\",\"2022-01-30\",\"30\",\"1\"]], " +
                        "\"mcv\" : [[\"2022-01-27\",\"8\"],[\"2022-01-19\",\"5\"],[\"2022-01-20\",\"4\"]] }");
                return ImmutableList.of(data1, data2);
            }
        };

        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String tableUUID) {
                return connectContext.getGlobalStateMgr().getMetadataMgr().
                        getTable(connectContext, "hive0", "partitioned_db", "t1_par");
            }

        };

        List<ConnectorTableColumnKey> cacheKeys = ImmutableList.of(
                new ConnectorTableColumnKey("hive0.partitioned_db.t1_par.1234", "c1"),
                new ConnectorTableColumnKey("hive0.partitioned_db.t1_par.1234", "c2"),
                new ConnectorTableColumnKey("hive0.partitioned_db.t1_par.1234", "par_date"));
        CompletableFuture<Map<ConnectorTableColumnKey, Optional<Histogram>>> future =
                connectorHistogramCache.getAll(cacheKeys);
        Map<ConnectorTableColumnKey, Optional<Histogram>> result = future.get();
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(5, result.get(new ConnectorTableColumnKey("hive0.partitioned_db.t1_par.1234",
                "c1")).get().getBuckets().size());
        Assertions.assertEquals(3, result.get(new ConnectorTableColumnKey("hive0.partitioned_db.t1_par.1234",
                "c1")).get().getMCV().size());
        Assertions.assertEquals(5, result.get(new ConnectorTableColumnKey("hive0.partitioned_db.t1_par.1234",
                "par_date")).get().getBuckets().size());
        Assertions.assertEquals(3, result.get(new ConnectorTableColumnKey("hive0.partitioned_db.t1_par.1234",
                "par_date")).get().getMCV().size());
        Assertions.assertEquals("MCV: [[27:8][19:5][20:4]]", result.get(new ConnectorTableColumnKey(
                "hive0.partitioned_db.t1_par.1234", "c1")).get().getMcvString());
        Assertions.assertEquals("MCV: [[2022-01-27:8][2022-01-19:5][2022-01-20:4]]", result.get(new ConnectorTableColumnKey(
                "hive0.partitioned_db.t1_par.1234", "par_date")).get().getMcvString());
    }

    private static TStatisticData regionKeyStats() {
        TStatisticData data = new TStatisticData();
        data.setColumnName("r_regionkey");
        data.setRowCount(5);
        data.setDataSize(100);
        data.setCountDistinct(5);
        data.setNullCount(0);
        data.setMin("0");
        data.setMax("4");
        return data;
    }

    private static AtomicBoolean mockReady(boolean initial) {
        AtomicBoolean ready = new AtomicBoolean(initial);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isReady() {
                return ready.get();
            }
        };
        return ready;
    }

    @Test
    public void testLoadBeforeReadyIsNotCached() throws ExecutionException, InterruptedException {
        AtomicInteger queries = new AtomicInteger();
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID,
                                                            List<String> columns) {
                queries.incrementAndGet();
                return ImmutableList.of(regionKeyStats());
            }
        };
        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String tableUUID) {
                return connectContext.getGlobalStateMgr().getMetadataMgr().
                        getTable(connectContext, "hive0", "tpch", "region");
            }
        };
        AtomicBoolean ready = mockReady(false);

        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> cache =
                // Load on the calling thread: with an async executor, Caffeine removes an entry whose load
                // completed empty in a callback that can still be pending when get() returns.
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ConnectorColumnStatsCacheLoader());
        ConnectorTableColumnKey regionKey = new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey");
        ConnectorTableColumnKey name = new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_name");

        // Before ready: nothing is queried, and nothing is cached as "no statistics".
        Assertions.assertTrue(cache.getAll(ImmutableList.of(regionKey, name)).get().isEmpty());
        Assertions.assertNull(cache.get(regionKey).get());
        Assertions.assertEquals(0, queries.get());
        Assertions.assertTrue(cache.synchronous().asMap().isEmpty());

        // Once ready, the same keys load the real statistics; a column without any is cached as empty.
        ready.set(true);
        Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> result =
                cache.getAll(ImmutableList.of(regionKey, name)).get();
        Assertions.assertEquals(1, queries.get());
        Assertions.assertEquals(5, result.get(regionKey).get().getRowCount());
        Assertions.assertFalse(result.get(name).isPresent());
        Assertions.assertEquals(2, cache.synchronous().asMap().size());
    }

    @Test
    public void testReloadBeforeReadyKeepsOldValue() throws ExecutionException, InterruptedException {
        mockReady(false);
        ConnectorTableColumnKey key = new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey");

        Optional<ConnectorTableColumnStats> columnStats = Optional.of(new ConnectorTableColumnStats(
                ColumnStatistic.builder().setDistinctValuesCount(5).build(), 5, "2024-01-01 01:00:00"));
        Assertions.assertSame(columnStats, new ConnectorColumnStatsCacheLoader()
                .asyncReload(key, columnStats, Runnable::run).get());

        Optional<Histogram> histogram = Optional.of(new Histogram(Lists.newArrayList(), Maps.newHashMap()));
        Assertions.assertSame(histogram, new ConnectorHistogramColumnStatsCacheLoader()
                .asyncReload(key, histogram, Runnable::run).get());
    }

    @Test
    public void testHistogramLoadBeforeReadyIsNotCached() throws ExecutionException, InterruptedException {
        AtomicInteger queries = new AtomicInteger();
        new MockUp<ConnectorHistogramColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryHistogramStatistics(ConnectContext context, String tableUUID,
                                                                 List<String> column) {
                queries.incrementAndGet();
                return ImmutableList.of();
            }
        };
        AtomicBoolean ready = mockReady(false);

        AsyncLoadingCache<ConnectorTableColumnKey, Optional<Histogram>> cache =
                // Load on the calling thread: with an async executor, Caffeine removes an entry whose load
                // completed empty in a callback that can still be pending when get() returns.
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ConnectorHistogramColumnStatsCacheLoader());
        ConnectorTableColumnKey key = new ConnectorTableColumnKey("hive0.partitioned_db.t1_par.1234", "c1");

        Assertions.assertTrue(cache.getAll(ImmutableList.of(key)).get().isEmpty());
        Assertions.assertNull(cache.get(key).get());
        Assertions.assertEquals(0, queries.get());
        Assertions.assertTrue(cache.synchronous().asMap().isEmpty());

        ready.set(true);
        Assertions.assertFalse(cache.getAll(ImmutableList.of(key)).get().get(key).isPresent());
        Assertions.assertEquals(1, queries.get());
        Assertions.assertEquals(1, cache.synchronous().asMap().size());
    }

    @Test
    public void testFailedLoadIsNotCached() throws ExecutionException, InterruptedException {
        AtomicInteger queries = new AtomicInteger();
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID,
                                                            List<String> columns) {
                if (queries.incrementAndGet() == 1) {
                    throw new RuntimeException("transient statistics query failure");
                }
                return ImmutableList.of(regionKeyStats());
            }
        };
        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String tableUUID) {
                return connectContext.getGlobalStateMgr().getMetadataMgr().
                        getTable(connectContext, "hive0", "tpch", "region");
            }
        };
        mockReady(true);

        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ConnectorColumnStatsCacheLoader());
        ConnectorTableColumnKey key = new ConnectorTableColumnKey("hive0.tpch.region.1234", "r_regionkey");

        // A failed load completes exceptionally and is not cached as "no statistics".
        Assertions.assertThrows(ExecutionException.class, () -> cache.get(key).get());
        Assertions.assertNull(cache.synchronous().getIfPresent(key));

        // The next access loads again and recovers.
        Assertions.assertEquals(5, cache.get(key).get().get().getRowCount());
        Assertions.assertEquals(2, queries.get());

        // A failed refresh keeps the old value instead of replacing it with an empty one.
        queries.set(0);
        cache.synchronous().refresh(key);
        Assertions.assertEquals(1, queries.get());
        Assertions.assertEquals(5, cache.synchronous().getIfPresent(key).get().getRowCount());
    }

    @Test
    public void testFailedHistogramLoadAllIsNotCached() throws ExecutionException, InterruptedException {
        AtomicInteger queries = new AtomicInteger();
        new MockUp<ConnectorHistogramColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryHistogramStatistics(ConnectContext context, String tableUUID,
                                                                 List<String> column) {
                if (queries.incrementAndGet() == 1) {
                    throw new RuntimeException("transient histogram query failure");
                }
                return ImmutableList.of();
            }
        };
        mockReady(true);

        AsyncLoadingCache<ConnectorTableColumnKey, Optional<Histogram>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ConnectorHistogramColumnStatsCacheLoader());
        ConnectorTableColumnKey key = new ConnectorTableColumnKey("hive0.partitioned_db.t1_par.1234", "c1");

        Assertions.assertThrows(ExecutionException.class, () -> cache.getAll(ImmutableList.of(key)).get());
        Assertions.assertNull(cache.synchronous().getIfPresent(key));

        Assertions.assertFalse(cache.getAll(ImmutableList.of(key)).get().get(key).isPresent());
        Assertions.assertEquals(2, queries.get());
    }

    @Test
    public void testConvert2ColumnStatistics() {
        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String tableUUID) {
                return connectContext.getGlobalStateMgr().getMetadataMgr().
                        getTable(connectContext, "hive0", "partitioned_db", "t1");
            }
        };
        ConnectorColumnStatsCacheLoader cachedStatisticStorage =
                Deencapsulation.newInstance(ConnectorColumnStatsCacheLoader.class);

        TStatisticData statisticData = new TStatisticData();
        statisticData.setColumnName("c1");
        statisticData.setMax("123");
        statisticData.setMin("0");

        ConnectorTableColumnStats columnStatistic =
                Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics",
                        connectContext,
                        "hive0.partitioned_db.t1.1234",
                        statisticData);
        Assertions.assertEquals(123, columnStatistic.getColumnStatistic().getMaxValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getColumnStatistic().getMinValue(), 0.001);
    }
}
