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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.statistics.ConnectorColumnStatsCacheLoader;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.connector.statistics.ConnectorTableTriggerAnalyzeMgr;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.metric.CounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.StarRocksMetricRegistry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.LocalDateTime;
import java.util.ArrayList;
<<<<<<< HEAD
=======
import java.util.Collection;
import java.util.Collections;
>>>>>>> c0000f582c3... [BugFix][RB-699] Bound partition statistics refresh batches (#66794)
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.starrocks.metric.MetricRepo.SYNC_STATS_LOAD_BUDGET_EXHAUSTED_TOTAL;

public class CachedStatisticStorageTest {
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    public static final String DEFAULT_CREATE_TABLE_TEMPLATE = ""
            + "CREATE TABLE IF NOT EXISTS `table_statistic_v1` (\n"
            + "  `table_id` bigint NOT NULL,\n"
            + "  `column_name` varchar(65530) NOT NULL,\n"
            + "  `db_id` bigint NOT NULL,\n"
            + "  `table_name` varchar(65530) NOT NULL,\n"
            + "  `db_name` varchar(65530) NOT NULL,\n"
            + "  `row_count` bigint NOT NULL,\n"
            + "  `data_size` bigint NOT NULL,\n"
            + "  `distinct_count` bigint NOT NULL,\n"
            + "  `null_count` bigint NOT NULL,\n"
            + "  `max` varchar(65530) NOT NULL,\n"
            + "  `min` varchar(65530) NOT NULL,\n"
            + "  `update_time` datetime NOT NULL\n"
            + "  )\n"
            + "ENGINE=OLAP\n"
            + "UNIQUE KEY(`table_id`,  `column_name`, `db_id`)\n"
            + "DISTRIBUTED BY HASH(`table_id`, `column_name`, `db_id`) BUCKETS 2\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"1\",\n"
            + "\"in_memory\" = \"false\"\n"
            + ");";

    public static void createStatisticsTable() throws Exception {
        CreateDbStmt dbStmt = new CreateDbStmt(false, StatsConstants.STATISTICS_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(dbStmt.getFullDbName());
        } catch (DdlException e) {
            return;
        }
        starRocksAssert.useDatabase(StatsConstants.STATISTICS_DB_NAME);
        starRocksAssert.withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);

        createStatisticsTable();
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL COMMENT \"\",\n" +
                "  `v4` date NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Mocked
    StatisticExecutor statisticExecutor;

    @Test
    public void testGetColumnStatistic(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");

        new Expectations() {
            {
                cachedStatisticStorage.getColumnStatistic(table, "v1");
                result = ColumnStatistic.builder().setDistinctValuesCount(888).build();
                minTimes = 0;

                cachedStatisticStorage.getColumnStatistic(table, "v2");
                result = ColumnStatistic.builder().setDistinctValuesCount(999).build();

                cachedStatisticStorage.getColumnStatistic(table, "v3");
                result = ColumnStatistic.builder().setDistinctValuesCount(666).build();
            }
        };
        ColumnStatistic columnStatistic1 =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v1");
        Assertions.assertEquals(888, columnStatistic1.getDistinctValuesCount(), 0.001);

        ColumnStatistic columnStatistic2 =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v2");
        Assertions.assertEquals(999, columnStatistic2.getDistinctValuesCount(), 0.001);

        ColumnStatistic columnStatistic3 =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v3");
        Assertions.assertEquals(666, columnStatistic3.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testGetColumnStatistics(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");

        ColumnStatistic columnStatistic1 = ColumnStatistic.builder().setDistinctValuesCount(888).build();
        ColumnStatistic columnStatistic2 = ColumnStatistic.builder().setDistinctValuesCount(999).build();

        new Expectations() {
            {
                cachedStatisticStorage.getColumnStatistics(table, ImmutableList.of("v1", "v2"));
                result = ImmutableList.of(columnStatistic1, columnStatistic2);
                minTimes = 0;
            }
        };
        List<ColumnStatistic> columnStatistics = Deencapsulation
                .invoke(cachedStatisticStorage, "getColumnStatistics", table, ImmutableList.of("v1", "v2"));
        Assertions.assertEquals(2, columnStatistics.size());
        Assertions.assertEquals(888, columnStatistics.get(0).getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(999, columnStatistics.get(1).getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testGetHiveColumnStatistics(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "tpch", "region");

        ColumnStatistic columnStatistic1 = ColumnStatistic.builder().setDistinctValuesCount(888).build();
        ColumnStatistic columnStatistic2 = ColumnStatistic.builder().setDistinctValuesCount(999).build();
        ConnectorTableColumnStats connectorTableColumnStats1 =
                new ConnectorTableColumnStats(columnStatistic1, 5, "2024-01-01 01:00:00");
        ConnectorTableColumnStats connectorTableColumnStats2 =
                new ConnectorTableColumnStats(columnStatistic2, 5, "2024-01-01 02:00:00");

        new Expectations() {
            {
                cachedStatisticStorage.getConnectorTableStatistics(table, ImmutableList.of("r_regionkey", "r_name"));
                result = ImmutableList.of(connectorTableColumnStats1, connectorTableColumnStats2);
                minTimes = 0;
            }
        };
        List<ConnectorTableColumnStats> columnStatistics = Deencapsulation
                .invoke(cachedStatisticStorage, "getConnectorTableStatistics", table,
                        ImmutableList.of("r_regionkey", "r_name"));
        Assertions.assertEquals(2, columnStatistics.size());
        Assertions.assertEquals(888, columnStatistics.get(0).getColumnStatistic().getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(999, columnStatistics.get(1).getColumnStatistic().getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(5, columnStatistics.get(0).getRowCount());
        Assertions.assertEquals(5, columnStatistics.get(1).getRowCount());
        Assertions.assertEquals("2024-01-01 01:00:00", columnStatistics.get(0).getUpdateTime());
        Assertions.assertEquals("2024-01-01 02:00:00", columnStatistics.get(1).getUpdateTime());
    }

    @Test
    public void testGetConnectorTableStatistics() throws ExecutionException, InterruptedException {
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>>
                connectorTableCachedStatistics =
                Caffeine.newBuilder().expireAfterWrite(Config.statistic_update_interval_sec * 2, TimeUnit.SECONDS)
                        .refreshAfterWrite(Config.statistic_update_interval_sec, TimeUnit.SECONDS)
                        .maximumSize(Config.statistic_cache_columns)
                        .buildAsync(new ConnectorColumnStatsCacheLoader());
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID,
                                                            List<String> columns) {
                TStatisticData data1 = new TStatisticData();
                data1.setColumnName("c1");
                data1.setRowCount(5);
                data1.setDataSize(100);
                data1.setCountDistinct(5);
                data1.setNullCount(0);
                data1.setMin("0");
                data1.setMax("10");

                TStatisticData data2 = new TStatisticData();
                data2.setColumnName("c2");
                data2.setRowCount(5);
                data2.setDataSize(100);
                data2.setCountDistinct(5);
                data2.setNullCount(0);
                data2.setMin("a");
                data2.setMax("z");

                TStatisticData data3 = new TStatisticData();
                data3.setColumnName("c3");
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
                        getTable(connectContext, "hive0", "partitioned_db", "t1");
            }

        };

        List<ConnectorTableColumnKey> cacheKeys = ImmutableList.of(
                new ConnectorTableColumnKey("hive0.partitioned_db.t1.1234", "c1"),
                new ConnectorTableColumnKey("hive0.partitioned_db.t1.1234", "c2"),
                new ConnectorTableColumnKey("hive0.partitioned_db.t1.1234", "c3"));
        CompletableFuture<Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>>> future =
                connectorTableCachedStatistics.getAll(cacheKeys);
        Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> result = future.get();

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(5, result.get(new ConnectorTableColumnKey("hive0.partitioned_db.t1.1234",
                "c1")).get().getRowCount());
        Assertions.assertEquals(20, result.get(new ConnectorTableColumnKey("hive0.partitioned_db.t1.1234",
                "c1")).get().getColumnStatistic().getAverageRowSize(), 0.0001);
        Assertions.assertEquals(10, result.get(new ConnectorTableColumnKey("hive0.partitioned_db.t1.1234",
                "c1")).get().getColumnStatistic().getMaxValue(), 0.0001);
        Assertions.assertEquals(0, result.get(new ConnectorTableColumnKey("hive0.partitioned_db.t1.1234",
                "c1")).get().getColumnStatistic().getMinValue(), 0.0001);
    }

    @Test
    public void testConnectorTableStatisticsResolveAnalyzeMetaOnce() {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr()
                .getTable(connectContext, "hive0", "partitioned_db", "t1");
        new MockUp<StatisticUtils>() {
            @Mock
            public static boolean checkStatisticTableStateNormal() {
                return true;
            }
        };
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID,
                                                            List<String> columns) {
                List<TStatisticData> rows = new ArrayList<>();
                for (String column : columns) {
                    TStatisticData data = new TStatisticData();
                    data.setColumnName(column);
                    data.setRowCount(5);
                    data.setDataSize(100);
                    data.setCountDistinct(5);
                    data.setNullCount(0);
                    data.setMin("0");
                    data.setMax("10");
                    rows.add(data);
                }
                return rows;
            }
        };
        // Resolving the analyze metadata rebuilds the table; it must happen once per call, not once per column.
        AtomicInteger lookups = new AtomicInteger();
        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String tableUUID) {
                return table;
            }

            @Mock
            public ExternalBasicStatsMeta getExternalBasicStatsMeta(Table t) {
                lookups.incrementAndGet();
                return null;
            }
        };

        CachedStatisticStorage storage = new CachedStatisticStorage();
        // Load inline so the first lookup already finds the statistics.
        Deencapsulation.setField(storage, "connectorTableCachedStatistics",
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ConnectorColumnStatsCacheLoader()));
        try {
            List<ConnectorTableColumnStats> stats =
                    storage.getConnectorTableStatistics(table, ImmutableList.of("c1", "c2", "c3"));
            Assertions.assertEquals(3, stats.size());
            Assertions.assertEquals(1, lookups.get());
            // No analyze metadata still means unknown for every column, as before.
            Assertions.assertTrue(stats.stream().allMatch(ConnectorTableColumnStats::isUnknown));
        } finally {
            // The inline load clears the thread's context when it finishes.
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testFollowerQueryTriggersAnalyze() throws Exception {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr()
                .getTable(connectContext, "hive0", "partitioned_db", "t1");
        new MockUp<StatisticUtils>() {
            @Mock
            public static boolean checkStatisticTableStateNormal() {
                return true;
            }
        };
        // no statistics collected yet
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID,
                                                            List<String> columns) {
                return new ArrayList<>();
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }
        };
        CountDownLatch triggered = new CountDownLatch(1);
        new MockUp<ConnectorTableTriggerAnalyzeMgr>() {
            @Mock
            public void checkAndUpdateTableStats(
                    Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> columnStats) {
                triggered.countDown();
            }
        };

        CachedStatisticStorage storage = new CachedStatisticStorage();
        Deencapsulation.setField(storage, "connectorTableCachedStatistics",
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ConnectorColumnStatsCacheLoader()));
        try {
            storage.getConnectorTableStatistics(table, ImmutableList.of("c1"));
            // a follower's query reaches the trigger manager, which forwards the request to the leader
            Assertions.assertTrue(triggered.await(30, TimeUnit.SECONDS));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testGetConnectorTableStatisticsSync(
            @Mocked AsyncLoadingCache<ConnectorTableColumnKey,
                    Optional<ConnectorTableColumnStats>> connectorTableCachedStatistics,
            @Mocked LoadingCache<ConnectorTableColumnKey,
                    Optional<ConnectorTableColumnStats>> connectorTableTableSyncCachedStatistics) {
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db", "t1");

        Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> columnKeyOptionalMap = Maps.newHashMap();
        columnKeyOptionalMap.put(new ConnectorTableColumnKey(table.getUUID(), "c1"),
                Optional.of(new ConnectorTableColumnStats(
                        new ColumnStatistic(0, 10, 0, 20, 5), 5, "")));
        columnKeyOptionalMap.put(new ConnectorTableColumnKey(table.getUUID(), "c2"),
                Optional.of(new ConnectorTableColumnStats(
                        new ColumnStatistic(0, 100, 0, 200, 50), 50, "")));

        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTableStateNormal() {
                return true;
            }
        };

        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        List<ConnectorTableColumnStats> connectorColumnStatistics = cachedStatisticStorage.
                getConnectorTableStatisticsSync(table, ImmutableList.of("c1", "c2"));
        Assertions.assertEquals(2, connectorColumnStatistics.size());

        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTableStateNormal() {
                return false;
            }
        };
        connectorColumnStatistics = cachedStatisticStorage.
                getConnectorTableStatisticsSync(table, ImmutableList.of("c1", "c2"));
        Assertions.assertEquals(2, connectorColumnStatistics.size());
        Assertions.assertTrue(connectorColumnStatistics.get(0).getColumnStatistic().isUnknown());
        Assertions.assertTrue(connectorColumnStatistics.get(1).getColumnStatistic().isUnknown());
    }

    @Test
    public void testExpireConnectorTableColumnStatistics() {
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db", "t1");
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        try {
            cachedStatisticStorage.expireConnectorTableColumnStatistics(table, ImmutableList.of("c1", "c2"));
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testInvalidateConnectorTableColumnStatisticsByUuid() {
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        try {
            // null / empty UUID is a no-op and must not throw.
            cachedStatisticStorage.invalidateConnectorTableColumnStatistics(null, ImmutableList.of("c1"));
            cachedStatisticStorage.invalidateConnectorTableColumnStatistics("", ImmutableList.of("c1"));
            // A normal UUID invalidates by key without resolving the table.
            cachedStatisticStorage.invalidateConnectorTableColumnStatistics(
                    "hive0.partitioned_db.t1.1234", ImmutableList.of("c1", "c2"));
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testInvalidateConnectorHistogramStatisticsByUuid() {
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        try {
            cachedStatisticStorage.invalidateConnectorHistogramStatistics(null, ImmutableList.of("c1"));
            cachedStatisticStorage.invalidateConnectorHistogramStatistics(
                    "hive0.partitioned_db.t1.1234", ImmutableList.of("c1"));
        } catch (Exception e) {
            Assertions.fail();
        }
    }

<<<<<<< HEAD
=======
    private static Table tableWithPartitionCount(int partitionCount) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        return new Table(table.getId(), table.getName(), Table.TableType.OLAP, table.getFullSchema()) {
            @Override
            public Collection<Partition> getPartitions() {
                // Only the size is needed; avoid allocating a large partition catalog for this test.
                return Collections.nCopies(partitionCount, null);
            }
        };
    }

    private static List<ColumnStatsCacheKey> cachePartitionColumns(CachedStatisticStorage storage, Table table, int count) {
        List<ColumnStatsCacheKey> keys = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), "column_" + i);
            // A previously absent value is still a warm key and must be refreshed after collection.
            storage.partitionStatistics.synchronous().put(key, Optional.empty());
            keys.add(key);
        }
        return keys;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 30000, 100001, Integer.MAX_VALUE})
    public void testPartitionRefreshBatchesByTableSize(int partitionCount) {
        Table table = tableWithPartitionCount(partitionCount);
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<ColumnStatsCacheKey> keys = cachePartitionColumns(storage, table, partitionCount <= 1 ? 35 : 7);
        List<Integer> batchSizes = new ArrayList<>();
        List<ColumnStatsCacheKey> loadedKeys = new ArrayList<>();
        PartitionStats current = new PartitionStats();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> requested, java.util.concurrent.Executor executor) {
                List<ColumnStatsCacheKey> batch = ImmutableList.copyOf(requested);
                batchSizes.add(batch.size());
                loadedKeys.addAll(batch);
                Map<ColumnStatsCacheKey, Optional<PartitionStats>> result = Maps.newHashMap();
                batch.forEach(key -> result.put(key, Optional.of(current)));
                return CompletableFuture.completedFuture(result);
            }
        };

        storage.refreshColumnStatistics(table, keys.stream().map(key -> key.column).toList(), true);

        List<Integer> expectedSizes = switch (partitionCount) {
            case 0, 1 -> ImmutableList.of(32, 3);
            case 30000 -> ImmutableList.of(3, 3, 1);
            default -> Collections.nCopies(7, 1);
        };
        Assertions.assertEquals(expectedSizes, batchSizes);
        Assertions.assertEquals(keys, loadedKeys);
        keys.forEach(key -> Assertions.assertSame(current,
                storage.partitionStatistics.synchronous().getIfPresent(key).orElseThrow()));
    }

    @Test
    @Timeout(10)
    public void testAsyncPartitionRefreshPublishesBeforeAdvancingAndSkipsInvalidatedKeys() {
        Table table = tableWithPartitionCount(100001);
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<ColumnStatsCacheKey> keys = cachePartitionColumns(storage, table, 3);
        List<List<ColumnStatsCacheKey>> batches = new ArrayList<>();
        CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> firstLoad = new CompletableFuture<>();
        PartitionStats current = new PartitionStats();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> requested, java.util.concurrent.Executor executor) {
                List<ColumnStatsCacheKey> batch = ImmutableList.copyOf(requested);
                batches.add(batch);
                if (batches.size() == 1) {
                    return firstLoad;
                }
                // Publishing this batch must happen before allocating the next batch's result.
                Assertions.assertSame(current,
                        storage.partitionStatistics.synchronous().getIfPresent(keys.get(0)).orElseThrow());
                Assertions.assertFalse(storage.partitionStatistics.asMap().containsKey(keys.get(1)));
                return CompletableFuture.completedFuture(ImmutableMap.of(keys.get(2), Optional.of(current)));
            }
        };

        storage.refreshColumnStatistics(table, keys.stream().map(key -> key.column).toList(), false);
        Assertions.assertEquals(ImmutableList.of(ImmutableList.of(keys.get(0))), batches);
        Assertions.assertEquals(Optional.empty(), storage.partitionStatistics.synchronous().getIfPresent(keys.get(0)));
        storage.partitionStatistics.synchronous().invalidate(keys.get(1));
        firstLoad.complete(ImmutableMap.of(keys.get(0), Optional.of(current)));

        Assertions.assertEquals(ImmutableList.of(ImmutableList.of(keys.get(0)), ImmutableList.of(keys.get(2))), batches);
        Assertions.assertSame(current,
                storage.partitionStatistics.synchronous().getIfPresent(keys.get(2)).orElseThrow());
        Assertions.assertFalse(storage.partitionStatistics.asMap().containsKey(keys.get(1)));
    }

    @Test
    @Timeout(10)
    public void testSynchronousPartitionRefreshWaitsForEveryBatch() throws Exception {
        Table table = tableWithPartitionCount(100001);
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<ColumnStatsCacheKey> keys = cachePartitionColumns(storage, table, 2);
        CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> firstLoad = new CompletableFuture<>();
        CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> secondLoad = new CompletableFuture<>();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        PartitionStats current = new PartitionStats();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> requested, java.util.concurrent.Executor executor) {
                int batch = loads.getAndIncrement();
                Assertions.assertEquals(ImmutableList.of(keys.get(batch)), ImmutableList.copyOf(requested));
                if (batch == 0) {
                    firstStarted.countDown();
                    return firstLoad;
                }
                Assertions.assertSame(current,
                        storage.partitionStatistics.synchronous().getIfPresent(keys.get(0)).orElseThrow());
                secondStarted.countDown();
                return secondLoad;
            }
        };

        CompletableFuture<Void> refresh = CompletableFuture.runAsync(() ->
                storage.refreshColumnStatistics(table, keys.stream().map(key -> key.column).toList(), true));
        try {
            Assertions.assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, loads.get());
            Assertions.assertFalse(refresh.isDone());
            firstLoad.complete(ImmutableMap.of(keys.get(0), Optional.of(current)));
            Assertions.assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertFalse(refresh.isDone());
            secondLoad.complete(ImmutableMap.of(keys.get(1), Optional.of(current)));
            refresh.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(2, loads.get());
            Assertions.assertSame(current,
                    storage.partitionStatistics.synchronous().getIfPresent(keys.get(1)).orElseThrow());
        } finally {
            firstLoad.complete(ImmutableMap.of(keys.get(0), Optional.of(current)));
            secondLoad.complete(ImmutableMap.of(keys.get(1), Optional.of(current)));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPartitionRefreshContinuesAfterFailedBatch(boolean rejected) {
        Table table = tableWithPartitionCount(100001);
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<ColumnStatsCacheKey> keys = cachePartitionColumns(storage, table, 2);
        AtomicInteger loads = new AtomicInteger();
        PartitionStats current = new PartitionStats();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> requested, java.util.concurrent.Executor executor) {
                int batch = loads.getAndIncrement();
                Assertions.assertEquals(ImmutableList.of(keys.get(batch)), ImmutableList.copyOf(requested));
                if (batch == 0) {
                    if (rejected) {
                        throw new RejectedExecutionException("refresh executor rejected the batch");
                    }
                    return CompletableFuture.failedFuture(new RuntimeException("statistics query failure"));
                }
                Assertions.assertFalse(storage.partitionStatistics.asMap().containsKey(keys.get(0)));
                return CompletableFuture.completedFuture(ImmutableMap.of(keys.get(1), Optional.of(current)));
            }
        };

        storage.refreshColumnStatistics(table, keys.stream().map(key -> key.column).toList(), true);

        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(storage.partitionStatistics.asMap().containsKey(keys.get(0)));
        Assertions.assertSame(current,
                storage.partitionStatistics.synchronous().getIfPresent(keys.get(1)).orElseThrow());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRefreshReplacesInFlightPartitionLoad(boolean isSync) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        CachedStatisticStorage storage = new CachedStatisticStorage();
        storage.partitionStatistics = Caffeine.newBuilder().recordStats().executor(Runnable::run)
                .buildAsync(new PartitionStatsCacheLoader());
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), "v1");
        ColumnStatsCacheKey unrelated = new ColumnStatsCacheKey(table.getId(), "v2");
        ColumnStatsCacheKey cold = new ColumnStatsCacheKey(table.getId(), "v3");
        CompletableFuture<Optional<PartitionStats>> unrelatedLoad = new CompletableFuture<>();
        storage.partitionStatistics.put(unrelated, unrelatedLoad);
        PartitionStats previous = new PartitionStats();
        previous.getDistinctCount().put(1L, 9.0);
        PartitionStats current = new PartitionStats();
        current.getDistinctCount().put(1L, 3.0);
        CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> earlierLoad = new CompletableFuture<>();
        AtomicInteger loads = new AtomicInteger();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> keys, java.util.concurrent.Executor executor) {
                Assertions.assertEquals(ImmutableList.of(key), ImmutableList.copyOf(keys));
                return loads.incrementAndGet() == 1 ? earlierLoad :
                        CompletableFuture.completedFuture(ImmutableMap.of(key, Optional.of(current)));
            }
        };
        // A real bulk cache miss installs incomplete per-key proxy futures before ANALYZE finishes.
        CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> queried =
                storage.partitionStatistics.getAll(ImmutableList.of(key));
        Assertions.assertFalse(queried.isDone());
        long hits = storage.partitionStatistics.synchronous().stats().hitCount();
        long misses = storage.partitionStatistics.synchronous().stats().missCount();

        storage.refreshColumnStatistics(table, ImmutableList.of("v1", "v3"), isSync);
        Assertions.assertEquals(2, loads.get());
        earlierLoad.complete(ImmutableMap.of(key, Optional.of(previous)));

        // The earlier query may finish with its old snapshot, but must not overwrite the refreshed cache entry.
        Assertions.assertSame(previous, queried.join().get(key).orElseThrow());
        Assertions.assertSame(current, storage.partitionStatistics.asMap().get(key).join().orElseThrow());
        Assertions.assertSame(unrelatedLoad, storage.partitionStatistics.asMap().get(unrelated));
        Assertions.assertFalse(storage.partitionStatistics.asMap().containsKey(cold));
        Assertions.assertEquals(hits, storage.partitionStatistics.synchronous().stats().hitCount());
        Assertions.assertEquals(misses, storage.partitionStatistics.synchronous().stats().missCount());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAsyncPartitionRefreshHandlesBothCompletionOrders(boolean earlierLoadCompletesFirst) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        CachedStatisticStorage storage = new CachedStatisticStorage();
        storage.partitionStatistics = Caffeine.newBuilder().executor(Runnable::run)
                .buildAsync(new PartitionStatsCacheLoader());
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), "v1");
        PartitionStats previous = new PartitionStats();
        PartitionStats current = new PartitionStats();
        CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> earlierLoad = new CompletableFuture<>();
        CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> refreshedLoad = new CompletableFuture<>();
        AtomicInteger loads = new AtomicInteger();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> keys, java.util.concurrent.Executor executor) {
                return loads.incrementAndGet() == 1 ? earlierLoad : refreshedLoad;
            }
        };
        storage.partitionStatistics.getAll(ImmutableList.of(key));

        storage.refreshColumnStatistics(table, ImmutableList.of("v1"), false);
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(refreshedLoad.isDone());
        if (earlierLoadCompletesFirst) {
            earlierLoad.complete(ImmutableMap.of(key, Optional.of(previous)));
            Assertions.assertSame(previous, storage.partitionStatistics.asMap().get(key).join().orElseThrow());
        }
        refreshedLoad.complete(ImmutableMap.of(key, Optional.of(current)));
        if (!earlierLoadCompletesFirst) {
            earlierLoad.complete(ImmutableMap.of(key, Optional.of(previous)));
        }
        Assertions.assertSame(current, storage.partitionStatistics.asMap().get(key).join().orElseThrow());
    }

    @Test
    public void testFailedPartitionRefreshInvalidatesInFlightLoad() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), "v1");
        AtomicInteger outcome = new AtomicInteger();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> keys, java.util.concurrent.Executor executor) {
                return switch (outcome.get()) {
                    case 0 -> CompletableFuture.completedFuture(ImmutableMap.of());
                    case 1 -> CompletableFuture.completedFuture(null);
                    default -> CompletableFuture.failedFuture(new RuntimeException("statistics query failure"));
                };
            }
        };
        for (boolean isSync : new boolean[] {true, false}) {
            for (int i = 0; i < 3; i++) {
                outcome.set(i);
                CachedStatisticStorage storage = new CachedStatisticStorage();
                CompletableFuture<Optional<PartitionStats>> earlierLoad = new CompletableFuture<>();
                storage.partitionStatistics.put(key, earlierLoad);
                storage.refreshColumnStatistics(table, ImmutableList.of("v1"), isSync);
                Assertions.assertFalse(storage.partitionStatistics.asMap().containsKey(key));
                earlierLoad.complete(Optional.of(new PartitionStats()));
                Assertions.assertFalse(storage.partitionStatistics.asMap().containsKey(key),
                        "A late pre-collection load must not undo invalidation: isSync=" + isSync + ", outcome=" + i);
            }
        }
    }

    @Test
    public void testRefreshReloadsOnlyCachedPartitionStatistics() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        CachedStatisticStorage storage = new CachedStatisticStorage();
        ColumnStatsCacheKey v1 = new ColumnStatsCacheKey(table.getId(), "v1");
        ColumnStatsCacheKey v2 = new ColumnStatsCacheKey(table.getId(), "v2");
        ColumnStatsCacheKey v3 = new ColumnStatsCacheKey(table.getId(), "v3");
        PartitionStats previous = new PartitionStats();
        previous.getDistinctCount().put(1L, 9.0);
        storage.partitionStatistics.synchronous().put(v1, Optional.of(previous));
        storage.partitionStatistics.synchronous().put(v2, Optional.of(previous));
        new Expectations() {
            {
                statisticExecutor.queryPartitionLevelColumnNDV((ConnectContext) any, table.getId(),
                        (List<Long>) any, ImmutableList.of("v1"));
                TStatisticData data = new TStatisticData();
                data.setColumnName("v1");
                data.setPartitionId(1L);
                data.setCountDistinct(3);
                data.setRowCount(10);
                data.setNullCount(2);
                result = ImmutableList.of(data);
            }
        };

        // ANALYZE v1 and v3: update the warm entry, without loading cold or unrelated columns.
        storage.refreshColumnStatistics(table, ImmutableList.of("v1", "v3"), true);
        PartitionStats refreshed = storage.partitionStatistics.synchronous().getIfPresent(v1).orElseThrow();
        Assertions.assertEquals(3.0, refreshed.getDistinctCount().get(1L));
        Assertions.assertEquals(0.2, refreshed.getNullFraction().get(1L));
        Assertions.assertSame(previous, storage.partitionStatistics.synchronous().getIfPresent(v2).orElseThrow());
        Assertions.assertNull(storage.partitionStatistics.synchronous().getIfPresent(v3));
    }

    @Test
    public void testRefreshDoesNotLoadColdPartitionStatistics() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        AtomicInteger loads = new AtomicInteger();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> keys, java.util.concurrent.Executor executor) {
                loads.incrementAndGet();
                return CompletableFuture.completedFuture(ImmutableMap.of());
            }
        };
        CachedStatisticStorage storage = new CachedStatisticStorage();
        storage.refreshColumnStatistics(table, ImmutableList.of("v1", "v2"), true);
        Assertions.assertEquals(0, loads.get());
        Assertions.assertEquals(0, storage.partitionStatistics.synchronous().estimatedSize());
    }

    @Test
    @Timeout(10)
    public void testAsyncRefreshAppliesPartitionStatisticsAfterCompletion() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        CachedStatisticStorage storage = new CachedStatisticStorage();
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), "v1");
        PartitionStats previous = new PartitionStats();
        storage.partitionStatistics.synchronous().put(key, Optional.of(previous));
        CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> loaded = new CompletableFuture<>();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> keys, java.util.concurrent.Executor executor) {
                Assertions.assertEquals(ImmutableList.of(key), ImmutableList.copyOf(keys));
                return loaded;
            }
        };

        // Follower replay must return while the asynchronous load is still pending.
        storage.refreshColumnStatistics(table, ImmutableList.of("v1"), false);
        Assertions.assertSame(previous, storage.partitionStatistics.synchronous().getIfPresent(key).orElseThrow());
        PartitionStats current = new PartitionStats();
        current.getDistinctCount().put(1L, 3.0);
        loaded.complete(ImmutableMap.of(key, Optional.of(current)));
        Assertions.assertSame(current, storage.partitionStatistics.synchronous().getIfPresent(key).orElseThrow());
    }

    @Test
    public void testPartitionRefreshDropsStaleValuesWhenNotLoaded() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        CachedStatisticStorage storage = new CachedStatisticStorage();
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), "v1");
        AtomicInteger outcome = new AtomicInteger();
        new MockUp<PartitionStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> keys, java.util.concurrent.Executor executor) {
                return switch (outcome.get()) {
                    case 0 -> CompletableFuture.completedFuture(ImmutableMap.of());
                    case 1 -> CompletableFuture.completedFuture(null);
                    default -> CompletableFuture.failedFuture(new RuntimeException("statistics query failure"));
                };
            }
        };

        for (boolean isSync : new boolean[] {true, false}) {
            for (int i = 0; i < 3; i++) {
                outcome.set(i);
                storage.partitionStatistics.synchronous().put(key, Optional.of(new PartitionStats()));
                storage.refreshColumnStatistics(table, ImmutableList.of("v1"), isSync);
                Assertions.assertNull(storage.partitionStatistics.synchronous().getIfPresent(key),
                        "isSync=" + isSync + ", outcome=" + i);
            }
        }
    }

    @Test
    public void testPartitionRefreshCachesConfirmedAbsence() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        CachedStatisticStorage storage = new CachedStatisticStorage();
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), "v1");
        storage.partitionStatistics.synchronous().put(key, Optional.of(new PartitionStats()));

        // The real loader distinguishes a successful query with no rows from a load failure.
        storage.refreshColumnStatistics(table, ImmutableList.of("v1"), true);
        Assertions.assertEquals(Optional.empty(), storage.partitionStatistics.synchronous().getIfPresent(key));
    }

    @Test
    public void testExpireDropsOnlyRequestedPartitionStatistics() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        CachedStatisticStorage storage = new CachedStatisticStorage();
        ColumnStatsCacheKey v1 = new ColumnStatsCacheKey(table.getId(), "v1");
        ColumnStatsCacheKey v2 = new ColumnStatsCacheKey(table.getId(), "v2");
        storage.partitionStatistics.synchronous().put(v1, Optional.of(new PartitionStats()));
        storage.partitionStatistics.synchronous().put(v2, Optional.empty());
        storage.addColumnStatistic(table, "v1", ColumnStatistic.builder().setDistinctValuesCount(9).build());

        storage.expireTableAndColumnStatistics(table, ImmutableList.of("v1"));
        Assertions.assertNull(storage.partitionStatistics.synchronous().getIfPresent(v1));
        Assertions.assertNull(storage.columnStatistics.synchronous().getIfPresent(v1));
        Assertions.assertEquals(Optional.empty(), storage.partitionStatistics.synchronous().getIfPresent(v2));
        storage.expireTableAndColumnStatistics(table, ImmutableList.of("v2"));
        Assertions.assertNull(storage.partitionStatistics.synchronous().getIfPresent(v2));
    }

>>>>>>> c0000f582c3... [BugFix][RB-699] Bound partition statistics refresh batches (#66794)
    @Test
    public void testGetColumnNDVForPartitions(@Mocked AsyncLoadingCache<ColumnStatsCacheKey, Optional<PartitionStats>>
                                                          partitionStatistics) {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");

        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), "c1");
        new Expectations() {
            {
                partitionStatistics.getAll((Iterable<? extends ColumnStatsCacheKey>) any);
                result = CompletableFuture.completedFuture(ImmutableMap.of(key, Optional.empty()));
                minTimes = 0;
            }
        };
        Map<String, PartitionStats> partitionStatsMap =
                cachedStatisticStorage.getColumnNDVForPartitions(table, ImmutableList.of("c1"));
        Assertions.assertEquals(0, partitionStatsMap.size());


        new MockUp<CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>>>() {
            @Mock
            public Map<ColumnStatsCacheKey, Optional<PartitionStats>> get(long timeout, TimeUnit unit) throws
                    InterruptedException, ExecutionException, TimeoutException {
                throw new InterruptedException("test");
            }
        };

        partitionStatsMap =
                cachedStatisticStorage.getColumnNDVForPartitions(table, ImmutableList.of("c1"));
        Assertions.assertEquals(0, partitionStatsMap.size());


        new MockUp<CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>>>() {
            @Mock
            public Map<ColumnStatsCacheKey, Optional<PartitionStats>> get(long timeout, TimeUnit unit) throws
                    InterruptedException, ExecutionException, TimeoutException {
                throw new ExecutionException("test", new Exception());
            }
        };

        partitionStatsMap =
                cachedStatisticStorage.getColumnNDVForPartitions(table, ImmutableList.of("c1"));
        Assertions.assertEquals(0, partitionStatsMap.size());


        new MockUp<CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>>>() {
            @Mock
            public Map<ColumnStatsCacheKey, Optional<PartitionStats>> get(long timeout, TimeUnit unit) throws
                    InterruptedException, ExecutionException, TimeoutException {
                throw new TimeoutException("test");
            }
        };

        partitionStatsMap =
                cachedStatisticStorage.getColumnNDVForPartitions(table, ImmutableList.of("c1"));
        Assertions.assertEquals(0, partitionStatsMap.size());
    }

    @Test
    public void testGetConnectorHistogramStatistics(@Mocked AsyncLoadingCache<ConnectorTableColumnKey, Optional<Histogram>>
                                                            histogramCache) {
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db", "t1");
        ConnectorTableColumnKey key = new ConnectorTableColumnKey("hive0.partitioned_db.t1.1234", "c1");
        new Expectations() {
            {
                histogramCache.getAll((Iterable<? extends ConnectorTableColumnKey>) any);
                result = CompletableFuture.completedFuture(ImmutableMap.of(key, Optional.empty()));
                minTimes = 0;
            }
        };
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        Map<String, Histogram> histogramMap =
                cachedStatisticStorage.getConnectorHistogramStatistics(table, ImmutableList.of("c1"));
        Assertions.assertEquals(0, histogramMap.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGetHistogramStatisticsSkipInStatisticsConnection(
            @Mocked AsyncLoadingCache<ColumnStatsCacheKey, Optional<Histogram>> histogramCache) {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");

        // A histogram-collect INSERT holds the histogram_statistics READ lock while its plan is
        // optimized; estimating the source scan then calls getHistogramStatistics on the source
        // table. If that synchronously loads the histogram, the loader re-acquires the
        // histogram_statistics READ lock and (behind a queued publish WRITE) self-deadlocks.
        // The guard must short-circuit for statistics-collect connections BEFORE touching the
        // cache, so the loader is never dispatched. Assert getAll is never called.
        new Expectations() {
            {
                histogramCache.getAll((Iterable<? extends ColumnStatsCacheKey>) any);
                times = 0;
            }
        };

        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();

        connectContext.setThreadLocalInfo();
        boolean prev = connectContext.isStatisticsConnection();
        connectContext.setStatisticsConnection(true);
        try {
            Map<String, Histogram> result =
                    cachedStatisticStorage.getHistogramStatistics(table, ImmutableList.of("v1"));
            Assertions.assertTrue(result.isEmpty());
        } finally {
            connectContext.setStatisticsConnection(prev);
            ConnectContext.remove();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGetHistogramStatisticsSkipForStatisticsInternalTable(
            @Mocked AsyncLoadingCache<ColumnStatsCacheKey, Optional<Histogram>> histogramCache) {
        Database statsDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(StatsConstants.STATISTICS_DB_NAME);
        Table statsTable = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(statsDb.getFullName(), "table_statistic_v1");
        Assertions.assertNotNull(statsTable);

        // The other guard branch: statistics-internal tables (in the collect blacklist DB) must also
        // skip the load, independent of the connection type, so the cache is never touched.
        new Expectations() {
            {
                histogramCache.getAll((Iterable<? extends ColumnStatsCacheKey>) any);
                times = 0;
            }
        };

        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        ConnectContext.remove();
        Map<String, Histogram> result =
                cachedStatisticStorage.getHistogramStatistics(statsTable, ImmutableList.of("column_name"));
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testExpireConnectorHistogramStatistics() {
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable(connectContext, "hive0", "partitioned_db", "t1");
        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        try {
            cachedStatisticStorage.expireConnectorHistogramStatistics(table, ImmutableList.of("c1", "c2"));
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testLoadCacheLoadEmpty(@Mocked CachedStatisticStorage cachedStatisticStorage) {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");

        new Expectations() {
            {
                cachedStatisticStorage.getColumnStatistic(table, "v1");
                result = ColumnStatistic.unknown();
                minTimes = 0;
            }
        };
        ColumnStatistic columnStatistic =
                Deencapsulation.invoke(cachedStatisticStorage, "getColumnStatistic", table, "v1");
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0.0, columnStatistic.getNullsFraction(), 0.001);
        Assertions.assertEquals(1.0, columnStatistic.getAverageRowSize(), 0.001);
        Assertions.assertEquals(1.0, columnStatistic.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testConvert2ColumnStatistics() {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");
        ColumnBasicStatsCacheLoader cachedStatisticStorage =
                Deencapsulation.newInstance(ColumnBasicStatsCacheLoader.class);

        TStatisticData statisticData = new TStatisticData();
        statisticData.setDbId(db.getId());
        statisticData.setTableId(table.getId());
        statisticData.setColumnName("v1");
        statisticData.setMax("123");
        statisticData.setMin("0");

        ColumnStatistic columnStatistic =
                Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assertions.assertEquals(123, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v4");
        statisticData.setMax("2021-05-21");
        statisticData.setMin("2021-05-20");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2021, 5, 21, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2021, 5, 20, 0, 0, 0)),
                columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v1");
        statisticData.setMin("aa");
        statisticData.setMax("bb");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v1");
        statisticData.setMin("");
        statisticData.setMax("");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v4");
        statisticData.setMin("");
        statisticData.setMax("");
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);

        statisticData.setColumnName("v4");
        statisticData.setMin("");
        statisticData.setMax("");
        statisticData.setRowCount(0);
        statisticData.setDataSize(0);
        statisticData.setNullCount(0);
        columnStatistic = Deencapsulation.invoke(cachedStatisticStorage, "convert2ColumnStatistics", statisticData);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getAverageRowSize(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getNullsFraction(), 0.001);
    }

    @Test
    @Timeout(5)
    public void testWaitForStatsFutureTimeout() {
        // GIVEN
        final var storage = new CachedStatisticStorage();
        // Create a future that will never complete
        final var neverCompletingFuture = new CompletableFuture<>();

        boolean originalEnabled = Config.enable_sync_statistics_load;
        int originalTimeout = Config.sync_statistics_load_timeout_ms;
        try {
            Config.enable_sync_statistics_load = true;
            Config.sync_statistics_load_timeout_ms = 100; // 100ms timeout

            // WHEN
            // The method should return without throwing despite the future not completing
            Supplier<String> contextSupplier = () -> "context";
            Deencapsulation.invoke(storage, "waitForStatsFutureIfWaitEnabled", neverCompletingFuture,
                    contextSupplier);

            // THEN
            // Future should still not be done (timeout was caught internally)
            Assertions.assertFalse(neverCompletingFuture.isDone());
        } finally {
            Config.enable_sync_statistics_load = originalEnabled;
            Config.sync_statistics_load_timeout_ms = originalTimeout;
        }
    }

    @Test
    @Timeout(5)
    public void testWaitForStatsFutureDisabled() {
        // GIVEN
        final var storage = new CachedStatisticStorage();
        final var neverCompletingFuture = new CompletableFuture<>();

        boolean originalEnabled = Config.enable_sync_statistics_load;
        try {
            Config.enable_sync_statistics_load = false;

            // WHEN
            // Should return immediately without waiting
            Supplier<String> contextSupplier = () -> "context";
            Deencapsulation.invoke(storage, "waitForStatsFutureIfWaitEnabled", neverCompletingFuture,
                    contextSupplier);

            // THEN
            Assertions.assertFalse(neverCompletingFuture.isDone());
        } finally {
            Config.enable_sync_statistics_load = originalEnabled;
        }
    }

    private static class TimeoutFuture<T> extends CompletableFuture<T> {
        private int timedGetCalls;
        private long timeout;
        private TimeUnit unit;

        @Override
        public T get(long timeout, TimeUnit unit) throws TimeoutException {
            this.timedGetCalls++;
            this.timeout = timeout;
            this.unit = unit;
            throw new TimeoutException("test");
        }
    }

    private static class FixedWaitStatisticsLoadBudget extends StatisticsLoadBudget {
        private final long waitMsToRecord;

        private FixedWaitStatisticsLoadBudget(long totalBudgetMs, long waitMsToRecord) {
            super(totalBudgetMs);
            this.waitMsToRecord = waitMsToRecord;
        }

        @Override
        public synchronized void recordWait(long elapsedNanos) {
            super.recordWait(TimeUnit.MILLISECONDS.toNanos(waitMsToRecord));
        }
    }

    @Test
    public void testWaitForStatsFutureUsesPerCallTimeoutWithoutQueryBudget() {
        // GIVEN
        final var storage = new CachedStatisticStorage();
        final var timeoutFuture = new TimeoutFuture<>();

        boolean originalEnabled = Config.enable_sync_statistics_load;
        int originalTimeout = Config.sync_statistics_load_timeout_ms;
        int originalQueryBudget = Config.sync_statistics_load_per_query_budget_ms;
        ConnectContext previousContext = ConnectContext.exchangeThreadLocalInfo(connectContext);
        StatisticsLoadBudget previousBudget = connectContext.getStatisticsLoadBudget();
        long beforeMetric = SYNC_STATS_LOAD_BUDGET_EXHAUSTED_TOTAL.getValue();
        try {
            Config.enable_sync_statistics_load = true;
            Config.sync_statistics_load_timeout_ms = 100;
            Config.sync_statistics_load_per_query_budget_ms = 0;
            connectContext.setStatisticsLoadBudget(null);

            // WHEN
            Deencapsulation.invoke(storage, "waitForStatsFutureIfWaitEnabled", timeoutFuture,
                    (Supplier<String>) () -> "context");

            // THEN
            Assertions.assertEquals(1, timeoutFuture.timedGetCalls);
            Assertions.assertEquals(100, timeoutFuture.timeout);
            Assertions.assertEquals(TimeUnit.MILLISECONDS, timeoutFuture.unit);
            Assertions.assertFalse(timeoutFuture.isDone());
            Assertions.assertNull(connectContext.getStatisticsLoadBudget());
            Assertions.assertEquals(beforeMetric, SYNC_STATS_LOAD_BUDGET_EXHAUSTED_TOTAL.getValue());
        } finally {
            connectContext.setStatisticsLoadBudget(previousBudget);
            ConnectContext.exchangeThreadLocalInfo(previousContext);
            Config.enable_sync_statistics_load = originalEnabled;
            Config.sync_statistics_load_timeout_ms = originalTimeout;
            Config.sync_statistics_load_per_query_budget_ms = originalQueryBudget;
        }
    }

    @Test
    public void testWaitForStatsFutureDoesNotReportBudgetExceededWhenPerCallTimeoutExpiresWithBudgetRemaining() {
        // GIVEN
        final var storage = new CachedStatisticStorage();
        final var timeoutFuture = new TimeoutFuture<>();
        final var budget = new FixedWaitStatisticsLoadBudget(1000, 100);

        boolean originalEnabled = Config.enable_sync_statistics_load;
        int originalTimeout = Config.sync_statistics_load_timeout_ms;
        ConnectContext previousContext = ConnectContext.exchangeThreadLocalInfo(connectContext);
        StatisticsLoadBudget previousBudget = connectContext.getStatisticsLoadBudget();
        long beforeMetric = SYNC_STATS_LOAD_BUDGET_EXHAUSTED_TOTAL.getValue();
        try {
            Config.enable_sync_statistics_load = true;
            Config.sync_statistics_load_timeout_ms = 100;
            connectContext.setStatisticsLoadBudget(budget);

            // WHEN
            Deencapsulation.invoke(storage, "waitForStatsFutureIfWaitEnabled", timeoutFuture,
                    (Supplier<String>) () -> "context");

            // THEN
            Assertions.assertEquals(1, timeoutFuture.timedGetCalls);
            Assertions.assertEquals(100, timeoutFuture.timeout);
            Assertions.assertEquals(TimeUnit.MILLISECONDS, timeoutFuture.unit);
            Assertions.assertEquals(900, budget.getRemainingBudgetMs());
            Assertions.assertEquals(beforeMetric, SYNC_STATS_LOAD_BUDGET_EXHAUSTED_TOTAL.getValue());
        } finally {
            connectContext.setStatisticsLoadBudget(previousBudget);
            ConnectContext.exchangeThreadLocalInfo(previousContext);
            Config.enable_sync_statistics_load = originalEnabled;
            Config.sync_statistics_load_timeout_ms = originalTimeout;
        }
    }

    @Test
    public void testWaitForStatsFutureReportsBudgetExceededAfterTimedWaitExhaustsBudget() {
        // GIVEN
        final var storage = new CachedStatisticStorage();
        final var timeoutFuture = new TimeoutFuture<>();
        final var budget = new FixedWaitStatisticsLoadBudget(100, 100);

        boolean originalEnabled = Config.enable_sync_statistics_load;
        int originalTimeout = Config.sync_statistics_load_timeout_ms;
        ConnectContext previousContext = ConnectContext.exchangeThreadLocalInfo(connectContext);
        StatisticsLoadBudget previousBudget = connectContext.getStatisticsLoadBudget();
        long beforeMetric = SYNC_STATS_LOAD_BUDGET_EXHAUSTED_TOTAL.getValue();
        try {
            Config.enable_sync_statistics_load = true;
            Config.sync_statistics_load_timeout_ms = 1000;
            connectContext.setStatisticsLoadBudget(budget);

            // WHEN
            Deencapsulation.invoke(storage, "waitForStatsFutureIfWaitEnabled", timeoutFuture,
                    (Supplier<String>) () -> "context");

            // THEN
            Assertions.assertEquals(1, timeoutFuture.timedGetCalls);
            Assertions.assertEquals(100, timeoutFuture.timeout);
            Assertions.assertEquals(TimeUnit.MILLISECONDS, timeoutFuture.unit);
            Assertions.assertEquals(0, budget.getRemainingBudgetMs());
            Assertions.assertEquals(beforeMetric + 1, SYNC_STATS_LOAD_BUDGET_EXHAUSTED_TOTAL.getValue());
        } finally {
            connectContext.setStatisticsLoadBudget(previousBudget);
            ConnectContext.exchangeThreadLocalInfo(previousContext);
            Config.enable_sync_statistics_load = originalEnabled;
            Config.sync_statistics_load_timeout_ms = originalTimeout;
        }
    }

    @Test
    public void testStatisticsLoadBudgetDefaultUsesPerCallTimeout() {
        // GIVEN
        int originalTimeout = Config.sync_statistics_load_timeout_ms;
        int originalQueryBudget = Config.sync_statistics_load_per_query_budget_ms;
        try {
            Config.sync_statistics_load_timeout_ms = 123;
            Config.sync_statistics_load_per_query_budget_ms = -1;

            // WHEN
            StatisticsLoadBudget budget = StatisticsLoadBudget.fromConfig();

            // THEN
            Assertions.assertEquals(123, budget.getTotalBudgetMs());
        } finally {
            Config.sync_statistics_load_timeout_ms = originalTimeout;
            Config.sync_statistics_load_per_query_budget_ms = originalQueryBudget;
        }
    }

    @Test
    public void testWaitForStatsFutureSkippedWhenQueryBudgetIsZero() {
        // GIVEN
        final var storage = new CachedStatisticStorage();
        final var timeoutFuture = new TimeoutFuture<>();

        boolean originalEnabled = Config.enable_sync_statistics_load;
        int originalTimeout = Config.sync_statistics_load_timeout_ms;
        int originalQueryBudget = Config.sync_statistics_load_per_query_budget_ms;
        ConnectContext previousContext = ConnectContext.exchangeThreadLocalInfo(connectContext);
        StatisticsLoadBudget previousBudget = connectContext.getStatisticsLoadBudget();
        try {
            Config.enable_sync_statistics_load = true;
            Config.sync_statistics_load_timeout_ms = 1000;
            Config.sync_statistics_load_per_query_budget_ms = 0;
            connectContext.setStatisticsLoadBudget(null);

            try (var ignored = StatisticsLoadBudget.openScope(connectContext)) {
                // WHEN
                Deencapsulation.invoke(storage, "waitForStatsFutureIfWaitEnabled", timeoutFuture,
                        (Supplier<String>) () -> "context");

                // THEN
                Assertions.assertEquals(0, timeoutFuture.timedGetCalls);
                Assertions.assertFalse(timeoutFuture.isDone());
                Assertions.assertEquals(0, connectContext.getStatisticsLoadBudget().getConsumedBudgetMs());
            }
            // THEN
            Assertions.assertNull(connectContext.getStatisticsLoadBudget());
        } finally {
            connectContext.setStatisticsLoadBudget(previousBudget);
            ConnectContext.exchangeThreadLocalInfo(previousContext);
            Config.enable_sync_statistics_load = originalEnabled;
            Config.sync_statistics_load_timeout_ms = originalTimeout;
            Config.sync_statistics_load_per_query_budget_ms = originalQueryBudget;
        }
    }

    @Test
    public void testWaitForStatsFutureUsesRemainingQueryBudget() {
        // GIVEN
        final var storage = new CachedStatisticStorage();

        boolean originalEnabled = Config.enable_sync_statistics_load;
        int originalTimeout = Config.sync_statistics_load_timeout_ms;
        int originalQueryBudget = Config.sync_statistics_load_per_query_budget_ms;
        ConnectContext previousContext = ConnectContext.exchangeThreadLocalInfo(connectContext);
        StatisticsLoadBudget previousBudget = connectContext.getStatisticsLoadBudget();
        try {
            Config.enable_sync_statistics_load = true;
            Config.sync_statistics_load_timeout_ms = 1000;
            Config.sync_statistics_load_per_query_budget_ms = 100;
            connectContext.setStatisticsLoadBudget(null);

            try (var ignored = StatisticsLoadBudget.openScope(connectContext)) {
                final var firstFuture = new TimeoutFuture<>();

                // WHEN
                Deencapsulation.invoke(storage, "waitForStatsFutureIfWaitEnabled", firstFuture,
                        (Supplier<String>) () -> "first");

                // THEN
                Assertions.assertEquals(1, firstFuture.timedGetCalls);
                Assertions.assertEquals(100, firstFuture.timeout);
                Assertions.assertEquals(TimeUnit.MILLISECONDS, firstFuture.unit);

                // GIVEN
                connectContext.getStatisticsLoadBudget().recordWait(TimeUnit.MILLISECONDS.toNanos(100));
                Assertions.assertEquals(0, connectContext.getStatisticsLoadBudget().getRemainingBudgetMs());

                final var secondFuture = new TimeoutFuture<>();

                // WHEN
                Deencapsulation.invoke(storage, "waitForStatsFutureIfWaitEnabled", secondFuture,
                        (Supplier<String>) () -> "second");

                // THEN
                Assertions.assertEquals(0, secondFuture.timedGetCalls);
                Assertions.assertFalse(secondFuture.isDone());
            }
            // THEN
            Assertions.assertNull(connectContext.getStatisticsLoadBudget());
        } finally {
            connectContext.setStatisticsLoadBudget(previousBudget);
            ConnectContext.exchangeThreadLocalInfo(previousContext);
            Config.enable_sync_statistics_load = originalEnabled;
            Config.sync_statistics_load_timeout_ms = originalTimeout;
            Config.sync_statistics_load_per_query_budget_ms = originalQueryBudget;
        }
    }

    @Test
    @Timeout(5)
    public void testGetColumnStatisticReturnsUnknownOnTimeout() {
        // GIVEN
        final var db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        final var table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t0");

        boolean originalEnabled = Config.enable_sync_statistics_load;
        int originalTimeout = Config.sync_statistics_load_timeout_ms;
        try {
            Config.enable_sync_statistics_load = true;
            Config.sync_statistics_load_timeout_ms = 100;

            // Install a cache that returns a never-completing future
            final var storage = new CachedStatisticStorage();
            final var slowCache = Caffeine.newBuilder()
                    .maximumSize(100)
                    .buildAsync((key, executor) -> new CompletableFuture<>());
            Deencapsulation.setField(storage, "columnStatistics", slowCache);

            // WHEN
            // Should return unknown after timeout, not block forever
            ColumnStatistic result = storage.getColumnStatistic(table, "v1");

            // THEN
            Assertions.assertTrue(result.isUnknown());
        } finally {
            Config.enable_sync_statistics_load = originalEnabled;
            Config.sync_statistics_load_timeout_ms = originalTimeout;
        }
    }

    @Test
    @Timeout(5)
    public void testGetColumnStatisticsTimeoutWithSyncStats() {
        // GIVEN
        final var db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        final var table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t0");

        final var originalEnabled = Config.enable_sync_statistics_load;
        final var originalTimeout = Config.sync_statistics_load_timeout_ms;

        try {
            Config.enable_sync_statistics_load = true;
            Config.sync_statistics_load_timeout_ms = 100;

            // Install a cache whose loader returns a future that never completes
            final var storage = new CachedStatisticStorage();
            final AsyncLoadingCache<ColumnStatsCacheKey, Optional<ColumnStatistic>> slowCache =
                    Caffeine.newBuilder()
                            .maximumSize(100)
                            .buildAsync((key, executor) -> new CompletableFuture<>());
            Deencapsulation.setField(storage, "columnStatistics", slowCache);

            // WHEN
            final var stats = storage.getColumnStatistics(table, ImmutableList.of("v1", "v2"));

            // THEN
            // Each column resolves to the default unknown statistic.
            Assertions.assertEquals(2, stats.size());
            Assertions.assertTrue(stats.get(0).isUnknown());
            Assertions.assertTrue(stats.get(1).isUnknown());
        } finally {
            Config.enable_sync_statistics_load = originalEnabled;
            Config.sync_statistics_load_timeout_ms = originalTimeout;
        }
    }

    @Test
    public void testGetCachesExposesAllNamedCacheMapWithStatsRecording() {
        final var originalEnabled = Config.enable_statistic_cache_metrics;

        try {
            Config.enable_statistic_cache_metrics = true;
            CachedStatisticStorage storage = new CachedStatisticStorage();
            Map<String, LoadingCache<?, ?>> caches = storage.getNamedCacheMap();

            Assertions.assertEquals(
                    ImmutableList.of("table_stats", "column_stats", "partition_stats", "connector_table_stats",
                            "histogram_stats", "connector_histogram_stats", "multi_column_stats").stream().sorted().toList(),
                    caches.keySet().stream().sorted().toList());

            // recordStats() must be enabled so the metric layer can read hit/miss/eviction/load counts.
            for (Map.Entry<String, LoadingCache<?, ?>> entry : caches.entrySet()) {
                Assertions.assertTrue(entry.getValue().policy().isRecordingStats(),
                        "stats recording should be enabled for cache " + entry.getKey());
            }
        } finally {
            Config.enable_statistic_cache_metrics = originalEnabled;
        }

    }

    @Test
    public void testInitStatisticsCacheMetricsRegistersCountersAndGauge() {
        // GIVEN
        final boolean originalEnabled = Config.enable_statistic_cache_metrics;
        final var globalStateMgr = GlobalStateMgr.getCurrentState();
        final var originalStorage = globalStateMgr.getStatisticStorage();

        final var counterMetrics = List.of(
                "statistics_cache_hit_count",
                "statistics_cache_miss_count",
                "statistics_cache_eviction_count",
                "statistics_cache_load_success_count",
                "statistics_cache_load_failure_count");
        final var gaugeMetrics = List.of("statistics_cache_entries");
        // Each metric is registered once per named cache, so this is the expected series count per metric name.
        final int expectedCacheCount = new CachedStatisticStorage().getNamedCacheMap().size();

        try {
            Config.enable_statistic_cache_metrics = true;
            globalStateMgr.setStatisticStorage(new CachedStatisticStorage());

            // WHEN
            Deencapsulation.invoke(MetricRepo.class, "initStatisticsCacheMetrics");

            // THEN
            for (String metricName : counterMetrics) {
                final var metrics = MetricRepo.getMetricsByName(metricName);
                Assertions.assertEquals(expectedCacheCount, metrics.size(),
                        "expected one metric per cache for " + metricName);
                for (var metric : metrics) {
                    Assertions.assertEquals(Metric.MetricType.COUNTER, metric.getType(),
                            metricName + " should be exposed as a counter");
                    Assertions.assertTrue((Long) metric.getValue() >= 0L);
                    // increase() is a no-op: the value is pulled from Caffeine, never pushed.
                    ((CounterMetric<Long>) metric).increase(1L);
                    Assertions.assertTrue((Long) metric.getValue() >= 0L);
                }
            }

            for (String metricName : gaugeMetrics) {
                final var metrics = MetricRepo.getMetricsByName(metricName);
                Assertions.assertEquals(expectedCacheCount, metrics.size(),
                        "expected one metric per cache for " + metricName);

                for (var metric : metrics) {
                    Assertions.assertEquals(Metric.MetricType.GAUGE, metric.getType(),
                            metric + " should be exposed as a gauge");
                    Assertions.assertTrue((Long) metric.getValue() >= 0L);
                }
            }
        } finally {
            // Avoid leaking the registered metrics into the shared registry used by other tests.
            final var registry = Deencapsulation.getField(MetricRepo.class, StarRocksMetricRegistry.class);
            counterMetrics.forEach(registry::removeMetrics);
            gaugeMetrics.forEach(registry::removeMetrics);
            globalStateMgr.setStatisticStorage(originalStorage);
            Config.enable_statistic_cache_metrics = originalEnabled;
        }
    }

    @Test
    public void testRefreshConnectorTableColumnStatisticsBeforeReady() {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr()
                .getTable(connectContext, "hive0", "tpch", "region");
        AtomicInteger queries = new AtomicInteger();
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID,
                                                            List<String> columns) {
                queries.incrementAndGet();
                TStatisticData data = new TStatisticData();
                data.setColumnName("r_regionkey");
                data.setRowCount(5);
                data.setCountDistinct(5);
                data.setMin("0");
                data.setMax("4");
                return ImmutableList.of(data);
            }
        };
        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String tableUUID) {
                return table;
            }
        };
        AtomicBoolean ready = new AtomicBoolean(false);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isReady() {
                return ready.get();
            }
        };

        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(table.getUUID(), "r_regionkey");
        ConnectorTableColumnStats oldStats = new ConnectorTableColumnStats(
                ColumnStatistic.builder().setDistinctValuesCount(1).build(), 1, "2024-01-01 01:00:00");
        cachedStatisticStorage.connectorTableCachedStatistics.synchronous().put(key, Optional.of(oldStats));

        // Replay before ready (startup, or a lagging follower): drop the old value instead of caching an empty one.
        cachedStatisticStorage.refreshConnectorTableColumnStatistics(table, ImmutableList.of("r_regionkey"), true);
        Assertions.assertNull(cachedStatisticStorage.connectorTableCachedStatistics.synchronous().getIfPresent(key));
        Assertions.assertEquals(0, queries.get());

        ready.set(true);
        cachedStatisticStorage.refreshConnectorTableColumnStatistics(table, ImmutableList.of("r_regionkey"), true);
        Assertions.assertEquals(1, queries.get());
        Assertions.assertEquals(5, cachedStatisticStorage.connectorTableCachedStatistics.synchronous()
                .getIfPresent(key).get().getRowCount());
    }

    @Test
    public void testFailedNativeColumnLoadIsNotCached() throws Exception {
        AtomicInteger queries = new AtomicInteger();
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> queryStatisticSync(ConnectContext context, Long dbId, Long tableId,
                                                           List<String> columnNames) {
                if (queries.incrementAndGet() == 1) {
                    throw new RuntimeException("transient statistics query failure");
                }
                return ImmutableList.of();
            }
        };
        boolean enableUnitStatistics = FeConstants.enableUnitStatistics;
        FeConstants.enableUnitStatistics = false;
        try {
            AsyncLoadingCache<ColumnStatsCacheKey, Optional<ColumnStatistic>> cache =
                    Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ColumnBasicStatsCacheLoader());
            ColumnStatsCacheKey key = new ColumnStatsCacheKey(1L, "c1");

            // A failed load completes exceptionally and is not cached as "no statistics".
            Assertions.assertThrows(ExecutionException.class, () -> cache.get(key).get());
            Assertions.assertNull(cache.synchronous().getIfPresent(key));

            // The next access loads again; a column that really has no statistics is cached as empty.
            Assertions.assertFalse(cache.get(key).get().isPresent());
            Assertions.assertEquals(2, queries.get());
            Assertions.assertNotNull(cache.synchronous().getIfPresent(key));
        } finally {
            FeConstants.enableUnitStatistics = enableUnitStatistics;
        }
    }

    @Test
    public void testRefreshConnectorTableColumnStatisticsDropsOldValueWhenNotLoaded() {
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr()
                .getTable(connectContext, "hive0", "tpch", "region");
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(table.getUUID(), "r_regionkey");
        ConnectorTableColumnStats oldStats = new ConnectorTableColumnStats(
                ColumnStatistic.builder().setDistinctValuesCount(1).build(), 1, "2024-01-01 01:00:00");
        AtomicBoolean fail = new AtomicBoolean(false);
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            // The FE turns not-ready between the ready check in refresh and the load (follower lag): the loader
            // returns no entries. Or the statistics query fails.
            @Mock
            public CompletableFuture<Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>>> asyncLoadAll(
                    Iterable<? extends ConnectorTableColumnKey> keys, java.util.concurrent.Executor executor) {
                if (fail.get()) {
                    return CompletableFuture.failedFuture(new RuntimeException("statistics query failure"));
                }
                return CompletableFuture.completedFuture(Maps.newHashMap());
            }
        };

        CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
        for (boolean isSync : new boolean[] {true, false}) {
            for (boolean failed : new boolean[] {false, true}) {
                fail.set(failed);
                cachedStatisticStorage.connectorTableCachedStatistics.synchronous().put(key, Optional.of(oldStats));
                cachedStatisticStorage.refreshConnectorTableColumnStatistics(table, ImmutableList.of("r_regionkey"),
                        isSync);
                Assertions.assertNull(
                        cachedStatisticStorage.connectorTableCachedStatistics.synchronous().getIfPresent(key),
                        "isSync=" + isSync + ", failed=" + failed);
            }
        }
    }

    @Test
    public void testRefreshNativeColumnStatisticsDropsOldValueOnFailure() {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> queryStatisticSync(ConnectContext context, Long dbId, Long tableId,
                                                           List<String> columnNames) {
                throw new RuntimeException("statistics query failure");
            }
        };
        boolean enableUnitStatistics = FeConstants.enableUnitStatistics;
        FeConstants.enableUnitStatistics = false;
        try {
            CachedStatisticStorage cachedStatisticStorage = new CachedStatisticStorage();
            ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), "v1");
            cachedStatisticStorage.columnStatistics.synchronous()
                    .put(key, Optional.of(ColumnStatistic.builder().setDistinctValuesCount(1).build()));

            // Before, the failed refresh hit putAll(null) and left the old value in place as if it were current.
            cachedStatisticStorage.refreshColumnStatistics(table, ImmutableList.of("v1"), true);
            Assertions.assertNull(cachedStatisticStorage.columnStatistics.synchronous().getIfPresent(key));
        } finally {
            FeConstants.enableUnitStatistics = enableUnitStatistics;
        }
    }
}
