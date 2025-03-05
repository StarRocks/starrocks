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

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.partitiontraits.DefaultTraits;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.PartitionField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StatisticsCollectJobTest extends PlanTestNoneDBBase {
    private static long t0StatsTableId = 0;

    private static LocalDateTime t0UpdateTime = LocalDateTime.of(2022, 1, 1, 1, 1, 1);

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, temp.newFolder().toURI().toString());
        Config.statistic_auto_collect_predicate_columns_threshold = 0;
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        starRocksAssert.withTable("CREATE TABLE `t0_stats` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL,\n" +
                "  `v4` date NULL,\n" +
                "  `v5` datetime NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0_stats");
        t0StatsTableId = t0.getId();
        Partition partition = new ArrayList<>(t0.getPartitions()).get(0);
        partition.getDefaultPhysicalPartition().updateVisibleVersion(2, t0UpdateTime
                .atZone(Clock.systemDefaultZone().getZone()).toEpochSecond() * 1000);
        setTableStatistics(t0, 20000000);

        starRocksAssert.withTable("CREATE TABLE `t1_stats` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t1_stats");
        new ArrayList<>(t1.getPartitions()).get(0).getDefaultPhysicalPartition().updateVisibleVersion(2);
        setTableStatistics(t1, 20000000);

        starRocksAssert.withTable("CREATE TABLE `t0_stats_partition` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL,\n" +
                "  `v4` date NULL,\n" +
                "  `v5` datetime NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "PARTITION BY RANGE(`v1`)\n" +
                "(PARTITION p0 VALUES [(\"0\"), (\"1\")),\n" +
                "PARTITION p1 VALUES [(\"1\"), (\"2\")),\n" +
                "PARTITION p2 VALUES [(\"2\"), (\"3\")),\n" +
                "PARTITION p3 VALUES [(\"3\"), (\"4\")),\n" +
                "PARTITION p4 VALUES [(\"4\"), (\"5\")),\n" +
                "PARTITION p5 VALUES [(\"5\"), (\"6\")),\n" +
                "PARTITION p6 VALUES [(\"6\"), (\"7\")),\n" +
                "PARTITION p7 VALUES [(\"7\"), (\"8\")),\n" +
                "PARTITION p8 VALUES [(\"8\"), (\"9\")),\n" +
                "PARTITION p9 VALUES [(\"9\"), (\"10\")))\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        OlapTable t0p = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0_stats_partition");
        new ArrayList<>(t0p.getPartitions()).get(0).getDefaultPhysicalPartition().updateVisibleVersion(2);
        setTableStatistics(t0p, 20000000);

        starRocksAssert.withDatabase("stats");
        starRocksAssert.useDatabase("stats");
        starRocksAssert.withTable("CREATE TABLE `tprimary_stats` (\n" +
                "  `pk` bigint NOT NULL COMMENT \"\",\n" +
                "  `v1` string NOT NULL COMMENT \"\",\n" +
                "  `v2` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        OlapTable tps = (OlapTable) globalStateMgr.getLocalMetastore().getDb("stats").getTable("tprimary_stats");
        new ArrayList<>(tps.getPartitions()).get(0).getDefaultPhysicalPartition().updateVisibleVersion(2);
        setTableStatistics(tps, 20000000);

        starRocksAssert.withTable("CREATE TABLE `tunique_stats` (\n" +
                "  `pk` bigint NOT NULL COMMENT \"\",\n" +
                "  `v1` string NOT NULL COMMENT \"\",\n" +
                "  `v2` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        OlapTable tus = (OlapTable) globalStateMgr.getLocalMetastore().getDb("stats").getTable("tunique_stats");
        new ArrayList<>(tus.getPartitions()).get(0).getDefaultPhysicalPartition().updateVisibleVersion(2);
        setTableStatistics(tps, 20000000);

        starRocksAssert.withTable("CREATE TABLE `tcount` (\n" +
                "  `v1` bigint NOT NULL COMMENT \"\",\n" +
                "  `count` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        OlapTable tcount = (OlapTable) globalStateMgr.getLocalMetastore().getDb("stats").getTable("tcount");
        new ArrayList<>(tcount.getPartitions()).get(0).getDefaultPhysicalPartition().updateVisibleVersion(2);
        setTableStatistics(tcount, 20000000);

        String createStructTableSql = "CREATE TABLE struct_a(\n" +
                "a INT, \n" +
                "b STRUCT<a INT, c INT> COMMENT 'smith',\n" +
                "c STRUCT<a INT, b DOUBLE>,\n" +
                "d STRUCT<a INT, b ARRAY<STRUCT<a INT, b DOUBLE>>, c STRUCT<a INT>>,\n" +
                "struct_a STRUCT<struct_a STRUCT<struct_a INT>, other INT> COMMENT 'alias test'\n" +
                ") DISTRIBUTED BY HASH(`a`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createStructTableSql);
        OlapTable structTable = (OlapTable) globalStateMgr.getLocalMetastore().getDb("stats").getTable("struct_a");
        new ArrayList<>(structTable.getPartitions()).get(0).getDefaultPhysicalPartition().updateVisibleVersion(2);
        setTableStatistics(structTable, 20000000);
    }

    @Before
    public void setUp() {
        super.setUp();
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().clear();
    }

    @Test
    public void testAnalyzeALLDB() {
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID, null, null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(7, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        jobs = jobs.stream().sorted(Comparator.comparingLong(o -> o.getTable().getId())).collect(Collectors.toList());
        HyperStatisticsCollectJob fullStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertTrue("[v1, v2, v3, v4, v5]".contains(
                fullStatisticsCollectJob.getColumnNames().toString()));
        Assert.assertTrue(jobs.get(1) instanceof HyperStatisticsCollectJob);
        fullStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(1);
        Assert.assertTrue("[v4, v5, v6]".contains(
                fullStatisticsCollectJob.getColumnNames().toString()));
    }

    @Test
    public void testAnalyzeDB() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), StatsConstants.DEFAULT_ALL_ID, null, null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(3, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        jobs = jobs.stream().sorted(Comparator.comparingLong(o -> o.getTable().getId())).collect(Collectors.toList());
        HyperStatisticsCollectJob fullStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fullStatisticsCollectJob.getColumnNames().toString());
        Assert.assertTrue(jobs.get(1) instanceof HyperStatisticsCollectJob);
        fullStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(1);
        Assert.assertEquals("[v4, v5, v6]", fullStatisticsCollectJob.getColumnNames().toString());
    }

    @Test
    public void testAnalyzeTable() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(testDb.getId(), t0StatsTableId, null, null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        HyperStatisticsCollectJob fullStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("t0_stats", fullStatisticsCollectJob.getTable().getName());
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fullStatisticsCollectJob.getColumnNames().toString());

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("stats");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tprimary_stats");
        jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), table.getId(), null, null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        fullStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("tprimary_stats", fullStatisticsCollectJob.getTable().getName());
        Assert.assertEquals("[pk, v1, v2]", fullStatisticsCollectJob.getColumnNames().toString());

        table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tunique_stats");
        jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), table.getId(), null, null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        fullStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("tunique_stats", fullStatisticsCollectJob.getTable().getName());
        Assert.assertEquals("[pk]", fullStatisticsCollectJob.getColumnNames().toString());
    }

    @Test
    public void testAnalyzeStructSubFiled() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("stats");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "struct_a");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), table.getId(), ImmutableList.of("b.a", "b.c", "d.c.a"),
                        ImmutableList.of(Type.INT, Type.INT, Type.INT),
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        HyperStatisticsCollectJob fullStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("struct_a", fullStatisticsCollectJob.getTable().getName());
        Assert.assertEquals("[b.a, b.c, d.c.a]", fullStatisticsCollectJob.getColumnNames().toString());
    }

    @Test
    public void testAnalyzeColumn() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), t0StatsTableId, Lists.newArrayList("v2"),
                        Lists.newArrayList(Type.BIGINT),
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        HyperStatisticsCollectJob fullStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v2]", fullStatisticsCollectJob.getColumnNames().toString());
    }

    @Test
    public void testAnalyzeColumnSample() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), t0StatsTableId, Lists.newArrayList("v2"),
                        Lists.newArrayList(Type.BIGINT),
                        StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        HyperStatisticsCollectJob sampleStatisticsCollectJob = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v2]", sampleStatisticsCollectJob.getColumnNames().toString());
    }

    @Test
    public void testAnalyzeColumnSample2() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable olapTable =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0_stats");

        BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), olapTable.getId(), null,
                StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1), Maps.newHashMap());
        basicStatsMeta.increaseDeltaRows(10000000L);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(basicStatsMeta);

        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), olapTable.getId(), Lists.newArrayList("v2"),
                        Lists.newArrayList(Type.BIGINT),
                        StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());

        jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), olapTable.getId(), Lists.newArrayList("v2"),
                        Lists.newArrayList(Type.BIGINT),
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());

        BasicStatsMeta basicStatsMeta2 = new BasicStatsMeta(db.getId(), olapTable.getId(), null,
                StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.of(2022, 1, 1, 1, 1, 1), Maps.newHashMap(),
                basicStatsMeta.getUpdateRows());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(basicStatsMeta2);

        List<StatisticsCollectJob> jobs2 = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), olapTable.getId(), Lists.newArrayList("v2"),
                        Lists.newArrayList(Type.BIGINT),
                        StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs2.size());

        BasicStatsMeta basicStatsMeta3 = new BasicStatsMeta(db.getId(), olapTable.getId(), null,
                StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.of(2021, 1, 1, 1, 1, 1), Maps.newHashMap());
        basicStatsMeta3.increaseDeltaRows(10000000L);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(basicStatsMeta3);

        List<StatisticsCollectJob> job3 = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new NativeAnalyzeJob(db.getId(), olapTable.getId(), Lists.newArrayList("v2"),
                        Lists.newArrayList(Type.BIGINT),
                        StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, job3.size());
        Assert.assertTrue(job3.get(0) instanceof HyperStatisticsCollectJob);
        Assert.assertTrue(job3.get(0).toString().contains("partitionIdList=[10010]"));
    }

    @Test
    public void testAnalyzeHistogram() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable olapTable =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0_stats");
        long dbid = db.getId();

        Map<String, String> properties = new HashMap<>();
        properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, "0.1");
        properties.put(StatsConstants.HISTOGRAM_BUCKET_NUM, "64");
        properties.put(StatsConstants.HISTOGRAM_MCV_SIZE, "100");
        HistogramStatisticsCollectJob histogramStatisticsCollectJob = new HistogramStatisticsCollectJob(
                db, olapTable, Lists.newArrayList("v2"), Lists.newArrayList(Type.BIGINT),
                StatsConstants.ScheduleType.ONCE, properties);

        Config.enable_use_table_sample_collect_statistics = false;
        Function<String, String> normalize = str -> str.replaceAll(" +", " ").toLowerCase();
        String sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                db, olapTable, 0.1, 64L, Maps.newHashMap(), "v2", Type.BIGINT);
        Assert.assertEquals(normalize.apply(String.format("INSERT INTO histogram_statistics(" +
                        "table_id, column_name, db_id, table_name, buckets, mcv, update_time) SELECT %s, 'v2', %d, " +
                        "'test.t0_stats', histogram(`column_key`, cast(64 as int), cast(0.1 as double)),  " +
                        "NULL, NOW() FROM (   SELECT `v2` as column_key    FROM `test`.`t0_stats`     " +
                        "WHERE  rand() <= 0.100000 and `v2` is not null    ORDER BY `v2` LIMIT 10000000) t",
                t0StatsTableId, dbid)), normalize.apply(sql));

        Map<String, String> mostCommonValues = new HashMap<>();
        mostCommonValues.put("1", "10");
        mostCommonValues.put("2", "20");
        sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                db, olapTable, 0.1, 64L, mostCommonValues, "v2", Type.BIGINT);
        Assert.assertEquals(normalize.apply(String.format("INSERT INTO histogram_statistics(" +
                "table_id, column_name, db_id, table_name, buckets, mcv, update_time) SELECT %d, 'v2', %d, " +
                "'test" +
                ".t0_stats'," +
                " " +
                "histogram(`column_key`, cast(64 as int), cast(0.1 as double)),  '[[\"1\",\"10\"],[\"2\",\"20\"]]', NOW() " +
                "FROM (   SELECT `v2` as column_key FROM `test`.`t0_stats` where rand() <= 0.100000 and `v2` is not " +
                "null " +
                " and `v2` " +
                "not in (1,2) ORDER BY `v2` LIMIT 10000000) t", t0StatsTableId, dbid)), normalize.apply(sql));

        mostCommonValues.clear();
        mostCommonValues.put("0000-01-01", "10");
        mostCommonValues.put("1991-01-01", "20");
        sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                db, olapTable, 0.1, 64L, mostCommonValues, "v4", Type.DATE);
        Assert.assertEquals(normalize.apply(String.format("INSERT INTO histogram_statistics(" +
                        "table_id, column_name, db_id, table_name, buckets, mcv, update_time) SELECT %d, 'v4', %d, " +
                        "'test" +
                        ".t0_stats', " +
                "histogram(`column_key`, cast(64 as int), cast(0.1 as double)),  " +
                "'[[\"0000-01-01\",\"10\"],[\"1991-01-01\",\"20\"]]', NOW() FROM " +
                        "( SELECT `v4` as column_key FROM `test`.`t0_stats` where rand() <= 0.100000 and `v4` is not " +
                        "null  " +
                        "and `v4` " +
                        "not in (\"0000-01-01\",\"1991-01-01\") ORDER BY `v4` LIMIT 10000000) t", t0StatsTableId,
                        dbid)),
                normalize.apply(sql));

        mostCommonValues.clear();
        mostCommonValues.put("0000-01-01 00:00:00", "10");
        mostCommonValues.put("1991-01-01 00:00:00", "20");
        sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                db, olapTable, 0.1, 64L, mostCommonValues, "v5", Type.DATETIME);
        Assert.assertEquals(normalize.apply(String.format("INSERT INTO histogram_statistics(" +
                        "table_id, column_name, db_id, table_name, buckets, mcv, update_time) SELECT %d, 'v5', %d, " +
                        "'test.t0_stats', " +
                        "histogram(`column_key`, cast(64 as int), cast(0.1 as double)),  " +
                        "'[[\"1991-01-01 00:00:00\",\"20\"],[\"0000-01-01 00:00:00\",\"10\"]]', NOW() FROM " +
                        "( SELECT `v5` as column_key FROM `test`.`t0_stats` where rand() <= 0.100000 and `v5` is not " +
                        "null  and " +
                        "`v5` not in (\"1991-01-01 00:00:00\",\"0000-01-01 00:00:00\") ORDER BY `v5` LIMIT 10000000) t",
                t0StatsTableId, dbid)), normalize.apply(sql));

        Config.enable_use_table_sample_collect_statistics = true;
        sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                db, olapTable, 0.1, 64L, mostCommonValues, "v5", Type.DATETIME);
        Assert.assertEquals(normalize.apply(String.format("INSERT INTO histogram_statistics(" +
                        "table_id, column_name, db_id, table_name, buckets, mcv, update_time) SELECT %d, 'v5', %d, " +
                        "'test.t0_stats', " +
                        "histogram(`column_key`, cast(64 as int), cast(0.1 as double)),  " +
                        "'[[\"1991-01-01 00:00:00\",\"20\"],[\"0000-01-01 00:00:00\",\"10\"]]', NOW() FROM " +
                        "( SELECT `v5` as column_key FROM `test`.`t0_stats` SAMPLE('percent'='10') where true and " +
                        "`v5` is not null  and " +
                        "`v5` not in (\"1991-01-01 00:00:00\",\"0000-01-01 00:00:00\") ORDER BY `v5` LIMIT 10000000) t",
                t0StatsTableId, dbid)), normalize.apply(sql));

        sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectMCV",
                db, olapTable, 100L, "v2", 0.1);
        Assert.assertEquals(normalize.apply("select cast(version as INT), cast(db_id as BIGINT), cast(table_id as " +
                "BIGINT), " +
                "cast(column_key as varchar), cast(column_value as varchar) from (select 2 as version, " + dbid +
                " as db_id, " + t0StatsTableId +
                " as table_id, `v2` as column_key, count(`v2`) as column_value from `test`.`t0_stats` sample" +
                "('percent'='10') " +
                "where `v2` is not null group by `v2` order by count(`v2`) desc limit 100 ) t"), normalize.apply(sql));
    }

    @Test
    public void testNativeAnalyzeJob() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        NativeAnalyzeJob nativeAnalyzeJob = new NativeAnalyzeJob(testDb.getId(), t0StatsTableId, null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        Assert.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, nativeAnalyzeJob.getCatalogName());

        nativeAnalyzeJob.setWorkTime(LocalDateTime.of(2023, 1, 1, 12, 0, 0));
        Assert.assertEquals("2023-01-01T12:00", nativeAnalyzeJob.getWorkTime().toString());

        nativeAnalyzeJob.setReason("test");
        Assert.assertEquals("test", nativeAnalyzeJob.getReason());

        nativeAnalyzeJob.setStatus(StatsConstants.ScheduleStatus.FINISH);
        Assert.assertEquals(StatsConstants.ScheduleStatus.FINISH, nativeAnalyzeJob.getStatus());

        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);
        statsConnectCtx.setThreadLocalInfo();
        StatisticExecutor statisticExecutor = new StatisticExecutor();

        new MockUp<HyperStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
            }
        };

        nativeAnalyzeJob.run(statsConnectCtx, statisticExecutor);
        Assert.assertEquals(StatsConstants.ScheduleStatus.FINISH, nativeAnalyzeJob.getStatus());

        new MockUp<HyperStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
                throw new RuntimeException("mock exception");
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("stats");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tprimary_stats");
        nativeAnalyzeJob = new NativeAnalyzeJob(db.getId(), table.getId(), null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        nativeAnalyzeJob.run(statsConnectCtx, statisticExecutor);
        Assert.assertEquals(StatsConstants.ScheduleStatus.FAILED, nativeAnalyzeJob.getStatus());
        Assert.assertEquals("mock exception", nativeAnalyzeJob.getReason());
    }

    @Test
    public void testExternalAnalyzeJob() {
        Database database = connectContext.getGlobalStateMgr().getMetadataMgr().getDb("hive0", "partitioned_db");
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");

        ExternalAnalyzeJob externalAnalyzeJob = new ExternalAnalyzeJob("hive0", database.getFullName(),
                table.getName(), null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        Assert.assertEquals("hive0", externalAnalyzeJob.getCatalogName());

        externalAnalyzeJob.setWorkTime(LocalDateTime.of(2023, 1, 1, 12, 0, 0));
        Assert.assertEquals("2023-01-01T12:00", externalAnalyzeJob.getWorkTime().toString());

        externalAnalyzeJob.setReason("test");
        Assert.assertEquals("test", externalAnalyzeJob.getReason());

        externalAnalyzeJob.setStatus(StatsConstants.ScheduleStatus.FINISH);
        Assert.assertEquals(StatsConstants.ScheduleStatus.FINISH, externalAnalyzeJob.getStatus());

        Assert.assertEquals("ExternalAnalyzeJob{id=-1, dbName=partitioned_db, tableName=t1, columns=null, " +
                "type=FULL, scheduleType=SCHEDULE, properties={}, status=FINISH, " +
                "workTime=2023-01-01T12:00, reason='test'}", externalAnalyzeJob.toString());
    }

    @Test
    public void testExternalAnalyzeJobCollect() {
        Database database = connectContext.getGlobalStateMgr().getMetadataMgr().getDb("hive0", "partitioned_db");
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");

        ExternalAnalyzeJob externalAnalyzeJob = new ExternalAnalyzeJob("hive0", database.getFullName(),
                table.getName(), null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);
        statsConnectCtx.setThreadLocalInfo();
        StatisticExecutor statisticExecutor = new StatisticExecutor();

        new MockUp<ExternalFullStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
            }
        };

        externalAnalyzeJob.run(statsConnectCtx, statisticExecutor);
        Assert.assertEquals(StatsConstants.ScheduleStatus.FINISH, externalAnalyzeJob.getStatus());

        new MockUp<ExternalFullStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
                throw new RuntimeException("mock exception");
            }
        };
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> metaMap = Maps.newHashMap();
                metaMap.put(new AnalyzeMgr.StatsMetaKey("hive0", "partitioned_db", "t1"),
                        new ExternalBasicStatsMeta("hive0", "partitioned_db", "t1", null,
                                StatsConstants.AnalyzeType.FULL,
                                LocalDateTime.now().minusHours(2), Maps.newHashMap()));
                return metaMap;
            }
        };
        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ConnectorTableColumnStats> getConnectorTableStatisticsSync(Table table, List<String> columns) {
                return ImmutableList.of(new ConnectorTableColumnStats(new ColumnStatistic(1, 100, 0, 4, 100),
                        100, ""));
            }
        };

        externalAnalyzeJob.run(statsConnectCtx, statisticExecutor);
        Assert.assertEquals(StatsConstants.ScheduleStatus.FAILED, externalAnalyzeJob.getStatus());
        Assert.assertEquals("mock exception", externalAnalyzeJob.getReason());
    }

    @Test
    public void testExternalAnalyzeDb() {
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                return Maps.newHashMap();
            }
        };
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(
                new ExternalAnalyzeJob("hive0", "partitioned_db", null, null,
                        null, StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(13, jobs.size());
    }

    @Test
    public void testExternalAnalyzeTable() {
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(
                new ExternalAnalyzeJob("hive0", "partitioned_db", "t1", null,
                        null, StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        StatisticsCollectJob statisticsCollectJob = jobs.get(0);
        Assert.assertTrue(statisticsCollectJob instanceof ExternalFullStatisticsCollectJob);
        Assert.assertEquals("hive0", statisticsCollectJob.getCatalogName());
        Assert.assertEquals("partitioned_db", statisticsCollectJob.getDb().getFullName());
        Assert.assertEquals("t1", statisticsCollectJob.getTable().getName());
        Assert.assertTrue("[c1, c2, c3, par_col]".contains(
                statisticsCollectJob.getColumnNames().toString()));
    }

    @Test
    public void testCreateHiveAnalyzeJob() {
        ExternalAnalyzeJob analyzeJob = new ExternalAnalyzeJob("hive0", "partitioned_db",
                "t1", List.of("c1"), null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        // do not have stats meta, need to collect
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().
                removeExternalBasicStatsMeta("hive0", "partitioned_db", "t1");
        List<StatisticsCollectJob> statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());

        // test collect statistics time after table update time
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> metaMap = Maps.newHashMap();
                ExternalBasicStatsMeta externalBasicStatsMeta = new ExternalBasicStatsMeta("hive0",
                        "partitioned_db", "t1", null,
                        StatsConstants.AnalyzeType.FULL,
                        LocalDateTime.now().plusHours(2), Maps.newHashMap());
                externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.FULL,
                        LocalDateTime.now().plusHours(2)));
                metaMap.put(new AnalyzeMgr.StatsMetaKey("hive0", "partitioned_db", "t1"),
                        externalBasicStatsMeta);
                return metaMap;
            }
        };
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now();
            }
        };
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(0, statsJobs.size());

        // test collect statistics time before table update time
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> metaMap = Maps.newHashMap();
                ExternalBasicStatsMeta externalBasicStatsMeta = new ExternalBasicStatsMeta("hive0",
                        "partitioned_db", "t1", null,
                        StatsConstants.AnalyzeType.FULL,
                        LocalDateTime.now().minusHours(2), Maps.newHashMap());
                externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("c1", StatsConstants.AnalyzeType.FULL,
                        LocalDateTime.now().minusHours(2)));
                metaMap.put(new AnalyzeMgr.StatsMetaKey("hive0", "partitioned_db", "t1"),
                        externalBasicStatsMeta);
                return metaMap;
            }
        };
        // the default row count is Config.statistic_auto_collect_small_table_rows -1 , need to collect statistics now
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());

        // test collect statistics time before table update time, and row count is 100, need to collect statistics
        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ConnectorTableColumnStats> getConnectorTableStatisticsSync(Table table, List<String> columns) {
                return ImmutableList.of(new ConnectorTableColumnStats(new ColumnStatistic(1, 100, 0, 4, 100),
                        100, ""));
            }
        };
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());
        Assert.assertEquals(3, ((ExternalFullStatisticsCollectJob) statsJobs.get(0)).getPartitionNames().size());

        // test set property STATISTIC_AUTO_COLLECT_INTERVAL to 300s
        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ConnectorTableColumnStats> getConnectorTableStatisticsSync(Table table, List<String> columns) {
                return ImmutableList.of(new ConnectorTableColumnStats(new ColumnStatistic(1, 100, 0, 4, 100),
                        100000000, ""));
            }
        };
        // analyze all columns
        analyzeJob = new ExternalAnalyzeJob("hive0", "partitioned_db",
                "t1", null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                ImmutableMap.of(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL, "300"),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());
        Assert.assertEquals(3, ((ExternalFullStatisticsCollectJob) statsJobs.get(0)).getPartitionNames().size());
    }

    @Test
    public void testCreateIcebergAnalyzeJob() {
        ExternalAnalyzeJob analyzeJob = new ExternalAnalyzeJob("iceberg0", "partitioned_db",
                "t1", List.of("id"), null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        // do not have stats meta, need to collect
        List<StatisticsCollectJob> statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());

        // test collect statistics time after table update time
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> metaMap = Maps.newHashMap();

                ExternalBasicStatsMeta externalBasicStatsMeta = new ExternalBasicStatsMeta("iceberg0",
                        "partitioned_db", "t1",  List.of("id"), StatsConstants.AnalyzeType.FULL,
                        LocalDateTime.now().plusHours(2), Maps.newHashMap());
                externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("id", StatsConstants.AnalyzeType.FULL,
                        LocalDateTime.now().plusHours(2)));
                metaMap.put(new AnalyzeMgr.StatsMetaKey("iceberg0", "partitioned_db", "t1"),
                        externalBasicStatsMeta);
                return metaMap;
            }
        };
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now();
            }
        };
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(0, statsJobs.size());

        // test collect statistics time before table update time
        LocalDateTime statsUpdateTime = LocalDateTime.now().minusHours(2);
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> metaMap = Maps.newHashMap();
                ExternalBasicStatsMeta externalBasicStatsMeta = new ExternalBasicStatsMeta("iceberg0",
                        "partitioned_db", "t1", List.of("id"), StatsConstants.AnalyzeType.FULL,
                        statsUpdateTime, Maps.newHashMap());
                externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("id", StatsConstants.AnalyzeType.FULL,
                        statsUpdateTime));
                metaMap.put(new AnalyzeMgr.StatsMetaKey("iceberg0", "partitioned_db", "t1"),
                        externalBasicStatsMeta);
                return metaMap;
            }
        };
        new MockUp<DefaultTraits>() {
            @Mock
            public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo() {
                return ImmutableMap.of("date=2020-01-01", new com.starrocks.connector.iceberg.Partition(
                        statsUpdateTime.plusSeconds(2).atZone(Clock.systemDefaultZone().getZone()).
                                toInstant().toEpochMilli() * 1000));
            }
        };

        // the default row count is Config.statistic_auto_collect_small_table_rows - 1, need to collect statistics now
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());
        Assert.assertEquals(1, ((ExternalFullStatisticsCollectJob) statsJobs.get(0)).getPartitionNames().size());

        // test collect statistics time before table update time, and row count is 100, need to collect statistics
        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ConnectorTableColumnStats> getConnectorTableStatisticsSync(Table table, List<String> columns) {
                return ImmutableList.of(new ConnectorTableColumnStats(new ColumnStatistic(1, 100, 0, 4, 100),
                        100, ""));
            }
        };
        new MockUp<DefaultTraits>() {
            @Mock
            public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo() {
                long needUpdateTime = statsUpdateTime.plusSeconds(120).
                        atZone(Clock.systemDefaultZone().getZone()).toInstant().toEpochMilli() * 1000;
                long noNeedUpdateTime = statsUpdateTime.minusSeconds(120).
                        atZone(Clock.systemDefaultZone().getZone()).toInstant().toEpochMilli() * 1000;
                return ImmutableMap.of("date=2020-01-01", new com.starrocks.connector.iceberg.Partition(needUpdateTime),
                        "date=2020-01-02", new com.starrocks.connector.iceberg.Partition(needUpdateTime),
                        "date=2020-01-03", new com.starrocks.connector.iceberg.Partition(needUpdateTime),
                        "date=2020-01-04", new com.starrocks.connector.iceberg.Partition(noNeedUpdateTime),
                        "date=2020-01-05", new com.starrocks.connector.iceberg.Partition(noNeedUpdateTime));
            }
        };
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());
        Assert.assertEquals(3, ((ExternalFullStatisticsCollectJob) statsJobs.get(0)).getPartitionNames().size());

        // test set property STATISTIC_AUTO_COLLECT_INTERVAL to 300s
        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ConnectorTableColumnStats> getConnectorTableStatisticsSync(Table table, List<String> columns) {
                return ImmutableList.of(new ConnectorTableColumnStats(new ColumnStatistic(1, 100, 0, 4, 100),
                        100000000, ""));
            }
        };
        // analyze all columns
        analyzeJob = new ExternalAnalyzeJob("iceberg0", "partitioned_db",
                "t1", null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                ImmutableMap.of(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL, "300"),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());
        Assert.assertEquals(5, ((ExternalFullStatisticsCollectJob) statsJobs.get(0)).getPartitionNames().size());
    }

    @Test
    public void testCreatePaimonAnalyzeJob() {
        ExternalAnalyzeJob analyzeJob = new ExternalAnalyzeJob("paimon0", "pmn_db1",
                "partitioned_table", null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        // do not have stats meta, need to collect
        List<StatisticsCollectJob> statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());


        // test collect statistics time after table update time
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> metaMap = Maps.newHashMap();
                ExternalBasicStatsMeta externalBasicStatsMeta =
                        new ExternalBasicStatsMeta("paimon0", "pmn_db1", "partitioned_table", null,
                                StatsConstants.AnalyzeType.FULL, LocalDateTime.now().plusHours(20), Maps.newHashMap());
                externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("pk", null,  LocalDateTime.now().plusHours(20)));
                externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("d", null,  LocalDateTime.now().plusHours(20)));
                externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("pt", null,  LocalDateTime.now().plusHours(20)));

                metaMap.put(new AnalyzeMgr.StatsMetaKey("paimon0", "pmn_db1", "partitioned_table"), externalBasicStatsMeta);
                return metaMap;
            }
        };
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(0, statsJobs.size());

        // test collect statistics time before table update time
        LocalDateTime statsUpdateTime = LocalDateTime.now().minusHours(2);
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> metaMap = Maps.newHashMap();
                metaMap.put(new AnalyzeMgr.StatsMetaKey("paimon0", "pmn_db1", "partitioned_table"),
                        new ExternalBasicStatsMeta("paimon0", "pmn_db1", "partitioned_table", null,
                                StatsConstants.AnalyzeType.FULL,
                                statsUpdateTime, Maps.newHashMap()));
                return metaMap;
            }
        };
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());
    }

    @Test
    public void testCreatePaimonAnalyzeJobWithUnpartitioned() {
        ExternalAnalyzeJob analyzeJob = new ExternalAnalyzeJob("paimon0", "pmn_db1",
                "unpartitioned_table", null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                ExternalBasicStatsMeta externalBasicStatsMeta =
                        new ExternalBasicStatsMeta("paimon0", "pmn_db1", "unpartitioned_table", null,
                                StatsConstants.AnalyzeType.FULL, LocalDateTime.now().plusHours(20), Maps.newHashMap());
                externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("pk", null,  LocalDateTime.now().plusHours(20)));
                externalBasicStatsMeta.addColumnStatsMeta(new ColumnStatsMeta("d", null,  LocalDateTime.now().plusHours(20)));
                Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> metaMap = Maps.newHashMap();
                metaMap.put(new AnalyzeMgr.StatsMetaKey("paimon0", "pmn_db1", "unpartitioned_table"),
                        externalBasicStatsMeta);
                return metaMap;
            }
        };
        List<StatisticsCollectJob> statsJobs =
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(0, statsJobs.size());
    }

    @Test
    public void testFullStatisticsBuildCollectSQLList() {
        OlapTable t0p = (OlapTable) connectContext.getGlobalStateMgr()
                .getLocalMetastore().getDb("test").getTable("t0_stats_partition");
        int i = 1;
        for (Partition p : t0p.getAllPartitions()) {
            p.getDefaultPhysicalPartition().updateVisibleVersion(2);
            p.getDefaultPhysicalPartition().getBaseIndex().setRowCount(i * 100L);
            i++;
        }

        Database database = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(database.getFullName(), "t0_stats_partition");
        List<Long> partitionIdList =
                table.getAllPartitions().stream().map(Partition::getId).collect(Collectors.toList());

        FullStatisticsCollectJob collectJob = new FullStatisticsCollectJob(database, table, partitionIdList,
                Lists.newArrayList("v1", "v2", "v3", "v4", "v5"),
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap()
        );

        List<List<String>> collectSqlList = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(50, collectSqlList.size());

        collectSqlList = collectJob.buildCollectSQLList(128);
        Assert.assertEquals(1, collectSqlList.size());
        assertContains(collectSqlList.get(0).toString(), "v1", "v2", "v3", "v4", "v5");
        assertContains(collectSqlList.get(0).toString(), "p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9");
        Assert.assertEquals(50, StringUtils.countMatches(collectSqlList.toString(), "COUNT(`column_key`)"));
        Assert.assertEquals(5, StringUtils.countMatches(collectSqlList.toString(), "partition `p0`"));
        Assert.assertEquals(5, StringUtils.countMatches(collectSqlList.toString(), "partition `p1`"));
        Assert.assertEquals(5, StringUtils.countMatches(collectSqlList.toString(), "partition `p9`"));

        collectSqlList = collectJob.buildCollectSQLList(15);
        Assert.assertEquals(4, collectSqlList.size());

        for (Partition p : t0p.getAllPartitions()) {
            p.getDefaultPhysicalPartition().updateVisibleVersion(2);
            p.getDefaultPhysicalPartition().getBaseIndex().setRowCount(0);
        }

        collectSqlList = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(50, collectSqlList.size());
    }

    @Test
    public void testExternalFullStatisticsBuildCollectSQLList() {
        Database database = connectContext.getGlobalStateMgr().getMetadataMgr().getDb("hive0", "partitioned_db");
        Table table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1");

        ExternalFullStatisticsCollectJob collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("hive0",
                        database,
                        table, null,
                        Lists.newArrayList("c1", "c2", "c3", "par_col"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        List<List<String>> collectSqlList = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(12, collectSqlList.size());

        collectSqlList = collectJob.buildCollectSQLList(128);
        Assert.assertEquals(1, collectSqlList.size());
        assertContains(collectSqlList.get(0).toString(), "c1", "c2", "c3", "par_col");
        assertContains(collectSqlList.get(0).toString(), "`par_col` = '0'", "`par_col` = '1'", "`par_col` = '2'");

        collectSqlList = collectJob.buildCollectSQLList(3);
        Assert.assertEquals(4, collectSqlList.size());

        database = connectContext.getGlobalStateMgr().getMetadataMgr().getDb("hive0", "tpch");
        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "tpch", "region");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("hive0",
                        database,
                        table, null,
                        Lists.newArrayList("r_regionkey", "r_name", "r_comment"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(3, collectSqlList.size());

        collectSqlList = collectJob.buildCollectSQLList(128);
        Assert.assertEquals(1, collectSqlList.size());
        assertContains(collectSqlList.get(0).toString(), "r_regionkey", "r_name", "r_comment");
        assertContains(collectSqlList.get(0).toString(), "1=1");

        database = connectContext.getGlobalStateMgr().getMetadataMgr().getDb("hive0", "partitioned_db");
        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("hive0", "partitioned_db", "t1_par_null");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("hive0",
                        database,
                        table, null,
                        Lists.newArrayList("c1", "c2", "c3", "par_col", "par_date"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(30, collectSqlList.size());
        collectSqlList = collectJob.buildCollectSQLList(128);
        Assert.assertEquals(1, collectSqlList.size());
        assertContains(collectSqlList.get(0).toString(), "par_col=1/par_date=NULL");
        assertContains(collectSqlList.get(0).toString(), "`par_col` = '1' AND `par_date` IS NULL");
        assertContains(collectSqlList.get(0).toString(), "par_col=NULL/par_date=2020-01-03");
        assertContains(collectSqlList.get(0).toString(), "`par_col` IS NULL AND `par_date` = '2020-01-03'");
    }

    @Test
    public void testIcebergPartitionTransformFullStatisticsBuildCollectSQLList() {
        // test partition column type is timestamp without time zone
        Database database =
                connectContext.getGlobalStateMgr().getMetadataMgr().getDb("iceberg0", "partitioned_transforms_db");
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                        "t0_year");

        ExternalFullStatisticsCollectJob collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        List<List<String>> collectSqlList = collectJob.buildCollectSQLList(1);
        assertContains(collectSqlList.get(0).toString(),
                "`ts` >= '2019-01-01 00:00:00' and `ts` < '2020-01-01 00:00:00'");

        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                "t0_month");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        assertContains(collectSqlList.get(0).toString(),
                "ts` >= '2022-01-01 00:00:00' and `ts` < '2022-02-01 00:00:00'");

        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                "t0_day");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        assertContains(collectSqlList.get(0).toString(),
                "`ts` >= '2022-01-01 00:00:00' and `ts` < '2022-01-02 00:00:00'");

        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                "t0_hour");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        assertContains(collectSqlList.get(0).toString(),
                "`ts` >= '2022-01-01 00:00:00' and `ts` < '2022-01-01 01:00:00'");

        // test partition column type is timestamp with time zone
        String oldTimeZone = connectContext.getSessionVariable().getTimeZone();
        connectContext.getSessionVariable().setTimeZone("America/New_York");
        connectContext.setThreadLocalInfo();

        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                "t0_year_tz");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        assertContains(collectSqlList.get(0).toString(),
                "`ts` >= '2018-12-31 19:00:00' and `ts` < '2019-12-31 19:00:00'");

        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                "t0_month_tz");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        assertContains(collectSqlList.get(0).toString(),
                "`ts` >= '2021-12-31 19:00:00' and `ts` < '2022-01-31 19:00:00'");

        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                "t0_day_tz");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        assertContains(collectSqlList.get(0).toString(),
                "`ts` >= '2021-12-31 19:00:00' and `ts` < '2022-01-01 19:00:00'");

        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                "t0_hour_tz");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        assertContains(collectSqlList.get(0).toString(),
                "`ts` >= '2021-12-31 19:00:00' and `ts` < '2021-12-31 20:00:00'");
        connectContext.getSessionVariable().setTimeZone(oldTimeZone);

        // test partition transform is identity
        database = connectContext.getGlobalStateMgr().getMetadataMgr().getDb("iceberg0", "partitioned_db");
        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_db",
                "t1");
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "date"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        collectSqlList = collectJob.buildCollectSQLList(1);
        assertContains(collectSqlList.get(0).toString(), "`date` = '2020-01-01'");
    }

    @Test
    public void testExternalFullStatisticsBuildCollectSQLWithException1() {
        // test partition transform is bucket
        Database database =
                connectContext.getGlobalStateMgr().getMetadataMgr().getDb("iceberg0", "partitioned_transforms_db");
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                        "t0_bucket");
        ExternalFullStatisticsCollectJob collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());

        expectedException.expect(StarRocksConnectorException.class);
        expectedException.expectMessage("Partition transform BUCKET not supported to analyze, table: t0_bucket");
        collectJob.buildCollectSQLList(1);
    }

    @Test
    public void testExternalFullStatisticsBuildCollectSQLWithException2() {
        // test partition field is null
        Database database =
                connectContext.getGlobalStateMgr().getMetadataMgr().getDb("iceberg0", "partitioned_db");
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_db",
                        "t1");
        ExternalFullStatisticsCollectJob collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "date"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        new MockUp<IcebergTable>() {
            @Mock
            public PartitionField getPartitionFiled(String colName) {
                return null;
            }
        };

        expectedException.expect(StarRocksConnectorException.class);
        expectedException.expectMessage("Partition column date not found in table iceberg0.partitioned_db.t1");
        collectJob.buildCollectSQLList(1);
    }

    @Test
    public void testExternalFullStatisticsBuildCollectSQLWithException3() {
        // test partition transform is bucket
        Database database =
                connectContext.getGlobalStateMgr().getMetadataMgr().getDb("iceberg0", "partitioned_transforms_db");
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable("iceberg0", "partitioned_transforms_db",
                        "t0_date_month_identity_evolution");
        ExternalFullStatisticsCollectJob collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("iceberg0",
                        database,
                        table, null,
                        Lists.newArrayList("id", "data", "ts"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());

        expectedException.expect(StarRocksConnectorException.class);
        expectedException.expectMessage("Do not supported analyze iceberg table" +
                " t0_date_month_identity_evolution with partition evolution");
        collectJob.buildCollectSQLList(1);
    }

    @Test
    public void testExternalPaimonFullStatisticsBuildCollectSQL() {
        // test partition
        Database database =
                connectContext.getGlobalStateMgr().getMetadataMgr().getDb("paimon0", "pmn_db1");
        Table table =
                connectContext.getGlobalStateMgr().getMetadataMgr().getTable("paimon0", "pmn_db1",
                        "partitioned_table");
        ExternalFullStatisticsCollectJob collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("paimon0",
                        database,
                        table, null,
                        Lists.newArrayList("pk", "d", "pt"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        List<List<String>> lists = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(30, lists.size());

        //test partition is null
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("paimon0",
                        database,
                        table, Lists.newArrayList("pt=null"),
                        Lists.newArrayList("pk", "d", "pt"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        lists = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(3, lists.size());

        //test unpartitioned table
        table = connectContext.getGlobalStateMgr().getMetadataMgr().getTable("paimon0", "pmn_db1",
                "unpartitioned_table");
        //test partition is null
        collectJob = (ExternalFullStatisticsCollectJob)
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob("paimon0",
                        database,
                        table, null,
                        Lists.newArrayList("pk", "d"),
                        StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE,
                        Maps.newHashMap());
        lists = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(2, lists.size());
    }

    @Test
    public void testExcludeStatistics() {
        OlapTable table = (OlapTable) connectContext.getGlobalStateMgr()
                .getLocalMetastore().getDb("test").getTable("t0_stats_partition");

        Database database = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");

        NativeAnalyzeJob job = new NativeAnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID,
                null, null,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                ImmutableMap.of(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        List<StatisticsCollectJob> allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);

        Assert.assertEquals(7, allJobs.size());
        Assert.assertTrue(allJobs.stream().anyMatch(j -> table.equals(j.getTable())));

        job = new NativeAnalyzeJob(database.getId(), StatsConstants.DEFAULT_ALL_ID, null, null,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                ImmutableMap.of(StatsConstants.STATISTIC_EXCLUDE_PATTERN, ".*"),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(0, allJobs.size());

        job = new NativeAnalyzeJob(database.getId(), StatsConstants.DEFAULT_ALL_ID, null, null,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                ImmutableMap.of(StatsConstants.STATISTIC_EXCLUDE_PATTERN, "test/."),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(3, allJobs.size());

        job = new NativeAnalyzeJob(database.getId(), StatsConstants.DEFAULT_ALL_ID, null, null,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                ImmutableMap.of(StatsConstants.STATISTIC_EXCLUDE_PATTERN, "test.t0_stats_partition"),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(2, allJobs.size());
        Assert.assertTrue(allJobs.stream().noneMatch(j -> j.getTable().getName().contains("t0_stats_partition")));

        job = new NativeAnalyzeJob(database.getId(), StatsConstants.DEFAULT_ALL_ID, null, null,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                ImmutableMap.of(StatsConstants.STATISTIC_EXCLUDE_PATTERN, "(test.t0_stats_partition)|(test.t1_stats)"),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(1, allJobs.size());
        Assert.assertTrue(allJobs.stream().noneMatch(j -> j.getTable().getName().contains("t0_stats_partition")));
        Assert.assertTrue(allJobs.stream().noneMatch(j -> j.getTable().getName().contains("t1_stats")));
    }

    @Test
    public void testCount() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("stats");
        OlapTable olapTable =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tcount");
        long dbid = db.getId();

        SampleStatisticsCollectJob sampleStatisticsCollectJob = new SampleStatisticsCollectJob(
                db, olapTable, Lists.newArrayList("v1", "count"),
                StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap());

        FullStatisticsCollectJob fullStatisticsCollectJob = new FullStatisticsCollectJob(
                db, olapTable,
                olapTable.getAllPartitionIds(),
                Lists.newArrayList("v1", "count"),
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap()
        );
        String sql = Deencapsulation.invoke(fullStatisticsCollectJob, "buildBatchCollectFullStatisticSQL",
                olapTable, olapTable.getPartition("tcount"), "count", Type.INT);
        assertContains(sql, "`stats`.`tcount` partition `tcount`");
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
    }

    @Test
    public void testAnalyzeBeforeUpdate() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        NativeAnalyzeJob job = new NativeAnalyzeJob(db.getId(), t0StatsTableId, null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        HyperStatisticsCollectJob fjb = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("t0_stats", fjb.getTable().getName());
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumnNames().toString());

        // collect 1st
        BasicStatsMeta execMeta = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), Maps.newHashMap());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(execMeta);

        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now().minusDays(1);
            }
        };

        jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertTrue(jobs.isEmpty());
    }

    @Test
    public void testAnalyzeInInterval() {
        LocalDateTime now = LocalDateTime.now();
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return now;
            }

            @Mock
            public LocalDateTime getPartitionLastUpdateTime(Partition partition) {
                return now;
            }
        };
        new MockUp<Partition>() {
            @Mock
            public long getDataSize() {
                return Config.statistic_auto_collect_small_table_size + 10;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        BasicStatsMeta execMeta1 = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusSeconds(Config.statistic_auto_collect_large_table_interval).minusHours(1),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(execMeta1);

        new Expectations(execMeta1) {
            {
                execMeta1.getHealthy();
                result = 0.7;
            }
        };

        NativeAnalyzeJob job = new NativeAnalyzeJob(db.getId(), t0StatsTableId, null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        HyperStatisticsCollectJob fjb = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumnNames().toString());

        BasicStatsMeta execMeta2 = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusHours(1),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(execMeta2);

        new Expectations(execMeta2) {
            {
                execMeta2.getHealthy();
                times = 1;
            }
        };

        jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(0, jobs.size());
    }

    @Test
    public void testAnalyzeInIntervalSmallTable() {
        LocalDateTime now = LocalDateTime.now();
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return now;
            }

            @Mock
            public LocalDateTime getPartitionLastUpdateTime(Partition partition) {
                return now;
            }
        };
        new MockUp<Partition>() {
            @Mock
            public long getDataSize() {
                return Config.statistic_auto_collect_small_table_size - 10;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        BasicStatsMeta execMeta1 = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusSeconds(Config.statistic_auto_collect_large_table_interval).minusHours(1),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(execMeta1);

        new Expectations(execMeta1) {
            {
                execMeta1.getHealthy();
                result = 0.7;
            }
        };

        NativeAnalyzeJob job = new NativeAnalyzeJob(db.getId(), t0StatsTableId, null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
        HyperStatisticsCollectJob fjb = (HyperStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumnNames().toString());

        Config.statistic_auto_collect_small_table_interval = 100;
        BasicStatsMeta execMeta2 = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusSeconds(50),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(execMeta2);

        Config.statistic_auto_collect_small_table_interval = 100;
        jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Config.statistic_auto_collect_small_table_interval = 0;
        Assert.assertEquals(0, jobs.size());
    }

    @Test
    public void testAnalyzeHealth() {
        LocalDateTime now = LocalDateTime.now();
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return now;
            }

            @Mock
            public LocalDateTime getPartitionLastUpdateTime(Partition partition) {
                return now;
            }
        };
        new MockUp<Partition>() {
            @Mock
            public long getDataSize() {
                return Config.statistic_auto_collect_small_table_size + 10;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        BasicStatsMeta execMeta = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusSeconds(Config.statistic_auto_collect_large_table_interval).minusHours(1),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(execMeta);

        NativeAnalyzeJob job = new NativeAnalyzeJob(db.getId(), t0StatsTableId, null, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        {
            // healthy = 1
            new Expectations(execMeta) {
                {
                    execMeta.getHealthy();
                    result = 1;
                }

            };

            List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
            Assert.assertEquals(0, jobs.size());
        }

        {
            // healthy = 0.7
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(execMeta);

            new Expectations(execMeta) {
                {
                    execMeta.getHealthy();
                    result = 0.7;
                }
            };

            List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
            Assert.assertEquals(1, jobs.size());
            Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
            HyperStatisticsCollectJob fjb = (HyperStatisticsCollectJob) jobs.get(0);
            Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumnNames().toString());
        }

        {
            // healthy = 0.2 && big table
            new Expectations(execMeta) {
                {
                    execMeta.getHealthy();
                    result = 0.2d;
                }
            };

            List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
            Assert.assertEquals(1, jobs.size());
            Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
            HyperStatisticsCollectJob fjb = (HyperStatisticsCollectJob) jobs.get(0);
            Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumnNames().toString());
        }

        {
            new MockUp<Partition>() {
                @Mock
                public long getDataSize() {
                    return Config.statistic_auto_collect_small_table_size - 10;
                }
            };
            // healthy = 0.1 && small table
            new Expectations(execMeta) {
                {
                    execMeta.getHealthy();
                    result = 0.1;
                }
            };

            List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
            Assert.assertEquals(1, jobs.size());
            Assert.assertTrue(jobs.get(0) instanceof HyperStatisticsCollectJob);
            HyperStatisticsCollectJob fjb = (HyperStatisticsCollectJob) jobs.get(0);
            Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumnNames().toString());
        }
    }

    @Test
    public void testGetCollectibleColumns1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.t_gen_col (" +
                " c1 datetime NOT NULL," +
                " c2 bigint," +
                " c3 DATETIME NULL AS date_trunc('month', c1) " +
                " ) " +
                " DUPLICATE KEY(c1) " +
                " PARTITION BY (c2, c3) " +
                " PROPERTIES('replication_num'='1')");
        Table table = starRocksAssert.getTable("test", "t_gen_col");
        List<String> cols =  StatisticUtils.getCollectibleColumns(table);
        Assert.assertTrue(cols.size() == 3);
        Assert.assertTrue(cols.contains("c3"));
        starRocksAssert.dropTable("test.t_gen_col");
    }

    @Test
    public void testGetCollectibleColumns2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.t_gen_col (" +
                " c1 datetime NOT NULL," +
                " c2 bigint" +
                " ) " +
                " DUPLICATE KEY(c1) " +
                " PARTITION BY c2, date_trunc('month', c1) " +
                " PROPERTIES('replication_num'='1')");
        Table table = starRocksAssert.getTable("test", "t_gen_col");
        List<String> cols =  StatisticUtils.getCollectibleColumns(table);
        Assert.assertTrue(cols.size() == 2);
        starRocksAssert.dropTable("test.t_gen_col");
    }

    @Test
    public void testPriorityComparison() {
        // Test case with different health values
        StatisticsCollectJob.Priority priority1 =
                new StatisticsCollectJob.Priority(LocalDateTime.now(), LocalDateTime.now(), 0.5);
        StatisticsCollectJob.Priority priority2 =
                new StatisticsCollectJob.Priority(LocalDateTime.now(), LocalDateTime.now(), 0.6);
        Assert.assertTrue(priority1.compareTo(priority2) < 0);

        // Test case with different staleness values
        LocalDateTime now = LocalDateTime.now();
        StatisticsCollectJob.Priority priority3 = new StatisticsCollectJob.Priority(now, now.minusSeconds(100), 0.5);
        StatisticsCollectJob.Priority priority4 = new StatisticsCollectJob.Priority(now, now.minusSeconds(50), 0.5);
        Assert.assertTrue(priority3.compareTo(priority4) < 0);

        // Test case with both different health and staleness values
        StatisticsCollectJob.Priority priority5 = new StatisticsCollectJob.Priority(now, now.minusSeconds(100), 0.5);
        StatisticsCollectJob.Priority priority6 = new StatisticsCollectJob.Priority(now, now.minusSeconds(50), 0.6);
        Assert.assertTrue(priority5.compareTo(priority6) < 0);

        // Test case with statsUpdateTime set to LocalDateTime.MIN
        StatisticsCollectJob.Priority priority7 = new StatisticsCollectJob.Priority(now, LocalDateTime.MIN, 0.5);
        StatisticsCollectJob.Priority priority8 = new StatisticsCollectJob.Priority(now, now.minusSeconds(10), 0.5);
        Assert.assertTrue(priority7.compareTo(priority8) < 0);
    }
}
