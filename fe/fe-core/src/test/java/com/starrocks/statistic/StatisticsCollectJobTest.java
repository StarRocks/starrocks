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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatisticsCollectJobTest extends PlanTestNoneDBBase {
    private static long t0StatsTableId = 0;

    private static LocalDateTime t0UpdateTime = LocalDateTime.of(2022, 1, 1, 1, 1, 1);

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0_stats");
        t0StatsTableId = t0.getId();
        Partition partition = new ArrayList<>(t0.getPartitions()).get(0);
        partition.updateVisibleVersion(2, t0UpdateTime
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1_stats");
        new ArrayList<>(t1.getPartitions()).get(0).updateVisibleVersion(2);
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
        OlapTable t0p = (OlapTable) globalStateMgr.getDb("test").getTable("t0_stats_partition");
        new ArrayList<>(t0p.getPartitions()).get(0).updateVisibleVersion(2);
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        OlapTable tps = (OlapTable) globalStateMgr.getDb("stats").getTable("tprimary_stats");
        new ArrayList<>(tps.getPartitions()).get(0).updateVisibleVersion(2);
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
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        OlapTable tus = (OlapTable) globalStateMgr.getDb("stats").getTable("tunique_stats");
        new ArrayList<>(tus.getPartitions()).get(0).updateVisibleVersion(2);
        setTableStatistics(tps, 20000000);

        starRocksAssert.withTable("CREATE TABLE `tcount` (\n" +
                "  `v1` bigint NOT NULL COMMENT \"\",\n" +
                "  `count` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        OlapTable tcount = (OlapTable) globalStateMgr.getDb("stats").getTable("tcount");
        new ArrayList<>(tcount.getPartitions()).get(0).updateVisibleVersion(2);
        setTableStatistics(tcount, 20000000);
    }

    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().clear();
    }

    @Test
    public void testAnalyzeALLDB() {
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID, null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(6, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
        jobs = jobs.stream().sorted(Comparator.comparingLong(o -> o.getTable().getId())).collect(Collectors.toList());
        FullStatisticsCollectJob fullStatisticsCollectJob = (FullStatisticsCollectJob) jobs.get(0);
        Assert.assertTrue("[v1, v2, v3, v4, v5]".contains(
                fullStatisticsCollectJob.getColumns().toString()));
        Assert.assertTrue(jobs.get(1) instanceof FullStatisticsCollectJob);
        fullStatisticsCollectJob = (FullStatisticsCollectJob) jobs.get(1);
        Assert.assertTrue("[v4, v5, v6]".contains(
                fullStatisticsCollectJob.getColumns().toString()));
    }

    @Test
    public void testAnalyzeDB() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(db.getId(), StatsConstants.DEFAULT_ALL_ID, null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(3, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
        jobs = jobs.stream().sorted(Comparator.comparingLong(o -> o.getTable().getId())).collect(Collectors.toList());
        FullStatisticsCollectJob fullStatisticsCollectJob = (FullStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fullStatisticsCollectJob.getColumns().toString());
        Assert.assertTrue(jobs.get(1) instanceof FullStatisticsCollectJob);
        fullStatisticsCollectJob = (FullStatisticsCollectJob) jobs.get(1);
        Assert.assertEquals("[v4, v5, v6]", fullStatisticsCollectJob.getColumns().toString());
    }

    @Test
    public void testAnalyzeTable() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(testDb.getId(), t0StatsTableId, null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
        FullStatisticsCollectJob fullStatisticsCollectJob = (FullStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("t0_stats", fullStatisticsCollectJob.getTable().getName());
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fullStatisticsCollectJob.getColumns().toString());

        Database db = GlobalStateMgr.getCurrentState().getDb("stats");
        Table table = db.getTable("tprimary_stats");
        jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(db.getId(), table.getId(), null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
        fullStatisticsCollectJob = (FullStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("tprimary_stats", fullStatisticsCollectJob.getTable().getName());
        Assert.assertEquals("[pk, v1, v2]", fullStatisticsCollectJob.getColumns().toString());

        table = db.getTable("tunique_stats");
        jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(db.getId(), table.getId(), null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
        fullStatisticsCollectJob = (FullStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("tunique_stats", fullStatisticsCollectJob.getTable().getName());
        Assert.assertEquals("[pk]", fullStatisticsCollectJob.getColumns().toString());
    }

    @Test
    public void testAnalyzeColumn() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(db.getId(), t0StatsTableId, Lists.newArrayList("v2"),
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
        FullStatisticsCollectJob fullStatisticsCollectJob = (FullStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v2]", fullStatisticsCollectJob.getColumns().toString());
    }

    @Test
    public void testAnalyzeColumnSample() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(db.getId(), t0StatsTableId, Lists.newArrayList("v2"),
                        StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof SampleStatisticsCollectJob);
        SampleStatisticsCollectJob sampleStatisticsCollectJob = (SampleStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v2]", sampleStatisticsCollectJob.getColumns().toString());
    }

    @Test
    public void testAnalyzeColumnSample2() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable olapTable = (OlapTable) db.getTable("t0_stats");

        BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), olapTable.getId(), null,
                StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1), Maps.newHashMap());
        basicStatsMeta.increaseUpdateRows(10000000L);
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(basicStatsMeta);

        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(db.getId(), olapTable.getId(), Lists.newArrayList("v2"),
                        StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());

        jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(db.getId(), olapTable.getId(), Lists.newArrayList("v2"),
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());

        BasicStatsMeta basicStatsMeta2 = new BasicStatsMeta(db.getId(), olapTable.getId(), null,
                StatsConstants.AnalyzeType.SAMPLE,
                LocalDateTime.of(2022, 1, 1, 1, 1, 1), Maps.newHashMap());
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(basicStatsMeta2);

        List<StatisticsCollectJob> jobs2 = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(db.getId(), olapTable.getId(), Lists.newArrayList("v2"),
                        StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(0, jobs2.size());
    }

    @Test
    public void testAnalyzeHistogram() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable olapTable = (OlapTable) db.getTable("t0_stats");
        long dbid = db.getId();

        Map<String, String> properties = new HashMap<>();
        properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, "0.1");
        properties.put(StatsConstants.HISTOGRAM_BUCKET_NUM, "64");
        properties.put(StatsConstants.HISTOGRAM_MCV_SIZE, "100");
        HistogramStatisticsCollectJob histogramStatisticsCollectJob = new HistogramStatisticsCollectJob(
                db, olapTable, Lists.newArrayList("v2"),
                StatsConstants.AnalyzeType.HISTOGRAM, StatsConstants.ScheduleType.ONCE,
                properties);

        String sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                db, olapTable, 0.1, 64L, Maps.newHashMap(), "v2");
        Assert.assertEquals(String.format("INSERT INTO histogram_statistics SELECT %d, 'v2', %d, 'test.t0_stats', " +
                        "histogram(`v2`, cast(64 as int), cast(0.1 as double)),  NULL, NOW() FROM " +
                        "(SELECT `v2` FROM `test`.`t0_stats` where rand() <= 0.1 and `v2` is not null  " +
                        "ORDER BY `v2` LIMIT 10000000) t",
                t0StatsTableId, dbid), sql);

        Map<String, String> mostCommonValues = new HashMap<>();
        mostCommonValues.put("1", "10");
        mostCommonValues.put("2", "20");
        sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                db, olapTable, 0.1, 64L, mostCommonValues, "v2");
        Assert.assertEquals(String.format("INSERT INTO histogram_statistics SELECT %s, 'v2', %d, 'test.t0_stats', " +
                "histogram(`v2`, cast(64 as int), cast(0.1 as double)),  '[[\"1\",\"10\"],[\"2\",\"20\"]]', NOW() " +
                "FROM (SELECT `v2` FROM `test`.`t0_stats` where rand() <= 0.1 and `v2` is not null  and `v2` not in (1,2) " +
                "ORDER BY `v2` LIMIT 10000000) t", t0StatsTableId, dbid), sql);

        mostCommonValues.clear();
        mostCommonValues.put("0000-01-01", "10");
        mostCommonValues.put("1991-01-01", "20");
        sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                db, olapTable, 0.1, 64L, mostCommonValues, "v4");
        Assert.assertEquals(String.format("INSERT INTO histogram_statistics SELECT %s, 'v4', %d, 'test.t0_stats', " +
                "histogram(`v4`, cast(64 as int), cast(0.1 as double)),  '[[\"0000-01-01\",\"10\"],[\"1991-01-01\",\"20\"]]', " +
                "NOW() FROM (SELECT `v4` FROM `test`.`t0_stats` where rand() <= 0.1 and `v4` is not null  and `v4` not in " +
                "(\"0000-01-01\",\"1991-01-01\") ORDER BY `v4` LIMIT 10000000) t", t0StatsTableId, dbid), sql);

        mostCommonValues.clear();
        mostCommonValues.put("0000-01-01 00:00:00", "10");
        mostCommonValues.put("1991-01-01 00:00:00", "20");
        sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectHistogram",
                db, olapTable, 0.1, 64L, mostCommonValues, "v5");
        Assert.assertEquals("INSERT INTO histogram_statistics SELECT " + t0StatsTableId + ", 'v5', " + dbid +
                        ", 'test" +
                        ".t0_stats', " +
                        "histogram(`v5`, cast(64 as int), cast(0.1 as double)),  " +
                        "'[[\"1991-01-01 00:00:00\",\"20\"],[\"0000-01-01 00:00:00\",\"10\"]]', NOW() FROM " +
                        "(SELECT `v5` FROM `test`.`t0_stats` where rand() <= 0.1 and `v5` is not null  " +
                        "and `v5` not in (\"1991-01-01 00:00:00\",\"0000-01-01 00:00:00\") ORDER BY `v5` LIMIT 10000000) t",
                sql);

        sql = Deencapsulation.invoke(histogramStatisticsCollectJob, "buildCollectMCV",
                db, olapTable, 100L, "v2");
        Assert.assertEquals("select cast(version as INT), cast(db_id as BIGINT), cast(table_id as BIGINT), " +
                "cast(column_key as varchar), cast(column_value as varchar) from (select 2 as version, " + dbid +
                " as db_id, " + t0StatsTableId +
                " as table_id, `v2` as column_key, count(`v2`) as column_value from `test`.`t0_stats` " +
                "where `v2` is not null group by `v2` order by count(`v2`) desc limit 100 ) t", sql);
    }

    @Test
<<<<<<< HEAD
=======
    public void testNativeAnalyzeJob() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

        NativeAnalyzeJob nativeAnalyzeJob = new NativeAnalyzeJob(testDb.getId(), t0StatsTableId, null,
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

        new MockUp<FullStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {}
        };

        nativeAnalyzeJob.run(statsConnectCtx, statisticExecutor);
        Assert.assertEquals(StatsConstants.ScheduleStatus.PENDING, nativeAnalyzeJob.getStatus());

        new MockUp<FullStatisticsCollectJob>() {
            @Mock
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
                throw new RuntimeException("mock exception");
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getDb("stats");
        Table table = db.getTable("tprimary_stats");
        nativeAnalyzeJob = new NativeAnalyzeJob(db.getId(), table.getId(), null,
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
                table.getName(), null,
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
                table.getName(), null,
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
            public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {}
        };

        externalAnalyzeJob.run(statsConnectCtx, statisticExecutor);
        Assert.assertEquals(StatsConstants.ScheduleStatus.PENDING, externalAnalyzeJob.getStatus());

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
                        100));
            }
        };

        externalAnalyzeJob.run(statsConnectCtx, statisticExecutor);
        Assert.assertEquals(StatsConstants.ScheduleStatus.FAILED, externalAnalyzeJob.getStatus());
        Assert.assertEquals("mock exception", externalAnalyzeJob.getReason());
    }

    @Test
    public void testExternalAnalyzeAllDb() {
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                return Maps.newHashMap();
            }
        };
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(
                new ExternalAnalyzeJob("hive0", null, null, null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(27, jobs.size());
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
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(11, jobs.size());
    }

    @Test
    public void testExternalAnalyzeTable() {
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(
                new ExternalAnalyzeJob("hive0", "partitioned_db", "t1", null,
                        StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
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
                statisticsCollectJob.getColumns().toString()));
    }

    @Test
    public void testCreateExternalAnalyzeJob() {
        ExternalAnalyzeJob analyzeJob = new ExternalAnalyzeJob("hive0", "partitioned_db",
                "t1", null,
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
                metaMap.put(new AnalyzeMgr.StatsMetaKey("hive0", "partitioned_db", "t1"),
                        new ExternalBasicStatsMeta("hive0", "partitioned_db", "t1", null,
                                StatsConstants.AnalyzeType.FULL,
                                LocalDateTime.now().plusHours(2), Maps.newHashMap()));
                return metaMap;
            }
        };
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(0, statsJobs.size());

        // test collect statistics time before table update time
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
        // the default row count is Config.statistic_auto_collect_small_table_rows, do not need to collect statistics
        // until table update time + time interval(12h by default) > now
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(0, statsJobs.size());

        // test collect statistics time before table update time, and row count is 100, need to collect statistics
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
                        100));
            }
        };
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());
        Assert.assertEquals(3, ((ExternalFullStatisticsCollectJob) statsJobs.get(0)).getPartitionNames().size());

        // test set property STATISTIC_AUTO_COLLECT_INTERVAL to 300s
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
                        100000000));
            }
        };

        analyzeJob = new ExternalAnalyzeJob("hive0", "partitioned_db",
                "t1", null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                ImmutableMap.of(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL, "300"),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);
        statsJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(analyzeJob);
        Assert.assertEquals(1, statsJobs.size());
        Assert.assertEquals(3, ((ExternalFullStatisticsCollectJob) statsJobs.get(0)).getPartitionNames().size());
    }

    @Test
>>>>>>> fc74a4dd60 ([Enhancement] Fix the checkstyle of semicolons (#33130))
    public void testSplitColumns() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(
                new AnalyzeJob(db.getId(), t0StatsTableId, null,
                        StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.SCHEDULE,
                        Maps.newHashMap(),
                        StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN));
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof SampleStatisticsCollectJob);

        int splitSize = Deencapsulation.invoke(jobs.get(0), "splitColumns", 10L);
        Assert.assertEquals(5, splitSize);

        splitSize = Deencapsulation.invoke(jobs.get(0), "splitColumns",
                Config.statistic_collect_max_row_count_per_query);
        Assert.assertEquals(2, splitSize);

        splitSize = Deencapsulation.invoke(jobs.get(0), "splitColumns",
                Config.statistic_collect_max_row_count_per_query + 1);
        Assert.assertEquals(1, splitSize);

        splitSize = Deencapsulation.invoke(jobs.get(0), "splitColumns", 0L);
        Assert.assertEquals(5, splitSize);
    }

    @Test
    public void testFullStatisticsBuildCollectSQLList() {
        OlapTable t0p = (OlapTable) connectContext.getGlobalStateMgr()
                .getDb("test").getTable("t0_stats_partition");
        int i = 1;
        for (Partition p : t0p.getAllPartitions()) {
            p.updateVisibleVersion(2);
            p.getBaseIndex().setRowCount(i * 100L);
            i++;
        }

        Database database = connectContext.getGlobalStateMgr().getDb("test");
        OlapTable table = (OlapTable) database.getTable("t0_stats_partition");
        List<Long> partitionIdList =
                table.getAllPartitions().stream().map(Partition::getId).collect(Collectors.toList());

        FullStatisticsCollectJob collectJob = new FullStatisticsCollectJob(database, table, partitionIdList,
                Lists.newArrayList("v1", "v2", "v3", "v4", "v5"),
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap());

        List<List<String>> collectSqlList = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(50, collectSqlList.size());

        collectSqlList = collectJob.buildCollectSQLList(128);
        Assert.assertEquals(1, collectSqlList.size());
        assertContains(collectSqlList.get(0).toString(), "v1", "v2", "v3", "v4", "v5");
        assertContains(collectSqlList.get(0).toString(), "p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9");
        Assert.assertEquals(10, StringUtils.countMatches(collectSqlList.toString(), "COUNT(`v1`)"));
        Assert.assertEquals(10, StringUtils.countMatches(collectSqlList.toString(), "COUNT(`v3`)"));
        Assert.assertEquals(10, StringUtils.countMatches(collectSqlList.toString(), "COUNT(`v5`)"));
        Assert.assertEquals(5, StringUtils.countMatches(collectSqlList.toString(), "partition `p0`"));
        Assert.assertEquals(5, StringUtils.countMatches(collectSqlList.toString(), "partition `p1`"));
        Assert.assertEquals(5, StringUtils.countMatches(collectSqlList.toString(), "partition `p9`"));

        collectSqlList = collectJob.buildCollectSQLList(15);
        Assert.assertEquals(4, collectSqlList.size());

        for (Partition p : t0p.getAllPartitions()) {
            p.updateVisibleVersion(2);
            p.getBaseIndex().setRowCount(0);
        }

        collectSqlList = collectJob.buildCollectSQLList(1);
        Assert.assertEquals(50, collectSqlList.size());
    }

    @Test
    public void testExcludeStatistics() {
        OlapTable table = (OlapTable) connectContext.getGlobalStateMgr()
                .getDb("test").getTable("t0_stats_partition");

        Database database = connectContext.getGlobalStateMgr().getDb("test");

        AnalyzeJob job = new AnalyzeJob(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID, null,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                ImmutableMap.of(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        List<StatisticsCollectJob> allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);

        Assert.assertEquals(6, allJobs.size());
        Assert.assertTrue(allJobs.stream().anyMatch(j -> table.equals(j.getTable())));

        job = new AnalyzeJob(database.getId(), StatsConstants.DEFAULT_ALL_ID, null,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                ImmutableMap.of(StatsConstants.STATISTIC_EXCLUDE_PATTERN, ".*"),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(0, allJobs.size());

        job = new AnalyzeJob(database.getId(), StatsConstants.DEFAULT_ALL_ID, null,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                ImmutableMap.of(StatsConstants.STATISTIC_EXCLUDE_PATTERN, "test/."),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(3, allJobs.size());

        job = new AnalyzeJob(database.getId(), StatsConstants.DEFAULT_ALL_ID, null,
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                ImmutableMap.of(StatsConstants.STATISTIC_EXCLUDE_PATTERN, "test.t0_stats_partition"),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        allJobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(2, allJobs.size());
        Assert.assertTrue(allJobs.stream().noneMatch(j -> j.getTable().getName().contains("t0_stats_partition")));

        job = new AnalyzeJob(database.getId(), StatsConstants.DEFAULT_ALL_ID, null,
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
        Database db = GlobalStateMgr.getCurrentState().getDb("stats");
        OlapTable olapTable = (OlapTable) db.getTable("tcount");
        long dbid = db.getId();

        SampleStatisticsCollectJob sampleStatisticsCollectJob = new SampleStatisticsCollectJob(
                db, olapTable, Lists.newArrayList("v1", "count"),
                StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap());

        String sql = Deencapsulation.invoke(sampleStatisticsCollectJob, "buildSampleInsertSQL",
                dbid, olapTable.getId(), Lists.newArrayList("v1", "count"), 100L);
        assertContains(sql, "`stats`.`tcount`  Tablet(");
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);

        FullStatisticsCollectJob fullStatisticsCollectJob = new FullStatisticsCollectJob(
                db, olapTable,
                olapTable.getAllPartitionIds(),
                Lists.newArrayList("v1", "count"),
                StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE,
                Maps.newHashMap());
        sql = Deencapsulation.invoke(fullStatisticsCollectJob, "buildBatchCollectFullStatisticSQL",
                olapTable, olapTable.getPartition("tcount"), "count");
        assertContains(sql, "`stats`.`tcount` partition `tcount`");
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
    }

    @Test
    public void testAnalyzeBeforeUpdate() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        AnalyzeJob job = new AnalyzeJob(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
        FullStatisticsCollectJob fjb = (FullStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("t0_stats", fjb.getTable().getName());
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumns().toString());

        // collect 1st
        BasicStatsMeta execMeta = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), Maps.newHashMap());
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(execMeta);

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

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        BasicStatsMeta execMeta1 = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusSeconds(Config.statistic_auto_collect_large_table_interval).minusHours(1),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(execMeta1);

        new Expectations(execMeta1) {
            {
                execMeta1.getHealthy();
                result = 0.7;
            }
        };

        AnalyzeJob job = new AnalyzeJob(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
        FullStatisticsCollectJob fjb = (FullStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumns().toString());

        BasicStatsMeta execMeta2 = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusHours(1),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(execMeta2);

        new Expectations(execMeta2) {
            {
                execMeta2.getHealthy();
                times = 0;
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

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        BasicStatsMeta execMeta1 = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusSeconds(Config.statistic_auto_collect_large_table_interval).minusHours(1),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(execMeta1);

        new Expectations(execMeta1) {
            {
                execMeta1.getHealthy();
                result = 0.7;
            }
        };

        AnalyzeJob job = new AnalyzeJob(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN);

        List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
        Assert.assertEquals(1, jobs.size());
        Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
        FullStatisticsCollectJob fjb = (FullStatisticsCollectJob) jobs.get(0);
        Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumns().toString());

        Config.statistic_auto_collect_small_table_interval = 100;
        BasicStatsMeta execMeta2 = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusSeconds(50),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(execMeta2);

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

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        BasicStatsMeta execMeta = new BasicStatsMeta(db.getId(), t0StatsTableId, null,
                StatsConstants.AnalyzeType.FULL,
                now.minusSeconds(Config.statistic_auto_collect_large_table_interval).minusHours(1),
                Maps.newHashMap());
        GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(execMeta);

        AnalyzeJob job = new AnalyzeJob(db.getId(), t0StatsTableId, null,
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
            GlobalStateMgr.getCurrentAnalyzeMgr().addBasicStatsMeta(execMeta);

            new Expectations(execMeta) {
                {
                    execMeta.getHealthy();
                    result = 0.7;
                }
            };

            List<StatisticsCollectJob> jobs = StatisticsCollectJobFactory.buildStatisticsCollectJob(job);
            Assert.assertEquals(1, jobs.size());
            Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
            FullStatisticsCollectJob fjb = (FullStatisticsCollectJob) jobs.get(0);
            Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumns().toString());
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
            Assert.assertTrue(jobs.get(0) instanceof SampleStatisticsCollectJob);
            SampleStatisticsCollectJob fjb = (SampleStatisticsCollectJob) jobs.get(0);
            Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumns().toString());
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
            Assert.assertTrue(jobs.get(0) instanceof FullStatisticsCollectJob);
            FullStatisticsCollectJob fjb = (FullStatisticsCollectJob) jobs.get(0);
            Assert.assertEquals("[v1, v2, v3, v4, v5]", fjb.getColumns().toString());
        }
    }

}
