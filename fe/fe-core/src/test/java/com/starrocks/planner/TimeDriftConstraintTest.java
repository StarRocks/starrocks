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

package com.starrocks.planner;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.rewrite.TimeDriftConstraint;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TimeDriftConstraintTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert createStarRocksAssert() {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        FeConstants.runningUnitTest = true;
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        String hits = "" +
                " CREATE TABLE `hits` (\n" +
                "  `CounterID` int(11) NULL COMMENT \"\",\n" +
                "  `EventDate` date NOT NULL COMMENT \"\",\n" +
                "  `UserID` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `EventTime` datetime NOT NULL COMMENT \"\",\n" +
                "  `WatchID` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `LoadTime` datetime NOT NULL COMMENT \"\",\n" +
                "  `Title` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `GoodEvent` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `ClientIP` int(11) NOT NULL COMMENT \"\",\n" +
                "  `RegionID` int(11) NOT NULL COMMENT \"\",\n" +
                "  `URL` string NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`CounterID`, `EventDate`, `UserID`, `EventTime`, `WatchID`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`LoadTime`)\n" +
                "(PARTITION p20130701 VALUES [('2013-07-01 00:00:00'), ('2013-07-02 00:00:00')),\n" +
                "PARTITION p20130702 VALUES [('2013-07-02 00:00:00'), ('2013-07-03 00:00:00')),\n" +
                "PARTITION p20130703 VALUES [('2013-07-03 00:00:00'), ('2013-07-04 00:00:00')),\n" +
                "PARTITION p20130704 VALUES [('2013-07-04 00:00:00'), ('2013-07-05 00:00:00')),\n" +
                "PARTITION p20130705 VALUES [('2013-07-05 00:00:00'), ('2013-07-06 00:00:00')),\n" +
                "PARTITION p20130706 VALUES [('2013-07-06 00:00:00'), ('2013-07-07 00:00:00')),\n" +
                "PARTITION p20130707 VALUES [('2013-07-07 00:00:00'), ('2013-07-08 00:00:00')),\n" +
                "PARTITION p20130708 VALUES [('2013-07-08 00:00:00'), ('2013-07-09 00:00:00')),\n" +
                "PARTITION p20130709 VALUES [('2013-07-09 00:00:00'), ('2013-07-10 00:00:00')),\n" +
                "PARTITION p20130710 VALUES [('2013-07-10 00:00:00'), ('2013-07-11 00:00:00')),\n" +
                "PARTITION p20130711 VALUES [('2013-07-11 00:00:00'), ('2013-07-12 00:00:00')),\n" +
                "PARTITION p20130712 VALUES [('2013-07-12 00:00:00'), ('2013-07-13 00:00:00')),\n" +
                "PARTITION p20130713 VALUES [('2013-07-13 00:00:00'), ('2013-07-14 00:00:00')),\n" +
                "PARTITION p20130714 VALUES [('2013-07-14 00:00:00'), ('2013-07-15 00:00:00')),\n" +
                "PARTITION p20130715 VALUES [('2013-07-15 00:00:00'), ('2013-07-16 00:00:00')),\n" +
                "PARTITION p20130716 VALUES [('2013-07-16 00:00:00'), ('2013-07-17 00:00:00')),\n" +
                "PARTITION p20130717 VALUES [('2013-07-17 00:00:00'), ('2013-07-18 00:00:00')),\n" +
                "PARTITION p20130718 VALUES [('2013-07-18 00:00:00'), ('2013-07-19 00:00:00')),\n" +
                "PARTITION p20130719 VALUES [('2013-07-19 00:00:00'), ('2013-07-20 00:00:00')),\n" +
                "PARTITION p20130720 VALUES [('2013-07-20 00:00:00'), ('2013-07-21 00:00:00')),\n" +
                "PARTITION p20130721 VALUES [('2013-07-21 00:00:00'), ('2013-07-22 00:00:00')),\n" +
                "PARTITION p20130722 VALUES [('2013-07-22 00:00:00'), ('2013-07-23 00:00:00')),\n" +
                "PARTITION p20130723 VALUES [('2013-07-23 00:00:00'), ('2013-07-24 00:00:00')),\n" +
                "PARTITION p20130724 VALUES [('2013-07-24 00:00:00'), ('2013-07-25 00:00:00')),\n" +
                "PARTITION p20130725 VALUES [('2013-07-25 00:00:00'), ('2013-07-26 00:00:00')),\n" +
                "PARTITION p20130726 VALUES [('2013-07-26 00:00:00'), ('2013-07-27 00:00:00')),\n" +
                "PARTITION p20130727 VALUES [('2013-07-27 00:00:00'), ('2013-07-28 00:00:00')),\n" +
                "PARTITION p20130728 VALUES [('2013-07-28 00:00:00'), ('2013-07-29 00:00:00')),\n" +
                "PARTITION p20130729 VALUES [('2013-07-29 00:00:00'), ('2013-07-30 00:00:00')),\n" +
                "PARTITION p20130730 VALUES [('2013-07-30 00:00:00'), ('2013-07-31 00:00:00')),\n" +
                "PARTITION p20130731 VALUES [('2013-07-31 00:00:00'), ('2013-08-01 00:00:00')))\n" +
                "DISTRIBUTED BY HASH(`UserID`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"time_drift_constraint\" = \"EventTime between seconds_add(LoadTime, -300) and seconds_add(LoadTime, 600)\",\n" +
                "\"enable_persistent_index\" = \"true\"\n" +
                ");";
        String hitsDailyList = "CREATE TABLE `hits_daily_list` (\n" +
                "  `CounterID` int(11) NULL COMMENT \"\",\n" +
                "  `EventDate` date NOT NULL COMMENT \"\",\n" +
                "  `UserID` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `EventTime` datetime NOT NULL COMMENT \"\",\n" +
                "  `WatchID` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `LoadTime` datetime NOT NULL COMMENT \"\",\n" +
                "  `Title` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `GoodEvent` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `ClientIP` int(11) NOT NULL COMMENT \"\",\n" +
                "  `RegionID` int(11) NOT NULL COMMENT \"\",\n" +
                "  `URL` string NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`CounterID`, `EventDate`, `UserID`, `EventTime`, `WatchID`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY LIST(`LoadTime`)\n" +
                "(\n" +
                "PARTITION p01 VALUES IN (\"2013-07-01\"),\n" +
                "PARTITION p02 VALUES IN (\"2013-07-02\"),\n" +
                "PARTITION p03 VALUES IN (\"2013-07-03\"),\n" +
                "PARTITION p04 VALUES IN (\"2013-07-04\"),\n" +
                "PARTITION p05 VALUES IN (\"2013-07-05\"),\n" +
                "PARTITION p06 VALUES IN (\"2013-07-06\"),\n" +
                "PARTITION p07 VALUES IN (\"2013-07-07\"),\n" +
                "PARTITION p08 VALUES IN (\"2013-07-08\"),\n" +
                "PARTITION p09 VALUES IN (\"2013-07-09\"),\n" +
                "PARTITION p10 VALUES IN (\"2013-07-10\"),\n" +
                "PARTITION p11 VALUES IN (\"2013-07-11\"),\n" +
                "PARTITION p12 VALUES IN (\"2013-07-12\"),\n" +
                "PARTITION p13 VALUES IN (\"2013-07-13\"),\n" +
                "PARTITION p14 VALUES IN (\"2013-07-14\"),\n" +
                "PARTITION p15 VALUES IN (\"2013-07-15\"),\n" +
                "PARTITION p16 VALUES IN (\"2013-07-16\"),\n" +
                "PARTITION p17 VALUES IN (\"2013-07-17\"),\n" +
                "PARTITION p18 VALUES IN (\"2013-07-18\"),\n" +
                "PARTITION p19 VALUES IN (\"2013-07-19\"),\n" +
                "PARTITION p20 VALUES IN (\"2013-07-20\"),\n" +
                "PARTITION p21 VALUES IN (\"2013-07-21\"),\n" +
                "PARTITION p22 VALUES IN (\"2013-07-22\"),\n" +
                "PARTITION p23 VALUES IN (\"2013-07-23\"),\n" +
                "PARTITION p24 VALUES IN (\"2013-07-24\"),\n" +
                "PARTITION p25 VALUES IN (\"2013-07-25\"),\n" +
                "PARTITION p26 VALUES IN (\"2013-07-26\"),\n" +
                "PARTITION p27 VALUES IN (\"2013-07-27\"),\n" +
                "PARTITION p28 VALUES IN (\"2013-07-28\"),\n" +
                "PARTITION p29 VALUES IN (\"2013-07-29\"),\n" +
                "PARTITION p30 VALUES IN (\"2013-07-30\"),\n" +
                "PARTITION p31 VALUES IN (\"2013-07-31\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`UserID`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"time_drift_constraint\" = \"EventTime between seconds_add(LoadTime, -300) and seconds_add(LoadTime, 600)\",\n" +
                "\"enable_persistent_index\" = \"true\"\n" +
                ");";
        try {
            starRocksAssert.withDatabase("test_db").useDatabase("test_db");
            System.out.println(hits);
            System.out.println(hitsDailyList);
            starRocksAssert.withTable(hits);
            starRocksAssert.withTable(hitsDailyList);
        } catch (Throwable ignored) {
            Assert.fail();
        }
        return starRocksAssert;
    }

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        FeConstants.enablePruneEmptyOutputScan = false;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(createStarRocksAssert());
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        UtFrameUtils.mockTimelinessForAsyncMVTest(starRocksAssert.getCtx());
    }

    private void testHelper(String sql, String expectPredicate, String spec, String table) throws Exception {
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        if (spec != null && table != null) {
            if (table.contains("mv")) {
                String alterProperties = String.format(
                        "alter materialized view `%s` set ('time_drift_constraint'='%s')", table, spec);
                starRocksAssert.alterMvProperties(alterProperties);
            } else {
                String alterProperties = String.format(
                        "alter table `%s` set ('time_drift_constraint'='%s')", table, spec);
                starRocksAssert.alterTableProperties(alterProperties);
            }
            String createTableSql = starRocksAssert.showCreateTable(String.format("show create table %s", table));
            Assert.assertTrue(createTableSql, createTableSql.contains(spec));
        }
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        Assert.assertTrue(plan, plan.contains(expectPredicate));
    }

    @Test
    public void testParseTimeDriftConstraint() {
        String[] specs = new String[] {
                "EventTime between seconds_add(LoadTime, -86400) and seconds_add(LoadTime, 172800)",
                "EventTime between seconds_add(LoadTime, -86400) and seconds_sub(LoadTime, -172800)",
                "EventTime between seconds_sub(LoadTime, 86400) and seconds_add(LoadTime, 172800)",
                "EventTime between seconds_sub(LoadTime, 86400) and seconds_sub(LoadTime, -172800)",

                "EventTime between minutes_add(LoadTime, -1440) and minutes_add(LoadTime, 2880)",
                "EventTime between minutes_add(LoadTime, -1440) and minutes_sub(LoadTime, -2880)",
                "EventTime between minutes_sub(LoadTime, 1440) and minutes_add(LoadTime, 2880)",
                "EventTime between minutes_sub(LoadTime, 1440) and minutes_sub(LoadTime, -2880)",

                "EventTime between hours_add(LoadTime, -24) and hours_add(LoadTime, 48)",
                "EventTime between hours_add(LoadTime, -24) and hours_sub(LoadTime, -48)",
                "EventTime between hours_sub(LoadTime, 24) and hours_add(LoadTime, 48)",
                "EventTime between hours_sub(LoadTime, 24) and hours_sub(LoadTime, -48)",

                "EventTime between days_add(LoadTime, -1) and days_add(LoadTime, 2)",
                "EventTime between days_add(LoadTime, -1) and days_sub(LoadTime, -2)",
                "EventTime between days_sub(LoadTime, 1) and days_add(LoadTime, 2)",
                "EventTime between days_sub(LoadTime, 1) and days_sub(LoadTime, -2)",
        };
        List<TimeDriftConstraint> constraintList = Arrays.stream(specs)
                .map(TimeDriftConstraint::parseSpec).collect(Collectors.toList());
        for (TimeDriftConstraint constraint : constraintList) {
            Assert.assertEquals(constraint.getReferenceColumn(), "LoadTime");
            Assert.assertEquals(constraint.getTargetColumn(), "EventTime");
            Assert.assertEquals(constraint.getLowerGapSecs(), -86400L);
            Assert.assertEquals(constraint.getUpperGapSecs(), 172800L);
        }
    }

    @Test
    public void testInvalidTimeDriftConstraint() {
        String[] specs = new String[] {
                "",
                "abc",
                "EventTime = LoadTime",
                "EventTime <= days_add(LoadTime, 1) and EventTime >= days_add(LoadTime, -1)",
                "EventTime between days_sub(LoadTime, -2) and days_sub(LoadTime, -1)",
                "EventTime between days_sub(LoadTime, -2) and days_sub(EventDate, 4)",
                "EventTime between days_sub(EventDate, -2) and days_sub(EventDate, 4)",
                "LoadTime between days_sub(LoadTime, -2) and days_add(LoadTime, 4)",
                "EventTime between days_sub(EventDate, -2) and days_add(EventDate, 4)",
        };

        StarRocksAssert starRocksAssert = getStarRocksAssert();
        for (String spec : specs) {
            String alterProperties = String.format("alter table `hits` set ('time_drift_constraint'='%s')", spec);
            boolean failure;
            try {
                starRocksAssert.alterTableProperties(alterProperties);
                failure = false;
            } catch (Throwable ignored) {
                failure = true;
            }
            Assert.assertTrue(failure);
        }
    }

    @Test
    public void testRangePartitionTableAndBetweenPredicate() throws Exception {
        String q = "select * from hits where EventTime between '2013-07-03 12:00:00' and '2013-07-04 12:00:00'";
        String expect = "     PREDICATES: 4: EventTime >= '2013-07-03 12:00:00', " +
                "4: EventTime <= '2013-07-04 12:00:00', 6: LoadTime <= '2013-07-04 12:05:00', " +
                "6: LoadTime >= '2013-07-03 11:50:00'\n" +
                "     partitions=2/31";
        testHelper(q, expect, null, null);
        String spec1 = "EventTime between seconds_sub(LoadTime, 300) and seconds_add(LoadTime, 600)";
        testHelper(q, expect, spec1, "hits");
        String spec2 = "EventTime between seconds_sub(LoadTime, 300) and seconds_sub(LoadTime, -600)";
        testHelper(q, expect, spec2, "hits");

        String spec3 = "EventTime between minutes_add(LoadTime, -5) and minutes_add(LoadTime, 10)";
        testHelper(q, expect, spec3, "hits");

        String spec4 = "EventTime between minutes_sub(LoadTime, 5) and minutes_add(LoadTime, 10)";
        testHelper(q, expect, spec4, "hits");
        String spec5 = "EventTime between minutes_sub(LoadTime, 5) and minutes_sub(LoadTime, -10)";
        testHelper(q, expect, spec5, "hits");
    }

    @Test
    public void testRangePartitionTableAndBinaryPredicate() throws Exception {
        testHelper(
                "select * from hits where EventTime > '2013-07-15'",
                "     PREDICATES: 4: EventTime > '2013-07-15 00:00:00', " +
                        "6: LoadTime > '2013-07-14 23:50:00'\n" +
                        "     partitions=18/31",
                null, null);

        testHelper(
                "select * from hits where EventTime < '2013-07-15'",
                "     PREDICATES: 4: EventTime < '2013-07-15 00:00:00', " +
                        "6: LoadTime < '2013-07-15 00:05:00'\n" +
                        "     partitions=15/31",
                null, null);

        testHelper(
                "select * from hits where EventTime >= '2013-07-15'",
                "     PREDICATES: 4: EventTime >= '2013-07-15 00:00:00', " +
                        "6: LoadTime >= '2013-07-14 23:50:00'\n" +
                        "     partitions=18/31",
                null, null);

        testHelper(
                "select * from hits where EventTime <= '2013-07-15'",
                "     PREDICATES: 4: EventTime <= '2013-07-15 00:00:00', " +
                        "6: LoadTime <= '2013-07-15 00:05:00'\n" +
                        "     partitions=15/31",
                null, null);

        testHelper(
                "select * from hits where EventTime = '2013-07-15'",
                "     PREDICATES: 4: EventTime = '2013-07-15 00:00:00', 6: LoadTime <= '2013-07-15 00:05:00', 6: LoadTime >= '2013-07-14 23:50:00'\n" +
                        "     partitions=2/31",
                null, null);
    }

    @Test
    public void testRangePartitionTableAndInPredicate() throws Exception {
        testHelper(
                "select * from hits where EventTime in ('2013-07-15', '2013-07-18')",
                "     PREDICATES: 4: EventTime IN ('2013-07-15 00:00:00', '2013-07-18 00:00:00'), 6: LoadTime <= '2013-07-18 00:05:00', 6: LoadTime >= '2013-07-14 23:50:00'\n" +
                        "     partitions=5/31",
                null, null);
    }

    @Test
    public void testRangePartitionTableAndAndPredicate() throws Exception {
        testHelper(
                "select * from hits where EventTime < '2013-07-15' AND EventTime > '2013-07-03'",
                "     PREDICATES: 4: EventTime < '2013-07-15 00:00:00', 4: EventTime > '2013-07-03 00:00:00', 6: LoadTime < '2013-07-15 00:05:00', 6: LoadTime > '2013-07-02 23:50:00'\n" +
                        "     partitions=14/31",
                null, null);
    }

    @Test
    public void testRangePartitionTableAndOrPredicate() throws Exception {
        testHelper(
                "select * from hits where EventTime >'2013-07-28' OR EventTime < '2013-07-03'",
                "     PREDICATES: (4: EventTime > '2013-07-28 00:00:00') OR (4: EventTime < '2013-07-03 00:00:00')\n" +
                        "     partitions=31/31",
                null, null);
    }

    @Test
    public void testListPartitionTableAndBetweenPredicate() throws Exception {
        String q =
                "select * from hits_daily_list where EventTime between '2013-07-03 12:00:00' and '2013-07-04 12:00:00'";
        String expect = "     PREDICATES: 4: EventTime >= '2013-07-03 12:00:00', " +
                "4: EventTime <= '2013-07-04 12:00:00', 6: LoadTime <= '2013-07-04 12:05:00', " +
                "6: LoadTime >= '2013-07-03 11:50:00'\n" +
                "     partitions=1/31";
        testHelper(q, expect, null, null);
        String spec1 = "EventTime between seconds_sub(LoadTime, 300) and seconds_add(LoadTime, 600)";
        testHelper(q, expect, spec1, "hits_daily_list");
        String spec2 = "EventTime between seconds_sub(LoadTime, 300) and seconds_sub(LoadTime, -600)";
        testHelper(q, expect, spec2, "hits_daily_list");

        String spec3 = "EventTime between minutes_add(LoadTime, -5) and minutes_add(LoadTime, 10)";
        testHelper(q, expect, spec3, "hits_daily_list");

        String spec4 = "EventTime between minutes_sub(LoadTime, 5) and minutes_add(LoadTime, 10)";
        testHelper(q, expect, spec4, "hits_daily_list");
        String spec5 = "EventTime between minutes_sub(LoadTime, 5) and minutes_sub(LoadTime, -10)";
        testHelper(q, expect, spec5, "hits_daily_list");
    }

    @Test
    public void testListPartitionTableAndBinaryPredicate() throws Exception {
        testHelper(
                "select * from hits_daily_list where EventTime > '2013-07-15'",
                "     PREDICATES: 4: EventTime > '2013-07-15 00:00:00', " +
                        "6: LoadTime > '2013-07-14 23:50:00'\n" +
                        "     partitions=17/31",
                null, null);

        testHelper(
                "select * from hits_daily_list where EventTime < '2013-07-15'",
                "     PREDICATES: 4: EventTime < '2013-07-15 00:00:00', " +
                        "6: LoadTime < '2013-07-15 00:05:00'\n" +
                        "     partitions=15/31",
                null, null);

        testHelper(
                "select * from hits_daily_list where EventTime >= '2013-07-15'",
                "     PREDICATES: 4: EventTime >= '2013-07-15 00:00:00', " +
                        "6: LoadTime >= '2013-07-14 23:50:00'\n" +
                        "     partitions=17/31",
                null, null);

        testHelper(
                "select * from hits_daily_list where EventTime <= '2013-07-15'",
                "     PREDICATES: 4: EventTime <= '2013-07-15 00:00:00', " +
                        "6: LoadTime <= '2013-07-15 00:05:00'\n" +
                        "     partitions=15/31",
                null, null);

        testHelper(
                "select * from hits_daily_list where EventTime = '2013-07-15'",
                "     PREDICATES: 4: EventTime = '2013-07-15 00:00:00', 6: LoadTime <= '2013-07-15 00:05:00', 6: LoadTime >= '2013-07-14 23:50:00'\n" +
                        "     partitions=1/31",
                null, null);
    }

    @Test
    public void testListPartitionTableAndInPredicate() throws Exception {
        testHelper(
                "select * from hits_daily_list where EventTime in ('2013-07-15', '2013-07-18')",
                "     PREDICATES: 4: EventTime IN ('2013-07-15 00:00:00', '2013-07-18 00:00:00'), 6: LoadTime <= '2013-07-18 00:05:00', 6: LoadTime >= '2013-07-14 23:50:00'\n" +
                        "     partitions=4/31",
                null, null);
    }

    @Test
    public void testListPartitionTableAndAndPredicate() throws Exception {
        testHelper(
                "select * from hits_daily_list where EventTime < '2013-07-15' AND EventTime > '2013-07-03'",
                "     PREDICATES: 4: EventTime < '2013-07-15 00:00:00', 4: EventTime > '2013-07-03 00:00:00', 6: LoadTime < '2013-07-15 00:05:00', 6: LoadTime > '2013-07-02 23:50:00'\n" +
                        "     partitions=13/31",
                null, null);
    }

    @Test
    public void testListPartitionTableAndOrPredicate() throws Exception {
        testHelper(
                "select * from hits_daily_list where EventTime >'2013-07-28' OR EventTime < '2013-07-03'",
                "     PREDICATES: (4: EventTime > '2013-07-28 00:00:00') OR (4: EventTime < '2013-07-03 00:00:00')\n" +
                        "     partitions=31/31",
                null, null);
    }

    @Test
    public void testRangePartitionMVAndBetweenPredicate() throws Exception {
        String hitsMV = "CREATE MATERIALIZED VIEW hits_mv (\n" +
                "  CounterID\n" +
                "  , EventDate\n" +
                "  , UserID\n" +
                "  , EventTime\n" +
                "  , WatchID\n" +
                "  , LoadTime\n" +
                "  , Title\n" +
                "  , GoodEvent\n" +
                "  , ClientIP\n" +
                "  , RegionID\n" +
                "  , URL\n" +
                ")\n" +
                "PARTITION BY date_trunc(\"day\", LoadTime)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "ORDER BY (CounterID, EventDate, UserID)\n" +
                "REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)\n" +
                "PROPERTIES (\n" +
                "  \"replicated_storage\" = \"true\",\n" +
                "  \"partition_refresh_number\" = \"1\",\n" +
                "  \"session.enable_spill\" = \"true\",\n" +
                "  \"time_drift_constraint\" = \"EventTime between seconds_add(LoadTime, -300) and seconds_add(LoadTime, 600)\",\n" +
                "  \"storage_medium\" = \"HDD\",\n" +
                "  \"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT\n" +
                "  CounterID\n" +
                "  ,EventDate\n" +
                "  ,UserID\n" +
                "  ,EventTime\n" +
                "  ,WatchID\n" +
                "  ,LoadTime\n" +
                "  ,Title\n" +
                "  ,GoodEvent\n" +
                "  ,ClientIP\n" +
                "  ,RegionID\n" +
                "  ,URL\n" +
                "FROM\n" +
                "  `hits`";
        getStarRocksAssert().withMaterializedView(hitsMV);
        getStarRocksAssert().refreshMV("refresh materialized view hits_mv");

        String q = "select * from hits_mv where EventTime between '2013-07-03 12:00:00' and '2013-07-04 12:00:00'";
        String expect = "     PREDICATES: 4: EventTime >= '2013-07-03 12:00:00', " +
                "4: EventTime <= '2013-07-04 12:00:00', 6: LoadTime <= '2013-07-04 12:05:00', " +
                "6: LoadTime >= '2013-07-03 11:50:00'\n" +
                "     partitions=2/31";
        testHelper(q, expect, null, null);
        String spec1 = "EventTime between seconds_sub(LoadTime, 300) and seconds_add(LoadTime, 600)";
        testHelper(q, expect, spec1, "hits_mv");
        String spec2 = "EventTime between seconds_sub(LoadTime, 300) and seconds_sub(LoadTime, -600)";
        testHelper(q, expect, spec2, "hits_mv");

        String spec3 = "EventTime between minutes_add(LoadTime, -5) and minutes_add(LoadTime, 10)";
        testHelper(q, expect, spec3, "hits_mv");

        String spec4 = "EventTime between minutes_sub(LoadTime, 5) and minutes_add(LoadTime, 10)";
        testHelper(q, expect, spec4, "hits_mv");
        String spec5 = "EventTime between minutes_sub(LoadTime, 5) and minutes_sub(LoadTime, -10)";
        testHelper(q, expect, spec5, "hits_mv");
        getStarRocksAssert().dropMaterializedView("hits_mv");
    }

    @Test
    public void testListPartitionMVAndBetweenPredicate() throws Exception {
        String hitsMV = "CREATE MATERIALIZED VIEW hits_daily_list_mv (\n" +
                "  CounterID\n" +
                "  , EventDate\n" +
                "  , UserID\n" +
                "  , EventTime\n" +
                "  , WatchID\n" +
                "  , LoadTime\n" +
                "  , Title\n" +
                "  , GoodEvent\n" +
                "  , ClientIP\n" +
                "  , RegionID\n" +
                "  , URL\n" +
                ")\n" +
                "PARTITION BY LoadTime\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "ORDER BY (CounterID, EventDate, UserID)\n" +
                "REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)\n" +
                "PROPERTIES (\n" +
                "  \"replicated_storage\" = \"true\",\n" +
                "  \"partition_refresh_number\" = \"1\",\n" +
                "  \"session.enable_spill\" = \"true\",\n" +
                "  \"time_drift_constraint\" = \"EventTime between seconds_add(LoadTime, -300) and seconds_add(LoadTime, 600)\",\n" +
                "  \"storage_medium\" = \"HDD\",\n" +
                "  \"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT\n" +
                "  CounterID\n" +
                "  ,EventDate\n" +
                "  ,UserID\n" +
                "  ,EventTime\n" +
                "  ,WatchID\n" +
                "  ,LoadTime\n" +
                "  ,Title\n" +
                "  ,GoodEvent\n" +
                "  ,ClientIP\n" +
                "  ,RegionID\n" +
                "  ,URL\n" +
                "FROM\n" +
                "  `hits_daily_list`";
        getStarRocksAssert().withMaterializedView(hitsMV);
        getStarRocksAssert().refreshMV("refresh materialized view hits_daily_list_mv");

        String q =
                "select * from hits_daily_list_mv where EventTime between '2013-07-03 12:00:00' and '2013-07-04 12:00:00'";
        String expect = "     PREDICATES: 4: EventTime >= '2013-07-03 12:00:00', " +
                "4: EventTime <= '2013-07-04 12:00:00', 6: LoadTime <= '2013-07-04 12:05:00', " +
                "6: LoadTime >= '2013-07-03 11:50:00'\n" +
                "     partitions=1/31";
        testHelper(q, expect, null, null);
        String spec1 = "EventTime between seconds_sub(LoadTime, 300) and seconds_add(LoadTime, 600)";
        testHelper(q, expect, spec1, "hits_daily_list_mv");
        String spec2 = "EventTime between seconds_sub(LoadTime, 300) and seconds_sub(LoadTime, -600)";
        testHelper(q, expect, spec2, "hits_daily_list_mv");

        String spec3 = "EventTime between minutes_add(LoadTime, -5) and minutes_add(LoadTime, 10)";
        testHelper(q, expect, spec3, "hits_daily_list_mv");

        String spec4 = "EventTime between minutes_sub(LoadTime, 5) and minutes_add(LoadTime, 10)";
        testHelper(q, expect, spec4, "hits_daily_list_mv");
        String spec5 = "EventTime between minutes_sub(LoadTime, 5) and minutes_sub(LoadTime, -10)";
        testHelper(q, expect, spec5, "hits_daily_list_mv");
        getStarRocksAssert().dropMaterializedView("hits_daily_list_mv");
    }
}
