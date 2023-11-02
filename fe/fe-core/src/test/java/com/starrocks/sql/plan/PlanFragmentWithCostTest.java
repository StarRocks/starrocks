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

package com.starrocks.sql.plan;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Reference;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

public class PlanFragmentWithCostTest extends PlanTestBase {
    private static final int NUM_TABLE2_ROWS = 10000;
    private static final int NUM_TABLE0_ROWS = 10000;

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        Config.alter_scheduler_interval_millisecond = 1;

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table2 = (OlapTable) globalStateMgr.getDb("test").getTable("test_all_type");
        setTableStatistics(table2, NUM_TABLE2_ROWS);

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, NUM_TABLE0_ROWS);

        OlapTable colocateT0 = (OlapTable) globalStateMgr.getDb("test").getTable("colocate_t0");
        setTableStatistics(colocateT0, NUM_TABLE0_ROWS);

        OlapTable lineitem = (OlapTable) globalStateMgr.getDb("test").getTable("lineitem");
        setTableStatistics(lineitem, NUM_TABLE0_ROWS * NUM_TABLE0_ROWS);

        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE test_mv\n" +
                "    (\n" +
                "        event_day int,\n" +
                "        siteid INT,\n" +
                "        citycode SMALLINT,\n" +
                "        username VARCHAR(32),\n" +
                "        pv BIGINT SUM DEFAULT '0'\n" +
                "    )\n" +
                "    AGGREGATE KEY(event_day, siteid, citycode, username)\n" +
                "    DISTRIBUTED BY HASH(siteid) BUCKETS 10\n" +
                "    rollup (\n" +
                "    r1(event_day,siteid),\n" +
                "    r2(event_day,citycode),\n" +
                "    r3(event_day),\n" +
                "    r4(event_day,pv),\n" +
                "    r5(event_day,siteid,pv)\n" +
                "    )\n" +
                "    PROPERTIES(\"replication_num\" = \"1\");");

        starRocksAssert.withTable(" CREATE TABLE `duplicate_table_with_null` ( `k1`  date, `k2`  datetime, " +
                "`k3`  char(20), `k4`  varchar(20), `k5`  boolean, `k6`  tinyint, " +
                "`k7`  smallint, `k8`  int, `k9`  bigint, `k10` largeint, " +
                "`k11` float, `k12` double, `k13` decimal(27,9) ) " +
                "ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) " +
                "COMMENT \"OLAP\" DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) " +
                "BUCKETS 3 PROPERTIES ( \"replication_num\" = \"1\");");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW bitmap_mv\n" +
                "                             AS\n" +
                "                             SELECT k1,k2,k3,k4, bitmap_union(to_bitmap(k7)), " +
                "bitmap_union(to_bitmap(k8)) FROM duplicate_table_with_null group by k1,k2,k3,k4");

        starRocksAssert.withTable("CREATE TABLE `test_dict` (\n" +
                "  `name` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `dt` date NOT NULL  ,\n" +
                "  `id` bigint(20) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`name`,`dt`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "PARTITION p20221202 VALUES [('2022-12-02'), ('2022-12-03')),\n" +
                "PARTITION p20221203 VALUES [('2022-12-03'), ('2022-12-04')),\n" +
                "PARTITION p20221204 VALUES [('2022-12-04'), ('2022-12-05')),\n" +
                "PARTITION p20221205 VALUES [('2022-12-05'), ('2022-12-06')),\n" +
                "PARTITION p20221206 VALUES [('2022-12-06'), ('2022-12-07')),\n" +
                "PARTITION p20221207 VALUES [('2022-12-07'), ('2022-12-08')),\n" +
                "PARTITION p20221208 VALUES [('2022-12-08'), ('2022-12-09')),\n" +
                "PARTITION p20221209 VALUES [('2022-12-09'), ('2022-12-10')),\n" +
                "PARTITION p20221210 VALUES [('2022-12-10'), ('2022-12-11')),\n" +
                "PARTITION p20221211 VALUES [('2022-12-11'), ('2022-12-12')),\n" +
                "PARTITION p20221212 VALUES [('2022-12-12'), ('2022-12-13')),\n" +
                "PARTITION p20221213 VALUES [('2022-12-13'), ('2022-12-14')))\n" +
                "DISTRIBUTED BY HASH(`name`) BUCKETS 9 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");
        FeConstants.runningUnitTest = true;
    }

    @Before
    public void before() {
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    private static final String V1 = "v1";
    private static final String V2 = "v2";
    private static final String V3 = "v3";

    @Test
    public void testCrossJoinBroadCast() throws Exception {
        // check cross join generate plan without exception
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table2 = (OlapTable) globalStateMgr.getDb("test").getTable("test_all_type");
        setTableStatistics(table2, 20000000);
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 20000000);

        String sql = "select t1a,v1 from test_all_type, t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:NESTLOOP JOIN");

        setTableStatistics(table2, 10000);
        setTableStatistics(t0, 10000);
    }

    @Test
    public void testAggWithLowCardinality(@Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, Lists.newArrayList("v2"));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 100));
            }
        };

        String sql = "select sum(v2) from t0 group by v2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:AGGREGATE (merge finalize)\n"
                + "  |  output: sum(4: sum)\n"
                + "  |  group by: 2: v2");
        assertContains(plan, "  1:AGGREGATE (update serialize)\n"
                + "  |  STREAMING\n"
                + "  |  output: sum(2: v2)\n"
                + "  |  group by: 2: v2");
    }

    // still choose two stage agg even it's a high cardinality scene to cover bad case when statistics is uncorrect
    @Test
    public void testAggWithHighCardinality(@Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, Lists.newArrayList("v2"));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };

        String sql = "select sum(v2) from t0 group by v2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 2: v2");
    }

    @Test
    public void testSortWithLowCardinality(@Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 100),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 100));
            }
        };
        String sql = "select v1, sum(v2) from t0 group by v1 order by v1";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("TOP-N"));
        assertContains(plan, "  2:SORT\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0");
    }

    @Test
    public void testSortWithHighCardinality(@Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "select v1, sum(v2) from t0 group by v1 order by v1";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("TOP-N"));
        assertContains(plan, "  2:SORT\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0");
    }

    @Test
    public void testTopNWithLowCardinality(@Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 100),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 100));
            }
        };
        String sql = "select v1, sum(v2) from t0 group by v1 order by v1 limit 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:TOP-N\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0\n"
                + "  |  limit: 1");
    }

    @Test
    public void testTopNWithHighCardinality(@Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "select v1, sum(v2) from t0 group by v1 order by v1 limit 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "TOP-N");
        assertContains(plan, "  2:TOP-N\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0\n"
                + "  |  limit: 1");
    }

    @Test
    public void testDistinctWithoutGroupByWithLowCardinality(@Mocked MockTpchStatisticStorage mockedStatisticStorage)
            throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 100),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 100));
            }
        };
        String sql = "select count(distinct v2), sum(v1) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:AGGREGATE (merge serialize)\n"
                + "  |  output: sum(5: sum)\n"
                + "  |  group by: 2: v2");
        assertContains(plan, "  6:AGGREGATE (merge finalize)\n"
                + "  |  output: count(4: count), sum(5: sum)\n"
                + "  |  group by: \n");
        assertContains(plan, "  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 05\n"
                + "    UNPARTITIONED");
        Assert.assertFalse(plan.contains("PLAN FRAGMENT 3"));

    }

    @Test
    public void testDistinctWithoutGroupByWithHighCardinalityForceOneStage(
            @Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V1, V2));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "select count(distinct v2), sum(v1) from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n"
                + "  |  output: multi_distinct_count(2: v2), sum(1: v1)\n"
                + "  |  group by:");

    }

    @Test
    public void testDistinctWithGroupByWithLowCardinalityForceThreeStage(
            @Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V2, V3));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 50),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 50));
            }
        };
        String sql = "select count(distinct v2) from t0 group by v3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 2: v2, 3: v3");
        assertContains(plan, "  PARTITION: HASH_PARTITIONED: 3: v3\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    UNPARTITIONED");
    }

    @Test
    public void testDistinctWithGroupByWithHighCardinality(@Mocked MockTpchStatisticStorage mockedStatisticStorage)
            throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V2, V3));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "select count(distinct v2) from t0 group by v3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 4:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: v2)\n" +
                "  |  group by: 3: v3");
    }

    @Test
    public void testPredicateRewrittenByProjectWithLowCardinality(
            @Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V2, V3));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 10),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 10));
            }
        };
        String sql = "SELECT -v3 from t0 group by v3, v2 having -v3 < 63;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:Project\n" +
                "  |  <slot 4> : -1 * 3: v3");
        assertContains(plan, "PREDICATES: -1 * 3: v3 < 63");
    }

    @Test
    public void testPredicateRewrittenByProjectWithHighCardinality(
            @Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V2, V3));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };
        String sql = "SELECT -v3 from t0 group by v3, v2 having -v3 < 63;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:Project\n"
                + "  |  <slot 4> : -1 * 3: v3");
    }

    @Test
    public void testShuffleInnerJoin() throws Exception {
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table1 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(table1, 10000);
        OlapTable table2 = (OlapTable) globalStateMgr.getDb("test").getTable("test_all_type");
        setTableStatistics(table2, 5000);
        String sql = "SELECT v2,t1d from t0 join test_all_type on t0.v2 = test_all_type.t1d ;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:HASH JOIN\n"
                + "  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 2: v2 = 7: t1d\n"
                + "  |  \n"
                + "  |----3:EXCHANGE\n"
                + "  |    \n"
                + "  1:EXCHANGE");
        assertContains(plan, "    EXCHANGE ID: 03\n"
                + "    HASH_PARTITIONED: 7: t1d\n"
                + "\n"
                + "  2:OlapScanNode");
        assertContains(plan, "  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 01\n"
                + "    HASH_PARTITIONED: 2: v2\n"
                + "\n"
                + "  0:OlapScanNode");
        UtFrameUtils.dropMockBackend(10002);
        UtFrameUtils.dropMockBackend(10003);
    }

    @Test
    public void testBroadcastInnerJoin() throws Exception {
        String sql = "SELECT v1, t1d from t0 join test_all_type on t0.v2 = test_all_type.t1d ;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:HASH JOIN\n"
                + "  |  join op: INNER JOIN (BROADCAST)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 2: v2 = 7: t1d\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n");
    }

    @Test
    public void testBroadcastInnerJoinWithCommutativity() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(table, 1000);
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d ;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:HASH JOIN\n"
                + "  |  join op: INNER JOIN (BROADCAST)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 7: t1d = 1: v1\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: test_all_type\n");
        setTableStatistics(table, 10000);
    }

    @Test
    public void testColocateJoin() throws Exception {
        String sql = "SELECT * from t0 join t0 as b on t0.v1 = b.v1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN (COLOCATE)");
    }

    @Test
    public void testColocateAgg() throws Exception {
        String sql = "SELECT count(*) from t0 group by t0.v1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)");
    }

    @Test
    public void testDistinctExpr() throws Exception {
        String sql = "SELECT DISTINCT - - v1 DIV - 98 FROM t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 4>\n" +
                "  |  \n" +
                "  1:Project"));
        assertContains(plan, "EXCHANGE");
    }

    @Test
    public void testRollUp() throws Exception {
        String sql = "select event_day from test_mv group by event_day;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: r3");

        sql = "select count(*) from test_mv group by event_day;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: test_mv");

        sql = "select count(*), event_day from test_mv group by event_day;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: test_mv");

        sql = "select event_day from test_mv where citycode = 1 group by event_day;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: r2");

        sql = "select siteid from test_mv where event_day  = 1 group by siteid;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: r1");

        sql = "select siteid from test_mv group by siteid, event_day;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: r1");

        sql = "select siteid from test_mv group by siteid, username;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: test_mv");

        sql = "select siteid,sum(pv) from test_mv group by siteid, username;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: test_mv");

        sql = "select sum(pv) from test_mv group by event_day;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: r4");

        sql = "select max(pv) from test_mv group by event_day;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: test_mv");

        sql = "select max(event_day) from test_mv group by event_day;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: r3");

        sql = "select max(event_day), sum(pv) from test_mv group by event_day;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: r4");

        sql = "select max(event_day), max(pv) from test_mv group by event_day;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "rollup: test_mv");
    }

    @Test
    public void testMV() throws Exception {
        String sql = "select count(distinct k7), count(distinct k8) from duplicate_table_with_null;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:16: count | 17: count");
        assertContains(plan, "14: mv_bitmap_union_k7");
        assertContains(plan, "15: mv_bitmap_union_k8");
        assertContains(plan, "rollup: bitmap_mv");
    }

    @Test
    @Ignore("disable replicate join temporarily")
    public void testReplicatedJoin() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);
        String sql = "select s_name, s_address from supplier, nation where s_suppkey in " +
                "( select ps_suppkey from partsupp where ps_partkey " +
                "in ( select p_partkey from part where p_name like 'forest%' ) and ps_availqty > " +
                "( select 0.5 * sum(l_quantity) from lineitem " +
                "where l_partkey = ps_partkey and l_suppkey = ps_suppkey and " +
                "l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year ) ) " +
                "and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  11:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (REPLICATED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: PS_PARTKEY = 20: P_PARTKEY");
        assertContains(plan, "  14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 32: L_PARTKEY = 14: PS_PARTKEY\n" +
                "  |  equal join conjunct: 33: L_SUPPKEY = 15: PS_SUPPKEY\n" +
                "  |  other join predicates: CAST(16: PS_AVAILQTY AS DOUBLE) > 0.5 * 48: sum");
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    @Test
    public void testReapNodeStatistics() throws Exception {
        String sql = "select v1, v2, grouping_id(v1,v2), SUM(v3) from t0 group by cube(v1, v2)";
        String plan = getCostExplain(sql);
        // check scan node
        assertContains(plan, "cardinality: 10000");
        // check repeat node
        assertContains(plan, "cardinality: 40000");
        assertContains(plan, " * GROUPING_ID-->[0.0, 3.0, 0.0, 8.0, 4.0] ESTIMATE\n" +
                "  |  * GROUPING-->[0.0, 3.0, 0.0, 8.0, 4.0] ESTIMATE");

        sql = "select v1, v2, grouping_id(v1,v2), SUM(v3) from t0 group by rollup(v1, v2)";
        plan = getCostExplain(sql);
        // check scan node
        assertContains(plan, "cardinality: 10000");
        // check repeat node
        assertContains(plan, "cardinality: 30000");
        assertContains(plan, "* GROUPING_ID-->[0.0, 3.0, 0.0, 8.0, 3.0] ESTIMATE\n" +
                "  |  * GROUPING-->[0.0, 3.0, 0.0, 8.0, 3.0] ESTIMATE");
    }

    @Test
    public void testRepeatNodeExchange() throws Exception {
        String sql = "select v1, v2, SUM(v3) from t0 group by rollup(v1, v2)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 1: v1, 2: v2, 5: GROUPING_ID\n" +
                "\n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1, 2: v2, 5: GROUPING_ID\n" +
                "  |  \n" +
                "  1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select v1, SUM(v3) from t0 group by rollup(v1)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 1: v1, 5: GROUPING_ID\n" +
                "\n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1, 5: GROUPING_ID\n" +
                "  |  \n" +
                "  1:REPEAT_NODE\n" +
                "  |  repeat: repeat 1 lines [[], [1]]\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON");

        sql = "select SUM(v3) from t0 group by grouping sets(())";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: GROUPING_ID\n" +
                "\n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 5: GROUPING_ID\n" +
                "  |  \n" +
                "  1:REPEAT_NODE");
    }

    @Test
    public void testDisableOnePhaseWithTableRowCountMayNotAccurate() throws Exception {
        // check can not generate 1 phase aggregation if fe do not get real table row count from be.
        String sql = "select count(1) from orders group by O_CUSTKEY, O_ORDERDATE";
        String plan = getFragmentPlan(sql);
        // check has 3 phase aggregation
        assertContains(plan, "3:AGGREGATE (merge finalize)");
        sql = "select count(distinct O_ORDERKEY) from orders group by O_CUSTKEY, O_ORDERDATE";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: O_CUSTKEY, 5: O_ORDERDATE, 1: O_ORDERKEY\n");
    }

    @Test
    public void testDisableOnePhaseWithUnknownColumnStatistics() throws Exception {
        // check can not generate 1 phase aggregation if column statistics is unknown
        String sql = "select count(v1) from t0 group by v2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:AGGREGATE (merge finalize)");
        sql = "select count(distinct v1) from t0 group by v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: v2, 1: v1");
    }

    @Test
    public void testSemiJoinPushDownPredicate() throws Exception {
        String sql =
                "select * from t0 left semi join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 and t0.v1 = 1 and t1.v5 = 2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 1, 2: v2 = 2");
        assertContains(plan, "TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: v5 = 2, 4: v4 = 1");
    }

    @Test
    public void testOuterJoinPushDownPredicate() throws Exception {
        String sql =
                "select * from t0 left outer join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 and t0.v1 = 1 and t1.v5 = 2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1");
        assertContains(plan, "TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: v5 = 2, 4: v4 = 1");
    }

    @Test
    public void testThriftWaitingNodeIds() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 10000000);
        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 10000);

        String sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 and t0.v1 = 1 and t1.v5 = 2";
        String plan = getThriftPlan(sql);
        assertContains(plan, "TPlanNode(node_id:3, node_type:HASH_JOIN_NODE");
        assertContains(plan, "local_rf_waiting_set:[3]");
    }

    @Test
    public void testIntersectReorder() throws Exception {
        // check cross join generate plan without exception
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 1000);
        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 100);
        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("t2");
        setTableStatistics(t2, 1);

        String sql = "select v1 from t0 intersect select v7 from t2 intersect select v4 from t1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  0:INTERSECT\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n");
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    HASH_PARTITIONED: 4: v7\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2");
        setTableStatistics(t0, 10000);
    }

    @Test
    public void testNotPushDownRuntimeFilterToSortNode() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 10);

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 1000000000L);

        String sql = "select t0.v1 from (select v4 from t1 order by v4 limit 1000000000) as t1x " +
                "join [broadcast] t0 where t0.v1 = t1x.v4";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "  1:TOP-N\n" +
                "  |  order by: [1, BIGINT, true] ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 1000000000\n" +
                "  |  cardinality: 1000000000\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        assertContains(plan, "  2:MERGING-EXCHANGE\n" +
                "     distribution type: GATHER\n" +
                "     limit: 1000000000\n" +
                "     cardinality: 1000000000\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 1, probe_expr = (1: v4)");

        setTableStatistics(t0, 10000);
    }

    @Test
    public void testPushMultiColumnRuntimeFiltersCrossDataExchange() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        connectContext.getSessionVariable().setEnableMultiColumnsOnGlobbalRuntimeFilter(true);
        connectContext.getSessionVariable().setGlobalRuntimeFilterBuildMaxSize(0);
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 10000000L);

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 1000000000L);

        String sql = "select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6";
        String plan = getVerboseExplain(sql);
        System.out.println(plan);

        assertContains(plan, "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (5: v5), remote = true\n" +
                "  |  - filter_id = 1, build_expr = (6: v6), remote = true");

        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (2: v2), partition_exprs = (2: v2,3: v3)\n" +
                "     - filter_id = 1, probe_expr = (3: v3), partition_exprs = (2: v2,3: v3)");
    }

    @Test
    public void testPushMultiColumnRuntimeFiltersCrossDataExchangeOnMultiJoins() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        connectContext.getSessionVariable().setEnableMultiColumnsOnGlobbalRuntimeFilter(true);
        connectContext.getSessionVariable().setGlobalRuntimeFilterBuildMaxSize(0);
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 10000000L);

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 10000000L);

        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("t2");
        setTableStatistics(t2, 10000000L);

        String sql = "select * from (select t1.v5 as v5, t0.v3 as v3 from t0 join[shuffle] t1 on t0.v2 = " +
                "t1.v5 and t0.v3 = t1.v6) tt join[shuffle] t2 on tt.v5 = t2.v8 and tt.v3 = t2.v7";
        String plan = getVerboseExplain(sql);
        System.out.println(plan);

        assertContains(plan, "  |  - filter_id = 0, build_expr = (5: v5), remote = true\n" +
                "  |  - filter_id = 1, build_expr = (6: v6), remote = true");

        assertContains(plan, "  |  - filter_id = 2, build_expr = (8: v8), remote = true\n" +
                "  |  - filter_id = 3, build_expr = (7: v7), remote = true");

        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 2, probe_expr = (5: v5), partition_exprs = (5: v5,6: v6)\n" +
                "     - filter_id = 3, probe_expr = (6: v6), partition_exprs = (5: v5,6: v6)");

        assertContains(plan, "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (2: v2), partition_exprs = (2: v2,3: v3)\n" +
                "     - filter_id = 1, probe_expr = (3: v3), partition_exprs = (2: v2,3: v3)\n" +
                "     - filter_id = 2, probe_expr = (2: v2), partition_exprs = (2: v2,3: v3)\n" +
                "     - filter_id = 3, probe_expr = (3: v3), partition_exprs = (2: v2,3: v3)");
    }

    @Test
    public void testMergeTwoAggArgTypes() throws Exception {
        String sql = "select sum(t.int_sum) from (select sum(t1c) as int_sum from test_all_type)t";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  aggregate: sum[([3: t1c, INT, true]); args: INT; result: BIGINT;");
    }

    @Test
    public void testPushDownRuntimeFilterAcrossSetOperationNode() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 1000);
        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 100);
        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("t2");
        List<OlapTable> tables = new ArrayList<>(Arrays.asList(t0, t1, t2));
        List<String> tabletIdsStrList = new ArrayList<>();
        tables.forEach(olapTable -> tabletIdsStrList.add(Joiner.on(",")
                .join(olapTable.getPartition(olapTable.getAllPartitionIds().get(0))
                        .getBaseIndex().getTablets().stream().map(t -> t.getId()).collect(Collectors.toList()))));

        ArrayList<String> plans = new ArrayList<>();
        /// ===== union =====
        setTableStatistics(t2, 200000);
        setTableStatistics(t0, 400000);
        setTableStatistics(t1, 400000);

        String sql = "select * from (select v1+1 as v1 ,v2,v3 from t0 union " +
                "select v4 +2  as v1, v5 as v2, v6 as v3 from t1) as tx join [shuffle] t2 on tx.v1 = t2.v7;";
        String plan = getVerboseExplain(sql);
        plans.add(plan);

        // ==== except =====
        setTableStatistics(t2, 200000);
        setTableStatistics(t0, 800000);
        setTableStatistics(t1, 400000);
        sql = "select * from (select v1+1 as v1 ,v2,v3 from t0 except " +
                "select v4 +2  as v1, v5 as v2, v6 as v3 from t1) as tx join [shuffle] t2 on tx.v1 = t2.v7;";
        plan = getVerboseExplain(sql);
        plans.add(plan);

        // ===== intersect =====
        setTableStatistics(t2, 200000);
        setTableStatistics(t0, 400000);
        setTableStatistics(t1, 400000);

        sql = "select * from (select v1+1 as v1 ,v2,v3 from t0 intersect " +
                "select v4 +2  as v1, v5 as v2, v6 as v3 from t1) as tx join [shuffle] t2 on tx.v1 = t2.v7;";
        plan = getVerboseExplain(sql);
        plans.add(plan);

        setTableStatistics(t0, 10000);

        // === check union plan ====
        {
            String unionPlan = plans.get(0);
            assertContains(unionPlan, "  0:UNION\n" +
                    "  |  output exprs:\n" +
                    "  |      [9, BIGINT, true] | [10, BIGINT, true] | [11, BIGINT, true]\n" +
                    "  |  child exprs:\n" +
                    "  |      [4: expr, BIGINT, true] | [2: v2, BIGINT, true] | [3: v3, BIGINT, true]\n" +
                    "  |      [8: expr, BIGINT, true] | [6: v5, BIGINT, true] | [7: v6, BIGINT, true]\n" +
                    "  |  pass-through-operands: all\n");
            assertContains(unionPlan, "  4:OlapScanNode\n" +
                    "     table: t1, rollup: t1\n" +
                    "     preAggregation: on\n" +
                    "     Predicates: 5: v4 + 2 IS NOT NULL\n" +
                    "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                    "     tabletList=" + tabletIdsStrList.get(1) + "\n" +
                    "     actualRows=0, avgRowSize=4.0\n" +
                    "     cardinality: 360000\n" +
                    "     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (5: v4 + 2)");
            assertContains(unionPlan, "  1:OlapScanNode\n" +
                    "     table: t0, rollup: t0\n" +
                    "     preAggregation: on\n" +
                    "     Predicates: 1: v1 + 1 IS NOT NULL\n" +
                    "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                    "     tabletList=" + tabletIdsStrList.get(0) + "\n" +
                    "     actualRows=0, avgRowSize=4.0\n" +
                    "     cardinality: 360000\n" +
                    "     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (1: v1 + 1)");
        }
        // === check except plan ===
        {
            String exceptPlan = plans.get(1);
            assertContains(exceptPlan, "  0:EXCEPT\n" +
                    "  |  output exprs:\n" +
                    "  |      [9, BIGINT, true] | [10, BIGINT, true] | [11, BIGINT, true]\n" +
                    "  |  child exprs:\n" +
                    "  |      [4: expr, BIGINT, true] | [2: v2, BIGINT, true] | [3: v3, BIGINT, true]\n" +
                    "  |      [8: expr, BIGINT, true] | [6: v5, BIGINT, true] | [7: v6, BIGINT, true]\n");
            Assert.assertTrue(exceptPlan.contains("     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (5: v4 + 2)"));
            Assert.assertTrue(exceptPlan.contains("     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (1: v1 + 1)"));
        }
        // === check intersect plan ====
        {
            String intersectPlan = plans.get(2);
            assertContains(intersectPlan, "  0:INTERSECT\n" +
                    "  |  output exprs:\n" +
                    "  |      [9, BIGINT, true] | [10, BIGINT, true] | [11, BIGINT, true]\n" +
                    "  |  child exprs:\n" +
                    "  |      [4: expr, BIGINT, true] | [2: v2, BIGINT, true] | [3: v3, BIGINT, true]\n" +
                    "  |      [8: expr, BIGINT, true] | [6: v5, BIGINT, true] | [7: v6, BIGINT, true]\n");
            Assert.assertTrue(intersectPlan.contains("     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (5: v4 + 2)"));
            Assert.assertTrue(intersectPlan.contains("     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (1: v1 + 1)"));
        }
    }

    @Test
    public void testLimitTabletPrune(@Mocked Replica replica) throws Exception {
        new Expectations() {
            {
                replica.getRowCount();
                result = 10000;
                replica.isBad();
                result = false;
                replica.getLastFailedVersion();
                result = -1;
                replica.getState();
                result = Replica.ReplicaState.NORMAL;
                replica.getSchemaHash();
                result = -1;
                replica.getBackendId();
                result = 10001;
                replica.checkVersionCatchUp(anyLong, anyBoolean);
                result = true;
            }
        };
        String sql = "select * from lineitem limit 10";
        ExecPlan execPlan = getExecPlan(sql);
        Assert.assertFalse(execPlan.getScanNodes().isEmpty());
        Assert.assertEquals(1, ((OlapScanNode) execPlan.getScanNodes().get(0)).getScanTabletIds().size());

        sql = "select * from test_mv limit 10";
        execPlan = getExecPlan(sql);
        Assert.assertFalse(execPlan.getScanNodes().isEmpty());
        Assert.assertTrue(((OlapScanNode) execPlan.getScanNodes().get(0)).getScanTabletIds().size() > 1);
    }

    @Test
    public void testNullArithmeticExpression() throws Exception {
        // check constant operator with null
        String sql = "SELECT supplier.S_NATIONKEY FROM supplier WHERE (supplier.S_NATIONKEY) " +
                "BETWEEN (((NULL)/(CAST(\"\" AS INT ) ))) AND (supplier.S_NATIONKEY)";
        String plan = getFragmentPlan(sql);

        sql = "SELECT supplier.S_NATIONKEY FROM supplier WHERE (supplier.S_NATIONKEY) " +
                "BETWEEN (((NULL) + (CAST(\"\" AS INT ) ))) AND (supplier.S_NATIONKEY)";
        plan = getFragmentPlan(sql);

        sql = "SELECT supplier.S_NATIONKEY FROM supplier WHERE (supplier.S_NATIONKEY) " +
                "BETWEEN (((NULL) - (CAST(\"\" AS INT ) ))) AND (supplier.S_NATIONKEY)";
        plan = getFragmentPlan(sql);

        sql = "SELECT supplier.S_NATIONKEY FROM supplier WHERE (supplier.S_NATIONKEY) " +
                "BETWEEN (((NULL) * (CAST(\"\" AS INT ) ))) AND (supplier.S_NATIONKEY)";
        plan = getFragmentPlan(sql);
        // check variable operator with null
        sql = "SELECT supplier.S_NATIONKEY FROM supplier WHERE (null / supplier.S_NATIONKEY) " +
                "BETWEEN (((NULL) * (CAST(\"\" AS INT ) ))) AND (supplier.S_NATIONKEY)";
        plan = getFragmentPlan(sql);

        sql = "SELECT supplier.S_NATIONKEY FROM supplier WHERE (supplier.S_NATIONKEY / null) " +
                "BETWEEN (((NULL) * (CAST(\"\" AS INT ) ))) AND (supplier.S_NATIONKEY)";
        plan = getFragmentPlan(sql);

        sql = "SELECT supplier.S_NATIONKEY FROM supplier WHERE (null / S_NAME) " +
                "BETWEEN (((NULL) * (CAST(\"\" AS INT ) ))) AND (supplier.S_NATIONKEY)";
        plan = getFragmentPlan(sql);
    }

    @Test
    public void testNotPushDownRuntimeFilterAcrossCTE() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("t2");
        List<OlapTable> tables = new ArrayList<>(Arrays.asList(t1, t2));
        List<String> tabletIdsStrList = new ArrayList<>();
        tables.forEach(olapTable -> tabletIdsStrList.add(Joiner.on(",")
                .join(olapTable.getPartition(olapTable.getAllPartitionIds().get(0))
                        .getBaseIndex().getTablets().stream().map(t -> t.getId()).collect(Collectors.toList()))));

        setTableStatistics(t1, 400000);
        setTableStatistics(t2, 100);

        String sql = " with cte as ( select v4, v5, v6 from t1 )\n" +
                " select count(*) from (\n" +
                "    select * from cte union all\n" +
                "    select cte.v4, cte.v5, cte.v6 from t2 join cte on cte.v4 = t2.v7 and t2.v8 < 10) as t\n";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     table: t1, rollup: t1\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                "     tabletList=" + tabletIdsStrList.get(0) + "\n" +
                "     actualRows=0, avgRowSize=1.0\n" +
                "     cardinality: 400000");

        assertContains(plan, "  5:EXCHANGE\n" +
                "     distribution type: SHUFFLE\n" +
                "     cardinality: 400000\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: v4)");

        assertContains(plan, "  11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [10: v4, BIGINT, true] = [7: v7, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (7: v7), remote = false\n" +
                "  |  output columns: 10\n" +
                "  |  cardinality: 360000");
        connectContext.getSessionVariable().setEnablePipelineEngine(false);
    }

    @Test
    public void testLocalGroupingSet1() throws Exception {
        String sql = "select v1, v2, v3 from t0 group by grouping sets((v1, v2), (v1, v3), (v1))";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: v1, 2: v2, 3: v3, 4: GROUPING_ID\n" +
                "  |  \n" +
                "  1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[1, 2], [1, 3], [1]]\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testLocalGroupingSetAgg() throws Exception {
        String sql = "select v1, sum(v2) from " +
                "(select v1, v2, v3 from t0 group by grouping sets((v1, v2), (v1, v3), (v1))) as xx group by v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[1, 2], [1, 3], [1]]\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testOverflowFilterOnColocateJoin() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("colocate1");
        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("colocate2");

        StatisticStorage ss = globalStateMgr.getCurrentStatisticStorage();
        new Expectations(ss) {
            {
                ss.getColumnStatistic((Table) any, "k1");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic((Table) any, "k2");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic((Table) any, "k3");
                result = new ColumnStatistic(1, 2, 0, 4, 3);
            }
        };

        setTableStatistics(t1, 400000);
        setTableStatistics(t2, 100);

        String sql = "select * from colocate1 t1 join colocate2 t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2" +
                " where t1.k3 = 100000";

        String plan = getCostExplain(sql);
        assertContains(plan, "  |  join op: INNER JOIN (COLOCATE)");
        assertContains(plan, "k3-->[NaN, NaN, 0.0, 4.0, 1.0] ESTIMATE");
    }

    @Test
    public void testPruneShuffleColumns() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");

        StatisticStorage ss = globalStateMgr.getCurrentStatisticStorage();
        new Expectations(ss) {
            {
                ss.getColumnStatistic(t0, "v1");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic(t0, "v2");
                result = new ColumnStatistic(1, 4000000, 0, 4, 4000000);

                ss.getColumnStatistic(t0, "v3");
                result = new ColumnStatistic(1, 2000000, 0, 4, 2000000);

                ss.getColumnStatistic(t1, "v4");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic(t1, "v5");
                result = new ColumnStatistic(1, 100000, 0, 4, 100000);

                ss.getColumnStatistic(t1, "v6");
                result = new ColumnStatistic(1, 200000, 0, 4, 200000);
            }
        };

        setTableStatistics(t0, 4000000);
        setTableStatistics(t1, 100000);

        String sql = "select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 2: v2");

        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: v5");

        sql = "select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6 " +
                "               join[broadcast] t2 on t0.v2 = t2.v8";

        plan = getFragmentPlan(sql);
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 2: v2");

        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: v5");
        sql = "with xx as (select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6) " +
                "select x1.v1, x2.v2 from xx x1 join xx x2 on x1.v1 = x2.v1;";
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setCboCTERuseRatio(1.2);

        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: v5");
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 2: v2");
    }

    @Test
    public void testDateDiffWithStringConstant() throws Exception {
        String sql =
                "select count(t.a) from (select datediff(\"1981-09-06t03:40:33\", L_SHIPDATE) as a from lineitem) as t;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "output: count(datediff(CAST('1981-09-06t03:40:33' AS DATETIME), CAST(11: L_SHIPDATE AS DATETIME)))");
    }

    @Test
    public void testToDateToDays() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("test_all_type");
        StatisticStorage ss = GlobalStateMgr.getCurrentStatisticStorage();
        new Expectations(ss) {
            {
                ss.getColumnStatistic(t0, "id_datetime");
                result = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 4, 3);
            }
        };
        String sql = "select to_date(id_datetime) from test_all_type";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 11> : to_date(8: id_datetime)");

        sql = "select to_days(id_datetime) from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 1:Project\n" +
                "  |  <slot 11> : to_days(CAST(8: id_datetime AS DATE))");
    }

    @Test
    public void testMultiJoinColumnPruneShuffleColumnsAndGRF() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");

        StatisticStorage ss = GlobalStateMgr.getCurrentStatisticStorage();
        new Expectations(ss) {
            {
                ss.getColumnStatistic(t0, "v1");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic(t0, "v2");
                result = new ColumnStatistic(1, 4000000, 0, 4, 4000000);

                ss.getColumnStatistic(t0, "v3");
                result = new ColumnStatistic(1, 2000000, 0, 4, 2000000);

                ss.getColumnStatistic(t1, "v4");
                result = new ColumnStatistic(1, 2, 0, 4, 3);

                ss.getColumnStatistic(t1, "v5");
                result = new ColumnStatistic(1, 100000, 0, 4, 100000);

                ss.getColumnStatistic(t1, "v6");
                result = new ColumnStatistic(1, 200000, 0, 4, 200000);
            }
        };

        setTableStatistics(t0, 4000000);
        setTableStatistics(t1, 100000);

        String sql = "select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6";
        String plan = getVerboseExplain(sql);
        System.out.println(plan);

        assertContains(plan, "  Input Partition: RANDOM\n" +
                "  OutPut Partition: HASH_PARTITIONED: 2: v2\n" +
                "  OutPut Exchange Id: 01");

        assertContains(plan, "Input Partition: RANDOM\n" +
                "  OutPut Partition: HASH_PARTITIONED: 5: v5\n" +
                "  OutPut Exchange Id: 03");

        assertContains(plan, "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (5: v5), remote = true\n");

        assertContains(plan, "probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (2: v2)");
    }

    @Test
    public void testGroupByDistinct1() throws Exception {
        String sql = "select count(distinct v1), sum(v3), count(v3), max(v3), min(v3)" +
                "from t0 group by v2;";
        connectContext.getSessionVariable().setCboCteReuse(false);
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setCboCteReuse(true);
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: v1), sum(9: sum), sum(10: count), max(11: max), min(12: min)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3), count(3: v3), max(3: v3), min(3: v3)\n" +
                "  |  group by: 2: v2, 1: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testGroupByDistinct2() throws Exception {
        String sql = "select count(distinct v1), sum(distinct v1), sum(v3), count(v3), max(v3), min(v3)" +
                "from t0 group by v2;";
        connectContext.getSessionVariable().setCboCteReuse(false);
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setCboCteReuse(true);
        assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: v1), sum(1: v1), sum(10: sum), sum(11: count), max(12: max), min(13: min)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3), count(3: v3), max(3: v3), min(3: v3)\n" +
                "  |  group by: 2: v2, 1: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testExpressionWithCTE() throws Exception {
        String sql = "WITH x1 as (" +
                "   select * from t0" +
                ") " +
                "select sum(k2) from (" +
                "   SELECT * " +
                "   from x1 join[bucket] " +
                "   (SELECT v1 + 1 as k1, v2 + 2 as k2, v3 + 3 as k3 from x1) as y1 on x1.v1 = y1.k1" +
                ") d group by v3, k3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v1 = 10: expr\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "    BUCKET_SHUFFLE_HASH_PARTITIONED: 10: expr\n" +
                "\n" +
                "  2:Project\n" +
                "  |  <slot 10> : 7: v1 + 1\n" +
                "  |  <slot 11> : 8: v2 + 2\n" +
                "  |  <slot 12> : 9: v3 + 3\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testRemoveAggFromAggTable() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW agg_mv as select" +
                " LO_ORDERDATE," +
                " LO_ORDERKEY," +
                " sum(LO_REVENUE)," +
                " count(C_NAME)" +
                " from lineorder_flat_for_mv" +
                " group by LO_ORDERDATE, LO_ORDERKEY");
        Thread.sleep(3000);
        String sql = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv group by LO_ORDERDATE, LO_ORDERKEY";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: lineorder_flat_for_mv\n" +
                "     PREAGGREGATION: OFF. Reason: None aggregate function\n" +
                "     partitions=7/7\n" +
                "     rollup: agg_mv");

        String sql2 = "select LO_ORDERDATE, LO_ORDERKEY, sum(LO_REVENUE)" +
                " from lineorder_flat_for_mv group by LO_ORDERDATE, LO_ORDERKEY";
        String plan2 = getFragmentPlan(sql2);
        assertContains(plan2, "  0:OlapScanNode\n" +
                "     TABLE: lineorder_flat_for_mv\n" +
                "     PREAGGREGATION: OFF. Reason: None aggregate function\n" +
                "     partitions=7/7\n" +
                "     rollup: agg_mv\n" +
                "     tabletRatio=1050/1050");

        String sql3 = "select LO_ORDERDATE, LO_ORDERKEY, sum(LO_REVENUE), count(C_NAME)" +
                " from lineorder_flat_for_mv group by LO_ORDERDATE, LO_ORDERKEY";
        String plan3 = getFragmentPlan(sql3);
        assertContains(plan3, "  0:OlapScanNode\n" +
                        "     TABLE: lineorder_flat_for_mv\n" +
                        "     PREAGGREGATION: OFF. Reason: None aggregate function\n" +
                        "     partitions=7/7\n" +
                        "     rollup: agg_mv\n" +
                        "     tabletRatio=1050/1050");

        String sql4 = "select LO_ORDERDATE, LO_ORDERKEY, sum(LO_REVENUE) + 1, count(C_NAME) * 3" +
                " from lineorder_flat_for_mv group by LO_ORDERDATE, LO_ORDERKEY";
        String plan4 = getFragmentPlan(sql4);
        assertContains(plan4, "0:OlapScanNode\n" +
                        "     TABLE: lineorder_flat_for_mv\n" +
                        "     PREAGGREGATION: OFF. Reason: None aggregate function\n" +
                        "     partitions=7/7\n" +
                        "     rollup: agg_mv\n" +
                        "     tabletRatio=1050/1050");
    }

    @Test
    public void testRepeatNodeWithUnionAllRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, v2, SUM(v3) from t0 group by rollup(v1, v2)";
        String plan = getFragmentPlan(sql).replaceAll(" ", "");
        assertContains(plan, "1:UNION\n" +
                "|\n" +
                "|----15:EXCHANGE\n" +
                "|\n" +
                "|----21:EXCHANGE\n" +
                "|\n" +
                "8:EXCHANGE\n");

        sql = "select v1, SUM(v3) from t0 group by rollup(v1)";
        plan = getFragmentPlan(sql).replaceAll(" ", "");
        assertContains(plan, "1:UNION\n" +
                "|\n" +
                "|----14:EXCHANGE\n" +
                "|\n" +
                "8:EXCHANGE\n");

        sql = "select SUM(v3) from t0 group by grouping sets(())";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: GROUPING_ID\n" +
                "\n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 5: GROUPING_ID\n" +
                "  |  \n" +
                "  1:REPEAT_NODE");
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testLimitSemiJoin() throws Exception {
        String sql = "select * from t0 " +
                "        left semi join t2 on t0.v1 = t2.v7 " +
                "        left semi join t1 on t0.v1 = t1.v4 " +
                "limit 25;";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "  7:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: v4\n" +
                "  |  limit: 25");
        assertContains(plan, "  3:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v7\n" +
                "  |  \n" +
                "  |----2:EXCHANGE");
    }

    @Test
    public void testGroupingSetWithSameDistributeAgg() throws Exception {
        String sql = "select v2, v3, max(x1) " +
                " from (select v3, v2, sum(v1) as x1 from t0 group by v3, v2 ) x " +
                " group by grouping sets((v3, v2), (v2));";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "  7:AGGREGATE (merge finalize)\n" +
                "  |  output: max(5: max)\n" +
                "  |  group by: 3: v3, 2: v2, 6: GROUPING_ID\n" +
                "  |  \n" +
                "  6:EXCHANGE");
    }

    @Test
    public void testGroupingSetWithSameDistributeJoin() throws Exception {
        String sql = "select v2, v3, max(x1) " +
                " from (select v3, v2, v1 as x1 from t0 right outer join[shuffle] t1 on v3 = v6 and v2 = v5) x " +
                " group by grouping sets((v3, v2), (v2));";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "  9:AGGREGATE (merge finalize)\n" +
                "  |  output: max(7: max)\n" +
                "  |  group by: 3: v3, 2: v2, 8: GROUPING_ID\n" +
                "  |  \n" +
                "  8:EXCHANGE");
    }

    @Test
    public void testGroupingSetWithSameDistributeWindow() throws Exception {
        String sql = "select v2, v3, max(x1) " +
                " from (select v3, v2, sum(v1) over (partition by v3, v2 order by v3) as x1 from t0) x " +
                " group by grouping sets((v3, v2), (v2));";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "  8:AGGREGATE (merge finalize)\n" +
                "  |  output: max(5: max)\n" +
                "  |  group by: 3: v3, 2: v2, 6: GROUPING_ID\n" +
                "  |  \n" +
                "  7:EXCHANGE");
    }

    private boolean containAnyColocateNode(PlanNode root) {
        if (root.isColocate()) {
            return true;
        }
        for (PlanNode child : root.getChildren()) {
            if (containAnyColocateNode(child)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testOnePhaseAggWithLocalShuffle() throws Exception {
        final Reference<Boolean> isSingleBackendAndComputeNode = new Reference<>(true);
        final List<ColumnStatistic> avgHighCardinality = ImmutableList.of(
                new ColumnStatistic(0.0, NUM_TABLE0_ROWS, 0.0, 10, NUM_TABLE0_ROWS));
        final List<ColumnStatistic> avgLowCardinality = ImmutableList.of(
                new ColumnStatistic(0.0, NUM_TABLE0_ROWS, 0.0, 10, 100));
        final Reference<List<ColumnStatistic>> cardinality = new Reference<>(avgHighCardinality);
        new MockUp<SystemInfoService>() {
            @Mock
            public boolean isSingleBackendAndComputeNode() {
                return isSingleBackendAndComputeNode.getRef();
            }
        };
        new MockUp<MockTpchStatisticStorage>() {
            @Mock
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                if (columns.size() == 1) {
                    if (columns.get(0).equals("v1")) {
                        return avgLowCardinality;
                    } else if (columns.get(0).equals("v2")) {
                        return cardinality.getRef();
                    }
                }
                return avgLowCardinality;
            }
        };

        boolean prevEnableLocalShuffleAgg = connectContext.getSessionVariable().isEnableLocalShuffleAgg();
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);

        String sql;
        String plan;
        ExecPlan execPlan;
        OlapScanNode olapScanNode;

        try {
            // case 1: use one-phase local aggregation with local shuffle for high-cardinality agg and single BE.
            isSingleBackendAndComputeNode.setRef(true);
            cardinality.setRef(avgHighCardinality);
            sql = "select sum(v2) from colocate_t0 group by v2";
            execPlan = getExecPlan(sql);
            olapScanNode = (OlapScanNode) execPlan.getScanNodes().get(0);
            Assert.assertEquals(0, olapScanNode.getBucketExprs().size());
            Assert.assertFalse(containAnyColocateNode(execPlan.getFragments().get(1).getPlanRoot()));
            plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                    "  |  output: sum(2: v2)\n" +
                    "  |  group by: 2: v2\n" +
                    "  |  withLocalShuffle: true\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");

            // case 2: use one-phase local aggregation without local shuffle for high-cardinality agg and single BE.
            isSingleBackendAndComputeNode.setRef(true);
            cardinality.setRef(avgHighCardinality);
            sql = "select sum(v1) from colocate_t0 group by v1";
            execPlan = getExecPlan(sql);
            olapScanNode = (OlapScanNode) execPlan.getScanNodes().get(0);
            Assert.assertEquals(0, olapScanNode.getBucketExprs().size());
            Assert.assertTrue(containAnyColocateNode(execPlan.getFragments().get(1).getPlanRoot()));
            plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                    "  |  output: sum(1: v1)\n" +
                    "  |  group by: 1: v1\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");

            // case 3: use two-phase aggregation for non-grouping agg.
            isSingleBackendAndComputeNode.setRef(true);
            cardinality.setRef(avgHighCardinality);
            sql = "select sum(v2) from t0";
            execPlan = getExecPlan(sql);
            olapScanNode = (OlapScanNode) execPlan.getScanNodes().get(0);
            Assert.assertEquals(0, olapScanNode.getBucketExprs().size());
            plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, "1:AGGREGATE (update serialize)\n" +
                    "  |  output: sum(2: v2)\n" +
                    "  |  group by: \n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "3:AGGREGATE (merge finalize)\n" +
                    "  |  output: sum(4: sum)\n" +
                    "  |  group by: \n" +
                    "  |  \n" +
                    "  2:EXCHANGE");

            // case 4: use two-phase aggregation for multiple BEs.
            isSingleBackendAndComputeNode.setRef(false);
            cardinality.setRef(avgHighCardinality);
            sql = "select sum(v2) from t0 group by v2";
            execPlan = getExecPlan(sql);
            olapScanNode = (OlapScanNode) execPlan.getScanNodes().get(0);
            Assert.assertEquals(0, olapScanNode.getBucketExprs().size());
            plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, " 3:AGGREGATE (merge finalize)\n" +
                    "  |  output: sum(4: sum)\n" +
                    "  |  group by: 2: v2\n" +
                    "  |  \n" +
                    "  2:EXCHANGE");

            // case 5: use two-phase aggregation for low-cardinality agg.
            isSingleBackendAndComputeNode.setRef(true);
            cardinality.setRef(avgLowCardinality);
            sql = "select sum(v2) from t0 group by v2";
            execPlan = getExecPlan(sql);
            olapScanNode = (OlapScanNode) execPlan.getScanNodes().get(0);
            Assert.assertEquals(0, olapScanNode.getBucketExprs().size());
            plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, "1:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  output: sum(2: v2)\n" +
                    "  |  group by: 2: v2\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "  3:AGGREGATE (merge finalize)\n" +
                    "  |  output: sum(4: sum)\n" +
                    "  |  group by: 2: v2\n" +
                    "  |  \n" +
                    "  2:EXCHANGE");

            // case 6: insert into cannot use one-phase local aggregation with local shuffle.
            isSingleBackendAndComputeNode.setRef(true);
            cardinality.setRef(avgHighCardinality);
            sql = "insert into colocate_t0 select v2, v2, sum(v2) from t0 group by v2";
            execPlan = getExecPlan(sql);
            olapScanNode = (OlapScanNode) execPlan.getScanNodes().get(0);
            Assert.assertEquals(0, olapScanNode.getBucketExprs().size());
            Assert.assertFalse(containAnyColocateNode(execPlan.getFragments().get(1).getPlanRoot()));
            plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, "3:AGGREGATE (merge finalize)\n" +
                    "  |  output: sum(4: sum)\n" +
                    "  |  group by: 2: v2\n" +
                    "  |  \n" +
                    "  2:EXCHANGE");

            // case 7: Plan with join cannot use one-phase local aggregation with local shuffle.
            isSingleBackendAndComputeNode.setRef(true);
            cardinality.setRef(avgHighCardinality);
            sql = "select count(1) from " +
                    "(select v2, sum(v2) from t0 group by v2) t1 join " +
                    "(select v2, sum(v2) from t0 group by v2) t2 on t1.v2=t2.v2";
            execPlan = getExecPlan(sql);
            plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, "8:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 6: v2\n" +
                    "  |  \n" +
                    "  |----7:AGGREGATE (merge finalize)\n" +
                    "  |    |  group by: 6: v2\n" +
                    "  |    |  \n" +
                    "  |    6:EXCHANGE\n" +
                    "  |    \n" +
                    "  3:AGGREGATE (merge finalize)\n" +
                    "  |  group by: 2: v2\n" +
                    "  |  \n" +
                    "  2:EXCHANGE");
        } finally {
            connectContext.getSessionVariable().setEnableLocalShuffleAgg(prevEnableLocalShuffleAgg);
        }
    }

    @Test
    public void testExcessiveOR() throws Exception {
        String sql = "SELECT\n" +
                "  *\n" +
                "FROM t0\n" +
                "WHERE\n" +
                "  ((v1 >= 27 AND v1 < 28)) OR \n" +
                " (((v1 >= 26 AND v1 < 27)) OR \n" +
                " (((v1 >= 25 AND v1 < 26)) OR \n" +
                " (((v1 >= 24 AND v1 < 25)) OR \n" +
                " (((v1 >= 23 AND v1 < 24)) OR \n" +
                " (((v1 >= 22 AND v1 < 23)) OR \n" +
                " (((v1 >= 21 AND v1 < 22)) OR \n" +
                " (((v1 >= 20 AND v1 < 21)) OR \n" +
                " (((v1 >= 19 AND v1 < 20)) OR \n" +
                " (((v1 >= 18 AND v1 < 19)) OR \n" +
                " (((v1 >= 17 AND v1 < 18)) OR \n" +
                " (((v1 >= 16 AND v1 < 17)) OR \n" +
                " (((v1 >= 15 AND v1 < 16)) OR \n" +
                " (((v1 >= 14 AND v1 < 15)) OR \n" +
                " (((v1 >= 13 AND v1 < 14)) OR \n" +
                " (((v1 >= 12 AND v1 < 13)) OR \n" +
                " (((v1 >= 11 AND v1 < 12)) OR \n" +
                " (((v1 >= 10 AND v1 < 11)) OR \n" +
                " (((v1 >= 09 AND v1 < 10)) OR \n" +
                " (((v1 >= 08 AND v1 < 09)) OR \n" +
                " (((v1 >= 07 AND v1 < 08)) OR \n" +
                " (((v1 >= 06 AND v1 < 07)) OR \n" +
                " (((v1 >= 05 AND v1 < 06)) OR \n" +
                " (((v1 >= 04 AND v1 < 05)) OR \n" +
                " (((v1 >= 03 AND v1 < 04)) OR \n" +
                " (((v1 >= 02 AND v1 < 03)) OR \n" +
                "  ((v1 >= 01 AND v1 < 02)))))))))))))))))))))))))))";

        //Test excessive recursion and optimizer timeout due to too many OR
        getFragmentPlan(sql);
    }

    @Test
    public void testExcessiveAnd() throws Exception {
        String sql = "SELECT\n" +
                "  *\n" +
                "FROM t0\n" +
                "WHERE\n" +
                "  ((v1 >= 27 OR v1 < 28)) AND \n" +
                " (((v1 >= 26 OR v1 < 27)) AND \n" +
                " (((v1 >= 25 OR v1 < 26)) AND \n" +
                " (((v1 >= 24 OR v1 < 25)) AND \n" +
                " (((v1 >= 23 OR v1 < 24)) AND \n" +
                " (((v1 >= 22 OR v1 < 23)) AND \n" +
                " (((v1 >= 21 OR v1 < 22)) AND \n" +
                " (((v1 >= 20 OR v1 < 21)) AND \n" +
                " (((v1 >= 19 OR v1 < 20)) AND \n" +
                " (((v1 >= 18 OR v1 < 19)) AND \n" +
                " (((v1 >= 17 OR v1 < 18)) AND \n" +
                " (((v1 >= 16 OR v1 < 17)) AND \n" +
                " (((v1 >= 15 OR v1 < 16)) AND \n" +
                " (((v1 >= 14 OR v1 < 15)) AND \n" +
                " (((v1 >= 13 OR v1 < 14)) AND \n" +
                " (((v1 >= 12 OR v1 < 13)) AND \n" +
                " (((v1 >= 11 OR v1 < 12)) AND \n" +
                " (((v1 >= 10 OR v1 < 11)) AND \n" +
                " (((v1 >= 09 OR v1 < 10)) AND \n" +
                " (((v1 >= 08 OR v1 < 09)) AND \n" +
                " (((v1 >= 07 OR v1 < 08)) AND \n" +
                " (((v1 >= 06 OR v1 < 07)) AND \n" +
                " (((v1 >= 05 OR v1 < 06)) AND \n" +
                " (((v1 >= 04 OR v1 < 05)) AND \n" +
                " (((v1 >= 03 OR v1 < 04)) AND \n" +
                " (((v1 >= 02 OR v1 < 03)) AND \n" +
                "  ((v1 >= 01 OR v1 < 02)))))))))))))))))))))))))))";

        //Test excessive recursion and optimizer timeout due to too many OR
        getFragmentPlan(sql);
    }

    @Test
    public void testTimeOutOrAnd50() throws Exception {
        String sql = "SELECT v1\n" +
                "FROM t0\n" +
                "WHERE (v2 = 1\n" +
                " OR ((v2 = 2\n" +
                "  OR ((v2 = 3\n" +
                "   OR ((v2 = 4\n" +
                "    OR ((v2 = 5\n" +
                "     OR ((v2 = 6\n" +
                "      OR ((v2 = 7\n" +
                "       OR ((v2 = 8\n" +
                "        OR ((v2 = 9\n" +
                "         OR ((v2 = 10\n" +
                "          OR (v1 = 11 AND (v2 = 12\n" +
                "           OR (v2 = 13\n" +
                "            OR (v2 = 14\n" +
                "             OR (v1 = 15 AND (v2 = 16\n" +
                "              OR (v2 = 17\n" +
                "               OR (v2 = 18\n" +
                "                OR (v2 = 19\n" +
                "                 OR (v2 = 20\n" +
                "                  OR ((v2 = 21 AND v1 = 22)\n" +
                "                     OR (v2 = 23 AND v1 = 24)) AND v1 = 25) AND v1 = 26\n" +
                "                 OR (v2 = 27 AND v1 = 28)\n" +
                "                 OR (v2 = 29 AND v1 = 30)\n" +
                "                 OR (v2 = 31 AND v1 = 32)\n" +
                "                 OR (v2 = 33 AND v1 = 34)\n" +
                "                 OR (v2 = 35 AND v1 = 36)\n" +
                "                 OR (v2 = 37 AND v1 = 38)) AND v1 = 39\n" +
                "                OR v2 = 40 AND v1 = 41\n" +
                "                OR v2 = 42 AND v1 = 43\n" +
                "                OR v2 = 44 AND v1 = 45\n" +
                "                OR v2 = 46 AND v1 = 47\n" +
                "                OR v2 = 48 AND v1 = 49\n" +
                "                OR v2 = 50 AND v1 = 51\n" +
                "                OR v2 = 52 AND v1 = 53\n" +
                "                OR v2 = 54 AND v1 = 55\n" +
                "                OR v2 = 56 AND v1 = 57\n" +
                "                OR v2 = 58 AND v1 = 59\n" +
                "                OR v2 = 60 AND v1 = 61\n" +
                "                OR v2 = 62 AND v1 = 63\n" +
                "                OR v2 = 64 AND v1 = 65\n" +
                "                OR v2 = 66 AND v1 = 67\n" +
                "                OR v2 = 68 AND v1 = 69) AND v1 = 70\n" +
                "               OR v2 = 71 AND v1 = 72) AND v1 = 73\n" +
                "              OR v2 = 74 AND v1 = 75\n" +
                "              OR v2 = 76 AND v1 = 77\n" +
                "              OR v2 = 78 AND v1 = 79\n" +
                "              OR v2 = 80))) AND v1 = 81 AND v1 = 82\n" +
                "            OR v2 = 83 AND v1 = 84\n" +
                "            OR v2 = 85 AND v1 = 86) AND v1 = 87\n" +
                "           OR v2 = 88 AND v1 = 89\n" +
                "           OR v2 = 90)))\n" +
                "            AND v1 = 91))\n" +
                "           AND v1 = 92))\n" +
                "          AND v1 = 93))\n" +
                "         AND v1 = 94))\n" +
                "        AND v1 = 95))\n" +
                "       AND v1 = 96))\n" +
                "      AND v1 = 97))\n" +
                "     AND v1 = 98))\n" +
                "    AND v1 = 99))\n" +
                "   AND v1 = 100";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
    }

    @Test
    public void testTimeOutOr16() throws Exception {
        String sql = "SELECT v1\n" +
                "FROM t0\n" +
                "WHERE (v2 = 1\n" +
                " OR ((v2 = 2\n" +
                "  OR ((v2 = 3\n" +
                "   OR ((v2 = 4\n" +
                "    OR ((v2 = 5\n" +
                "     OR ((v2 = 6\n" +
                "      OR ((v2 = 7\n" +
                "       OR ((v2 = 8\n" +
                "        OR ((v2 = 9\n" +
                "         OR ((v2 = 10\n" +
                "          OR (v1 = 11 AND (v2 = 12\n" +
                "           OR (v2 = 13\n" +
                "            OR (v2 = 14\n" +
                "             OR (v1 = 15 AND (v2 = 16\n" +
                "              OR (v2 = 17  AND v1 = 70\n" +
                "               OR v2 = 71 AND v1 = 72) AND v1 = 73\n" +
                "              OR v2 = 80 AND (v1 = 2 OR (v2 = 99 AND v1 = 29))))) AND v1 = 81 AND v1 = 82\n" +
                "            OR v2 = 85 AND v1 = 86) AND v1 = 87)))\n" +
                "            AND v1 = 91))\n" +
                "           AND v1 = 92))\n" +
                "          AND v1 = 93))\n" +
                "         AND v1 = 94))\n" +
                "        AND v1 = 95))\n" +
                "       AND v1 = 96))\n" +
                "      AND v1 = 97))\n" +
                "     AND v1 = 98))\n" +
                "    AND v1 = 99))\n" +
                "   AND v1 = 100";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON");
    }

    @Test
    public void testTimeOutOr11() throws Exception {
        String sql = "SELECT v1\n" +
                "FROM t0\n" +
                "WHERE (v2 = 1\n" +
                " OR ((v2 = 2\n" +
                "  OR ((v2 = 3\n" +
                "   OR ((v2 = 4\n" +
                "    OR ((v2 = 5\n" +
                "     OR ((v2 = 6\n" +
                "      OR ((v2 = 7\n" +
                "       OR ((v2 = 8\n" +
                "        OR ((v2 = 9\n" +
                "         OR ((v2 = 10\n" +
                "          OR (v1 = 11 AND (v2 = 12\n" +
                "           OR (v2 = 13)\n" +
                "                   AND v1 = 87\n" +
                "           OR v2 = 90)))\n" +
                "                 AND v1 = 91))\n" +
                "                AND v1 = 92))\n" +
                "               AND v1 = 93))\n" +
                "              AND v1 = 94))\n" +
                "             AND v1 = 95))\n" +
                "            AND v1 = 96))\n" +
                "           AND v1 = 97))\n" +
                "          AND v1 = 98))\n" +
                "         AND v1 = 99))\n" +
                "        AND v1 = 100;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON");
    }

    @Test
    public void testTimeOutOnlyOr20() throws Exception {
        String sql = "SELECT v1\n" +
                "FROM t0\n" +
                "WHERE 1 = 0" +
                "   OR v1 = 1" +
                "   OR v2 = 2" +
                "   OR v1 = 3" +
                "   OR v2 = 4" +
                "   OR v1 = 5" +
                "   OR v2 = 6" +
                "   OR v1 = 7" +
                "   OR v2 = 8" +
                "   OR v1 = 9" +
                "   OR v2 = 0" +
                "   OR v1 = 11" +
                "   OR v2 = 12" +
                "   OR v1 = 13" +
                "   OR v2 = 14" +
                "   OR v1 = 15" +
                "   OR v2 = 16" +
                "   OR v1 = 17" +
                "   OR v2 = 18" +
                "   OR v1 = 19" +
                "   OR v2 = 20" +
                "   OR v1 = 21" +
                "   OR v2 = 22" +
                "   OR v1 = 23" +
                "   OR v2 = 24" +
                "   OR v1 = 25" +
                "   OR v2 = 26" +
                "   OR v1 = 27" +
                "   OR v2 = 28" +
                "   OR v1 = 29" +
                "   OR v2 = 30" +
                "   OR v1 = 31" +
                "   OR v2 = 32" +
                "   OR v1 = 33" +
                "   OR v2 = 34" +
                "   OR v1 = 35" +
                "   OR v2 = 36" +
                "   OR v1 = 37" +
                "   OR v2 = 38" +
                "   OR v1 = 39" +
                "   ;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON");
    }

    @Ignore
    public void testDeepTreePredicate() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table2 = (OlapTable) globalStateMgr.getDb("test").getTable("test_dict");
        setTableStatistics(table2, 20000000);

        String sql = getSQLFile("optimized-plan/large_predicate");
        // need JVM -Xss10m

        String plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: test_dict");
    }

    @Test
    public void testJoinUnreorder() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 10000000);
        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 10000);

        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("t2");
        setTableStatistics(t1, 10);

        String sql = "Select * " +
                " from t0 join t1 on t0.v3 = t1.v6 " +
                "         join t2 on t0.v2 = t2.v8 and t1.v5 = t2.v7";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: v5 = 7: v7");
        assertContains(plan, "6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 3: v3 = 6: v6");

        sql = "Select * " +
                " from t0 join[unreorder] t1 on t0.v3 = t1.v6 " +
                "         join[unreorder] t2 on t0.v2 = t2.v8 and t1.v5 = t2.v7";

        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 5: v5 = 7: v7\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: v3 = 6: v6\n" +
                "  |  \n" +
                "  |----2:EXCHANGE");
    }

    @Test
    public void testPruneLimit() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table2 = (OlapTable) globalStateMgr.getDb("test").getTable("lineitem_partition");
        setTableStatistics(table2, 10);

        new MockUp<LocalTablet>() {
            @Mock
            public long getRowCount(long version) {
                return 10;
            }
        };

        String sql = "select * from lineitem_partition limit 2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "     partitions=7/7\n" +
                "     rollup: lineitem_partition\n" +
                "     tabletRatio=1");
    }

    @Test
    public void testPruneInvalidPredicate() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table2 = (OlapTable) globalStateMgr.getDb("test").getTable("lineitem_partition");
        setTableStatistics(table2, 10);

        new MockUp<LocalTablet>() {
            @Mock
            public long getRowCount(long version) {
                return 10;
            }
        };

        String sql = "select * from lineitem_partition where L_SHIPDATE > cast(true as date)";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "TABLE: lineitem_partition\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 11: L_SHIPDATE > CAST(TRUE AS DATE)");
    }

    @Test
    public void testRankWindowOptimization() throws Exception {
        String sql;
        String plan;

        // Partition columns include aggregation grouping columns, so needdn't exchange between PartitionTopN and Analytic.
        {
            sql = "with " +
                    "w1 as (select v2, max(v1) as max_v1 from t0 group by v2), " +
                    "w2 as (select v2, max_v1, row_number() over(partition by v2, max_v1 order by max_v1) as rn from w1) " +
                    "select * from w2 where rn = 2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  6:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  partition by: 11: v2, 13: max\n" +
                    "  |  order by: 13: max ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  5:SORT\n" +
                    "  |  order by: <slot 11> 11: v2 ASC, <slot 13> 13: max ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  4:PARTITION-TOP-N\n" +
                    "  |  partition by: 11: v2 , 13: max \n" +
                    "  |  partition limit: 2\n" +
                    "  |  order by: <slot 11> 11: v2 ASC, <slot 13> 13: max ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  3:AGGREGATE (merge finalize)\n" +
                    "  |  output: max(13: max)\n" +
                    "  |  group by: 11: v2\n" +
                    "  |  \n" +
                    "  2:EXCHANGE");

            sql = "with " +
                    "w1 as (select v2, max(v1) as max_v1 from t0 group by v2), " +
                    "w2 as (select v2, max_v1, row_number() over(partition by v2, max_v1 order by max_v1) as rn from w1) " +
                    "select * from w2 order by rn, v2 limit 2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "6:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  partition by: 11: v2, 13: max\n" +
                    "  |  order by: 13: max ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  5:SORT\n" +
                    "  |  order by: <slot 11> 11: v2 ASC, <slot 13> 13: max ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  4:PARTITION-TOP-N\n" +
                    "  |  partition by: 11: v2 , 13: max \n" +
                    "  |  partition limit: 2\n" +
                    "  |  order by: <slot 11> 11: v2 ASC, <slot 13> 13: max ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  3:AGGREGATE (merge finalize)\n" +
                    "  |  output: max(13: max)\n" +
                    "  |  group by: 11: v2\n" +
                    "  |  \n" +
                    "  2:EXCHANGE");
        }

        // Partition columns don't include aggregation grouping columns, so need exchange between PartitionTopN and Analytic.
        {
            sql = "with " +
                    "w1 as (select v2, max(v1) as max_v1 from t0 group by v2), " +
                    "w2 as (select v2, max_v1, row_number() over(partition by max_v1 order by v2) as rn from w1) " +
                    "select * from w2 where rn = 2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  7:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  partition by: 13: max\n" +
                    "  |  order by: 11: v2 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  6:SORT\n" +
                    "  |  order by: <slot 13> 13: max ASC, <slot 11> 11: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  5:EXCHANGE");
            assertContains(plan, "  4:PARTITION-TOP-N\n" +
                    "  |  partition by: 13: max \n" +
                    "  |  partition limit: 2\n" +
                    "  |  order by: <slot 13> 13: max ASC, <slot 11> 11: v2 ASC\n" +
                    "  |  offset: 0");

            sql = "with " +
                    "w1 as (select v2, max(v1) as max_v1 from t0 group by v2), " +
                    "w2 as (select v2, max_v1, row_number() over(partition by max_v1 order by v2) as rn from w1) " +
                    "select * from w2 order by rn, v2 limit 2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  7:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  partition by: 13: max\n" +
                    "  |  order by: 11: v2 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  6:SORT\n" +
                    "  |  order by: <slot 13> 13: max ASC, <slot 11> 11: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  5:EXCHANGE");
            assertContains("  4:PARTITION-TOP-N\n" +
                    "  |  partition by: 13: max \n" +
                    "  |  partition limit: 2\n" +
                    "  |  order by: <slot 13> 13: max ASC, <slot 11> 11: v2 ASC\n" +
                    "  |  offset: 0");
        }

        // Partition columns include table bucket columns, so needn't exchange between PartitionTopN and Analytic.
        {
            sql = "with w1 as (select v1, v2, v3, row_number() over(partition by v1, v2 order by v3) as rn from t0) " +
                    "select * from w1 where rn = 2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  4:SELECT\n" +
                    "  |  predicates: 8: row_number() = 2\n" +
                    "  |  \n" +
                    "  3:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  partition by: 5: v1, 6: v2\n" +
                    "  |  order by: 7: v3 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  2:SORT\n" +
                    "  |  order by: <slot 5> 5: v1 ASC, <slot 6> 6: v2 ASC, <slot 7> 7: v3 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 5: v1 , 6: v2 \n" +
                    "  |  partition limit: 2\n" +
                    "  |  order by: <slot 5> 5: v1 ASC, <slot 6> 6: v2 ASC, <slot 7> 7: v3 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");

            sql = "with w1 as (select v1, v2, v3, row_number() over(partition by v1, v2 order by v3) as rn from t0) " +
                    "select * from w1 order by rn, v1 limit 2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  4:TOP-N\n" +
                    "  |  order by: <slot 8> 8: row_number() ASC, <slot 5> 5: v1 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 2\n" +
                    "  |  \n" +
                    "  3:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  partition by: 5: v1, 6: v2\n" +
                    "  |  order by: 7: v3 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  2:SORT\n" +
                    "  |  order by: <slot 5> 5: v1 ASC, <slot 6> 6: v2 ASC, <slot 7> 7: v3 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 5: v1 , 6: v2 \n" +
                    "  |  partition limit: 2\n" +
                    "  |  order by: <slot 5> 5: v1 ASC, <slot 6> 6: v2 ASC, <slot 7> 7: v3 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
        }

        // Partition columns don't include table bucket columns, so need exchange between PartitionTopN and Analytic.
        {
            sql = "with w1 as (select v1, v2, v3, row_number() over(partition by v2, v3 order by v1) as rn from t0) " +
                    "select * from w1 where rn = 2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  5:SELECT\n" +
                    "  |  predicates: 8: row_number() = 2\n" +
                    "  |  \n" +
                    "  4:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  partition by: 6: v2, 7: v3\n" +
                    "  |  order by: 5: v1 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  3:SORT\n" +
                    "  |  order by: <slot 6> 6: v2 ASC, <slot 7> 7: v3 ASC, <slot 5> 5: v1 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  2:EXCHANGE\n");
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 6: v2 , 7: v3 \n" +
                    "  |  partition limit: 2\n" +
                    "  |  order by: <slot 6> 6: v2 ASC, <slot 7> 7: v3 ASC, <slot 5> 5: v1 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");

            sql = "with w1 as (select v1, v2, v3, row_number() over(partition by v2, v3 order by v1) as rn from t0) " +
                    "select * from w1 order by rn, v1 limit 2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  5:TOP-N\n" +
                    "  |  order by: <slot 8> 8: row_number() ASC, <slot 5> 5: v1 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 2\n" +
                    "  |  \n" +
                    "  4:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  partition by: 6: v2, 7: v3\n" +
                    "  |  order by: 5: v1 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  3:SORT\n" +
                    "  |  order by: <slot 6> 6: v2 ASC, <slot 7> 7: v3 ASC, <slot 5> 5: v1 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n");
            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 6: v2 , 7: v3 \n" +
                    "  |  partition limit: 2\n" +
                    "  |  order by: <slot 6> 6: v2 ASC, <slot 7> 7: v3 ASC, <slot 5> 5: v1 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
        }
    }

    @Test
    public void testPlanCost() throws Exception {
        String plan = getVerboseExplain("select t1a, v1 " +
                "from t0 join [broadcast] test_all_type " +
                "join [shuffle] (select 1 as v1_c1 where abs(1) = 2) v1 on t1a=v1 and t1a=v1_c1");
        assertContains(plan, "  11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [4: t1a, VARCHAR, true] = [16: cast, VARCHAR, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 1, build_expr = (16: cast), remote = true\n" +
                "  |  output columns: 1, 4\n" +
                "  |  cardinality: 9000\n" +
                "  |  \n" +
                "  |----10:EXCHANGE\n" +
                "  |       distribution type: SHUFFLE\n" +
                "  |       partition exprs: [16: cast, VARCHAR, false]\n" +
                "  |       cardinality: 1\n" +
                "  |    \n" +
                "  6:EXCHANGE\n" +
                "     distribution type: SHUFFLE\n" +
                "     partition exprs: [4: t1a, VARCHAR, true]\n" +
                "     cardinality: 9000");

        AuditEvent event = connectContext.getAuditEventBuilder().build();
        Assert.assertTrue("planMemCosts should be > 1, but: " + event.planMemCosts, event.planMemCosts > 1);
        Assert.assertTrue("planCpuCosts should be > 1, but: " + event.planCpuCosts, event.planCpuCosts > 1);

    }

    @Test
    public void testStringInPredicateEstimate(
            @Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any,
                        Lists.newArrayList("t1a", (String) any, (String) any, (String) any));
                result = Lists.newArrayList(new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY,
                        0.0, 10, 3),
                        new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY,
                                0.0, 10, 3, null,
                                ColumnStatistic.StatisticType.UNKNOWN),
                        new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY,
                                0.0, 10, 3, null,
                                ColumnStatistic.StatisticType.UNKNOWN),
                        new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY,
                                0.0, 10, 3, null,
                                ColumnStatistic.StatisticType.UNKNOWN));
            }
        };

        String sql = "SELECT t1a from test_all_type where t1a in ('a', 'b', 'c');";
        String plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 10000");

        sql = "SELECT t1a from test_all_type where t1a not in ('a', 'b', 'c');";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 5000");

        sql = "SELECT t1a from test_all_type where t1a  in ('a', 'b', 'c', 'd', 'e');";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 10000");

        sql = "SELECT t1a from test_all_type where t1a != 'a' and  t1a != 'a';";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 6667");

        sql = "SELECT t1.L_PARTKEY from lineitem t1 join nation t2 on t1.L_PARTKEY = t2.N_NATIONKEY " +
                "and t1.L_SUPPKEY = t2.N_NATIONKEY;";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 6250000");

        sql = "SELECT t1.L_PARTKEY from lineitem t1 left join nation t2 on t1.L_PARTKEY = t2.N_NATIONKEY " +
                "and t1.L_SUPPKEY = t2.N_NATIONKEY;";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 100000000");
    }
}
