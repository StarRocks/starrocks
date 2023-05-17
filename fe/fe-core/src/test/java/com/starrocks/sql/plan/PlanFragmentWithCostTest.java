// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Reference;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MockTpchStatisticStorage;
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
import java.util.List;

public class PlanFragmentWithCostTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        FeConstants.default_scheduler_interval_millisecond = 1;

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table2 = (OlapTable) globalStateMgr.getDb("test").getTable("test_all_type");
        setTableStatistics(table2, 10000);

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 10000);

        OlapTable colocateT0 = (OlapTable) globalStateMgr.getDb("test").getTable("colocate_t0");
        setTableStatistics(colocateT0, 10000);

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
                "BUCKETS 3 PROPERTIES ( \"replication_num\" = \"1\", " +
                "\"storage_format\" = \"v2\" );");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW bitmap_mv\n" +
                "                             AS\n" +
                "                             SELECT k1,k2,k3,k4, bitmap_union(to_bitmap(k7)), " +
                "bitmap_union(to_bitmap(k8)) FROM duplicate_table_with_null group by k1,k2,k3,k4");
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("3:NESTLOOP JOIN"));

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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:AGGREGATE (merge finalize)\n"
                + "  |  output: sum(4: sum)\n"
                + "  |  group by: 2: v2"));
        Assert.assertTrue(planFragment.contains("  1:AGGREGATE (update serialize)\n"
                + "  |  STREAMING\n"
                + "  |  output: sum(2: v2)\n"
                + "  |  group by: 2: v2"));
    }

    @Test
    public void testAggWithHighCardinality(@Mocked MockTpchStatisticStorage mockedStatisticStorage) throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, Lists.newArrayList("v2"));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 7000));
            }
        };

        String sql = "select sum(v2) from t0 group by v2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:AGGREGATE (update finalize)\n"
                + "  |  output: sum(2: v2)\n"
                + "  |  group by: 2: v2"));
        Assert.assertFalse(planFragment.contains("  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 2: v2"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertFalse(planFragment.contains("TOP-N"));
        Assert.assertTrue(planFragment.contains("  2:SORT\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertFalse(planFragment.contains("TOP-N"));
        Assert.assertTrue(planFragment.contains("  2:SORT\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:TOP-N\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0\n"
                + "  |  limit: 1"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("TOP-N"));
        Assert.assertTrue(planFragment.contains("  2:TOP-N\n"
                + "  |  order by: <slot 1> 1: v1 ASC\n"
                + "  |  offset: 0\n"
                + "  |  limit: 1"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:AGGREGATE (merge serialize)\n"
                + "  |  output: sum(5: sum)\n"
                + "  |  group by: 2: v2"));
        Assert.assertTrue(planFragment.contains("  6:AGGREGATE (merge finalize)\n"
                + "  |  output: count(4: count), sum(5: sum)\n"
                + "  |  group by: \n"));
        Assert.assertTrue(planFragment.contains("  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 05\n"
                + "    UNPARTITIONED"));
        Assert.assertFalse(planFragment.contains("PLAN FRAGMENT 3"));

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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:AGGREGATE (update finalize)\n"
                + "  |  output: multi_distinct_count(2: v2), sum(1: v1)\n"
                + "  |  group by:"));

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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:AGGREGATE (merge serialize)\n"
                + "  |  group by: 2: v2, 3: v3"));
        Assert.assertTrue(planFragment.contains("  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 06\n"
                + "    UNPARTITIONED"));

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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains(" 4:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: v2)\n" +
                "  |  group by: 3: v3"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  4:Project\n" +
                "  |  <slot 4> : -1 * 3: v3"));
        Assert.assertTrue(planFragment.contains("PREDICATES: -1 * 3: v3 < 63"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  4:Project\n"
                + "  |  <slot 4> : -1 * 3: v3"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  4:HASH JOIN\n"
                + "  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 2: v2 = 7: t1d\n"
                + "  |  \n"
                + "  |----3:EXCHANGE\n"
                + "  |    \n"
                + "  1:EXCHANGE"));
        Assert.assertTrue(planFragment.contains("    EXCHANGE ID: 03\n"
                + "    HASH_PARTITIONED: 7: t1d\n"
                + "\n"
                + "  2:OlapScanNode"));
        Assert.assertTrue(planFragment.contains("  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 01\n"
                + "    HASH_PARTITIONED: 2: v2\n"
                + "\n"
                + "  0:OlapScanNode"));
        UtFrameUtils.dropMockBackend(10002);
        UtFrameUtils.dropMockBackend(10003);
    }

    @Test
    public void testBroadcastInnerJoin() throws Exception {
        String sql = "SELECT v1, t1d from t0 join test_all_type on t0.v2 = test_all_type.t1d ;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:HASH JOIN\n"
                + "  |  join op: INNER JOIN (BROADCAST)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 2: v2 = 7: t1d\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n"));
    }

    @Test
    public void testBroadcastInnerJoinWithCommutativity() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(table, 1000);
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d ;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:HASH JOIN\n"
                + "  |  join op: INNER JOIN (BROADCAST)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 7: t1d = 1: v1\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: test_all_type\n"));
        setTableStatistics(table, 10000);
    }

    @Test
    public void testColocateJoin() throws Exception {
        String sql = "SELECT * from t0 join t0 as b on t0.v1 = b.v1;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("join op: INNER JOIN (COLOCATE)"));
    }

    @Test
    public void testColocateAgg() throws Exception {
        String sql = "SELECT count(*) from t0 group by t0.v1;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:AGGREGATE (update finalize)"));
    }

    @Test
    public void testDistinctExpr() throws Exception {
        String sql = "SELECT DISTINCT - - v1 DIV - 98 FROM t0;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertFalse(planFragment.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 4>\n" +
                "  |  \n" +
                "  1:Project"));
        Assert.assertTrue(planFragment.contains("EXCHANGE"));
    }

    @Test
    public void testRollUp() throws Exception {
        String sql = "select event_day from test_mv group by event_day;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r3"));

        sql = "select count(*) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));

        sql = "select count(*), event_day from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));

        sql = "select event_day from test_mv where citycode = 1 group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r2"));

        sql = "select siteid from test_mv where event_day  = 1 group by siteid;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r1"));

        sql = "select siteid from test_mv group by siteid, event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r1"));

        sql = "select siteid from test_mv group by siteid, username;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));

        sql = "select siteid,sum(pv) from test_mv group by siteid, username;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));

        sql = "select sum(pv) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r4"));

        sql = "select max(pv) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));

        sql = "select max(event_day) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r3"));

        sql = "select max(event_day), sum(pv) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: r4"));

        sql = "select max(event_day), max(pv) from test_mv group by event_day;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: test_mv"));
    }

    @Test
    public void testMV() throws Exception {
        String sql = "select count(distinct k7), count(distinct k8) from duplicate_table_with_null;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("OUTPUT EXPRS:16: count | 17: count"));
        Assert.assertTrue(planFragment.contains("14: mv_bitmap_union_k7"));
        Assert.assertTrue(planFragment.contains("15: mv_bitmap_union_k8"));
        Assert.assertTrue(planFragment.contains("rollup: bitmap_mv"));
    }

    @Test
    @Ignore("disable replicate join temporarily")
    public void testReplicatedJoin() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);
        String sql = "select s_name, s_address from supplier, nation where s_suppkey in " +
                "( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like 'forest%' ) and ps_availqty > " +
                "( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and " +
                "l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year ) ) " +
                "and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  11:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (REPLICATED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: PS_PARTKEY = 20: P_PARTKEY"));
        Assert.assertTrue(plan.contains("  14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 32: L_PARTKEY = 14: PS_PARTKEY\n" +
                "  |  equal join conjunct: 33: L_SUPPKEY = 15: PS_SUPPKEY\n" +
                "  |  other join predicates: CAST(16: PS_AVAILQTY AS DOUBLE) > 0.5 * 48: sum"));
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    @Test
    public void testReapNodeStatistics() throws Exception {
        String sql = "select v1, v2, grouping_id(v1,v2), SUM(v3) from t0 group by cube(v1, v2)";
        String plan = getCostExplain(sql);
        // check scan node
        Assert.assertTrue(plan.contains("cardinality: 10000"));
        // check repeat node
        Assert.assertTrue(plan.contains("cardinality: 40000"));
        Assert.assertTrue(plan.contains(" * GROUPING_ID-->[0.0, 3.0, 0.0, 8.0, 4.0] ESTIMATE\n" +
                "  |  * GROUPING-->[0.0, 3.0, 0.0, 8.0, 4.0] ESTIMATE"));

        sql = "select v1, v2, grouping_id(v1,v2), SUM(v3) from t0 group by rollup(v1, v2)";
        plan = getCostExplain(sql);
        // check scan node
        Assert.assertTrue(plan.contains("cardinality: 10000"));
        // check repeat node
        Assert.assertTrue(plan.contains("cardinality: 30000"));
        Assert.assertTrue(plan.contains("* GROUPING_ID-->[0.0, 3.0, 0.0, 8.0, 3.0] ESTIMATE\n" +
                "  |  * GROUPING-->[0.0, 3.0, 0.0, 8.0, 3.0] ESTIMATE"));
    }

    @Test
    public void testReapNodeExchange() throws Exception {
        String sql = "select v1, v2, SUM(v3) from t0 group by rollup(v1, v2)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
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
                "  0:OlapScanNode"));

        sql = "select v1, SUM(v3) from t0 group by rollup(v1)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
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
                "     PREAGGREGATION: ON"));

        sql = "select SUM(v3) from t0 group by grouping sets(())";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:EXCHANGE\n" +
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
                "  1:REPEAT_NODE"));
    }

    @Test
    public void testDisableOnePhaseWithTableRowCountMayNotAccurate() throws Exception {
        // check can not generate 1 phase aggregation if fe do not get real table row count from be.
        String sql = "select count(1) from orders group by O_CUSTKEY, O_ORDERDATE";
        String plan = getFragmentPlan(sql);
        // check has 3 phase aggregation
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge finalize)"));
        sql = "select count(distinct O_ORDERKEY) from orders group by O_CUSTKEY, O_ORDERDATE";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: O_ORDERKEY, 2: O_CUSTKEY, 5: O_ORDERDATE"));
    }

    @Test
    public void testDisableOnePhaseWithUnknownColumnStatistics() throws Exception {
        // check can not generate 1 phase aggregation if column statistics is unknown
        String sql = "select count(v1) from t0 group by v2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge finalize)"));
        sql = "select count(distinct v1) from t0 group by v2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: v1, 2: v2"));
    }

    @Test
    public void testSemiJoinPushDownPredicate() throws Exception {
        String sql =
                "select * from t0 left semi join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 and t0.v1 = 1 and t1.v5 = 2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 1, 2: v2 = 2"));
        Assert.assertTrue(plan.contains("TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: v5 = 2, 4: v4 = 1"));
    }

    @Test
    public void testOuterJoinPushDownPredicate() throws Exception {
        String sql =
                "select * from t0 left outer join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 and t0.v1 = 1 and t1.v5 = 2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1"));
        Assert.assertTrue(plan.contains("TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: v5 = 2, 4: v4 = 1"));
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
        Assert.assertTrue(plan.contains("TPlanNode(node_id:3, node_type:HASH_JOIN_NODE"));
        Assert.assertTrue(plan.contains("local_rf_waiting_set:[3]"));
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  0:INTERSECT\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));
        Assert.assertTrue(planFragment.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    HASH_PARTITIONED: 4: v7\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        setTableStatistics(t0, 10000);
    }

    @Test
    public void testNotPushDownRuntimeFilterToSortNode() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 10);

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 1000000000L);

        String sql =
                "select t0.v1 from (select v4 from t1 order by v4 limit 1000000000) as t1x join [broadcast] t0 where t0.v1 = t1x.v4";
        String planFragment = getVerboseExplain(sql);

        Assert.assertTrue(planFragment.contains("  1:TOP-N\n" +
                "  |  order by: [1, BIGINT, true] ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 1000000000\n" +
                "  |  cardinality: 1000000000\n" +
                "  |  \n" +
                "  0:OlapScanNode"));

        Assert.assertTrue(planFragment.contains("  2:MERGING-EXCHANGE\n" +
                "     limit: 1000000000\n" +
                "     cardinality: 1000000000\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: v4)"));

        setTableStatistics(t0, 10000);
    }

    @Test
    public void testMergeTwoAggArgTypes() throws Exception {
        String sql = "select sum(t.int_sum) from (select sum(t1c) as int_sum from test_all_type)t";
        String planFragment = getVerboseExplain(sql);
        Assert.assertTrue(planFragment.contains("  1:AGGREGATE (update serialize)\n" +
                "  |  aggregate: sum[([3: t1c, INT, true]); args: INT; result: BIGINT;"));
    }

    @Test
    public void testPushDownRuntimeFilterAcrossSetOperationNode() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 1000);
        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 100);
        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("t2");

        ArrayList<String> plans = new ArrayList<>();
        /// ===== union =====
        setTableStatistics(t2, 200000);
        setTableStatistics(t0, 400000);
        setTableStatistics(t1, 400000);

        String sql =
                "select * from (select v1+1 as v1 ,v2,v3 from t0 union select v4 +2  as v1, v5 as v2, v6 as v3 from t1) as tx join [shuffle] t2 on tx.v1 = t2.v7;";
        String plan = getVerboseExplain(sql);
        plans.add(plan);

        // ==== except =====
        setTableStatistics(t2, 200000);
        setTableStatistics(t0, 800000);
        setTableStatistics(t1, 400000);
        sql =
                "select * from (select v1+1 as v1 ,v2,v3 from t0 except select v4 +2  as v1, v5 as v2, v6 as v3 from t1) as tx join [shuffle] t2 on tx.v1 = t2.v7;";
        plan = getVerboseExplain(sql);
        plans.add(plan);

        // ===== intersect =====
        setTableStatistics(t2, 200000);
        setTableStatistics(t0, 400000);
        setTableStatistics(t1, 400000);

        sql =
                "select * from (select v1+1 as v1 ,v2,v3 from t0 intersect select v4 +2  as v1, v5 as v2, v6 as v3 from t1) as tx join [shuffle] t2 on tx.v1 = t2.v7;";
        plan = getVerboseExplain(sql);
        plans.add(plan);

        setTableStatistics(t0, 10000);

        // === check union plan ====
        {
            String unionPlan = plans.get(0);
            assertContains(unionPlan, "  0:UNION\n" +
                    "  |  child exprs:\n" +
                    "  |      [4: expr, BIGINT, true] | [2: v2, BIGINT, true] | [3: v3, BIGINT, true]\n" +
                    "  |      [8: expr, BIGINT, true] | [6: v5, BIGINT, true] | [7: v6, BIGINT, true]\n" +
                    "  |  pass-through-operands: all\n");
            assertContains(unionPlan, "     cardinality: 360000\n" +
                    "     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (5: v4 + 2)\n");
            assertContains(unionPlan, "     cardinality: 360000\n" +
                    "     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (1: v1 + 1)");
        }
        // === check except plan ===
        {
            String exceptPlan = plans.get(1);
            Assert.assertTrue(exceptPlan.contains("  0:EXCEPT\n" +
                    "  |  child exprs:\n" +
                    "  |      [4: expr, BIGINT, true] | [2: v2, BIGINT, true] | [3: v3, BIGINT, true]\n" +
                    "  |      [8: expr, BIGINT, true] | [6: v5, BIGINT, true] | [7: v6, BIGINT, true]\n"));
            Assert.assertTrue(exceptPlan.contains("     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (5: v4 + 2)"));
            Assert.assertTrue(exceptPlan.contains("     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (1: v1 + 1)"));
        }
        // === check intersect plan ====
        {
            String intersectPlan = plans.get(2);
            Assert.assertTrue(intersectPlan.contains("  0:INTERSECT\n" +
                    "  |  child exprs:\n" +
                    "  |      [4: expr, BIGINT, true] | [2: v2, BIGINT, true] | [3: v3, BIGINT, true]\n" +
                    "  |      [8: expr, BIGINT, true] | [6: v5, BIGINT, true] | [7: v6, BIGINT, true]\n"));
            Assert.assertTrue(intersectPlan.contains("     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (5: v4 + 2)"));
            Assert.assertTrue(intersectPlan.contains("     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (1: v1 + 1)"));
        }
    }

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

        setTableStatistics(t1, 400000);
        setTableStatistics(t2, 100);

        String sql = " with cte as ( select v4, v5, v6 from t1 )\n" +
                " select count(*) from (\n" +
                "    select * from cte union all\n" +
                "    select cte.v4, cte.v5, cte.v6 from t2 join cte on cte.v4 = t2.v7 and t2.v8 < 10) as t\n";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  0:OlapScanNode\n" +
                "     table: t1, rollup: t1\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=1/1, tabletsRatio=3/3\n"));

        Assert.assertTrue(plan.contains("5:EXCHANGE\n" +
                "     cardinality: 400000\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: v4)"));

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
        assertContains(plan, " 1:Project\n" +
                "  |  <slot 18> : datediff(CAST('1981-09-06t03:40:33' AS DATETIME), CAST(11: L_SHIPDATE AS DATETIME))\n" +
                "  |  ");
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
                new ColumnStatistic(0.0, 10000, 0.0, 10, 10000));
        final List<ColumnStatistic> avgLowCardinality = ImmutableList.of(
                new ColumnStatistic(0.0, 10000, 0.0, 10, 100));
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
            plan = getFragmentPlan(sql);
            execPlan = getExecPlan(sql);
            olapScanNode = (OlapScanNode) execPlan.getScanNodes().get(0);
            Assert.assertEquals(0, olapScanNode.getBucketExprs().size());
            plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                    "  |  output: sum(2: v2)\n" +
                    "  |  group by: 2: v2\n" +
                    "  |  \n" +
                    "  1:EXCHANGE");

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
            assertContains(plan, "3:AGGREGATE (merge finalize)\n" +
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
            assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                    "  |  output: sum(2: v2)\n" +
                    "  |  group by: 2: v2\n" +
                    "  |  \n" +
                    "  1:EXCHANGE");

            // case 7: Plan with join cannot use one-phase local aggregation with local shuffle.
            isSingleBackendAndComputeNode.setRef(true);
            cardinality.setRef(avgHighCardinality);
            sql = "select count(1) from " +
                    "(select v2, sum(v2) from t0 group by v2) t1 join " +
                    "(select v2, sum(v2) from t0 group by v2) t2 on t1.v2=t2.v2";
            execPlan = getExecPlan(sql);
            plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, "  6:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 6: v2\n" +
                    "  |  \n" +
                    "  |----5:AGGREGATE (update finalize)\n" +
                    "  |    |  group by: 6: v2\n" +
                    "  |    |  \n" +
                    "  |    4:EXCHANGE\n" +
                    "  |    \n" +
                    "  2:AGGREGATE (update finalize)\n" +
                    "  |  group by: 2: v2\n" +
                    "  |  \n" +
                    "  1:EXCHANGE");
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
    public void testTimeOutOr50() throws Exception {
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
    }

    @Test
    public void testTimeOutOr15() throws Exception {
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
        getFragmentPlan(sql);
    }
    
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
    public void testPlanCost() throws Exception {
        String plan = getVerboseExplain("select t1a, v1 " +
                "from t0 join [broadcast] test_all_type " +
                "join [shuffle] (select 1 as v1_c1 where false) v1 on t1a=v1 and t1a=v1_c1");
        assertContains(plan, "  9:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [4: t1a, VARCHAR, true] = [16: cast, VARCHAR(1048576), true]\n" +
                "  |  output columns: 1, 4\n" +
                "  |  cardinality: 9000\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |       cardinality: 1\n" +
                "  |    \n" +
                "  6:EXCHANGE\n" +
                "     cardinality: 9000");

        AuditEvent event = connectContext.getAuditEventBuilder().build();
        Assert.assertTrue("planMemCosts should be > 1, but: " + event.planMemCosts, event.planMemCosts > 1);
        Assert.assertTrue("planCpuCosts should be > 1, but: " + event.planCpuCosts, event.planCpuCosts > 1);

    }
}
