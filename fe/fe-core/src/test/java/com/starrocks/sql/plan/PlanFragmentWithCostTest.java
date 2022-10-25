// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MockTpchStatisticStorage;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

public class PlanFragmentWithCostTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        FeConstants.default_scheduler_interval_millisecond = 1;

        Catalog catalog = connectContext.getCatalog();
        OlapTable table2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("test_all_type");
        setTableStatistics(table2, 10000);

        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(t0, 10000);

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
        Catalog catalog = connectContext.getCatalog();
        OlapTable table2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("test_all_type");
        setTableStatistics(table2, 20000000);
        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(t0, 20000000);

        String sql = "select t1a,v1 from test_all_type, t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("3:CROSS JOIN"));

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
        System.out.println(planFragment);
        Assert.assertTrue(planFragment.contains("  2:AGGREGATE (update finalize)\n"
                + "  |  output: multi_distinct_count(2: v2)\n"
                + "  |  group by: 3: v3"));
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
        Catalog catalog = connectContext.getCatalog();
        OlapTable table1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(table1, 10000);
        OlapTable table2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("test_all_type");
        setTableStatistics(table2, 5000);
        String sql = "SELECT v2,t1d from t0 join test_all_type on t0.v2 = test_all_type.t1d ;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  4:HASH JOIN\n"
                + "  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  hash predicates:\n"
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
                + "  |  hash predicates:\n"
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
        Catalog catalog = connectContext.getCatalog();
        OlapTable table = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(table, 1000);
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d ;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:HASH JOIN\n"
                + "  |  join op: INNER JOIN (BROADCAST)\n"
                + "  |  hash predicates:\n"
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

    // todo(ywb) disable replicate join temporarily
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
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: PS_PARTKEY = 20: P_PARTKEY"));
        Assert.assertTrue(plan.contains("  14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
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
        System.out.println(plan);
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
        Catalog catalog = connectContext.getCatalog();
        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(t0, 10000000);
        OlapTable t1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t1");
        setTableStatistics(t1, 10000);

        String sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 and t0.v1 = 1 and t1.v5 = 2";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("TPlanNode(node_id:3, node_type:HASH_JOIN_NODE"));
        Assert.assertTrue(plan.contains("local_rf_waiting_set:[3]"));
    }

    @Test
    public void testIntersectReorder() throws Exception {
        // check cross join generate plan without exception
        Catalog catalog = connectContext.getCatalog();
        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(t0, 1000);
        OlapTable t1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t1");
        setTableStatistics(t1, 100);
        OlapTable t2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t2");
        setTableStatistics(t2, 1);

        String sql = "select v1 from t0 intersect select v7 from t2 intersect select v4 from t1";
        String planFragment = getFragmentPlan(sql);
        System.out.println(planFragment);
        Assert.assertTrue(planFragment.contains("  0:INTERSECT\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));
        Assert.assertTrue(planFragment.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    HASH_PARTITIONED: <slot 4>\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        setTableStatistics(t0, 10000);
    }

    @Test
    public void testNotPushDownRuntimeFilterToSortNode() throws Exception {
        Catalog catalog = connectContext.getCatalog();
        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(t0, 10);

        OlapTable t1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t1");
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
        Catalog catalog = connectContext.getCatalog();

        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(t0, 1000);
        OlapTable t1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t1");
        setTableStatistics(t1, 100);
        OlapTable t2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t2");

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
            Assert.assertTrue(unionPlan.contains("  0:UNION\n" +
                    "  |  child exprs:\n" +
                    "  |      [4, BIGINT, true] | [2, BIGINT, true] | [3, BIGINT, true]\n" +
                    "  |      [8, BIGINT, true] | [6, BIGINT, true] | [7, BIGINT, true]\n" +
                    "  |  pass-through-operands: all\n" +
                    "  |  cardinality: 800000"));
            Assert.assertTrue(unionPlan.contains("  4:OlapScanNode\n" +
                    "     table: t1, rollup: t1\n" +
                    "     preAggregation: on\n" +
                    "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                    "     tabletList=10015,10017,10019\n" +
                    "     actualRows=0, avgRowSize=4.0\n" +
                    "     cardinality: 400000\n" +
                    "     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (5: v4 + 2)"));
            Assert.assertTrue(unionPlan.contains("  1:OlapScanNode\n" +
                    "     table: t0, rollup: t0\n" +
                    "     preAggregation: on\n" +
                    "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                    "     tabletList=10006,10008,10010\n" +
                    "     actualRows=0, avgRowSize=4.0\n" +
                    "     cardinality: 400000\n" +
                    "     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (1: v1 + 1)"));
        }
        // === check except plan ===
        {
            String exceptPlan = plans.get(1);
            Assert.assertTrue(exceptPlan.contains("  0:EXCEPT\n" +
                    "  |  child exprs:\n" +
                    "  |      [4, BIGINT, true] | [2, BIGINT, true] | [3, BIGINT, true]\n" +
                    "  |      [8, BIGINT, true] | [6, BIGINT, true] | [7, BIGINT, true]\n" +
                    "  |  cardinality: 800000"));
            Assert.assertTrue(exceptPlan.contains("  4:OlapScanNode\n" +
                    "     table: t1, rollup: t1\n" +
                    "     preAggregation: on\n" +
                    "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                    "     tabletList=10015,10017,10019\n" +
                    "     actualRows=0, avgRowSize=4.0\n" +
                    "     cardinality: 400000\n" +
                    "     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (5: v4 + 2)"));
            Assert.assertTrue(exceptPlan.contains("  1:OlapScanNode\n" +
                    "     table: t0, rollup: t0\n" +
                    "     preAggregation: on\n" +
                    "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                    "     tabletList=10006,10008,10010\n" +
                    "     actualRows=0, avgRowSize=4.0\n" +
                    "     cardinality: 800000\n" +
                    "     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (1: v1 + 1)"));
        }
        // === check intersect plan ====
        {
            String intersectPlan = plans.get(2);
            Assert.assertTrue(intersectPlan.contains("  0:INTERSECT\n" +
                    "  |  child exprs:\n" +
                    "  |      [4, BIGINT, true] | [2, BIGINT, true] | [3, BIGINT, true]\n" +
                    "  |      [8, BIGINT, true] | [6, BIGINT, true] | [7, BIGINT, true]\n" +
                    "  |  cardinality: 400000"));
            Assert.assertTrue(intersectPlan.contains("  4:OlapScanNode\n" +
                    "     table: t1, rollup: t1\n" +
                    "     preAggregation: on\n" +
                    "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                    "     tabletList=10015,10017,10019\n" +
                    "     actualRows=0, avgRowSize=4.0\n" +
                    "     cardinality: 400000\n" +
                    "     probe runtime filters:\n" +
                    "     - filter_id = 0, probe_expr = (5: v4 + 2)"));
            Assert.assertTrue(intersectPlan.contains("  1:OlapScanNode\n" +
                    "     table: t0, rollup: t0\n" +
                    "     preAggregation: on\n" +
                    "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                    "     tabletList=10006,10008,10010\n" +
                    "     actualRows=0, avgRowSize=4.0\n" +
                    "     cardinality: 400000\n" +
                    "     probe runtime filters:\n" +
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
        Catalog catalog = connectContext.getCatalog();
        connectContext.getSessionVariable().setCboCteReuse(true);
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);

        OlapTable t1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t1");
        OlapTable t2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t2");

        setTableStatistics(t1, 400000);
        setTableStatistics(t2, 100);

        String sql = " with cte as ( select v4, v5, v6 from t1 )\n" +
                " select count(*) from (\n" +
                "    select * from cte union all\n" +
                "    select cte.v4, cte.v5, cte.v6 from t2 join cte on cte.v4 = t2.v7 and t2.v8 < 10) as t\n";
        String plan = getVerboseExplain(sql);

        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     table: t1, rollup: t1\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=1/1, tabletsRatio=3/3\n" +
                "     tabletList=10015,10017,10019\n" +
                "     actualRows=0, avgRowSize=1.0\n" +
                "     cardinality: 400000"));

        Assert.assertTrue(plan.contains("5:EXCHANGE\n" +
                "     cardinality: 400000\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: v4)"));

        Assert.assertTrue(plan.contains("  10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [10: v4, BIGINT, true] = [7: v7, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (7: v7), remote = false\n" +
                "  |  output columns: 10\n" +
                "  |  cardinality: 400000"));

        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setEnablePipelineEngine(false);
    }


    @Test
    public void testSubqueryLimit1() throws Exception {
        String sql = "select sum(v2) from (select * from t0 limit 1) x where 1=1";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  2:SELECT\n" +
                "  |  predicates: TRUE\n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 1"));
    }

    @Test
    public void testSubqueryLimit2() throws Exception {
        String sql = "select sum(v2) over (partition by v3) from (select * from t0 limit 1) x ";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 1\n" +
                "\n" +
                "PLAN FRAGMENT 2"));
    }

    @Test
    public void testSubqueryLimit3() throws Exception {
        String sql = "select v2 from (select * from t0 limit 1) x where 1=1 limit 2";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  2:SELECT\n" +
                "  |  predicates: TRUE\n" +
                "  |  limit: 2\n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 1\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:"));
    }
}
