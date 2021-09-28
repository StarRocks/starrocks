// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
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
                + "  |  output: sum(4: sum(2: v2))\n"
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
                + "  |  output: sum(5: sum(1: v1))\n"
                + "  |  group by: 2: v2"));
        Assert.assertTrue(planFragment.contains("  6:AGGREGATE (merge finalize)\n"
                + "  |  output: count(4: count(distinct 2: v2)), sum(5: sum(1: v1))\n"
                + "  |  group by: \n"
                + "  |  use vectorized: true"));
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
        Assert.assertTrue(planFragment.contains("  2:AGGREGATE (update finalize)\n"
                + "  |  output: multi_distinct_count(2: v2)\n"
                + "  |  group by: 3: v3"));
    }

    @Test
    public void testPredicateRewrittenByProjectWithLowCardinality(
            @Mocked MockTpchStatisticStorage mockedStatisticStorage)
            throws Exception {
        new Expectations() {
            {
                mockedStatisticStorage.getColumnStatistics((Table) any, ImmutableList.of(V2, V3));
                result = ImmutableList.of(new ColumnStatistic(0.0, 100, 0.0, 10, 10),
                        new ColumnStatistic(0.0, 100, 0.0, 10, 10));
            }
        };
        String sql = "SELECT -v3 from t0 group by v3, v2 having -v3 < 63;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  4:Project\n"
                + "  |  <slot 4> : -1 * 3: v3"));
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
        Assert.assertTrue(planFragment.contains("  3:Project\n"
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
        connectContext.getSessionVariable().setPreferJoinMethod("shuffle");
        String sql = "SELECT v2,t1d from t0 join test_all_type on t0.v2 = test_all_type.t1d ;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  4:HASH JOIN\n"
                + "  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 2: v2 = 7: t1d\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----3:EXCHANGE\n"
                + "  |       use vectorized: true\n"
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
        Catalog.getCurrentSystemInfo().dropBackend(10002);
        Catalog.getCurrentSystemInfo().dropBackend(10003);
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
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |       use vectorized: true\n"
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
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |       use vectorized: true\n"
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
                "  |  use vectorized: true\n" +
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
        Assert.assertTrue(planFragment.contains("OUTPUT EXPRS:16: count(distinct 7: k7) | 17: count(distinct 8: k8)"));
        Assert.assertTrue(planFragment.contains("14: mv_bitmap_union_k7"));
        Assert.assertTrue(planFragment.contains("15: mv_bitmap_union_k8"));
        Assert.assertTrue(planFragment.contains("rollup: bitmap_mv"));
    }

    @Test
    public void testReplicatedJoin() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);
        String sql = "select s_name, s_address from supplier, nation where s_suppkey in " +
                "( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like 'forest%' ) and ps_availqty > " +
                "( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and " +
                "l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year ) ) " +
                "and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name;";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains(" 3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (REPLICATED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: S_NATIONKEY = 9: N_NATIONKEY"));
        Assert.assertTrue(plan.contains("19:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: S_SUPPKEY = 15: PS_SUPPKEY"));
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
        Assert.assertTrue(plan.contains(" * GROUPING_ID-->[0.0, 3.0, 0.0, 8.0, 4.0]\n" +
                "  |  * GROUPING-->[0.0, 3.0, 0.0, 8.0, 4.0]"));

        sql = "select v1, v2, grouping_id(v1,v2), SUM(v3) from t0 group by rollup(v1, v2)";
        plan = getCostExplain(sql);
        // check scan node
        Assert.assertTrue(plan.contains("cardinality: 10000"));
        // check repeat node
        Assert.assertTrue(plan.contains("cardinality: 30000"));
        Assert.assertTrue(plan.contains("* GROUPING_ID-->[0.0, 3.0, 0.0, 8.0, 3.0]\n" +
                "  |  * GROUPING-->[0.0, 3.0, 0.0, 8.0, 3.0]"));
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
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  use vectorized: true\n" +
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
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  1:REPEAT_NODE\n" +
                "  |  repeat: repeat 1 lines [[], [1]]\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON"));

        sql = "select SUM(v3) from t0 group by grouping sets(())";
        plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  3:EXCHANGE\n" +
                "     use vectorized: true\n" +
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
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  1:REPEAT_NODE"));
    }
}
