// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedEnvPlanWithCostTest extends DistributedEnvPlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        Config.tablet_sched_disable_colocate_overall_balance = true;
    }

    @After
    public void after() {
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    //  agg
    //   |
    // project
    //   |
    //  scan
    // should not change agg node hash distribution source type
    @Test
    public void testAggChangeHashDistributionSourceType() throws Exception {
        String sql = "select o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from orders, lineitem where" +
                " o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 315) and " +
                " o_orderkey = l_orderkey group by o_orderkey, o_orderdate, o_totalprice;";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "COLOCATE");
    }

    @Test
    public void testCountDistinctWithGroupLowCountLow() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct P_TYPE) from part group by P_BRAND;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: multi_distinct_count(5: P_TYPE)\n" +
                "  |  group by: 4: P_BRAND");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctWithGroupLowCountHigh() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct P_PARTKEY) from part group by P_BRAND;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: P_PARTKEY)\n" +
                "  |  group by: 4: P_BRAND\n" +
                "  |  \n" +
                "  2:AGGREGATE (merge finalize)\n" +
                "  |  group by: 4: P_BRAND, 1: P_PARTKEY\n" +
                "  |  \n" +
                "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 4: P_BRAND, 1: P_PARTKEY");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctWithGroupHighCountLow() throws Exception {
        String sql = "select count(distinct P_SIZE) from part group by P_PARTKEY;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n"
                + "  |  output: multi_distinct_count(6: P_SIZE)");
    }

    @Test
    public void testCountDistinctWithGroupHighCountLowCHAR() throws Exception {
        String sql = "select count(distinct P_BRAND) from part group by P_PARTKEY;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(4: P_BRAND)");
    }

    @Test
    public void testCountDistinctWithGroupHighCountHigh() throws Exception {
        String sql = "select count(distinct P_NAME) from part group by P_PARTKEY;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: multi_distinct_count(2: P_NAME)\n" +
                "  |  group by: 1: P_PARTKEY");
    }

    @Test
    public void testCountDistinctWithoutGroupBy() throws Exception {
        String sql = "SELECT COUNT (DISTINCT l_partkey) FROM lineitem";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 2: L_PARTKEY");
        assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                "  |  output: count(2: L_PARTKEY)\n" +
                "  |  group by: ");
    }

    @Test
    public void testJoinDateAndDateTime() throws Exception {
        String sql = "select count(a.id_date) from test_all_type a " +
                "join test_all_type b on a.id_date = b.id_datetime ;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n"
                + "  |  <slot 9> : 9: id_date\n"
                + "  |  <slot 22> : CAST(9: id_date AS DATETIME)\n");
    }

    @Test
    public void testJoinWithLimit2() throws Exception {
        String sql = "select n1.n_name as supp_nation, n2.n_name as cust_nation, " +
                "n2.n_nationkey, C_NATIONKEY from nation n1, nation n2, customer " +
                "where ((n1.n_name = 'CANADA' and n2.n_name = 'IRAN') " +
                "or (n1.n_name = 'IRAN' and n2.n_name = 'CANADA')) a" +
                "nd  c_nationkey = n2.n_nationkey limit 10;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "|  equal join conjunct: 14: C_NATIONKEY = 6: N_NATIONKEY\n"
                + "  |  limit: 10");
    }

    @Test
    public void testSumDistinctConstant() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select sum(2), sum(distinct 2) from test_all_type;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE (merge finalize)");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctNull() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = " select COUNT( DISTINCT null) from test_all_type;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE (merge finalize)");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testInsert() throws Exception {
        String sql = "insert into test_all_type select * from test_all_type limit 5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: t1a | 2: t1b | 3: t1c | 4: t1d | 5: t1e | 6: t1f | 7: t1g " +
                "| 8: id_datetime | 9: id_date | 10: id_decimal\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  OLAP TABLE SINK\n" +
                "    TABLE: test_all_type\n" +
                "    TUPLE ID: 1\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:EXCHANGE\n" +
                "     limit: 5\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: test_all_type\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: test_all_type\n" +
                "     tabletRatio=3/3\n");

        sql = "insert into test_all_type select * from test_all_type";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: t1a | 2: t1b | 3: t1c | 4: t1d | 5: t1e | 6: t1f | 7: t1g " +
                "| 8: id_datetime | 9: id_date | 10: id_decimal\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  OLAP TABLE SINK\n" +
                "    TABLE: test_all_type\n" +
                "    TUPLE ID: 1\n" +
                "    RANDOM\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: test_all_type\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: test_all_type\n" +
                "     tabletRatio=3/3\n");

        sql = "insert into test_all_type(t1a,t1b) select t1a,t1b from test_all_type limit 5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: t1a | 2: t1b | 11: expr | 12: expr | 13: expr | 14: expr " +
                "| 15: expr | 16: expr | 17: expr | 18: expr\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  OLAP TABLE SINK\n" +
                "    TABLE: test_all_type\n" +
                "    TUPLE ID: 2\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:EXCHANGE\n" +
                "     limit: 5\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: t1a\n" +
                "  |  <slot 2> : 2: t1b\n" +
                "  |  <slot 11> : NULL\n" +
                "  |  <slot 12> : NULL\n" +
                "  |  <slot 13> : NULL\n" +
                "  |  <slot 14> : NULL\n" +
                "  |  <slot 15> : NULL\n" +
                "  |  <slot 16> : NULL\n" +
                "  |  <slot 17> : NULL\n" +
                "  |  <slot 18> : NULL\n" +
                "  |  limit: 5\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: test_all_type\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: test_all_type\n" +
                "     tabletRatio=3/3\n");
    }

    @Test
    public void testJoin() throws Exception {
        String sql = "select\n" +
                "        SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as amount\n" +
                "from\n" +
                "        part,\n" +
                "        partsupp,\n" +
                "        lineitem\n" +
                "where\n" +
                "        p_partkey = cast(l_partkey as bigint)\n" +
                "        and ps_suppkey = l_suppkey\n" +
                "        and ps_partkey = l_partkey        \n" +
                "        and p_name like '%peru%';";
        String plan = getFragmentPlan(sql);
    }

    @Test
    public void testCountStarWithoutGroupBy() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql =
                "select count(*) from lineorder_new_l where LO_ORDERDATE not in ('1998-04-10', '1994-04-11', '1994-04-12');";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (merge finalize)");
        assertContains(plan, "2:AGGREGATE (update serialize)");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountStarWithNotEquals() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql =
                "select count(*) from lineorder_new_l where LO_ORDERDATE > '1994-01-01' " +
                        "and LO_ORDERDATE < '1995-01-01' and LO_ORDERDATE != '1994-04-11'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (merge finalize)");
        assertContains(plan, "2:AGGREGATE (update serialize)");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testLeftAntiJoinWithoutColumnStatistics() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql =
                "SELECT SUM(l_extendedprice) FROM lineitem " +
                        "LEFT ANTI JOIN dates_n ON l_suppkey=d_datekey AND d_daynuminmonth=10;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:AGGREGATE (update serialize)");
        assertContains(plan, "8:AGGREGATE (merge finalize)");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctWithoutColumnStatistics() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql =
                "SELECT S_NATION, COUNT(distinct C_NAME) FROM lineorder_new_l where LO_LINENUMBER > 5 GROUP BY S_NATION";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update serialize)");
        assertContains(plan, "4:AGGREGATE (merge finalize)");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testBroadcastHint() throws Exception {
        String sql = "select l_orderkey from lineitem join[broadcast] orders where l_orderkey = o_orderkey";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN (BROADCAST)");

        sql = "select l_orderkey from lineitem join[SHUFFLE] orders on l_orderkey = o_orderkey";
        plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN (PARTITIONED)");
    }

    @Test
    public void testBroadcastHintExceedMaxExecMemByte() throws Exception {
        long maxMemExec = connectContext.getSessionVariable().getMaxExecMemByte();
        connectContext.getSessionVariable().setMaxExecMemByte(200);
        String sql = "select l_orderkey from lineitem join[broadcast] orders where l_orderkey = o_orderkey";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN (BROADCAST)");
        connectContext.getSessionVariable().setMaxExecMemByte(maxMemExec);
    }

    @Test
    public void testProjectCardinality() throws Exception {
        String sql = "select\n" +
                "    s_acctbal,\n" +
                "    s_name,\n" +
                "    p_partkey,\n" +
                "    p_mfgr,\n" +
                "    s_address,\n" +
                "    s_phone,\n" +
                "    s_comment\n" +
                "from\n" +
                "    part,\n" +
                "    supplier,\n" +
                "    partsupp \n" +
                "where\n" +
                "      p_partkey = ps_partkey\n" +
                "  and s_suppkey = ps_suppkey\n" +
                "  and p_size = 12\n" +
                "  and p_type like '%COPPER'";
        String plan = getCostExplain(sql);
        assertContains(plan, "9:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: P_PARTKEY, INT, false]\n" +
                "  |  3 <-> [3: P_MFGR, VARCHAR, false]\n" +
                "  |  12 <-> [12: S_NAME, CHAR, false]\n" +
                "  |  13 <-> [13: S_ADDRESS, VARCHAR, false]\n" +
                "  |  15 <-> [15: S_PHONE, CHAR, false]\n" +
                "  |  16 <-> [16: S_ACCTBAL, DOUBLE, false]\n" +
                "  |  17 <-> [17: S_COMMENT, VARCHAR, false]\n" +
                "  |  cardinality: 400000");
    }

    @Test
    public void testDistinctGroupByAggWithDefaultStatistics() throws Exception {
        String sql = "SELECT C_NATION, COUNT(distinct LO_CUSTKEY) FROM lineorder_new_l GROUP BY C_NATION;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 4: LO_CUSTKEY, 21: C_NATION");
        assertContains(plan, "3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 4: LO_CUSTKEY, 21: C_NATION");
    }

    @Test
    public void testDistinctAggWithDefaultStatistics() throws Exception {
        String sql = "SELECT COUNT(distinct P_NAME) FROM lineorder_new_l;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:AGGREGATE (merge finalize)");
        assertContains(plan, "4:AGGREGATE (update serialize)");
        assertContains(plan, "3:AGGREGATE (merge serialize)");
        assertContains(plan, "1:AGGREGATE (update serialize)");
    }

    @Test
    public void testTPCHQ7GlobalRuntimeFilter() throws Exception {
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);
        String sql = "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as "
                + "supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, "
                + "l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, "
                + "nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey"
                + " = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1"
                + ".n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = "
                + "'FRANCE') ) and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping "
                + "group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year;";
        String plan = getCostExplain(sql);
        assertContains(plan, "build_expr = (1: S_SUPPKEY)");
    }

    @Test
    public void testAggPreferTwoPhaseWithDefaultColumnStatistics() throws Exception {
        String sql = "select t1d, t1e, sum(t1c) from test_all_type group by t1d, t1e";
        String costPlan = getCostExplain(sql);
        Assert.assertTrue(costPlan.contains("3:AGGREGATE (merge finalize)"));
    }

    @Test
    public void testAggPreferTwoPhaseWithDefaultColumnStatistics2() throws Exception {
        String sql = "select t1d, t1e, sum(t1c), avg(t1c) from test_all_type group by t1d, t1e";
        String costPlan = getCostExplain(sql);
        Assert.assertTrue(costPlan.contains("3:AGGREGATE (merge finalize)"));
    }

    private void checkTwoPhaseAgg(String planString) {
        Assert.assertTrue(planString.contains("AGGREGATE (merge finalize)"));
    }

    private void checkOnePhaseAgg(String planString) {
        Assert.assertTrue(planString.contains("AGGREGATE (update finalize)"));
    }

    @Test
    public void testAggregateCompatPartition() throws Exception {
        connectContext.getSessionVariable().disableJoinReorder();

        // Left outer join
        String sql = "select distinct join1.id from join1 left join join2 on join1.id = join2.id;";
        String plan = getFragmentPlan(sql);
        checkOnePhaseAgg(plan);

        sql = "select distinct join2.id from join1 left join join2 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        checkTwoPhaseAgg(plan);

        sql = "select distinct join2.id + join1.id as a from join1 left join join2 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        checkTwoPhaseAgg(plan);

        // Right outer join
        sql = "select distinct join2.id from join1 right join join2 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        checkTwoPhaseAgg(plan);

        sql = "select distinct join1.id from join1 right join join2 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        checkTwoPhaseAgg(plan);

        // Full outer join
        sql = "select distinct join2.id from join1 full join join2 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        checkTwoPhaseAgg(plan);

        sql = "select distinct join1.id from join1 full join join2 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        checkTwoPhaseAgg(plan);

        // Inner join
        sql = "select distinct join2.id from join1 join join2 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        checkOnePhaseAgg(plan);

        sql = "select distinct join1.id from join1 join join2 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        checkOnePhaseAgg(plan);

        // cross join
        sql = "select distinct join2.id from join1 join join2 on join1.id = join2.id, baseall;";
        plan = getFragmentPlan(sql);
        checkOnePhaseAgg(plan);

        connectContext.getSessionVariable().enableJoinReorder();
    }

    @Test
    public void testSetVar() throws Exception {
        String sql = "explain select c2 from db1.tbl3;";
        String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);

        sql = "explain select /*+ SET_VAR(enable_vectorized_engine=false) */c2 from db1.tbl3";
        plan = UtFrameUtils.getFragmentPlan(connectContext, sql);

        sql = "explain select c2 from db1.tbl3";
        plan = UtFrameUtils.getFragmentPlan(connectContext, sql);

        sql = "explain select /*+ SET_VAR(enable_vectorized_engine=true, enable_cbo=true) */ c2 from db1.tbl3";
        plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
    }

    @Test
    public void testCountPruneJoinByProject() throws Exception {
        String sql = "select count(*) from customer join orders on C_CUSTKEY = O_CUSTKEY";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:AGGREGATE (update serialize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 22> : 1");
        checkTwoPhaseAgg(plan);
    }

    @Test
    public void testCrossJoinPruneChildByProject() throws Exception {
        String sql = "SELECT t2.v7 FROM  t0 right SEMI JOIN t1 on t0.v1=t1.v4, t2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  7:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n");
    }

    @Test
    public void testJoinOnExpression() throws Exception {
        String sql =
                "SELECT COUNT(*)  FROM lineitem JOIN [shuffle] orders ON l_orderkey = o_orderkey + 1 " +
                        "GROUP BY l_shipmode, l_shipinstruct, o_orderdate, o_orderstatus;";
        String plan = getCostExplain(sql);
        assertContains(plan, "6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [29: cast, BIGINT, true] = [30: add, BIGINT, true]\n" +
                "  |  output columns: 14, 15, 20, 22\n" +
                "  |  cardinality: 600000000");
        assertContains(plan, "|----5:EXCHANGE\n" +
                "  |       cardinality: 150000000\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     cardinality: 600000000");
        sql =
                "SELECT COUNT(*)  FROM lineitem JOIN orders ON l_orderkey * 2 = o_orderkey + 1 " +
                        "GROUP BY l_shipmode, l_shipinstruct, o_orderdate, o_orderstatus;";
        plan = getCostExplain(sql);
        Assert.assertTrue(
                plan.contains("equal join conjunct: [29: multiply, BIGINT, true] = [30: add, BIGINT, true]\n" +
                        "  |  output columns: 14, 15, 20, 22\n" +
                        "  |  cardinality: 600000000"));
    }

    @Test
    public void testAddProjectForJoinPrune() throws Exception {
        String sql = "select\n" +
                "    c_custkey,\n" +
                "    c_name,\n" +
                "    sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
                "    c_acctbal,\n" +
                "    n_name,\n" +
                "    c_address,\n" +
                "    c_phone,\n" +
                "    c_comment\n" +
                "from\n" +
                "    customer,\n" +
                "    orders,\n" +
                "    lineitem,\n" +
                "    nation\n" +
                "where\n" +
                "        c_custkey = o_custkey\n" +
                "  and l_orderkey = o_orderkey\n" +
                "  and o_orderdate >= date '1994-05-01'\n" +
                "  and o_orderdate < date '1994-08-01'\n" +
                "  and l_returnflag = 'R'\n" +
                "  and c_nationkey = n_nationkey\n" +
                "group by\n" +
                "    c_custkey,\n" +
                "    c_name,\n" +
                "    c_acctbal,\n" +
                "    c_phone,\n" +
                "    n_name,\n" +
                "    c_address,\n" +
                "    c_comment\n" +
                "order by\n" +
                "    revenue desc limit 20;";
        String plan = getCostExplain(sql);
        assertContains(plan, "7:Project\n" +
                "  |  output columns:\n" +
                "  |  11 <-> [11: O_CUSTKEY, INT, false]\n" +
                "  |  25 <-> [25: L_EXTENDEDPRICE, DOUBLE, false]\n" +
                "  |  26 <-> [26: L_DISCOUNT, DOUBLE, false]\n" +
                "  |  cardinality: 7650728\n" +
                "  |  column statistics: \n" +
                "  |  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5738045.738045738] ESTIMATE\n" +
                "  |  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE\n" +
                "  |  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE\n" +
                "  |  \n" +
                "  6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [20: L_ORDERKEY, INT, false] = [10: O_ORDERKEY, INT, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (10: O_ORDERKEY), remote = false\n" +
                "  |  output columns: 11, 25, 26\n" +
                "  |  cardinality: 7650728\n" +
                "  |  column statistics: \n" +
                "  |  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 5738045.738045738] ESTIMATE\n" +
                "  |  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5738045.738045738] ESTIMATE\n" +
                "  |  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 5738045.738045738] ESTIMATE\n" +
                "  |  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE\n" +
                "  |  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE");
    }

    @Test
    public void testNotEvalStringTypePredicateCardinality() throws Exception {
        String sql = "select\n" +
                "            n1.n_name as supp_nation,\n" +
                "            n2.n_name as cust_nation\n" +
                "        from\n" +
                "            supplier,\n" +
                "            customer,\n" +
                "            nation n1,\n" +
                "            nation n2\n" +
                "        where \n" +
                "          s_nationkey = n1.n_nationkey\n" +
                "          and c_nationkey = n2.n_nationkey\n" +
                "          and (\n" +
                "                (n1.n_name = 'CANADA' and n2.n_name = 'IRAN')\n" +
                "                or (n1.n_name = 'IRAN' and n2.n_name = 'CANADA')\n" +
                "            )";
        String plan = getCostExplain(sql);
        // not eval char/varchar type predicate cardinality in scan node
        assertContains(plan, "Predicates: 24: N_NAME IN ('IRAN', 'CANADA')");
        assertContains(plan, "cardinality: 25");
        // eval char/varchar type predicate cardinality in join node
        assertContains(plan, "  5:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: ((19: N_NAME = 'CANADA') AND (24: N_NAME = 'IRAN')) " +
                "OR ((19: N_NAME = 'IRAN') AND (24: N_NAME = 'CANADA'))\n" +
                "  |  cardinality: 1\n");

    }

    @Test
    public void testEvalPredicateCardinality() throws Exception {
        String sql = "select\n" +
                "            n1.n_name as supp_nation,\n" +
                "            n2.n_name as cust_nation\n" +
                "        from\n" +
                "            supplier,\n" +
                "            customer,\n" +
                "            nation n1,\n" +
                "            nation n2\n" +
                "        where \n" +
                "          s_nationkey = n1.n_nationkey\n" +
                "          and c_nationkey = n2.n_nationkey\n" +
                "          and (\n" +
                "                (n1.n_nationkey = 1 and n2.n_nationkey = 2)\n" +
                "                or (n1.n_nationkey = 2 and n2.n_nationkey = 1)\n" +
                "            )";
        String plan = getCostExplain(sql);

        // eval predicate cardinality in scan node
        assertContains(plan, "4:OlapScanNode\n" +
                "     table: nation, rollup: nation\n" +
                "     preAggregation: on\n" +
                "     Predicates: 18: N_NATIONKEY IN (1, 2)\n" +
                "     partitionsRatio=1/1, tabletsRatio=1/1");
        // eval predicate cardinality in join node
        assertContains(plan, "6:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: ((18: N_NATIONKEY = 1) AND (23: N_NATIONKEY = 2)) " +
                "OR ((18: N_NATIONKEY = 2) AND (23: N_NATIONKEY = 1))\n" +
                "  |  cardinality: 1");
    }

    @Test
    public void testOneAggUponBroadcastJoinWithoutExchange() throws Exception {
        String sql = "select \n" +
                "  sum(p1) \n" +
                "from \n" +
                "  (\n" +
                "    select \n" +
                "      t0.p1 \n" +
                "    from \n" +
                "      (\n" +
                "        select \n" +
                "          count(n1.P_PARTKEY) as p1 \n" +
                "        from \n" +
                "          part n1\n" +
                "      ) t0 \n" +
                "      join (\n" +
                "        select \n" +
                "          count(n2.P_PARTKEY) as p2 \n" +
                "        from \n" +
                "          part n2\n" +
                "      ) t1\n" +
                "  ) t2;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "12:AGGREGATE (update finalize)\n" +
                "  |  output: sum(11: count)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  11:Project\n" +
                "  |  <slot 11> : 11: count");
    }

    @Test
    public void testGroupByDistributedColumnWithMultiPartitions() throws Exception {
        String sql = "select k1, sum(k2) from pushdown_test group by k1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update serialize)");
        assertContains(plan, "3:AGGREGATE (merge finalize)");
    }

    // check outer join with isNull predicate on inner table
    // The estimate cardinality of join should not be 0.
    @Test
    public void testOuterJoinWithIsNullPredicate() throws Exception {
        // test left outer join
        String sql = "select ps_partkey,ps_suppkey from partsupp left outer join part on " +
                "ps_partkey = p_partkey where p_partkey is null";
        String plan = getCostExplain(sql);
        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [1: PS_PARTKEY, INT, false] = [7: P_PARTKEY, INT, true]\n" +
                "  |  other predicates: 7: P_PARTKEY IS NULL\n" +
                "  |  output columns: 1, 2\n" +
                "  |  cardinality: 8000000");
        // test right outer join
        sql = "select ps_partkey,ps_suppkey from partsupp right outer join part on " +
                "ps_partkey = p_partkey where ps_partkey is null";
        plan = getCostExplain(sql);
        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [1: PS_PARTKEY, INT, true] = [7: P_PARTKEY, INT, false]\n" +
                "  |  other predicates: 1: PS_PARTKEY IS NULL\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (7: P_PARTKEY), remote = false\n" +
                "  |  output columns: 1, 2\n" +
                "  |  cardinality: 8000000");
        // test full outer join
        sql = "select ps_partkey,ps_suppkey from partsupp full outer join part on " +
                "ps_partkey = p_partkey where ps_partkey is null";
        plan = getCostExplain(sql);
        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: FULL OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  equal join conjunct: [1: PS_PARTKEY, INT, true] = [7: P_PARTKEY, INT, true]\n" +
                "  |  other predicates: 1: PS_PARTKEY IS NULL\n" +
                "  |  output columns: 1, 2\n" +
                "  |  cardinality: 4000000");
    }

    @Test
    public void testDateFunctionCardinality() throws Exception {
        String sql = "SELECT Month(P_PARTKEY) AS month FROM part GROUP BY month ORDER BY month DESC LIMIT 5";
        String plan = getCostExplain(sql);
        assertContains(plan, "* month-->[1.0, 12.0, 0.0, 1.0, 12.0]");
        assertContains(plan, "2:AGGREGATE (update serialize)");
        assertContains(plan, "4:AGGREGATE (merge finalize)");

        sql = "SELECT day(P_PARTKEY) AS day FROM part GROUP BY day ORDER BY day DESC LIMIT 5";
        plan = getCostExplain(sql);
        assertContains(plan, "day-->[1.0, 31.0, 0.0, 1.0, 31.0]");
        assertContains(plan, "2:AGGREGATE (update serialize)");
        assertContains(plan, "4:AGGREGATE (merge finalize)");

        sql = "SELECT hour(P_PARTKEY) AS hour FROM part GROUP BY hour ORDER BY hour DESC LIMIT 5";
        plan = getCostExplain(sql);
        assertContains(plan, " hour-->[0.0, 23.0, 0.0, 1.0, 24.0]");
        assertContains(plan, "2:AGGREGATE (update serialize)");
        assertContains(plan, "4:AGGREGATE (merge finalize)");

        sql = "SELECT minute(P_PARTKEY) AS minute FROM part GROUP BY minute ORDER BY minute DESC LIMIT 5";
        plan = getCostExplain(sql);
        assertContains(plan, "minute-->[0.0, 59.0, 0.0, 1.0, 60.0]");
        assertContains(plan, "2:AGGREGATE (update serialize)");
        assertContains(plan, "4:AGGREGATE (merge finalize)");

        sql = "SELECT second(P_PARTKEY) AS second FROM part GROUP BY second ORDER BY second DESC LIMIT 5";
        plan = getCostExplain(sql);
        assertContains(plan, "* second-->[0.0, 59.0, 0.0, 1.0, 60.0]");
        assertContains(plan, "2:AGGREGATE (update serialize)");
        assertContains(plan, "4:AGGREGATE (merge finalize)");
    }

    @Test
    public void testDateFunctionBinaryPredicate() throws Exception {
        // check cardinality is not 0
        String sql = "SELECT sum(L_DISCOUNT * L_TAX) AS revenue FROM lineitem WHERE weekofyear(L_RECEIPTDATE) = 6";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "cardinality=11111111");

        sql = "SELECT sum(L_DISCOUNT * L_TAX) AS revenue FROM lineitem WHERE weekofyear(L_RECEIPTDATE) in (6)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "cardinality=11111111");
    }

    @Test
    public void testColumnNotEqualsConstant() throws Exception {
        // check cardinality not 0
        String sql =
                "select S_SUPPKEY,S_NAME from supplier where s_name <> 'Supplier#000000050' and s_name >= 'Supplier#000000086'";
        String plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 500000");

        // test_all_type column statistics are unknown
        sql = "select t1a,t1b from test_all_type where t1a <> 'xxx'";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 3000000");

        sql = "select t1b,t1c from test_all_type where t1c <> 10";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 3000000");

        sql = "select t1b,t1c from test_all_type where id_date <> '2020-01-01'";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 3000000");
    }

    @Test
    public void testLocalAggregationUponJoin() throws Exception {
        // check that local aggregation is set colocate which upon join with colocate table
        String sql =
                "select tbl5.c2,sum(tbl5.c3) from db1.tbl5 join[broadcast] db1.tbl4 on tbl5.c2 = tbl4.c2 group by tbl5.c2;";
        Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        ExecPlan execPlan = pair.second;
        Assert.assertTrue(execPlan.getFragments().get(1).getPlanRoot() instanceof AggregationNode);
        AggregationNode aggregationNode = (AggregationNode) execPlan.getFragments().get(1).getPlanRoot();
        Assert.assertTrue(aggregationNode.isColocate());
    }

    @Test
    public void testCountFunctionColumnStatistics() throws Exception {
        String sql = "select count(S_SUPPKEY) from supplier";
        String plan = getCostExplain(sql);
        assertContains(plan, "* count-->[0.0, 1000000.0, 0.0, 8.0, 1.0] ESTIMATE");
        sql = "select count(*) from supplier";
        plan = getCostExplain(sql);
        assertContains(plan, "count-->[0.0, 1000000.0, 0.0, 8.0, 1.0] ESTIMATE");
    }

    @Test
    public void testGenRuntimeFilterWhenRightJoin() throws Exception {
        String sql = "select * from lineitem right anti join [shuffle] part on lineitem.l_partkey = part.p_partkey";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: RIGHT ANTI JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [2: L_PARTKEY, INT, false] = [18: P_PARTKEY, INT, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (18: P_PARTKEY), remote = true");
        sql = "select * from lineitem right semi join [shuffle] part on lineitem.l_partkey = part.p_partkey";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: RIGHT SEMI JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [2: L_PARTKEY, INT, false] = [18: P_PARTKEY, INT, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (18: P_PARTKEY), remote = true");
        sql = "select * from lineitem right outer join [shuffle] part on lineitem.l_partkey = part.p_partkey";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [2: L_PARTKEY, INT, true] = [18: P_PARTKEY, INT, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (18: P_PARTKEY), remote = true");
    }

    @Test
    public void testCaseWhenCardinalityEstimate() throws Exception {
        String sql = "select (case when `O_ORDERKEY` = 0 then 'ALGERIA' when `O_ORDERKEY` = 1 then 'ARGENTINA' " +
                "else 'others' end) a from orders group by 1";
        String plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 3");
        assertContains(plan, "* case-->[-Infinity, Infinity, 0.0, 16.0, 3.0]");

        sql = "select (case when `O_ORDERKEY` = 0 then 'ALGERIA' when `O_ORDERKEY` = 1 then 'ARGENTINA' end) a " +
                "from orders group by 1";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 2");
        assertContains(plan, "* case-->[-Infinity, Infinity, 0.0, 16.0, 2.0]");

        sql = "select (case when `O_ORDERKEY` = 0 then O_ORDERSTATUS when `O_ORDERKEY` = 1 then 'ARGENTINA' " +
                "else 'other' end) a from orders group by 1";
        plan = getCostExplain(sql);
        assertContains(plan, "* case-->[-Infinity, Infinity, 0.0, 16.0, 5.0] ESTIMATE");
    }

    @Test
    public void testIFFunctionCardinalityEstimate() throws Exception {
        String sql = "select (case when `O_ORDERKEY` = 0 then 'ALGERIA' else 'others' end) a from orders group by 1";
        String plan = getCostExplain(sql);
        assertContains(plan, "* case-->[-Infinity, Infinity, 0.0, 16.0, 2.0] ESTIMATE");

        sql = "select if(`O_ORDERKEY` = 0, 'ALGERIA', 'others') a from orders group by 1";
        plan = getCostExplain(sql);
        assertContains(plan, "* if-->[-Infinity, Infinity, 0.0, 16.0, 2.0] ESTIMATE");

        sql = "select if(`O_ORDERKEY` = 0, 'ALGERIA', " +
                "if (`O_ORDERKEY` = 1, 'ARGENTINA', 'others')) a from orders group by 1";
        plan = getCostExplain(sql);
        assertContains(plan, "* if-->[-Infinity, Infinity, 0.0, 16.0, 3.0] ESTIMATE");

        sql = "select if(`O_ORDERKEY` = 0, 'ALGERIA', if (`O_ORDERKEY` = 1, 'ARGENTINA', " +
                "if(`O_ORDERKEY` = 2, 'BRAZIL', 'Others'))) a from orders group by 1";
        plan = getCostExplain(sql);
        assertContains(plan, "* if-->[-Infinity, Infinity, 0.0, 16.0, 4.0] ESTIMATE");
    }

    @Test
    public void testPartitionColumnColumnStatistics() throws Exception {
        String sql =
                "select l_shipdate, count(1) from lineitem_partition where l_shipdate = '1992-01-01' group by l_shipdate";
        String plan = getCostExplain(sql);
        // check L_SHIPDATE is not unknown
        Assert.assertTrue(
                plan.contains("* L_SHIPDATE-->[6.941952E8, 6.941952E8, 0.0, 4.0, 360.85714285714283] ESTIMATE"));

        sql = "select count(1) from lineitem_partition where l_shipdate = '1992-01-01'";
        plan = getCostExplain(sql);
        Assert.assertTrue(
                plan.contains("* L_SHIPDATE-->[6.941952E8, 6.941952E8, 0.0, 4.0, 360.85714285714283] ESTIMATE"));
    }

    @Test
    public void testCastDatePredicate() throws Exception {
        OlapTable lineitem =
                (OlapTable) connectContext.getGlobalStateMgr().getDb("test").getTable("lineitem");

        MockTpchStatisticStorage mock = new MockTpchStatisticStorage(100);
        connectContext.getGlobalStateMgr().setStatisticStorage(mock);

        // ===========================
        // To handle cast(int) in normal range
        String sql = "select L_PARTKEY from lineitem where year(L_PARTKEY) = 1998";
        new Expectations(mock) {
            {
                mock.getColumnStatistic(lineitem, "L_PARTKEY");
                result = new ColumnStatistic(19921212, 19980202, 0, 8, 20000);
            }
        };

        String plan = getCostExplain(sql);
        assertContains(plan, "     column statistics: \n" +
                "     * L_PARTKEY-->[1.9921212E7, 1.9980202E7, 0.0, 8.0, 20000.0] ESTIMATE");

        // ===========================
        // To handle cast(int) in infinity range
        sql = "select L_PARTKEY from lineitem where year(L_PARTKEY) = 1998";
        new Expectations(mock) {
            {
                mock.getColumnStatistic(lineitem, "L_PARTKEY");
                result = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 20000);
            }
        };

        plan = getCostExplain(sql);
        assertContains(plan, "     column statistics: \n" +
                "     * L_PARTKEY-->[-Infinity, Infinity, 0.0, 8.0, 20000.0] ESTIMATE");

        // ===========================
        // To handle cast(date) in normal range
        sql = "select L_SHIPDATE from lineitem where year(L_SHIPDATE) = 1998";
        new Expectations(mock) {
            {
                mock.getColumnStatistic(lineitem, "L_SHIPDATE");
                result = new ColumnStatistic(19921212, 19980202, 0, 8, 20000);
            }
        };

        plan = getCostExplain(sql);
        assertContains(plan, "     column statistics: \n" +
                "     * L_SHIPDATE-->[1.9921212E7, 1.9980202E7, 0.0, 8.0, 1.0] ESTIMATE");

        // ===========================
        // To handle cast(date) in infinity range
        new Expectations(mock) {
            {
                mock.getColumnStatistic(lineitem, "L_SHIPDATE");
                result = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 20000);
            }
        };

        plan = getCostExplain(sql);
        assertContains(plan, "     column statistics: \n" +
                "     * L_SHIPDATE-->[-Infinity, Infinity, 0.0, 8.0, 20000.0] ESTIMATE");

        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(100));
    }

    @Test
    public void testInPredicateStatisticsEstimate() throws Exception {
        String sql = "select * from lineitem where L_LINENUMBER in (1,2,3,4)";
        String plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 342857143");
        assertContains(plan, "* L_LINENUMBER-->[1.0, 4.0, 0.0, 4.0, 4.0] ESTIMATE");

        sql = "select * from lineitem where L_LINENUMBER in (1+1,2+1,3+1,4+1)";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 342857143");
        assertContains(plan, "* L_LINENUMBER-->[2.0, 5.0, 0.0, 4.0, 4.0] ESTIMATE");

        sql = "select * from lineitem where L_LINENUMBER in (L_RETURNFLAG+1,2+1)";
        plan = getCostExplain(sql);
        Assert.assertTrue("plan is " + plan,
                plan.contains("* L_LINENUMBER-->[1.0, 7.0, 0.0, 4.0, 3.0] ESTIMATE"));

        sql = "select * from lineitem where L_LINENUMBER + 1 in (L_RETURNFLAG+1,2+1)";
        plan = getCostExplain(sql);
        assertContains(plan, "* L_LINENUMBER-->[1.0, 7.0, 0.0, 4.0, 7.0] ESTIMATE");
        // check without column statistics
        sql = "select * from test_all_type where t1b in (1,2,3,4)";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 3000000");

        sql = "select * from test_all_type where t1b in (1+1,2+1,3+1,4+1)";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 3000000");

        sql = "select * from test_all_type where t1b+1 in (1+1,2+1,3+1,4+1)";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 3000000");
        // check not in
        sql = "select * from lineitem where L_LINENUMBER not in (1,2,3,4)";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 257142857");
        assertContains(plan, " * L_LINENUMBER-->[1.0, 7.0, 0.0, 4.0, 7.0] ESTIMATE");

        sql = "select * from lineitem where L_LINENUMBER not in (1+1,2+1,3+1,4+1)";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 257142857");
        assertContains(plan, " * L_LINENUMBER-->[1.0, 7.0, 0.0, 4.0, 7.0] ESTIMATE");

        sql = "select * from lineitem where L_LINENUMBER + 1 not in (1+1,2+1,3+1,4+1)";
        plan = getCostExplain(sql);
        assertContains(plan, "cardinality: 3000000");
        assertContains(plan, " * L_LINENUMBER-->[1.0, 7.0, 0.0, 4.0, 7.0] ESTIMATE");
    }

    @Test
    public void testPruneAggNode() throws Exception {
        ConnectContext.get().getSessionVariable().setNewPlanerAggStage(3);
        String sql = "select count(distinct C_NAME) from customer group by C_CUSTKEY;";
        ExecPlan plan = getExecPlan(sql);
        Assert.assertTrue(plan.getFragments().get(1).isAssignScanRangesPerDriverSeq());
        assertContains(plan.getExplainString(TExplainLevel.NORMAL), "2:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: C_NAME)\n" +
                "  |  group by: 1: C_CUSTKEY\n" +
                "  |  \n" +
                "  1:AGGREGATE (update serialize)\n" +
                "  |  group by: 1: C_CUSTKEY, 2: C_NAME");

        ConnectContext.get().getSessionVariable().setNewPlanerAggStage(4);
        sql = "select count(distinct C_CUSTKEY) from customer;";
        plan = getExecPlan(sql);
        Assert.assertTrue(plan.getFragments().get(1).isAssignScanRangesPerDriverSeq());
        assertContains(plan.getExplainString(TExplainLevel.NORMAL), " 2:AGGREGATE (update serialize)\n" +
                "  |  output: count(1: C_CUSTKEY)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:AGGREGATE (update serialize)\n" +
                "  |  group by: 1: C_CUSTKEY");

        ConnectContext.get().getSessionVariable().setNewPlanerAggStage(0);

        sql = "select count(distinct C_CUSTKEY, C_NAME) from customer;";
        plan = getExecPlan(sql);
        Assert.assertTrue(plan.getFragments().get(1).isAssignScanRangesPerDriverSeq());
        assertContains(plan.getExplainString(TExplainLevel.NORMAL), " 2:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(1: C_CUSTKEY IS NULL, NULL, 2: C_NAME))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:AGGREGATE (update serialize)\n" +
                "  |  group by: 1: C_CUSTKEY, 2: C_NAME");

        sql = "select count(distinct C_CUSTKEY, C_NAME) from customer group by C_CUSTKEY;";
        plan = getExecPlan(sql);
        Assert.assertTrue(plan.getFragments().get(1).isAssignScanRangesPerDriverSeq());
        assertContains(plan.getExplainString(TExplainLevel.NORMAL), "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(1: C_CUSTKEY IS NULL, NULL, 2: C_NAME))\n" +
                "  |  group by: 1: C_CUSTKEY\n" +
                "  |  \n" +
                "  1:AGGREGATE (update serialize)\n" +
                "  |  group by: 1: C_CUSTKEY, 2: C_NAME");
    }

    @Test
    public void testAggNodeAndBucketDistribution() throws Exception {
        // For the local one-phase aggregation, enable AssignScanRangesPerDriverSeq and disable SharedScan.
        String sql = "select count(1) from customer group by C_CUSTKEY";
        ExecPlan plan = getExecPlan(sql);
        PlanFragment fragment = plan.getFragments().get(1);
        Assert.assertTrue(fragment.isAssignScanRangesPerDriverSeq());
        assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "  1:AGGREGATE (update finalize)\n" +
                "  |  output: count(1)\n" +
                "  |  group by: 1: C_CUSTKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        // For the none-local one-phase aggregation, disable AssignScanRangesPerDriverSeq and enable SharedScan.
        ConnectContext.get().getSessionVariable().setNewPlanerAggStage(1);
        sql = "select count(1) from customer group by C_NAME";
        plan = getExecPlan(sql);
        fragment = plan.getFragments().get(2);
        Assert.assertFalse(fragment.isAssignScanRangesPerDriverSeq());
        assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 2: C_NAME\n" +
                "\n" +
                "  0:OlapScanNode");

        // For the two-phase aggregation, disable AssignScanRangesPerDriverSeq and enable SharedScan.
        ConnectContext.get().getSessionVariable().setNewPlanerAggStage(2);
        sql = "select count(1) from customer group by C_NAME";
        plan = getExecPlan(sql);
        fragment = plan.getFragments().get(2);
        Assert.assertFalse(fragment.isAssignScanRangesPerDriverSeq());
        assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1)\n" +
                "  |  group by: 2: C_NAME\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        // For the none-pruned four-phase aggregation, disable AssignScanRangesPerDriverSeq and enable SharedScan.
        ConnectContext.get().getSessionVariable().setNewPlanerAggStage(0);
        sql = "select count(distinct C_ADDRESS) from customer group by C_NAME";
        plan = getExecPlan(sql);
        fragment = plan.getFragments().get(2);
        Assert.assertFalse(fragment.isAssignScanRangesPerDriverSeq());
        assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 2: C_NAME, 3: C_ADDRESS\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        // For the none-pruned three-phase aggregation, disable AssignScanRangesPerDriverSeq and enable SharedScan.
        sql = "select count(distinct C_ADDRESS) from customer";
        plan = getExecPlan(sql);
        System.out.println(plan.getExplainString(TExplainLevel.NORMAL));
        fragment = plan.getFragments().get(2);
        Assert.assertFalse(fragment.isAssignScanRangesPerDriverSeq());
        assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 3: C_ADDRESS\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testCastDistributionPrune() throws Exception {
        String sql =
                "select * from test_all_type_distributed_by_datetime where cast(id_datetime as date) >= '1970-01-01' " +
                        "and cast(id_datetime as date) <= '1970-01-01'";
        String plan = getFragmentPlan(sql);
        // check not prune tablet
        assertContains(plan, "tabletRatio=3/3");

        sql =
                "select * from test_all_type_distributed_by_datetime where cast(id_datetime as datetime) >= '1970-01-01' " +
                        "and cast(id_datetime as datetime) <= '1970-01-01'";
        plan = getFragmentPlan(sql);
        // check prune tablet
        assertContains(plan, "tabletRatio=1/3");

        sql = "select * from test_all_type_distributed_by_date where cast(id_date as datetime) >= '1970-01-01' " +
                "and cast(id_date as datetime) <= '1970-01-01'";
        plan = getFragmentPlan(sql);
        // check not prune tablet
        assertContains(plan, "tabletRatio=1/3");

        sql = "select * from test_all_type_distributed_by_date where cast(id_date as date) >= '1970-01-01' " +
                "and cast(id_date as date) <= '1970-01-01'";
        plan = getFragmentPlan(sql);
        // check prune tablet
        assertContains(plan, "tabletRatio=1/3");
    }

    @Test
    public void testCastPartitionPrune() throws Exception {
        String sql = "select * from test_all_type_partition_by_datetime where cast(id_datetime as date) = '1991-01-01'";
        String plan = getFragmentPlan(sql);
        // check prune partition
        assertContains(plan, "partitions=2/3");

        sql =
                "select * from test_all_type_partition_by_date where cast(id_date as datetime) >= '1991-01-01 00:00:00' " +
                        "and cast(id_date as datetime) < '1992-01-01 12:00:00'";
        plan = getFragmentPlan(sql);
        // check prune partition
        assertContains(plan, "partitions=2/3");
    }

    @Test
    public void testTimestampPartitionPruneOptimize() throws Exception {
        String sql =
                "select * from test_partition_prune_optimize_by_date where dt >= timestamp('2021-12-27 00:00:00.123456') " +
                        "and dt <= timestamp('2021-12-29 00:00:00.123456')";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "partitions=2/4");

        sql =
                "select * from test_partition_prune_optimize_by_date where dt > timestamp('2021-12-27 00:00:00.123456') " +
                        "and dt < timestamp('2021-12-29 00:00:00.123456')";
        plan = getFragmentPlan(sql);
        assertContains(plan, "partitions=2/4");

        sql =
                "select * from test_partition_prune_optimize_by_date where dt >= timestamp('2021-12-27 00:00:00.0') " +
                        "and dt <= timestamp('2021-12-29 00:00:00.0')";
        plan = getFragmentPlan(sql);
        assertContains(plan, "partitions=3/4");

        sql =
                "select * from test_partition_prune_optimize_by_date where dt > timestamp('2021-12-27 00:00:00.0') " +
                        "and dt < timestamp('2021-12-29 00:00:00.0')";
        plan = getFragmentPlan(sql);
        assertContains(plan, "partitions=1/4");
    }

    @Test
    public void testJoinDifferentAggChild1() throws Exception {
        String sql = "select * from (select v2, sum(v3) as x3 from t0 group by v2) j0 join " +
                "(select v3, sum(v2) as x2 from t0 group by v3) j1 on j0.v2 = j1.v3 and j0.x3 = j1.x2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)");
    }

    @Test
    public void testJoinDifferentAggChild2() throws Exception {
        String sql = "select * from t0 j0 join " +
                "(select v3, sum(v2) as x2 from t0 group by v3) j1 on j0.v3 = j1.v3 and j0.v2 = j1.x2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    HASH_PARTITIONED: 6: v3, 7: sum\n" +
                "\n" +
                "  5:AGGREGATE (merge finalize)");
    }

    @Test
    public void testJoinDifferentAggChild3() throws Exception {
        String sql = "select * from (select v2, sum(v3) as x3 from t0 group by v2) j0 join " +
                "(select v3, sum(v2) as x2 from t0 group by v3) j1 on j0.v2 = j1.v3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  equal join conjunct: 2: v2 = 7: v3\n" +
                "  |  \n" +
                "  |----7:AGGREGATE (merge finalize)\n" +
                "  |    |  output: sum(8: sum)\n" +
                "  |    |  group by: 7: v3\n" +
                "  |    |  \n" +
                "  |    6:EXCHANGE\n" +
                "  |    \n" +
                "  3:AGGREGATE (merge finalize)");
    }

    @Test
    public void testOneStageAggJoinChild() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        String sql = "select xx.v2, sum(xx.v3) from (select * from t0 join t1 on t0.v2 = t1.v5) as xx group by xx.v2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)");
    }

    @Test
    public void testStringMinMaxPredicate() throws Exception {
        String sql = "select C_CUSTKEY, Min(C_NAME) as min, Max(C_NAME) as max " +
                "from customer group by C_CUSTKEY having min > '123'";
        String costPlan = getCostExplain(sql);

        assertContains(costPlan, "* min-->[-Infinity, Infinity, 0.0, 25.0, 7500000.0] ESTIMATE\n" +
                "  |  * max-->[-Infinity, Infinity, 0.0, 25.0, 7500000.0] ESTIMATE");

        sql = "select C_CUSTKEY, Min(C_NAME) as min, Max(C_NAME) as max from customer group by C_CUSTKEY having max < '1234'";
        costPlan = getCostExplain(sql);
        assertContains(costPlan, " * min-->[-Infinity, Infinity, 0.0, 25.0, 7500000.0] ESTIMATE\n" +
                "  |  * max-->[-Infinity, Infinity, 0.0, 25.0, 7500000.0] ESTIMATE");

        sql = "select Min(C_NAME) as min, Max(C_NAME) as max from customer having max < '1234'";
        costPlan = getCostExplain(sql);
        assertContains(costPlan, "* min-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE\n" +
                "  |  * max-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE");
        assertContains(costPlan, "having: [11: max, VARCHAR, true] < '1234'\n" +
                "  |  cardinality: 1");
    }

    @Test
    public void testSQLSmithGenQuery() throws Exception {
        String sql = "select * from " +
                "  (select  \n" +
                "          ref_0.id_decimal as c0, \n" +
                "          ref_0.t1c as c1, \n" +
                "          ref_0.id_datetime as c2\n" +
                "        from \n" +
                "          test_all_type as ref_0\n" +
                "        where cast(null as DOUBLE) <= ref_0.t1f) as subq_0\n" +
                "    left join part as ref_1\n" +
                "    on (subq_0.c0 = ref_1.P_RETAILPRICE )\n" +
                "where subq_0.c1 <> subq_0.c1\n" +
                "limit 63;";
        ExecPlan execPlan = getExecPlan(sql);
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        // check without error
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 18: P_RETAILPRICE = 21: cast\n" +
                "  |  limit: 63");
    }

    @Test
    public void testBucketShuffleJoinWindowFunction() throws Exception {
        String sql = "select \n" +
                "  subq_1.c3 as c2, \n" +
                "  ref_1.c_address as c3 \n" +
                "from \n" +
                "  (\n" +
                "    select \n" +
                "      subq_0.c0 as c2, \n" +
                "      subq_0.c0 as c3, \n" +
                "      avg(\n" +
                "        cast(null as DOUBLE)\n" +
                "      ) over (\n" +
                "        partition by subq_0.c0, \n" +
                "        subq_0.c0 \n" +
                "        order by \n" +
                "          subq_0.c0\n" +
                "      ) as c4 \n" +
                "    from \n" +
                "      (\n" +
                "        select \n" +
                "          ref_0.o_custkey as c0 \n" +
                "        from \n" +
                "          orders as ref_0 \n" +
                "        where \n" +
                "          ref_0.o_totalprice > ref_0.o_totalprice\n" +
                "      ) as subq_0 \n" +
                "    where \n" +
                "      null\n" +
                "  ) as subq_1 \n" +
                "  left join customer as ref_1 on (subq_1.c2 = ref_1.c_custkey) \n" +
                "where \n" +
                "  subq_1.c4 >= subq_1.c4;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  8:Project\n" +
                "  |  <slot 2> : 2: O_CUSTKEY\n" +
                "  |  <slot 14> : 14: C_ADDRESS\n" +
                "  |  \n" +
                "  7:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 12: C_CUSTKEY = 2: O_CUSTKEY");
    }

    @Test
    public void testMultiTableCrossJoin() throws Exception {
        String sql = "select * from lineitem a,lineitem b,lineitem c,lineitem d,lineitem e,lineitem f";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n");
    }

    @Test
    public void testDisableOneStageAggWithDistinct() throws Exception {
        String sql = "select count(distinct L_ORDERKEY) from lineitem group by L_PARTKEY";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: L_PARTKEY, 1: L_ORDERKEY\n");

        sql = "select count(distinct P_NAME) from part";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 2: P_NAME");
    }

    @Test
    public void testManyCrossJoin() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000000);
        String sql = "select t1.LO_ORDERKEY from lineorder_new_l t1 join lineorder_new_l t2 " +
                "join lineorder_new_l t3 join lineorder_new_l t4 join lineorder_new_l t5 join lineorder_new_l t6 " +
                "join lineorder_new_l t7 join lineorder_new_l t8 join lineorder_new_l t9 join lineorder_new_l t10 " +
                "join lineorder_new_l t11 join lineorder_new_l t12 join lineorder_new_l t13 join lineorder_new_l t14 " +
                "join lineorder_new_l t15 join lineorder_new_l t16 join lineorder_new_l t17 join lineorder_new_l t18 " +
                "join lineorder_new_l t19 join lineorder_new_l t20 join lineorder_new_l t21 join lineorder_new_l t22 " +
                "join lineorder_new_l t23 join lineorder_new_l t24 join lineorder_new_l t25 join lineorder_new_l t26 " +
                "join lineorder_new_l t27 join lineorder_new_l t28 join lineorder_new_l t29 join lineorder_new_l t30 " +
                "join lineorder_new_l t31 join lineorder_new_l t32 join lineorder_new_l t33 join lineorder_new_l t34 " +
                "join lineorder_new_l t35 join lineorder_new_l t36 join lineorder_new_l t37 join lineorder_new_l t38";
        String plan = getCostExplain(sql);
        assertContains(plan, "150:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: LO_ORDERKEY, INT, false]\n" +
                "  |  cardinality: 9223372036854775807");
    }

    @Test
    public void testAggExecuteInOneTablet() throws Exception {
        String sql;
        String plan;

        // dates_n only contains one tablet.
        sql = "select count(d_datekey), d_date from dates_n group by d_date";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "LocalShuffleColumns");
        assertContains(plan, "  1:AGGREGATE (update finalize)");
        assertContains(plan, "  0:OlapScanNode");

        // dates_n only contains one tablet.
        sql = "select count(d_date), d_datekey from dates_n group by d_datekey";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "LocalShuffleColumns");
        assertContains(plan, "  1:AGGREGATE (update finalize)");
        assertContains(plan, "  0:OlapScanNode");

        // dates_n only contains one tablet.
        //       HashJoin(Colocate)
        //      /               \
        // OlapScanNode       BlockingAgg
        //                       |
        //                    OlapScanNode
        sql = "WITH w1 as (SELECT count(d_date) as cnt, d_datekey FROM dates_n group by d_datekey) " +
                "SELECT * from dates_n join [colocate] w1 using(d_datekey)";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "LocalShuffleColumns");
        assertContains(plan, "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: [19: d_datekey, INT, true] = [36: d_datekey, INT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (36: d_datekey), remote = false\n" +
                "  |  cardinality: 2300\n" +
                "  |  \n" +
                "  |----2:AGGREGATE (update finalize)");

        // lineorder_new_l contains more than one tablet.
        sql = "select count(P_TYPE), LO_ORDERKEY from lineorder_new_l group by LO_ORDERKEY";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "     LocalShuffleColumns:\n" +
                "     - 1: LO_ORDERKEY");
        assertContains(plan, "  1:AGGREGATE (update finalize)");
        assertContains(plan, "  0:OlapScanNode");
    }

    @Test
    public void testValidateJoinReorderPlan() throws Exception {
        String sql = "select  \n" +
                "  ref_0.N_NAME as c0, \n" +
                "  case when ref_3.C_NATIONKEY > ref_3.C_ACCTBAL then ref_1.O_ORDERPRIORITY else " +
                "ref_1.O_SHIPPRIORITY end as c1\n" +
                "from \n" +
                "  nation as ref_0 inner join nation n1 on ref_0.N_NAME = n1.N_NAME" +
                "  join nation n2 on ref_0.N_NAME = n2.N_NAME join nation n3 on ref_0.N_NAME = n3.N_NAME" +
                "  join nation n4 on ref_0.N_NAME = n4.N_NAME" +
                "            inner join orders as ref_1\n" +
                "            on (ref_0.N_REGIONKEY = ref_1.O_CUSTKEY )\n" +
                "          left join t2 as ref_2\n" +
                "          on (ref_0.N_NATIONKEY = ref_2.v8 )\n" +
                "        inner join customer as ref_3\n" +
                "        on (ref_1.O_ORDERPRIORITY = ref_3.C_NAME )\n" +
                "      inner join supplier as ref_4\n" +
                "      on (ref_1.O_CLERK = ref_4.S_PHONE )\n" +
                "    left join supplier as ref_5\n" +
                "    on (ref_2.v9 = ref_5.S_SUPPKEY )\n" +
                "where cast(coalesce(ref_3.C_MKTSEGMENT, ref_0.N_NAME) as VARCHAR) = ref_5.S_ADDRESS";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "39:Project\n" +
                "  |  <slot 2> : 2: N_NAME\n" +
                "  |  <slot 64> : if(CAST(42: C_NATIONKEY AS DOUBLE) > 44: C_ACCTBAL, 31: O_ORDERPRIORITY, " +
                "CAST(33: O_SHIPPRIORITY AS VARCHAR(15)))\n" +
                "  |  \n" +
                "  38:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 65: cast = 38: v9\n" +
                "  |  equal join conjunct: 58: S_ADDRESS = 66: coalesce");
    }

    @Test
    public void testNdvInOrPredicate() throws Exception {
        String sql = "select count(*) from customer where C_NAME = 'b' or C_NATIONKEY = 1";
        String plan = getCostExplain(sql);
        assertCContains(plan, "C_NAME-->[-Infinity, Infinity, 0.0, 25.0, 600000.0] ESTIMATE",
                "C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE");
    }
}
