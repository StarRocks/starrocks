package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedEnvPlanWithCostTest extends DistributedEnvPlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
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
        String planFragment = getFragmentPlan(sql);
        Assert.assertFalse(planFragment.contains("COLOCATE"));
    }

    @Test
    public void testCountDistinctWithGroupLowCountLow() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct P_TYPE) from part group by P_BRAND;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:AGGREGATE (update serialize)\n"
                + "  |  STREAMING\n"
                + "  |  output: multi_distinct_count(5: P_TYPE)"));
        Assert.assertTrue(planFragment.contains("3:AGGREGATE (merge finalize)\n"
                + "  |  output: multi_distinct_count(11: count(distinct 5: P_TYPE))"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctWithGroupLowCountHigh() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct P_PARTKEY) from part group by P_BRAND;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:AGGREGATE (update serialize)\n"
                + "  |  STREAMING\n"
                + "  |  output: multi_distinct_count(1: P_PARTKEY)"));
        Assert.assertTrue(planFragment.contains("3:AGGREGATE (merge finalize)\n"
                + "  |  output: multi_distinct_count(11: count(distinct 1: P_PARTKEY))"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctWithGroupHighCountLow() throws Exception {
        String sql = "select count(distinct P_SIZE) from part group by P_PARTKEY;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:AGGREGATE (update finalize)\n"
                + "  |  output: multi_distinct_count(6: P_SIZE)"));
    }

    @Test
    public void testCountDistinctWithGroupHighCountLowCHAR() throws Exception {
        String sql = "select count(distinct P_BRAND) from part group by P_PARTKEY;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:AGGREGATE (update serialize)\n"
                + "  |  STREAMING\n"
                + "  |  group by: 1: P_PARTKEY, 4: P_BRAND"));
        Assert.assertTrue(planFragment.contains("2:AGGREGATE (merge serialize)\n"
                + "  |  group by: 1: P_PARTKEY, 4: P_BRAND"));
        Assert.assertTrue(planFragment.contains("3:AGGREGATE (update finalize)\n"
                + "  |  output: count(4: P_BRAND)"));
    }

    @Test
    public void testCountDistinctWithGroupHighCountHigh() throws Exception {
        String sql = "select count(distinct P_NAME) from part group by P_PARTKEY;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:AGGREGATE (update serialize)\n"
                + "  |  STREAMING\n"
                + "  |  group by: 1: P_PARTKEY, 2: P_NAME"));
        Assert.assertTrue(planFragment.contains("  2:AGGREGATE (merge serialize)\n"
                + "  |  group by: 1: P_PARTKEY, 2: P_NAME"));
        Assert.assertTrue(planFragment.contains("  3:AGGREGATE (update finalize)\n"
                + "  |  output: count(2: P_NAME)"));
    }

    @Test
    public void testJoinDateAndDateTime() throws Exception {
        String sql = "select count(a.id_date) from test_all_type a " +
                "join test_all_type b on a.id_date = b.id_datetime ;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n"
                + "  |  <slot 22> : CAST(9: id_date AS DATETIME)\n"
                + "  |  <slot 9> : 9: id_date\n"));
    }

    @Test
    public void testJoinWithLimit2() throws Exception {
        String sql = "select n1.n_name as supp_nation, n2.n_name as cust_nation, " +
                "n2.n_nationkey, C_NATIONKEY from nation n1, nation n2, customer " +
                "where ((n1.n_name = 'CANADA' and n2.n_name = 'IRAN') " +
                "or (n1.n_name = 'IRAN' and n2.n_name = 'CANADA')) a" +
                "nd  c_nationkey = n2.n_nationkey limit 10;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("|  equal join conjunct: 14: C_NATIONKEY = 6: N_NATIONKEY\n"
                + "  |  limit: 10"));
    }

    @Test
    public void testSumDistinctConstant() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select sum(2), sum(distinct 2) from test_all_type;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("AGGREGATE (merge finalize)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctNull() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = " select COUNT( DISTINCT null) from test_all_type;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("AGGREGATE (merge finalize)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
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
        String planFragment = getFragmentPlan(sql);
    }

    @Test
    public void testCountStarWithoutGroupBy() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql =
                "select count(*) from lineorder_new_l where LO_ORDERDATE not in ('1998-04-10', '1994-04-11', '1994-04-12');";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("3:AGGREGATE (merge finalize)"));
        Assert.assertTrue(planFragment.contains("1:AGGREGATE (update serialize)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountStarWithNotEquals() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql =
                "select count(*) from lineorder_new_l where LO_ORDERDATE > '1994-01-01' and LO_ORDERDATE < '1995-01-01' and LO_ORDERDATE != '1994-04-11'";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("3:AGGREGATE (merge finalize)"));
        Assert.assertTrue(planFragment.contains("1:AGGREGATE (update serialize)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testLeftAntiJoinWithoutColumnStatistics() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql =
                "SELECT SUM(l_extendedprice) FROM lineitem LEFT ANTI JOIN dates_n ON l_suppkey=d_datekey AND d_daynuminmonth=10;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("6:AGGREGATE (update serialize)"));
        Assert.assertTrue(planFragment.contains("8:AGGREGATE (merge finalize)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctWithoutColumnStatistics() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql =
                "SELECT S_NATION, COUNT(distinct C_NAME) FROM lineorder_new_l where LO_LINENUMBER > 5 GROUP BY S_NATION";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("2:AGGREGATE (update serialize)"));
        Assert.assertTrue(planFragment.contains("4:AGGREGATE (merge finalize)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testBroadcastHint() throws Exception {
        String sql = "select l_orderkey from lineitem join[broadcast] orders where l_orderkey = o_orderkey";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("join op: INNER JOIN (BROADCAST)"));

        sql = "select l_orderkey from lineitem join[SHUFFLE] orders on l_orderkey = o_orderkey";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testBroadcastHintExceedMaxExecMemByte() throws Exception {
        long maxMemExec = connectContext.getSessionVariable().getMaxExecMemByte();
        connectContext.getSessionVariable().setMaxExecMemByte(200);
        String sql = "select l_orderkey from lineitem join[broadcast] orders where l_orderkey = o_orderkey";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("join op: INNER JOIN (BROADCAST)"));
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
        String planFragment = getCostExplain(sql);
        Assert.assertTrue(planFragment.contains("9:Project\n" +
                "  |  output columns:\n" +
                "  |  16 <-> [16: S_ACCTBAL, DOUBLE, false]\n" +
                "  |  1 <-> [1: P_PARTKEY, INT, false]\n" +
                "  |  17 <-> [17: S_COMMENT, VARCHAR, false]\n" +
                "  |  3 <-> [3: P_MFGR, VARCHAR, false]\n" +
                "  |  12 <-> [12: S_NAME, CHAR, false]\n" +
                "  |  13 <-> [13: S_ADDRESS, VARCHAR, false]\n" +
                "  |  15 <-> [15: S_PHONE, CHAR, false]\n" +
                "  |  cardinality: 400000"));
    }

    @Test
    public void testDistinctGroupByAggWithDefaultStatistics() throws Exception {
        String sql = "SELECT C_NATION, COUNT(distinct LO_CUSTKEY) FROM lineorder_new_l GROUP BY C_NATION;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge finalize)\n" +
                "  |  output: multi_distinct_count"));
        Assert.assertTrue(plan.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: multi_distinct_count"));
    }

    @Test
    public void testDistinctAggWithDefaultStatistics() throws Exception {
        String sql = "SELECT COUNT(distinct P_NAME) FROM lineorder_new_l;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("6:AGGREGATE (merge finalize)"));
        Assert.assertTrue(plan.contains("4:AGGREGATE (update serialize)"));
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge serialize)"));
        Assert.assertTrue(plan.contains("1:AGGREGATE (update serialize)"));
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
        System.out.println(plan);
        Assert.assertTrue(plan.contains("     probe runtime filters:\n"
                + "     - filter_id = 2, probe_expr = (11: L_SUPPKEY)\n"));
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
        checkTwoPhaseAgg(plan);

        sql = "select distinct join1.id from join1 join join2 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        checkOnePhaseAgg(plan);

        // cross join
        sql = "select distinct join2.id from join1 join join2 on join1.id = join2.id, baseall;";
        plan = getFragmentPlan(sql);
        checkTwoPhaseAgg(plan);

        connectContext.getSessionVariable().enableJoinReorder();
    }

    @Test(expected = NullPointerException.class)
    public void testSetVar() throws Exception {
        String sql = "explain select c2 from db1.tbl3;";
        String plan = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql);
        Assert.assertTrue(plan.contains("use vectorized: true"));

        sql = "explain select /*+ SET_VAR(enable_vectorized_engine=false) */c2 from db1.tbl3";
        plan = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql);
        Assert.assertTrue(plan.contains("use vectorized: false"));

        sql = "explain select c2 from db1.tbl3";
        plan = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql);
        Assert.assertTrue(plan.contains("use vectorized: true"));

        // will throw NullPointException
        sql = "explain select /*+ SET_VAR(enable_vectorized_engine=true, enable_cbo=true) */ c2 from db1.tbl3";
        plan = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, sql);
    }

    @Test
    public void testCountPruneJoinByProject() throws Exception {
        String sql = "select count(*) from customer join orders on C_CUSTKEY = O_CUSTKEY";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("5:AGGREGATE (update serialize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: \n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 1> : 1: C_CUSTKEY"));
        checkTwoPhaseAgg(plan);
    }

    @Test
    public void testCrossJoinPruneChildByProject() throws Exception {
        String sql = "SELECT t2.v7 FROM  t0 right SEMI JOIN t1 on t0.v1=t1.v4, t2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("7:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  4:Project\n" +
                "  |  <slot 1> : 1: v1"));
    }

    @Test
    public void testJoinOnExpression() throws Exception {
        String sql = "SELECT COUNT(*)  FROM lineitem JOIN [shuffle] orders ON l_orderkey = o_orderkey + 1  GROUP BY l_shipmode, l_shipinstruct, o_orderdate, o_orderstatus;";
        String plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [29: cast, BIGINT, true] = [30: add, BIGINT, true]\n" +
                "  |  cardinality: 600000000"));
        Assert.assertTrue(plan.contains("|----5:EXCHANGE\n" +
                "  |       cardinality: 150000000\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     cardinality: 600000000"));
        sql = "SELECT COUNT(*)  FROM lineitem JOIN orders ON l_orderkey * 2 = o_orderkey + 1  GROUP BY l_shipmode, l_shipinstruct, o_orderdate, o_orderstatus;";
        plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("equal join conjunct: [29: multiply, BIGINT, true] = [30: add, BIGINT, true]\n" +
                "  |  cardinality: 600000000"));
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
        Assert.assertTrue(plan.contains("Predicates: 24: N_NAME IN ('IRAN', 'CANADA')"));
        Assert.assertTrue(plan.contains("cardinality: 25"));
        // eval char/varchar type predicate cardinality in join node
        Assert.assertTrue(plan.contains(" 5:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates: ((19: N_NAME = 'CANADA') AND (24: N_NAME = 'IRAN')) OR ((19: N_NAME = 'IRAN') AND (24: N_NAME = 'CANADA'))\n" +
                "  |  cardinality: 2"));
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
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     table: nation, rollup: nation\n" +
                "     preAggregation: on\n" +
                "     Predicates: 23: N_NATIONKEY IN (2, 1)\n" +
                "     partitionsRatio=1/1, tabletsRatio=1/1\n" +
                "     tabletList=10185\n" +
                "     actualRows=0, avgRowSize=29.0\n" +
                "     cardinality: 2"));
        // eval predicate cardinality in join node
        Assert.assertTrue(plan.contains("3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates: ((18: N_NATIONKEY = 1) AND (23: N_NATIONKEY = 2)) OR ((18: N_NATIONKEY = 2) AND (23: N_NATIONKEY = 1))\n" +
                "  |  cardinality: 2"));
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
        Assert.assertTrue(plan.contains(" 11:AGGREGATE (update finalize)"));
        Assert.assertTrue(plan.contains("10:Project"));
    }

    @Test
    public void testGroupByDistributedColumnWithMultiPartitions() throws Exception {
        String sql = "select k1, sum(k2) from pushdown_test group by k1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:AGGREGATE (update serialize)"));
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge finalize)"));
    }
}
