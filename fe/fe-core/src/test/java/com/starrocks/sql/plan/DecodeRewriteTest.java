// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DecodeRewriteTest extends PlanTestBase{
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE supplier_nullable ( S_SUPPKEY     INTEGER NOT NULL,\n" +
                "                             S_NAME        CHAR(25) NOT NULL,\n" +
                "                             S_ADDRESS     VARCHAR(40), \n" +
                "                             S_NATIONKEY   INTEGER NOT NULL,\n" +
                "                             S_PHONE       CHAR(15) NOT NULL,\n" +
                "                             S_ACCTBAL     double NOT NULL,\n" +
                "                             S_COMMENT     VARCHAR(101) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_suppkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE part_v2  ( P_PARTKEY     INTEGER NOT NULL,\n" +
                "                          P_NAME        VARCHAR(55) NOT NULL,\n" +
                "                          P_MFGR        VARCHAR(25) NOT NULL,\n" +
                "                          P_BRAND       VARCHAR(10) NOT NULL,\n" +
                "                          P_TYPE        VARCHAR(25) NOT NULL,\n" +
                "                          P_SIZE        INTEGER NOT NULL,\n" +
                "                          P_CONTAINER   VARCHAR(10) NOT NULL,\n" +
                "                          P_RETAILPRICE double NOT NULL,\n" +
                "                          P_COMMENT     VARCHAR(23) NOT NULL,\n" +
                "                          PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");


        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        FeConstants.USE_MOCK_DICT_MANAGER = true;
    }


    @Test
    public void testDecodeNodeRewrite() throws Exception {
        String sql = "select\n" +
                "            100.00 * sum(case\n" +
                "                             when p_type like 'PROMO%'\n" +
                "                                 then l_extendedprice * (1 - l_discount)\n" +
                "                             else 0\n" +
                "            end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\n" +
                "from\n" +
                "    lineitem,\n" +
                "    part\n" +
                "where\n" +
                "        l_partkey = p_partkey\n" +
                "  and l_shipdate >= date '1997-02-01'\n" +
                "  and l_shipdate < date '1997-03-01';";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testDecodeNodeRewrite2() throws Exception {
        String sql = "select\n" +
                "    p_brand,\n" +
                "    p_type,\n" +
                "    p_size,\n" +
                "    count(distinct ps_suppkey) as supplier_cnt\n" +
                "from\n" +
                "    partsupp,\n" +
                "    part\n" +
                "where\n" +
                "        p_partkey = ps_partkey\n" +
                "  and p_brand <> 'Brand#43'\n" +
                "  and p_type not like 'PROMO BURNISHED%'\n" +
                "  and p_size in (31, 43, 9, 6, 18, 11, 25, 1)\n" +
                "  and ps_suppkey not in (\n" +
                "    select\n" +
                "        s_suppkey\n" +
                "    from\n" +
                "        supplier\n" +
                "    where\n" +
                "            s_comment like '%Customer%Complaints%'\n" +
                ")\n" +
                "group by\n" +
                "    p_brand,\n" +
                "    p_type,\n" +
                "    p_size\n;";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testDecodeNodeRewrite3() throws Exception {
        String sql = "select L_COMMENT from lineitem group by L_COMMENT";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 18> : <string id 16>\n" +
                "  |  use vectorized: true"));
    }

    @Test
    public void testDecodeNodeRewrite4() throws Exception {
        String sql = "select dept_name from dept group by dept_name,state";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 4> : <string id 2>\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 4> : 4: dept_name"));
    }

    @Test
    public void testDecodeNodeRewrite5() throws Exception {
        String sql = "select S_ADDRESS from supplier where S_ADDRESS " +
                "like '%Customer%Complaints%' group by S_ADDRESS ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 9> : <string id 3>"));
        Assert.assertTrue(plan.contains("     PREDICATES: 9: S_ADDRESS LIKE '%Customer%Complaints%'"));
    }

    @Test
    public void testDecodeNodeRewrite6() throws Exception {
        String sql = "select count(S_ADDRESS) from supplier";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("count(10: S_ADDRESS)"));

        sql = "select count(distinct S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("count(10: S_ADDRESS)"));
        Assert.assertTrue(plan.contains("HASH_PARTITIONED: 10: S_ADDRESS"));
    }

    @Test
    public void testDecodeNodeRewrite7() throws Exception {
        String sql = "select S_ADDRESS, count(S_ADDRESS) from supplier group by S_ADDRESS";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>"));
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("TGlobalDict(columnId:10, strings:[mock], ids:[1])"));
    }

    @Test
    public void testDecodeNodeRewrite8() throws Exception {
        String sql = "select S_ADDRESS, count(S_ADDRESS) from supplier group by S_ADDRESS";
        String plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0]\n" +
                "  |  * count(3: S_ADDRESS)-->[-Infinity, Infinity, 0.0, 40.0, 10000.0]"));
    }

    @Test
    public void testDecodeNodeRewrite9() throws Exception {
        String sql = "select S_ADDRESS, upper(S_ADDRESS) from supplier";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 9>"));
        String thriftPlan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 9>"));
        Assert.assertTrue(thriftPlan.contains("could_apply_dict_optimize:true"));
        Assert.assertTrue(thriftPlan.contains("string_functions:{11=TExpr(nodes"));
    }

    @Test
    public void testDecodeRewrite9Scan() throws Exception {
        String sql = "select S_ADDRESS from supplier";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testDecodeNodeRewrite10() throws Exception {
        String sql = "select upper(S_ADDRESS) as a, count(*) from supplier group by a";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 12> : <string id 9>"));
        Assert.assertTrue(plan.contains("<function id 12> : upper(11: S_ADDRESS)"));

        sql = "select S_ADDRESS, count(*) from supplier_nullable group by S_ADDRESS";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("group by: [10: S_ADDRESS, INT, true]"));
    }

    @Test
    public void testDecodeNodeRewriteMultiCountDistinct() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct a),count(distinct b) from (" +
                "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, " +
                "count(*) from supplier group by a,b) as t ";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("output: multi_distinct_count(16: upper), " +
                "multi_distinct_count(17: lower)"));

        sql = "select count(distinct S_ADDRESS), count(distinct S_COMMENT) from supplier;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" multi_distinct_count(11: S_ADDRESS), " +
                "multi_distinct_count(12: S_COMMENT)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testDecodeNodeRewriteTwoPhaseAgg() throws Exception {
        String sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by a,b";
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:13, strings:[mock], ids:[1])]"));
        Assert.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:13, strings:[mock], ids:[1])]"));

        sql = "select count(*) from supplier group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("  3:AGGREGATE (merge finalize)\n" +
                "  |  output: count(9: count())\n" +
                "  |  group by: 10: S_ADDRESS"));

        sql = "select count(*) from supplier group by S_ADDRESS";
        plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:10, strings:[mock], ids:[1])"));
        Assert.assertTrue(plan.contains("partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                "global_dicts:[TGlobalDict(columnId:10, strings:[mock], ids:[1])"));

        sql = "select count(distinct S_NATIONKEY) from supplier group by S_ADDRESS";
        plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                "global_dicts:[TGlobalDict(columnId:10, strings:[mock], ids:[1])"));

        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testDecodeRewriteTwoFunctions() throws Exception {
        String sql = "select substr(S_ADDRESS, 0, 1), S_ADDRESS from supplier";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 9>\n" +
                "  |  string functions:\n" +
                "  |  <function id 11> : substr(10: S_ADDRESS, 0, 1)"));

        sql = "select substr(S_ADDRESS, 0, 1), lower(upper(S_ADDRESS)), S_ADDRESS from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 11> : <string id 3>\n" +
                "  |  <dict id 12> : <string id 9>\n" +
                "  |  <dict id 13> : <string id 10>\n" +
                "  |  string functions:\n" +
                "  |  <function id 12> : substr(11: S_ADDRESS, 0, 1)\n" +
                "  |  <function id 13> : lower(upper(11: S_ADDRESS))"));
    }

    @Test
    public void testDecodeRewrite1() throws Exception {
        String sql = "select substr(S_ADDRESS, 0, S_NATIONKEY), S_ADDRESS from supplier";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testDecodeNodeTupleId() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(*), S_ADDRESS from supplier group by S_ADDRESS";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("node_type:DECODE_NODE, num_children:1, limit:-1, row_tuples:[3, 2]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testDecodeNodeRewrite11() throws Exception {
        String sql = "select lower(upper(S_ADDRESS)) as a, count(*) from supplier group by a";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("<function id 12> : lower(upper(11: S_ADDRESS))"));
        Assert.assertTrue(plan.contains("group by: [12: lower, INT, true]"));

        sql = "select lower(substr(S_ADDRESS, 0, 1)) as a, count(*) from supplier group by a";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<function id 12> : lower(substr(11: S_ADDRESS, 0, 1))"));

        sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by a,b";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 14> : <string id 10>\n" +
                "  |  <dict id 15> : <string id 9>\n" +
                "  |  string functions:\n" +
                "  |  <function id 14> : upper(13: S_ADDRESS)\n" +
                "  |  <function id 15> : lower(14: upper)\n" +
                "  |  use vectorized: true"));

        sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 14> : <string id 11>\n" +
                "  |  <dict id 15> : <string id 10>\n" +
                "  |  string functions:\n" +
                "  |  <function id 14> : upper(13: S_ADDRESS)\n" +
                "  |  <function id 15> : lower(14: upper)"));
    }

    @Test
    public void testScanFilter() throws Exception {
        String sql = "select count(*) from supplier where S_ADDRESS = 'kks' group by S_ADDRESS ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3: S_ADDRESS = 'kks'"));
        Assert.assertTrue(plan.contains("group by: 10: S_ADDRESS"));

        sql = "select count(*) from supplier where S_ADDRESS + 2 > 'kks' group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("group by: 3: S_ADDRESS"));
    }

    @Test
    public void testAggHaving() throws Exception {
        String sql = "select count(*) from supplier group by S_ADDRESS having S_ADDRESS = 'kks' ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3: S_ADDRESS = 'kks'"));
        Assert.assertTrue(plan.contains("group by: 10: S_ADDRESS"));

        sql = "select count(*) as b from supplier group by S_ADDRESS having b > 3";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  group by: 10: S_ADDRESS\n" +
                "  |  having: 9: count() > 3"));
    }

    @Test
    public void testJoin() throws Exception {
        String sql = "select * from test.join1 right join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  5:Decode\n" +
                "  |  <dict id 7> : <string id 3>\n" +
                "  |  <dict id 8> : <string id 6>"));


        sql = "SELECT * \n" +
                "FROM   emp \n" +
                "WHERE  EXISTS (SELECT dept.dept_id \n" +
                "               FROM   dept \n" +
                "               WHERE  emp.dept_id = dept.dept_id \n" +
                "               ORDER  BY state) \n" +
                "ORDER  BY hiredate";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  5:Decode\n" +
                "  |  <dict id 10> : <string id 2>"));

        sql = "select * from join1 join pushdown_test on join1.id = pushdown_test.k1;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  6:Decode\n" +
                "  |  <dict id 16> : <string id 12>\n" +
                "  |  <dict id 17> : <string id 3>"));
        Assert.assertTrue(plan.contains("INNER JOIN (BROADCAST)"));

    }

    @Test
    public void testJoinGlobalDict() throws Exception {
        String sql = "select part_v2.P_COMMENT from lineitem join part_v2 on L_PARTKEY = p_partkey where p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2';";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("enable_column_expr_predicate:false, dict_string_id_to_int_ids:{}"));
        Assert.assertTrue(plan.contains("P_MFGR IN ('MFGR#1', 'MFGR#2'), enable_column_expr_predicate:false, " +
                "dict_string_id_to_int_ids:{}"));
        Assert.assertTrue(plan.contains("RESULT_SINK, result_sink:TResultSink(type:MYSQL_PROTOCAL)), " +
                "partition:TDataPartition(type:RANDOM, partition_exprs:[]), global_dicts:[TGlobalDict(columnId:28"));
        Assert.assertTrue(plan.contains("TDataPartition(type:UNPARTITIONED, partition_exprs:[]))), " +
                "partition:TDataPartition(type:RANDOM, partition_exprs:[]), global_dicts:[TGlobalDict(columnId:28"));
    }

    @Test
    public void testCountDistinctMultiColumns() throws Exception {
        String sql = "select count(distinct S_SUPPKEY, S_COMMENT) from supplier";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:Decode\n" +
                "  |  <dict id 10> : <string id 7>"));
        Assert.assertTrue(plan.contains(":AGGREGATE (update serialize)\n" +
                "  |  output: count(if(1: S_SUPPKEY IS NULL, NULL, 7))"));

        sql = "select count(distinct S_ADDRESS, S_COMMENT) from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("4:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 7>"));
        Assert.assertTrue(plan.contains(" 5:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(3 IS NULL, NULL, 7))"));
    }

    @Test
    public void testGroupByWithOrderBy() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select max(S_NAME) as b from supplier group by S_ADDRESS order by b";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("group by: 10: S_ADDRESS"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }
}
