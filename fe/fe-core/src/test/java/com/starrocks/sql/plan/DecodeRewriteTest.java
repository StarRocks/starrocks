// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.catalog.ScalarType;
import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DecodeRewriteTest extends PlanTestBase {
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

        starRocksAssert.withTable("CREATE TABLE lineorder_flat (\n" +
                "LO_ORDERDATE date NOT NULL COMMENT \"\",\n" +
                "LO_ORDERKEY int(11) NOT NULL COMMENT \"\",\n" +
                "LO_LINENUMBER tinyint(4) NOT NULL COMMENT \"\",\n" +
                "LO_CUSTKEY int(11) NOT NULL COMMENT \"\",\n" +
                "LO_PARTKEY int(11) NOT NULL COMMENT \"\",\n" +
                "LO_SUPPKEY int(11) NOT NULL COMMENT \"\",\n" +
                "LO_ORDERPRIORITY varchar(100) NOT NULL COMMENT \"\",\n" +
                "LO_SHIPPRIORITY tinyint(4) NOT NULL COMMENT \"\",\n" +
                "LO_QUANTITY tinyint(4) NOT NULL COMMENT \"\",\n" +
                "LO_EXTENDEDPRICE int(11) NOT NULL COMMENT \"\",\n" +
                "LO_ORDTOTALPRICE int(11) NOT NULL COMMENT \"\",\n" +
                "LO_DISCOUNT tinyint(4) NOT NULL COMMENT \"\",\n" +
                "LO_REVENUE int(11) NOT NULL COMMENT \"\",\n" +
                "LO_SUPPLYCOST int(11) NOT NULL COMMENT \"\",\n" +
                "LO_TAX tinyint(4) NOT NULL COMMENT \"\",\n" +
                "LO_COMMITDATE date NOT NULL COMMENT \"\",\n" +
                "LO_SHIPMODE varchar(100) NOT NULL COMMENT \"\",\n" +
                "C_NAME varchar(100) NOT NULL COMMENT \"\",\n" +
                "C_ADDRESS varchar(100) NOT NULL COMMENT \"\",\n" +
                "C_CITY varchar(100) NOT NULL COMMENT \"\",\n" +
                "C_NATION varchar(100) NOT NULL COMMENT \"\",\n" +
                "C_REGION varchar(100) NOT NULL COMMENT \"\",\n" +
                "C_PHONE varchar(100) NOT NULL COMMENT \"\",\n" +
                "C_MKTSEGMENT varchar(100) NOT NULL COMMENT \"\",\n" +
                "S_NAME varchar(100) NOT NULL COMMENT \"\",\n" +
                "S_ADDRESS varchar(100) NOT NULL COMMENT \"\",\n" +
                "S_CITY varchar(100) NOT NULL COMMENT \"\",\n" +
                "S_NATION varchar(100) NOT NULL COMMENT \"\",\n" +
                "S_REGION varchar(100) NOT NULL COMMENT \"\",\n" +
                "S_PHONE varchar(100) NOT NULL COMMENT \"\",\n" +
                "P_NAME varchar(100) NOT NULL COMMENT \"\",\n" +
                "P_MFGR varchar(100) NOT NULL COMMENT \"\",\n" +
                "P_CATEGORY varchar(100) NOT NULL COMMENT \"\",\n" +
                "P_BRAND varchar(100) NOT NULL COMMENT \"\",\n" +
                "P_COLOR varchar(100) NOT NULL COMMENT \"\",\n" +
                "P_TYPE varchar(100) NOT NULL COMMENT \"\",\n" +
                "P_SIZE tinyint(4) NOT NULL COMMENT \"\",\n" +
                "P_CONTAINER varchar(100) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(LO_ORDERDATE, LO_ORDERKEY)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(LO_ORDERKEY) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
    }

    @Test
    public void testOlapScanNodeOutputColumns() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        String sql = "SELECT C_CITY, S_CITY, year(LO_ORDERDATE) as year, sum(LO_REVENUE) AS revenue FROM lineorder_flat " +
                "WHERE C_CITY in ('UNITED KI1', 'UNITED KI5') AND S_CITY in ( 'UNITED KI1', 'UNITED\n" +
                "KI5') AND LO_ORDERDATE >= '1997-12-01' AND LO_ORDERDATE <= '1997-12-31' GROUP BY C_CITY, S_CITY, year " +
                "ORDER BY year ASC, revenue DESC;";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("unused_output_column_name:[]"));
        connectContext.getSessionVariable().disableTrimOnlyFilteredColumnsInScanStage();
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
                "  |  <dict id 18> : <string id 16>\n"));
    }

    @Test
    public void testDecodeNodeRewrite4() throws Exception {
        String sql = "select dept_name from dept group by dept_name,state";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 4> : <string id 2>\n" +
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
        Assert.assertTrue(plan.contains("PREDICATES: DictExpr(9: S_ADDRESS,[<place-holder> LIKE '%Customer%Complaints%'])"));
    }

    @Test
    public void testDecodeNodeRewrite6() throws Exception {
        String sql = "select count(S_ADDRESS) from supplier";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("count(10: S_ADDRESS)"));

        sql = "select count(distinct S_ADDRESS) from supplier";
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("count(10: S_ADDRESS)"));
        Assert.assertTrue(plan.contains("HASH_PARTITIONED: 10: S_ADDRESS"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testDecodeNodeRewrite7() throws Exception {
        String sql = "select S_ADDRESS, count(S_ADDRESS) from supplier group by S_ADDRESS";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>"));
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1])"));
    }

    @Test
    public void testDecodeNodeRewrite8() throws Exception {
        String sql = "select S_ADDRESS, count(S_ADDRESS) from supplier group by S_ADDRESS";
        String plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE\n" +
                "  |  * count-->[0.0, 1.0, 0.0, 8.0, 1.0] ESTIMATE"));
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
        Assert.assertTrue(plan.contains("<function id 12> : DictExpr(11: S_ADDRESS,[upper(<place-holder>)])"));

        sql = "select S_ADDRESS, count(*) from supplier_nullable group by S_ADDRESS";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("group by: [10: S_ADDRESS, INT, true]"));
    }

    @Test
    public void testDecodeNodeRewriteMultiCountDistinct() throws Exception {
        String sql;
        String plan;
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select count(distinct a),count(distinct b) from (" +
                "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, " +
                "count(*) from supplier group by a,b) as t ";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("7:AGGREGATE (merge finalize)\n" +
                "  |  output: multi_distinct_count(12: count), multi_distinct_count(13: count)"));

        sql = "select count(distinct S_ADDRESS), count(distinct S_COMMENT) from supplier;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" multi_distinct_count(11: S_ADDRESS), " +
                "multi_distinct_count(12: S_COMMENT)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        sql = "select max(S_ADDRESS), count(distinct S_ADDRESS) from supplier group by S_ADDRESS;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  4:AGGREGATE (update finalize)\n" +
                "  |  output: max(13: S_ADDRESS), count(11: S_ADDRESS)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testDecodeNodeRewriteDistinct() throws Exception {
        String sql;
        String plan;
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        sql = "select count(distinct S_ADDRESS) from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: multi_distinct_count[([10: S_ADDRESS, INT, false]); args: INT; result: BIGINT; args nullable: false; result nullable: false]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  3:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: multi_distinct_count[([9: count, VARCHAR, false]); args: INT; result: BIGINT; args nullable: true; result nullable: false]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  4:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[([10: S_ADDRESS, INT, false]); args: INT; result: BIGINT; args nullable: false; result nullable: false]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  6:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: count[([9: count, BIGINT, false]); args: VARCHAR; result: BIGINT; args nullable: true; result nullable: false]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testDecodeNodeRewriteTwoPaseDistinct() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct S_ADDRESS), count(distinct S_NATIONKEY) from supplier";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: multi_distinct_count[([9: count, VARCHAR, false]); args: INT; result: BIGINT; args nullable: true; result nullable: false], " +
                "multi_distinct_count[([10: count, VARCHAR, false]); args: INT; result: BIGINT; args nullable: true; result nullable: false]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testDecodeNodeRewriteTwoPhaseAgg() throws Exception {
        String sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by a,b";
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 13> : DictExpr(12: S_ADDRESS,[lower(upper(<place-holder>))])\n" +
                "  |  <slot 14> : DictExpr(12: S_ADDRESS,[upper(<place-holder>)])"));
        Assert.assertFalse(plan.contains("common expressions"));
        plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:12, strings:[6D 6F 63 6B], ids:[1])]"));
        Assert.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:12, strings:[6D 6F 63 6B], ids:[1])]"));

        sql = "select count(*) from supplier group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("  3:AGGREGATE (merge finalize)\n" +
                "  |  output: count(9: count)\n" +
                "  |  group by: 10: S_ADDRESS"));

        sql = "select count(*) from supplier group by S_ADDRESS";
        plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1])"));
        Assert.assertTrue(plan.contains("partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                "query_global_dicts:[TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1])"));

        sql = "select count(distinct S_NATIONKEY) from supplier group by S_ADDRESS";
        plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                "query_global_dicts:[TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1])"));

        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testDecodeRewriteTwoFunctions() throws Exception {
        String sql;
        String plan;

        sql = "select substr(S_ADDRESS, 0, S_NATIONKEY), upper(S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));

        sql = "select substr(S_ADDRESS, 0, 1), S_ADDRESS from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 9>\n" +
                "  |  string functions:\n" +
                "  |  <function id 11> : DictExpr(10: S_ADDRESS,[substr(<place-holder>, 0, 1)])"));

        sql = "select substr(S_ADDRESS, 0, 1), lower(upper(S_ADDRESS)), S_ADDRESS from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 11> : <string id 3>\n" +
                "  |  <dict id 12> : <string id 9>\n" +
                "  |  <dict id 13> : <string id 10>\n" +
                "  |  string functions:\n" +
                "  |  <function id 12> : DictExpr(11: S_ADDRESS,[substr(<place-holder>, 0, 1)])\n" +
                "  |  <function id 13> : DictExpr(11: S_ADDRESS,[lower(upper(<place-holder>))])"));
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
        Assert.assertTrue(plan.contains("node_type:DECODE_NODE, num_children:1, limit:-1, row_tuples:[3]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testDecodeNodeRewrite11() throws Exception {
        String sql = "select lower(upper(S_ADDRESS)) as a, count(*) from supplier group by a";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("<function id 12> : DictExpr(11: S_ADDRESS,[lower(upper(<place-holder>))])"));
        Assert.assertTrue(plan.contains("group by: [12: lower, INT, true]"));

        sql = "select lower(substr(S_ADDRESS, 0, 1)) as a, count(*) from supplier group by a";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<function id 12> : DictExpr(11: S_ADDRESS,[lower(substr(<place-holder>, 0, 1))])"));

        sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by a,b";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 13> : <string id 9>\n" +
                "  |  <dict id 14> : <string id 10>\n" +
                "  |  string functions:\n" +
                "  |  <function id 13> : DictExpr(12: S_ADDRESS,[lower(upper(<place-holder>))])\n" +
                "  |  <function id 14> : DictExpr(12: S_ADDRESS,[upper(<place-holder>)])"));

        sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 13> : <string id 10>\n" +
                "  |  <dict id 14> : <string id 11>\n" +
                "  |  string functions:\n" +
                "  |  <function id 13> : DictExpr(12: S_ADDRESS,[lower(upper(<place-holder>))])\n" +
                "  |  <function id 14> : DictExpr(12: S_ADDRESS,[upper(<place-holder>)])"));
    }

    @Test
    public void testDecodeNodeRewrite12() throws Exception {
        String sql;
        String plan;

        sql = "select max(S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("Decode"));

        sql = "select min(S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("Decode"));

        sql = "select max(upper(S_ADDRESS)) from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 13> : <string id 10>\n" +
                "  |  string functions:\n" +
                "  |  <function id 13> : DictExpr(11: S_ADDRESS,[upper(<place-holder>)])"));

        sql = "select max(\"CONST\") from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testDecodeNodeRewrite13() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql;
        String plan;
        // case join:
        // select unsupported_function(dict_col) from table1 join table2
        // Add Decode Node before unsupported Projection 1
        sql = "select coalesce(l.S_ADDRESS,l.S_NATIONKEY) from supplier l join supplier r on l.s_suppkey = r.s_suppkey";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  4:Project\n" +
                "  |  <slot 17> : coalesce(3, CAST(4: S_NATIONKEY AS VARCHAR))"));
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 18> : <string id 3>"));

        // select unsupported_function(dict_col), dict_col from table1 join table2
        // Add Decode Node before unsupported Projection 2
        sql = "select coalesce(l.S_ADDRESS,l.S_NATIONKEY),l.S_ADDRESS,r.S_ADDRESS " +
                "from supplier l join supplier r on l.s_suppkey = r.s_suppkey";
        plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("  4:Project\n" +
                "  |  <slot 3> : 3\n" +
                "  |  <slot 11> : 11\n" +
                "  |  <slot 17> : coalesce(3, CAST(4: S_NATIONKEY AS VARCHAR))"));
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 18> : <string id 3>\n" +
                "  |  <dict id 19> : <string id 11>"));

        // select unsupported_function(dict_col), supported_func(dict_col), dict_col
        // from table1 join table2;
        // projection has both supported operator and no-supported operator
        sql = "select coalesce(l.S_ADDRESS,l.S_NATIONKEY), upper(l.S_ADDRESS), l.S_ADDRESS " +
                "from supplier l join supplier r on l.s_suppkey = r.s_suppkey";
        plan = getFragmentPlan(sql);

        Assert.assertFalse(plan.contains("Decode"));

        // select unsupported_function(dict_col), supported_func(table2.dict_col2), table2.dict_col2
        // from table1 join table2;
        // left table don't support dict optimize, but right table support it
        sql = "select coalesce(l.S_ADDRESS,l.S_NATIONKEY), upper(r.P_MFGR),r.P_MFGR " +
                "from supplier l join part_v2 r on l.s_suppkey = r.P_PARTKEY";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  5:Decode\n" +
                "  |  <dict id 21> : <string id 11>\n" +
                "  |  <dict id 22> : <string id 20>\n" +
                "  |  string functions:\n" +
                "  |  <function id 22> : DictExpr(21: P_MFGR,[upper(<place-holder>)])"));

        Assert.assertTrue(plan.contains("  4:Project\n" +
                "  |  <slot 19> : coalesce(3: S_ADDRESS, CAST(4: S_NATIONKEY AS VARCHAR))\n" +
                "  |  <slot 21> : 21: P_MFGR\n" +
                "  |  <slot 22> : DictExpr(21: P_MFGR,[upper(<place-holder>)])"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testDecodeNodeRewrite14() throws Exception {
        String sql;
        String plan;
        // case agg:
        // select supported_agg(dict),unsupported_agg(dict) from table1
        // Add Decode Node before unsupported Projection 1
        sql = "select count(*), approx_count_distinct(S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));

        sql = "select max(S_ADDRESS), approx_count_distinct(S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testDecodeNodeRewrite15() throws Exception {
        // Test Predicate dict columns both has support predicate and no-support predicate
        String sql;
        String plan;
        {
            sql = "select count(*) from " +
                    "supplier where S_ADDRESS like '%A%' and S_ADDRESS not like '%B%'";
            plan = getCostExplain(sql);
            Assert.assertFalse(plan.contains(" dict_col=S_ADDRESS "));
        }
        {
            sql = "select * from supplier l join supplier r on " +
                    "l.S_NAME = r.S_NAME where upper(l.S_ADDRESS) like '%A%' and upper(l.S_ADDRESS) not like '%B%'";
            plan = getCostExplain(sql);
            Assert.assertTrue(plan.contains("  1:OlapScanNode\n" +
                    "     table: supplier, rollup: supplier\n" +
                    "     preAggregation: on\n" +
                    "     Predicates: upper(3: S_ADDRESS) LIKE '%A%', NOT (upper(3: S_ADDRESS) LIKE '%B%')\n" +
                    "     dict_col=S_COMMENT"));
        }
    }

    @Test
    public void testCastRewrite() throws Exception {
        String sql;
        String plan;
        // test cast low cardinality column as other type column
        sql = "select cast (S_ADDRESS as datetime)  from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertFalse(plan.contains("     dict_col=S_ADDRESS"));
    }

    @Test
    public void testLeftJoinWithUnion() throws Exception {
        String sql;
        String plan;

        sql = "SELECT subt1.S_ADDRESS\n" +
                "FROM (\n" +
                "        SELECT S_ADDRESS, S_NATIONKEY\n" +
                "        FROM supplier\n" +
                "    ) subt1 LEFT ANTI\n" +
                "    JOIN (\n" +
                "        SELECT S_ADDRESS, S_NATIONKEY\n" +
                "        FROM supplier\n" +
                "    ) subt0 ON subt1.S_NATIONKEY = subt0.S_NATIONKEY  \n" +
                "WHERE true\n" +
                "UNION ALL\n" +
                "SELECT subt1.S_ADDRESS\n" +
                "FROM (\n" +
                "        SELECT S_ADDRESS, S_NATIONKEY\n" +
                "        FROM supplier\n" +
                "    ) subt1 LEFT ANTI\n" +
                "    JOIN (\n" +
                "        SELECT S_ADDRESS, S_NATIONKEY\n" +
                "        FROM supplier\n" +
                "    ) subt0 ON subt1.S_NATIONKEY = subt0.S_NATIONKEY\n" +
                "WHERE (NOT (true));";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  5:Decode\n" +
                "  |  <dict id 34> : <string id 33>\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 34> : 34: S_ADDRESS"));
    }

    @Test
    public void testScanFilter() throws Exception {
        String sql = "select count(*) from supplier where S_ADDRESS = 'kks' group by S_ADDRESS ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("DictExpr(10: S_ADDRESS,[<place-holder> = 'kks'])"));
        Assert.assertTrue(plan.contains("group by: 10: S_ADDRESS"));

        sql = "select count(*) from supplier where S_ADDRESS + 2 > 'kks' group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("group by: 3: S_ADDRESS"));
    }

    @Test
    public void testAggHaving() throws Exception {
        String sql = "select count(*) from supplier group by S_ADDRESS having S_ADDRESS = 'kks' ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("DictExpr(10: S_ADDRESS,[<place-holder> = 'kks'])"));
        Assert.assertTrue(plan.contains("group by: 10: S_ADDRESS"));

        sql = "select count(*) as b from supplier group by S_ADDRESS having b > 3";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  group by: 10: S_ADDRESS\n" +
                "  |  having: 9: count > 3"));
        // test couldn't push down predicate
        sql = "select sum(S_NATIONKEY) a, sum(S_ACCTBAL) as b, S_ADDRESS as c from supplier group by S_ADDRESS " +
                "having a < b*1.2 or c not like '%open%'";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));

        // test couldn't push down having predicate
        sql = "SELECT count(*) a FROM supplier having max(S_ADDRESS)='123'";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testJoin() throws Exception {
        String sql;
        String plan;
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql =
                "select count(*) from supplier l join [shuffle] (select max(S_ADDRESS) as S_ADDRESS from supplier) r on l.S_ADDRESS = r.S_ADDRESS;";
        plan = getVerboseExplain(sql);
        Assert.assertFalse(plan.contains("Decode"));
        sql =
                "select count(*) from supplier l join [broadcast] (select max(S_ADDRESS) as S_ADDRESS from supplier) r on l.S_ADDRESS = r.S_ADDRESS;";
        plan = getVerboseExplain(sql);
        Assert.assertFalse(plan.contains("Decode"));

        sql = "select *\n" +
                "from(\n" +
                "        select S_SUPPKEY,\n" +
                "            S_NATIONKEY\n" +
                "        from supplier\n" +
                "    ) l\n" +
                "    right outer join [shuffle] (\n" +
                "        select S_SUPPKEY,\n" +
                "            max(S_ADDRESS) as MS\n" +
                "        from supplier_nullable\n" +
                "        group by S_SUPPKEY\n" +
                "    ) r on l.S_SUPPKEY = r.S_SUPPKEY\n" +
                "    and l.S_NATIONKEY = r.MS;";
        plan = getVerboseExplain(sql);
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        Assert.assertTrue(plan.contains("OutPut Partition: HASH_PARTITIONED: 9: S_SUPPKEY, 17"));

        sql = "select * from test.join1 right join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        plan = getFragmentPlan(sql);
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

        sql = "select part_v2.p_partkey from lineitem join part_v2 on L_COMMENT = hex(P_NAME);";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));

        // TopN with HashJoinNode
        sql = "select * from supplier l join supplier_nullable r where l.S_SUPPKEY = r.S_SUPPKEY " +
                "order by l.S_ADDRESS limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:TOP-N\n" +
                "  |  order by: <slot 3> 3 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  4:Decode\n" +
                "  |  <dict id 17> : <string id 3>\n" +
                "  |  <dict id 18> : <string id 7>\n" +
                "  |  <dict id 19> : <string id 11>\n" +
                "  |  <dict id 20> : <string id 15>\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: S_SUPPKEY = 9: S_SUPPKEY");

        // Decode
        sql = "select max(S_ADDRESS), max(S_COMMENT) from " +
                "( select l.S_ADDRESS as S_ADDRESS,r.S_COMMENT as S_COMMENT,l.S_SUPPKEY from supplier l " +
                "join supplier_nullable r " +
                " on l.S_SUPPKEY = r.S_SUPPKEY ) tb group by S_SUPPKEY";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  9:Decode\n" +
                "  |  <dict id 23> : <string id 17>\n" +
                "  |  <dict id 24> : <string id 18>\n" +
                "  |  \n" +
                "  8:Project\n" +
                "  |  <slot 23> : 23: S_ADDRESS\n" +
                "  |  <slot 24> : 24: S_COMMENT\n" +
                "  |  \n" +
                "  7:AGGREGATE (merge finalize)\n" +
                "  |  output: max(21: S_ADDRESS), max(22: S_COMMENT)\n" +
                "  |  group by: 1: S_SUPPKEY");
        // the fragment on the top don't have to send global dicts
        sql = "select upper(ST_S_ADDRESS),\n" +
                "    upper(ST_S_COMMENT)\n" +
                "from (\n" +
                "        select ST_S_ADDRESS, ST_S_COMMENT\n" +
                "        from (\n" +
                "                select l.S_ADDRESS as ST_S_ADDRESS,\n" +
                "                    l.S_COMMENT ST_S_COMMENT,\n" +
                "                    l.S_SUPPKEY S_SUPPKEY,\n" +
                "                    l.S_NATIONKEY S_NATIONKEY\n" +
                "                from supplier l\n" +
                "                    join [shuffle] supplier m on l.S_SUPPKEY = m.S_SUPPKEY\n" +
                "                order by l.S_ADDRESS\n" +
                "                limit 10\n" +
                "        ) star join [shuffle] supplier r on star.S_NATIONKEY = r.S_NATIONKEY\n" +
                "        union select 1,2\n" +
                "    ) sys";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  19:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 30: S_ADDRESS, 31: S_COMMENT\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  \n" +
                "  |----18:EXCHANGE\n" +
                "  |    \n" +
                "  15:EXCHANGE"));
        Assert.assertTrue(plan.contains("Decode"));
        plan = getThriftPlan(sql);
        Assert.assertFalse(plan.split("\n")[1].contains("query_global_dicts"));
    }

    @Test
    public void testJoinGlobalDict() throws Exception {
        String sql =
                "select part_v2.P_COMMENT from lineitem join part_v2 on L_PARTKEY = p_partkey where p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2';";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("enable_column_expr_predicate:false, dict_string_id_to_int_ids:{}"));
        Assert.assertTrue(plan.contains("DictExpr(28: P_MFGR,[<place-holder> IN ('MFGR#1', 'MFGR#2')])"));
        Assert.assertTrue(plan.contains("RESULT_SINK, result_sink:TResultSink(type:MYSQL_PROTOCAL)), " +
                "partition:TDataPartition(type:RANDOM, partition_exprs:[]), query_global_dicts:[TGlobalDict(columnId:28"));
        Assert.assertTrue(
                plan.contains("TDataPartition(type:UNPARTITIONED, partition_exprs:[]), is_merge:false, dest_dop:0)), " +
                        "partition:TDataPartition(type:RANDOM, partition_exprs:[]), query_global_dicts:[TGlobalDict(columnId:28"));
    }

    @Test
    public void testCountDistinctMultiColumns() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct S_SUPPKEY, S_COMMENT) from supplier";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:Decode\n" +
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
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testGroupByWithOrderBy() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select max(S_NAME) as b from supplier group by S_ADDRESS order by b";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("group by: 10: S_ADDRESS"));

        sql = "select substr(S_ADDRESS, 0, 1) from supplier group by substr(S_ADDRESS, 0, 1) " +
                "order by substr(S_ADDRESS, 0, 1)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  5:Decode\n" +
                "  |  <dict id 11> : <string id 9>\n" +
                "  |  string functions:\n" +
                "  |  <function id 11> : DictExpr(10: S_ADDRESS,[substr(<place-holder>, 0, 1)])"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testProjectionPredicate() throws Exception {
        String sql = "select count(t.a) from(select S_ADDRESS in ('kks', 'kks2') as a from supplier) as t";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" dict_col=S_ADDRESS"));
        Assert.assertTrue(plan.contains("9 <-> DictExpr(11: S_ADDRESS,[<place-holder> IN ('kks', 'kks2')])"));

        sql = "select count(t.a) from(select S_ADDRESS = 'kks' as a from supplier) as t";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" dict_col=S_ADDRESS"));
        Assert.assertTrue(plan.contains("9 <-> DictExpr(11: S_ADDRESS,[<place-holder> = 'kks'])"));

        sql = "select count(t.a) from(select S_ADDRESS is null as a from supplier) as t";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" dict_col=S_ADDRESS"));
        Assert.assertTrue(plan.contains("9 <-> DictExpr(11: S_ADDRESS,[<place-holder> IS NULL])"));

        sql = "select count(t.a) from(select S_ADDRESS is not null as a from supplier) as t";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" dict_col=S_ADDRESS"));
        Assert.assertTrue(plan.contains("9 <-> DictExpr(11: S_ADDRESS,[<place-holder> IS NOT NULL])"));

        sql = "select count(t.a) from(select S_ADDRESS <=> 'kks' as a from supplier) as t";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("[3: S_ADDRESS, VARCHAR, false] <=> 'kks'"));

        // TODO:
        sql = "select S_ADDRESS not like '%key%' from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertFalse(plan.contains(" dict_col=S_ADDRESS"));

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select count(distinct S_ADDRESS), count(distinct S_NAME) as a from supplier_nullable";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("multi_distinct_count[([9: count, VARCHAR, false]);"));
        Assert.assertTrue(plan.contains("multi_distinct_count[([11: S_ADDRESS, INT, true]);"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testNestedExpressions() throws Exception {
        String sql;
        String plan;
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select upper(lower(S_ADDRESS)) from supplier group by lower(S_ADDRESS);";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  6:Project\n" +
                "  |  output columns:\n" +
                "  |  10 <-> upper[([9, VARCHAR, true]); args: VARCHAR; result: VARCHAR; args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 0\n" +
                "  |  \n" +
                "  5:Decode\n" +
                "  |  <dict id 12> : <string id 9>\n" +
                "  |  string functions:\n" +
                "  |  <function id 12> : DictExpr(11: S_ADDRESS,[lower(<place-holder>)])"));
        Assert.assertTrue(plan.contains("  4:AGGREGATE (merge finalize)\n" +
                "  |  group by: [12: lower, INT, true]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testMultiMaxMin() throws Exception {
        String sql;
        String plan;
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select count(distinct S_ADDRESS), max(S_ADDRESS), count(distinct S_SUPPKEY) as a from supplier_nullable";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("1:AGGREGATE (update serialize)\n" +
                "  |  aggregate: multi_distinct_count[([12: S_ADDRESS, INT, true]);"));
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: multi_distinct_count[([9: count, VARCHAR, false]);"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select min(distinct S_ADDRESS), max(S_ADDRESS) from supplier_nullable";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update serialize)\n" +
                "  |  output: min(11: S_ADDRESS), max(11: S_ADDRESS)"));
        Assert.assertTrue(plan.contains("  3:AGGREGATE (merge finalize)\n" +
                "  |  output: min(12: S_ADDRESS), max(13: S_ADDRESS)"));
        Assert.assertTrue(plan.contains("  4:Decode\n" +
                "  |  <dict id 14> : <string id 9>\n" +
                "  |  <dict id 15> : <string id 10>"));

        sql = "select max(upper(S_ADDRESS)) from supplier_nullable";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  5:Decode\n" +
                "  |  <dict id 14> : <string id 10>\n" +
                "  |  string functions:\n" +
                "  |  <function id 14> : DictExpr(11: S_ADDRESS,[upper(<place-holder>)])\n" +
                "  |  "));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

    }

    @Test
    public void testSubqueryWithLimit() throws Exception {
        String sql = "select t0.S_ADDRESS from (select S_ADDRESS, S_NATIONKEY from supplier_nullable limit 10) t0" +
                " inner join supplier on t0.S_NATIONKEY = supplier.S_NATIONKEY;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  6:Decode\n" +
                "  |  <dict id 17> : <string id 3>"));
    }

    @Test
    public void testDecodeWithCast() throws Exception {
        String sql = "select reverse(conv(cast(S_ADDRESS as bigint), NULL, NULL)) from supplier";
        String plan = getFragmentPlan(sql);
        // Currently, we disable cast operator
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("reverse(conv(CAST(3: S_ADDRESS AS BIGINT), NULL, NULL))"));
    }

    @Test
    public void testAssignWrongNullableProperty() throws Exception {
        String sql =
                "SELECT S_ADDRESS, Dense_rank() OVER ( ORDER BY S_SUPPKEY) FROM supplier UNION SELECT S_ADDRESS, Dense_rank() OVER ( ORDER BY S_SUPPKEY) FROM supplier;";
        String plan = getCostExplain(sql);
        // No need for low-card optimization for
        // SCAN->DECODE->SORT
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testHavingAggFunctionOnConstant() throws Exception {
        String sql = "select S_ADDRESS from supplier GROUP BY S_ADDRESS HAVING (cast(count(null) as string)) IN (\"\")";
        String plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: count[(NULL); args: BOOLEAN; result: BIGINT; args nullable: true; result nullable: false]\n" +
                "  |  group by: [10: S_ADDRESS, INT, false]\n" +
                "  |  having: cast([9: count, BIGINT, false] as VARCHAR(" + ScalarType.DEFAULT_STRING_LENGTH +
                ")) = ''"));
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  cardinality: 1"));
    }

    @Test
    public void testDecodeWithLimit() throws Exception {
        String sql = "select count(*), S_ADDRESS from supplier group by S_ADDRESS limit 10";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  limit: 10"));
    }

    @Test
    public void testNoDecode() throws Exception {
        String sql = "select *, to_bitmap(S_SUPPKEY) from supplier limit 1";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));

        sql = "select hex(10), s_address from supplier";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));

        sql = "SELECT SUM(count) FROM (SELECT CAST((CAST((((\"C\")||(CAST(s_address AS STRING ) ))) " +
                "BETWEEN (((\"T\")||(\"\"))) AND (\"\") AS BOOLEAN) = true) " +
                "AND (CAST((((\"C\")||(CAST(s_address AS STRING ) ))) BETWEEN (((\"T\")||(\"\"))) " +
                "AND (\"\") AS BOOLEAN) IS NOT NULL) AS INT) as count FROM supplier ) t;";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));

        sql =
                "SELECT SUM(count) FROM (SELECT CAST((CAST((s_address) BETWEEN (((CAST(s_address AS STRING ) )||(\"\"))) " +
                        "AND (s_address) AS BOOLEAN) = true) AND (CAST((s_address) " +
                        "BETWEEN (((CAST(s_address AS STRING ) )||(\"\"))) AND (s_address) AS BOOLEAN) IS NOT NULL) AS INT) " +
                        "as count FROM supplier ) t;";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testDecodeOnExchange() throws Exception {
        String sql = " SELECT \n" +
                "  DISTINCT * \n" +
                "FROM \n" +
                "  (\n" +
                "    SELECT \n" +
                "      DISTINCT t1.v4 \n" +
                "    FROM \n" +
                "      t1, \n" +
                "      test_all_type as t2, \n" +
                "      test_all_type as t0 \n" +
                "    WHERE \n" +
                "      NOT (\n" +
                "        (t2.t1a) != (\n" +
                "          concat(t0.t1a, \"ji\")\n" +
                "        )\n" +
                "      ) \n" +
                "  ) t;";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testProjectWithUnionEmptySet() throws Exception {
        String sql;
        String plan;
        sql = "select t1a from test_all_type group by t1a union all select v4 from t1 where false";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 16> : <string id 15>"));
        Assert.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 16> : 16: t1a"));

        // COW Case
        sql = "SELECT 'all', 'allx' where 1 = 2 union all select distinct S_ADDRESS, S_ADDRESS from supplier;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 14> : 8\n" +
                "  |  <slot 15> : clone(8)\n" +
                "  |  \n" +
                "  2:Decode\n" +
                "  |  <dict id 16> : <string id 8>\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 16: S_ADDRESS");

        sql = "SELECT 'all', 'all', 'all', 'all' where 1 = 2 union all " +
                "select distinct S_ADDRESS, S_SUPPKEY + 1, S_SUPPKEY + 1, S_ADDRESS + 1 from supplier;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 20> : 9: S_ADDRESS\n" +
                "  |  <slot 21> : 24: cast\n" +
                "  |  <slot 22> : CAST(15: expr AS VARCHAR)\n" +
                "  |  <slot 23> : CAST(16: expr AS VARCHAR)\n" +
                "  |  common expressions:\n" +
                "  |  <slot 24> : CAST(15: expr AS VARCHAR)\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  group by: 9: S_ADDRESS, 15: expr, 16: expr");

    }

    @Test
    public void testCTEWithDecode() throws Exception {
        connectContext.getSessionVariable().setCboCteReuse(true);
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        String sql =
                "with v1 as( select S_ADDRESS a, count(*) b from supplier group by S_ADDRESS) select x1.a, x1.b from v1 x1" +
		"join v1 x2 on x1.a=x2.a";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(
                plan.contains("query_global_dicts:[TGlobalDict(columnId:28, strings:[6D 6F 63 6B], ids:[1])"));
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setEnablePipelineEngine(false);
    }

}
