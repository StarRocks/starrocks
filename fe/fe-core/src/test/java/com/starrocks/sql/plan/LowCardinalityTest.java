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

import com.starrocks.common.FeConstants;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import mockit.Expectations;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Optional;

public class LowCardinalityTest extends PlanTestBase {
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
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE table_int (id_int INT, id_bigint BIGINT) " +
                "DUPLICATE KEY(`id_int`) " +
                "DISTRIBUTED BY HASH(`id_int`) BUCKETS 1 " +
                "PROPERTIES (\"replication_num\" = \"1\");");

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
                "\"in_memory\" = \"false\"\n" +
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
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `low_card_t1` (\n" +
                "  `d_date` date ,\n" +
                "  `c_user` varchar(50) ,\n" +
                "  `c_dept` varchar(50) ,\n" +
                "  `c_par` varchar(50) ,\n" +
                "  `c_nodevalue` varchar(50) ,\n" +
                "  `c_brokername` varchar(50) ,\n" +
                "  `f_asset` decimal128(20, 5) ,\n" +
                "  `f_asset_zb` decimal128(20, 5) ,\n" +
                "  `f_managerfee` decimal128(20, 5) ,\n" +
                "  `fee_zb` decimal128(20, 5) ,\n" +
                "  `cpc` int(11) \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`d_date`, `c_user`, `c_dept`, `c_par`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_date`, `c_user`, `c_dept`, `c_par`) BUCKETS 16 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `low_card_t2` (\n" +
                "  `d_date` date ,\n" +
                "  `c_mr` varchar(40) ,\n" +
                "  `fee_zb` decimal128(20, 5) ,\n" +
                "  `c_new` int(11) ,\n" +
                "  `cpc` int(11)\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`d_date`, `c_mr`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_date`, `c_mr`) BUCKETS 16 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setCboCteReuse(false);
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setSqlMode(0);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
    }

    @Test
    public void testOlapScanNodeOutputColumns() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        String sql =
                "SELECT C_CITY, S_CITY, year(LO_ORDERDATE) as year, sum(LO_REVENUE) AS revenue FROM lineorder_flat " +
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

    // test simple group by one lowcardinality column
    @Test
    public void testDecodeNodeRewrite3() throws Exception {
        String sql = "select L_COMMENT from lineitem group by L_COMMENT";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 18> : <string id 16>\n"));
    }

    @Test
    public void testPushdownRuntimeFilterAcrossDecodeNode() throws Exception {
        String sql = "with cte1 as (\n" +
                "select L_SHIPMODE,L_COMMENT\n" +
                "from(\n" +
                "select L_SHIPMODE, MAX(L_COMMENT) as L_COMMENT from lineitem group by L_SHIPMODE\n" +
                "union all\n" +
                "select L_SHIPMODE, \"ABCD\" from lineitem where L_SHIPMODE like \"A%\" group by L_SHIPMODE\n" +
                ") t\n" +
                "),\n" +
                "cte2 as (\n" +
                "select P_COMMENT from part where P_TYPE = \"AAAA\"\n" +
                ")\n" +
                "select cte1.L_SHIPMODE, cte1.L_COMMENT from cte1 join[broadcast] cte2 on cte1.L_SHIPMODE = cte2.P_COMMENT";

        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  3:Decode\n" +
                "  |  <dict id 98> : <string id 66>\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  aggregate: max[([97: L_COMMENT, INT, false]);" +
                " args: INT; result: INT; args nullable: false; result nullable: true]\n" +
                "  |  group by: [63: L_SHIPMODE, CHAR, false]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     table: lineitem, rollup: lineitem\n" +
                "     preAggregation: on\n" +
                "     dict_col=L_COMMENT\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=54.0\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (63: L_SHIPMODE)\n"));
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
        Assert.assertTrue(
                plan.contains("PREDICATES: DictExpr(9: S_ADDRESS,[<place-holder> LIKE '%Customer%Complaints%'])"));
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
    public void testDecodeNodeRewriteMultiAgg()
            throws Exception {
        boolean cboCteReuse = connectContext.getSessionVariable().isCboCteReuse();
        boolean enableLowCardinalityOptimize = connectContext.getSessionVariable().isEnableLowCardinalityOptimize();
        int newPlannerAggStage = connectContext.getSessionVariable().getNewPlannerAggStage();
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setNewPlanerAggStage(2);

        try {
            String sql = "select count(distinct S_ADDRESS), count(distinct S_NATIONKEY) from supplier";
            String plan = getVerboseExplain(sql);
            Assert.assertTrue(plan, plan.contains("dict_col=S_ADDRESS"));
            sql = "select count(distinct S_ADDRESS), count(distinct S_NATIONKEY) from supplier " +
                    "having count(1) > 0";
            plan = getVerboseExplain(sql);
            Assert.assertFalse(plan, plan.contains("dict_col="));
            Assert.assertFalse(plan, plan.contains("Decode"));
        } finally {
            connectContext.getSessionVariable().setCboCteReuse(cboCteReuse);
            connectContext.getSessionVariable().setEnableLowCardinalityOptimize(enableLowCardinalityOptimize);
            connectContext.getSessionVariable().setNewPlanerAggStage(newPlannerAggStage);
        }
    }

    @Test
    public void testDecodeNodeRewrite7() throws Exception {
        String sql = "select S_ADDRESS, count(S_ADDRESS) from supplier group by S_ADDRESS";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>"));
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1]"));
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
        Assert.assertTrue(plan, plan.contains("  3:Decode\n" +
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
                "  |  aggregate: multi_distinct_count[([10: S_ADDRESS, INT, false]); " +
                "args: INT; result: BIGINT; args nullable: false; result nullable: false]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  3:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: multi_distinct_count[([9: count, VARBINARY, false]); " +
                "args: INT; result: BIGINT; args nullable: true; result nullable: false]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  4:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[([10: S_ADDRESS, INT, false]); args: INT; result: BIGINT; " +
                "args nullable: false; result nullable: false]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  6:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: count[([9: count, BIGINT, false]); args: VARCHAR; result: BIGINT; " +
                "args nullable: true; result nullable: false]"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

        // TODO Fix unused Decode Node
        sql = "select count(distinct S_ADDRESS, S_COMMENT) from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "aggregate: count[(if[(3 IS NULL, NULL, [7, VARCHAR, false]); " +
                        "args: BOOLEAN,VARCHAR,VARCHAR; result: VARCHAR; " +
                        "args nullable: true; result nullable: true]); " +
                        "args: VARCHAR; result: BIGINT; args nullable: true; result nullable: false]\n"));
        Assert.assertTrue(plan.contains("  4:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 7>\n" +
                "  |  cardinality: 1"));
    }

    @Test
    public void testDecodeNodeRewriteTwoPaseDistinct() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct S_ADDRESS), count(distinct S_NATIONKEY) from supplier";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: multi_distinct_count[([9: count, VARBINARY, false]); " +
                "args: INT; result: BIGINT; args nullable: true; result nullable: false], " +
                "multi_distinct_count[([10: count, VARBINARY, false]); args: INT; result: BIGINT; " +
                "args nullable: true; result nullable: false]"));
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
        Assert.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:12, strings:[6D 6F 63 6B], ids:[1]"));

        sql = "select count(*) from supplier group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
        Assert.assertTrue(plan.contains("  3:AGGREGATE (merge finalize)\n" +
                "  |  output: count(9: count)\n" +
                "  |  group by: 10: S_ADDRESS"));

        sql = "select count(*) from supplier group by S_ADDRESS";
        plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1]"));
        Assert.assertTrue(plan.contains("partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                "query_global_dicts:[TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1]"));

        sql = "select count(distinct S_NATIONKEY) from supplier group by S_ADDRESS";
        plan = getThriftPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan, plan.contains(
                "partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                        "query_global_dicts:[TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1]"));

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
        Assert.assertTrue(
                plan.contains("<function id 12> : DictExpr(11: S_ADDRESS,[lower(substr(<place-holder>, 0, 1))])"));

        sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by a,b";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("  3:Decode\n" +
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
        Assert.assertTrue(plan, plan.contains("  3:Decode\n" +
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
        Assert.assertTrue(plan, plan.contains("  5:Decode\n" +
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
    public void testWithCaseWhen() throws Exception {
        String sql;
        String plan;
        // test if with only one dictionary column
        sql = "select case when S_ADDRESS = 'key' then 1 else 0 end from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("9 <-> DictExpr(10: S_ADDRESS,[if(<place-holder> = 'key', 1, 0)])"));
        Assert.assertTrue(plan.contains("dict_col=S_ADDRESS"));
        // test case when result no-string
        sql = "select case when S_ADDRESS = 'key' then 1 when S_ADDRESS = '2' then 2 else 0 end from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("     dict_col=S_ADDRESS"));
        // test case when output variable
        sql =
                "select case when S_ADDRESS = 'key' then 1 when S_ADDRESS = '2' then 2 else S_NATIONKEY end from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("     dict_col=S_ADDRESS"));
        Assert.assertTrue(plan.contains(
                "  |  9 <-> CASE WHEN DictExpr(10: S_ADDRESS,[<place-holder> = 'key']) " +
                        "THEN 1 WHEN DictExpr(10: S_ADDRESS,[<place-holder> = '2']) THEN 2 ELSE 4: S_NATIONKEY END"));
        // test case when with common expression 1
        sql = "select S_ADDRESS = 'key' , " +
                "case when S_ADDRESS = 'key' then 1 when S_ADDRESS = '2' then 2 else 3 end from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  output columns:\n" +
                "  |  9 <-> DictExpr(11: S_ADDRESS,[<place-holder> = 'key'])\n" +
                "  |  10 <-> DictExpr(11: S_ADDRESS,[CASE WHEN <place-holder> = 'key' " +
                "THEN 1 WHEN <place-holder> = '2' THEN 2 ELSE 3 END])\n" +
                "  |  cardinality: 1"));
        Assert.assertTrue(plan.contains("     dict_col=S_ADDRESS"));
        // test case when result string
        sql = "select case when S_ADDRESS = 'key' then 'key1' when S_ADDRESS = '2' " +
                "then 'key2' else 'key3' end from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 11> : <string id 9>"));
        // test case when with unsupported function call
        sql = "select case when S_ADDRESS = 'key' then rand() when S_ADDRESS = '2' " +
                "then 'key2' else 'key3' end from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" |  9 <-> CASE WHEN DictExpr(10: S_ADDRESS,[<place-holder> = 'key']) " +
                "THEN CAST(rand() AS VARCHAR) " +
                "WHEN DictExpr(10: S_ADDRESS,[<place-holder> = '2']) " +
                "THEN 'key2' ELSE 'key3' END"));
        Assert.assertFalse(plan.contains("Decode"));
        // test multi low cardinality column input
        sql = "select if(S_ADDRESS = 'key', S_COMMENT, 'y') from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  |  9 <-> if[(DictExpr(10: S_ADDRESS,[<place-holder> = 'key']), " +
                "DictExpr(11: S_COMMENT,[<place-holder>]), 'y'); " +
                "args: BOOLEAN,VARCHAR,VARCHAR; result: VARCHAR; args nullable: true; result nullable: true]"));
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
    public void testProject() throws Exception {
        String sql;
        String plan;

        // test cast low cardinality column as other type column
        sql = "select cast (S_ADDRESS as datetime)  from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("     dict_col=S_ADDRESS"));

        // test simple string function
        sql = "select substring(S_ADDRESS,1,2)  from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" 11 <-> DictExpr(10: S_ADDRESS,[substring(<place-holder>, 1, 2)])"));

        // test simple string function with two column
        // test worth for rewrite
        sql = "select substring(S_ADDRESS, S_SUPPKEY, 2)  from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "9 <-> substring[([3: S_ADDRESS, VARCHAR, false], [1: S_SUPPKEY, INT, false], 2); " +
                        "args: VARCHAR,INT,INT; result: VARCHAR; args nullable: false; result nullable: true]"));
        Assert.assertFalse(plan.contains("Decode"));

        // test string function with one column
        sql = "select substring(S_ADDRESS, S_ADDRESS, 1) from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "11 <-> DictExpr(10: S_ADDRESS,[substring(<place-holder>, CAST(<place-holder> AS INT), 1)])"));

        // test simple string function with two column
        // test worth for rewrite
        sql = "select substring(upper(S_ADDRESS), S_SUPPKEY, 2) from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "9 <-> substring[(DictExpr(10: S_ADDRESS,[upper(<place-holder>)]), [1: S_SUPPKEY, INT, false], 2); " +
                        "args: VARCHAR,INT,INT; result: VARCHAR; args nullable: true; result nullable: true]"));

        // test two dictionary column
        // test worth for rewrite
        sql = "select concat(S_ADDRESS, S_COMMENT) from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "  |  9 <-> concat[([3: S_ADDRESS, VARCHAR, false], [7: S_COMMENT, VARCHAR, false]); " +
                        "args: VARCHAR; result: VARCHAR; args nullable: false; result nullable: true]"));
        // Test common expression reuse 1
        // couldn't reuse case
        // DictExpr return varchar and int
        sql = "select if(S_SUPPKEY='kks', upper(S_ADDRESS), S_COMMENT), upper(S_ADDRESS) from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "  |  9 <-> if[(cast([1: S_SUPPKEY, INT, false] as VARCHAR(1048576)) = 'kks', " +
                        "DictExpr(11: S_ADDRESS,[upper(<place-holder>)]), DictExpr(12: S_COMMENT,[<place-holder>])); " +
                        "args: BOOLEAN,VARCHAR,VARCHAR; result: VARCHAR; args nullable: true; result nullable: true]\n" +
                        "  |  13 <-> DictExpr(11: S_ADDRESS,[upper(<place-holder>)])"));
        Assert.assertTrue(plan.contains("Decode"));

        // TODO: return dict column for this case
        // common expression reuse 2
        // test input two string column
        sql = "select if(S_ADDRESS='kks', S_COMMENT, S_COMMENT) from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "  |  9 <-> if[(DictExpr(10: S_ADDRESS,[<place-holder> = 'kks']), [12: expr, VARCHAR(101), true], " +
                        "[12: expr, VARCHAR(101), true]); args: BOOLEAN,VARCHAR,VARCHAR; result: VARCHAR; " +
                        "args nullable: true; result nullable: true]\n" +
                        "  |  common expressions:\n" +
                        "  |  12 <-> DictExpr(11: S_COMMENT,[<place-holder>])"));
        Assert.assertFalse(plan.contains("Decode"));

        // common expression reuse 3
        sql =
                "select if(S_ADDRESS='kks', upper(S_COMMENT), S_COMMENT), concat(upper(S_COMMENT), S_ADDRESS) from supplier";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  |  output columns:\n" +
                "  |  9 <-> if[(DictExpr(11: S_ADDRESS,[<place-holder> = 'kks']), [13: expr, VARCHAR, true], " +
                "DictExpr(12: S_COMMENT,[<place-holder>])); args: BOOLEAN,VARCHAR,VARCHAR; result: VARCHAR; " +
                "args nullable: true; result nullable: true]\n" +
                "  |  10 <-> concat[([13: expr, VARCHAR, true], DictExpr(11: S_ADDRESS,[<place-holder>])); " +
                "args: VARCHAR; result: VARCHAR; args nullable: true; result nullable: true]"));
        Assert.assertTrue(plan.contains("  |  common expressions:\n" +
                "  |  13 <-> DictExpr(12: S_COMMENT,[upper(<place-holder>)])"));

        // support(support(unsupport(Column), unsupport(Column)))
        sql = "select REVERSE(SUBSTR(LEFT(REVERSE(S_ADDRESS),INSTR(REVERSE(S_ADDRESS),'/')-1),5)) FROM supplier";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 9> : reverse(substr(left(11: expr, CAST(CAST(instr(11: expr, '/') AS BIGINT)" +
                " - 1 AS INT)), 5))\n" +
                "  |  common expressions:\n" +
                "  |  <slot 11> : DictExpr(10: S_ADDRESS,[reverse(<place-holder>)])");
    }

    @Test
    public void testScanPredicate() throws Exception {
        String sql;
        String plan;

        // Test Predicate dict columns both has support predicate and no-support predicate
        sql = "select count(*) from " +
                "supplier where S_ADDRESS like '%A%' and S_ADDRESS not like '%B%'";
        plan = getCostExplain(sql);
        Assert.assertFalse(plan.contains(" dict_col=S_ADDRESS "));

        sql = "select * from supplier l join supplier r on " +
                "l.S_NAME = r.S_NAME where upper(l.S_ADDRESS) like '%A%' and upper(l.S_ADDRESS) not like '%B%'";
        plan = getCostExplain(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     table: supplier, rollup: supplier\n" +
                "     preAggregation: on\n" +
                "     Predicates: upper(3: S_ADDRESS) LIKE '%A%', NOT (upper(3: S_ADDRESS) LIKE '%B%')\n" +
                "     dict_col=S_COMMENT");

        // Test Simple Filter
        sql = "select count(*) from supplier where S_ADDRESS = 'kks' group by S_ADDRESS ";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("DictExpr(10: S_ADDRESS,[<place-holder> = 'kks'])"));
        Assert.assertTrue(plan.contains("group by: 10: S_ADDRESS"));

        // Test unsupported predicate
        // binary columns only support slotRef op const
        sql = "select count(*) from supplier where S_ADDRESS + 2 > 'kks' group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("group by: 3: S_ADDRESS"));

        // Test Predicate with if predicate
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks', true, false)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan,
                plan.contains("PREDICATES: DictExpr(12: S_ADDRESS,[<place-holder> = 'kks'])"));

        // Test single input Expression
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks', cast(S_ADDRESS as boolean), false)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "PREDICATES: DictExpr(12: S_ADDRESS,[if(<place-holder> = 'kks', CAST(<place-holder> AS BOOLEAN), FALSE)])"));

        // Test multi input Expression with DictColumn
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks',cast(S_COMMENT as boolean), false)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains(
                "PREDICATES: if(DictExpr(12: S_ADDRESS,[<place-holder> = 'kks']), " +
                        "DictExpr(13: S_COMMENT,[CAST(<place-holder> AS BOOLEAN)]), FALSE)"));

        // Test multi input Expression with No-String Column
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks',cast(S_NAME as boolean), false)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "PREDICATES: if(DictExpr(12: S_ADDRESS,[<place-holder> = 'kks']), CAST(2: S_NAME AS BOOLEAN), FALSE)"));

        // Test Two input column. one could apply the other couldn't apply
        // The first expression that can accept a full rewrite. the second couldn't apply
        sql = "select count(*) from supplier where S_ADDRESS = 'kks' and S_COMMENT not like '%kks%'";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "PREDICATES: DictExpr(12: S_ADDRESS,[<place-holder> = 'kks']), NOT (7: S_COMMENT LIKE '%kks%')"));

        // Test Two input column. one could apply the other couldn't apply
        // Two Predicate, The first expression that can accept a partial rewrite.
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks',cast(S_COMMENT as boolean), false) " +
                "and S_COMMENT not like '%kks%'";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "PREDICATES: if(DictExpr(12: S_ADDRESS,[<place-holder> = 'kks']), " +
                        "CAST(7: S_COMMENT AS BOOLEAN), FALSE), NOT (7: S_COMMENT LIKE '%kks%')"));

    }

    @Test
    public void testAggHaving() throws Exception {
        String sql = "select count(*) from supplier group by S_ADDRESS having S_ADDRESS = 'kks' ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("DictExpr(10: S_ADDRESS,[<place-holder> = 'kks'])"));
        Assert.assertTrue(plan.contains("group by: 10: S_ADDRESS"));

        sql = "select count(*) as b from supplier group by S_ADDRESS having b > 3";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("  |  group by: 10: S_ADDRESS\n" +
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
                "select count(*) from supplier l " +
                        "join [shuffle] (select max(S_ADDRESS) as S_ADDRESS from supplier) r " +
                        "on l.S_ADDRESS = r.S_ADDRESS;";
        plan = getVerboseExplain(sql);
        Assert.assertFalse(plan.contains("Decode"));
        sql =
                "select count(*) from supplier l " +
                        "join [broadcast] (select max(S_ADDRESS) as S_ADDRESS from supplier) r " +
                        "on l.S_ADDRESS = r.S_ADDRESS;";
        plan = getVerboseExplain(sql);
        Assert.assertFalse(plan.contains("Decode"));

        sql = "select count(*) from supplier l " +
                "join [broadcast] (select max(id_int) as id_int from table_int) r " +
                "on l.S_ADDRESS = r.id_int where l.S_ADDRESS not like '%key%'";
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
        assertContains(plan, "  4:TOP-N\n" +
                "  |  order by: <slot 17> 17: S_ADDRESS ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: S_SUPPKEY = 9: S_SUPPKEY");

        // Decode
        sql = "select max(S_ADDRESS), max(S_COMMENT) from " +
                "( select l.S_ADDRESS as S_ADDRESS,r.S_COMMENT as S_COMMENT,l.S_SUPPKEY from supplier l " +
                "join supplier_nullable r " +
                " on l.S_SUPPKEY = r.S_SUPPKEY ) tb group by S_SUPPKEY";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  8:Decode\n" +
                "  |  <dict id 21> : <string id 17>\n" +
                "  |  <dict id 22> : <string id 18>\n" +
                "  |  \n" +
                "  7:Project\n" +
                "  |  <slot 21> : 21: S_ADDRESS\n" +
                "  |  <slot 22> : 22: S_COMMENT\n" +
                "  |  \n" +
                "  6:AGGREGATE (update finalize)\n" +
                "  |  output: max(19: S_ADDRESS), max(20: S_COMMENT)\n" +
                "  |  group by: 1: S_SUPPKEY");
        plan = getThriftPlan(sql);
        Assert.assertEquals(plan.split("\n").length, 3);
        System.out.println(plan);
        assertContains(plan.split("\n")[0], "query_global_dicts:" +
                "[TGlobalDict(columnId:19, strings:[6D 6F 63 6B], ids:[1], version:1), " +
                "TGlobalDict(columnId:20, strings:[6D 6F 63 6B], ids:[1], version:1), " +
                "TGlobalDict(columnId:21, strings:[6D 6F 63 6B], ids:[1], version:1), " +
                "TGlobalDict(columnId:22, strings:[6D 6F 63 6B], ids:[1], version:1)])");
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
        assertContains(plan, "  20:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 30: S_ADDRESS, 31: S_COMMENT\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  \n" +
                "  |----19:EXCHANGE\n" +
                "  |    \n" +
                "  16:EXCHANGE");
        assertContains(plan, "Decode");
        plan = getThriftPlan(sql);
        assertNotContains(plan.split("\n")[1], "query_global_dicts");
    }

    @Test
    public void testJoinGlobalDict() throws Exception {
        String sql =
                "select part_v2.P_COMMENT from lineitem join part_v2 " +
                        "on L_PARTKEY = p_partkey where p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2';";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("dict_string_id_to_int_ids:{}"));
        Assert.assertTrue(plan.contains("DictExpr(28: P_MFGR,[<place-holder> IN ('MFGR#1', 'MFGR#2')])"));
        Assert.assertTrue(plan.contains("RESULT_SINK, result_sink:TResultSink(type:MYSQL_PROTOCAL)), " +
                "partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                "query_global_dicts:[TGlobalDict(columnId:28"));
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
        String sql;
        String plan;

        sql = "select max(S_NAME) as b from supplier group by S_ADDRESS order by b";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("group by: 10: S_ADDRESS"));

        sql = "select S_ADDRESS from supplier order by S_ADDRESS";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  1:SORT\n" +
                "  |  order by: [9, INT, false] ASC"));

        sql = "select S_NAME from supplier_nullable order by upper(S_ADDRESS), S_NAME";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  2:SORT\n" +
                "  |  order by: [11, INT, true] ASC, [2, VARCHAR, false] ASC"));

        sql = "select substr(S_ADDRESS, 0, 1) from supplier group by substr(S_ADDRESS, 0, 1) " +
                "order by substr(S_ADDRESS, 0, 1)";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  7:Decode\n" +
                "  |  <dict id 11> : <string id 9>\n" +
                "  |  string functions:\n" +
                "  |  <function id 11> : DictExpr(10: S_ADDRESS,[substr(<place-holder>, 0, 1)])"));
        Assert.assertTrue(plan.contains("  5:SORT\n" +
                "  |  order by: [11, INT, true] ASC"));

        sql = "select approx_count_distinct(S_ADDRESS), upper(S_ADDRESS) from supplier " +
                " group by upper(S_ADDRESS)" +
                "order by 2";
        plan = getVerboseExplain(sql);
        assertContains(plan, " 3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  aggregate: approx_count_distinct[([3, VARCHAR, false]);");
        assertContains(plan, "2:Decode\n" +
                "  |  <dict id 11> : <string id 3>\n" +
                "  |  <dict id 12> : <string id 9>");

        // TODO add a case: Decode node before Sort Node

        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testAnalytic() throws Exception {
        // Test partition by string column
        String sql;
        String plan;
        // Analytic with where
        sql = "select sum(rm) from (" +
                "select row_number() over( partition by L_COMMENT order by L_PARTKEY) as rm from lineitem" +
                ") t where rm < 10";
        plan = getCostExplain(sql);

        Assert.assertTrue(plan.contains("  2:SORT\n" +
                "  |  order by: [20, INT, false] ASC, [2, INT, false] ASC"));
        Assert.assertTrue(plan.contains("  1:PARTITION-TOP-N\n" +
                "  |  partition by: [20: L_COMMENT, INT, false] "));
        Assert.assertTrue(plan.contains("  |  order by: [20, INT, false] ASC, [2, INT, false] ASC"));

        // row number
        sql = "select * from (select L_COMMENT,l_quantity, row_number() over " +
                "(partition by L_COMMENT order by l_quantity desc) rn from lineitem )t where rn <= 10;";
        plan = getCostExplain(sql);
        assertContains(plan, "  1:PARTITION-TOP-N\n" +
                "  |  partition by: [19: L_COMMENT, INT, false] \n" +
                "  |  partition limit: 10\n" +
                "  |  order by: [19, INT, false] ASC, [5, DOUBLE, false] DESC\n" +
                "  |  offset: 0");

        // rank
        sql = "select * from (select L_COMMENT,l_quantity, rank() over " +
                "(partition by L_COMMENT order by l_quantity desc) rn from lineitem )t where rn <= 10;";
        plan = getCostExplain(sql);
        assertContains(plan, "  1:PARTITION-TOP-N\n" +
                "  |  type: RANK\n" +
                "  |  partition by: [19: L_COMMENT, INT, false] \n" +
                "  |  partition limit: 10\n" +
                "  |  order by: [19, INT, false] ASC, [5, DOUBLE, false] DESC");

        // mul-column partition by
        sql = "select * from (select L_COMMENT,l_quantity, rank() over " +
                "(partition by L_COMMENT, l_shipmode order by l_quantity desc) rn from lineitem )t where rn <= 10;";
        plan = getCostExplain(sql);
        assertContains(plan, "  1:PARTITION-TOP-N\n" +
                "  |  type: RANK\n" +
                "  |  partition by: [19: L_COMMENT, INT, false] , [15: L_SHIPMODE, CHAR, false] \n" +
                "  |  partition limit: 10\n" +
                "  |  order by: [19, INT, false] ASC, [15, VARCHAR, false] ASC, [5, DOUBLE, false] DESC\n" +
                "  |  offset: 0");
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
        Assert.assertFalse(plan, plan.contains(" dict_col=S_ADDRESS"));

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select count(distinct S_ADDRESS), count(distinct S_NAME) as a from supplier_nullable";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("multi_distinct_count[([9: count, VARBINARY, false]);"));
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
        assertContains(plan, "6:Project\n" +
                "  |  output columns:\n" +
                "  |  10 <-> upper[([9, VARCHAR, true]); args: VARCHAR; result: VARCHAR; " +
                "args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  5:Decode\n" +
                "  |  <dict id 12> : <string id 9>\n" +
                "  |  string functions:\n" +
                "  |  <function id 12> : DictExpr(11: S_ADDRESS,[lower(<place-holder>)])");
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
                "  |  aggregate: multi_distinct_count[([9: count, VARBINARY, false]);"));
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
        sql = "select max(if(S_ADDRESS='kks', upper(S_COMMENT), S_COMMENT)), " +
                "min(upper(S_COMMENT)) from supplier_nullable " +
                "group by upper(S_COMMENT)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("6:Decode\n" +
                "  |  <dict id 17> : <string id 12>\n" +
                "  |  string functions:\n" +
                "  |  <function id 17> : DictExpr(14: S_COMMENT,[upper(<place-holder>)])\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 11> : 11: max\n" +
                "  |  <slot 17> : 17: upper"));

        connectContext.getSessionVariable().setNewPlanerAggStage(0);

    }

    @Test
    public void testSubqueryWithLimit() throws Exception {
        String sql = "select t0.S_ADDRESS from (select S_ADDRESS, S_NATIONKEY from supplier_nullable limit 10) t0" +
                " inner join supplier on t0.S_NATIONKEY = supplier.S_NATIONKEY;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Decode\n" +
                "  |  <dict id 17> : <string id 3>\n");
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
        String sql;
        String plan;

        sql = "SELECT S_ADDRESS, Dense_rank() OVER ( ORDER BY S_SUPPKEY) " +
                "FROM supplier UNION SELECT S_ADDRESS, Dense_rank() OVER ( ORDER BY S_SUPPKEY) FROM supplier;";
        plan = getCostExplain(sql);
        // No need for low-card optimization for
        // SCAN->DECODE->SORT
        Assert.assertFalse(plan.contains("Decode"));

        // window function with full order by
        sql = "select rank() over (order by S_ADDRESS) as rk from supplier_nullable";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:ANALYTIC\n" +
                "  |  functions: [, rank(), ]\n" +
                "  |  order by: 3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                "  |  \n" +
                "  3:Decode\n" +
                "  |  <dict id 10> : <string id 3>");

        // Decode node under sort node
        sql = "select S_ADDRESS, S_COMMENT from (select S_ADDRESS, " +
                "S_COMMENT from supplier_nullable order by S_COMMENT limit 10) tb where S_ADDRESS = 'SS' order by S_ADDRESS ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:SORT\n" +
                "  |  order by: <slot 3> 3 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  4:SELECT\n" +
                "  |  predicates: 3 = 'SS'\n" +
                "  |  \n" +
                "  3:Decode\n" +
                "  |  <dict id 9> : <string id 3>\n" +
                "  |  <dict id 10> : <string id 7>");
    }

    @Test
    public void testHavingAggFunctionOnConstant() throws Exception {
        String sql = "select S_ADDRESS from supplier GROUP BY S_ADDRESS HAVING (cast(count(null) as string)) IN (\"\")";
        String plan = getCostExplain(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: count[(NULL); args: BOOLEAN; result: BIGINT; args nullable: true; result nullable: false]\n" +
                "  |  group by: [10: S_ADDRESS, INT, false]");
        assertContains(plan, "  3:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  cardinality: 1");
    }

    @Test
    public void testDecodeWithLimit() throws Exception {
        String sql = "select count(*), S_ADDRESS from supplier group by S_ADDRESS limit 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n");
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
        assertContains(plan, "  4:Project\n" +
                "  |  <slot 20> : 9\n" +
                "  |  <slot 21> : 25: cast\n" +
                "  |  <slot 22> : CAST(15: expr AS VARCHAR)\n" +
                "  |  <slot 23> : CAST(CAST(9 AS DOUBLE) + 1.0 AS VARCHAR)\n" +
                "  |  common expressions:\n" +
                "  |  <slot 25> : CAST(15: expr AS VARCHAR)\n" +
                "  |  \n" +
                "  3:Decode\n" +
                "  |  <dict id 24> : <string id 9>\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  group by: 24: S_ADDRESS, 15: expr\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 15> : CAST(7: S_SUPPKEY AS BIGINT) + 1\n" +
                "  |  <slot 24> : 24: S_ADDRESS");

    }

    @Test
    public void testCTEWithDecode() throws Exception {
        connectContext.getSessionVariable().setCboCteReuse(true);
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        String sql = "with v1 as( select S_ADDRESS a, count(*) b from supplier group by S_ADDRESS) " +
                "select x1.a, x1.b from v1 x1 join v1 x2 on x1.a=x2.a";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(
                plan.contains("query_global_dicts:[TGlobalDict(columnId:28, strings:[6D 6F 63 6B], ids:[1]"));
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setEnablePipelineEngine(false);
    }

    @Test
    public void testMetaScan() throws Exception {
        String sql = "select max(v1), min(v1) from t0 [_META_]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:MetaScan\n" +
                "     Table: t0\n" +
                "     <id 6> : max_v1\n" +
                "     <id 7> : min_v1"));
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("id_to_names:{6=max_v1, 7=min_v1}"));
    }

    @Test
    public void testMetaScan2() throws Exception {
        String sql = "select max(t1c), min(t1d), dict_merge(t1a) from test_all_type [_META_]";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("  0:MetaScan\n" +
                "     Table: test_all_type\n" +
                "     <id 16> : dict_merge_t1a\n" +
                "     <id 14> : max_t1c\n" +
                "     <id 15> : min_t1d"));

        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("TFunctionName(function_name:dict_merge), " +
                "binary_type:BUILTIN, arg_types:[TTypeDesc(types:[TTypeNode(type:ARRAY), " +
                "TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))])]"));
    }

    @Test
    public void testMetaScan3() throws Exception {
        String sql = "select max(t1c), min(t1d), dict_merge(t1a) from test_all_type [_META_]";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update serialize)\n" +
                "  |  output: max(max_t1c), min(min_t1d), dict_merge(dict_merge_t1a)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:MetaScan\n" +
                "     Table: test_all_type\n" +
                "     <id 16> : dict_merge_t1a\n" +
                "     <id 14> : max_t1c\n" +
                "     <id 15> : min_t1d");
    }

    @Test
    public void testMetaScan4() throws Exception {
        String sql = "select sum(t1c), min(t1d), t1a from test_all_type [_META_]";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  output: sum(3: t1c), min(4: t1d), any_value(1: t1a)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : t1a\n" +
                "  |  <slot 3> : t1c\n" +
                "  |  <slot 4> : t1d\n" +
                "  |  \n" +
                "  0:MetaScan\n" +
                "     Table: test_all_type");
        sql = "select sum(t1c) from test_all_type [_META_] group by t1a";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(3: t1c)\n" +
                "  |  group by: 1: t1a\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : t1a\n" +
                "  |  <slot 3> : t1c\n" +
                "  |  \n" +
                "  0:MetaScan\n" +
                "     Table: test_all_type");
    }

    @Test
    public void testHasGlobalDictButNotFound() throws Exception {
        IDictManager dictManager = IDictManager.getInstance();

        new Expectations(dictManager) {
            {
                dictManager.hasGlobalDict(anyLong, "S_ADDRESS", anyLong);
                result = true;
                dictManager.getGlobalDict(anyLong, "S_ADDRESS");
                result = Optional.empty();
            }
        };

        String sql = "select S_ADDRESS from supplier group by S_ADDRESS";
        // Check No Exception
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("Decode"));
    }

    @Test
    public void testExtractProject() throws Exception {
        String sql;
        String plan;

        sql = "select max(upper(S_ADDRESS)), min(upper(S_ADDRESS)), max(S_ADDRESS), sum(S_SUPPKEY + 1) from supplier";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: max(18: upper), min(18: upper), max(17: S_ADDRESS), sum(1: S_SUPPKEY), count(1: S_SUPPKEY)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: S_SUPPKEY\n" +
                "  |  <slot 17> : 17: S_ADDRESS\n" +
                "  |  <slot 18> : DictExpr(17: S_ADDRESS,[upper(<place-holder>)])");
    }

    @Test
    public void testCompoundPredicate() throws Exception {
        String sql = "select count(*) from supplier group by S_ADDRESS having " +
                "if(S_ADDRESS > 'a' and S_ADDRESS < 'b', true, false)";
        String plan = getVerboseExplain(sql);
        assertContains(plan,
                "DictExpr(10: S_ADDRESS,[<place-holder> > 'a']), " +
                        "DictExpr(10: S_ADDRESS,[<place-holder> < 'b'])");

        sql = "select count(*) from supplier group by S_ADDRESS having " +
                "if(not S_ADDRESS like '%a%' and S_ADDRESS < 'b', true, false)";
        plan = getVerboseExplain(sql);
        assertContains(plan,
                "NOT (3: S_ADDRESS LIKE '%a%'), [3: S_ADDRESS, VARCHAR, false] < 'b'");
    }

    @Test
    public void testComplexScalarOperator_1() throws Exception {
        String sql = "select case when s_address = 'test' then 'a' " +
                "when s_phone = 'b' then 'b' " +
                "when coalesce(s_address, 'c') = 'c' then 'c' " +
                "else 'a' end from supplier; ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 9> : CASE WHEN DictExpr(10: S_ADDRESS,[<place-holder> = 'test']) THEN 'a' " +
                "WHEN 5: S_PHONE = 'b' THEN 'b' " +
                "WHEN coalesce(DictExpr(10: S_ADDRESS,[<place-holder>]), 'c') = 'c' THEN 'c' " +
                "ELSE 'a' END\n" +
                "  |");

        sql = "select case when s_address = 'test' then 'a' " +
                "when s_phone = 'b' then 'b' " +
                "when upper(s_address) = 'c' then 'c' " +
                "else 'a' end from supplier; ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 9> : CASE WHEN DictExpr(10: S_ADDRESS,[<place-holder> = 'test']) THEN 'a' " +
                "WHEN 5: S_PHONE = 'b' THEN 'b' " +
                "WHEN DictExpr(10: S_ADDRESS,[upper(<place-holder>)]) = 'c' THEN 'c' " +
                "ELSE 'a' END\n" +
                "  |");
    }

    @Test
    public void testComplexScalarOperator_2() throws Exception {
        String sql = "select count(*) from supplier where s_phone = 'a' or coalesce(s_address, 'c') = 'c' " +
                "or s_address = 'address'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: supplier\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: ((5: S_PHONE = 'a') OR (coalesce(DictExpr(12: S_ADDRESS,[<place-holder>]), 'c') = 'c')) " +
                "OR (DictExpr(12: S_ADDRESS,[<place-holder> = 'address']))");

        sql = "select count(*) from supplier where s_phone = 'a' or upper(s_address) = 'c' " +
                "or s_address = 'address'";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: supplier\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: ((5: S_PHONE = 'a') OR (DictExpr(12: S_ADDRESS,[upper(<place-holder>)]) = 'c')) " +
                "OR (DictExpr(12: S_ADDRESS,[<place-holder> = 'address']))");
    }

    @Test
    public void testAggWithProjection() throws Exception {
        String sql = "select cast(max(s_address) as date) from supplier";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:Project\n" +
                "  |  <slot 10> : DictExpr(12: S_ADDRESS,[CAST(<place-holder> AS DATE)])\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: max(11: S_ADDRESS)\n" +
                "  |  group by: ");
    }

    @Test
    public void testJoinWithProjection() throws Exception {
        String sql = "select s_address, cast(t1.s_address as date), cast(t1.s_phone as date), upper(t1.s_address)," +
                " cast(t2.a as date), 123 from supplier t1 join (select max(s_address) a from supplier) t2 ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "5:Project\n" +
                "  |  <slot 18> : DictExpr(23: S_ADDRESS,[CAST(<place-holder> AS DATE)])\n" +
                "  |  <slot 19> : CAST(5: S_PHONE AS DATE)\n" +
                "  |  <slot 21> : DictExpr(25: S_ADDRESS,[CAST(<place-holder> AS DATE)])\n" +
                "  |  <slot 22> : 123\n" +
                "  |  <slot 23> : 23: S_ADDRESS\n" +
                "  |  <slot 26> : DictExpr(23: S_ADDRESS,[upper(<place-holder>)])\n" +
                "  |  \n" +
                "  4:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----3:EXCHANGE");
    }

    @Test
    public void testTopNWithProjection() throws Exception {
        String sql =
                "select t2.s_address, cast(t1.a as date), concat(t1.b, '') from (select max(s_address) a, min(s_phone) b " +
                        "from supplier group by s_address) t1 join (select s_address from supplier) t2 order by t1.a";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "10:Decode\n" +
                "  |  <dict id 23> : <string id 13>\n" +
                "  |  \n" +
                "  9:Project\n" +
                "  |  <slot 19> : 19: cast\n" +
                "  |  <slot 20> : 20: concat\n" +
                "  |  <slot 23> : 23: S_ADDRESS\n" +
                "  |  \n" +
                "  8:MERGING-EXCHANGE");
    }

    @Test
    public void testLogicalProperty() throws Exception {
        String sql = "select cast(max(s_address) as date) from supplier where s_suppkey = 1 group by S_PHONE";
        ExecPlan execPlan = getExecPlan(sql);
        OlapScanNode olapScanNode = (OlapScanNode) execPlan.getScanNodes().get(0);
        Assert.assertEquals(0, olapScanNode.getBucketExprs().size());

        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertContains(plan, "3:Project\n" +
                "  |  <slot 10> : DictExpr(12: S_ADDRESS,[CAST(<place-holder> AS DATE)])\n" +
                "  |  \n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: max(11: S_ADDRESS)\n" +
                "  |  group by: 5: S_PHONE");
    }

    @Test
    public void testLowCardForLimit() throws Exception {
        String sql = "SELECT * from (SELECT t_a_0.`S_ADDRESS` AS f_ax_0, t_a_0.`S_ADDRESS` AS f_ax_1 FROM " +
                "(select * from (select * from supplier limit 20000) b) t_a_0) t_a_1 ORDER BY t_a_1.f_ax_0 desc LIMIT 0,20;";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:Decode\n" +
                "  |  <dict id 9> : <string id 3>\n" +
                "  |  \n" +
                "  2:TOP-N\n" +
                "  |  order by: <slot 9> 9: S_ADDRESS DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 20");

    }

    @Test
    public void testNeedDecode_1() throws Exception {
        String sql = "with cte_1 as (\n" +
                "    select\n" +
                "        t0.P_NAME as a,\n" +
                "        t0.P_BRAND as b,\n" +
                "        t1.s_name as c,\n" +
                "        t1.s_address as d,\n" +
                "        t1.s_address as e,\n" +
                "        t1.s_nationkey as f\n" +
                "    from\n" +
                "        part_v2 t0\n" +
                "        left join supplier_nullable t1 on t0.P_SIZE > t1.s_suppkey\n" +
                ")\n" +
                "select\n" +
                "    cte_1.b,\n" +
                "    if(\n" +
                "        cte_1.d in ('hz', 'bj'),\n" +
                "        cte_1.b,\n" +
                "        if (cte_1.e in ('hz'), 1035, cte_1.f)\n" +
                "    ),\n" +
                "    count(distinct if(cte_1.c = '', cte_1.e, null))\n" +
                "from\n" +
                "    cte_1\n" +
                "group by\n" +
                "    cte_1.b,\n" +
                "    if(\n" +
                "        cte_1.d in ('hz', 'bj'),\n" +
                "        cte_1.b,\n" +
                "        if (cte_1.e in ('hz'), 1035, cte_1.f)\n" +
                "    );";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:Project\n" +
                "  |  <slot 22> : 22\n" +
                "  |  <slot 37> : if(31: S_ADDRESS IN ('hz', 'bj'), 22, CAST(if(31: S_ADDRESS = 'hz', " +
                "1035, 32: S_NATIONKEY) AS VARCHAR))\n" +
                "  |  <slot 38> : if(30: S_NAME = '', 31: S_ADDRESS, NULL)\n" +
                "  |  \n" +
                "  5:Decode\n" +
                "  |  <dict id 40> : <string id 22>");
    }

    @Test
    public void testNeedDecode_2() throws Exception {
        String sql = "with cte_1 as (\n" +
                "    select\n" +
                "        t0.P_NAME as a,\n" +
                "        t0.P_BRAND as b,\n" +
                "        t1.s_name as c,\n" +
                "        t1.s_address as d,\n" +
                "        t1.s_address as e,\n" +
                "        t1.s_nationkey as f\n" +
                "    from\n" +
                "        part_v2 t0\n" +
                "        left join supplier_nullable t1 on t0.P_SIZE > t1.s_suppkey\n" +
                ")\n" +
                "select\n" +
                "    if(\n" +
                "        cte_1.d in ('hz', 'bj'),\n" +
                "        cte_1.b,\n" +
                "        if (cte_1.e in ('hz'), 1035, cte_1.f)\n" +
                "    ),\n" +
                "    count(distinct if(cte_1.c = '', cte_1.e, null))\n" +
                "from\n" +
                "    cte_1\n" +
                "group by\n" +
                "    if(\n" +
                "        cte_1.d in ('hz', 'bj'),\n" +
                "        cte_1.b,\n" +
                "        if (cte_1.e in ('hz'), 1035, cte_1.f)\n" +
                "    );";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:Project\n" +
                "  |  <slot 37> : if(31: S_ADDRESS IN ('hz', 'bj'), 22, CAST(if(31: S_ADDRESS = 'hz', 1035, " +
                "32: S_NATIONKEY) AS VARCHAR))\n" +
                "  |  <slot 38> : if(30: S_NAME = '', 31: S_ADDRESS, NULL)\n" +
                "  |  \n" +
                "  5:Decode\n" +
                "  |  <dict id 40> : <string id 22>");
    }

    @Test
    public void testProjectionRewrite() throws Exception {
        String sql = "SELECT '2023-03-26' D_DATE, c_user, concat(C_NODEVALUE, '-', C_BROKERNAME) AS C_NAME, c_dept, " +
                "c_par, '人', '1', '规', '1', 'KPI1' C_KPICODE, round(sum(if(c_par='01', F_ASSET_zb, F_ASSET))/100000000, 5) " +
                "F_CURRENTDATA FROM low_card_t1 WHERE c_par IN ( '02', '01' ) AND D_DATE='2023-03-26' " +
                "GROUP BY C_BROKERNAME, c_dept, c_par, c_user, C_NODEVALUE " +
                "union all " +
                "SELECT '2023-03-26' D_DATE, c_mr AS C_CODE, CASE WHEN c_mr = '01' THEN '部' ELSE '户部' END C_NAME, " +
                "c_mr c_dept, c_mr c_par, '门' AS C_ROLE, '3' AS F_ROLERANK, '入' AS C_KPITYPE, '2' AS F_KPIRANK, " +
                "'KPI2' C_KPICODE, ifnull(ROUND(SUM(fee_zb)/100000000, 5), 0) AS F_CURRENTDATA FROM low_card_t2 " +
                "WHERE c_mr IN ('02', '03') AND D_DATE>concat(year(str_to_date('2023-03-26', '%Y-%m-%d'))-1, '1231') " +
                "AND d_date<='2023-03-26' GROUP BY c_mr;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "10:Decode\n" +
                "  |  <dict id 55> : <string id 23>\n" +
                "  |  \n" +
                "  9:AGGREGATE (update finalize)\n" +
                "  |  output: sum(24: fee_zb)\n" +
                "  |  group by: 55: c_mr");
    }
}
