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

import com.starrocks.catalog.ColumnId;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.rule.tree.lowcardinality.DecodeCollector;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class LowCardinalityTest2 extends PlanTestBase {
    @BeforeAll
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
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `low_card_t2` (\n" +
                "  `d_date` date ,\n" +
                "  `c_mr` varchar(40) ,\n" +
                "  `fee_zb` decimal128(20, 5) ,\n" +
                "  `c_new` int(11) ,\n" +
                "  `cpc` int(11)\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`d_date`, `c_mr`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_date`, `c_mr`) BUCKETS 16 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        connectContext.getSessionVariable().setEnableEliminateAgg(false);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setSqlMode(0);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
    }

    @Test
    public void testOlapScanNodeOutputColumns() throws Exception {
        connectContext.getSessionVariable().enableTrimOnlyFilteredColumnsInScanStage();
        try {
            String sql =
                    "SELECT C_CITY, S_CITY, year(LO_ORDERDATE) as year, sum(LO_REVENUE) AS revenue FROM lineorder_flat " +
                            "WHERE C_CITY in ('UNITED KI1', 'UNITED KI5') AND S_CITY in ( 'UNITED KI1', 'UNITED\n" +
                            "KI5') AND LO_ORDERDATE >= '1997-12-01' AND " +
                            "LO_ORDERDATE <= '1997-12-31' GROUP BY C_CITY, S_CITY, year " +
                            "ORDER BY year ASC, revenue DESC;";
            String plan = getThriftPlan(sql);
            Assertions.assertTrue(plan.contains("unused_output_column_name:[]"), plan);
        } finally {
            connectContext.getSessionVariable().disableTrimOnlyFilteredColumnsInScanStage();
        }
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
        assertNotContains(plan, "DecodeNode");
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
        Assertions.assertTrue(plan.contains("Decode\n" +
                "  |  <dict id 27> : <string id 11>"), plan);
    }

    // test simple group by one lowcardinality column
    @Test
    public void testDecodeNodeRewrite3() throws Exception {
        String sql = "select L_COMMENT from lineitem group by L_COMMENT";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 18> : <string id 16>\n"), plan);
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
        Assertions.assertTrue(plan.contains("  4:Decode\n" +
                "  |  <dict id 51> : <string id 18>\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  3:EXCHANGE\n" +
                "     distribution type: ROUND_ROBIN\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (<slot 15>)"), plan);
    }

    @Test
    public void testDecodeNodeRewrite4() throws Exception {
        String sql = "select dept_name from dept group by dept_name,state";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 4> : <string id 2>\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 4> : 4: dept_name"), plan);
    }

    @Test
    public void testDecodeNodeRewriteLength() throws Exception {
        String sql = "select length(dept_name), char_length(dept_name) from dept group by dept_name,state";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 4> : DictDecode(6: dept_name, [length(<place-holder>)])\n" +
                "  |  <slot 5> : DictDecode(6: dept_name, [char_length(<place-holder>)])"), plan);
    }

    @Test
    public void testDecodeNodeRewrite5() throws Exception {
        String sql = "select S_ADDRESS from supplier where S_ADDRESS " +
                "like '%Customer%Complaints%' group by S_ADDRESS ";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 9> : <string id 3>"), plan);
        Assertions.assertTrue(
                plan.contains("PREDICATES: DictDecode(9: S_ADDRESS, [<place-holder> LIKE '%Customer%Complaints%'])"));
    }

    @Test
    public void testDecodeNodeRewrite6() throws Exception {
        String sql = "select count(S_ADDRESS) from supplier";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "DecodeNode");
        Assertions.assertTrue(plan.contains("count(10: S_ADDRESS)"), plan);

        sql = "select count(distinct S_ADDRESS) from supplier";
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        try {
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "DecodeNode");
            Assertions.assertTrue(plan.contains("count(10: S_ADDRESS)"), plan);
            Assertions.assertTrue(plan.contains("HASH_PARTITIONED: 10: S_ADDRESS"), plan);
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
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
            Assertions.assertTrue(plan.contains("dict_col=S_ADDRESS"), plan);
            sql = "select count(distinct S_ADDRESS), count(distinct S_NATIONKEY) from supplier " +
                    "having count(1) > 0";
            plan = getVerboseExplain(sql);
            Assertions.assertTrue(plan.contains("dict_col=S_ADDRESS"), plan);
            Assertions.assertFalse(plan.contains("Decode"), plan);
        } finally {
            connectContext.getSessionVariable().setCboCteReuse(cboCteReuse);
            connectContext.getSessionVariable().setEnableLowCardinalityOptimize(enableLowCardinalityOptimize);
            connectContext.getSessionVariable().setNewPlanerAggStage(newPlannerAggStage);
        }
    }

    @Test
    public void testIdentifyBlocking() throws Exception {
        String sql = "select * from low_card_t1 order by 1";
        boolean hasBlockingNode = new DecodeCollector.CheckBlockingNode().check(
                UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPhysicalPlan());
        Assertions.assertTrue(hasBlockingNode);
        sql = "select sum(cpc) from low_card_t1";
        hasBlockingNode = new DecodeCollector.CheckBlockingNode().check(
                UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPhysicalPlan());
        Assertions.assertTrue(hasBlockingNode);
        sql = "(select sum(cpc) from low_card_t1) union all (select sum(cpc) from low_card_t2)";
        hasBlockingNode = new DecodeCollector.CheckBlockingNode().check(
                UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPhysicalPlan());
        Assertions.assertFalse(hasBlockingNode);
        sql = "(select sum(cpc) from low_card_t1) union all (select sum(cpc) from low_card_t2) order by 1";
        hasBlockingNode = new DecodeCollector.CheckBlockingNode().check(
                UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPhysicalPlan());
        Assertions.assertTrue(hasBlockingNode);
        sql = "select sum(ss) from " +
                "((select sum(cpc) as ss from low_card_t1) union all (select sum(cpc) as ss from low_card_t2)) x";
        hasBlockingNode = new DecodeCollector.CheckBlockingNode().check(
                UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPhysicalPlan());
        Assertions.assertTrue(hasBlockingNode);
        sql = "select * from low_card_t1 a join low_card_t2 b on a.cpc = b.cpc";
        hasBlockingNode = new DecodeCollector.CheckBlockingNode().check(
                UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPhysicalPlan());
        Assertions.assertFalse(hasBlockingNode);
        sql = "select * from low_card_t1 a join low_card_t2 b on a.cpc = b.cpc order by 1";
        hasBlockingNode = new DecodeCollector.CheckBlockingNode().check(
                UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPhysicalPlan());
        Assertions.assertTrue(hasBlockingNode);
        sql = "select sum(a.cpc) from low_card_t1 a join low_card_t2 b on a.cpc = b.cpc order by 1";
        hasBlockingNode = new DecodeCollector.CheckBlockingNode().check(
                UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPhysicalPlan());
        Assertions.assertTrue(hasBlockingNode);
    }

    @Test
    public void testDecodeNodeRewrite7() throws Exception {
        String sql = "select S_ADDRESS, count(S_ADDRESS) from supplier group by S_ADDRESS";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>"), plan);
        String thrift = getThriftPlan(sql);
        Assertions.assertTrue(thrift.contains("TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1]"), thrift);
    }

    @Test
    public void testDecodeNodeRewrite8() throws Exception {
        String sql = "select S_ADDRESS, count(S_ADDRESS) from supplier group by S_ADDRESS";
        String plan = getCostExplain(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE\n" +
                "  |  * count-->[0.0, 1.0, 0.0, 8.0, 1.0] ESTIMATE"), plan);
    }

    @Test
    public void testDecodeNodeRewrite9() throws Exception {
        String sql = "select S_ADDRESS, upper(S_ADDRESS) from supplier";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 9> : DictDecode(10: S_ADDRESS, [upper(<place-holder>)])\n" +
                "  |  <slot 10> : 10: S_ADDRESS"), plan);
        String thriftPlan = getThriftPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 9> : DictDecode(10: S_ADDRESS, [upper(<place-holder>)])\n" +
                "  |  <slot 10> : 10: S_ADDRESS"), plan);
        Assertions.assertTrue(thriftPlan.contains("could_apply_dict_optimize:true"), thriftPlan);
    }

    @Test
    public void testDecodeRewrite9Scan() throws Exception {
        String sql = "select S_ADDRESS from supplier";
        String plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("Decode"), plan);
    }

    @Test
    public void testDecodeNodeRewrite10() throws Exception {
        String sql = "select upper(S_ADDRESS) as a, count(*) from supplier group by a";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 12> : <string id 9>"), plan);
        Assertions.assertTrue(plan.contains("<slot 12> : DictDefine(11: S_ADDRESS, [upper(<place-holder>)])"), plan);

        sql = "select S_ADDRESS, count(*) from supplier_nullable group by S_ADDRESS";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("group by: [10: S_ADDRESS, INT, true]"), plan);
    }

    @Test
    public void testDecodeNodeRewriteMultiCountDistinct() throws Exception {
        String sql;
        String plan;
        try {
            connectContext.getSessionVariable().setNewPlanerAggStage(2);
            sql = "select count(distinct a),count(distinct b) from (" +
                    "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, " +
                    "count(*) from supplier group by a,b) as t ";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "DecodeNode");
            Assertions.assertTrue(plan.contains("7:AGGREGATE (merge finalize)\n" +
                    "  |  output: multi_distinct_count(12: count), multi_distinct_count(13: count)"), plan);

            sql = "select count(distinct S_ADDRESS), count(distinct S_COMMENT) from supplier;";
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains(" multi_distinct_count(11: S_ADDRESS), " +
                    "multi_distinct_count(12: S_COMMENT)"), plan);
            connectContext.getSessionVariable().setNewPlanerAggStage(3);
            sql = "select max(S_ADDRESS), count(distinct S_ADDRESS) from supplier group by S_ADDRESS;";
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("  4:AGGREGATE (update finalize)\n" +
                    "  |  output: max(12: max), count(11: S_ADDRESS)"), plan);
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
    }

    @Test
    public void testDecodeNodeRewriteDistinct() throws Exception {
        String sql;
        String plan;

        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        sql = "select count(distinct S_ADDRESS) from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: multi_distinct_count[([10: S_ADDRESS, INT, false]); " +
                "args: INT; result: BIGINT; args nullable: false; result nullable: false]"), plan);
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  3:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: multi_distinct_count[([9: count, VARBINARY, false]); " +
                "args: INT; result: BIGINT; args nullable: true; result nullable: false]"), plan);
        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  4:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[([10: S_ADDRESS, INT, false]); args: INT; result: BIGINT; " +
                "args nullable: false; result nullable: false]"), plan);
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  6:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: count[([9: count, BIGINT, false]); args: INT; result: BIGINT; " +
                "args nullable: true; result nullable: false]"), plan);
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

        sql = "select count(distinct S_ADDRESS, S_COMMENT) from supplier";
        plan = getVerboseExplain(sql);
        assertContains(plan,
                "aggregate: count[(if[(3: S_ADDRESS IS NULL, NULL, [7: S_COMMENT, VARCHAR(101), false]); " +
                        "args: BOOLEAN,VARCHAR,VARCHAR; result: VARCHAR; args nullable: true; result nullable: true]); " +
                        "args: VARCHAR; result: BIGINT; args nullable: true; result nullable: false]\n");
        Assertions.assertTrue(plan.contains("  4:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 7>\n" +
                "  |  cardinality: 1"));
    }

    @Test
    public void testDecodeNodeRewriteTwoPaseDistinct() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        try {
            String sql = "select count(distinct S_ADDRESS), count(distinct S_NATIONKEY) from supplier";
            String plan = getVerboseExplain(sql);
            Assertions.assertTrue(plan.contains(" 3:AGGREGATE (merge finalize)\n" +
                    "  |  aggregate: multi_distinct_count[([9: count, VARBINARY, false]); " +
                    "args: INT; result: BIGINT; args nullable: true; result nullable: false], " +
                    "multi_distinct_count[([10: count, VARBINARY, false]); args: INT; result: BIGINT; " +
                    "args nullable: true; result nullable: false]"), plan);
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
    }

    @Test
    public void testDecodeNodeRewriteTwoPhaseAgg() throws Exception {
        String sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by a,b";
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        try {
            String plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains(" 1:Project\n" +
                    "  |  <slot 13> : DictDefine(12: S_ADDRESS, [lower(upper(<place-holder>))])\n" +
                    "  |  <slot 14> : DictDefine(12: S_ADDRESS, [upper(<place-holder>)])"), plan);
            Assertions.assertFalse(plan.contains("common expressions"));
            plan = getThriftPlan(sql);
            Assertions.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:12, strings:[6D 6F 63 6B], ids:[1]"),
                    plan);

            sql = "select count(*) from supplier group by S_ADDRESS";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "DecodeNode");
            Assertions.assertTrue(plan.contains("  3:AGGREGATE (merge finalize)\n" +
                    "  |  output: count(9: count)\n" +
                    "  |  group by: 10: S_ADDRESS"), plan);

            sql = "select count(*) from supplier group by S_ADDRESS";
            plan = getThriftPlan(sql);
            Assertions.assertTrue(plan.contains("global_dicts:[TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1]"),
                    plan);
            Assertions.assertTrue(plan.contains("partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                    "query_global_dicts:[TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1]"), plan);

            sql = "select count(distinct S_NATIONKEY) from supplier group by S_ADDRESS";
            plan = getThriftPlan(sql);
            System.out.println(plan);
            Assertions.assertTrue(plan.contains(
                    "partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                            "query_global_dicts:[TGlobalDict(columnId:10, strings:[6D 6F 63 6B], ids:[1]"), plan);
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
    }

    @Test
    public void testDecodeRewriteTwoFunctions() throws Exception {
        String sql;
        String plan;

        sql = "select substr(S_ADDRESS, 0, S_NATIONKEY), upper(S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 9> : substr(DictDecode(11: S_ADDRESS, [<place-holder>]), 0, 4: S_NATIONKEY)\n" +
                "  |  <slot 10> : DictDecode(11: S_ADDRESS, [upper(<place-holder>)])"), plan);

        sql = "select substr(S_ADDRESS, 0, 1), S_ADDRESS from supplier";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 9> : DictDecode(10: S_ADDRESS, [substr(<place-holder>, 0, 1)])\n" +
                "  |  <slot 10> : 10: S_ADDRESS"), plan);

        sql = "select substr(S_ADDRESS, 0, 1), lower(upper(S_ADDRESS)), S_ADDRESS from supplier";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 11> : <string id 3>\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 9> : DictDecode(11: S_ADDRESS, [substr(<place-holder>, 0, 1)])\n" +
                "  |  <slot 10> : DictDecode(11: S_ADDRESS, [lower(upper(<place-holder>))])\n" +
                "  |  <slot 11> : 11: S_ADDRESS"), plan);
    }

    @Test
    public void testDecodeRewrite1() throws Exception {
        String sql = "select substr(S_ADDRESS, 0, S_NATIONKEY), S_ADDRESS from supplier";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "DecodeNode");
    }

    @Test
    public void testDecodeNodeTupleId() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        try {
            String sql = "select count(*), S_ADDRESS from supplier group by S_ADDRESS";
            String plan = getThriftPlan(sql);
            Assertions.assertTrue(plan.contains("node_type:DECODE_NODE, num_children:1, limit:-1, row_tuples:[3]"), plan);
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
    }

    @Test
    public void testDecodeNodeRewrite11() throws Exception {
        String sql = "select lower(upper(S_ADDRESS)) as a, count(*) from supplier group by a";
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 12> : <string id 9>"), plan);
        Assertions.assertTrue(plan.contains("group by: [12: lower, INT, true]"), plan);

        sql = "select lower(substr(S_ADDRESS, 0, 1)) as a, count(*) from supplier group by a";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("<slot 12> : DictDefine(11: S_ADDRESS, [lower(substr(<place-holder>, 0, 1))])"),
                plan);

        sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by a,b";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 13> : <string id 9>\n" +
                "  |  <dict id 14> : <string id 10>"), plan);

        sql = "select lower(upper(S_ADDRESS)) as a, upper(S_ADDRESS) as b, count(*) from supplier group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 9> : 9: count\n" +
                "  |  <slot 10> : DictDecode(12: S_ADDRESS, [lower(upper(<place-holder>))])\n" +
                "  |  <slot 11> : DictDecode(12: S_ADDRESS, [upper(<place-holder>)])"), plan);
    }

    @Test
    public void testDecodeNodeRewrite12() throws Exception {
        String sql;
        String plan;

        sql = "select max(S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("Decode"), plan);

        sql = "select min(S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("Decode"), plan);

        sql = "select max(upper(S_ADDRESS)) from supplier";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("Decode\n" +
                "  |  <dict id 13> : <string id 10>"), plan);

        sql = "select max(\"CONST\") from supplier";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "DecodeNode");
    }

    @Test
    public void testDecodeNodeRewrite13() throws Exception {
        FeConstants.runningUnitTest = true;
        try {

            String sql;
            String plan;
            // case join:
            // select unsupported_function(dict_col) from table1 join table2
            // Add Decode Node before unsupported Projection 1
            sql =
                    "select coalesce(l.S_ADDRESS,l.S_NATIONKEY) from supplier l join supplier r on l.s_suppkey = r.s_suppkey";
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("  3:Project\n" +
                            "  |  <slot 17> : coalesce(DictDecode(18: S_ADDRESS, [<place-holder>]), " +
                            "CAST(4: S_NATIONKEY AS VARCHAR))"),
                    plan);

            // select unsupported_function(dict_col), dict_col from table1 join table2
            // Add Decode Node before unsupported Projection 2
            sql = "select coalesce(l.S_ADDRESS,l.S_NATIONKEY),l.S_ADDRESS,r.S_ADDRESS " +
                    "from supplier l join supplier r on l.s_suppkey = r.s_suppkey";
            plan = getFragmentPlan(sql);

            Assertions.assertTrue(plan.contains("  3:Project\n" +
                    "  |  <slot 17> : coalesce(DictDecode(18: S_ADDRESS, [<place-holder>]), CAST(4: S_NATIONKEY AS VARCHAR))\n" +
                    "  |  <slot 18> : 18: S_ADDRESS\n" +
                    "  |  <slot 19> : 19: S_ADDRESS"), plan);
            Assertions.assertTrue(plan.contains("  4:Decode\n" +
                    "  |  <dict id 18> : <string id 3>\n" +
                    "  |  <dict id 19> : <string id 11>"), plan);

            // select unsupported_function(dict_col), supported_func(dict_col), dict_col
            // from table1 join table2;
            // projection has both supported operator and no-supported operator
            sql = "select coalesce(l.S_ADDRESS,l.S_NATIONKEY), upper(l.S_ADDRESS), l.S_ADDRESS " +
                    "from supplier l join supplier r on l.s_suppkey = r.s_suppkey";
            plan = getFragmentPlan(sql);

            Assertions.assertTrue(plan.contains("<dict id 19> : <string id 3>"), plan);

            // select unsupported_function(dict_col), supported_func(table2.dict_col2), table2.dict_col2
            // from table1 join table2;
            // left table don't support dict optimize, but right table support it
            sql = "select coalesce(l.S_ADDRESS,l.S_NATIONKEY), upper(r.P_MFGR),r.P_MFGR " +
                    "from supplier l join part_v2 r on l.s_suppkey = r.P_PARTKEY";
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("  5:Decode\n" +
                    "  |  <dict id 22> : <string id 11>"), plan);

            Assertions.assertTrue(plan.contains("  4:Project\n" +
                    "  |  <slot 19> : coalesce(DictDecode(21: S_ADDRESS, [<place-holder>]), CAST(4: S_NATIONKEY AS VARCHAR))\n" +
                    "  |  <slot 20> : DictDecode(22: P_MFGR, [upper(<place-holder>)])\n" +
                    "  |  <slot 22> : 22: P_MFGR"), plan);

        } finally {
            FeConstants.runningUnitTest = false;
        }
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
        assertNotContains(plan, "DecodeNode");

        sql = "select max(S_ADDRESS), approx_count_distinct(S_ADDRESS) from supplier";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 12> : <string id 9>"), plan);
    }

    @Test
    public void testWithCaseWhen() throws Exception {
        String sql;
        String plan;
        // test if with only one dictionary column
        sql = "select case when S_ADDRESS = 'key' then 1 else 0 end from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("9 <-> DictDecode(10: S_ADDRESS, [if(<place-holder> = 'key', 1, 0)]"), plan);
        Assertions.assertTrue(plan.contains("dict_col=S_ADDRESS"), plan);
        // test case when result no-string
        sql = "select case when S_ADDRESS = 'key' then 1 when S_ADDRESS = '2' then 2 else 0 end from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("     dict_col=S_ADDRESS"), plan);
        // test case when output variable
        sql =
                "select case when S_ADDRESS = 'key' then 1 when S_ADDRESS = '2' then 2 else S_NATIONKEY end from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("     dict_col=S_ADDRESS"), plan);
        Assertions.assertTrue(plan.contains(
                "  |  9 <-> CASE WHEN DictDecode(10: S_ADDRESS, [<place-holder> = 'key']) " +
                        "THEN 1 WHEN DictDecode(10: S_ADDRESS, [<place-holder> = '2']) THEN 2 ELSE 4: S_NATIONKEY END"), plan);
        // test case when with common expression 1
        sql = "select S_ADDRESS = 'key' , " +
                "case when S_ADDRESS = 'key' then 1 when S_ADDRESS = '2' then 2 else 3 end from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  1:Project\n" +
                "  |  output columns:\n" +
                "  |  9 <-> DictDecode(11: S_ADDRESS, [<place-holder> = 'key'])\n" +
                "  |  10 <-> DictDecode(11: S_ADDRESS, [CASE WHEN <place-holder> = 'key' THEN 1 " +
                "WHEN <place-holder> = '2' THEN 2 ELSE 3 END])\n" +
                "  |  cardinality: 1"), plan);
        Assertions.assertTrue(plan.contains("     dict_col=S_ADDRESS"), plan);
        // test case when result string
        sql = "select case when S_ADDRESS = 'key' then 'key1' when S_ADDRESS = '2' " +
                "then 'key2' else 'key3' end from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("DictDecode(10: S_ADDRESS, [CASE WHEN <place-holder> = 'key' THEN 'key1'" +
                " WHEN <place-holder> = '2' THEN 'key2' ELSE 'key3' END])"), plan);
        // test case when with unsupported function call
        sql = "select case when S_ADDRESS = 'key' then rand() when S_ADDRESS = '2' " +
                "then 'key2' else 'key3' end from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(" |  9 <-> CASE WHEN DictDecode(10: S_ADDRESS, [<place-holder> = 'key']) " +
                "THEN CAST(rand() AS VARCHAR) " +
                "WHEN DictDecode(10: S_ADDRESS, [<place-holder> = '2']) " +
                "THEN 'key2' ELSE 'key3' END"), plan);
        assertNotContains(plan, "DecodeNode");
        // test multi low cardinality column input
        sql = "select if(S_ADDRESS = 'key', S_COMMENT, 'y') from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  |  9 <-> if[(DictDecode(10: S_ADDRESS, [<place-holder> = 'key']), " +
                "[7: S_COMMENT, VARCHAR, false], 'y'); args: BOOLEAN,VARCHAR,VARCHAR; " +
                "result: VARCHAR; args nullable: true; result nullable: true]"), plan);
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
        Assertions.assertTrue(plan.contains("  4:Project\n" +
                "  |  <slot 33> : DictDecode(34: S_ADDRESS, [<place-holder>])"), plan);
    }

    @Test
    public void testProject() throws Exception {
        String sql;
        String plan;

        // test cast low cardinality column as other type column
        sql = "select cast (S_ADDRESS as datetime)  from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("     dict_col=S_ADDRESS"), plan);

        // test simple string function
        sql = "select substring(S_ADDRESS,1,2)  from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("9 <-> DictDecode(10: S_ADDRESS, [substring(<place-holder>, 1, 2)])"), plan);

        // test simple string function with two column
        // test worth for rewrite
        sql = "select substring(S_ADDRESS, S_SUPPKEY, 2)  from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(
                "9 <-> substring[([3: S_ADDRESS, VARCHAR, false], [1: S_SUPPKEY, INT, false], 2); " +
                        "args: VARCHAR,INT,INT; result: VARCHAR; args nullable: false; result nullable: true]"), plan);
        assertNotContains(plan, "DecodeNode");

        // test string function with one column
        sql = "select substring(S_ADDRESS, S_ADDRESS, 1) from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(
                "9 <-> DictDecode(10: S_ADDRESS, [substring(<place-holder>, CAST(<place-holder> AS INT), 1)])"), plan);

        // test simple string function with two column
        // test worth for rewrite
        sql = "select substring(upper(S_ADDRESS), S_SUPPKEY, 2) from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(
                "9 <-> substring[(DictDecode(10: S_ADDRESS, [upper(<place-holder>)]), [1: S_SUPPKEY, INT, false], 2); " +
                        "args: VARCHAR,INT,INT; result: VARCHAR; args nullable: true; result nullable: true]"), plan);

        // test two dictionary column
        // test worth for rewrite
        sql = "select concat(S_ADDRESS, S_COMMENT) from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(
                "  |  9 <-> concat[([3: S_ADDRESS, VARCHAR, false], [7: S_COMMENT, VARCHAR, false]); " +
                        "args: VARCHAR; result: VARCHAR; args nullable: false; result nullable: true]"), plan);
        // Test common expression reuse 1
        // couldn't reuse case
        // DictExpr return varchar and int
        sql = "select if(S_SUPPKEY='kks', upper(S_ADDRESS), S_COMMENT), upper(S_ADDRESS) from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("9 <-> if[(cast([1: S_SUPPKEY, INT, false] as VARCHAR(1048576)) = 'kks', " +
                "[12: expr, VARCHAR, true], [7: S_COMMENT, VARCHAR, false]); args: BOOLEAN,VARCHAR,VARCHAR; " +
                "result: VARCHAR; args nullable: true; result nullable: true]\n" +
                "  |  10 <-> [12: expr, VARCHAR, true]\n" +
                "  |  common expressions:\n" +
                "  |  12 <-> DictDecode(11: S_ADDRESS, [upper(<place-holder>)])"), plan);

        // TODO: return dict column for this case
        // common expression reuse 2
        // test input two string column
        sql = "select if(S_ADDRESS='kks', S_COMMENT, S_COMMENT) from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(
                "9 <-> if[(DictDecode(10: S_ADDRESS, [<place-holder> = 'kks']), " +
                        "[7: S_COMMENT, VARCHAR, false], [7: S_COMMENT, VARCHAR, false]); " +
                        "args: BOOLEAN,VARCHAR,VARCHAR; result: VARCHAR; args nullable: true; " +
                        "result nullable: true]"), plan);
        assertNotContains(plan, "DecodeNode");

        // common expression reuse 3
        sql =
                "select if(S_ADDRESS='kks', upper(S_COMMENT), S_COMMENT), concat(upper(S_COMMENT), S_ADDRESS) from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("9 <-> if[(DictDecode(11: S_ADDRESS, [<place-holder> = 'kks'])"), plan);

        // support(support(unsupport(Column), unsupport(Column)))
        sql = "select REVERSE(SUBSTR(LEFT(REVERSE(S_ADDRESS),INSTR(REVERSE(S_ADDRESS),'/')-1),5)) FROM supplier";
        plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 9> : reverse(substr(left(DictDecode(10: S_ADDRESS, [reverse(<place-holder>)])");
    }

    @Test
    public void testScanPredicate() throws Exception {
        String sql;
        String plan;

        // Test Predicate dict columns both has support predicate and no-support predicate
        sql = "select count(*) from " +
                "supplier where S_ADDRESS like '%A%' and S_ADDRESS not like '%B%'";
        plan = getCostExplain(sql);
        Assertions.assertFalse(plan.contains(" dict_col=S_ADDRESS "));

        sql = "select * from supplier l join supplier r on " +
                "l.S_NAME = r.S_NAME where upper(l.S_ADDRESS) like '%A%' and upper(l.S_ADDRESS) not like '%B%'";
        plan = getCostExplain(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     table: supplier, rollup: supplier\n" +
                "     preAggregation: on\n" +
                "     Predicates: DictDecode(17: S_ADDRESS, [(upper(<place-holder>) LIKE '%A%')" +
                " AND (NOT (upper(<place-holder>) LIKE '%B%'))])\n" +
                "     dict_col=S_ADDRESS,S_COMMENT");

        // Test Simple Filter
        sql = "select count(*) from supplier where S_ADDRESS = 'kks' group by S_ADDRESS ";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("DictDecode(10: S_ADDRESS, [<place-holder> = 'kks'])"), plan);
        Assertions.assertTrue(plan.contains("group by: 10: S_ADDRESS"), plan);

        // Test unsupported predicate
        // binary columns only support slotRef op const
        sql = "select count(*) from supplier where S_ADDRESS + 2 > 'kks' group by S_ADDRESS";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("group by: 10: S_ADDRESS"), plan);

        // Test Predicate with if predicate
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks', true, false)";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("PREDICATES: DictDecode(12: S_ADDRESS, [<place-holder> = 'kks'])"),
                plan);

        // Test single input Expression
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks', cast(S_ADDRESS as boolean), false)";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains(
                        "PREDICATES: DictDecode(12: S_ADDRESS, [if(<place-holder> = 'kks', " +
                                "CAST(<place-holder> AS BOOLEAN), FALSE)])"),
                plan);

        // Test multi input Expression with DictColumn
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks',cast(S_COMMENT as boolean), false)";
        plan = getFragmentPlan(sql);
        assertContains(plan,
                "predicates: if(DictDecode(12: S_ADDRESS, [<place-holder> = 'kks']), " +
                        "DictDecode(13: S_COMMENT, [CAST(<place-holder> AS BOOLEAN)]), FALSE)");

        // Test multi input Expression with No-String Column
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks',cast(S_NAME as boolean), false)";
        plan = getFragmentPlan(sql);
        assertContains(plan,
                "predicates: if(DictDecode(12: S_ADDRESS, [<place-holder> = 'kks']), CAST(2: S_NAME AS BOOLEAN), FALSE)");

        // Test Two input column. one could apply the other couldn't apply
        // The first expression that can accept a full rewrite. the second couldn't apply
        sql = "select count(*) from supplier where S_ADDRESS = 'kks' and S_COMMENT not like '%kks%'";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("PREDICATES: DictDecode(12: S_ADDRESS, [<place-holder> = 'kks'])," +
                " DictDecode(13: S_COMMENT, [NOT (<place-holder> LIKE '%kks%')])"), plan);

        // Test Two input column. one could apply the other couldn't apply
        // Two Predicate, The first expression that can accept a partial rewrite.
        sql = "select count(*) from supplier where if(S_ADDRESS = 'kks',cast(S_COMMENT as boolean), false) " +
                "and S_COMMENT not like '%kks%'";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:SELECT\n" +
                "  |  predicates: if(DictDecode(12: S_ADDRESS, [<place-holder> = 'kks']), " +
                "DictDecode(13: S_COMMENT, [CAST(<place-holder> AS BOOLEAN)]), FALSE)\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: supplier\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: DictDecode(13: S_COMMENT, [NOT (<place-holder> LIKE '%kks%')])");

    }

    @Test
    public void testAggHaving() throws Exception {
        String sql = "select count(*) from supplier group by S_ADDRESS having S_ADDRESS = 'kks' ";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("DictDecode(10: S_ADDRESS, [<place-holder> = 'kks'])"), plan);
        Assertions.assertTrue(plan.contains("group by: 10: S_ADDRESS"), plan);

        sql = "select count(*) as b from supplier group by S_ADDRESS having b > 3";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  |  group by: 10: S_ADDRESS\n" +
                "  |  having: 9: count > 3"), plan);
        // test couldn't push down predicate
        sql = "select sum(S_NATIONKEY) a, sum(S_ACCTBAL) as b, S_ADDRESS as c from supplier group by S_ADDRESS " +
                "having a < b*1.2 or c not like '%open%'";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Decode\n" +
                "  |  <dict id 11> : <string id 3>"), plan);

        // test couldn't push down having predicate
        sql = "SELECT count(*) a FROM supplier having max(S_ADDRESS)='123'";
        plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("DecodeNode"), plan);
    }

    @Test
    public void testJoin() throws Exception {
        String sql;
        String plan;
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        try {
            sql = "select count(*) from supplier l " +
                    "join [shuffle] (select max(S_ADDRESS) as S_ADDRESS from supplier) r " +
                    "on l.S_ADDRESS = r.S_ADDRESS;";
            plan = getVerboseExplain(sql);
            assertNotContains(plan, "DecodeNode");
            sql = "select count(*) from supplier l " +
                    "join [broadcast] (select max(S_ADDRESS) as S_ADDRESS from supplier) r " +
                    "on l.S_ADDRESS = r.S_ADDRESS;";
            plan = getVerboseExplain(sql);
            assertNotContains(plan, "DecodeNode");

            sql = "select count(*) from supplier l " +
                    "join [broadcast] (select max(id_int) as id_int from table_int) r " +
                    "on l.S_ADDRESS = r.id_int where l.S_ADDRESS not like '%key%'";
            plan = getVerboseExplain(sql);
            assertNotContains(plan, "DecodeNode");

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
            Assertions.assertTrue(plan.contains("OutPut Partition: HASH_PARTITIONED: 9: S_SUPPKEY, 17"), plan);
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }

        sql = "select * from test.join1 right join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  5:Decode\n" +
                "  |  <dict id 7> : <string id 3>\n" +
                "  |  <dict id 8> : <string id 6>"), plan);

        sql = "SELECT * \n" +
                "FROM   emp \n" +
                "WHERE  EXISTS (SELECT dept.dept_id \n" +
                "               FROM   dept \n" +
                "               WHERE  emp.dept_id = dept.dept_id \n" +
                "               ORDER  BY state) \n" +
                "ORDER  BY hiredate";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  7:Decode\n" +
                "  |  <dict id 10> : <string id 2>"), plan);

        sql = "select * from join1 join pushdown_test on join1.id = pushdown_test.k1;";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  6:Decode\n" +
                "  |  <dict id 17> : <string id 3>\n" +
                "  |  <dict id 16> : <string id 12>"), plan);
        Assertions.assertTrue(plan.contains("INNER JOIN (BROADCAST)"), plan);

        sql = "select part_v2.p_partkey from lineitem join part_v2 on L_COMMENT = hex(P_NAME);";
        plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("DecodeNode"), plan);

        // TopN with HashJoinNode
        sql = "select * from supplier l join supplier_nullable r where l.S_SUPPKEY = r.S_SUPPKEY " +
                "order by l.S_ADDRESS limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, plan, "  4:TOP-N\n" +
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
        assertContains(plan, plan, "  9:Decode\n" +
                "  |  <dict id 21> : <string id 17>\n" +
                "  |  <dict id 22> : <string id 18>\n" +
                "  |  \n" +
                "  8:Project\n" +
                "  |  <slot 21> : 21: max\n" +
                "  |  <slot 22> : 22: max\n" +
                "  |  \n" +
                "  7:AGGREGATE (merge finalize)\n" +
                "  |  output: max(21: max), max(22: max)\n" +
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
        assertContains(plan, plan, "  20:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 30: S_ADDRESS, 31: S_COMMENT\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  \n" +
                "  |----19:EXCHANGE\n" +
                "  |    \n" +
                "  16:EXCHANGE");
        assertContains(plan, plan, "Decode");
        plan = getThriftPlan(sql);
        assertNotContains(plan.split("\n")[1], "query_global_dicts");
    }

    @Test
    public void testJoinGlobalDict() throws Exception {
        String sql =
                "select part_v2.P_COMMENT from lineitem join part_v2 " +
                        "on L_PARTKEY = p_partkey where p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2';";
        String plan = getThriftPlan(sql);
        Assertions.assertTrue(plan.contains("dict_string_id_to_int_ids:{}"), plan);
        Assertions.assertTrue(plan.contains("DictDecode(28: P_MFGR, [<place-holder> IN ('MFGR#1', 'MFGR#2')])"), plan);
        Assertions.assertTrue(plan.contains("output_sink:TDataSink(type:RESULT_SINK, " +
                "result_sink:TResultSink(type:MYSQL_PROTOCAL, " +
                "is_binary_row:false)), partition:TDataPartition(type:RANDOM, partition_exprs:[]), " +
                "query_global_dicts:[TGlobalDict(columnId:28"), plan);
    }

    @Test
    public void testCountDistinctMultiColumns() throws Exception {
        FeConstants.runningUnitTest = true;
        try {
            String sql = "select count(distinct S_SUPPKEY, S_COMMENT) from supplier";
            String plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("AGGREGATE (update serialize)\n" +
                    "  |  output: count(if(1: S_SUPPKEY IS NULL, NULL, 7: S_COMMENT))"), plan);

            sql = "select count(distinct S_ADDRESS, S_COMMENT) from supplier";
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("4:Decode\n" +
                    "  |  <dict id 10> : <string id 3>\n" +
                    "  |  <dict id 11> : <string id 7>"), plan);
            Assertions.assertTrue(plan.contains("  5:AGGREGATE (update serialize)\n" +
                    "  |  output: count(if(3: S_ADDRESS IS NULL, NULL, 7: S_COMMENT))"), plan);
        } finally {
            FeConstants.runningUnitTest = false;
        }
    }

    @Test
    public void testGroupByWithOrderBy() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        try {
            String sql;
            String plan;

            sql = "select max(S_NAME) as b from supplier group by S_ADDRESS order by b";
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("group by: 10: S_ADDRESS"), plan);

            sql = "select S_ADDRESS from supplier order by S_ADDRESS";
            plan = getVerboseExplain(sql);
            Assertions.assertTrue(plan.contains("  1:SORT\n" +
                    "  |  order by: [9, INT, false] ASC"), plan);

            sql = "select S_NAME from supplier_nullable order by upper(S_ADDRESS), S_NAME";
            plan = getVerboseExplain(sql);
            Assertions.assertTrue(plan.contains("  2:SORT\n" +
                    "  |  order by: [11, INT, true] ASC, [2, VARCHAR, false] ASC"), plan);
            Assertions.assertTrue(plan.contains("Global Dict Exprs:\n" +
                    "    11: DictDefine(10: S_ADDRESS, [upper(<place-holder>)])"), plan);
            sql = "select substr(S_ADDRESS, 0, 1) from supplier group by substr(S_ADDRESS, 0, 1) " +
                    "order by substr(S_ADDRESS, 0, 1)";
            plan = getVerboseExplain(sql);
            Assertions.assertTrue(plan.contains("  7:Decode\n" +
                    "  |  <dict id 11> : <string id 9>"), plan);
            Assertions.assertTrue(plan.contains("  5:SORT\n" +
                    "  |  order by: [11, INT, true] ASC"), plan);
            Assertions.assertTrue(plan.contains("Global Dict Exprs:\n" +
                    "    11: DictDefine(10: S_ADDRESS, [substr(<place-holder>, 0, 1)])"), plan);

            sql = "select approx_count_distinct(S_ADDRESS), upper(S_ADDRESS) from supplier " +
                    " group by upper(S_ADDRESS)" +
                    "order by 2";
            plan = getVerboseExplain(sql);
            assertContains(plan, "  2:AGGREGATE (update serialize)\n" +
                    "  |  STREAMING\n" +
                    "  |  aggregate: approx_count_distinct[([11: S_ADDRESS, INT, false]); args: INT; " +
                    "result: VARBINARY; args nullable: false; result nullable: false]\n" +
                    "  |  group by: [12: upper, INT, true]\n" +
                    "  |  cardinality: 1");
            assertContains(plan, "Global Dict Exprs:\n" +
                    "    12: DictDefine(11: S_ADDRESS, [upper(<place-holder>)])");
            // TODO add a case: Decode node before Sort Node
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
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
        assertContains(plan, "  4:ANALYTIC\n" +
                "  |  functions: [, row_number[(); args: ; result: BIGINT; args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [16: L_COMMENT, VARCHAR(44), false]\n" +
                "  |  order by: [2: L_PARTKEY, INT, false] ASC");
        assertContains(plan, "  3:Decode\n" +
                "  |  <dict id 20> : <string id 16>");
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: [20, INT, false] ASC, [2, INT, false] ASC\n" +
                "  |  analytic partition by: [20, INT, false]");
        assertContains(plan, "  1:PARTITION-TOP-N\n" +
                "  |  partition by: [20: L_COMMENT, INT, false] ");
        assertContains(plan, "  |  order by: [20, INT, false] ASC, [2, INT, false] ASC");

        // row number
        sql = "select * from (select L_COMMENT,l_quantity, row_number() over " +
                "(partition by L_COMMENT order by l_quantity desc) rn from lineitem )t where rn <= 10;";
        plan = getCostExplain(sql);
        assertContains(plan, "  4:ANALYTIC\n" +
                "  |  functions: [, row_number[(); args: ; result: BIGINT; args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [16: L_COMMENT, VARCHAR(44), false]\n" +
                "  |  order by: [5: L_QUANTITY, DOUBLE, false] DESC");
        assertContains(plan, "  3:Decode\n" +
                "  |  <dict id 19> : <string id 16>");
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: [19, INT, false] ASC, [5, DOUBLE, false] DESC\n" +
                "  |  analytic partition by: [19, INT, false]");
        assertContains(plan, "  1:PARTITION-TOP-N\n" +
                "  |  partition by: [19: L_COMMENT, INT, false] \n" +
                "  |  partition limit: 10\n" +
                "  |  order by: [19, INT, false] ASC, [5, DOUBLE, false] DESC\n" +
                "  |  offset: 0");

        // rank
        sql = "select * from (select L_COMMENT,l_quantity, rank() over " +
                "(partition by L_COMMENT order by l_quantity desc) rn from lineitem )t where rn <= 10;";
        plan = getCostExplain(sql);
        assertContains(plan, "  4:ANALYTIC\n" +
                "  |  functions: [, rank[(); args: ; result: BIGINT; args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [16: L_COMMENT, VARCHAR(44), false]\n" +
                "  |  order by: [5: L_QUANTITY, DOUBLE, false] DESC");
        assertContains(plan, "  3:Decode\n" +
                "  |  <dict id 19> : <string id 16>");
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: [19, INT, false] ASC, [5, DOUBLE, false] DESC\n" +
                "  |  analytic partition by: [19, INT, false]");
        assertContains(plan, "  1:PARTITION-TOP-N\n" +
                "  |  type: RANK\n" +
                "  |  partition by: [19: L_COMMENT, INT, false] \n" +
                "  |  partition limit: 10\n" +
                "  |  order by: [19, INT, false] ASC, [5, DOUBLE, false] DESC");

        // mul-column partition by
        sql = "select * from (select L_COMMENT,l_quantity, rank() over " +
                "(partition by L_COMMENT, l_shipmode order by l_quantity desc) rn from lineitem )t where rn <= 10;";
        plan = getCostExplain(sql);
        assertContains(plan, "  4:ANALYTIC\n" +
                "  |  functions: [, rank[(); args: ; result: BIGINT; args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [16: L_COMMENT, VARCHAR(44), false], [15: L_SHIPMODE, VARCHAR, false]\n" +
                "  |  order by: [5: L_QUANTITY, DOUBLE, false] DESC");
        assertContains(plan, "  3:Decode\n" +
                "  |  <dict id 19> : <string id 16>");
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: [19, INT, false] ASC, [15, VARCHAR, false] ASC, [5, DOUBLE, false] DESC\n" +
                "  |  analytic partition by: [19, INT, false], [15: L_SHIPMODE, VARCHAR, false]");
        assertContains(plan, "  1:PARTITION-TOP-N\n" +
                "  |  type: RANK\n" +
                "  |  partition by: [19: L_COMMENT, INT, false] , [15: L_SHIPMODE, CHAR, false] \n" +
                "  |  partition limit: 10\n" +
                "  |  order by: [19, INT, false] ASC, [15, VARCHAR, false] ASC, [5, DOUBLE, false] DESC\n" +
                "  |  offset: 0");

        // partition column with expr
        sql = "SELECT S_ADDRESS, MAX(S_SUPPKEY) over(partition by S_COMMENT order by S_NAME) FROM supplier_nullable";
        plan = getCostExplain(sql);
        assertContains(plan, "  3:ANALYTIC\n" +
                "  |  functions: [, max[([1: S_SUPPKEY, INT, false]); args: INT; result: INT; " +
                "args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [7: S_COMMENT, VARCHAR(101), false]\n" +
                "  |  order by: [2: S_NAME, VARCHAR, false] ASC");
        assertContains(plan, "  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 7>");
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: [11, INT, false] ASC, [2, VARCHAR, false] ASC\n" +
                "  |  analytic partition by: [11, INT, false]");

        sql = "SELECT S_ADDRESS, MAX(S_SUPPKEY) over(partition by concat(S_COMMENT, 'a') order by S_NAME) FROM supplier_nullable";
        plan = getCostExplain(sql);
        assertContains(plan, "  4:ANALYTIC\n" +
                "  |  functions: [, max[([9: S_SUPPKEY, INT, false]); args: INT; result: INT; " +
                "args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [17: concat, VARCHAR, true]\n" +
                "  |  order by: [2: S_NAME, VARCHAR, false] ASC");
        assertContains(plan, "  3:Decode\n" +
                "  |  <dict id 20> : <string id 17>");
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: [20, INT, true] ASC, [2, VARCHAR, false] ASC\n" +
                "  |  analytic partition by: [20, INT, true]");

        // partition column is not dict column
        sql = "SELECT S_ADDRESS, MAX(S_SUPPKEY) over(partition by S_NAME order by S_COMMENT) FROM supplier_nullable";
        plan = getCostExplain(sql);
        assertContains(plan, "  3:ANALYTIC\n" +
                "  |  functions: [, max[([1: S_SUPPKEY, INT, false]); args: INT; result: INT; " +
                "args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [2: S_NAME, VARCHAR, false]\n" +
                "  |  order by: [7: S_COMMENT, VARCHAR(101), false] ASC");
        assertContains(plan, "  2:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 7>");
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: [2, VARCHAR, false] ASC, [11, INT, false] ASC\n" +
                "  |  analytic partition by: [2: S_NAME, VARCHAR, false]");

        // there is not DecodeNode
        sql = "SELECT /*+SET_VAR(cbo_enable_low_cardinality_optimize=false)*/" +
                "S_ADDRESS, MAX(S_SUPPKEY) over(partition by S_COMMENT order by S_NAME) FROM supplier_nullable";
        plan = getCostExplain(sql);
        assertContains(plan, "  2:ANALYTIC\n" +
                "  |  functions: [, max[([1: S_SUPPKEY, INT, false]); args: INT; result: INT; " +
                "args nullable: false; result nullable: true], ]\n" +
                "  |  partition by: [7: S_COMMENT, VARCHAR, false]\n" +
                "  |  order by: [2: S_NAME, VARCHAR, false] ASC");
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: [7, VARCHAR, false] ASC, [2, VARCHAR, false] ASC\n" +
                "  |  analytic partition by: [7: S_COMMENT, VARCHAR, false]");

    }

    @Test
    public void testProjectionPredicate() throws Exception {
        String sql = "select count(t.a) from(select S_ADDRESS in ('kks', 'kks2') as a from supplier) as t";
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(" dict_col=S_ADDRESS"), plan);
        Assertions.assertTrue(plan.contains("9 <-> DictDecode(11: S_ADDRESS, [<place-holder> IN ('kks', 'kks2')])"), plan);

        sql = "select count(t.a) from(select S_ADDRESS = 'kks' as a from supplier) as t";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(" dict_col=S_ADDRESS"), plan);
        Assertions.assertTrue(plan.contains("9 <-> DictDecode(11: S_ADDRESS, [<place-holder> = 'kks'])"), plan);

        sql = "select count(t.a) from(select S_ADDRESS is null as a from supplier) as t";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(" dict_col=S_ADDRESS"), plan);
        Assertions.assertTrue(plan.contains("9 <-> DictDecode(11: S_ADDRESS, [<place-holder> IS NULL])"), plan);

        sql = "select count(t.a) from(select S_ADDRESS is not null as a from supplier) as t";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(" dict_col=S_ADDRESS"), plan);
        Assertions.assertTrue(plan.contains("9 <-> DictDecode(11: S_ADDRESS, [<place-holder> IS NOT NULL])"), plan);

        sql = "select count(t.a) from(select S_ADDRESS <=> 'kks' as a from supplier) as t";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("[3: S_ADDRESS, VARCHAR, false] <=> 'kks'"));

        sql = "select S_ADDRESS not like '%key%' from supplier";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(" dict_col=S_ADDRESS"), plan);

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select count(distinct S_ADDRESS), count(distinct S_NAME) as a from supplier_nullable";
        plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("multi_distinct_count[([9: count, VARBINARY, false]);"), plan);
        Assertions.assertTrue(plan.contains("multi_distinct_count[([11: S_ADDRESS, INT, true]);"), plan);
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testNestedExpressions() throws Exception {
        String sql;
        String plan;
        try {
            connectContext.getSessionVariable().setNewPlanerAggStage(2);
            sql = "select upper(lower(S_ADDRESS)) from supplier group by lower(S_ADDRESS);";
            plan = getVerboseExplain(sql);
            assertContains(plan, "  5:Project\n" +
                    "  |  output columns:\n" +
                    "  |  10 <-> DictDecode(12: lower, [upper(<place-holder>)])\n" +
                    "  |  cardinality: 1");
            Assertions.assertTrue(plan.contains("  4:AGGREGATE (merge finalize)\n" +
                    "  |  group by: [12: lower, INT, true]"), plan);
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
    }

    @Test
    public void testMultiMaxMin() throws Exception {
        String sql;
        String plan;
        try {
            connectContext.getSessionVariable().setNewPlanerAggStage(2);
            sql =
                    "select count(distinct S_ADDRESS), max(S_ADDRESS), count(distinct S_SUPPKEY) as a from supplier_nullable";
            plan = getVerboseExplain(sql);
            Assertions.assertTrue(plan.contains("1:AGGREGATE (update serialize)\n" +
                    "  |  aggregate: multi_distinct_count[([12: S_ADDRESS, INT, true]);"), plan);
            Assertions.assertTrue(plan.contains("3:AGGREGATE (merge finalize)\n" +
                    "  |  aggregate: multi_distinct_count[([9: count, VARBINARY, false]);"), plan);
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }

        try {
            connectContext.getSessionVariable().setNewPlanerAggStage(2);
            sql = "select min(distinct S_ADDRESS), max(S_ADDRESS) from supplier_nullable";
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("  1:AGGREGATE (update serialize)\n" +
                    "  |  output: min(11: S_ADDRESS), max(11: S_ADDRESS)"), plan);
            Assertions.assertTrue(plan.contains("  3:AGGREGATE (merge finalize)\n" +
                    "  |  output: min(12: min), max(13: max)"), plan);
            Assertions.assertTrue(plan.contains("  4:Decode\n" +
                    "  |  <dict id 12> : <string id 9>\n" +
                    "  |  <dict id 13> : <string id 10>"), plan);

            sql = "select max(upper(S_ADDRESS)) from supplier_nullable";
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("  5:Decode\n" +
                    "  |  <dict id 13> : <string id 10>"), plan);
            sql = "select max(if(S_ADDRESS='kks', upper(S_COMMENT), S_COMMENT)), " +
                    "min(upper(S_COMMENT)) from supplier_nullable " +
                    "group by upper(S_COMMENT)";
            plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("6:Decode\n" +
                    "  |  <dict id 16> : <string id 12>\n" +
                    "  |  \n" +
                    "  5:Project\n" +
                    "  |  <slot 11> : 11: max\n" +
                    "  |  <slot 16> : 16: min"), plan);
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
    }

    @Test
    public void testSubqueryWithLimit() throws Exception {
        String sql = "select t0.S_ADDRESS from (select S_ADDRESS, S_NATIONKEY from supplier_nullable limit 10) t0" +
                " inner join supplier on t0.S_NATIONKEY = supplier.S_NATIONKEY;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  2:Decode\n" +
                "  |  <dict id 17> : <string id 3>\n");
    }

    @Test
    public void testDecodeWithCast() throws Exception {
        String sql = "select reverse(conv(cast(S_ADDRESS as bigint), NULL, NULL)) from supplier";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 9> : reverse(conv(DictDecode(10: S_ADDRESS, [CAST(<place-holder> AS BIGINT)]), NULL, NULL))"), plan);
    }

    @Test
    public void testAssignWrongNullableProperty() throws Exception {
        String sql;
        String plan;

        sql = "SELECT S_ADDRESS, Dense_rank() OVER ( ORDER BY S_SUPPKEY) " +
                "FROM supplier UNION SELECT S_ADDRESS, Dense_rank() OVER ( ORDER BY S_SUPPKEY) FROM supplier;";
        plan = getFragmentPlan(sql);
        // SCAN->DECODE->SORT
        Assertions.assertTrue(plan.contains("Decode"), plan);

        // window function with full order by
        sql = "select rank() over (order by S_ADDRESS) as rk from supplier_nullable";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:ANALYTIC\n" +
                "  |  functions: [, rank(), ]\n" +
                "  |  order by: 3: S_ADDRESS ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                "  |  \n" +
                "  3:Decode\n" +
                "  |  <dict id 10> : <string id 3>");

        // Decode node under sort node
        sql = "select S_ADDRESS, S_COMMENT from (select S_ADDRESS, " +
                "S_COMMENT from supplier_nullable order by S_COMMENT limit 10) tb where S_ADDRESS = 'SS' order by S_ADDRESS ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:SORT\n" +
                "  |  order by: <slot 3> 3: S_ADDRESS ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  4:SELECT\n" +
                "  |  predicates: 3: S_ADDRESS = 'SS'\n" +
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
        Assertions.assertTrue(plan.contains("  3:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 7>"), plan);

        sql = "select hex(10), s_address from supplier";
        plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("Decode"), plan);

        sql = "SELECT SUM(count) FROM (SELECT CAST((CAST((((\"C\")||(CAST(s_address AS STRING ) ))) " +
                "BETWEEN (((\"T\")||(\"\"))) AND (\"\") AS BOOLEAN) = true) " +
                "AND (CAST((((\"C\")||(CAST(s_address AS STRING ) ))) BETWEEN (((\"T\")||(\"\"))) " +
                "AND (\"\") AS BOOLEAN) IS NOT NULL) AS INT) as count FROM supplier ) t;";
        plan = getFragmentPlan(sql);
        Assertions.assertFalse(plan.contains("DecodeNode"), plan);

        sql =
                "SELECT SUM(count) FROM (SELECT CAST((CAST((s_address) BETWEEN (((CAST(s_address AS STRING ) )||(\"\"))) " +
                        "AND (s_address) AS BOOLEAN) = true) AND (CAST((s_address) " +
                        "BETWEEN (((CAST(s_address AS STRING ) )||(\"\"))) AND (s_address) AS BOOLEAN) IS NOT NULL) AS INT) " +
                        "as count FROM supplier ) t;";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "DecodeNode");
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
        assertNotContains(plan, "DecodeNode");
    }

    @Test
    public void testProjectWithUnionEmptySet() throws Exception {
        String sql;
        String plan;
        sql = "select t1a from test_all_type group by t1a union all select v4 from t1 where false";
        plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 15> : DictDecode(16: t1a, [<place-holder>])"), plan);

        // COW Case
        sql = "SELECT 'all', 'allx' where 1 = 2 union all select distinct S_ADDRESS, S_ADDRESS from supplier;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 14> : 17: expr\n" +
                "  |  <slot 15> : DictDecode(16: S_ADDRESS, [<place-holder>])\n" +
                "  |  common expressions:\n" +
                "  |  <slot 17> : DictDecode(16: S_ADDRESS, [<place-holder>])");

        sql = "SELECT 'all', 'all', 'all', 'all' where 1 = 2 union all " +
                "select distinct S_ADDRESS, S_SUPPKEY + 1, S_SUPPKEY + 1, S_ADDRESS + 1 from supplier;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 20> : DictDecode(24: S_ADDRESS, [<place-holder>])\n" +
                "  |  <slot 21> : 25: cast\n" +
                "  |  <slot 22> : CAST(15: expr AS VARCHAR)\n" +
                "  |  <slot 23> : CAST(DictDecode(24: S_ADDRESS, [CAST(<place-holder> AS DOUBLE)]) + 1.0 AS VARCHAR)\n" +
                "  |  common expressions:\n" +
                "  |  <slot 25> : CAST(15: expr AS VARCHAR)\n" +
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
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setEnablePipelineEngine(false);

        Assertions.assertTrue(
                plan.contains("query_global_dicts:[TGlobalDict(columnId:28, strings:[6D 6F 63 6B], ids:[1]"));
    }

    @Test
    public void testMetaScan() throws Exception {
        String sql = "select max(v1), min(v1) from t0 [_META_]";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("  0:MetaScan\n" +
                "     Table: t0\n" +
                "     <id 6> : max_v1\n" +
                "     <id 7> : min_v1"), plan);
        String thrift = getThriftPlan(sql);
        Assertions.assertTrue(thrift.contains("id_to_names:{6=max_v1, 7=min_v1}"));
    }

    @Test
    public void testMetaScan2() throws Exception {
        String sql = "select max(t1c), min(t1d), dict_merge(t1a, 255) from test_all_type [_META_]";
        String plan = getFragmentPlan(sql);

        Assertions.assertTrue(plan.contains("  0:MetaScan\n" +
                "     Table: test_all_type\n" +
                "     <id 16> : dict_merge_t1a\n" +
                "     <id 14> : max_t1c\n" +
                "     <id 15> : min_t1d"), plan);

        String thrift = getThriftPlan(sql);
        Assertions.assertTrue(thrift.contains("TFunctionName(function_name:dict_merge), " +
                "binary_type:BUILTIN, arg_types:[TTypeDesc(types:[TTypeNode(type:ARRAY), " +
                "TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))]), " +
                "TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:INT))])]"));
    }

    @Test
    public void testMetaScan3() throws Exception {
        String sql = "select max(t1c), min(t1d), dict_merge(t1a, 255) from test_all_type [_META_]";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update serialize)\n" +
                "  |  output: max(max_t1c), min(min_t1d), dict_merge(dict_merge_t1a, 255)\n" +
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
    public void testMetaScanException() throws Exception {
        String sql = "select dict_merge(t1a, 10000000000) from test_all_type [_META_]";
        try {
            String plan = getFragmentPlan(sql);
            Assertions.fail();
        } catch (SemanticException e) {
            assertContains(e.getMessage(), "The second parameter of DICT_MERGE must be a constant positive integer");
        }
        sql = "select dict_merge(t1a, -1) from test_all_type [_META_]";
        try {
            String plan = getFragmentPlan(sql);
            Assertions.fail();
        } catch (SemanticException e) {
            assertContains(e.getMessage(), "The second parameter of DICT_MERGE must be a constant positive integer");
        }

    }

    @Test
    public void testHasGlobalDictButNotFound() throws Exception {
        IDictManager dictManager = IDictManager.getInstance();

        new Expectations(dictManager) {
            {
                dictManager.hasGlobalDict(anyLong, ColumnId.create("S_ADDRESS"), anyLong);
                result = true;
                dictManager.getGlobalDict(anyLong, ColumnId.create("S_ADDRESS"));
                result = Optional.empty();
            }
        };

        String sql = "select S_ADDRESS from supplier group by S_ADDRESS";
        // Check No Exception
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "DecodeNode");
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
                "  |  <slot 18> : DictDefine(17: S_ADDRESS, [upper(<place-holder>)])");
    }

    @Test
    public void testCompoundPredicate() throws Exception {
        String sql = "select count(*) from supplier group by S_ADDRESS having " +
                "if(S_ADDRESS > 'a' and S_ADDRESS < 'b', true, false)";
        String plan = getVerboseExplain(sql);
        assertContains(plan,
                "Predicates: DictDecode(10: S_ADDRESS, [(<place-holder> > 'a') AND (<place-holder> < 'b')])");

        sql = "select count(*) from supplier group by S_ADDRESS having " +
                "if(not S_ADDRESS like '%a%' and S_ADDRESS < 'b', true, false)";
        plan = getVerboseExplain(sql);
        assertContains(plan, "DictDecode(10: S_ADDRESS, [(NOT (<place-holder> LIKE '%a%')" +
                ") AND (<place-holder> < 'b')])");
    }

    @Test
    public void testComplexScalarOperator_1() throws Exception {
        String sql = "select case when s_address = 'test' then 'a' " +
                "when s_phone = 'b' then 'b' " +
                "when coalesce(s_address, 'c') = 'c' then 'c' " +
                "else 'a' end from supplier; ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 9> : CASE WHEN DictDecode(10: S_ADDRESS, [<place-holder> = 'test']) THEN 'a' " +
                "WHEN 5: S_PHONE = 'b' THEN 'b' " +
                "WHEN coalesce(DictDecode(10: S_ADDRESS, [<place-holder>]), 'c') = 'c' THEN 'c' " +
                "ELSE 'a' END\n" +
                "  |");

        sql = "select case when s_address = 'test' then 'a' " +
                "when s_phone = 'b' then 'b' " +
                "when upper(s_address) = 'c' then 'c' " +
                "else 'a' end from supplier; ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 9> : CASE WHEN DictDecode(10: S_ADDRESS, [<place-holder> = 'test']) THEN 'a' " +
                "WHEN 5: S_PHONE = 'b' THEN 'b' " +
                "WHEN DictDecode(10: S_ADDRESS, [upper(<place-holder>) = 'c']) THEN 'c' " +
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
                "     PREDICATES: ((5: S_PHONE = 'a') OR (coalesce(DictDecode(12: S_ADDRESS, [<place-holder>]), 'c') = 'c')) " +
                "OR (DictDecode(12: S_ADDRESS, [<place-holder> = 'address']))");

        sql = "select count(*) from supplier where s_phone = 'a' or upper(s_address) = 'c' " +
                "or s_address = 'address'";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: supplier\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: ((5: S_PHONE = 'a') OR (DictDecode(12: S_ADDRESS, [upper(<place-holder>) = 'c']))) " +
                "OR (DictDecode(12: S_ADDRESS, [<place-holder> = 'address']))");
    }

    @Test
    public void testAggWithProjection() throws Exception {
        String sql = "select cast(max(s_address) as date) from supplier";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "2:Project\n" +
                "  |  <slot 10> : DictDecode(12: max, [CAST(<place-holder> AS DATE)])\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: max(11: S_ADDRESS)\n" +
                "  |  group by: ");
    }

    @Test
    public void testJoinWithProjection() throws Exception {
        String sql = "select s_address, cast(t1.s_address as date), cast(t1.s_phone as date), upper(t1.s_address)," +
                " cast(t2.a as date), 123 from supplier t1 join (select max(s_address) a from supplier) t2 ";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "5:Project\n" +
                "  |  output columns:\n" +
                "  |  18 <-> DictDecode(23: S_ADDRESS, [CAST(<place-holder> AS DATE)])\n" +
                "  |  19 <-> cast([5: S_PHONE, CHAR, false] as DATE)\n" +
                "  |  20 <-> DictDecode(23: S_ADDRESS, [upper(<place-holder>)])\n" +
                "  |  21 <-> DictDecode(25: max, [CAST(<place-holder> AS DATE)])\n" +
                "  |  22 <-> 123\n" +
                "  |  23 <-> [23: S_ADDRESS, INT, false]");
        assertContains(plan, "Global Dict Exprs:\n" +
                "    25: DictDefine(24: S_ADDRESS, [<place-holder>])");
    }

    @Test
    public void testShuffleJoinProjection() throws Exception {
        String sql = "select MIN(a1), MAX(a2), MIN(concat(b1, '1')), MAX(concat(b2, '2')) from " +
                "(select upper(s_address) a1, s_address b1, S_SUPPKEY c1 from supplier) t1 " +
                "inner join[shuffle] " +
                "(select lower(s_address) a2, s_address b2, S_SUPPKEY c2 from supplier) t2 " +
                "on t1.c1 = t2.c2 " +
                "group by c1;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  Global Dict Exprs:\n" +
                "    34: DictDefine(25: S_ADDRESS, [upper(<place-holder>)])");
        assertContains(plan, "  Global Dict Exprs:\n" +
                "    27: DictDefine(26: S_ADDRESS, [lower(<place-holder>)])");

        assertContains(plan, "  Global Dict Exprs:\n" +
                "    32: DictDefine(25: S_ADDRESS, [concat(<place-holder>, '1')])\n" +
                "    33: DictDefine(26: S_ADDRESS, [concat(<place-holder>, '2')])\n" +
                "    34: DictDefine(25: S_ADDRESS, [upper(<place-holder>)])\n" +
                "    27: DictDefine(26: S_ADDRESS, [lower(<place-holder>)])\n" +
                "    28: DictDefine(25: S_ADDRESS, [concat(<place-holder>, '1')])\n" +
                "    29: DictDefine(26: S_ADDRESS, [concat(<place-holder>, '2')])\n" +
                "    30: DictDefine(25: S_ADDRESS, [upper(<place-holder>)])\n" +
                "    31: DictDefine(26: S_ADDRESS, [lower(<place-holder>)])\n");
    }

    @Test
    public void testShuffleJoinProjection2() throws Exception {
        String sql = "select MIN(a1), MAX(a2), MIN(concat(b1, '1')) d1, MAX(concat(b2, '2')) d2 from " +
                "(select upper(s_address) a1, s_address b1, S_SUPPKEY c1 from supplier) t1 " +
                "inner join[shuffle] " +
                "(select lower(s_address) a2, s_address b2, S_SUPPKEY c2 from supplier) t2 " +
                "on t1.c1 = t2.c2 " +
                "where a1 = 'XX'" +
                "group by c1 " +
                "having d2 = 'X2';";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  Global Dict Exprs:\n" +
                "    34: DictDefine(25: S_ADDRESS, [upper(<place-holder>)])");
        assertContains(plan, "  Global Dict Exprs:\n" +
                "    27: DictDefine(26: S_ADDRESS, [lower(<place-holder>)])");

        assertContains(plan, "  Global Dict Exprs:\n" +
                "    32: DictDefine(25: S_ADDRESS, [concat(<place-holder>, '1')])\n" +
                "    33: DictDefine(26: S_ADDRESS, [concat(<place-holder>, '2')])\n" +
                "    34: DictDefine(25: S_ADDRESS, [upper(<place-holder>)])\n" +
                "    27: DictDefine(26: S_ADDRESS, [lower(<place-holder>)])\n" +
                "    28: DictDefine(25: S_ADDRESS, [concat(<place-holder>, '1')])\n" +
                "    29: DictDefine(26: S_ADDRESS, [concat(<place-holder>, '2')])\n" +
                "    30: DictDefine(25: S_ADDRESS, [upper(<place-holder>)])\n" +
                "    31: DictDefine(26: S_ADDRESS, [lower(<place-holder>)])\n");
    }

    @Test
    public void testTopNWithProjection() throws Exception {
        String sql =
                "select t2.s_address, cast(t1.a as date), concat(t1.b, '') from (select max(s_address) a, min(s_phone) b " +
                        "from supplier group by s_address) t1 join (select s_address from supplier) t2 order by t1.a";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  10:Decode\n" +
                "  |  <dict id 22> : <string id 13>\n" +
                "  |  \n" +
                "  9:Project\n" +
                "  |  <slot 19> : 19: cast\n" +
                "  |  <slot 20> : 20: concat\n" +
                "  |  <slot 22> : 22: S_ADDRESS\n" +
                "  |  \n" +
                "  8:MERGING-EXCHANGE");
    }

    @Test
    public void testLogicalProperty() throws Exception {
        String sql = "select cast(max(s_address) as date) from supplier where s_suppkey = 1 group by S_PHONE";
        ExecPlan execPlan = getExecPlan(sql);
        OlapScanNode olapScanNode = (OlapScanNode) execPlan.getScanNodes().get(0);
        Assertions.assertEquals(0, olapScanNode.getBucketExprs().size());

        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertContains(plan, "3:Project\n" +
                "  |  <slot 10> : DictDecode(12: max, [CAST(<place-holder> AS DATE)])\n" +
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
    public void testProjectAggregate() throws Exception {
        String sql = "SELECT /*+SET_VAR(enable_eliminate_agg=false)*/ DISTINCT x1, x2 from (" +
                "   SELECT lower(t_a_0.`c`) AS x1, t_a_0.`c` AS x2 " +
                "   FROM (select distinct upper(S_ADDRESS) c from supplier) t_a_0) t_a_1;";

        String plan = getVerboseExplain(sql);
        assertContains(plan, "Global Dict Exprs:\n" +
                "    12: DictDefine(11: S_ADDRESS, [upper(<place-holder>)])\n" +
                "    13: DictDefine(11: S_ADDRESS, [lower(upper(<place-holder>))])");
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
        assertContains(plan, "  5:Project\n" +
                "  |  <slot 19> : if(DictDecode(22: S_ADDRESS, [<place-holder> IN ('hz', 'bj')]), " +
                "DictDecode(23: P_BRAND, [<place-holder>]), CAST(if(DictDecode(22: S_ADDRESS, [<place-holder> = 'hz']), " +
                "1035, 14: S_NATIONKEY) AS VARCHAR))\n" +
                "  |  <slot 20> : if(12: S_NAME = '', DictDecode(22: S_ADDRESS, [<place-holder>]), NULL)\n" +
                "  |  <slot 23> : 23: P_BRAND");
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
        assertContains(plan, "  5:Project\n" +
                "  |  <slot 19> : if(DictDecode(22: S_ADDRESS, [<place-holder> IN ('hz', 'bj')]), " +
                "DictDecode(23: P_BRAND, [<place-holder>]), CAST(if(DictDecode(22: S_ADDRESS, [<place-holder> = 'hz']), " +
                "1035, 14: S_NATIONKEY) AS VARCHAR))\n" +
                "  |  <slot 20> : if(12: S_NAME = '', DictDecode(22: S_ADDRESS, [<place-holder>]), NULL)");
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
        assertContains(plan, "  0:UNION\n" +
                "  |  \n" +
                "  |----12:Decode\n" +
                "  |    |  <dict id 56> : <string id 36>\n" +
                "  |    |  <dict id 57> : <string id 37>\n" +
                "  |    |  <dict id 58> : <string id 38>\n" +
                "  |    |  <dict id 59> : <string id 29>\n" +
                "  |    |  \n" +
                "  |    11:EXCHANGE\n" +
                "  |    \n" +
                "  6:Decode\n" +
                "  |  <dict id 50> : <string id 2>\n" +
                "  |  <dict id 51> : <string id 3>\n" +
                "  |  <dict id 52> : <string id 4>\n" +
                "  |  \n" +
                "  5:EXCHANGE\n");
    }

    @Test
    public void testShuffleJoinOn() throws Exception {
        String sql = "select * from " +
                "(select upper(P_NAME) u1, max(P_MFGR) from part_v2 group by u1) t1 " +
                "inner join[shuffle] " +
                "(select s_address l1, s_suppkey from supplier) t2 " +
                "on u1 = l1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  7:Decode\n" +
                "  |  <dict id 22> : <string id 12>\n" +
                "  |  \n" +
                "  6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 11: upper = 15: S_ADDRESS");
    }

    @Test
    public void testShuffleJoinOn2() throws Exception {
        String sql = "select * from (select * from " +
                "(select upper(P_NAME) u1, max(P_MFGR) from part_v2 group by u1) t1 " +
                "inner join[shuffle] " +
                "(select s_address l1, s_suppkey from supplier where s_address = 'BJ') t2 " +
                "on u1 = l1 " +
                "order by t2.s_suppkey limit 100) tt " +
                "where l1 = 'BJ'";
        String plan = getFragmentPlan(sql);
        // TODO: rewrite physical operator
        assertContains(plan, "  9:Decode\n" +
                "  |  <dict id 22> : <string id 12>");
    }

    @Test
    public void testDuplicateSortWindow() throws Exception {
        String sql = "SELECT S_ADDRESS, S_COMMENT, S_PHONE, " +
                "first_value(S_PHONE) OVER(ORDER BY S_ADDRESS, S_COMMENT rows between 1 preceding and 1 following) " +
                "FROM supplier " +
                "ORDER BY S_ADDRESS, S_COMMENT;\n";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 7>");
        assertContains(plan, "  5:SORT\n" +
                "  |  order by: <slot 3> 3: S_ADDRESS ASC, <slot 7> 7: S_COMMENT ASC\n" +
                "  |  offset: 0");
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: <slot 10> 10: S_ADDRESS ASC, <slot 11> 11: S_COMMENT ASC\n" +
                "  |  offset: 0");
    }

    @Test
    public void testCountDistinctPos() throws Exception {
        try {
            connectContext.getSessionVariable().setNewPlanerAggStage(3);
            String sql = "select count(distinct S_COMMENT), MAX(S_ADDRESS), MIN(S_NAME) from supplier";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "count(13: S_COMMENT), max(14: max), min(11: min)");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
    }

    @Test
    public void testDateDiff() throws Exception {
        String sql = "select date_diff(S_COMMENT, '2017-02-28', S_ADDRESS), S_COMMENT, S_ADDRESS " +
                "from supplier order by S_COMMENT, S_ADDRESS";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "date_diff(DictDecode(11: S_COMMENT, [<place-holder>]), " +
                "'2017-02-28 00:00:00', " +
                "DictDecode(10: S_ADDRESS, [CAST(<place-holder> AS DATETIME)]))");
    }

    @Test
    public void testJoinShuffle() throws Exception {
        String sql = "select S_ADDRESS, S_COMMENT from supplier join[shuffle] " +
                " low_card_t2 on S_ADDRESS = c_new where S_ADDRESS in ('a', 'b')";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  7:Decode\n" +
                "  |  <dict id 15> : <string id 7>\n" +
                "  |  cardinality: 1");
        assertContains(plan, "  1:EXCHANGE\n" +
                "     distribution type: SHUFFLE\n" +
                "     partition exprs: [3: S_ADDRESS, VARCHAR, false]\n" +
                "     cardinality: 1");
    }

    @Test
    public void testArrayFnSameStringFnReserve() throws Exception {
        String sql = "SELECT MAX(x1), MAX(y1) FROM (" +
                "   SELECT REVERSE(S_ADDRESS) x1, CONCAT(S_COMMENT, '1') y1, S_SUPPKEY FROM supplier_nullable) x " +
                "GROUP BY S_SUPPKEY ";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "17: DictDefine(13: S_ADDRESS, [reverse(<place-holder>)])");
        assertContains(plan, "15: DictDefine(13: S_ADDRESS, [reverse(<place-holder>)])");
    }

    @Test
    public void testTempPartition() throws Exception {
        FeConstants.unitTestView = false;
        try {
            String sql = "ALTER TABLE lineitem_partition ADD TEMPORARY PARTITION px VALUES [('1998-01-01'), ('1999-01-01'));";
            starRocksAssert.alterTable(sql);
            sql = "select distinct L_COMMENT from lineitem_partition TEMPORARY PARTITION(px)";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "dict_col");
        } finally {
            FeConstants.unitTestView = true;
        }
    }

    @Test
    public void testExistRequiredDistribution() throws Exception {
        String sql = "select coalesce(l.S_ADDRESS,l.S_NATIONKEY) from supplier l join supplier r on l.s_suppkey = r.s_suppkey";
        ExecPlan execPlan = getExecPlan(sql);
        Assertions.assertTrue(execPlan.getOptExpression(3).isExistRequiredDistribution(),
                "joinNode is in the same fragment with a table contains global dict, " +
                        "we cannot change its distribution");
        Assertions.assertTrue(execPlan.getOptExpression(0).isExistRequiredDistribution(),
                "table contains global dict, we cannot change its distribution");

        Assertions.assertFalse(execPlan.getOptExpression(1).isExistRequiredDistribution(),
                "table doesn't contain global dict, we can change its distribution");
    }

    @Test
    public void testShortCircuitQuery() throws Exception {
        connectContext.getSessionVariable().setEnableShortCircuit(true);
        String sql = "select * from low_card_t2 where d_date='20160404' and c_mr = '12'";
        final String plan = getFragmentPlan(sql);
        assertContains(plan, "Short Circuit Scan: true");
    }

    @Test
    public void testAggregateWithUnion() throws Exception {
        try {
            connectContext.getSessionVariable().setNewPlanerAggStage(2);
            String sql = "SELECT *\n" +
                    "FROM (\n" +
                    "        SELECT DISTINCT concat(S_ADDRESS, 'a'), S_NAME, S_NATIONKEY\n" +
                    "        FROM supplier\n" +
                    "    ) t1\n" +
                    "UNION ALL\n" +
                    "SELECT *\n" +
                    "FROM (" +
                    "        SELECT * \n" +
                    "        from (select P_NAME, P_MFGR, COUNT(P_BRAND) \n" +
                    "        FROM part_v2 \n" +
                    "        GROUP BY concat(P_TYPE, 'b') ) xxx\n" +
                    "\n) t2;";
            String plan = getThriftPlan(sql);
            assertContains(plan, "TPlanFragment");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
    }

    @Test
    public void testMultiDistinctCount() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(4);
        String sql = "select multi_distinct_count(S_ADDRESS), count(distinct S_NATIONKEY) from supplier";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  aggregate: multi_distinct_count[([11: S_ADDRESS, INT, false]); args: INT; result: VARBINARY; " +
                "args nullable: false; result nullable: false]\n" +
                "  |  group by: [4: S_NATIONKEY, INT, false]\n" +
                "  |  cardinality: 1");

        assertContains(plan, "  4:AGGREGATE (update serialize)\n" +
                "  |  aggregate: multi_distinct_count[([9: multi_distinct_count, BIGINT, false]); " +
                "args: INT; result: VARBINARY; " +
                "args nullable: true; result nullable: false], count[([4: S_NATIONKEY, INT, false]); " +
                "args: INT; result: BIGINT; " +
                "args nullable: false; result nullable: false]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  aggregate: multi_distinct_count[([9: multi_distinct_count, VARBINARY, false]); " +
                "args: INT; result: BIGINT; " +
                "args nullable: true; result nullable: false]\n" +
                "  |  group by: [4: S_NATIONKEY, INT, false]\n" +
                "  |  cardinality: 1");
        assertContains(plan, "  6:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: multi_distinct_count[([9: multi_distinct_count, VARBINARY, false]); " +
                "args: INT; result: BIGINT; " +
                "args nullable: true; result nullable: false], count[([10: count, BIGINT, false]); " +
                "args: INT; result: BIGINT; " +
                "args nullable: true; result nullable: false]\n" +
                "  |  cardinality: 1");
    }
    
    @Test
    public void testWindowFunction() throws Exception {
        String sql = "SELECT\n" +
                "    t.S_ADDRESS,\n" +
                "    t.min_date_create\n" +
                "FROM\n" +
                "    (\n" +
                "        SELECT\n" +
                "            tt.S_ADDRESS,\n" +
                "            MIN(tt.S_ADDRESS) OVER (PARTITION BY tt.S_ADDRESS) AS min_date_create,\n" +
                "            ROW_NUMBER () OVER (\n" +
                "                PARTITION BY tt.S_ADDRESS\n" +
                "                ORDER BY\n" +
                "                    tt.S_ADDRESS\n" +
                "            ) AS row_num\n" +
                "        FROM\n" +
                "            supplier tt\n" +
                "    ) t\n" +
                "WHERE\n" +
                "    t.row_num = 1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Decode\n" +
                "  |  <dict id 12> : <string id 3>\n" +
                "  |  <dict id 13> : <string id 11>\n" +
                "  |  \n" +
                "  2:SORT\n" +
                "  |  order by: <slot 12> 12: S_ADDRESS ASC\n" +
                "  |  analytic partition by: <slot 12> 12: S_ADDRESS\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:PARTITION-TOP-N\n" +
                "  |  partition by: 12: S_ADDRESS \n" +
                "  |  partition limit: 1\n" +
                "  |  order by: <slot 12> 12: S_ADDRESS ASC\n" +
                "  |  pre agg functions: [, min(12: S_ADDRESS), ]\n" +
                "  |  offset: 0");
    }
}
