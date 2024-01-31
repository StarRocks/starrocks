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

package com.starrocks.planner;

import com.starrocks.common.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ViewBaseMvRewriteTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();

        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        starRocksAssert.useDatabase("test");
        Config.default_replication_num = 1;
        String viewQ1 = "create view view_q1\n" +
                "as\n" +
                "select\n" +
                "    l_returnflag,\n" +
                "    l_linestatus,\n" +
                "    l_shipdate,\n" +
                "    sum(l_quantity) as sum_qty,\n" +
                "    sum(l_extendedprice) as sum_base_price,\n" +
                "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
                "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
                "    avg(l_quantity) as avg_qty,\n" +
                "    avg(l_extendedprice) as avg_price,\n" +
                "    avg(l_discount) as avg_disc,\n" +
                "    count(*) as count_order\n" +
                "from\n" +
                "    test.lineitem\n" +
                "group by\n" +
                "    l_returnflag,\n" +
                "    l_linestatus,\n" +
                "    l_shipdate";
        starRocksAssert.withView(viewQ1);

        String viewQ2 = "create view view_q2\n" +
                "as\n" +
                "select\n" +
                "    s_name,\n" +
                "    s_acctbal,\n" +
                "    n_name,\n" +
                "    p_partkey,\n" +
                "    p_mfgr,\n" +
                "    s_address,\n" +
                "    s_phone,\n" +
                "    s_comment\n" +
                "from\n" +
                "    part,\n" +
                "    supplier,\n" +
                "    partsupp,\n" +
                "    nation,\n" +
                "    region\n" +
                "where\n" +
                "        p_partkey = ps_partkey\n" +
                "  and s_suppkey = ps_suppkey\n" +
                "  and p_size = 12\n" +
                "  and p_type like '%COPPER'\n" +
                "  and s_nationkey = n_nationkey\n" +
                "  and n_regionkey = r_regionkey\n" +
                "  and r_name = 'AMERICA'\n" +
                "  and ps_supplycost = (\n" +
                "    select\n" +
                "        min(ps_supplycost)\n" +
                "    from\n" +
                "        partsupp,\n" +
                "        supplier,\n" +
                "        nation,\n" +
                "        region\n" +
                "    where\n" +
                "            p_partkey = ps_partkey\n" +
                "      and s_suppkey = ps_suppkey\n" +
                "      and s_nationkey = n_nationkey\n" +
                "      and n_regionkey = r_regionkey\n" +
                "      and r_name = 'AMERICA'\n" +
                ")";
        starRocksAssert.withView(viewQ2);

        String viewQ3 = "create view view_q3\n" +
                "as\n" +
                "select\n" +
                "    l_orderkey,\n" +
                "    sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
                "    o_orderdate,\n" +
                "    o_shippriority,\n" +
                "    l_shipdate,\n" +
                "    c_mktsegment\n" +
                "from\n" +
                "    customer,\n" +
                "    orders,\n" +
                "    lineitem\n" +
                "where\n" +
                "  c_custkey = o_custkey\n" +
                "  and l_orderkey = o_orderkey\n" +
                "group by\n" +
                "    l_orderkey,\n" +
                "    o_orderdate,\n" +
                "    o_shippriority,\n" +
                "    l_shipdate,\n" +
                "    c_mktsegment";
        starRocksAssert.withView(viewQ3);

        String viewQ4 = "create view view_q4\n" +
                "as\n" +
                "select\n" +
                "    o_orderdate,\n" +
                "    o_orderpriority,\n" +
                "    count(*) as order_count\n" +
                "from\n" +
                "    orders\n" +
                "where exists (\n" +
                "        select\n" +
                "            *\n" +
                "        from\n" +
                "            lineitem\n" +
                "        where\n" +
                "                l_orderkey = o_orderkey\n" +
                "          and l_receiptdate > l_commitdate\n" +
                "    )\n" +
                "group by\n" +
                "    o_orderpriority,\n" +
                "    o_orderdate;";
        starRocksAssert.withView(viewQ4);

        String viewWithAllScalarType = "create view view_with_all_type\n" +
                "as\n" +
                "select * from test_all_type";
        starRocksAssert.withView(viewWithAllScalarType);

        String testAggView = "create view test_agg_view\n" +
                "as\n" +
                "select * from test_agg";
        starRocksAssert.withView(testAggView);

        String testArrayView = "create view test_array_view\n" +
                "as\n" +
                "select * from tarray";
        starRocksAssert.withView(testArrayView);

        String testMapView = "create view test_map_view\n" +
                "as\n" +
                "select * from tmap";
        starRocksAssert.withView(testMapView);

        String testJsonView = "create view test_json_view\n" +
                "as\n" +
                "select * from tjson";
        starRocksAssert.withView(testJsonView);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        starRocksAssert.dropView("view_q1");
        starRocksAssert.dropView("view_q2");
        starRocksAssert.dropView("view_q3");
        starRocksAssert.dropView("view_q4");
        starRocksAssert.dropView("view_with_all_type");
        starRocksAssert.dropView("test_agg_view");
        starRocksAssert.dropView("test_array_view");
        starRocksAssert.dropView("test_map_view");
        starRocksAssert.dropView("test_json_view");
    }

    @Test
    public void testViewBaseMvRewriteBasic() throws Exception {
        {
            starRocksAssert.withView("create view agg_view_1" +
                    " as " +
                    " select v4, sum(v4) as total from t1 group by v4");
            {
                String mv = "select v7, v8, v4, total from t2 join agg_view_1 on v7 = v4";
                String query = "select v7, v8, v4, total from t2 join agg_view_1 on v7 = v4";
                testRewriteOK(mv, query);
            }
            starRocksAssert.dropView("agg_view_1");
        }
    }

    @Test
    public void testViewBasedMvRewriteOnTpch() {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000000);

        {
            String mv = "select * from view_q1 ";
            String query = "select * from view_q1 limit 10;";
            testRewriteOK(mv, query);
        }
        {
            String mv = "select * from view_q1 ";
            String query = "select * from view_q1 where l_shipdate <= date '1998-12-01';";
            testRewriteOK(mv, query);
        }
        {
            String mv = "select * from view_q1 ";
            String query = "select * from view_q1;";
            testRewriteOK(mv, query);
        }
        {
            String mv = "select *  from view_q1";
            String query = "select l_returnflag, l_shipdate, sum(sum_qty) from view_q1 group by l_returnflag, l_shipdate;";
            testRewriteOK(mv, query);
        }
        {
            String mv = "select * from view_q2";
            String query = "select * from view_q2 order by\n" +
                    "    s_acctbal desc,\n" +
                    "    n_name,\n" +
                    "    s_name,\n" +
                    "    p_partkey limit 100;";
            testRewriteOK(mv, query);
        }

        {
            String mv = "select * from view_q3";
            String query = "select\n" +
                    "    l_orderkey,\n" +
                    "    sum(revenue) as revenue,\n" +
                    "    o_orderdate,\n" +
                    "    o_shippriority\n" +
                    "from view_q3\n" +
                    "where\n" +
                    "  c_mktsegment = 'HOUSEHOLD'\n" +
                    "  and o_orderdate < date '1995-03-11'\n" +
                    "  and l_shipdate > date '1995-03-11'\n" +
                    "group by\n" +
                    "    l_orderkey,\n" +
                    "    o_orderdate,\n" +
                    "    o_shippriority\n" +
                    "order by\n" +
                    "    revenue desc,\n" +
                    "    o_orderdate limit 10;";
            testRewriteOK(mv, query);
        }
        {
            String mv = "select * from view_q4";
            String query = "select\n" +
                    "    o_orderpriority,\n" +
                    "    count(*) as order_count\n" +
                    "from\n" +
                    "    view_q4\n" +
                    "where\n" +
                    "  o_orderdate >= date '1994-09-01'\n" +
                    "  and o_orderdate < date '1994-12-01'\n" +
                    "group by\n" +
                    "    o_orderpriority\n" +
                    "order by\n" +
                    "    o_orderpriority ;";
            testRewriteOK(mv, query);
        }
    }

    @Test
    public void testMultiView() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(3000000);
        starRocksAssert.withView("create view view_1 as " +
                "select v1, sum(v2) as total1 from t0 group by v1");
        starRocksAssert.withView("create view view_2 as " +
                "select v4, sum(v5) as total2 from t1 group by v4");
        starRocksAssert.withView("create view view_3 as " +
                "select v4, sum(v5) as total3 from t1 group by v4");
        starRocksAssert.withView("create view view_4 as " +
                "select v4, sum(v5) as total4 from t1 group by v4");
        starRocksAssert.withView("create view view_5 as " +
                "select v4, sum(v5) as total5 from t1 group by v4");

        {
            starRocksAssert.withView("create view view_6 as " +
                    "select v1, total1, total2 from view_1 join view_2 on v1 = v4;");
            String mv = "select v1, total1 from view_6";
            String query = "select * from view_6";
            testRewriteFail(mv, query);
            starRocksAssert.dropView("view_6");
        }

        {
            // test extra LogicaiViewScanOperator of view_3 left
            starRocksAssert.withView("create view view_6 as " +
                    "select v1, total1, total2 from view_1 join view_2 on v1 = v4;");
            String mv = "select * from view_6";
            String query = "select view_6.v1, view_6.total1, view_3.total3 from view_6 join view_3 on view_6.v1 = view_3.v4;";
            testRewriteOK(mv, query);
            starRocksAssert.dropView("view_6");
        }

        {
            // test two views
            String mv = "select v1, sum(total1), sum(total2) from view_1 join view_2 on v1 = v4 group by v1;";
            String query = "select v1, sum(total1), sum(total2) from view_1 join view_2 on v1 = v4 group by v1;";
            testRewriteOK(mv, query);
        }
        {
            // test three views
            String mv = "select v1, sum(total1), sum(total2) from view_1 join view_2 on view_1.v1 = view_2.v4" +
                    " join view_3 on view_1.v1 = view_3.v4" +
                    " group by v1";
            String query = "select v1, sum(total1), sum(total2) from view_1 join view_2 on view_1.v1 = view_2.v4" +
                    " join view_3 on view_1.v1 = view_3.v4" +
                    " group by v1";
            testRewriteOK(mv, query);
        }
        {
            // test four views
            String mv = "select v1, sum(total1), sum(total2) from view_1 join view_2 on view_1.v1 = view_2.v4" +
                    " join view_3 on view_1.v1 = view_3.v4" +
                    " join view_4 on view_1.v1 = view_4.v4" +
                    " group by v1";
            String query = "select v1, sum(total1), sum(total2) from view_1 join view_2 on view_1.v1 = view_2.v4" +
                    " join view_3 on view_1.v1 = view_3.v4" +
                    " join view_4 on view_1.v1 = view_4.v4" +
                    " group by v1";
            testRewriteOK(mv, query);
        }
        {
            // test five views
            String mv = "select v1, sum(total1), sum(total2) from view_1 join view_2 on view_1.v1 = view_2.v4" +
                    " join view_3 on view_1.v1 = view_3.v4" +
                    " join view_4 on view_1.v1 = view_4.v4" +
                    " join view_5 on view_1.v1 = view_5.v4" +
                    " group by v1";
            String query = "select v1, sum(total1), sum(total2) from view_1 join view_2 on view_1.v1 = view_2.v4" +
                    " join view_3 on view_1.v1 = view_3.v4" +
                    " join view_4 on view_1.v1 = view_4.v4" +
                    " join view_5 on view_1.v1 = view_5.v4" +
                    " group by v1";
            testRewriteOK(mv, query);
        }
        starRocksAssert.dropView("view_1");
        starRocksAssert.dropView("view_2");
        starRocksAssert.dropView("view_3");
        starRocksAssert.dropView("view_4");
        starRocksAssert.dropView("view_5");
    }

    @Test
    public void testViewWithDifferentTypes() {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000000);
        {
            String mv = "select * from view_with_all_type";
            String query = "select * from view_with_all_type";
            testRewriteOK(mv, query);
        }

        // bitmap/hll/percentile
        {
            String mv = "select * from test_agg_view";
            String query = "select * from test_agg_view";
            testRewriteOK(mv, query);
        }

        {
            String mv = "select * from test_agg_view";
            String query = "select k1, bitmap_union(b1), hll_union(h1), percentile_union(p1) from test_agg_view group by k1";
            testRewriteOK(mv, query);
        }

        // array
        {
            String mv = "select * from test_array_view";
            String query = "select * from test_array_view";
            testRewriteOK(mv, query);
        }

        {
            String mv = "select * from test_array_view";
            String query = "select v1, array_agg(v3) from test_array_view group by v1";
            testRewriteOK(mv, query);
        }

        // map
        {
            String mv = "select * from test_map_view";
            String query = "select * from test_map_view";
            testRewriteOK(mv, query);
        }

        {
            String mv = "select * from test_map_view";
            String query = "select v1, map_keys(v3), map_values(v3) from test_map_view";
            testRewriteOK(mv, query);
        }

        // json
        {
            String mv = "select * from test_json_view";
            String query = "select * from test_json_view";
            testRewriteOK(mv, query);
        }
    }

    @Test
    public void testViewJoinRewrite() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(3000000);
        starRocksAssert.withView("CREATE VIEW view_1 AS\n" +
                "SELECT l_partkey,\n" +
                "       l_suppkey,\n" +
                "       sum(l_quantity) AS total_quantity\n" +
                "FROM lineitem\n" +
                "GROUP BY l_partkey,\n" +
                "         l_suppkey;");
        starRocksAssert.withView("CREATE VIEW view_2 AS\n" +
                "SELECT l_partkey,\n" +
                "       l_suppkey,\n" +
                "       sum(l_tax) AS total_tax\n" +
                "FROM lineitem\n" +
                "GROUP BY l_partkey,\n" +
                "         l_suppkey;");
        {
            starRocksAssert.withMaterializedView("CREATE materialized VIEW mv_2\n" +
                    "distributed BY hash(l_partkey) \n" +
                    "refresh manual AS\n" +
                    "SELECT l_partkey,\n" +
                    "       sum(l_discount) AS total_discount\n" +
                    "FROM lineitem\n" +
                    "group by l_partkey;");
            String mv = "SELECT v1.l_partkey,\n" +
                    "       v2.l_suppkey,\n" +
                    "       total_quantity,\n" +
                    "       total_tax\n" +
                    "FROM view_1 v1\n" +
                    "JOIN view_2 v2 ON v1.l_partkey = v2.l_partkey\n" +
                    "AND v1.l_suppkey = v2.l_suppkey;\n";
            String query = "SELECT v1.l_partkey,\n" +
                    "       v2.l_suppkey,\n" +
                    "       total_quantity,\n" +
                    "       total_tax\n" +
                    "FROM view_1 v1\n" +
                    "JOIN view_2 v2 ON v1.l_partkey = v2.l_partkey\n" +
                    "AND v1.l_suppkey = v2.l_suppkey;";
            setTracLogModule("MV");
            testRewriteOK(mv, query);
            starRocksAssert.dropMaterializedView("mv_2");
        }
        starRocksAssert.dropView("view_1");
        starRocksAssert.dropView("view_2");
    }

    @Test
    public void testViewWithInvalidPlan() {
        String view = "create view invalid_view0 as select count(distinct cnt) as ndv from " +
                "(select count(*) cnt from lineitem group by l_returnflag) t";
        try {
            starRocksAssert.withView(view);
        } catch (Exception e) {
            Assert.fail();
        }
        String sql = "create materialized view invalid_plan_mv distributed by random as select * from invalid_view0";
        starRocksAssert.withMaterializedView(sql, () -> {
            sql("select * from invalid_view0").contains("invalid_plan_mv");
            sql("select * from invalid_view0 where ndv > 10").contains("invalid_plan_mv");
        });
    }

    @Test
    public void testSingleCte() throws Exception {
        String createViewSql = "CREATE VIEW `v_q15` (`s_suppkey`, `s_name`, `s_address`, `s_phone`, `total_revenue`) AS " +
                "WITH `revenue0` (`supplier_no`, `total_revenue`)" +
                " AS (" +
                "SELECT " +
                "   `lineitem`.`l_suppkey`, " +
                "   sum(`lineitem`.`l_extendedprice` * (1 - `lineitem`.`l_discount`)) " +
                "       AS `sum(l_extendedprice * (1 - l_discount))`\n" +
                "FROM `lineitem`\n" +
                "WHERE (`lineitem`.`l_shipdate` >= '1996-01-01') " +
                "   AND (`lineitem`.`l_shipdate` < ('1996-01-01 00:00:00' + INTERVAL '3' MONTH))\n" +
                "GROUP BY `lineitem`.`l_suppkey`)" +
                " SELECT " +
                "   `supplier`.`s_suppkey`, " +
                "   `supplier`.`s_name`, " +
                "   `supplier`.`s_address`, " +
                "   `supplier`.`s_phone`, " +
                "   `revenue0`.`total_revenue`\n" +
                "FROM `supplier` , `revenue0`\n" +
                "WHERE (`supplier`.`s_suppkey` = `revenue0`.`supplier_no`) " +
                "   AND (`revenue0`.`total_revenue` = ((SELECT max(`revenue0`.`total_revenue`) AS `max(total_revenue)`\n" +
                "FROM `revenue0`))) " +
                "ORDER BY `supplier`.`s_suppkey` ASC ;";

        starRocksAssert.withView(createViewSql);

        String createMvSql = "create materialized view mv_q15 " +
                "refresh manual " +
                "as " +
                "select * from v_q15";
        starRocksAssert.withMaterializedView(createMvSql);
        {
            String query = "select * from v_q15";
            sql(query).contains("mv_q15");
        }
    }
}
