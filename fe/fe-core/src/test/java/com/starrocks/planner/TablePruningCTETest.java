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

import com.starrocks.common.FeConstants;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class TablePruningCTETest extends TablePruningTestBase {
    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("tpch").useDatabase("tpch");
        getTPCHCreateTableSqlList().forEach(createTblSql -> {
            try {
                starRocksAssert.withTable(createTblSql);
                starRocksAssert.withTable(replaceTableName(createTblSql));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });

        getSqlList("sql/tpch_pk_tables/", "AddFKConstraints")
                .forEach(fkConstraints -> Arrays.stream(fkConstraints.split("\n")).forEach(addFk ->
                        {
                            try {
                                starRocksAssert.alterTableProperties(addFk);
                            } catch (Exception e) {
                                e.printStackTrace();
                                Assert.fail();
                            }
                        }
                ));

        String[][] tpchShowAndResults = new String[][] {
                {"nation",
                        "\"foreign_key_constraints\" = \"(n_regionkey) REFERENCES default_catalog.tpch.region(r_regionkey)\",\n"
                },
                {"supplier",
                        "\"foreign_key_constraints\" = \"(s_nationkey) REFERENCES default_catalog.tpch.nation(n_nationkey)\",\n"
                },
                {"customer",
                        "\"foreign_key_constraints\" = \"(c_nationkey) REFERENCES default_catalog.tpch.nation(n_nationkey)\""
                },
                {"partsupp",
                        "\"foreign_key_constraints\" = \"(ps_partkey) REFERENCES default_catalog.tpch.part(p_partkey);(ps_suppkey) REFERENCES default_catalog.tpch.supplier(s_suppkey)\",\n"},
                {"orders",
                        "\"foreign_key_constraints\" = \"(o_custkey) REFERENCES default_catalog.tpch.customer(c_custkey)\",\n"},
                {"lineitem",
                        "\"foreign_key_constraints\" = \"(l_orderkey) REFERENCES default_catalog.tpch.orders(o_orderkey);(l_partkey) REFERENCES default_catalog.tpch.part(p_partkey);(l_suppkey) REFERENCES default_catalog.tpch.supplier(s_suppkey);(l_partkey,l_suppkey) REFERENCES default_catalog.tpch.partsupp(ps_partkey,ps_suppkey)\",\n"}
        };
        for (String[] showAndResult : tpchShowAndResults) {
            List<List<String>> res = starRocksAssert.show(String.format("show create table %s", showAndResult[0]));
            Assert.assertTrue(res.size() >= 1 && res.get(0).size() >= 2);
            String createTableSql = res.get(0).get(1);
            Assert.assertTrue(createTableSql, createTableSql.contains(showAndResult[1]));
        }
        FeConstants.runningUnitTest = true;
    }

    private static String replaceTableName(String createTableSql) {
        Pattern pat = Pattern.compile("(?i)^\\s*CREATE\\s*TABLE\\s*`?(\\w+)`?");
        return createTableSql.replaceAll(pat.pattern(), "CREATE TABLE `$10`");
    }

    @Test
    public void testUpdate() {
        String sql = "WITH cte0 as (\n" +
                "WITH cte1 as (\n" +
                "select l_suppkey as suppkey,\n" +
                "       l_partkey as partkey, \n" +
                "       avg(l_tax) as tax\n" +
                "from lineitem\n" +
                "group by partkey, suppkey\n" +
                "),\n" +
                "\n" +
                "cte2 as (\n" +
                "select \n" +
                "\tlineitem.l_tax as tax,\n" +
                "\tcte1.tax as avg_tax,\n" +
                "\tlineitem.l_shipdate as shipdate,\n" +
                "\tlineitem.l_orderkey as orderkey,\n" +
                "\tlineitem.l_partkey as partkey,\n" +
                "\tlineitem.l_suppkey as suppkey\n" +
                "from \n" +
                "lineitem left join cte1 on lineitem.l_suppkey = cte1.suppkey and lineitem.l_partkey = cte1.partkey\n" +
                ")\n" +
                "\n" +
                "select  shipdate,\n" +
                "\torderkey,\n" +
                "\tpartkey,\n" +
                "\tsuppkey,\n" +
                "\t(case when tax = 0 then 0 else avg_tax end) as tax\n" +
                "from cte2     \n" +
                ")\n" +
                "update lineitem set l_tax = cte0.tax from cte0\n" +
                "where\n" +
                "   lineitem.l_orderkey = cte0.orderkey and \n" +
                "   lineitem.l_partkey = cte0.partkey and \n" +
                "   lineitem.l_suppkey = cte0.suppkey";

        checkHashJoinCountWithOnlyRBO(sql, 1);
    }

    @Test
    public void testUpdateCTEFullInlined() {
        String q = getSqlList("sql/tpch_pk_tables/", "q1").get(0);
        checkHashJoinCountWithOnlyRBO(q, 3);
    }

    @Test
    public void testUpdateCTENotFullInlined() {
        String q = getSqlList("sql/tpch_pk_tables/", "q2").get(0);
        checkHashJoinCountWithOnlyRBO(q, 3);
    }

    @Test
    public void testUpdateCTEInnerJoin() {
        String q = getSqlList("sql/tpch_pk_tables/", "q3").get(0);
        checkHashJoinCountWithOnlyRBO(q, 3);
    }

    @Test
    public void testUpdateAggregationPreventPruning() {
        String q = getSqlList("sql/tpch_pk_tables/", "q4").get(0);
        checkHashJoinCountWithOnlyRBO(q, 5);
    }

    @Test
    public void testUpdateWithPredicates() {
        String q = getSqlList("sql/tpch_pk_tables/", "q5").get(0);
        String plan = checkHashJoinCountWithOnlyRBO(q, 3);
        plan = plan.replaceAll("\\d+:\\s*", "");
        Assert.assertTrue(plan.contains("Predicates: " +
                "l_suppkey IN (1, 2, 100), " +
                "l_partkey IN (200, 1000), " +
                "l_partkey IN (200, 1000), " +
                "l_suppkey IN (1, 2, 100), " +
                "[l_orderkey, BIGINT, false] >= 1, " +
                "[l_orderkey, BIGINT, false] <= 1000"));
    }

    @Test
    public void testUpdateContainsRightJoin() {
        String q = getSqlList("sql/tpch_pk_tables/", "q6").get(0);
        checkHashJoinCountWithOnlyRBO(q, 3);
    }

    @Test
    public void testTpchInnerJoinSubquery() {
        Object[][] cases = {
                {"sr_name,o_orderpriority,p_size,ps_comment", 6},
                {"l_comment,sr_name,l_quantity,o_orderstatus", 4},
                {"c_name,l_linenumber,c_mktsegment,p_mfgr", 3},
                {"l_shipdate,o_comment,s_phone,p_size", 3},
                {"o_orderpriority,sr_comment,l_suppkey,l_shipinstruct", 4},
                {"c_nationkey,p_container,o_comment,ps_comment", 4},
                {"o_orderstatus,o_orderdate,o_comment,s_name", 2},
                {"sr_name,l_extendedprice,p_container,o_custkey", 5},
                {"p_type,c_acctbal,l_shipmode,s_comment", 4},
                {"c_name,p_container,ps_availqty,sr_name", 7},
                {"o_custkey,l_orderkey,s_acctbal,s_name", 2},
                {"o_orderstatus,l_receiptdate,o_orderpriority,c_acctbal", 2},
                {"l_quantity,s_acctbal,o_totalprice,p_container", 3},
                {"s_acctbal,c_address,c_phone,l_shipmode", 3},
                {"sr_comment,s_comment,c_name,c_address", 5},
                {"s_nationkey,o_clerk,l_quantity,cn_name", 4},
                {"ps_comment,o_comment,s_phone,o_custkey", 3},
                {"p_size,p_comment,l_quantity,sn_name", 3},
                {"l_quantity,l_linestatus,o_totalprice,l_tax", 1},
                {"c_acctbal,l_tax,p_mfgr,cr_name", 5},
        };
        String subquery = getSqlList("sql/tpch_pk_tables/", "lineitem_flat_subquery").get(0);
        for (Object[] tc : cases) {
            String items = (String) tc[0];
            int numHashJoins = (Integer) tc[1];
            String q = String.format("select %s from (%s) t", items, subquery);

            checkHashJoinCountWithBothRBOAndCBO(q, numHashJoins);
        }
    }

    @Test
    public void testTpchLeftJoinSubquery() {
        Object[][] cases = {
                {"l_tax,o_orderdate,l_discount,l_suppkey", 1},
                {"ps_availqty,l_linestatus", 1},
                {"o_orderpriority,l_receiptdate,ps_supplycost,cr_name", 5},
                {"l_tax,p_brand,ps_supplycost,l_receiptdate", 2},
                {"c_phone", 2},
                {"p_brand,s_acctbal", 2},
                {"p_mfgr,c_comment", 3},
                {"o_orderdate,l_comment,c_nationkey,l_suppkey", 2},
                {"p_mfgr,p_container,c_name,sn_comment", 5},
                {"l_partkey,sr_comment", 3},
                {"cr_name,p_comment,p_container", 5},
                {"p_type,l_shipinstruct", 1},
                {"c_comment,l_orderkey,l_shipdate,l_discount", 2},
                {"sr_comment,l_commitdate,o_orderpriority", 4},
                {"p_type,s_nationkey,o_custkey", 3},
                {"o_shippriority,s_nationkey,ps_comment,s_comment", 3},
                {"o_totalprice,s_comment", 2},
                {"p_retailprice,sr_name", 4},
                {"c_acctbal", 2},
                {"o_custkey,p_brand,s_nationkey", 3},
        };
        String subquery = getSqlList("sql/tpch_pk_tables/", "lineitem_flat_subquery").get(0);
        subquery = subquery.replaceAll("inner join", "left join");
        Assert.assertTrue(subquery.contains("left join"));
        for (Object[] tc : cases) {
            String items = (String) tc[0];
            int numHashJoins = (Integer) tc[1];
            String q = String.format("select %s from (%s) t", items, subquery);
            checkHashJoinCountWithBothRBOAndCBO(items, q, numHashJoins);
        }
    }

    @Test
    public void testTpchCTE() {
        Object[][] cases = {
                {"l_tax,o_orderdate,l_discount,l_suppkey", 1},
                {"ps_availqty,l_linestatus", 1},
                {"o_orderpriority,l_receiptdate,ps_supplycost,cr_name", 5},
                {"l_tax,p_brand,ps_supplycost,l_receiptdate", 2},
                {"c_phone", 2},
                {"p_brand,s_acctbal", 2},
                {"p_mfgr,c_comment", 3},
                {"o_orderdate,l_comment,c_nationkey,l_suppkey", 2},
                {"p_mfgr,p_container,c_name,sn_comment", 5},
                {"l_partkey,sr_comment", 3},
                {"cr_name,p_comment,p_container", 5},
                {"p_type,l_shipinstruct", 1},
                {"c_comment,l_orderkey,l_shipdate,l_discount", 2},
                {"sr_comment,l_commitdate,o_orderpriority", 4},
                {"p_type,s_nationkey,o_custkey", 3},
                {"o_shippriority,s_nationkey,ps_comment,s_comment", 3},
                {"o_totalprice,s_comment", 2},
                {"p_retailprice,sr_name", 4},
                {"c_acctbal", 2},
                {"o_custkey,p_brand,s_nationkey", 3},
        };

        ctx.getSessionVariable().setCboCteReuse(false);
        String cte = getSqlList("sql/tpch_pk_tables/", "lineitem_flat_cte").get(0);
        for (Object[] tc : cases) {
            String items = (String) tc[0];
            int numHashJoins = (Integer) tc[1];
            String q = String.format("%s select %s from lineitem_flat", cte, items);
            checkHashJoinCountWithBothRBOAndCBO(items, q, numHashJoins);
        }
    }

    @Test
    public void testTpchLeftJoinCTE() {
        Object[][] cases = {
                {"sn_name", 2},
                {"o_orderdate", 1},
                {"ps_comment", 1},
                {"l_receiptdate,s_address,p_retailprice,l_shipdate", 2},
                {"cn_comment", 3},
                {"ps_supplycost", 1},
                {"o_shippriority,p_size,c_comment", 3},
                {"s_phone,ps_availqty,l_shipinstruct,p_comment", 3},
                {"l_shipdate,c_address,o_orderpriority,l_commitdate", 2},
                {"l_partkey,o_shippriority", 1},
                {"p_brand,l_linestatus", 1},
                {"o_orderpriority,s_nationkey", 2},
                {"l_linestatus,l_partkey,sr_name", 3},
                {"p_type,o_custkey,l_shipinstruct,o_comment", 2},
                {"s_comment,l_extendedprice,l_shipmode,s_acctbal", 1},
                {"l_comment,l_shipinstruct,p_mfgr", 1},
                {"sr_name,p_type", 4},
                {"c_comment,p_mfgr,cr_name,l_discount", 5},
                {"p_container", 1},
                {"o_orderpriority", 1},
        };
        String cte = getSqlList("sql/tpch_pk_tables/", "lineitem_flat_cte").get(0);
        cte = cte.replaceAll("inner join", "left join");
        Assert.assertTrue(cte.contains("left join"));
        for (Object[] tc : cases) {
            String items = (String) tc[0];
            int numHashJoins = (Integer) tc[1];
            String q = String.format("%s select %s from lineitem_flat", cte, items);
            checkHashJoinCountWithBothRBOAndCBO(items, q, numHashJoins);
        }
    }

    @Test
    public void testRandomlyPermuteTPCHTables() {
        String sql = "select\n" +
                "  l_orderkey,\n" +
                "  snation.n_name,\n" +
                "  sregion.r_name\n" +
                "from\n" +
                "  supplier,customer, nation snation, region sregion,nation cnation, region cregion, lineitem, orders, partsupp, part\n" +
                "where \n" +
                "  lineitem.l_orderkey = orders.o_orderkey \n" +
                "  and lineitem.l_partkey = partsupp.ps_partkey and lineitem.l_suppkey = partsupp.ps_suppkey \n" +
                "  and lineitem.l_partkey =  part.p_partkey\n" +
                "  and lineitem.l_suppkey = supplier.s_suppkey\n" +
                "  and orders.o_custkey = customer.c_custkey\n" +
                "  and supplier.s_nationkey = snation.n_nationkey \n" +
                "  and snation.n_regionkey = sregion.r_regionkey \n" +
                "  and customer.c_nationkey = cnation.n_nationkey \n" +
                "  and cnation.n_regionkey = cregion.r_regionkey";
        checkHashJoinCountWithBothRBOAndCBO(sql, 3);
    }

    @Test
    public void testCteWithPredicates() {
        String cte = getSqlList("sql/tpch_pk_tables/", "lineitem_flat_cte").get(0);
        cte = cte.replaceAll("inner join", "left join");
        cte = cte.replaceAll("lineitem_flat", "lineitem_flat_cte1");
        Object[][] cases = {
                {"select cn_name,o_orderdate,o_orderpriority \n" +
                        "from lineitem_flat_cte1 where cn_name = 'VIETNAM'",
                        3},
                {" select  cr_name,l_quantity,l_shipdate,l_shipinstruct,p_name,sr_comment \n" +
                        "from lineitem_flat_cte1 where\n" +
                        "sn_name = 'EGYPT' and cn_name = 'BRAZIL'",
                        8},
        };
        for (Object[] tc : cases) {
            String selectItems = (String) tc[0];
            int n = (Integer) tc[1];
            String sql = String.format("%s %s", cte, selectItems);
            String plan = checkHashJoinCountWithBothRBOAndCBO(sql, n);
            Assert.assertFalse(plan.contains("NESTLOOP"));
        }
    }
}