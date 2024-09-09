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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MaterializedViewRewriteWithSSBTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        MaterializedViewTestBase.beforeClass();

        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);
        // create SSB tables
        // put lineorder last because it depends on other tables for foreign key constraints
        createTables("sql/ssb/", Lists.newArrayList("customer", "dates", "supplier", "part", "lineorder"));
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        connectContext.getSessionVariable().setEnableMaterializedViewPushDownRewrite(true);
    }

    @Test
    public void testNestedMVRewriteWithSSB() {
        String mv1 = "CREATE MATERIALIZED VIEW mv1 REFRESH ASYNC every (interval 10 minute) AS\n" +
                "select lo_orderkey, lo_custkey, p_partkey, p_name\n" +
                "from lineorder join part on lo_partkey = p_partkey;";
        String mv2 = "CREATE MATERIALIZED VIEW mv2 REFRESH ASYNC every (interval 10 minute) AS\n" +
                "select c_custkey from customer group by c_custkey;";
        String mv3 = "CREATE MATERIALIZED VIEW mv3 REFRESH ASYNC every (interval 10 minute) AS\n" +
                "select * from mv1 lo join mv2 cust on lo.lo_custkey = cust.c_custkey;";

        starRocksAssert.withMaterializedViews(ImmutableList.of(mv1, mv2, mv3), (obj) -> {
            String query = "select *\n" +
                    "from (\n" +
                    "    select lo_orderkey, lo_custkey, p_partkey, p_name\n" +
                    "    from lineorder\n" +
                    "    join part on lo_partkey = p_partkey\n" +
                    ") lo\n" +
                    "join (\n" +
                    "    select c_custkey\n" +
                    "    from customer\n" +
                    "    group by c_custkey\n" +
                    ") cust\n" +
                    "on lo.lo_custkey = cust.c_custkey;";
            sql(query).contains("mv3");
        });
    }

    @Test
    public void testJoinWithPredicatePushDown() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select p_brand, LO_ORDERDATE, sum(LO_REVENUE) as revenue_sum\n" +
                "from lineorder l left join part p on l.LO_PARTKEY = p.P_PARTKEY\n" +
                "group by p_brand, LO_ORDERDATE";

        // This case must disable outer join to inner join in `JoinPredicatePushdown`
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = "select t1.p_brand as p_brand, t1.LO_ORDERDATE as LO_ORDERDATE,\n" +
                    "SUM(revenue_sum) + SUM(supplycost_sum) as revenue_and_supplycost_sum\n" +
                    "from\n" +
                    "(\n" +
                    "   select p_brand, LO_ORDERDATE, sum(LO_REVENUE) as revenue_sum\n" +
                    "   from lineorder l left join part p on l.LO_PARTKEY = p.P_PARTKEY\n" +
                    "   group by p_brand, LO_ORDERDATE" +
                    ") t1\n" +
                    "inner join (\n" +
                    "   select p_brand, LO_ORDERDATE, sum(LO_REVENUE) as supplycost_sum\n" +
                    "   from lineorder l left join part p on l.LO_PARTKEY = p.P_PARTKEY\n" +
                    "   group by p_brand, LO_ORDERDATE" +
                    ") t2 on t1.p_brand = t2.p_brand and t1.LO_ORDERDATE = t2.LO_ORDERDATE\n" +
                    "group by t1.p_brand, t1.LO_ORDERDATE;";
            sql(query).contains("mv0");
        });
    }

    @Test
    public void testJoinWithAggPushDown0() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, sum(LO_REVENUE) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE";
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = "select LO_ORDERDATE, sum(LO_REVENUE) as revenue_sum\n" +
                    "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                    "   group by LO_ORDERDATE";
            sql(query).contains("mv0");
        });
    }

    @Test
    public void testJoinWithMultiCountDistinct() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, " +
                "   bitmap_union(to_bitmap(LO_REVENUE)), \n" +
                "   bitmap_union(to_bitmap(lo_linenumber)), \n" +
                "   bitmap_union(to_bitmap(lo_custkey)), \n" +
                "   bitmap_union(to_bitmap(lo_partkey)), \n" +
                "   bitmap_union(to_bitmap(lo_suppkey)), \n" +
                "   bitmap_union(to_bitmap(lo_orderpriority)), \n" +
                "   bitmap_union(to_bitmap(lo_shippriority)), \n" +
                "   bitmap_union(to_bitmap(lo_ordtotalprice)), \n" +
                "   bitmap_union(to_bitmap(lo_tax)), \n" +
                "   bitmap_union(to_bitmap(lo_shipmode)) \n" +
                "from lineorder l group by LO_ORDERDATE";
        setTracLogModule("MV");
        starRocksAssert.withMaterializedView(mv, () -> {
            {
                String query = "select LO_ORDERDATE, count(distinct LO_REVENUE) as revenue_sum\n" +
                        "   from lineorder l \n" +
                        "   group by LO_ORDERDATE";
                sql(query).contains("mv0");
            }
            {
                String query = "select LO_ORDERDATE, " +
                    "   count(distinct LO_REVENUE), \n" +
                    "   count(distinct lo_linenumber), \n" +
                    "   count(distinct lo_custkey), \n" +
                    "   count(distinct lo_suppkey), \n" +
                    "   count(distinct lo_partkey), \n" +
                    "   count(distinct lo_orderpriority), \n" +
                    "   count(distinct lo_shippriority), \n" +
                    "   count(distinct lo_ordtotalprice), \n" +
                    "   count(distinct lo_tax), \n" +
                    "   count(distinct lo_shipmode) \n" +
                    "from lineorder l group by LO_ORDERDATE";
                sql(query).contains("mv0");
            }
        });
    }

    @Test
    public void testJoinWithAggPushDown1() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, bitmap_union(to_bitmap(LO_REVENUE)) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE";
        starRocksAssert.withMaterializedView(mv, () -> {
            {
                String query = "select LO_ORDERDATE, count(distinct LO_REVENUE) as revenue_sum\n" +
                        "   from lineorder l \n" +
                        "   group by LO_ORDERDATE";
                sql(query).contains("mv0");
            }
            {
                String query = "select LO_ORDERDATE, bitmap_union(to_bitmap(LO_REVENUE)) as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE";
                sql(query).contains("mv0");
            }
            {
                String query = "select LO_ORDERDATE, count(distinct LO_REVENUE) as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE";
                sql(query).contains("mv0");
            }
        });
    }

    @Test
    public void testJoinWithAggPushDown2() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, count(distinct LO_REVENUE) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE";
        starRocksAssert.withMaterializedView(mv, () -> {
            {
                String query = "select LO_ORDERDATE, count(distinct LO_REVENUE) as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE";
                sql(query).notContain("mv0");
            }
        });
    }
}
