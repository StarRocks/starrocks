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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

<<<<<<< HEAD
=======
@Ignore
>>>>>>> branch-2.5-mrs
public class MaterializedViewTPCHTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void setUp() throws Exception {
        MaterializedViewTestBase.setUp();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        executeSqlFile("sql/materialized-view/tpch/ddl_tpch.sql");
        executeSqlFile("sql/materialized-view/tpch/ddl_tpch_mv1.sql");
        executeSqlFile("sql/materialized-view/tpch/ddl_tpch_mv2.sql");
        executeSqlFile("sql/materialized-view/tpch/ddl_tpch_mv3.sql");
<<<<<<< HEAD
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
=======
       connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
>>>>>>> branch-2.5-mrs
    }

    @Test
    public void testQuery1() {
        runFileUnitTest("materialized-view/tpch/q1");
    }

    @Test
<<<<<<< HEAD
=======
    public void testQuery1_1() {
        runFileUnitTest("materialized-view/tpch/q1-1");
    }

    @Test
>>>>>>> branch-2.5-mrs
    public void testQuery2() {
        runFileUnitTest("materialized-view/tpch/q2");
    }

    @Test
    public void testQuery3() {
        runFileUnitTest("materialized-view/tpch/q3");
    }

    @Test
    public void testQuery4() {
        runFileUnitTest("materialized-view/tpch/q4");
    }

    @Test
    @Ignore
    public void testQuery5() {
        runFileUnitTest("materialized-view/tpch/q5");
    }

    @Test
<<<<<<< HEAD
=======
    @Ignore
    public void testQuery5_1() {
        runFileUnitTest("materialized-view/tpch/q5-1");
    }

    @Test
    @Ignore
    public void testQuery5_2() {
        runFileUnitTest("materialized-view/tpch/q5-2");
    }

    @Test
>>>>>>> branch-2.5-mrs
    public void testQuery6() {
        runFileUnitTest("materialized-view/tpch/q6");
    }

<<<<<<< HEAD
    @Ignore
=======
>>>>>>> branch-2.5-mrs
    @Test
    public void testQuery7() {
        runFileUnitTest("materialized-view/tpch/q7");
    }

    @Test
<<<<<<< HEAD
=======
    public void testQuery7_1() {
        runFileUnitTest("materialized-view/tpch/q7-1");
    }

    @Test
>>>>>>> branch-2.5-mrs
    @Ignore
    public void testQuery8() {
        runFileUnitTest("materialized-view/tpch/q8");
    }

    @Test
<<<<<<< HEAD
=======
    public void testQuery8_1() {
        runFileUnitTest("materialized-view/tpch/q8-1");
    }

    @Test
    @Ignore
>>>>>>> branch-2.5-mrs
    public void testQuery9() {
        runFileUnitTest("materialized-view/tpch/q9");
    }

    @Test
    public void testQuery10() {
        runFileUnitTest("materialized-view/tpch/q10");
    }

    @Test
<<<<<<< HEAD
=======
    public void testQuery10_1() {
        runFileUnitTest("materialized-view/tpch/q10-1");
    }

    @Test
>>>>>>> branch-2.5-mrs
    public void testQuery11() {
        runFileUnitTest("materialized-view/tpch/q11");
    }

    @Test
    public void testQuery12() {
        runFileUnitTest("materialized-view/tpch/q12");
    }

    @Test
    @Ignore
    public void testQuery13() {
        runFileUnitTest("materialized-view/tpch/q13");
    }

    @Test
    public void testQuery14() {
        runFileUnitTest("materialized-view/tpch/q14");
    }

    @Test
    public void testQuery15() {
        runFileUnitTest("materialized-view/tpch/q15");
    }

    @Test
<<<<<<< HEAD
=======
    public void testQuery15_1() {
        runFileUnitTest("materialized-view/tpch/q15-1");
    }

    @Test
    public void testQuery15_2() {
        runFileUnitTest("materialized-view/tpch/q15-2");
    }

    @Test
>>>>>>> branch-2.5-mrs
    public void testQuery16() {
        runFileUnitTest("materialized-view/tpch/q16");
    }

    @Test
<<<<<<< HEAD
=======
    public void testQuery16_1() {
        runFileUnitTest("materialized-view/tpch/q16-1");
    }

    @Test
>>>>>>> branch-2.5-mrs
    public void testQuery17() {
        runFileUnitTest("materialized-view/tpch/q17");
    }

    @Test
    public void testQuery18() {
        runFileUnitTest("materialized-view/tpch/q18");
    }

    @Test
    public void testQuery19() {
        runFileUnitTest("materialized-view/tpch/q19");
    }

    @Test
    public void testQuery20() {
        runFileUnitTest("materialized-view/tpch/q20");
    }

    @Test
<<<<<<< HEAD
=======
    public void testQuery20_1() {
        runFileUnitTest("materialized-view/tpch/q20-1");
    }

    @Test
>>>>>>> branch-2.5-mrs
    public void testQuery21() {
        runFileUnitTest("materialized-view/tpch/q21");
    }

    @Test
    public void testQuery22() {
        runFileUnitTest("materialized-view/tpch/q22");
    }

<<<<<<< HEAD
    /**
     * ========= Analyze TPCH MVs Result =========
     * <p>
     * TableName:partsupp_mv
     * Columns:n_name,p_mfgr,p_size,p_type,ps_partkey,ps_partvalue,ps_suppkey,ps_supplycost,r_name,s_acctbal,s_address,
     * s_comment,s_name,s_nationkey,s_phone
     * Queries:11,16,2
     * <p>
     * TableName:lineitem_mv
     * Columns:c_acctbal,c_address,c_comment,c_mktsegment,c_name,c_nationkey,c_phone,l_commitdate,l_extendedprice,
     * l_orderkey,l_partkey,l_quantity,l_receiptdate,l_returnflag,l_saleprice,l_shipdate,l_shipinstruct,
     * l_shipmode,l_shipyear,l_suppkey,l_supplycost,n_name1,n_name2,n_regionkey1,n_regionkey2,o_custkey,
     * o_orderdate,o_orderpriority,o_orderstatus,o_orderyear,o_shippriority,o_totalprice,p_brand,p_container,
     * p_name,p_size,p_type,r_name1,r_name2,s_name,s_nationkey
     * Queries:10,12,14,17,18,19,21,3,5,7,8,9
     * <p>
     * TableName:lineitem_agg_mv
     * Columns:l_orderkey,l_partkey,l_shipdate,l_suppkey,sum_disc_price,sum_qty
     * Queries:15,18,20
     * <p>
     * TableName:customer_order_mv
     * Columns:c_custkey,o_comment,o_orderkey
     * Queries:13
     * <p>
=======
    @Test
    public void testQuery22_1() {
        runFileUnitTest("materialized-view/tpch/q22-1");
    }

    /**
     * ========= Analyze TPCH MVs Result =========
     *
     * TableName:partsupp_mv
     * Columns:n_name,p_mfgr,p_size,p_type,ps_partkey,ps_partvalue,ps_suppkey,ps_supplycost,r_name,s_acctbal,s_address,
     *         s_comment,s_name,s_nationkey,s_phone
     * Queries:11,16,2
     *
     * TableName:lineitem_mv
     * Columns:c_acctbal,c_address,c_comment,c_mktsegment,c_name,c_nationkey,c_phone,l_commitdate,l_extendedprice,
     *          l_orderkey,l_partkey,l_quantity,l_receiptdate,l_returnflag,l_saleprice,l_shipdate,l_shipinstruct,
     *          l_shipmode,l_shipyear,l_suppkey,l_supplycost,n_name1,n_name2,n_regionkey1,n_regionkey2,o_custkey,
     *          o_orderdate,o_orderpriority,o_orderstatus,o_orderyear,o_shippriority,o_totalprice,p_brand,p_container,
     *          p_name,p_size,p_type,r_name1,r_name2,s_name,s_nationkey
     * Queries:10,12,14,17,18,19,21,3,5,7,8,9
     *
     * TableName:lineitem_agg_mv
     * Columns:l_orderkey,l_partkey,l_shipdate,l_suppkey,sum_disc_price,sum_qty
     * Queries:15,18,20
     *
     * TableName:customer_order_mv
     * Columns:c_custkey,o_comment,o_orderkey
     * Queries:13
     *
>>>>>>> branch-2.5-mrs
     * ========= Analyze TPCH MVs Result =========
     */
    @Test
    public void analyzeTPCHMVs() throws Exception {
        Map<String, Set<String>> mvTableColumnsMap = Maps.newHashMap();
        Map<String, Set<String>> mvTableQueryIds = Maps.newHashMap();
        // Pattern mvPattern = Pattern.compile("mv\\[(.*)] columns\\[(.*)] predicate\\[(.*)]");
        Pattern mvPattern = Pattern.compile("mv\\[(.*)] columns\\[(.*)] ");
        for (int i = 1; i < 23; i++) {
            String content = getFileContent("sql/materialized-view/tpch/q" + i + ".sql");
            String[] lines = content.split("\n");
<<<<<<< HEAD
            for (String line : lines) {
=======
            for (String line: lines) {
>>>>>>> branch-2.5-mrs
                if (line.contains("mv[")) {
                    Matcher matcher = mvPattern.matcher(line);
                    if (matcher.find()) {
                        String mvTableName = matcher.group(1);
                        List<String> mvTableColumns = splitMVTableColumns(matcher.group(2));

                        mvTableColumnsMap.putIfAbsent(mvTableName, Sets.newTreeSet());
                        mvTableColumnsMap.get(mvTableName).addAll(mvTableColumns);
                        mvTableQueryIds.putIfAbsent(mvTableName, Sets.newTreeSet());
                        mvTableQueryIds.get(mvTableName).add(Integer.toString(i));
                    }
                }
            }
        }

        System.out.println("========= Analyze TPCH MVs Result =========");
        System.out.println();
        int totolQueriesUsedMV = 0;
        for (Map.Entry<String, Set<String>> entry : mvTableColumnsMap.entrySet()) {
            String mvTableName = entry.getKey();
            Set<String> mvTableColumns = entry.getValue();
            System.out.println("TableName:" + mvTableName);
            System.out.println("Columns:" + String.join(",", mvTableColumns));
            System.out.println("Queries:" + String.join(",", mvTableQueryIds.get(mvTableName)));
            System.out.println();
            totolQueriesUsedMV += mvTableQueryIds.get(mvTableName).size();
        }
        System.out.println("========= Analyze TPCH MVs Result =========");
        Assert.assertTrue(mvTableColumnsMap.size() >= 4);
        Assert.assertTrue(totolQueriesUsedMV >= 19);
    }

    private List<String> splitMVTableColumns(String line) {
        String[] s1 = line.split(",");
        List<String> ret = new ArrayList<>();
<<<<<<< HEAD
        for (String s : s1) {
=======
        for (String s: s1) {
>>>>>>> branch-2.5-mrs
            String[] s2 = s.split(":");
            ret.add(s2[1].trim());
        }
        return ret;
    }
}
