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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.Test;

public class MvRewriteNestedMVTest extends MvRewriteTestBase {
    @Test
    public void testNestedMv() throws Exception {
        starRocksAssert.withTable("CREATE TABLE nest_base_table_1 (\n" +
                "    k1 INT,\n" +
                "    v1 INT,\n" +
                "    v2 INT)\n" +
                "DUPLICATE KEY(k1)\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(\n" +
                "PARTITION `p1` VALUES LESS THAN ('2'),\n" +
                "PARTITION `p2` VALUES LESS THAN ('3'),\n" +
                "PARTITION `p3` VALUES LESS THAN ('4'),\n" +
                "PARTITION `p4` VALUES LESS THAN ('5'),\n" +
                "PARTITION `p5` VALUES LESS THAN ('6'),\n" +
                "PARTITION `p6` VALUES LESS THAN ('7')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");
        cluster.runSql("test", "insert into t1 values (1,1,1),(1,1,2),(1,1,3),(1,2,1),(1,2,2),(1,2,3)," +
                " (1,3,1),(1,3,2),(1,3,3)\n" +
                " ,(2,1,1),(2,1,2),(2,1,3),(2,2,1),(2,2,2),(2,2,3),(2,3,1),(2,3,2),(2,3,3)\n" +
                " ,(3,1,1),(3,1,2),(3,1,3),(3,2,1),(3,2,2),(3,2,3),(3,3,1),(3,3,2),(3,3,3);");
        createAndRefreshMv("test", "nested_mv_1", "CREATE MATERIALIZED VIEW nested_mv_1" +
                " PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL AS SELECT k1, v1 as k2, v2 as k3 from t1;");
        createAndRefreshMv("test", "nested_mv_1", "CREATE MATERIALIZED VIEW nested_mv_2 " +
                "PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL AS SELECT k1, count(k2) as count_k2, sum(k3) as sum_k3 from nested_mv_1 group by k1;");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW nested_mv_3 DISTRIBUTED BY HASH(k1)\n" +
                "REFRESH MANUAL AS SELECT k1, count_k2, sum_k3 from nested_mv_2 where k1 >1;");
        cluster.runSql("test", "insert into t1 values (4,1,1);");
        refreshMaterializedView("test", "nested_mv_1");
        refreshMaterializedView("test", "nested_mv_2");
        String query1 = "SELECT k1, count(v1), sum(v2) from t1 where k1 >1 group by k1";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertNotContains(plan1, "nested_mv_3");

        dropMv("test", "nested_mv_1");
        dropMv("test", "nested_mv_2");
        dropMv("test", "nested_mv_3");
        starRocksAssert.dropTable("nest_base_table_1");
    }

    @Test
    public void testNestedMVs1() throws Exception {
        createAndRefreshMv("test", "nested_mv1", "create materialized view nested_mv1 " +
                " distributed by hash(empid) as" +
                " select * from emps;");
        createAndRefreshMv("test", "nested_mv2", "create materialized view nested_mv2 " +
                " distributed by hash(empid) as" +
                " select empid, sum(deptno) from emps group by empid;");
        createAndRefreshMv("test", "nested_mv3", "create materialized view nested_mv3 " +
                " distributed by hash(empid) as" +
                " select * from nested_mv2 where empid > 1;");
        String plan = getFragmentPlan("select empid, sum(deptno) from emps where empid > 1 group by empid");
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: nested_mv3\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: nested_mv3"));
        dropMv("test", "nested_mv1");
        dropMv("test", "nested_mv2");
        dropMv("test", "nested_mv3");
    }

    @Test
    public void testExternalNestedMVs1() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("test", "hive_nested_mv_1",
                "create materialized view hive_nested_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier");
        createAndRefreshMv("test", "hive_nested_mv_2",
                "create materialized view hive_nested_mv_2 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, sum(s_acctbal) from hive0.tpch.supplier group by s_suppkey");
        createAndRefreshMv("test", "hive_nested_mv_3",
                "create materialized view hive_nested_mv_3 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select * from hive_nested_mv_2 where s_suppkey > 1");
        String query1 = "select s_suppkey, sum(s_acctbal) from hive0.tpch.supplier where s_suppkey > 1 group by s_suppkey ";
        String plan1 = getFragmentPlan(query1);
        Assert.assertTrue(plan1.contains("0:OlapScanNode\n" +
                "     TABLE: hive_nested_mv_3\n" +
                "     PREAGGREGATION: ON\n"));
        dropMv("test", "hive_nested_mv_1");
        dropMv("test", "hive_nested_mv_2");
        dropMv("test", "hive_nested_mv_3");
    }
}
