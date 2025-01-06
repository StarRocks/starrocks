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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.profile.Tracers;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MvRewritePartitionTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "table_with_day_partition");
        starRocksAssert.withTable(cluster, "table_with_day_partition1");
        starRocksAssert.withTable(cluster, "table_with_day_partition2");
        prepareDatas();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        try {
            starRocksAssert.dropTable("test_partition_tbl1");
            starRocksAssert.dropTable("test_partition_tbl2");
            starRocksAssert.dropTable("test_partition_tbl_not_null1");
            starRocksAssert.dropTable("test_partition_tbl_not_null2");
        } catch (Exception e) {
            // ignore exceptions.
        }
    }

    public static void prepareDatas() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_partition_tbl1 (\n" +
                " k1 date NOT NULL,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2020-02-01'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2020-03-01'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2020-04-01'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2020-05-01'),\n" +
                "   PARTITION p6 VALUES LESS THAN ('2020-06-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");

        starRocksAssert.withTable("CREATE TABLE test_partition_tbl2 (\n" +
                " k1 date NOT NULL,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2020-02-01'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2020-03-01'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2020-04-01'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2020-05-01'),\n" +
                "   PARTITION p6 VALUES LESS THAN ('2020-06-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");

        cluster.runSql("test", "insert into test_partition_tbl1 values (\"2019-01-01\",1,1),(\"2019-01-01\",1,2)," +
                "(\"2019-01-01\",2,1),(\"2019-01-01\",2,2),\n" +
                "(\"2020-01-11\",1,1),(\"2020-01-11\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2),\n" +
                "(\"2020-02-11\",1,1),(\"2020-02-11\",1,2),(\"2020-02-11\",2,1),(\"2020-02-11\",2,2);");

        starRocksAssert.withTable("CREATE TABLE test_partition_tbl_not_null1 (\n" +
                " k1 date NOT NULL,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2020-02-01'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2020-03-01'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2020-04-01'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2020-05-01'),\n" +
                "   PARTITION p6 VALUES LESS THAN ('2020-06-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");

        starRocksAssert.withTable("CREATE TABLE test_partition_tbl_not_null2 (\n" +
                " k1 date NOT NULL,\n" +
                " v1 INT,\n" +
                " v2 INT)\n" +
                " DUPLICATE KEY(k1)\n" +
                " PARTITION BY RANGE(k1)\n" +
                " (\n" +
                "   PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                "   PARTITION p2 VALUES LESS THAN ('2020-02-01'),\n" +
                "   PARTITION p3 VALUES LESS THAN ('2020-03-01'),\n" +
                "   PARTITION p4 VALUES LESS THAN ('2020-04-01'),\n" +
                "   PARTITION p5 VALUES LESS THAN ('2020-05-01'),\n" +
                "   PARTITION p6 VALUES LESS THAN ('2020-06-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k1);");

        cluster.runSql("test", "insert into test_partition_tbl_not_null1 values (\"2019-01-01\",1,1),(\"2019-01-01\",1,2)," +
                "(\"2019-01-01\",2,1),(\"2019-01-01\",2,2),\n" +
                "(\"2020-01-11\",1,1),(\"2020-01-11\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2),\n" +
                "(\"2020-02-11\",1,1),(\"2020-02-11\",1,2),(\"2020-02-11\",2,1),(\"2020-02-11\",2,2);");
    }

    @Test
    public void testPartitionPrune1() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv1\n" +
                        " PARTITION BY k1\n" +
                        " DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        " REFRESH ASYNC\n" +
                        " PROPERTIES(\n" +
                        " \"partition_ttl_number\"=\"5\",\n" +
                        " \"auto_refresh_partitions_limit\"=\"4\"\n" +
                        " )\n" +
                        " AS SELECT k1, sum(v1) as sum_v1 FROM test_partition_tbl1 group by k1;");
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-02-11' group by k1;";
            String plan = getFragmentPlan(query);
            String pr = Tracers.printLogs();
            Tracers.close();
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "PREDICATES: 5: k1 >= '2020-02-11'\n" +
                    "     partitions=4/5");
        }
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-02-01' group by k1;";
            String plan = getFragmentPlan(query);
            String pr = Tracers.printLogs();
            Tracers.close();
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "partitions=4/5\n" +
                    "     rollup: test_partition_tbl_mv1");
        }
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-03-01' group by k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "partitions=3/5\n" +
                    "     rollup: test_partition_tbl_mv1");
        }
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-04-01' group by k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "partitions=2/5\n" +
                    "     rollup: test_partition_tbl_mv1");
        }
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-05-01' group by k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "partitions=1/5\n" +
                    "     rollup: test_partition_tbl_mv1");
        }
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-06-01' group by k1;";
            String plan = getFragmentPlan(query);

            String pr = Tracers.printLogs();
            Tracers.close();
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv1");
    }

    @Test
    public void testPartitionPrune2() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv1\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "   AS SELECT k1, v1, v2, sum(v1) as sum_v1 FROM test_partition_tbl1 group by k1, v1, v2;");
        // should not be rollup
        {
            String query = "select v2, sum(v1) FROM test_partition_tbl1 where k1 = '2020-01-01' and v1 = 1 group by v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 5: k1 = '2020-01-01', 6: v1 = 1\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv1");
        }

        // should not be rollup
        {
            String query = "select v1, v2, sum(v1) FROM test_partition_tbl1 where k1 = '2020-01-01' group by v1, v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 5: k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv1");
        }

        // should not be rollup
        {
            String query = "select v2, sum(v1) FROM test_partition_tbl1 where k1 = '2020-01-01' " +
                    "and v1 = 1 group by v2 having sum (v1) > 100";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "     PREDICATES: 5: k1 = '2020-01-01', 6: v1 = 1, 8: sum_v1 > 100\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv1");
        }

        // should not be rollup
        {
            String query = "select v2, sum(v1) FROM test_partition_tbl1 where k1 = '2020-03-31' " +
                    "and v1 = 1 group by v2 having sum (v1) > 100";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "     PREDICATES: 5: k1 = '2020-03-31', 6: v1 = 1, 8: sum_v1 > 100\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv1");
        }

        // rollup
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-01-01' group by k1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "TABLE: test_partition_tbl_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 5: k1 >= '2020-01-01'\n" +
                    "     partitions=5/6");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv1");
    }

    @Test
    public void testPartitionPrune3() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv2\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "   AS SELECT a.k1, a.v1, a.v2, b.v1 as b_v1, sum(a.v1) as sum_v1 " +
                        "FROM test_partition_tbl1 as a join test_partition_tbl2 as b on a.k1=b.k1 " +
                        "group by a.k1, a.v1, a.v2, b.v1;");
        // should not be rollup
        {
            String query = "select a.v2, sum(a.v1) FROM test_partition_tbl1 as a join test_partition_tbl2 as b on a.k1=b.k1" +
                    " where a.k1 = '2020-01-01' and a.v1 = 1 and b.v1 = 2 group by a.v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: v1 = 1, 11: b_v1 = 2\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' group by a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01'\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v2, sum(a.v1) FROM test_partition_tbl1 as a join test_partition_tbl2 as b on a.k1=b.k1" +
                    " where a.k1='2020-01-01' and a.v1=1 and b.v1=1 group by a.v2 having sum(a.v1) > 100;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: v1 = 1, " +
                    "11: b_v1 = 1, 12: sum_v1 > 100\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v1, a.v2,b.v1, sum(a.v1) FROM test_partition_tbl1 as a join test_partition_tbl2 as b on a" +
                    ".k1=b.k1 where a.k1 = '2020-01-01' group by a.v1, a.v2, b.v1 having sum(a.v1) > 100;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "     PREDICATES: 8: k1 = '2020-01-01', 12: sum_v1 > 100\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // rollup
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl1 as a join test_partition_tbl2 as b " +
                    "on a.k1=b.k1  where a.k1 >= '2020-01-01' group by a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "partitions=5/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv2");
    }

    @Test
    public void testPartitionPrune4() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv2\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "AS SELECT a.k1, a.v1, a.v2, b.v1 as b_v1, sum(a.v1) as sum_v1 " +
                        "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b on a.k1=b.k1 " +
                        "group by a.k1, a.v1, a.v2, b.v1;");
        // should not be rollup
        {
            String query = "select a.v2, sum(a.v1) FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and a.v1 = 1 and b.k1 = '2020-01-01' and b.v1 = 2 group by a.v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: v1 = 1, 11: b_v1 = 2\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rewritten
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl1 as a " +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' group by a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v2, sum(a.v1) FROM test_partition_tbl1 as a" +
                    " left join test_partition_tbl2 as b on a.k1=b.k1" +
                    " where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    " and a.v1=1 and b.v1=1 group by a.v2 having sum(a.v1) > 100;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', " +
                    "9: v1 = 1, 11: b_v1 = 1, 12: sum_v1 > 100\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rewritten
        {
            String query = "select a.v1, a.v2,b.v1, sum(a.v1) FROM test_partition_tbl1 as a " +
                    " left join test_partition_tbl2 as b on a.k1=b.k1 " +
                    " where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' group by a.v1, a.v2, b.v1 having sum(a.v1) > 100;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }

        // rollup
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl1 as a " +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1  where a.k1 >= '2020-01-01' and b.k1 >= '2020-01-01' group by a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv2");
    }

    @Test
    public void testPartitionPrune5() throws Exception {
        // join key is not null
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv2\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "   AS SELECT a.k1, a.v1, a.v2, b.v1 as b_v1, sum(a.v1) as sum_v1 " +
                        "FROM test_partition_tbl_not_null1 as a left join test_partition_tbl_not_null2 as b on a.k1=b.k1 " +
                        "group by a.k1, a.v1, a.v2, b.v1;");
        // should not be rollup
        {
            String query = "select a.v2, sum(a.v1) FROM test_partition_tbl_not_null1 as a left join " +
                    "test_partition_tbl_not_null2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and a.v1 = 1 and b.k1 = '2020-01-01' and b.v1 = 2 group by a.v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: v1 = 1, 11: b_v1 = 2\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl_not_null1 as a " +
                    "left join test_partition_tbl_not_null2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' group by a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v2, sum(a.v1) FROM test_partition_tbl_not_null1 as a" +
                    " left join test_partition_tbl_not_null2 as b on a.k1=b.k1" +
                    " where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    " and a.v1=1 and b.v1=1 group by a.v2 having sum(a.v1) > 100;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: v1 = 1, " +
                    "11: b_v1 = 1, 12: sum_v1 > 100\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v1, a.v2,b.v1, sum(a.v1) FROM test_partition_tbl_not_null1 as a " +
                    " left join test_partition_tbl_not_null2 as b on a.k1=b.k1 " +
                    " where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' group by a.v1, a.v2, b.v1 having sum(a.v1) > 100;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }

        // rollup
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl_not_null1 as a " +
                    "left join test_partition_tbl_not_null2 as b " +
                    "on a.k1=b.k1  where a.k1 >= '2020-01-01' and b.k1 >= '2020-01-01' group by a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv2");
    }

    @Test
    public void testPartitionPrune6() throws Exception {
        // a.k1/b.k1 are both output
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv2\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "AS SELECT a.k1, b.k1 as b_k1, a.v1, a.v2, b.v1 as b_v1, sum(a.v1) as sum_v1 " +
                        "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b on a.k1=b.k1 " +
                        "group by a.k1, a.v1, a.v2, b.k1, b.v1;");
        // should not be rollup
        {
            String query = "select a.v2, sum(a.v1) FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and a.v1 = 1 and b.k1 = '2020-01-01' and b.v1 = 2 group by a.v2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 10: v1 = 1, 12: b_v1 = 2\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rewritten
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl1 as a " +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' group by a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: b_k1 IS NOT NULL\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v2, sum(a.v1) FROM test_partition_tbl1 as a" +
                    " left join test_partition_tbl2 as b on a.k1=b.k1" +
                    " where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    " and a.v1=1 and b.v1=1 group by a.v2 having sum(a.v1) > 100;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 10: v1 = 1, " +
                    "12: b_v1 = 1, 13: sum_v1 > 100\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // rollup
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl1 as a " +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1  where a.k1 >= '2020-01-01' and b.k1 >= '2020-01-01' group by a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 >= '2020-01-01', 9: b_k1 IS NOT NULL\n" +
                    "     partitions=5/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv2");
    }


    @Test
    public void testMVPartitionPruneWithMultiLeftOuterJoin() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW if not exists test_mv1\n" +
                "PARTITION BY date_trunc('day', id_date) \n" +
                "DISTRIBUTED BY hash(t1a) BUCKETS 4  \n" +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"\n" +
                ") " +
                "AS " +
                "select a.t1a, a.id_date, sum(a.t1b), sum(b.t1b) " +
                "from table_with_day_partition a" +
                " left join table_with_day_partition1 b on a.id_date=b.id_date " +
                " left join table_with_day_partition2 c on a.id_date=c.id_date " +
                "group by a.t1a,a.id_date;");
        cluster.runSql("test", "refresh materialized view test_mv1 partition " +
                "start('1991-03-30') end('1991-03-31') with sync mode; ");

        {
            String query = "select a.t1a, a.id_date, sum(a.t1b), sum(b.t1b) \n" +
                    "from table_with_day_partition a\n" +
                    " left join table_with_day_partition1 b on a.id_date=b.id_date \n" +
                    " left join table_with_day_partition2 c on a.id_date=c.id_date \n" +
                    " where a.id_date='1991-03-30' " +
                    " group by a.t1a,a.id_date;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                    "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/1\n" +
                    "     rollup: test_mv1");
        }

        {
            String query = "select a.t1a, a.id_date, sum(a.t1b), sum(b.t1b) \n" +
                    "from table_with_day_partition a\n" +
                    " left join table_with_day_partition1 b on a.id_date=b.id_date \n" +
                    " left join table_with_day_partition2 c on a.id_date=c.id_date \n" +
                    " where a.id_date>='1991-03-30' " +
                    " group by a.t1a,a.id_date;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/1");
            PlanTestBase.assertContains(plan, "     TABLE: table_with_day_partition\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=3/4");
        }

        {
            String query = "select a.t1a, a.id_date, sum(a.t1b), sum(b.t1b) \n" +
                    "from table_with_day_partition a\n" +
                    " left join table_with_day_partition1 b on a.id_date=b.id_date \n" +
                    " left join table_with_day_partition2 c on a.id_date=c.id_date \n" +
                    " group by a.t1a,a.id_date;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/1\n" +
                    "     rollup: test_mv1\n" +
                    "     tabletRatio=4/4");
            PlanTestBase.assertContains(plan, "     TABLE: table_with_day_partition\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=3/4\n" +
                    "     rollup: table_with_day_partition\n" +
                    "     tabletRatio=9/9");
        }
    }

    static class PCompensateExpect {
        public String partitionPredicate;
        public boolean isCompensateUnionAll;
        public boolean isExpectRewrite;
        public PCompensateExpect(String partitionPredicate, boolean isCompensateUnionAll, boolean isExpectRewrite) {
            this.partitionPredicate = partitionPredicate;
            this.isCompensateUnionAll = isCompensateUnionAll;
            this.isExpectRewrite = isExpectRewrite;
        }

        public static PCompensateExpect create(String partitionPredicate, boolean isCompensateUnionAll,
                                               boolean isExpectRewrite) {
            return new PCompensateExpect(partitionPredicate, isCompensateUnionAll, isExpectRewrite);
        }

        public static PCompensateExpect create(String partitionPredicate, boolean isExpectRewrite) {
            return new PCompensateExpect(partitionPredicate, false, isExpectRewrite);
        }

        @Override
        public String toString() {
            return String.format("partitionPredicate=%s, isCompensateUnionAll=%s, isExpectRewrite=%s",
                    partitionPredicate, isCompensateUnionAll, isExpectRewrite);
        }
    }

    class PartitionCompensateParam {
        public String mvPartitionExpr;
        public String refreshStart;
        public String refreshEnd;
        public List<PCompensateExpect> expectPartitionPredicates;
        public PartitionCompensateParam(String mvPartitionExpr,
                                        String refreshStart, String refreshEnd,
                                        List<PCompensateExpect> expectPartitionPredicates) {
            this.mvPartitionExpr = mvPartitionExpr;
            this.refreshStart = refreshStart;
            this.refreshEnd = refreshEnd;
            this.expectPartitionPredicates = expectPartitionPredicates;
        }

        @Override
        public String toString() {
            return String.format("mvPartitionExpr=%s, refreshStart=%s, refreshEnd=%s, " +
                            "expectPartitionPredicates=%s", mvPartitionExpr, refreshStart, refreshEnd,
                    Joiner.on(",").join(expectPartitionPredicates));
        }
    }

    private void testRefreshAndRewriteWithMultiJoinMV(PartitionCompensateParam param) {
        starRocksAssert.withMaterializedView(String.format("CREATE MATERIALIZED VIEW if not exists test_mv1\n" +
                        "PARTITION BY %s \n" +
                        "REFRESH DEFERRED MANUAL " +
                        "AS " +
                        "select a.t1a, a.id_date, sum(a.t1b), sum(b.t1b) " +
                        "from table_with_day_partition a" +
                        " left join table_with_day_partition1 b on a.id_date=b.id_date " +
                        " left join table_with_day_partition2 c on a.id_date=c.id_date " +
                        "group by a.t1a,a.id_date;", param.mvPartitionExpr),
                (obj) -> {
                    String mvName = (String) obj;
                    cluster.runSql("test", String.format("refresh materialized view %s partition " +
                            "start('%s') end('%s') with sync mode;", mvName, param.refreshStart, param.refreshEnd));
                    for (PCompensateExpect expect : param.expectPartitionPredicates) {
                        if (!Strings.isNullOrEmpty(expect.partitionPredicate)) {
                            String query = String.format("select a.t1a, a.id_date, sum(a.t1b), sum(b.t1b) \n" +
                                    "from table_with_day_partition a\n" +
                                    " left join table_with_day_partition1 b on a.id_date=b.id_date \n" +
                                    " left join table_with_day_partition2 c on a.id_date=c.id_date \n" +
                                    " where %s " +
                                    " group by a.t1a,a.id_date;", expect.partitionPredicate);
                            String plan = getFragmentPlan(query);
                            if (expect.isExpectRewrite) {
                                if (expect.isCompensateUnionAll) {
                                    PlanTestBase.assertContains(plan, "UNION");
                                }
                                PlanTestBase.assertContains(plan, mvName);
                            } else {
                                PlanTestBase.assertNotContains(plan, mvName);
                                PlanTestBase.assertNotContains(plan, "UNION");
                            }
                        }
                    }
                });
    }

    @Test
    public void testMVPartitionWithCompensate() {
        connectContext.getSessionVariable().setEnableMaterializedViewTransparentUnionRewrite(false);
        List<PartitionCompensateParam> params = ImmutableList.of(
                // partition: date_trunc expr
                new PartitionCompensateParam("date_trunc('day', id_date)",
                        "1991-03-30", "1991-03-31",
                        ImmutableList.of(
                                // no partition expressions
                                PCompensateExpect.create("a.id_date='1991-03-30'", false, true),
                                PCompensateExpect.create("a.id_date>='1991-03-30'", true, true),
                                PCompensateExpect.create("a.id_date!='1991-03-30'", false),
                                // with partition expressions && partition expressions can be pruned
                                PCompensateExpect.create("date_format(a.id_date, '%Y%m%d')='19910330'", false, true),
                                PCompensateExpect.create("date_format(a.id_date, '%Y-%m-%d')='1991-03-30'", false, true),
                                PCompensateExpect.create("date_trunc('day', a.id_date)='1991-03-30'", false, true),
                                PCompensateExpect.create("date_trunc('day', a.id_date)>='1991-03-30'", true, true),
                                PCompensateExpect.create("subdate(a.id_date, interval 1 day)='1991-03-29'", false, true),
                                PCompensateExpect.create("adddate(a.id_date, interval 1 day)='1991-03-31'", false, true),
                                // with partition expressions && partition expressions can be pruned
                                PCompensateExpect.create("cast(a.id_date as string)='1991-03-30'", false),
                                PCompensateExpect.create("cast(a.id_date as string) >='1991-03-30'", false)
                        )
                ),
                // partition: slot
                new PartitionCompensateParam("id_date",
                        "1991-03-30", "1991-03-31",
                        ImmutableList.of(
                                // no partition expressions
                                PCompensateExpect.create("a.id_date='1991-03-30'", false, true),
                                PCompensateExpect.create("a.id_date>='1991-03-30'", true, true),
                                PCompensateExpect.create("a.id_date!='1991-03-30'", false),
                                // with partition expressions && partition expressions can be pruned
                                PCompensateExpect.create("date_format(a.id_date, '%Y%m%d')='19910330'", false, true),
                                PCompensateExpect.create("date_format(a.id_date, '%Y-%m-%d')='1991-03-30'", false, true),
                                PCompensateExpect.create("date_trunc('day', a.id_date)='1991-03-30'", false, true),
                                PCompensateExpect.create("date_trunc('day', a.id_date)>='1991-03-30'", true, true),
                                PCompensateExpect.create("subdate(a.id_date, interval 1 day)='1991-03-29'", false, true),
                                PCompensateExpect.create("adddate(a.id_date, interval 1 day)='1991-03-31'", false, true),
                                // with partition expressions && partition expressions can be pruned
                                PCompensateExpect.create("cast(a.id_date as string)='1991-03-30'", false),
                                PCompensateExpect.create("cast(a.id_date as string) >='1991-03-30'", false)
                        )
                )
        );
        for (PartitionCompensateParam param : params) {
            testRefreshAndRewriteWithMultiJoinMV(param);
        }
        connectContext.getSessionVariable().setEnableMaterializedViewTransparentUnionRewrite(true);
    }

    @Test
    public void testMVPartitionWithNoPartitionCompensate() {
        connectContext.getSessionVariable().setEnableMaterializedViewRewritePartitionCompensate(false);
        List<PartitionCompensateParam> params = ImmutableList.of(
                // partition: date_trunc expr
                new PartitionCompensateParam("date_trunc('day', id_date)",
                        "1991-03-30", "1991-03-31",
                        ImmutableList.of(
                                // no partition expressions
                                PCompensateExpect.create("a.id_date='1991-03-30'", true),
                                PCompensateExpect.create("a.id_date>='1991-03-30'", true),
                                PCompensateExpect.create("a.id_date!='1991-03-30'", true),
                                // with partition expressions && partition expressions can be pruned
                                PCompensateExpect.create("date_format(a.id_date, '%Y%m%d')='19910330'", true),
                                PCompensateExpect.create("date_format(a.id_date, '%Y-%m-%d')='1991-03-30'", true),
                                PCompensateExpect.create("date_trunc('day', a.id_date)='1991-03-30'", true),
                                PCompensateExpect.create("date_trunc('day', a.id_date)>='1991-03-30'", true),
                                PCompensateExpect.create("subdate(a.id_date, interval 1 day)='1991-03-29'", true),
                                PCompensateExpect.create("adddate(a.id_date, interval 1 day)='1991-03-31'", true),
                                // with partition expressions && partition expressions can be pruned
                                PCompensateExpect.create("cast(a.id_date as string)='1991-03-30'", true),
                                PCompensateExpect.create("cast(a.id_date as string) >='1991-03-30'", true)
                        )
                ),
                // partition: slot
                new PartitionCompensateParam("id_date",
                        "1991-03-30", "1991-03-31",
                        ImmutableList.of(
                                // no partition expressions
                                PCompensateExpect.create("a.id_date='1991-03-30'", true),
                                PCompensateExpect.create("a.id_date>='1991-03-30'", true),
                                PCompensateExpect.create("a.id_date!='1991-03-30'", true),
                                // with partition expressions && partition expressions can be pruned
                                PCompensateExpect.create("date_format(a.id_date, '%Y%m%d')='19910330'", true),
                                PCompensateExpect.create("date_format(a.id_date, '%Y-%m-%d')='1991-03-30'", true),
                                PCompensateExpect.create("date_trunc('day', a.id_date)='1991-03-30'", true),
                                PCompensateExpect.create("date_trunc('day', a.id_date)>='1991-03-30'", true),
                                PCompensateExpect.create("subdate(a.id_date, interval 1 day)='1991-03-29'", true),
                                PCompensateExpect.create("adddate(a.id_date, interval 1 day)='1991-03-31'", true),
                                // with partition expressions && partition expressions can be pruned
                                PCompensateExpect.create("cast(a.id_date as string)='1991-03-30'", true),
                                PCompensateExpect.create("cast(a.id_date as string) >='1991-03-30'", true)
                        )
                )
        );
        for (PartitionCompensateParam param : params) {
            testRefreshAndRewriteWithMultiJoinMV(param);
        }
        connectContext.getSessionVariable().setEnableMaterializedViewRewritePartitionCompensate(true);
    }

    @Test
    public void testMVRewriteWithHaving() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `test_pt` (\n" +
                "  `id` int(11) NULL,\n" +
                "  `pt` date NOT NULL,\n" +
                "  `gmv` int(11) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        executeInsertSql(connectContext, "insert into test_pt values(2,'2023-03-07',10), (2,'2023-03-08',10), (2," +
                "'2023-03-11',10);");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test_pt_mv4` \n" +
                        "PARTITION BY (`pt`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH ASYNC START(\"2024-03-08 03:00:00\") EVERY(INTERVAL 1 DAY)\n" +
                        "PROPERTIES (\n" +
                        "\"partition_refresh_number\" = \"1\"\n" +
                        ")\n" +
                        "AS SELECT `pt`, `id`, sum(gmv) AS `sum_gmv`, count(gmv) AS `count_gmv`\n" +
                        "FROM `test`.`test_pt`\n" +
                        "GROUP BY `pt`, `id`;",
                () -> {
                    String plan = getFragmentPlan("select pt, avg(gmv) as a from test_pt group by pt having a>0;");
                    PlanTestBase.assertContains(plan, "     TABLE: test_pt_mv4\n" +
                            "     PREAGGREGATION: ON\n" +
                            "     partitions=0/0");
                    PlanTestBase.assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                            "  |  output: sum(7: sum_gmv), sum(8: count_gmv)\n" +
                            "  |  group by: 5: pt\n" +
                            "  |  having: CAST(11: sum AS DOUBLE) / CAST(12: count AS DOUBLE) > 0.0");
                });
    }
}
