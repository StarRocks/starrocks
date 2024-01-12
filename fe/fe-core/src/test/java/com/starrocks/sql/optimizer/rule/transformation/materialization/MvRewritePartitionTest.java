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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRewritePartitionTest extends MvRewriteTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
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
            PlanTestBase.assertContains(plan, "test_partition_tbl_mv1");
            PlanTestBase.assertContains(plan, "PREDICATES: 5: k1 >= '2020-02-11'\n" +
                    "     partitions=4/5");
        }
        {
            String query = "select k1, sum(v1) FROM test_partition_tbl1 where k1>='2020-02-01' group by k1;";
            String plan = getFragmentPlan(query);
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
            PlanTestBase.assertContains(plan, "PREDICATES: 8: sum_v1 > 100, 5: k1 = '2020-01-01', 6: v1 = 1\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv1");
        }

        // should not be rollup
        {
            String query = "select v2, sum(v1) FROM test_partition_tbl1 where k1 = '2020-03-31' " +
                    "and v1 = 1 group by v2 having sum (v1) > 100";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 8: sum_v1 > 100, 5: k1 = '2020-03-31', 6: v1 = 1\n" +
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
                    "     partitions=5/6\n" +
                    "     rollup: test_partition_tbl_mv1");
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
            PlanTestBase.assertContains(plan, "PREDICATES: 12: sum_v1 > 100, 8: k1 = '2020-01-01', " +
                    "9: v1 = 1, 11: b_v1 = 1\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v1, a.v2,b.v1, sum(a.v1) FROM test_partition_tbl1 as a join test_partition_tbl2 as b on a" +
                    ".k1=b.k1 where a.k1 = '2020-01-01' group by a.v1, a.v2, b.v1 having sum(a.v1) > 100;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 12: sum_v1 > 100, 8: k1 = '2020-01-01'\n" +
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
            PlanTestBase.assertContains(plan, "PREDICATES: 12: sum_v1 > 100, 8: k1 = '2020-01-01', " +
                    "9: v1 = 1, 11: b_v1 = 1\n" +
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
            PlanTestBase.assertContains(plan, "PREDICATES: 12: sum_v1 > 100, 8: k1 = '2020-01-01', " +
                    "9: v1 = 1, 11: b_v1 = 1\n" +
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
            PlanTestBase.assertContains(plan, "PREDICATES: 13: sum_v1 > 100, 8: k1 = '2020-01-01', " +
                    "10: v1 = 1, 12: b_v1 = 1\n" +
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
            PlanTestBase.assertContains(plan, "PREDICATES: 9: b_k1 IS NOT NULL\n" +
                    "     partitions=5/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv2");
    }
}
