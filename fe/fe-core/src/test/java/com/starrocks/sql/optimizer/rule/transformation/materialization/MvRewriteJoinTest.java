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
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MvRewriteJoinTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        prepareDatas();
    }

    @AfterAll
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

        starRocksAssert.withTable("CREATE TABLE test_partition_tbl3 (\n" +
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
    public void testMVRewriteJoin1() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl_mv2\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        "REFRESH ASYNC\n" +
                        "AS SELECT a.k1, a.v1, a.v2, b.v1 as b_v1\n" +
                        "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b on a.k1=b.k1;");
        // should not be rollup
        {
            String query = "select a.v2 FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and a.v1 = 1 and b.k1 = '2020-01-01' and b.v1 = 2;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 7: k1 = '2020-01-01', 8: v1 = 1, 10: b_v1 = 2\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rewritten
        {
            String query = "select a.v1, a.v2, b.v1 FROM test_partition_tbl1 as a " +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and b.k1 = '2020-01-01';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v2 FROM test_partition_tbl1 as a" +
                    " left join test_partition_tbl2 as b on a.k1=b.k1" +
                    " where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    " and a.v1=1 and b.v1=1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 7: k1 = '2020-01-01', 8: v1 = 1, 10: b_v1 = 1\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // rollup
        {
            String query = "select a.v1, a.v2, b.v1 FROM test_partition_tbl1 as a " +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1  where a.k1 >= '2020-01-01' and b.k1 >= '2020-01-01';";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv2");
    }

    @Test
    public void testMVRewriteJoin2() throws Exception {
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
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and a.v1 = 1 and b.k1 = '2020-01-01' and b.v1 = 2 " +
                    "group by a.k1, a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: v1 = 1, 11: b_v1 = 2\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // should not be rewritten
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl1 as a " +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 where a.k1 = '2020-01-01' and b.k1 = '2020-01-01' " +
                    "group by a.k1, a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }

        // should not be rollup
        {
            String query = "select a.v2, sum(a.v1) FROM test_partition_tbl1 as a" +
                    " left join test_partition_tbl2 as b on a.k1=b.k1" +
                    " where a.k1='2020-01-01' and b.k1 = '2020-01-01' " +
                    " and a.v1=1 and b.v1=1 group by a.k1, a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "PREDICATES: 8: k1 = '2020-01-01', 9: v1 = 1, 11: b_v1 = 1\n" +
                    "     partitions=1/6\n" +
                    "     rollup: test_partition_tbl_mv2");
        }

        // rollup
        {
            String query = "select a.v1, a.v2, b.v1, sum(a.v1) FROM test_partition_tbl1 as a " +
                    "left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1  where a.k1 >= '2020-01-01' and b.k1 >= '2020-01-01' " +
                    "group by a.k1, a.v1, a.v2, b.v1;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_partition_tbl_mv2");
        }
        starRocksAssert.dropMaterializedView("test_partition_tbl_mv2");
    }


    @Test
    public void testMvColocateJoinRewrite() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl1_colocate_mv1\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        "REFRESH ASYNC PROPERTIES ('colocate_with' = 'cg_1')\n" +
                        "AS SELECT v2 + 1, k1, v1, v2 FROM test_partition_tbl1;");

        createAndRefreshMv("CREATE MATERIALIZED VIEW test_partition_tbl1_colocate_mv2\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                        "REFRESH ASYNC PROPERTIES ('colocate_with' = 'cg_1')\n" +
                        "AS SELECT v2 + 1, k1, v1, v2 + 2 FROM test_partition_tbl1 where v1 > 1 ;");

        String query = "select t1.v2 + 1, t2.v2 + 1 from test_partition_tbl1 t1 join " +
                "test_partition_tbl1 t2 on t1.k1 = t2.k1 and t2.v1 > 1";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: k1 = 4: k1");

        starRocksAssert.dropMaterializedView("test_partition_tbl1_colocate_mv1");
        starRocksAssert.dropMaterializedView("test_partition_tbl1_colocate_mv2");
    }

    // test mv rewrite conflicts with topn push down
    @Test
    public void testMVRewriteJoinWithTopN1() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY k1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL\n" +
                "AS SELECT a.k1, a.v1, a.v2, b.v1 as b_v1\n" +
                "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b on a.k1=b.k1;");

        {
            // if mv rewrite mode is default, should not be rewritten
            connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(false);
            String query = "select a.k1, a.v2 FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 order by 1 limit 3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_mv1");
        }
        connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(true);
        {
            // if mv rewrite mode is force, should be rewritten
            String query = "select a.k1, a.v2 FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 order by 1 limit 3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        {
            // if mv rewrite mode is force, should be rewritten
            String query = "select a.k1, a.v2 FROM test_partition_tbl1 as a left join test_partition_tbl2 as b " +
                    "on a.k1=b.k1 order by 1 limit 3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1");
        }
        connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(false);
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    // test mv rewrite conflicts with topn push down
    @Test
    public void testMVRewriteJoinWithTopN2() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY k1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL\n" +
                "AS SELECT a.k1, a.v1, a.v2, b.v1 as b_v1\n" +
                "FROM test_partition_tbl1 as a left join test_partition_tbl2 as b on a.k1=b.k1;");
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv2\n" +
                "PARTITION BY k1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH MANUAL\n" +
                "AS SELECT a.k1, a.b_v1, a.v2, b.v1 \n" +
                "FROM test_mv1 as a left join test_partition_tbl3 as b on a.k1=b.k1;");
        {
            // if mv rewrite mode is default, should not be rewritten
            connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(false);
            String query = "select a.k1, a.v2 FROM test_partition_tbl1 a " +
                    " left join test_partition_tbl2 b on a.k1=b.k1 " +
                    " left join test_partition_tbl3 c on a.k1=c.k1 " +
                    " order by 1 limit 3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "test_mv1", "test_mv2");
        }
        connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(true);
        {
            // if mv rewrite mode is default, should be rewritten
            String query = "select a.k1, a.v2 FROM test_partition_tbl1 a " +
                    " left join test_partition_tbl2 b on a.k1=b.k1 " +
                    " left join test_partition_tbl3 c on a.k1=c.k1 " +
                    " order by 1 limit 3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv2");
        }
        {
            // if mv rewrite mode is default, should be rewritten
            String query = "select a.k1, a.v2 FROM test_partition_tbl1 a " +
                    " left join test_partition_tbl2 b on a.k1=b.k1 " +
                    " left join test_partition_tbl3 c on a.k1=c.k1 " +
                    " where b.v1=1 and c.v1=1 order by 1 limit 3;";
            String plan = getFragmentPlan(query, "MV");
            PlanTestBase.assertContains(plan, "test_mv2");
        }
        connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(false);
        starRocksAssert.dropMaterializedView("test_mv1");
        starRocksAssert.dropMaterializedView("test_mv2");
    }

    @Test
    public void testMVViewRewriteJoin() throws Exception {
        String view1 = "CREATE VIEW view1 as select * from test_partition_tbl1;";
        String view2 = "CREATE VIEW view2 as select * from test_partition_tbl2;";
        starRocksAssert.withView(view1);
        starRocksAssert.withView(view2);
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv1\n" +
                "REFRESH MANUAL\n" +
                "AS select * from view1;");
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv2\n" +
                "REFRESH MANUAL\n" +
                "AS select * from view2;");
        {
            String query = "select a.k1, a.v2 FROM (select * from view1 where k1='2020-02-01') a " +
                    " left join (select * from view2 where k1 ='2020-02-01')b on a.k1=b.k1 " +
                    " order by 1 limit 3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "test_mv1", "test_mv2");
        }

        cluster.runSql("test", "insert into test_partition_tbl1 values (\"2019-01-01\",1,1),(\"2019-01-01\",1,2)," +
                "(\"2019-01-01\",2,1),(\"2019-01-01\",2,2),\n" +
                "(\"2020-01-11\",1,1),(\"2020-01-11\",1,2),(\"2020-01-11\",2,1),(\"2020-01-11\",2,2),\n" +
                "(\"2020-02-11\",1,1),(\"2020-02-11\",1,2),(\"2020-02-11\",2,1),(\"2020-02-11\",2,2);");

        {
            String query = "select a.k1, a.v2 FROM (select * from view1 where k1='2020-02-01') a " +
                    " left join (select * from view2 where k1 ='2020-02-01')b on a.k1=b.k1 " +
                    " order by 1 limit 3;";
            String plan = getFragmentPlan(query, "MV");
            // original predicate should be kept
            PlanTestBase.assertContains(plan, "     TABLE: test_partition_tbl1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: k1 = '2020-02-01'");
            PlanTestBase.assertContains(plan, "     TABLE: test_mv2\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 16: k1 = '2020-02-01'");
        }
        starRocksAssert.dropMaterializedView("test_mv1");
        starRocksAssert.dropMaterializedView("test_mv2");
    }

    // Regression test for s026: FETCH/LOOKUP slot must honor the storage column's
    // isAllowNull when MV rewrite produces a Project whose target ColumnRef inherits
    // a stale nullable=false from the original (pre-rewrite) PK-table scan.
    @Test
    public void testMVRewriteFullOuterJoinLazyMaterialization() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t1_s026 (\n" +
                " id BIGINT, dt DATE, region VARCHAR(32), name VARCHAR(64))\n" +
                " PRIMARY KEY(id, dt, region) DISTRIBUTED BY HASH(id) BUCKETS 4\n" +
                " PROPERTIES('replication_num'='1');");
        starRocksAssert.withTable("CREATE TABLE t2_s026 (\n" +
                " id BIGINT, dt DATE, region VARCHAR(32), amount DECIMAL(18, 2))\n" +
                " PRIMARY KEY(id, dt, region) DISTRIBUTED BY HASH(id) BUCKETS 4\n" +
                " PROPERTIES('replication_num'='1');");
        cluster.runSql("test", "insert into t1_s026 values " +
                "(1,'2026-01-15','north','alice'),(2,'2026-01-15','south','bob');");
        cluster.runSql("test", "insert into t2_s026 values " +
                "(1,'2026-01-15','north',100),(2,'2026-01-15','south',200);");

        createAndRefreshMv("CREATE MATERIALIZED VIEW mv_s026\n" +
                "DISTRIBUTED BY HASH(dt) BUCKETS 4\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES('replication_num'='1')\n" +
                "AS SELECT a.dt, a.region, SUM(b.amount) AS total_amount, COUNT(*) AS cnt\n" +
                "FROM t1_s026 a FULL OUTER JOIN t2_s026 b ON a.id = b.id\n" +
                "GROUP BY a.dt, a.region;");

        boolean prevGlm = connectContext.getSessionVariable().isEnableGlobalLateMaterialization();
        try {
            connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
            connectContext.getSessionVariable().setEnableGlobalLateMaterializationCostBased(false);

            String query = "SELECT a.dt, a.region, SUM(b.amount), COUNT(*)\n" +
                    "FROM t1_s026 a FULL OUTER JOIN t2_s026 b ON a.id = b.id\n" +
                    "GROUP BY a.dt, a.region\n" +
                    "ORDER BY a.dt LIMIT 10;";
            String plan = getFragmentPlan(query, TExplainLevel.COSTS);
            // Sanity: query is rewritten to MV and FETCH path is taken.
            PlanTestBase.assertContains(plan, "mv_s026");
            PlanTestBase.assertContains(plan, "FETCH");
            // Bug repro: the lazily-fetched region slot used to be emitted as nullable=false
            // because it inherited from the pre-rewrite t1_s026.region (PK column, NOT NULL).
            // After the fix, the slot's nullable is widened by the storage column's isAllowNull.
            PlanTestBase.assertNotContains(plan, "[region, VARCHAR(1048576), false]");
            PlanTestBase.assertContains(plan, "[region, VARCHAR(1048576), true]");
        } finally {
            connectContext.getSessionVariable().setEnableGlobalLateMaterialization(prevGlm);
            starRocksAssert.dropMaterializedView("mv_s026");
            starRocksAssert.dropTable("t1_s026");
            starRocksAssert.dropTable("t2_s026");
        }
    }
}
