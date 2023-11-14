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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRewriteTest extends MvRewriteTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
        MvRewriteTestBase.prepareDefaultDatas();
    }

    @Test
    public void testPlanCache() throws Exception {
        {
            String mvSql = "create materialized view agg_join_mv_1" +
                    " distributed by hash(v1) as SELECT t0.v1 as v1," +
                    " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                    " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                    " where t0.v1 < 100" +
                    " group by v1, test_all_type.t1d";
            starRocksAssert.withMaterializedView(mvSql);

            MaterializedView mv = getMv("test", "agg_join_mv_1");
            MvPlanContext planContext = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv, true);
            Assert.assertNotNull(planContext);
            Assert.assertTrue(CachingMvPlanContextBuilder.getInstance().contains(mv));
            planContext = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv, false);
            Assert.assertNotNull(planContext);
            starRocksAssert.dropMaterializedView("agg_join_mv_1");
        }

        {
            String mvSql = "create materialized view mv_with_window" +
                    " distributed by hash(t1d) as" +
                    " SELECT test_all_type.t1d, row_number() over (partition by t1c)" +
                    " from test_all_type";
            starRocksAssert.withMaterializedView(mvSql);

            MaterializedView mv = getMv("test", "mv_with_window");
            MvPlanContext planContext = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv, true);
            Assert.assertNotNull(planContext);
            Assert.assertFalse(CachingMvPlanContextBuilder.getInstance().contains(mv));
            starRocksAssert.dropMaterializedView("mv_with_window");
        }

        {
            for (int i = 0; i < 1010; i++) {
                String mvName = "plan_cache_mv_" + i;
                String mvSql = String.format("create materialized view %s" +
                        " distributed by hash(v1) as SELECT t0.v1 as v1," +
                        " test_all_type.t1d, sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                        " from t0 join test_all_type on t0.v1 = test_all_type.t1d" +
                        " where t0.v1 < 100" +
                        " group by v1, test_all_type.t1d", mvName);
                starRocksAssert.withMaterializedView(mvSql);

                MaterializedView mv = getMv("test", mvName);
                MvPlanContext planContext = CachingMvPlanContextBuilder.getInstance().getPlanContext(mv, true);
                Assert.assertNotNull(planContext);
            }
            for (int i = 0; i < 1010; i++) {
                String mvName = "plan_cache_mv_" + i;
                starRocksAssert.dropMaterializedView(mvName);
            }
        }
    }
<<<<<<< HEAD
=======

    @Test
    public void testMVAggregateTable() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `t1_agg` (\n" +
                "  `c_1_0` datetime NULL COMMENT \"\",\n" +
                "  `c_1_1` decimal128(24, 8) NOT NULL COMMENT \"\",\n" +
                "  `c_1_2` double SUM NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`c_1_0`, `c_1_1`)\n" +
                "DISTRIBUTED BY HASH(`c_1_1`) BUCKETS 3");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_t1_v0 " +
                "AS " +
                "SELECT t1_17.c_1_0, t1_17.c_1_1, SUM(t1_17.c_1_2) " +
                "FROM t1_agg AS t1_17 " +
                "GROUP BY t1_17.c_1_0, t1_17.c_1_1 ORDER BY t1_17.c_1_0 DESC, t1_17.c_1_1 ASC");

        {
            String query = "select * from t1_agg";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, query);
            PlanTestBase.assertContains(plan, "table: t1_agg, rollup: mv_t1_v0\n");
        }
        {

            String query = "select c_1_0, c_1_1, sum(c_1_2) from t1_agg group by c_1_0, c_1_1";
            String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, query);
            PlanTestBase.assertContains(plan, "table: t1_agg, rollup: mv_t1_v0\n");
        }

        starRocksAssert.dropMaterializedView("mv_t1_v0");
        starRocksAssert.dropTable("t1_agg");
    }

    @Test
    public void testQueryIncludingExcludingMVNames() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(3000000);
        createAndRefreshMv("test", "mv_agg_1", "CREATE MATERIALIZED VIEW mv_agg_1 " +
                " distributed by hash(empid) " +
                "AS " +
                "SELECT empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid");
        createAndRefreshMv("test", "mv_agg_2", "CREATE MATERIALIZED VIEW mv_agg_2 " +
                " distributed by hash(empid) " +
                "AS " +
                "SELECT empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid");
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("mv_agg_1");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_1");
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("mv_agg_2");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_2");
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("mv_agg_1, mv_agg_2");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_");
            starRocksAssert.getCtx().getSessionVariable().setQueryIncludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("mv_agg_1");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_2");
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("mv_agg_2");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_agg_1");
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("");
        }
        {
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("mv_agg_1, mv_agg_2");
            String query = "SELECT empid, sum(salary) as total " +
                    "FROM emps " +
                    "GROUP BY empid";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "mv_agg_");
            starRocksAssert.getCtx().getSessionVariable().setQueryExcludingMVNames("");
        }
        starRocksAssert.dropMaterializedView("mv_agg_1");
        starRocksAssert.dropMaterializedView("mv_agg_2");
    }
>>>>>>> 536efe49a3 ([BugFix] fix query_excluding_mv_names and query_including_mv_names bugs (#34873))
}
