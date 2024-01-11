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
import com.starrocks.qe.SessionVariable;
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
            Assert.assertTrue(CachingMvPlanContextBuilder.getInstance().contains(mv));
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

    @Test
    public void testWithSqlSelectLimit() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setSqlSelectLimit(1000);
        createAndRefreshMv("test", "mv_with_select_limit",
                "CREATE MATERIALIZED VIEW mv_with_select_limit " +
                " distributed by hash(empid) " +
                "AS " +
                "SELECT /*+set_var(sql_select_limit=1000)*/ empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid");
        starRocksAssert.query("SELECT empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid").explainContains("mv_with_select_limit");
        starRocksAssert.getCtx().getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
    }

    @Test
    public void testInsertMV() throws Exception {
        String mvName = "mv_insert";
        createAndRefreshMv("test", mvName, "create materialized view " + mvName +
                " distributed by hash(v1) " +
                "refresh async as " +
                "select * from t0");
        String sql = "insert into t0 select * from t0";

        // enable
        {
            starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewRewriteForInsert(true);
            starRocksAssert.query(sql).explainContains(mvName);
        }

        // disable
        {
            starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewRewriteForInsert(false);
            starRocksAssert.query(sql).explainWithout(mvName);
        }

        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewRewriteForInsert(
                SessionVariable.DEFAULT_SESSION_VARIABLE.isEnableMaterializedViewRewriteForInsert());
    }
}
