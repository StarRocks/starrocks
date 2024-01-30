// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
        createAndRefreshMv("test", "mv_with_select_limit", "CREATE MATERIALIZED VIEW mv_with_select_limit " +
                " distributed by hash(empid) " +
                "AS " +
                "SELECT /*+set_var(sql_select_limit=1000)*/ empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid");
        starRocksAssert.query("SELECT empid, sum(salary) as total " +
                "FROM emps " +
                "GROUP BY empid").explainContains("mv_with_select_limit");
        starRocksAssert.getCtx().getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        starRocksAssert.dropMaterializedView("mv_with_select_limit");
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

        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewRewriteForInsert(false);
    }

    @Test
    public void test() throws Exception {
        connectContext.executeSql("drop table if exists t11");
        starRocksAssert.withTable("create table t11(\n" +
                "shop_id int,\n" +
                "region int,\n" +
                "shop_type string,\n" +
                "shop_flag string,\n" +
                "store_id String,\n" +
                "store_qty Double\n" +
                ") DUPLICATE key(shop_id) distributed by hash(shop_id) buckets 1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        cluster.runSql("test", "insert into\n" +
                "t11\n" +
                "values\n" +
                "(1, 1, 's', 'o', '1', null),\n" +
                "(1, 1, 'm', 'o', '2', 2),\n" +
                "(1, 1, 'b', 'c', '3', 1);");
        connectContext.executeSql("drop materialized view if exists mv11");
        starRocksAssert.withMaterializedView("create MATERIALIZED VIEW mv11 (region, ct) " +
                "DISTRIBUTED BY RANDOM buckets 1 REFRESH MANUAL as\n" +
                "select region,\n" +
                "count(\n" +
                "distinct (\n" +
                "case\n" +
                "when store_qty > 0 then store_id\n" +
                "else null\n" +
                "end\n" +
                ")\n" +
                ")\n" +
                "from t11\n" +
                "group by region;");
        cluster.runSql("test", "refresh materialized view mv11 with sync mode");
        {
            String query = "select region,\n" +
                    "count(\n" +
                    "distinct (\n" +
                    "case\n" +
                    "when store_qty > 0.0 then store_id\n" +
                    "else null\n" +
                    "end\n" +
                    ")\n" +
                    ") as ct\n" +
                    "from t11\n" +
                    "group by region\n" +
                    "having\n" +
                    "count(\n" +
                    "distinct (\n" +
                    "case\n" +
                    "when store_qty > 0.0 then store_id\n" +
                    "else null\n" +
                    "end\n" +
                    ")\n" +
                    ") > 0\n";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv11", "PREDICATES: 10: ct > 0");
        }
    }
}
