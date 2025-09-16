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
import com.starrocks.catalog.MvId;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Tracers;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.JsonMetricVisitor;
import com.starrocks.metric.MaterializedViewMetricsBlackHoleEntity;
import com.starrocks.metric.MaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertContains;

@TestMethodOrder(MethodName.class)
public class MvRewriteMetricsTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        starRocksAssert.withTable(cluster, "depts");
        starRocksAssert.withTable(cluster, "locations");
        starRocksAssert.withTable(cluster, "dependents");
        starRocksAssert.withTable(cluster, "emps");
        starRocksAssert.withTable(cluster, "emps_par");
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
    }

    @Test
    public void testMvMetricsWithRuleBasedRewrite() {
        String mvName = "mv0";
        String sql = String.format("CREATE MATERIALIZED VIEW %s" +
                " REFRESH DEFERRED MANUAL " +
                " AS SELECT * FROM depts where deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);

            MaterializedView mv = (MaterializedView) getTable(DB_NAME, mvName);
            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertTrue(iEntity instanceof MaterializedViewMetricsEntity);
            MaterializedViewMetricsEntity mvMetric = (MaterializedViewMetricsEntity) iEntity;

            // basic test
            Assertions.assertTrue(mvMetric.counterPartitionCount.getValue() == 0);
            Assertions.assertTrue(mvMetric.counterRefreshJobTotal.getValue() == 1);
            Assertions.assertTrue(mvMetric.counterRefreshJobSuccessTotal.getValue() == 1);
            Assertions.assertTrue(mvMetric.counterRefreshJobFailedTotal.getValue() == 0);
            Assertions.assertTrue(mvMetric.counterRefreshJobEmptyTotal.getValue() == 0);
            Assertions.assertTrue(mvMetric.counterRefreshJobRetryCheckChangedTotal.getValue() > 0);

            Assertions.assertTrue(mvMetric.counterRefreshPendingJobs.getValue() == 0);
            Assertions.assertTrue(mvMetric.counterRefreshRunningJobs.getValue() >= 0);
            Assertions.assertTrue(mvMetric.counterInactiveState.getValue() == 0);
            // matched
            {
                String query = "select * from depts where deptno > 20";
                String plan = getFragmentPlan(query);
                assertContains(plan, mvName);

                Assertions.assertTrue(mvMetric.counterQueryTextBasedMatchedTotal.getValue() == 0);
                Assertions.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 1);
            }

            // directly query
            {
                String query = "select * from mv0";
                String plan = getFragmentPlan(query);
                assertContains(plan, mvName);
                Assertions.assertTrue(mvMetric.counterQueryTextBasedMatchedTotal.getValue() == 0);
                Assertions.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 2);
            }

            // non matched
            {
                String query = "select * from depts where deptno < 20";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertNotContains(plan, mvName);
                Assertions.assertTrue(mvMetric.counterQueryTextBasedMatchedTotal.getValue() == 0);
                Assertions.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 2);
                Assertions.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 2);
            }

        });
    }

    @Test
    public void testMvMetricsWithTextBasedRewrite() {
        String mvName = "mv0";
        String sql = String.format("CREATE MATERIALIZED VIEW %s" +
                " REFRESH DEFERRED MANUAL " +
                " AS SELECT * FROM depts where deptno > 10", mvName);
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(true);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);

            MaterializedView mv = (MaterializedView) getTable(DB_NAME, mvName);
            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertTrue(iEntity instanceof MaterializedViewMetricsEntity);
            MaterializedViewMetricsEntity mvMetric = (MaterializedViewMetricsEntity) iEntity;

            // basic test
            Assertions.assertTrue(mvMetric.counterPartitionCount.getValue() == 0);
            Assertions.assertTrue(mvMetric.counterRefreshJobTotal.getValue() == 1);
            Assertions.assertTrue(mvMetric.counterRefreshJobSuccessTotal.getValue() == 1);
            Assertions.assertTrue(mvMetric.counterRefreshJobFailedTotal.getValue() == 0);
            Assertions.assertTrue(mvMetric.counterRefreshJobEmptyTotal.getValue() == 0);
            Assertions.assertTrue(mvMetric.counterRefreshJobRetryCheckChangedTotal.getValue() > 0);

            Assertions.assertTrue(mvMetric.counterRefreshPendingJobs.getValue() == 0);
            Assertions.assertTrue(mvMetric.counterRefreshRunningJobs.getValue() >= 0);
            Assertions.assertTrue(mvMetric.counterInactiveState.getValue() == 0);

            connectContext.getSessionVariable().setTraceLogMode("command");
            Tracers.register(connectContext);
            Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
            // matched
            {
                String query = "select * from depts where deptno > 10";
                String plan = getFragmentPlan(query);
                assertContains(plan, mvName);

                String pr = Tracers.printLogs();
                Tracers.close();
                assertContains(pr, "TEXT_BASED_REWRITE: Rewrite Succeed");

                Assertions.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryTextBasedMatchedTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 0);
                Assertions.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 1);
            }

        });
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(false);
    }

    @Test
    public void testMvMetricsWithDisableMVMetrics() {
        String mvName = "mv0";
        String sql = String.format("CREATE MATERIALIZED VIEW %s" +
                " REFRESH DEFERRED MANUAL " +
                " AS SELECT * FROM depts where deptno != 10", mvName);

        Config.enable_materialized_view_metrics_collect = false;
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);

            MaterializedView mv = (MaterializedView) getTable(DB_NAME, mvName);
            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertTrue(iEntity instanceof MaterializedViewMetricsBlackHoleEntity);
            MaterializedViewMetricsBlackHoleEntity mvMetric = (MaterializedViewMetricsBlackHoleEntity) iEntity;
        });
        Config.enable_materialized_view_metrics_collect = true;
    }

    @Test
    public void testMvMetricsWithValidMvId() {
        disableMVRewriteConsiderDataLayout();
        String mvName = "mv0";
        String sql = String.format("CREATE MATERIALIZED VIEW %s" +
                " REFRESH DEFERRED MANUAL " +
                " AS SELECT * FROM depts where deptno = 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);
            MaterializedView mv = (MaterializedView) getTable(DB_NAME, mvName);
            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assertions.assertTrue(iEntity instanceof MaterializedViewMetricsEntity);
            MaterializedViewMetricsEntity mvMetric = (MaterializedViewMetricsEntity) iEntity;
            Assertions.assertTrue(mvMetric.counterRefreshJobSuccessTotal.getValue() == 1);
            Assertions.assertTrue(mvMetric.counterRefreshJobRetryCheckChangedTotal.getValue() == 1);
            Assertions.assertTrue(mvMetric.counterRefreshJobTotal.getValue() == 1);
            {
                // hit mv
                String query = "select * from depts where deptno = 10";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
                Assertions.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 1);
                Assertions.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 1);
            }

            {
                JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
                MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
                String json = visitor.build();
                System.out.println(json);
                Assertions.assertTrue(json.contains("mv_refresh_jobs"));
                Assertions.assertTrue(json.contains("mv_refresh_total_success_jobs"));
                Assertions.assertTrue(json.contains("mv_refresh_total_retry_meta_count"));
                Assertions.assertTrue(json.contains("mv_query_total_count"));
                Assertions.assertTrue(json.contains("mv_query_total_hit_count"));
                Assertions.assertTrue(json.contains("mv_query_total_considered_count"));
                Assertions.assertTrue(json.contains("mv_query_total_matched_count"));
            }
            {
                JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
                MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
                String json = visitor.build();
                System.out.println(json);
                Assertions.assertTrue(json.contains("mv_refresh_jobs"));
                Assertions.assertTrue(json.contains("mv_refresh_total_success_jobs"));
                Assertions.assertTrue(json.contains("mv_refresh_total_retry_meta_count"));
                Assertions.assertTrue(json.contains("mv_query_total_count"));
                Assertions.assertTrue(json.contains("mv_query_total_hit_count"));
                Assertions.assertTrue(json.contains("mv_query_total_considered_count"));
                Assertions.assertTrue(json.contains("mv_query_total_matched_count"));
            }

            {
                // hit mv
                String query = "select * from depts where deptno = 10";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
                Assertions.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 2);
                Assertions.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 2);
                Assertions.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 2);
                Assertions.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 2);
            }
            // empty refresh
            refreshMaterializedView(DB_NAME, mvName);
            Assertions.assertTrue(mvMetric.counterRefreshJobTotal.getValue() == 2);
            Assertions.assertTrue(mvMetric.counterRefreshJobSuccessTotal.getValue() == 1);
            Assertions.assertTrue(mvMetric.counterRefreshJobEmptyTotal.getValue() == 1);
            Assertions.assertTrue(mvMetric.counterRefreshJobRetryCheckChangedTotal.getValue() == 2);

            {
                JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
                MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
                String json = visitor.build();
                System.out.println(json);
                Assertions.assertTrue(json.contains("mv_refresh_jobs"));
                Assertions.assertTrue(json.contains("mv_refresh_total_success_jobs"));
                Assertions.assertTrue(json.contains("mv_refresh_total_empty_jobs"));
                Assertions.assertTrue(json.contains("mv_refresh_total_retry_meta_count"));
                Assertions.assertTrue(json.contains("mv_query_total_count"));
                Assertions.assertTrue(json.contains("mv_query_total_hit_count"));
                Assertions.assertTrue(json.contains("mv_query_total_considered_count"));
                Assertions.assertTrue(json.contains("mv_query_total_matched_count"));
            }
        });
        enableMVRewriteConsiderDataLayout();
    }

    @Test
    public void testMvMetricsWithInvalidMvId1() {
        MvId invalid = new MvId(-1, -1);
        MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(invalid);
        JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
        MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
        String json = visitor.build();
        System.out.println(json);
        Assertions.assertTrue(json.equals("[]"));
    }

    @Test
    public void testMvMetricsWithInvalidMvId2() {
        // mv1: invalid
        {
            MvId invalid = new MvId(-1, -1);
            MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(invalid);
            JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
            MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
            String json = visitor.build();
            System.out.println(json);
            Assertions.assertTrue(json.equals("[]"));
        }

        // mv2: valid
        String mvName = "mv0";
        String sql = String.format("CREATE MATERIALIZED VIEW %s" +
                " REFRESH DEFERRED MANUAL " +
                " AS SELECT * FROM depts where deptno = 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);
        });

        // visit after drop mv
        {
            JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
            MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
            String json = visitor.build();
            System.out.println(json);
            Assertions.assertEquals("[]", json);
        }
    }
}
