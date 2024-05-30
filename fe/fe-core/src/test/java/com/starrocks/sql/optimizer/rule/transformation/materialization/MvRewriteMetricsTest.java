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
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.JsonMetricVisitor;
import com.starrocks.metric.MaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MvRewriteMetricsTest extends MvRewriteTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();

        starRocksAssert.withTable(cluster, "depts");
        starRocksAssert.withTable(cluster, "locations");
        starRocksAssert.withTable(cluster, "dependents");
        starRocksAssert.withTable(cluster, "emps");
        starRocksAssert.withTable(cluster, "emps_par");
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
    }

    @Test
    public void testMvMetrics1() {
        String mvName = "mv0";
        String sql = String.format("CREATE MATERIALIZED VIEW %s" +
                " REFRESH DEFERRED MANUAL " +
                " AS SELECT * FROM depts where deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);

            MaterializedView mv = (MaterializedView) getTable(DB_NAME, mvName);
            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assert.assertTrue(iEntity instanceof MaterializedViewMetricsEntity);
            MaterializedViewMetricsEntity mvMetric = (MaterializedViewMetricsEntity) iEntity;

            // basic test
            Assert.assertTrue(mvMetric.counterPartitionCount.getValue() == 0);
            Assert.assertTrue(mvMetric.counterRefreshJobTotal.getValue() == 1);
            Assert.assertTrue(mvMetric.counterRefreshJobSuccessTotal.getValue() == 1);
            Assert.assertTrue(mvMetric.counterRefreshJobFailedTotal.getValue() == 0);
            Assert.assertTrue(mvMetric.counterRefreshJobEmptyTotal.getValue() == 0);
            Assert.assertTrue(mvMetric.counterRefreshJobRetryCheckChangedTotal.getValue() > 0);

            Assert.assertTrue(mvMetric.counterRefreshPendingJobs.getValue() == 0);
            Assert.assertTrue(mvMetric.counterRefreshRunningJobs.getValue() >= 0);
            Assert.assertTrue(mvMetric.counterInactiveState.getValue() == 0);

            // matched
            {
                String query = "select * from depts where deptno > 20";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
                Assert.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 1);
            }

            // directly query
            {
                String query = "select * from mv0";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
                Assert.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 2);
            }

            // non matched
            {
                String query = "select * from depts where deptno < 20";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertNotContains(plan, mvName);

                Assert.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 2);
                Assert.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 2);
            }

        });

    }

    @Test
    public void testMvMetricsWithValidMvId() {
        String mvName = "mv0";
        String sql = String.format("CREATE MATERIALIZED VIEW %s" +
                " REFRESH DEFERRED MANUAL " +
                " AS SELECT * FROM depts where deptno = 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);
            MaterializedView mv = (MaterializedView) getTable(DB_NAME, mvName);
            IMaterializedViewMetricsEntity iEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
            Assert.assertTrue(iEntity instanceof MaterializedViewMetricsEntity);
            MaterializedViewMetricsEntity mvMetric = (MaterializedViewMetricsEntity) iEntity;
            Assert.assertTrue(mvMetric.counterRefreshJobSuccessTotal.getValue() == 1);
            Assert.assertTrue(mvMetric.counterRefreshJobRetryCheckChangedTotal.getValue() == 1);
            Assert.assertTrue(mvMetric.counterRefreshJobTotal.getValue() == 1);
            {
                // hit mv
                String query = "select * from depts where deptno = 10";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
                Assert.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 1);
                Assert.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 1);
            }

            {
                JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
                MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
                String json = visitor.build();
                System.out.println(json);
                Assert.assertTrue(json.contains("mv_refresh_jobs"));
                Assert.assertTrue(json.contains("mv_refresh_total_success_jobs"));
                Assert.assertTrue(json.contains("mv_refresh_total_retry_meta_count"));
                Assert.assertTrue(json.contains("mv_query_total_count"));
                Assert.assertTrue(json.contains("mv_query_total_hit_count"));
                Assert.assertTrue(json.contains("mv_query_total_considered_count"));
                Assert.assertTrue(json.contains("mv_query_total_matched_count"));
            }
            {
                JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
                MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
                String json = visitor.build();
                System.out.println(json);
                Assert.assertTrue(json.contains("mv_refresh_jobs"));
                Assert.assertTrue(json.contains("mv_refresh_total_success_jobs"));
                Assert.assertTrue(json.contains("mv_refresh_total_retry_meta_count"));
                Assert.assertTrue(json.contains("mv_query_total_count"));
                Assert.assertTrue(json.contains("mv_query_total_hit_count"));
                Assert.assertTrue(json.contains("mv_query_total_considered_count"));
                Assert.assertTrue(json.contains("mv_query_total_matched_count"));
            }

            {
                // hit mv
                String query = "select * from depts where deptno = 10";
                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, mvName);
                Assert.assertTrue(mvMetric.counterQueryHitTotal.getValue() == 2);
                Assert.assertTrue(mvMetric.counterQueryConsideredTotal.getValue() == 2);
                Assert.assertTrue(mvMetric.counterQueryMatchedTotal.getValue() == 2);
                Assert.assertTrue(mvMetric.counterQueryMaterializedViewTotal.getValue() == 2);
            }
            // empty refresh
            refreshMaterializedView(DB_NAME, mvName);
            Assert.assertTrue(mvMetric.counterRefreshJobTotal.getValue() == 2);
            Assert.assertTrue(mvMetric.counterRefreshJobSuccessTotal.getValue() == 1);
            Assert.assertTrue(mvMetric.counterRefreshJobEmptyTotal.getValue() == 1);
            Assert.assertTrue(mvMetric.counterRefreshJobRetryCheckChangedTotal.getValue() == 2);

            {
                JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
                MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
                String json = visitor.build();
                System.out.println(json);
                Assert.assertTrue(json.contains("mv_refresh_jobs"));
                Assert.assertTrue(json.contains("mv_refresh_total_success_jobs"));
                Assert.assertTrue(json.contains("mv_refresh_total_empty_jobs"));
                Assert.assertTrue(json.contains("mv_refresh_total_retry_meta_count"));
                Assert.assertTrue(json.contains("mv_query_total_count"));
                Assert.assertTrue(json.contains("mv_query_total_hit_count"));
                Assert.assertTrue(json.contains("mv_query_total_considered_count"));
                Assert.assertTrue(json.contains("mv_query_total_matched_count"));
            }
        });
    }

    @Test
    public void testMvMetricsWithInvalidMvId1() {
        MvId invalid = new MvId(-1, -1);
        MaterializedViewMetricsRegistry.getInstance().clear();
        MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(invalid);
        JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
        MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
        String json = visitor.build();
        System.out.println(json);
        Assert.assertTrue(json.equals("[]"));
    }

    @Test
    public void testMvMetricsWithInvalidMvId2() {
        MaterializedViewMetricsRegistry.getInstance().clear();
        // mv1: invalid
        {
            MvId invalid = new MvId(-1, -1);
            MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(invalid);
            JsonMetricVisitor visitor = new JsonMetricVisitor("starrocks_fe");
            MaterializedViewMetricsRegistry.collectMaterializedViewMetrics(visitor, true);
            String json = visitor.build();
            System.out.println(json);
            Assert.assertTrue(json.equals("[]"));
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
            Assert.assertTrue(json.contains("mv_refresh_jobs"));
            Assert.assertTrue(json.contains("mv_refresh_total_success_jobs"));
            Assert.assertTrue(json.contains("mv_refresh_total_retry_meta_count"));
        }
    }
}
