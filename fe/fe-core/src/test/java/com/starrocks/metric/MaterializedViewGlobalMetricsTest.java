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

package com.starrocks.metric;

import com.codahale.metrics.Histogram;
import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertContains;

/**
 * Verifies the global materialized view metrics: mv_global_count, mv_global_query_mv_usage_total,
 * and mv_global_query_rewrite_queries_total.
 */
public class MaterializedViewGlobalMetricsTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "depts");
    }

    private static String refreshMode(MaterializedView mv) {
        return mv.getRefreshMode() == null ? "UNKNOWN" : mv.getRefreshMode().name();
    }

    @Test
    public void globalMvCountEmittedByModeAndStatus() {
        String mvName = "pm_cnt_mv";
        String sql = String.format("CREATE MATERIALIZED VIEW %s REFRESH DEFERRED MANUAL " +
                "AS SELECT * FROM depts where deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            MaterializedView mv = (MaterializedView) getTable(DB_NAME, mvName);
            String mode = refreshMode(mv);

            RecordingMetricVisitor visitor = new RecordingMetricVisitor();
            MaterializedViewMetricsRegistry.collectGlobalMvCount(visitor);

            Assertions.assertTrue(visitor.hasSeries("mv_global_count", "refresh_mode", mode, "status", "ACTIVE"),
                    "expected mv_global_count{refresh_mode=" + mode + ", status=ACTIVE}");
        });
    }

    @Test
    public void mvUsageRewriteAndDirectCounted() {
        String mvName = "pm_usage_mv";
        String sql = String.format("CREATE MATERIALIZED VIEW %s REFRESH DEFERRED MANUAL " +
                "AS SELECT * FROM depts where deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);
            String mode = refreshMode((MaterializedView) getTable(DB_NAME, mvName));

            long rewriteBefore = MaterializedViewMetricsRegistry.getMvUsageCount("REWRITE", mode);
            assertContains(getFragmentPlan("select * from depts where deptno > 20"), mvName);
            Assertions.assertEquals(rewriteBefore + 1,
                    MaterializedViewMetricsRegistry.getMvUsageCount("REWRITE", mode));

            long directBefore = MaterializedViewMetricsRegistry.getMvUsageCount("DIRECT", mode);
            assertContains(getFragmentPlan("select * from " + mvName), mvName);
            Assertions.assertEquals(directBefore + 1,
                    MaterializedViewMetricsRegistry.getMvUsageCount("DIRECT", mode));
        });
    }

    @Test
    public void queryRewriteOutcomeCounted() {
        String mvName = "pm_hit_mv";
        String sql = String.format("CREATE MATERIALIZED VIEW %s REFRESH DEFERRED MANUAL " +
                "AS SELECT * FROM depts where deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);

            long hitBefore = MetricRepo.COUNTER_MV_GLOBAL_QUERY_REWRITE.getMetric("HIT").getValue();
            assertContains(getFragmentPlan("select * from depts where deptno > 20"), mvName);
            long hitAfter = MetricRepo.COUNTER_MV_GLOBAL_QUERY_REWRITE.getMetric("HIT").getValue();
            Assertions.assertEquals(hitBefore + 1, hitAfter);

            long missBefore = MetricRepo.COUNTER_MV_GLOBAL_QUERY_REWRITE.getMetric("NO_HIT").getValue();
            PlanTestBase.assertNotContains(getFragmentPlan("select * from depts where deptno < 20"), mvName);
            long missAfter = MetricRepo.COUNTER_MV_GLOBAL_QUERY_REWRITE.getMetric("NO_HIT").getValue();
            Assertions.assertEquals(missBefore + 1, missAfter);
        });
    }

    @Test
    public void queryRewriteDisabledCounted() throws Exception {
        boolean original = connectContext.getSessionVariable().isEnableMaterializedViewRewrite();
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        try {
            long disabledBefore = MetricRepo.COUNTER_MV_GLOBAL_QUERY_REWRITE.getMetric("DISABLED").getValue();
            getFragmentPlan("select * from depts where deptno > 20");
            long disabledAfter = MetricRepo.COUNTER_MV_GLOBAL_QUERY_REWRITE.getMetric("DISABLED").getValue();
            Assertions.assertEquals(disabledBefore + 1, disabledAfter);
        } finally {
            connectContext.getSessionVariable().setEnableMaterializedViewRewrite(original);
        }
    }

    @Test
    public void directMvUsageCountedWhenRewriteDisabled() {
        String mvName = "pm_direct_mv";
        String sql = String.format("CREATE MATERIALIZED VIEW %s REFRESH DEFERRED MANUAL " +
                "AS SELECT * FROM depts where deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);
            String mode = refreshMode((MaterializedView) getTable(DB_NAME, mvName));

            boolean original = connectContext.getSessionVariable().isEnableMaterializedViewRewrite();
            connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
            try {
                long directBefore = MaterializedViewMetricsRegistry.getMvUsageCount("DIRECT", mode);
                assertContains(getFragmentPlan("select * from " + mvName), mvName);
                Assertions.assertEquals(directBefore + 1,
                        MaterializedViewMetricsRegistry.getMvUsageCount("DIRECT", mode),
                        "a direct MV read must count as DIRECT usage even when rewrite is disabled");
            } finally {
                connectContext.getSessionVariable().setEnableMaterializedViewRewrite(original);
            }
        });
    }

    @Test
    public void rewriteModeDisableCountedAsDisabled() throws Exception {
        String originalMode = connectContext.getSessionVariable().getMaterializedViewRewriteMode();
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("disable");
        try {
            long disabledBefore = MetricRepo.COUNTER_MV_GLOBAL_QUERY_REWRITE.getMetric("DISABLED").getValue();
            getFragmentPlan("select * from depts where deptno > 20");
            long disabledAfter = MetricRepo.COUNTER_MV_GLOBAL_QUERY_REWRITE.getMetric("DISABLED").getValue();
            Assertions.assertEquals(disabledBefore + 1, disabledAfter,
                    "materialized_view_rewrite_mode=disable must count as DISABLED, not NO_HIT");
        } finally {
            connectContext.getSessionVariable().setMaterializedViewRewriteMode(originalMode);
        }
    }

    private static final class RecordingMetricVisitor extends MetricVisitor {
        private final List<Metric> visited = Lists.newArrayList();

        private RecordingMetricVisitor() {
            super("");
        }

        @Override
        public void visit(Metric metric) {
            visited.add(metric);
        }

        private boolean hasSeries(String name, String k1, String v1, String k2, String v2) {
            return visited.stream().anyMatch(m -> name.equals(m.getName())
                    && hasLabel(m, k1, v1) && hasLabel(m, k2, v2));
        }

        private static boolean hasLabel(Metric metric, String key, String value) {
            List<MetricLabel> labels = metric.getLabels();
            return labels.stream().anyMatch(l -> key.equals(l.getKey()) && value.equals(l.getValue()));
        }

        @Override
        public void visitJvm(JvmStats jvmStats) {
        }

        @Override
        public void visitHistogram(String name, Histogram histogram) {
        }

        @Override
        public void visitHistogram(HistogramMetric histogram) {
        }

        @Override
        public void getNodeInfo() {
        }

        @Override
        public String build() {
            return "";
        }
    }
}
