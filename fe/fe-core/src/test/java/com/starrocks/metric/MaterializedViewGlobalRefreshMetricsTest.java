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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Verifies the global materialized view refresh metrics: mv_global_refresh_{jobs,success_jobs,failed_jobs}_total,
 * mv_global_refresh_duration, and mv_global_refresh_{pending,running}_jobs.
 */
public class MaterializedViewGlobalRefreshMetricsTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "depts");
    }

    private static String warehouseOf(MaterializedView mv) {
        return mv.getWarehouseName() == null ? "" : mv.getWarehouseName();
    }

    @Test
    public void globalRefreshCountersAndDurationByWarehouse() {
        String mvName = "gr_mv";
        String sql = String.format("CREATE MATERIALIZED VIEW %s REFRESH DEFERRED MANUAL " +
                "AS SELECT * FROM depts WHERE deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            MaterializedView mv = (MaterializedView) getTable(DB_NAME, mvName);
            String wh = warehouseOf(mv);

            long jobsBefore = MetricRepo.COUNTER_MV_GLOBAL_REFRESH_JOBS.getMetric(wh).getValue();
            long successBefore = MetricRepo.COUNTER_MV_GLOBAL_REFRESH_SUCCESS_JOBS.getMetric(wh).getValue();
            long durBefore = MaterializedViewMetricsRegistry.getGlobalDurationHistogram(wh).getCount();

            refreshMaterializedView(DB_NAME, mvName);

            Assertions.assertEquals(jobsBefore + 1,
                    MetricRepo.COUNTER_MV_GLOBAL_REFRESH_JOBS.getMetric(wh).getValue(),
                    "a successful refresh must bump the global jobs counter once");
            Assertions.assertEquals(successBefore + 1,
                    MetricRepo.COUNTER_MV_GLOBAL_REFRESH_SUCCESS_JOBS.getMetric(wh).getValue());
            Assertions.assertEquals(durBefore + 1,
                    MaterializedViewMetricsRegistry.getGlobalDurationHistogram(wh).getCount(),
                    "a refresh must record exactly one global duration sample");
        });
    }

    @Test
    public void globalPendingRunningGaugesEmitted() {
        String mvName = "gr_gauge_mv";
        String sql = String.format("CREATE MATERIALIZED VIEW %s REFRESH DEFERRED MANUAL " +
                "AS SELECT * FROM depts WHERE deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            // Refresh so the MV is tracked in the registry and has a refresh task to aggregate over.
            refreshMaterializedView(DB_NAME, mvName);

            RecordingMetricVisitor visitor = new RecordingMetricVisitor();
            MaterializedViewMetricsRegistry.collectGlobalGauges(visitor);

            Assertions.assertTrue(visitor.names.contains("mv_global_refresh_pending_jobs"),
                    "pending gauge must be emitted unconditionally");
            Assertions.assertTrue(visitor.names.contains("mv_global_refresh_running_jobs"),
                    "running gauge must be emitted unconditionally");
        });
    }

    @Test
    public void globalDurationHistogramEmittedUnconditionally() {
        String mvName = "gr_dur_mv";
        String sql = String.format("CREATE MATERIALIZED VIEW %s REFRESH DEFERRED MANUAL " +
                "AS SELECT * FROM depts WHERE deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            refreshMaterializedView(DB_NAME, mvName);

            RecordingMetricVisitor visitor = new RecordingMetricVisitor();
            MaterializedViewMetricsRegistry.collectGlobalDurationHistograms(visitor);

            Assertions.assertTrue(visitor.histogramNames.contains("mv_global_refresh_duration"),
                    "global duration histogram must scrape unconditionally, not behind the per-MV metrics gate");
        });
    }

    @Test
    public void idleWarehouseEmitsZeroRunningGauge() {
        String mvName = "gr_idle_mv";
        String sql = String.format("CREATE MATERIALIZED VIEW %s REFRESH DEFERRED MANUAL " +
                "AS SELECT * FROM depts WHERE deptno > 10", mvName);
        starRocksAssert.withMaterializedView(sql, () -> {
            // No refresh: the task exists but nothing runs, so the running gauge must still report 0, not vanish.
            MaterializedView mv = (MaterializedView) getTable(DB_NAME, mvName);
            RecordingMetricVisitor visitor = new RecordingMetricVisitor();
            MaterializedViewMetricsRegistry.collectGlobalGauges(visitor);
            Assertions.assertEquals(Long.valueOf(0L), visitor.gaugeValue("mv_global_refresh_running_jobs", warehouseOf(mv)),
                    "an idle warehouse must report running=0, not a missing series");
        });
    }

    private static final class RecordingMetricVisitor extends MetricVisitor {
        private final List<String> names = Lists.newArrayList();
        private final List<String> histogramNames = Lists.newArrayList();
        private final List<Metric> visited = Lists.newArrayList();

        private RecordingMetricVisitor() {
            super("");
        }

        @Override
        public void visit(Metric metric) {
            names.add(metric.getName());
            visited.add(metric);
        }

        private Long gaugeValue(String name, String warehouse) {
            for (Metric m : visited) {
                List<MetricLabel> labels = m.getLabels();
                if (name.equals(m.getName()) && labels.stream().anyMatch(
                        l -> "warehouse_name".equals(l.getKey()) && warehouse.equals(l.getValue()))) {
                    return (Long) m.getValue();
                }
            }
            return null;
        }

        @Override
        public void visitJvm(JvmStats jvmStats) {
        }

        @Override
        public void visitHistogram(String name, Histogram histogram) {
            histogramNames.add(histogram instanceof HistogramMetric ? ((HistogramMetric) histogram).getName() : name);
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
