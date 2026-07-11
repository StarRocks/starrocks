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

package com.starrocks.alter;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.starrocks.metric.HistogramMetric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.MetricVisitor;

import java.util.Map;

/**
 * Metric writers for ALTER TABLE column operations.
 *
 * <p>Two metrics:
 * <ul>
 *   <li>{@code alter_table_column_op_total} — a labeled counter (the field lives in
 *       {@link MetricRepo#COUNTER_ALTER_TABLE_COLUMN_OP}, mirroring the PreSplitMetrics
 *       pattern of keeping metric fields in MetricRepo).</li>
 *   <li>{@code alter_job_duration_ms} — a labeled histogram. {@link HistogramMetric} is not a
 *       {@code Metric<?>}, so it cannot use {@code MetricWithLabelGroup}; instead we keep one
 *       {@link HistogramMetric} per label value in a private {@link MetricRegistry} and emit them
 *       via {@link #collectDurationHistograms}, mirroring MaterializedViewMetricsRegistry.</li>
 * </ul>
 *
 * <p>All writers are no-ops until {@link MetricRepo#hasInit} is true.
 */
public final class AlterColumnMetrics {

    // No starrocks_fe_ prefix; PrometheusMetricVisitor prepends it.
    private static final String JOB_DURATION_HISTOGRAM_NAME = "alter_job_duration_ms";

    // Own registry so the duration histograms scrape unconditionally via collectDurationHistograms(),
    // instead of sharing a registry gated behind a conditional collect path.
    private static final MetricRegistry DURATION_REGISTRY = new MetricRegistry();

    private AlterColumnMetrics() {
    }

    /** Increment the ALTER column operation counter for {@code opType} ("add"/"drop"/"modify"). */
    public static void recordColumnOp(String opType) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_ALTER_TABLE_COLUMN_OP.getMetric(opType).increase(1L);
        }
    }

    /** Observe an ALTER schema-change job duration (ms) under its {@code type} label ("fse_v1"/"fse_v2"). */
    public static void recordJobDuration(String type, long durationMs) {
        if (MetricRepo.hasInit) {
            getDurationHistogram(type).update(durationMs);
        }
    }

    /** Lazily create/return the duration histogram for a {@code type} label value. */
    public static Histogram getDurationHistogram(String type) {
        HistogramMetric h = new HistogramMetric(JOB_DURATION_HISTOGRAM_NAME);
        h.addLabel(new MetricLabel("type", type));
        return DURATION_REGISTRY.histogram(h.getHistogramName(), () -> h);
    }

    /** Emit all duration histograms to the visitor. Wired into {@code MetricRepo.getMetric}. */
    public static void collectDurationHistograms(MetricVisitor visitor) {
        for (Map.Entry<String, Histogram> e : DURATION_REGISTRY.getHistograms().entrySet()) {
            visitor.visitHistogram(e.getKey(), e.getValue());
        }
    }
}
