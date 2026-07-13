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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.metric.LeaderAwareCounterMetricLong;
import com.starrocks.metric.LeaderAwareHistogramMetric;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricVisitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for ALTER TABLE metrics.
 */
public final class AlterMetricRegistry {

    /** Column operation type; the label value tags {@code alter_column_operation_total{type=...}}. */
    public enum AlterColumnOperationType {
        ADD("add"),
        DROP("drop"),
        MODIFY("modify");

        private final String labelValue;

        AlterColumnOperationType(String labelValue) {
            this.labelValue = labelValue;
        }

        public String getLabelValue() {
            return labelValue;
        }
    }

    /** Column-change execution mode; the label value tags {@code alter_column_duration_ms{execution_mode=...}}. */
    public enum AlterColumnExecutionMode {
        // Asynchronous lake schema-change job (LakeTableAsyncFastSchemaChangeJob); shared-data only.
        LEGACY_FAST_SCHEMA_EVOLUTION("legacy_fse"),
        // Synchronous, metadata-only fast schema evolution (shared-nothing, or shared-data FSE v2).
        FAST_SCHEMA_EVOLUTION("fse");

        private final String labelValue;

        AlterColumnExecutionMode(String labelValue) {
            this.labelValue = labelValue;
        }

        public String getLabelValue() {
            return labelValue;
        }
    }

    private static volatile AlterMetricRegistry instance;

    // One counter per column operation type, created on first use and emitted via report().
    private final Map<AlterColumnOperationType, LeaderAwareCounterMetricLong> alterColumnCounters = new ConcurrentHashMap<>();

    // One duration histogram per execution mode, created on first use and emitted via report().
    private final Map<AlterColumnExecutionMode, LeaderAwareHistogramMetric> alterColumnDurationHistograms =
            new ConcurrentHashMap<>();

    private AlterMetricRegistry() {
    }

    public static AlterMetricRegistry getInstance() {
        AlterMetricRegistry inst = instance;
        if (inst == null) {
            synchronized (AlterMetricRegistry.class) {
                if (instance == null) {
                    instance = new AlterMetricRegistry();
                }
                inst = instance;
            }
        }
        return inst;
    }

    /** Increment the ALTER column operation counter for {@code type}. */
    public void updateAlterColumnCounter(AlterColumnOperationType type) {
        alterColumnCounters.computeIfAbsent(type, t -> {
            LeaderAwareCounterMetricLong counter = new LeaderAwareCounterMetricLong("alter_column_operation_total",
                    MetricUnit.OPERATIONS, "Total number of ALTER TABLE column operations, by type.");
            counter.addLabel(new MetricLabel("type", t.getLabelValue()));
            return counter;
        }).increase(1L);
    }

    /** Observe an ALTER column-change duration (ms) for {@code executionMode}. */
    public void updateAlterColumnDuration(AlterColumnExecutionMode executionMode, long durationMs) {
        alterColumnDurationHistograms.computeIfAbsent(executionMode, mode -> {
            LeaderAwareHistogramMetric histogram = new LeaderAwareHistogramMetric("alter_column_duration_ms");
            histogram.addLabel(new MetricLabel("execution_mode", mode.getLabelValue()));
            return histogram;
        }).update(durationMs);
    }

    @VisibleForTesting
    public long getAlterColumnCount(AlterColumnOperationType type) {
        LeaderAwareCounterMetricLong counter = alterColumnCounters.get(type);
        return counter == null ? 0L : counter.getValue();
    }

    @VisibleForTesting
    public long getAlterColumnDurationCount(AlterColumnExecutionMode executionMode) {
        LeaderAwareHistogramMetric histogram = alterColumnDurationHistograms.get(executionMode);
        return histogram == null ? 0L : histogram.getCount();
    }

    /** Emit the counters and duration histograms to the visitor. */
    public void report(MetricVisitor visitor) {
        for (LeaderAwareCounterMetricLong counter : alterColumnCounters.values()) {
            visitor.visit(counter);
        }
        for (LeaderAwareHistogramMetric histogram : alterColumnDurationHistograms.values()) {
            visitor.visitHistogram(histogram);
        }
    }
}
