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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.starrocks.load.routineload.RoutineLoadJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class RoutineLoadLatencyMetricMgr {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadLatencyMetricMgr.class);

    private static final String ROUTINE_LOAD_LATENCY = "routine_load_latency";
    private static final ConcurrentHashMap<String, RoutineLoadLatencyMetrics> ROUTINE_LOAD_LATENCY_MAP
            = new ConcurrentHashMap<>();

    private RoutineLoadLatencyMetricMgr() {
        throw new UnsupportedOperationException("can't instantiate this class");
    }

    private static String formatJobName(RoutineLoadJob job) {
        return job.getDbId() + "." + job.getName();
    }

    public static void visitLatency() {
        for (Map.Entry<String, RoutineLoadLatencyMetrics> entry : ROUTINE_LOAD_LATENCY_MAP.entrySet()) {
            RoutineLoadLatencyMetrics metrics = ROUTINE_LOAD_LATENCY_MAP.get(entry.getKey());
            metrics.update();
        }
    }

    public static void updateLatency(RoutineLoadJob job, Long elapseMs) {
        RoutineLoadLatencyMetrics metrics = createRoutineLoadLatencyMetrics(formatJobName(job));
        if (metrics != null) {
            metrics.histogram.update(elapseMs);
        }
    }

    public static Snapshot getLatency(RoutineLoadJob job) {
        RoutineLoadLatencyMetrics metrics = createRoutineLoadLatencyMetrics(formatJobName(job));
        if (metrics != null) {
            return metrics.histogram.getSnapshot();
        }
        return null;
    }

    private static RoutineLoadLatencyMetrics createRoutineLoadLatencyMetrics(String jobName) {
        ROUTINE_LOAD_LATENCY_MAP.computeIfAbsent(jobName, ROUTINE_LOAD_LATENCY_METRICS_FUNCTION);
        return ROUTINE_LOAD_LATENCY_MAP.get(jobName);
    }

    private static final Function<String, RoutineLoadLatencyMetrics> ROUTINE_LOAD_LATENCY_METRICS_FUNCTION =
            routineLoadJobName -> new RoutineLoadLatencyMetrics(ROUTINE_LOAD_LATENCY, routineLoadJobName);

    private static final class RoutineLoadLatencyMetrics {
        private static final String[] ROUTINE_LOAD_LATENCY_LABEL =
                {"median", "95_quantile", "99_quantile", "max"};

        private final MetricRegistry metricRegistry;
        private Histogram histogram;
        private final List<GaugeMetricImpl<Double>> metricsList;
        private final String metricsName;

        private RoutineLoadLatencyMetrics(String metricsName, String routineLoadJobName) {
            this.metricsName = metricsName;
            this.metricRegistry = new MetricRegistry();
            initHistogram(metricsName);
            this.metricsList = new ArrayList<>();
            for (String label : ROUTINE_LOAD_LATENCY_LABEL) {
                GaugeMetricImpl<Double> metrics =
                        new GaugeMetricImpl<>(metricsName, Metric.MetricUnit.MILLISECONDS,
                                label + " of routine load latency");
                metrics.addLabel(new MetricLabel("type", label));
                metrics.addLabel(new MetricLabel("name", routineLoadJobName));
                metrics.setValue(0.0);
                MetricRepo.addMetric(metrics);
                LOG.info("Add {} metric, routine load job name is {}", ROUTINE_LOAD_LATENCY, routineLoadJobName);
                this.metricsList.add(metrics);
            }
        }

        private void initHistogram(String metricsName) {
            this.histogram = this.metricRegistry.histogram(metricsName);
        }

        private void update() {
            Histogram oldHistogram = this.histogram;
            this.metricRegistry.remove(this.metricsName);
            initHistogram(metricsName);

            Snapshot snapshot = oldHistogram.getSnapshot();
            metricsList.get(0).setValue(snapshot.getMedian());
            metricsList.get(1).setValue(snapshot.get95thPercentile());
            metricsList.get(2).setValue(snapshot.get99thPercentile());
            metricsList.get(3).setValue((double) snapshot.getMax());
        }
    }
}