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

package com.staros.metrics;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.Collector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Consumer;

/**
 * A metrics repository is a collection of metrics.
 * The Repository manages the metrics registration and un-registration as well as the visiting.
 */
public class MetricsSystem {
    public static final PrometheusRegistry METRIC_REGISTRY = new PrometheusRegistry();

    public static Gauge registerGauge(String name, String helpMsg) {
        return registerGauge(name, helpMsg, null);
    }

    public static Gauge registerGauge(String name, String helpMsg, List<String> labels) {
        Gauge.Builder builder = Gauge.builder().name(name).help(helpMsg);
        if (labels != null && !labels.isEmpty()) {
            builder.labelNames(labels.toArray(new String[0]));
        }
        return builder.register(METRIC_REGISTRY);
    }

    public static GaugeWithCallback registerGaugeCallback(String name, String helpMsg, Consumer<GaugeWithCallback.Callback> cb) {
        GaugeWithCallback.Builder builder = GaugeWithCallback.builder().name(name).help(helpMsg).callback(cb);
        return builder.register(METRIC_REGISTRY);
    }

    public static Counter registerCounter(String name, String helpMsg) {
        return registerCounter(name, helpMsg, null);
    }

    public static Counter registerCounter(String name, String helpMsg, List<String> labels) {
        Counter.Builder builder = Counter.builder().name(name).help(helpMsg);
        if (labels != null && !labels.isEmpty()) {
            builder.labelNames(labels.toArray(new String[0]));
        }
        return builder.register(METRIC_REGISTRY);
    }

    public static Histogram registerHistogram(String name, String helpMsg, List<String> labels) {
        Histogram.Builder builder = Histogram.builder().name(name).help(helpMsg);
        if (labels != null && !labels.isEmpty()) {
            builder.labelNames(labels.toArray(new String[0]));
        }
        return builder.register(METRIC_REGISTRY);
    }

    public static void exportMetricsTextFormat(OutputStream out) throws IOException {
        PrometheusTextFormatWriter writer = new PrometheusTextFormatWriter(false);
        writer.write(out, METRIC_REGISTRY.scrape());
    }

    public static void removeCollector(Collector collector) {
        METRIC_REGISTRY.unregister(collector);
    }
}
