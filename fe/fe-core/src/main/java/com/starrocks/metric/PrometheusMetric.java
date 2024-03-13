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

import com.google.common.collect.ImmutableMap;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Label;
import io.prometheus.metrics.model.snapshots.MetricMetadata;
import io.prometheus.metrics.model.snapshots.Unit;

import java.util.Map;

/**
 * A PrometheusMetric is a wrapper that wraps a prometheus metric into a com.starrocks.metric.Metric.
 * Then these metrics can be processed by those com.starrocks.metric.MetricVisitor
 */

public class PrometheusMetric extends Metric<Double> {
    // Can init up to 10 k-v pairs
    private static final Map<Unit, MetricUnit> UNIT_MAPPING = ImmutableMap.of(
            Unit.RATIO, MetricUnit.PERCENT,
            Unit.SECONDS, MetricUnit.SECONDS,
            Unit.BYTES, MetricUnit.BYTES,
            Unit.CELSIUS, MetricUnit.NOUNIT,
            Unit.JOULES, MetricUnit.NOUNIT,
            Unit.GRAMS, MetricUnit.NOUNIT,
            Unit.METERS, MetricUnit.NOUNIT,
            Unit.VOLTS, MetricUnit.NOUNIT,
            Unit.AMPERES, MetricUnit.NOUNIT
    );

    private double value = 0;

    private PrometheusMetric(String name, MetricType type, MetricUnit unit, String description) {
        super(name, type, unit, description);
    }

    @Override
    public Double getValue() {
        return value;
    }

    protected void setValue(double val) {
        value = val;
    }

    public static PrometheusMetric create(MetricMetadata meta, DataPointSnapshot snapshot) {
        MetricType type;
        double value = 0;

        // detect type and value from the snapshot class type
        if (snapshot instanceof CounterSnapshot.CounterDataPointSnapshot) {
            type = MetricType.COUNTER;
            value = ((CounterSnapshot.CounterDataPointSnapshot) snapshot).getValue();
        } else if (snapshot instanceof GaugeSnapshot.GaugeDataPointSnapshot) {
            type = MetricType.GAUGE;
            value = ((GaugeSnapshot.GaugeDataPointSnapshot) snapshot).getValue();
        } else {
            throw new RuntimeException(
                    String.format("unknown create metric from prometheus type: %s", snapshot.getClass().getName()));
        }

        MetricUnit unit = MetricUnit.NOUNIT;
        if (meta.hasUnit()) {
            unit = UNIT_MAPPING.get(meta.getUnit());
            if (unit == null) {
                unit = MetricUnit.NOUNIT;
            }
        }

        PrometheusMetric metric = new PrometheusMetric(meta.getName(), type, unit, meta.getHelp());
        for (Label label : snapshot.getLabels()) {
            metric.addLabel(new MetricLabel(label.getName(), label.getValue()));
        }
        metric.setValue(value);
        return metric;
    }
}
