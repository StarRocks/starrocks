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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class MetricWithLabelGroup<E extends Metric<?>> {
    /** Separates the label values in the map key; not a legal character in a label value. */
    private static final char KEY_SEPARATOR = '\u0000';

    private final String labelKey;
    private final String secondLabelKey;
    private final Supplier<E> metricCreator;

    private final Map<String, E> labelValueToMetric = Maps.newConcurrentMap();

    public MetricWithLabelGroup(String labelKey, Supplier<E> metricCreator) {
        this(labelKey, null, metricCreator);
    }

    /**
     * Two-label variant, for a metric whose values are naturally indexed by an independent pair -
     * so each dimension stays aggregatable on its own instead of being flattened into one label
     * value that queries then have to pattern-match apart.
     */
    public MetricWithLabelGroup(String labelKey, String secondLabelKey, Supplier<E> metricCreator) {
        this.labelKey = labelKey;
        this.secondLabelKey = secondLabelKey;
        this.metricCreator = metricCreator;
    }

    public Map<String, E> getMetrics() {
        return Collections.unmodifiableMap(labelValueToMetric);
    }

    public E getMetric(String labelValue) {
        Preconditions.checkState(secondLabelKey == null,
                "metric declares two labels (%s, %s) but only one value was given", labelKey, secondLabelKey);
        return labelValueToMetric.computeIfAbsent(labelValue, k -> {
            E metric = metricCreator.get();
            metric.addLabel(new MetricLabel(labelKey, labelValue));
            MetricRepo.addMetric(metric);
            return metric;
        });
    }

    public E getMetric(String labelValue, String secondLabelValue) {
        Preconditions.checkState(secondLabelKey != null,
                "metric declares one label (%s) but two values were given", labelKey);
        return labelValueToMetric.computeIfAbsent(labelValue + KEY_SEPARATOR + secondLabelValue, k -> {
            E metric = metricCreator.get();
            metric.addLabel(new MetricLabel(labelKey, labelValue));
            metric.addLabel(new MetricLabel(secondLabelKey, secondLabelValue));
            MetricRepo.addMetric(metric);
            return metric;
        });
    }
}
