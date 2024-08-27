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

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.function.Supplier;

public class MetricWithLabelGroup<E extends Metric<?>> {
    private final String labelKey;
    private final Supplier<E> metricCreator;

    private final Map<String, E> labelValueToMetric = Maps.newConcurrentMap();

    public MetricWithLabelGroup(String labelKey, Supplier<E> metricCreator) {
        this.labelKey = labelKey;
        this.metricCreator = metricCreator;
    }

    public E getMetric(String labelValue) {
        return labelValueToMetric.computeIfAbsent(labelValue, k -> {
            E metric = metricCreator.get();
            metric.addLabel(new MetricLabel(labelKey, labelValue));
            MetricRepo.addMetric(metric);
            return metric;
        });
    }
}
