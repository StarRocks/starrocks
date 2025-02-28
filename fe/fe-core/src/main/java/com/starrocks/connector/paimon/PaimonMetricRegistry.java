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

package com.starrocks.connector.paimon;

import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricGroupImpl;
import org.apache.paimon.metrics.MetricRegistry;

import java.util.Map;

public class PaimonMetricRegistry extends MetricRegistry {
    private MetricGroup metricGroup;

    @Override
    protected MetricGroup createMetricGroup(String groupName, Map<String, String> variables) {
        MetricGroup metricGroup = new MetricGroupImpl(groupName, variables);
        this.metricGroup = metricGroup;
        return metricGroup;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public Map<String, Metric> getMetrics() {
        return metricGroup.getMetrics();
    }
}
