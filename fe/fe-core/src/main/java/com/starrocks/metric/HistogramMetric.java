// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.metric;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.google.api.client.util.Lists;
import com.google.common.base.Joiner;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Histogram metric with tags to distinguish different metrics with the same name.
 * e.g. mv_refresh_duration{mv_db_name="db1", mv_name="mv1"}
 */
public final class HistogramMetric extends Histogram {
    private final List<MetricLabel> labels = Lists.newArrayList();
    private final String name;

    public HistogramMetric(String name) {
        super(new ExponentiallyDecayingReservoir());
        this.name = name;
    }

    public void addLabel(MetricLabel label) {
        labels.add(label);
    }

    public String getName() {
        return name;
    }

    public String getTagName() {
        List<String> labelStrings = labels.stream().map(l -> l.getKey() + "=\"" + l.getValue()
                + "\"").collect(Collectors.toList());
        return Joiner.on(", ").join(labelStrings);
    }

    public List<MetricLabel> getLabels() {
        return labels;
    }

    /**
     * Get the histogram name with tags in the format of "name_tag1=value1, tag2=value2"
     */
    public String getHistogramName() {
        String tagName = getTagName();
        if (!tagName.isEmpty()) {
            return name + "_" + tagName;
        } else {
            return name;
        }
    }
}
