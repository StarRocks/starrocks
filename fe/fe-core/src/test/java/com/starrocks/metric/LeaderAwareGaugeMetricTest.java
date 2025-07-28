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

import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LeaderAwareGaugeMetricTest {

    long metricLongValue;
    double metricDoubleValue;

    Metric<Long> metricLong;
    Metric<Double> metricDouble;

    void createMetrics() {
        metricLong = new LeaderAwareGaugeMetricLong("metricLong", Metric.MetricUnit.SECONDS, "metric long") {
            @Override
            public Long getValueLeader() {
                return metricLongValue;
            }
        };

        metricDouble = new LeaderAwareGaugeMetric<>("metricDouble", Metric.MetricUnit.SECONDS, "metric double") {
            @Override
            public Double getValueNonLeader() {
                return -1.5;
            }

            @Override
            public Double getValueLeader() {
                return metricDoubleValue;
            }
        };
    }

    @Test
    public void testLeaderAwareMetricLeaderOutputLeader(@Mocked GlobalStateMgr globalStateMgr) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
            }
        };

        // setup Expectations before creating the metrics
        createMetrics();
        {
            metricLongValue = 3;
            MetricVisitor visitor = new PrometheusMetricVisitor("");
            visitor.visit(metricLong);
            // _metricLong{is_leader="true"} 3
            String output = visitor.build();
            Assertions.assertTrue(output.contains("_metricLong{is_leader=\"true\"} 3"), output);
        }
        {
            metricDoubleValue = -10.3;
            MetricVisitor visitor = new PrometheusMetricVisitor("");
            visitor.visit(metricDouble);
            // _metricDouble{is_leader="true"} -10.3
            String output = visitor.build();
            Assertions.assertTrue(output.contains("_metricDouble{is_leader=\"true\"} -10.3"), output);
        }
    }

    @Test
    public void testLeaderAwareMetricLeaderOutputNonLeader(@Mocked GlobalStateMgr globalStateMgr) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
            }
        };

        // setup Expectations before creating the metrics
        createMetrics();
        {
            metricLongValue = 3;
            MetricVisitor visitor = new PrometheusMetricVisitor("");
            visitor.visit(metricLong);
            // _metricLong{is_leader="false"} 0
            String output = visitor.build();
            Assertions.assertTrue(output.contains("_metricLong{is_leader=\"false\"} 0"), output);
        }
        {
            metricDoubleValue = -10.3;
            MetricVisitor visitor = new PrometheusMetricVisitor("");
            visitor.visit(metricDouble);
            // _metricDouble{is_leader="false"} -1.5
            String output = visitor.build();
            Assertions.assertTrue(output.contains("_metricDouble{is_leader=\"false\"} -1.5"), output);
        }
    }
}
