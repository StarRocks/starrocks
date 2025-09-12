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

import java.util.concurrent.atomic.AtomicLong;

public class LeaderAwareCounterMetricTest {
    private LeaderAwareCounterMetricLong metricLong;

    private AtomicLong metricExternalLongValue;
    private Metric<Long> metricExternalLong;

    void createMetrics() {
        metricLong = new LeaderAwareCounterMetricLong("metricLong", Metric.MetricUnit.SECONDS, "metric long");

        metricExternalLongValue = new AtomicLong(0L);
        metricExternalLong = new LeaderAwareCounterMetricExternalLong(
                "metricExternalLong", Metric.MetricUnit.SECONDS, "metric external long", metricExternalLongValue);
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
            metricLong.increase(10L);
            MetricVisitor visitor = new PrometheusMetricVisitor("");
            visitor.visit(metricLong);
            // _metricLong{is_leader="true"} 10
            String output = visitor.build();
            Assertions.assertTrue(output.contains("_metricLong{is_leader=\"true\"} 10"), output);
        }
        {
            metricExternalLongValue.addAndGet(11L);
            MetricVisitor visitor = new PrometheusMetricVisitor("");
            visitor.visit(metricExternalLong);
            // _metricExternalLong{is_leader="true"} 11
            String output = visitor.build();
            Assertions.assertTrue(output.contains("_metricExternalLong{is_leader=\"true\"} 11"), output);
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
            metricLong.increase(10L);
            MetricVisitor visitor = new PrometheusMetricVisitor("");
            visitor.visit(metricLong);
            // _metricLong{is_leader="false"} 0
            String output = visitor.build();
            Assertions.assertTrue(output.contains("_metricLong{is_leader=\"false\"} 0"), output);
        }
        {
            metricExternalLongValue.addAndGet(11L);
            MetricVisitor visitor = new PrometheusMetricVisitor("");
            visitor.visit(metricExternalLong);
            // _metricExternalLong{is_leader="false"} 0
            String output = visitor.build();
            Assertions.assertTrue(output.contains("_metricExternalLong{is_leader=\"false\"} 0"), output);
        }
    }
}
