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

public class LeaderAwareHistogramMetricTest {
    private LeaderAwareHistogramMetric histogram;

    void createHistogram() {
        histogram = new LeaderAwareHistogramMetric("duration");
    }

    @Test
    public void testConstructorSetsLeaderLabelWhenLeader(@Mocked GlobalStateMgr globalStateMgr) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
            }
        };

        createHistogram();
        Assertions.assertEquals(1, histogram.getLabels().size());
        Assertions.assertEquals("is_leader", histogram.getLabels().get(0).getKey());
        Assertions.assertEquals("true", histogram.getLabels().get(0).getValue());
    }

    @Test
    public void testConstructorSetsLeaderLabelWhenFollower(@Mocked GlobalStateMgr globalStateMgr) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
            }
        };

        createHistogram();
        Assertions.assertEquals(1, histogram.getLabels().size());
        Assertions.assertEquals("is_leader", histogram.getLabels().get(0).getKey());
        Assertions.assertEquals("false", histogram.getLabels().get(0).getValue());
    }

    @Test
    public void testHistogramReportsRealValuesWhenLeader(@Mocked GlobalStateMgr globalStateMgr) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
            }
        };

        createHistogram();
        histogram.update(1);
        histogram.update(5);

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("");
        visitor.visitHistogram(histogram);
        String output = visitor.build();
        Assertions.assertTrue(output.contains("_duration_count{is_leader=\"true\"}"), output);
        Assertions.assertTrue(output.contains("_duration_count{is_leader=\"true\"} 2"), output);
    }

    @Test
    public void testHistogramReportsNoopWhenFollower(@Mocked GlobalStateMgr globalStateMgr) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
            }
        };

        createHistogram();
        histogram.update(10);

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("");
        visitor.visitHistogram(histogram);
        String output = visitor.build();
        Assertions.assertTrue(output.contains("_duration_count{is_leader=\"false\"}"), output);
        Assertions.assertTrue(output.contains("_duration_count{is_leader=\"false\"} 0"), output);
    }

    @Test
    public void testLeaderToFollowerSwitchUpdatesLabelAndGating(@Mocked GlobalStateMgr globalStateMgr) {
        // Phase 1: leader
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
            }
        };
        createHistogram();
        histogram.update(7);

        // Phase 2: switch to follower
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
            }
        };

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("");
        visitor.visitHistogram(histogram);
        String output = visitor.build();
        Assertions.assertTrue(output.contains("_duration_count{is_leader=\"false\"} 0"), output);
    }

    @Test
    public void testFollowerToLeaderSwitchUpdatesLabelAndGating(@Mocked GlobalStateMgr globalStateMgr) {
        // Phase 1: follower
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
            }
        };
        createHistogram();
        histogram.update(3);

        // Phase 2: switch to leader
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
            }
        };

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("");
        visitor.visitHistogram(histogram);
        String output = visitor.build();
        Assertions.assertTrue(output.contains("_duration_count{is_leader=\"true\"} 1"), output);
    }

    @Test
    public void testNonLeaderLabelPreservedOnRoleFlip(@Mocked GlobalStateMgr globalStateMgr) {
        // Start as leader
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
            }
        };
        createHistogram();
        histogram.addLabel(new MetricLabel("job", "load"));

        // Flip to follower
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
            }
        };

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("");
        visitor.visitHistogram(histogram);
        String output = visitor.build();
        Assertions.assertTrue(output.contains("job=\"load\""), output);
        Assertions.assertTrue(output.contains("is_leader=\"false\""), output);
    }

    @Test
    public void testHistogramTagsIncludeLeaderLabelForBothRoles(@Mocked GlobalStateMgr globalStateMgr) {
        // Leader phase
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
            }
        };
        createHistogram();
        Assertions.assertTrue(histogram.getTagName().contains("is_leader=\"true\""));

        // Follower phase
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
            }
        };
        // trigger update
        histogram.getCount();
        Assertions.assertTrue(histogram.getTagName().contains("is_leader=\"false\""));
    }

    @Test
    public void testUpdatesAccumulateAcrossRoleFlipButReportingGated(@Mocked GlobalStateMgr globalStateMgr) {
        // Phase 1: follower, perform updates
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
            }
        };
        createHistogram();
        histogram.update(10);
        histogram.update(20);

        PrometheusMetricVisitor v1 = new PrometheusMetricVisitor("");
        v1.visitHistogram(histogram);
        String out1 = v1.build();
        Assertions.assertTrue(out1.contains("_duration_count{is_leader=\"false\"} 0"), out1);

        // Phase 2: leader, verify accumulated count is reported
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
            }
        };
        PrometheusMetricVisitor v2 = new PrometheusMetricVisitor("");
        v2.visitHistogram(histogram);
        String out2 = v2.build();
        Assertions.assertTrue(out2.contains("_duration_count{is_leader=\"true\"} 2"), out2);
    }
}


