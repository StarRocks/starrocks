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

import com.google.common.collect.Lists;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.system.Frontend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JournalReplayLagMetricTest {

    private static Frontend frontend(FrontendNodeType role, String host, boolean alive, long replayedJournalId) {
        Frontend fe = new Frontend(role, host, host, 9010);
        fe.setAlive(alive);
        fe.setReplayedJournalId(replayedJournalId);
        return fe;
    }

    @Test
    public void testNonLeaderEmitsNothing() {
        List<Frontend> frontends = Lists.newArrayList(
                frontend(FrontendNodeType.FOLLOWER, "10.0.0.2", true, 990));
        List<Metric<Long>> metrics = MetricRepo.buildJournalReplayLagMetrics(false, 1000, frontends);
        Assertions.assertTrue(metrics.isEmpty());
    }

    @Test
    public void testLeaderReportsLagPerAliveFrontend() {
        List<Frontend> frontends = Lists.newArrayList(
                frontend(FrontendNodeType.FOLLOWER, "10.0.0.2", true, 990),
                frontend(FrontendNodeType.OBSERVER, "10.0.0.3", true, 1000),
                // dead frontend: must be skipped even though its replayed id is far behind
                frontend(FrontendNodeType.FOLLOWER, "10.0.0.4", false, 1),
                // follower ahead of the value we read for the leader: lag must clamp to 0
                frontend(FrontendNodeType.FOLLOWER, "10.0.0.5", true, 1005));

        List<Metric<Long>> metrics = MetricRepo.buildJournalReplayLagMetrics(true, 1000, frontends);

        // dead frontend (10.0.0.4) is skipped
        Assertions.assertEquals(3, metrics.size());

        Metric<Long> follower = metrics.get(0);
        Assertions.assertEquals("journal_replay_lag", follower.getName());
        Assertions.assertEquals(Long.valueOf(10), follower.getValue());
        Assertions.assertEquals(1, follower.getLabels().size());
        MetricLabel hostLabel = follower.getLabels().get(0);
        Assertions.assertEquals("host", hostLabel.getKey());
        Assertions.assertEquals("10.0.0.2", hostLabel.getValue());

        // observer caught up with the leader -> lag 0
        Assertions.assertEquals(Long.valueOf(0), metrics.get(1).getValue());
        Assertions.assertEquals("10.0.0.3", metrics.get(1).getLabels().get(0).getValue());

        // follower ahead of leader -> clamped to 0
        Assertions.assertEquals(Long.valueOf(0), metrics.get(2).getValue());
        Assertions.assertEquals("10.0.0.5", metrics.get(2).getLabels().get(0).getValue());
    }

    @Test
    public void testPrometheusOutputFormat() {
        List<Frontend> frontends = Lists.newArrayList(
                frontend(FrontendNodeType.FOLLOWER, "10.0.0.2", true, 990));
        List<Metric<Long>> metrics = MetricRepo.buildJournalReplayLagMetrics(true, 1000, frontends);

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("starrocks_fe");
        for (Metric<Long> metric : metrics) {
            visitor.visit(metric);
        }
        String output = visitor.build();
        Assertions.assertTrue(output.contains("starrocks_fe_journal_replay_lag{host=\"10.0.0.2\"} 10"), output);
        // leader-only metric, so it must not carry an is_leader label
        Assertions.assertFalse(output.contains("is_leader"), output);
    }
}
