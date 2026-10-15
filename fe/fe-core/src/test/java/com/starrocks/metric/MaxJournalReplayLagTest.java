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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MaxJournalReplayLagTest {

    private static Frontend frontend(FrontendNodeType role, String name, long replayedJournalId, boolean alive) {
        Frontend fe = new Frontend(role, name, "127.0.0.1", 9010);
        Deencapsulation.setField(fe, "replayedJournalId", replayedJournalId);
        fe.setAlive(alive);
        return fe;
    }

    private void mockCluster(GlobalStateMgr globalStateMgr, NodeMgr nodeMgr,
                             long leaderJournalId, List<Frontend> otherFrontends) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getMaxJournalId();
                result = leaderJournalId;
                minTimes = 0;

                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;

                nodeMgr.getOtherFrontends();
                result = otherFrontends;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testReportsSlowestNode(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        mockCluster(globalStateMgr, nodeMgr, 1000L, Lists.newArrayList(
                frontend(FrontendNodeType.FOLLOWER, "fe2", 990L, true),
                frontend(FrontendNodeType.FOLLOWER, "fe3", 700L, true),
                frontend(FrontendNodeType.OBSERVER, "fe4", 999L, true)));

        // fe3 is the slowest: 1000 - 700
        Assertions.assertEquals(300L, MetricRepo.getMaxJournalReplayLag());
    }

    @Test
    public void testObserverCounted(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        mockCluster(globalStateMgr, nodeMgr, 1000L, Lists.newArrayList(
                frontend(FrontendNodeType.FOLLOWER, "fe2", 995L, true),
                frontend(FrontendNodeType.OBSERVER, "fe3", 100L, true)));

        Assertions.assertEquals(900L, MetricRepo.getMaxJournalReplayLag());
    }

    @Test
    public void testDeadNodeSkipped(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        mockCluster(globalStateMgr, nodeMgr, 1000L, Lists.newArrayList(
                frontend(FrontendNodeType.FOLLOWER, "fe2", 980L, true),
                // dead node's replayed id is frozen at its last successful heartbeat, so it must not count
                frontend(FrontendNodeType.FOLLOWER, "fe3", 1L, false)));

        Assertions.assertEquals(20L, MetricRepo.getMaxJournalReplayLag());
    }

    @Test
    public void testAllDeadReportsZero(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        mockCluster(globalStateMgr, nodeMgr, 1000L, Lists.newArrayList(
                frontend(FrontendNodeType.FOLLOWER, "fe2", 1L, false),
                frontend(FrontendNodeType.OBSERVER, "fe3", 2L, false)));

        Assertions.assertEquals(0L, MetricRepo.getMaxJournalReplayLag());
    }

    @Test
    public void testSingleFeCluster(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        mockCluster(globalStateMgr, nodeMgr, 1000L, Lists.newArrayList());

        Assertions.assertEquals(0L, MetricRepo.getMaxJournalReplayLag());
    }

    @Test
    public void testCaughtUpReportsZero(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        mockCluster(globalStateMgr, nodeMgr, 1000L, Lists.newArrayList(
                frontend(FrontendNodeType.FOLLOWER, "fe2", 1000L, true)));

        Assertions.assertEquals(0L, MetricRepo.getMaxJournalReplayLag());
    }

    @Test
    public void testNeverNegative(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        // BDBJEJournal#getMaxJournalId returns -1 when the environment is not open yet, and a
        // follower's heartbeat may momentarily carry an id past the leader's last counted key
        mockCluster(globalStateMgr, nodeMgr, -1L, Lists.newArrayList(
                frontend(FrontendNodeType.FOLLOWER, "fe2", 1000L, true)));

        Assertions.assertEquals(0L, MetricRepo.getMaxJournalReplayLag());
    }

    @Test
    public void testGaugeIsZeroOnNonLeader(@Mocked GlobalStateMgr globalStateMgr, @Mocked NodeMgr nodeMgr) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.isLeader();
                result = false;
                minTimes = 0;
            }
        };

        // same shape as the gauge registered in MetricRepo.init()
        Metric<Long> gauge = new LeaderAwareGaugeMetricLong(
                "max_journal_replay_lag", Metric.MetricUnit.NOUNIT, "test") {
            @Override
            public Long getValueLeader() {
                return MetricRepo.getMaxJournalReplayLag();
            }
        };

        MetricVisitor visitor = new PrometheusMetricVisitor("");
        visitor.visit(gauge);
        String output = visitor.build();
        Assertions.assertTrue(output.contains("_max_journal_replay_lag{is_leader=\"false\"} 0"), output);
    }
}
