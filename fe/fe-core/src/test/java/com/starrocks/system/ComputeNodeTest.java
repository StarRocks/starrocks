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

package com.starrocks.system;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.conf.Config;
import com.starrocks.qe.CoordinatorMonitor;
import com.starrocks.system.HeartbeatResponse.HbStatus;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ComputeNodeTest {

    @Test
    public void testHbStatusAliveChanged() {

        class TestCase {
            final boolean isReplay;

            final HbStatus responseStatus;
            final HeartbeatResponse.AliveStatus aliveStatus;
            final boolean nodeAlive;

            final boolean expectedNeedSync;
            final boolean expectedNeedNotifyCoordinatorMonitor;
            final boolean expectedAlive;

            public TestCase(boolean isReplay,
                            HbStatus responseStatus, HeartbeatResponse.AliveStatus aliveStatus,
                            boolean nodeAlive,
                            boolean expectedNeedSync, boolean expectedNeedNotifyCoordinatorMonitor,
                            boolean expectedAlive) {
                this.isReplay = isReplay;
                this.responseStatus = responseStatus;
                this.aliveStatus = aliveStatus;
                this.nodeAlive = nodeAlive;
                this.expectedNeedSync = expectedNeedSync;
                this.expectedNeedNotifyCoordinatorMonitor = expectedNeedNotifyCoordinatorMonitor;
                this.expectedAlive = expectedAlive;
            }
        }

        BackendHbResponse hbResponse = new BackendHbResponse();
        ComputeNode node = new ComputeNode();
        node.setBrpcPort(hbResponse.getBrpcPort()); // Don't return needSync by different BrpcPort.
        CoordinatorMonitor coordinatorMonitor = CoordinatorMonitor.getInstance();

        List<TestCase> testCases = ImmutableList.of(
                new TestCase(false, HbStatus.BAD, null, true, true, true, false),
                new TestCase(false, HbStatus.BAD, null, false, true, false, false),
                new TestCase(false, HbStatus.OK, null, true, false, false, true),
                new TestCase(false, HbStatus.OK, null, false, true, false, true),

                new TestCase(true, HbStatus.BAD, null, true, false, true, false),
                new TestCase(true, HbStatus.BAD, null, false, false, false, false),
                new TestCase(true, HbStatus.OK, null, true, false, false, true),
                new TestCase(true, HbStatus.OK, null, false, false, false, true),

                new TestCase(true, HbStatus.BAD, HeartbeatResponse.AliveStatus.NOT_ALIVE, true, false, true, false),
                new TestCase(true, HbStatus.BAD, HeartbeatResponse.AliveStatus.NOT_ALIVE, false, false, false, false),
                new TestCase(true, HbStatus.BAD, HeartbeatResponse.AliveStatus.ALIVE, true, false, false, true),
                new TestCase(true, HbStatus.BAD, HeartbeatResponse.AliveStatus.ALIVE, false, false, false, true),
                new TestCase(true, HbStatus.OK, HeartbeatResponse.AliveStatus.ALIVE, true, false, false, true),
                new TestCase(true, HbStatus.OK, HeartbeatResponse.AliveStatus.ALIVE, false, false, false, true)

        );

        int prevHeartbeatRetryTimes = Config.heartbeat_retry_times;
        try {
            Config.heartbeat_retry_times = 0;

            long nextNodeId = 0L;
            for (TestCase tc : testCases) {
                hbResponse.status = tc.responseStatus;
                hbResponse.aliveStatus = tc.aliveStatus;
                node.setId(nextNodeId++);
                node.setAlive(tc.nodeAlive);
                new Expectations(coordinatorMonitor) {
                    {
                        coordinatorMonitor.addDeadBackend(node.getId());
                        times = tc.expectedNeedNotifyCoordinatorMonitor ? 1 : 0;
                    }
                };
                boolean needSync = node.handleHbResponse(hbResponse, tc.isReplay);
                if (!tc.isReplay) { // Only check needSync for the FE leader.
                    Assert.assertEquals("nodeId: " + node.getId(), tc.expectedNeedSync, needSync);
                }
                Assert.assertEquals("nodeId: " + node.getId(), tc.expectedAlive, node.isAlive());
            }
        } finally {
            Config.heartbeat_retry_times = prevHeartbeatRetryTimes;
        }
    }

    @Test
    public void testUpdateStartTime() {

        BackendHbResponse hbResponse = new BackendHbResponse();
        hbResponse.status = HbStatus.OK;
        hbResponse.setRebootTime(1000L);
        ComputeNode node = new ComputeNode();
        boolean needSync = node.handleHbResponse(hbResponse, false);
        Assert.assertTrue(node.getLastStartTime() == 1000000L);
        Assert.assertTrue(needSync);
    }
}
