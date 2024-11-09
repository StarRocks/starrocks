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
import com.starrocks.common.Config;
import com.starrocks.qe.CoordinatorMonitor;
import com.starrocks.system.HeartbeatResponse.HbStatus;
import com.starrocks.thrift.TStatusCode;
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

    BackendHbResponse generateHbResponse(ComputeNode node, TStatusCode tStatusCode, long rebootTimeSecs) {
        if (tStatusCode != TStatusCode.OK) {
            return new BackendHbResponse(node.getId(), "Unknown Error");
        }
        BackendHbResponse hbResponse =
                new BackendHbResponse(node.getId(), node.getBePort(), node.getHttpPort(), node.getBrpcPort(),
                        node.getStarletPort(), System.currentTimeMillis(), node.getVersion(), node.getCpuCores(), 0);
        hbResponse.setRebootTime(rebootTimeSecs);
        return hbResponse;
    }

    @Test
    public void testComputeNodeLastStartTimeUpdate() {
        int previousRetryTimes = Config.heartbeat_retry_times;
        // no retry, set to not_alive for the first hb failure
        Config.heartbeat_retry_times = 0;

        long nodeId = 1000;
        ComputeNode node = new ComputeNode(nodeId, "127.0.0.1", 9050);
        long rebootTimeA = System.currentTimeMillis() / 1000 - 60;
        BackendHbResponse hbResponse;

        hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTimeA);
        Assert.assertTrue(node.handleHbResponse(hbResponse, false));
        Assert.assertEquals(rebootTimeA * 1000, node.getLastStartTime());
        Assert.assertTrue(node.isAlive());

        // simulate that a few intermittent heartbeat probing failures due to high load or unstable network.
        // FE marks the BE as dead and then the node is back alive
        hbResponse = generateHbResponse(node, TStatusCode.THRIFT_RPC_ERROR, 0);
        Assert.assertTrue(node.handleHbResponse(hbResponse, false));
        Assert.assertFalse(node.isAlive());
        // BE reports heartbeat with the same rebootTime
        hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTimeA);
        Assert.assertTrue(node.handleHbResponse(hbResponse, false));
        Assert.assertTrue(node.isAlive());
        // reboot time should not change
        Assert.assertEquals(rebootTimeA * 1000, node.getLastStartTime());

        // response an OK status, but the rebootTime changed.
        // This simulates the case that the BE is restarted quickly in between the two heartbeat probes.
        long rebootTimeB = System.currentTimeMillis() / 1000;
        hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTimeB);
        Assert.assertTrue(node.handleHbResponse(hbResponse, false));
        Assert.assertTrue(node.isAlive());
        // reboot time changed
        Assert.assertEquals(rebootTimeB * 1000, node.getLastStartTime());

        Config.heartbeat_retry_times = previousRetryTimes;
    }
}
