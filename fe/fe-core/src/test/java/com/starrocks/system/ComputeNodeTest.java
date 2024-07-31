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
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.server.GlobalStateMgr;
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
                new TestCase(true, HbStatus.OK, HeartbeatResponse.AliveStatus.ALIVE, true, false, false, true),
                new TestCase(true, HbStatus.OK, HeartbeatResponse.AliveStatus.ALIVE, false, false, false, true)

        );

        int prevHeartbeatRetryTimes = Config.heartbeat_retry_times;
        try {
            Config.heartbeat_retry_times = 0;

            long nextNodeId = 0L;
            for (TestCase tc : testCases) {
                hbResponse.status = tc.responseStatus;
                hbResponse.setStatusCode(
                        tc.responseStatus == HbStatus.OK ? TStatusCode.OK : TStatusCode.INTERNAL_ERROR);
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

    @Test
    public void testShutdownStatus() {
        ComputeNode node = new ComputeNode();
        ComputeNode nodeInFollower = new ComputeNode();
        long hbTimestamp = System.currentTimeMillis();
        BackendHbResponse hbResponse =
                new BackendHbResponse(node.getId(), node.getBePort(), node.getHttpPort(), node.getBrpcPort(),
                        node.getStarletPort(), hbTimestamp, node.getVersion(), node.getCpuCores(), 0);

        node.handleHbResponse(hbResponse, false);
        { // regular HbResponse
            Assert.assertFalse(node.handleHbResponse(hbResponse, false));
            Assert.assertTrue(node.isAlive());
            Assert.assertEquals(ComputeNode.Status.OK, node.getStatus());
        }
        { // first shutdown HbResponse
            BackendHbResponse shutdownResponse =
                    new BackendHbResponse(node.getId(), TStatusCode.SHUTDOWN, "BE is in shutting down");

            Assert.assertTrue(node.handleHbResponse(shutdownResponse, false));
            Assert.assertFalse(node.isAlive());
            Assert.assertEquals(ComputeNode.Status.SHUTDOWN, node.getStatus());
            Assert.assertEquals(shutdownResponse.getHbTime(), node.getLastUpdateMs());
        }
        { // second shutdown HbResponse
            BackendHbResponse shutdownResponse =
                    new BackendHbResponse(node.getId(), TStatusCode.SHUTDOWN, "BE is in shutting down");

            Assert.assertTrue(node.handleHbResponse(shutdownResponse, false));
            Assert.assertTrue(nodeInFollower.handleHbResponse(shutdownResponse, true));
            Assert.assertFalse(node.isAlive());
            Assert.assertEquals(ComputeNode.Status.SHUTDOWN, node.getStatus());
            Assert.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
            Assert.assertEquals(shutdownResponse.getHbTime(), node.getLastUpdateMs());
            Assert.assertEquals(shutdownResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
        }
        long lastUpdateTime = node.getLastUpdateMs();
        for (int i = 0; i <= Config.heartbeat_retry_times + 2; ++i) {
            BackendHbResponse errorResponse =
                    new BackendHbResponse(node.getId(), TStatusCode.INTERNAL_ERROR, "Internal Error");

            Assert.assertTrue(node.handleHbResponse(errorResponse, false));
            Assert.assertTrue(nodeInFollower.handleHbResponse(errorResponse, true));
            Assert.assertFalse(node.isAlive());
            // lasUpdateTime will not be updated
            Assert.assertEquals(lastUpdateTime, node.getLastUpdateMs());

            // start from the (heartbeat_retry_times-1)th response (started from 0), the status changed to disconnected.
            if (i >= Config.heartbeat_retry_times - 1) {
                Assert.assertEquals(String.format("i=%d", i), ComputeNode.Status.DISCONNECTED, node.getStatus());
                Assert.assertEquals(String.format("i=%d", i), ComputeNode.Status.DISCONNECTED, nodeInFollower.getStatus());
            } else {
                Assert.assertEquals(String.format("i=%d", i), ComputeNode.Status.SHUTDOWN, node.getStatus());
                Assert.assertEquals(String.format("i=%d", i), ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
            }
        }
    }

    @Test
    public void testShutdownCancelQuery() {
        CoordinatorMonitor coordinatorMonitor = CoordinatorMonitor.getInstance();
        ResourceUsageMonitor resourceUsageMonitor = GlobalStateMgr.getCurrentState().getResourceUsageMonitor();

        int oldHeartbeatRetry = Config.heartbeat_retry_times;
        Config.heartbeat_retry_times = 2;

        ComputeNode nodeInLeader = new ComputeNode();
        ComputeNode nodeInFollower = new ComputeNode();
        long hbTimestamp = System.currentTimeMillis();
        BackendHbResponse hbResponse =
                new BackendHbResponse(nodeInLeader.getId(), nodeInLeader.getBePort(), nodeInLeader.getHttpPort(),
                        nodeInLeader.getBrpcPort(),
                        nodeInLeader.getStarletPort(), hbTimestamp, nodeInLeader.getVersion(),
                        nodeInLeader.getCpuCores(), 0);

        try {
            // first OK hbResponse for both leader and follower
            nodeInLeader.handleHbResponse(hbResponse, false);
            nodeInFollower.handleHbResponse(hbResponse, true);

            // second OK hbResponse for both leader and follower
            {
                Assert.assertFalse(nodeInLeader.handleHbResponse(hbResponse, false));
                Assert.assertTrue(nodeInLeader.isAlive());
                Assert.assertEquals(ComputeNode.Status.OK, nodeInLeader.getStatus());

                Assert.assertFalse(nodeInFollower.handleHbResponse(hbResponse, true));
                Assert.assertTrue(nodeInFollower.isAlive());
                Assert.assertEquals(ComputeNode.Status.OK, nodeInFollower.getStatus());
            }

            // second OK hbResponse for both leader and follower
            {
                Assert.assertFalse(nodeInLeader.handleHbResponse(hbResponse, false));
                Assert.assertTrue(nodeInLeader.isAlive());
                Assert.assertEquals(ComputeNode.Status.OK, nodeInLeader.getStatus());

                Assert.assertFalse(nodeInFollower.handleHbResponse(hbResponse, true));
                Assert.assertTrue(nodeInFollower.isAlive());
                Assert.assertEquals(ComputeNode.Status.OK, nodeInFollower.getStatus());
            }

            // first shutdown hbResponse
            {
                BackendHbResponse shutdownResponse =
                        new BackendHbResponse(nodeInLeader.getId(), TStatusCode.SHUTDOWN, "BE is in shutting down");
                new Expectations(resourceUsageMonitor) {
                    {
                        resourceUsageMonitor.notifyBackendDead();
                        times = 2; // one for leader and one for the follower
                    }
                };
                new Expectations(coordinatorMonitor) {
                    {
                        coordinatorMonitor.addDeadBackend(anyLong);
                        times = 0; // should not call at all
                    }
                };
                Assert.assertTrue(nodeInLeader.handleHbResponse(shutdownResponse, false));
                Assert.assertFalse(nodeInLeader.isAlive());
                Assert.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInLeader.getStatus());
                Assert.assertEquals(shutdownResponse.getHbTime(), nodeInLeader.getLastUpdateMs());

                Assert.assertTrue(nodeInFollower.handleHbResponse(shutdownResponse, true));
                Assert.assertFalse(nodeInFollower.isAlive());
                Assert.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
                Assert.assertEquals(shutdownResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
            }
            // second shutdown hbResponse
            {
                BackendHbResponse shutdownResponse =
                        new BackendHbResponse(nodeInLeader.getId(), TStatusCode.SHUTDOWN, "BE is in shutting down");
                // triggers nothing
                new Expectations(resourceUsageMonitor) {
                    {
                        resourceUsageMonitor.notifyBackendDead();
                        times = 0;
                    }
                };
                new Expectations(coordinatorMonitor) {
                    {
                        coordinatorMonitor.addDeadBackend(anyLong);
                        times = 0;
                    }
                };
                Assert.assertTrue(nodeInLeader.handleHbResponse(shutdownResponse, false));
                Assert.assertFalse(nodeInLeader.isAlive());
                Assert.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInLeader.getStatus());
                Assert.assertEquals(shutdownResponse.getHbTime(), nodeInLeader.getLastUpdateMs());

                Assert.assertTrue(nodeInFollower.handleHbResponse(shutdownResponse, true));
                Assert.assertFalse(nodeInFollower.isAlive());
                Assert.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
                Assert.assertEquals(shutdownResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
            }
            // first Error hbResponse
            {
                long hbTime = nodeInLeader.getLastUpdateMs();
                BackendHbResponse errorResponse =
                        new BackendHbResponse(nodeInLeader.getId(), TStatusCode.THRIFT_RPC_ERROR, "rpc error");
                // triggers nothing
                new Expectations(resourceUsageMonitor) {
                    {
                        resourceUsageMonitor.notifyBackendDead();
                        times = 0;
                    }
                };
                new Expectations(coordinatorMonitor) {
                    {
                        coordinatorMonitor.addDeadBackend(anyLong);
                        times = 0;
                    }
                };
                Assert.assertTrue(nodeInLeader.handleHbResponse(errorResponse, false));
                Assert.assertFalse(nodeInLeader.isAlive());
                Assert.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInLeader.getStatus());
                // lastUpdateMs not updated
                Assert.assertNotEquals(errorResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
                Assert.assertEquals(hbTime, nodeInFollower.getLastUpdateMs());

                Assert.assertTrue(nodeInFollower.handleHbResponse(errorResponse, true));
                Assert.assertFalse(nodeInFollower.isAlive());
                Assert.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
                Assert.assertNotEquals(errorResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
                Assert.assertEquals(hbTime, nodeInFollower.getLastUpdateMs());
            }
            // second Error hbResponse
            {
                long hbTime = nodeInLeader.getLastUpdateMs();
                BackendHbResponse errorResponse =
                        new BackendHbResponse(nodeInLeader.getId(), TStatusCode.THRIFT_RPC_ERROR, "rpc error");
                new Expectations(resourceUsageMonitor) {
                    {
                        resourceUsageMonitor.notifyBackendDead();
                        times = 0;
                    }
                };
                new Expectations(coordinatorMonitor) {
                    {
                        coordinatorMonitor.addDeadBackend(anyLong);
                        times = 2;
                    }
                };
                Assert.assertTrue(nodeInLeader.handleHbResponse(errorResponse, false));
                Assert.assertFalse(nodeInLeader.isAlive());
                Assert.assertEquals(ComputeNode.Status.DISCONNECTED, nodeInLeader.getStatus());
                // lastUpdateMs not updated
                Assert.assertNotEquals(errorResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
                Assert.assertEquals(hbTime, nodeInFollower.getLastUpdateMs());

                Assert.assertTrue(nodeInFollower.handleHbResponse(errorResponse, true));
                Assert.assertFalse(nodeInFollower.isAlive());
                Assert.assertEquals(ComputeNode.Status.DISCONNECTED, nodeInFollower.getStatus());
                Assert.assertNotEquals(errorResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
                Assert.assertEquals(hbTime, nodeInFollower.getLastUpdateMs());
            }
        } finally {
            Config.heartbeat_retry_times = oldHeartbeatRetry;
        }
    }
}
