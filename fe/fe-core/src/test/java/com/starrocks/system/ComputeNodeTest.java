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
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.CoordinatorMonitor;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.HeartbeatResponse.HbStatus;
import com.starrocks.thrift.TStatusCode;
import mockit.Expectations;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
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
        node.setArrowFlightPort(hbResponse.getArrowFlightPort());
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

            // start from the (heartbeat_retry_times)th response (started from 0), the status changed to disconnected.
            if (i >= Config.heartbeat_retry_times) {
                Assert.assertEquals(String.format("i=%d", i), ComputeNode.Status.DISCONNECTED, node.getStatus());
                Assert.assertEquals(String.format("i=%d", i), ComputeNode.Status.DISCONNECTED, nodeInFollower.getStatus());
            } else {
                Assert.assertEquals(String.format("i=%d", i), ComputeNode.Status.SHUTDOWN, node.getStatus());
                Assert.assertEquals(String.format("i=%d", i), ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
            }
        }
    }

    private BackendHbResponse generateReplayResponse(BackendHbResponse response) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(response), BackendHbResponse.class);
    }

    @Test
    public void testShutdownCancelQuery() {
        CoordinatorMonitor coordinatorMonitor = CoordinatorMonitor.getInstance();
        ResourceUsageMonitor resourceUsageMonitor = GlobalStateMgr.getCurrentState().getResourceUsageMonitor();

        int oldHeartbeatRetry = Config.heartbeat_retry_times;
        Config.heartbeat_retry_times = 1;

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
            nodeInFollower.handleHbResponse(generateReplayResponse(hbResponse), true);

            // second OK hbResponse for both leader and follower
            {
                Assert.assertFalse(nodeInLeader.handleHbResponse(hbResponse, false));
                Assert.assertTrue(nodeInLeader.isAlive());
                Assert.assertEquals(ComputeNode.Status.OK, nodeInLeader.getStatus());

                Assert.assertFalse(nodeInFollower.handleHbResponse(generateReplayResponse(hbResponse), true));
                Assert.assertTrue(nodeInFollower.isAlive());
                Assert.assertEquals(ComputeNode.Status.OK, nodeInFollower.getStatus());
            }

            // second OK hbResponse for both leader and follower
            {
                Assert.assertFalse(nodeInLeader.handleHbResponse(hbResponse, false));
                Assert.assertTrue(nodeInLeader.isAlive());
                Assert.assertEquals(ComputeNode.Status.OK, nodeInLeader.getStatus());

                Assert.assertFalse(nodeInFollower.handleHbResponse(generateReplayResponse(hbResponse), true));
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

                Assert.assertTrue(nodeInFollower.handleHbResponse(generateReplayResponse(shutdownResponse), true));
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

                Assert.assertTrue(nodeInFollower.handleHbResponse(generateReplayResponse(shutdownResponse), true));
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

                Assert.assertTrue(nodeInFollower.handleHbResponse(generateReplayResponse(errorResponse), true));
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

                Assert.assertTrue(nodeInFollower.handleHbResponse(generateReplayResponse(errorResponse), true));
                Assert.assertFalse(nodeInFollower.isAlive());
                Assert.assertEquals(ComputeNode.Status.DISCONNECTED, nodeInFollower.getStatus());
                Assert.assertNotEquals(errorResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
                Assert.assertEquals(hbTime, nodeInFollower.getLastUpdateMs());
            }
        } finally {
            Config.heartbeat_retry_times = oldHeartbeatRetry;
        }
    }

    private static class VerifyComputeNodeStatus {
        public boolean isAlive;
        public HeartbeatResponse.AliveStatus aliveStatus;
        public ComputeNode.Status status;

        public VerifyComputeNodeStatus() {
            // nothing
        }
        public VerifyComputeNodeStatus(boolean isAlive, HeartbeatResponse.AliveStatus aliveStatus, ComputeNode.Status status) {
            this.isAlive = isAlive;
            this.aliveStatus = aliveStatus;
            this.status = status;
        }
    }

    @Test
    public void testRpcErrorReplayRetryAndAliveStatusMismatch() throws InterruptedException {
        int prevHeartbeatRetryTimes = Config.heartbeat_retry_times;
        Config.heartbeat_retry_times = 3;
        ComputeNode node = new ComputeNode();

        List<BackendHbResponse> replayResponses = new ArrayList<>();
        List<VerifyComputeNodeStatus> verifyReplayNodeStatus = new ArrayList<>();

        BackendHbResponse hbResponse =
                new BackendHbResponse(node.getId(), node.getBePort(), node.getHttpPort(), node.getBrpcPort(),
                        node.getStarletPort(), System.currentTimeMillis(), node.getVersion(), node.getCpuCores(), 0);

        node.handleHbResponse(hbResponse, false);

        { // regular HbResponse
            Assert.assertFalse(node.handleHbResponse(hbResponse, false));
            Assert.assertTrue(node.isAlive());
            Assert.assertEquals(ComputeNode.Status.OK, node.getStatus());

            verifyReplayNodeStatus.add(
                    new VerifyComputeNodeStatus(node.isAlive(), hbResponse.aliveStatus, node.getStatus()));
            replayResponses.add(generateReplayResponse(hbResponse));
        }

        // 4 hbResponses
        // the first 3 hbResponses still record the node as ALIVE
        // the 4th hbResponses records the node as NOT_ALIVE
        for (int i = 0; i <= Config.heartbeat_retry_times; ++i) {
            BackendHbResponse errorResponse =
                    new BackendHbResponse(node.getId(), TStatusCode.INTERNAL_ERROR, "Internal Error");
            Assert.assertTrue(node.handleHbResponse(errorResponse, false));

            VerifyComputeNodeStatus verifyStatus = new VerifyComputeNodeStatus();
            verifyStatus.isAlive = node.isAlive();
            verifyStatus.status = node.getStatus();

            if (i == Config.heartbeat_retry_times) {
                Assert.assertFalse(node.isAlive());
                Assert.assertEquals(HeartbeatResponse.AliveStatus.NOT_ALIVE, errorResponse.aliveStatus);
                verifyStatus.aliveStatus = HeartbeatResponse.AliveStatus.NOT_ALIVE;
            } else {
                Assert.assertTrue(node.isAlive());
                Assert.assertEquals(HeartbeatResponse.AliveStatus.ALIVE, errorResponse.aliveStatus);
                verifyStatus.aliveStatus = HeartbeatResponse.AliveStatus.ALIVE;
            }
            replayResponses.add(generateReplayResponse(errorResponse));
            verifyReplayNodeStatus.add(verifyStatus);
            // delay a few milliseconds
            Thread.sleep(1);
        }

        // now reduce the retry config to 2
        // the first 2 hbResponses still detect the node as ALIVE
        // the 3th hbResponse will mark the node as NOT_ALIVE but then dictated by the aliveStatus, force to ALIVE
        // the 4th hbResponse will mark the node as NOT_ALIVE
        Config.heartbeat_retry_times = Config.heartbeat_retry_times - 1;
        ComputeNode replayNode = new ComputeNode();
        Assert.assertEquals(replayResponses.size(), verifyReplayNodeStatus.size());
        for (int i = 0; i < replayResponses.size(); ++i) {
            // even though the `heartbeat_retry_times` changed, the replay result
            // should be consistent with the one when generating the edit log.
            BackendHbResponse hb = replayResponses.get(i);
            VerifyComputeNodeStatus verifyStatus = verifyReplayNodeStatus.get(i);

            replayNode.handleHbResponse(hb, true);
            Assert.assertEquals(verifyStatus.isAlive, replayNode.isAlive());
            Assert.assertEquals(verifyStatus.status, replayNode.getStatus());
            Assert.assertEquals(verifyStatus.aliveStatus, hb.aliveStatus);
        }

        Config.heartbeat_retry_times = prevHeartbeatRetryTimes;
    }

    void verifyNodeAliveAndStatus(ComputeNode node, boolean expectedAlive, ComputeNode.Status expectedStatus) {
        Assert.assertEquals(expectedAlive, node.isAlive());
        Assert.assertEquals(expectedStatus, node.getStatus());
    }

    @Test
    public void testSetAliveInterface() {
        ComputeNode node = new ComputeNode();
        verifyNodeAliveAndStatus(node, false, ComputeNode.Status.CONNECTING);
        Assert.assertTrue(node.setAlive(true));
        verifyNodeAliveAndStatus(node, true, ComputeNode.Status.OK);
        // set again, nothing changed
        Assert.assertFalse(node.setAlive(true));
        verifyNodeAliveAndStatus(node, true, ComputeNode.Status.OK);

        Assert.assertTrue(node.setAlive(false));
        verifyNodeAliveAndStatus(node, false, ComputeNode.Status.DISCONNECTED);
        // set again, nothing changed
        Assert.assertFalse(node.setAlive(false));
        verifyNodeAliveAndStatus(node, false, ComputeNode.Status.DISCONNECTED);

        node = new ComputeNode();
        // isAlive: true, Status: Connecting
        node.getIsAlive().set(true);
        // setAlive will only change the alive variable, keep the status unchanged
        Assert.assertTrue(node.setAlive(false));
        verifyNodeAliveAndStatus(node, false, ComputeNode.Status.CONNECTING);
    }

    BackendHbResponse generateHbResponse(ComputeNode node, TStatusCode tStatusCode, long rebootTimeSecs) {
        if (tStatusCode != TStatusCode.OK) {
            return new BackendHbResponse(node.getId(), tStatusCode, "Unknown Error");
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

    String removeJsonKeyFromJsonString(String jsonString, String key) {
        JSONObject jsonObject = new JSONObject(jsonString);
        jsonObject.remove(key);
        return jsonObject.toString();
    }

    @Test
    public void testComputeNodeStatusUpgradeCompatibility() {
        long nodeId = 1000;
        ComputeNode node = new ComputeNode(nodeId, "127.0.0.1", 9050);
        long rebootTime = System.currentTimeMillis() / 1000 - 60;
        BackendHbResponse hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTime);
        Assert.assertTrue(node.handleHbResponse(hbResponse, false));
        Assert.assertEquals(ComputeNode.Status.OK, node.getStatus());
        Assert.assertTrue(node.isAlive());
        Assert.assertTrue(node.isAvailable());

        {
            String jsonString = GsonUtils.GSON.toJson(node);
            {
                // Replay the node from the jsonString, validate the gsonPostProcess doesn't have side effect
                ComputeNode reloadNode = GsonUtils.GSON.fromJson(jsonString, ComputeNode.class);
                Assert.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assert.assertTrue(reloadNode.isAlive());
                Assert.assertTrue(reloadNode.isAvailable());
                // Receive CN heartbeat, can correctly handle it.
                hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTime);
                reloadNode.handleHbResponse(hbResponse, false);
                Assert.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assert.assertTrue(reloadNode.isAlive());
                Assert.assertTrue(reloadNode.isAvailable());
            }
            // Remove the "status" serialization from the jsonString to simulate the scenario where the node was
            // upgraded from an old version where there is no "status" field.
            String reducedJsonString = removeJsonKeyFromJsonString(jsonString, "status");
            {
                // Replay the node from the reducedJsonString
                ComputeNode reloadNode = GsonUtils.GSON.fromJson(reducedJsonString, ComputeNode.class);
                Assert.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assert.assertTrue(reloadNode.isAlive());
                Assert.assertTrue(reloadNode.isAvailable());
                // Receive CN heartbeat, can correctly handle it.
                hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTime);
                reloadNode.handleHbResponse(hbResponse, false);
                Assert.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assert.assertTrue(reloadNode.isAlive());
                Assert.assertTrue(reloadNode.isAvailable());
            }
        }

        // Simulate a dead node and upgrade
        node.setAlive(false);
        Assert.assertEquals(ComputeNode.Status.DISCONNECTED, node.getStatus());
        Assert.assertFalse(node.isAlive());
        Assert.assertFalse(node.isAvailable());

        {
            String jsonString = GsonUtils.GSON.toJson(node);
            {
                // Replay the node from the jsonString, validate the gsonPostProcess doesn't have side effect
                ComputeNode reloadNode = GsonUtils.GSON.fromJson(jsonString, ComputeNode.class);
                Assert.assertEquals(ComputeNode.Status.DISCONNECTED, reloadNode.getStatus());
                Assert.assertFalse(reloadNode.isAlive());
                Assert.assertFalse(reloadNode.isAvailable());
                // Receive CN heartbeat, can correctly handle it.
                hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTime);
                reloadNode.handleHbResponse(hbResponse, false);
                Assert.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assert.assertTrue(reloadNode.isAlive());
                Assert.assertTrue(reloadNode.isAvailable());
            }
            // Remove the "status" serialization from the jsonString to simulate the scenario where the node was
            // upgraded from an old version where there is no "status" field.
            String reducedJsonString = removeJsonKeyFromJsonString(jsonString, "status");
            {
                // Replay the node from the reducedJsonString
                ComputeNode reloadNode = GsonUtils.GSON.fromJson(reducedJsonString, ComputeNode.class);
                // changed to CONNECTING, but it's fine
                Assert.assertEquals(ComputeNode.Status.CONNECTING, reloadNode.getStatus());
                Assert.assertFalse(reloadNode.isAlive());
                Assert.assertFalse(reloadNode.isAvailable());
                // Receive CN heartbeat, can correctly handle it.
                hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTime);
                reloadNode.handleHbResponse(hbResponse, false);
                Assert.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assert.assertTrue(reloadNode.isAlive());
                Assert.assertTrue(reloadNode.isAvailable());
            }
        }
    }
}
