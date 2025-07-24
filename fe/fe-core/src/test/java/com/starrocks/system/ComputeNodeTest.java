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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.CoordinatorMonitor;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.HeartbeatResponse.HbStatus;
import com.starrocks.thrift.TStatusCode;
import mockit.Expectations;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
                // need sync to reset the node's heartbeatRetryTimes
                new TestCase(false, HbStatus.OK, null, true, true, false, true),
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
                    Assertions.assertEquals(tc.expectedNeedSync, needSync, "nodeId: " + node.getId());
                }
                Assertions.assertEquals(tc.expectedAlive, node.isAlive(), "nodeId: " + node.getId());
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
        Assertions.assertEquals(1000000L, node.getLastStartTime());
        Assertions.assertTrue(needSync);
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
            Assertions.assertFalse(node.handleHbResponse(hbResponse, false));
            Assertions.assertTrue(node.isAlive());
            Assertions.assertEquals(ComputeNode.Status.OK, node.getStatus());
        }
        { // first shutdown HbResponse
            BackendHbResponse shutdownResponse =
                    new BackendHbResponse(node.getId(), TStatusCode.SHUTDOWN, "BE is in shutting down");

            Assertions.assertTrue(node.handleHbResponse(shutdownResponse, false));
            Assertions.assertFalse(node.isAlive());
            Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, node.getStatus());
            Assertions.assertEquals(shutdownResponse.getHbTime(), node.getLastUpdateMs());
        }
        { // second shutdown HbResponse
            BackendHbResponse shutdownResponse =
                    new BackendHbResponse(node.getId(), TStatusCode.SHUTDOWN, "BE is in shutting down");

            Assertions.assertTrue(node.handleHbResponse(shutdownResponse, false));
            Assertions.assertTrue(nodeInFollower.handleHbResponse(shutdownResponse, true));
            Assertions.assertFalse(node.isAlive());
            Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, node.getStatus());
            Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
            Assertions.assertEquals(shutdownResponse.getHbTime(), node.getLastUpdateMs());
            Assertions.assertEquals(shutdownResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
        }
        long lastUpdateTime = node.getLastUpdateMs();
        for (int i = 0; i <= Config.heartbeat_retry_times + 2; ++i) {
            BackendHbResponse errorResponse =
                    new BackendHbResponse(node.getId(), TStatusCode.INTERNAL_ERROR, "Internal Error");

            Assertions.assertTrue(node.handleHbResponse(errorResponse, false));
            Assertions.assertTrue(nodeInFollower.handleHbResponse(errorResponse, true));
            Assertions.assertFalse(node.isAlive());
            // lasUpdateTime will not be updated
            Assertions.assertEquals(lastUpdateTime, node.getLastUpdateMs());

            // start from the (heartbeat_retry_times)th response (started from 0), the status changed to disconnected.
            if (i >= Config.heartbeat_retry_times) {
                Assertions.assertEquals(ComputeNode.Status.DISCONNECTED, node.getStatus(), String.format("i=%d", i));
                Assertions.assertEquals(ComputeNode.Status.DISCONNECTED, nodeInFollower.getStatus(), String.format("i=%d", i));
            } else {
                Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, node.getStatus(), String.format("i=%d", i));
                Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus(), String.format("i=%d", i));
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
                Assertions.assertFalse(nodeInLeader.handleHbResponse(hbResponse, false));
                Assertions.assertTrue(nodeInLeader.isAlive());
                Assertions.assertEquals(ComputeNode.Status.OK, nodeInLeader.getStatus());

                Assertions.assertFalse(nodeInFollower.handleHbResponse(generateReplayResponse(hbResponse), true));
                Assertions.assertTrue(nodeInFollower.isAlive());
                Assertions.assertEquals(ComputeNode.Status.OK, nodeInFollower.getStatus());
            }

            // second OK hbResponse for both leader and follower
            {
                Assertions.assertFalse(nodeInLeader.handleHbResponse(hbResponse, false));
                Assertions.assertTrue(nodeInLeader.isAlive());
                Assertions.assertEquals(ComputeNode.Status.OK, nodeInLeader.getStatus());

                Assertions.assertFalse(nodeInFollower.handleHbResponse(generateReplayResponse(hbResponse), true));
                Assertions.assertTrue(nodeInFollower.isAlive());
                Assertions.assertEquals(ComputeNode.Status.OK, nodeInFollower.getStatus());
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
                Assertions.assertTrue(nodeInLeader.handleHbResponse(shutdownResponse, false));
                Assertions.assertFalse(nodeInLeader.isAlive());
                Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInLeader.getStatus());
                Assertions.assertEquals(shutdownResponse.getHbTime(), nodeInLeader.getLastUpdateMs());

                Assertions.assertTrue(nodeInFollower.handleHbResponse(generateReplayResponse(shutdownResponse), true));
                Assertions.assertFalse(nodeInFollower.isAlive());
                Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
                Assertions.assertEquals(shutdownResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
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
                Assertions.assertTrue(nodeInLeader.handleHbResponse(shutdownResponse, false));
                Assertions.assertFalse(nodeInLeader.isAlive());
                Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInLeader.getStatus());
                Assertions.assertEquals(shutdownResponse.getHbTime(), nodeInLeader.getLastUpdateMs());

                Assertions.assertTrue(nodeInFollower.handleHbResponse(generateReplayResponse(shutdownResponse), true));
                Assertions.assertFalse(nodeInFollower.isAlive());
                Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
                Assertions.assertEquals(shutdownResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
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
                Assertions.assertTrue(nodeInLeader.handleHbResponse(errorResponse, false));
                Assertions.assertFalse(nodeInLeader.isAlive());
                Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInLeader.getStatus());
                // lastUpdateMs not updated
                Assertions.assertNotEquals(errorResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
                Assertions.assertEquals(hbTime, nodeInFollower.getLastUpdateMs());

                Assertions.assertTrue(nodeInFollower.handleHbResponse(generateReplayResponse(errorResponse), true));
                Assertions.assertFalse(nodeInFollower.isAlive());
                Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, nodeInFollower.getStatus());
                Assertions.assertNotEquals(errorResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
                Assertions.assertEquals(hbTime, nodeInFollower.getLastUpdateMs());
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
                Assertions.assertTrue(nodeInLeader.handleHbResponse(errorResponse, false));
                Assertions.assertFalse(nodeInLeader.isAlive());
                Assertions.assertEquals(ComputeNode.Status.DISCONNECTED, nodeInLeader.getStatus());
                // lastUpdateMs not updated
                Assertions.assertNotEquals(errorResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
                Assertions.assertEquals(hbTime, nodeInFollower.getLastUpdateMs());

                Assertions.assertTrue(nodeInFollower.handleHbResponse(generateReplayResponse(errorResponse), true));
                Assertions.assertFalse(nodeInFollower.isAlive());
                Assertions.assertEquals(ComputeNode.Status.DISCONNECTED, nodeInFollower.getStatus());
                Assertions.assertNotEquals(errorResponse.getHbTime(), nodeInFollower.getLastUpdateMs());
                Assertions.assertEquals(hbTime, nodeInFollower.getLastUpdateMs());
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
            Assertions.assertFalse(node.handleHbResponse(hbResponse, false));
            Assertions.assertTrue(node.isAlive());
            Assertions.assertEquals(ComputeNode.Status.OK, node.getStatus());

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
            Assertions.assertTrue(node.handleHbResponse(errorResponse, false));

            VerifyComputeNodeStatus verifyStatus = new VerifyComputeNodeStatus();
            verifyStatus.isAlive = node.isAlive();
            verifyStatus.status = node.getStatus();

            if (i == Config.heartbeat_retry_times) {
                Assertions.assertFalse(node.isAlive());
                Assertions.assertEquals(HeartbeatResponse.AliveStatus.NOT_ALIVE, errorResponse.aliveStatus);
                verifyStatus.aliveStatus = HeartbeatResponse.AliveStatus.NOT_ALIVE;
            } else {
                Assertions.assertTrue(node.isAlive());
                Assertions.assertEquals(HeartbeatResponse.AliveStatus.ALIVE, errorResponse.aliveStatus);
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
        Assertions.assertEquals(replayResponses.size(), verifyReplayNodeStatus.size());
        for (int i = 0; i < replayResponses.size(); ++i) {
            // even though the `heartbeat_retry_times` changed, the replay result
            // should be consistent with the one when generating the edit log.
            BackendHbResponse hb = replayResponses.get(i);
            VerifyComputeNodeStatus verifyStatus = verifyReplayNodeStatus.get(i);

            replayNode.handleHbResponse(hb, true);
            Assertions.assertEquals(verifyStatus.isAlive, replayNode.isAlive());
            Assertions.assertEquals(verifyStatus.status, replayNode.getStatus());
            Assertions.assertEquals(verifyStatus.aliveStatus, hb.aliveStatus);
        }

        Config.heartbeat_retry_times = prevHeartbeatRetryTimes;
    }

    void verifyNodeAliveAndStatus(ComputeNode node, boolean expectedAlive, ComputeNode.Status expectedStatus) {
        Assertions.assertEquals(expectedAlive, node.isAlive());
        Assertions.assertEquals(expectedStatus, node.getStatus());
    }

    @Test
    public void testSetAliveInterface() {
        ComputeNode node = new ComputeNode();
        verifyNodeAliveAndStatus(node, false, ComputeNode.Status.CONNECTING);
        Assertions.assertTrue(node.setAlive(true));
        verifyNodeAliveAndStatus(node, true, ComputeNode.Status.OK);
        // set again, nothing changed
        Assertions.assertFalse(node.setAlive(true));
        verifyNodeAliveAndStatus(node, true, ComputeNode.Status.OK);

        Assertions.assertTrue(node.setAlive(false));
        verifyNodeAliveAndStatus(node, false, ComputeNode.Status.DISCONNECTED);
        // set again, nothing changed
        Assertions.assertFalse(node.setAlive(false));
        verifyNodeAliveAndStatus(node, false, ComputeNode.Status.DISCONNECTED);

        node = new ComputeNode();
        // isAlive: true, Status: Connecting
        node.getIsAlive().set(true);
        // setAlive will only change the alive variable, keep the status unchanged
        Assertions.assertTrue(node.setAlive(false));
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
        Assertions.assertTrue(node.handleHbResponse(hbResponse, false));
        Assertions.assertEquals(rebootTimeA * 1000, node.getLastStartTime());
        Assertions.assertTrue(node.isAlive());

        // simulate that a few intermittent heartbeat probing failures due to high load or unstable network.
        // FE marks the BE as dead and then the node is back alive
        hbResponse = generateHbResponse(node, TStatusCode.THRIFT_RPC_ERROR, 0);
        Assertions.assertTrue(node.handleHbResponse(hbResponse, false));
        Assertions.assertFalse(node.isAlive());
        // BE reports heartbeat with the same rebootTime
        hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTimeA);
        Assertions.assertTrue(node.handleHbResponse(hbResponse, false));
        Assertions.assertTrue(node.isAlive());
        // reboot time should not change
        Assertions.assertEquals(rebootTimeA * 1000, node.getLastStartTime());

        // response an OK status, but the rebootTime changed.
        // This simulates the case that the BE is restarted quickly in between the two heartbeat probes.
        long rebootTimeB = System.currentTimeMillis() / 1000;
        hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTimeB);
        Assertions.assertTrue(node.handleHbResponse(hbResponse, false));
        Assertions.assertTrue(node.isAlive());
        // reboot time changed
        Assertions.assertEquals(rebootTimeB * 1000, node.getLastStartTime());

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
        Assertions.assertTrue(node.handleHbResponse(hbResponse, false));
        Assertions.assertEquals(ComputeNode.Status.OK, node.getStatus());
        Assertions.assertTrue(node.isAlive());
        Assertions.assertTrue(node.isAvailable());

        {
            String jsonString = GsonUtils.GSON.toJson(node);
            {
                // Replay the node from the jsonString, validate the gsonPostProcess doesn't have side effect
                ComputeNode reloadNode = GsonUtils.GSON.fromJson(jsonString, ComputeNode.class);
                Assertions.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assertions.assertTrue(reloadNode.isAlive());
                Assertions.assertTrue(reloadNode.isAvailable());
                // Receive CN heartbeat, can correctly handle it.
                hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTime);
                reloadNode.handleHbResponse(hbResponse, false);
                Assertions.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assertions.assertTrue(reloadNode.isAlive());
                Assertions.assertTrue(reloadNode.isAvailable());
            }
            // Remove the "status" serialization from the jsonString to simulate the scenario where the node was
            // upgraded from an old version where there is no "status" field.
            String reducedJsonString = removeJsonKeyFromJsonString(jsonString, "status");
            {
                // Replay the node from the reducedJsonString
                ComputeNode reloadNode = GsonUtils.GSON.fromJson(reducedJsonString, ComputeNode.class);
                Assertions.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assertions.assertTrue(reloadNode.isAlive());
                Assertions.assertTrue(reloadNode.isAvailable());
                // Receive CN heartbeat, can correctly handle it.
                hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTime);
                reloadNode.handleHbResponse(hbResponse, false);
                Assertions.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assertions.assertTrue(reloadNode.isAlive());
                Assertions.assertTrue(reloadNode.isAvailable());
            }
        }

        // Simulate a dead node and upgrade
        node.setAlive(false);
        Assertions.assertEquals(ComputeNode.Status.DISCONNECTED, node.getStatus());
        Assertions.assertFalse(node.isAlive());
        Assertions.assertFalse(node.isAvailable());

        {
            String jsonString = GsonUtils.GSON.toJson(node);
            {
                // Replay the node from the jsonString, validate the gsonPostProcess doesn't have side effect
                ComputeNode reloadNode = GsonUtils.GSON.fromJson(jsonString, ComputeNode.class);
                Assertions.assertEquals(ComputeNode.Status.DISCONNECTED, reloadNode.getStatus());
                Assertions.assertFalse(reloadNode.isAlive());
                Assertions.assertFalse(reloadNode.isAvailable());
                // Receive CN heartbeat, can correctly handle it.
                hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTime);
                reloadNode.handleHbResponse(hbResponse, false);
                Assertions.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assertions.assertTrue(reloadNode.isAlive());
                Assertions.assertTrue(reloadNode.isAvailable());
            }
            // Remove the "status" serialization from the jsonString to simulate the scenario where the node was
            // upgraded from an old version where there is no "status" field.
            String reducedJsonString = removeJsonKeyFromJsonString(jsonString, "status");
            {
                // Replay the node from the reducedJsonString
                ComputeNode reloadNode = GsonUtils.GSON.fromJson(reducedJsonString, ComputeNode.class);
                // changed to CONNECTING, but it's fine
                Assertions.assertEquals(ComputeNode.Status.CONNECTING, reloadNode.getStatus());
                Assertions.assertFalse(reloadNode.isAlive());
                Assertions.assertFalse(reloadNode.isAvailable());
                // Receive CN heartbeat, can correctly handle it.
                hbResponse = generateHbResponse(node, TStatusCode.OK, rebootTime);
                reloadNode.handleHbResponse(hbResponse, false);
                Assertions.assertEquals(ComputeNode.Status.OK, reloadNode.getStatus());
                Assertions.assertTrue(reloadNode.isAlive());
                Assertions.assertTrue(reloadNode.isAvailable());
            }
        }
    }

    @Test
    public void testIsChangedWhenHeartbeatRetryTimeChanged() {
        int oldHeartbeatRetry = Config.heartbeat_retry_times;
        Config.heartbeat_retry_times = 3;
        long nodeId = 1020250724;
        ComputeNode cnNodeInLeader = new ComputeNode(nodeId, "127.0.0.1", 9050);
        ComputeNode cnNodeInFollower = new ComputeNode(nodeId, "127.0.0.1", 9050);

        Assertions.assertFalse(cnNodeInLeader.isAlive());
        Assertions.assertFalse(cnNodeInFollower.isAlive());

        // Generate a series of hbResponses in the following order
        // Leader:   OK | OK | ERR | OK | ERR | ERR | OK | ERR | ERR | ERR | OK
        // Follower:  N |  N |  Y  |  Y |  Y  |  Y  |  Y |  Y  |  Y  |   Y | Y
        List<BackendHbResponse> hbResponses = new ArrayList<>();
        List<TStatusCode> statusCodes = List.of(
                TStatusCode.OK,
                TStatusCode.OK,
                TStatusCode.INTERNAL_ERROR,
                TStatusCode.OK,
                TStatusCode.INTERNAL_ERROR,
                TStatusCode.INTERNAL_ERROR,
                TStatusCode.OK,
                TStatusCode.OK,
                TStatusCode.INTERNAL_ERROR,
                TStatusCode.INTERNAL_ERROR,
                TStatusCode.INTERNAL_ERROR,
                TStatusCode.INTERNAL_ERROR,
                TStatusCode.OK);
        List<Boolean> expectedIsChanged = List.of(
                true, // the first OK response, turn alive from false to true
                false, // OK
                true,  // ERR
                true,  // OK
                true,  // ERR
                true,  // ERR
                true,  // OK
                false, // OK, consecutive OKs should not change the state
                true,  // ERR
                true,  // ERR
                true,  // ERR
                true,  // ERR
                true   // OK
        );

        int i = 0;
        for (TStatusCode statusCode : statusCodes) {
            String identifier = String.format("i=%d, statusCode=%s", i, statusCode);
            BackendHbResponse hbResponse = generateHbResponse(cnNodeInLeader, statusCode, 0);
            boolean expectedChange = expectedIsChanged.get(i++);
            boolean isChanged = cnNodeInLeader.handleHbResponse(hbResponse, false);
            Assertions.assertEquals(expectedChange, isChanged, identifier);
            if (isChanged) {
                cnNodeInFollower.handleHbResponse(hbResponse, true);
            }
            Assertions.assertEquals(cnNodeInLeader.isAlive(), cnNodeInFollower.isAlive(), identifier);
            Assertions.assertEquals(getRetryHeartbeatTimes(cnNodeInLeader), getRetryHeartbeatTimes(cnNodeInFollower),
                    identifier);
        }
        Config.heartbeat_retry_times = oldHeartbeatRetry;
    }

    static int getRetryHeartbeatTimes(ComputeNode node) {
        return Deencapsulation.getField(node, "heartbeatRetryTimes");
    }
}
