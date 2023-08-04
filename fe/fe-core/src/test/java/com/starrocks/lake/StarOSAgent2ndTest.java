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


package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ShardInfo;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerState;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * compared to StarOSAgentTest, only mock the StarClient, others keep real
 */
public class StarOSAgent2ndTest {
    private StarOSAgent starosAgent;

    @Mocked
    StarClient client;

    @Before
    public void setUp() throws Exception {
        starosAgent = new StarOSAgent();
        starosAgent.init(null);
    }

    @Test
    public void testGetBackendIdsByShardMissingStarletPort() throws StarClientException, UserException {
        String workerHost = "127.0.0.1";
        int workerStarletPort = 9070;
        long beId = 123L;
        int workerHeartbeatPort = 9050;
        long shardId = 10;

        WorkerInfo workerInfo = WorkerInfo.newBuilder()
                .setIpPort(String.format("%s:%d", workerHost, workerStarletPort))
                .setWorkerId(1L)
                .setWorkerState(WorkerState.ON)
                .putWorkerProperties("be_heartbeat_port", String.valueOf(workerHeartbeatPort))
                .putWorkerProperties("be_brpc_port", "8060")
                .build();

        ReplicaInfo replica = ReplicaInfo.newBuilder()
                .setReplicaRole(ReplicaRole.PRIMARY)
                .setWorkerInfo(workerInfo.toBuilder().build())
                .build();

        ShardInfo shardInfo = ShardInfo.newBuilder().setShardId(shardId)
                .addReplicaInfo(replica)
                .build();

        WorkerGroupDetailInfo wgDetailInfo = WorkerGroupDetailInfo.newBuilder()
                .addWorkersInfo(workerInfo.toBuilder().build())
                .build();

        new Expectations() {
            {
                client.getShardInfo("1", Lists.newArrayList(shardId), StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 0;
                result = Lists.newArrayList(shardInfo);

                client.listWorkerGroup("1", Collections.singletonList(0L), true);
                minTimes = 0;
                result = Lists.newArrayList(wgDetailInfo);
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Map<Long, Long> workerToBackend = Maps.newHashMap();
        Deencapsulation.setField(starosAgent, "workerToBackend", workerToBackend);

        // Test Backend
        { // give a correct starlet port, wrong heartbeat port
            Backend backend = new Backend(beId, workerHost, workerHeartbeatPort + 1);
            backend.setStarletPort(workerStarletPort);
            GlobalStateMgr.getCurrentSystemInfo().addBackend(backend);
            Assert.assertEquals(Sets.newHashSet(beId), starosAgent.getBackendIdsByShard(shardId, 0));
            GlobalStateMgr.getCurrentSystemInfo().dropBackend(backend);
            workerToBackend.clear();
        }
        { // No starlet port in backend, mismatch heartbeat port
            Backend backend = new Backend(beId, workerHost, workerHeartbeatPort + 1);
            backend.setStarletPort(0);
            GlobalStateMgr.getCurrentSystemInfo().addBackend(backend);
            // empty result
            Assert.assertTrue(starosAgent.getBackendIdsByShard(shardId, 0).isEmpty());
            GlobalStateMgr.getCurrentSystemInfo().dropBackend(backend);
            workerToBackend.clear();
        }
        { // No starlet port in backend, correct heartbeat port, can find the correct backend!
            Backend backend = new Backend(beId, workerHost, workerHeartbeatPort);
            backend.setStarletPort(0);
            GlobalStateMgr.getCurrentSystemInfo().addBackend(backend);
            Assert.assertEquals(Sets.newHashSet(beId), starosAgent.getBackendIdsByShard(shardId, 0));
            GlobalStateMgr.getCurrentSystemInfo().dropBackend(backend);
            workerToBackend.clear();
        }
        { // No starlet port in backend, correct heartbeat port, can find the correct backend for the workerGroup!
            Backend backend = new Backend(beId, workerHost, workerHeartbeatPort);
            backend.setStarletPort(0);
            GlobalStateMgr.getCurrentSystemInfo().addBackend(backend);
            Assert.assertEquals(Lists.newArrayList(beId), starosAgent.getWorkersByWorkerGroup(0));
            GlobalStateMgr.getCurrentSystemInfo().dropBackend(backend);
            workerToBackend.clear();
        }

        // Test ComputeNode
        { // give a correct starlet port, wrong heartbeat port
            ComputeNode cn = new ComputeNode(beId, workerHost, workerHeartbeatPort + 1);
            cn.setStarletPort(workerStarletPort);
            GlobalStateMgr.getCurrentSystemInfo().addComputeNode(cn);
            Assert.assertEquals(Sets.newHashSet(beId), starosAgent.getBackendIdsByShard(shardId, 0));
            GlobalStateMgr.getCurrentSystemInfo().dropComputeNode(cn);
            workerToBackend.clear();
        }
        { // No starlet port in backend, mismatch heartbeat port
            ComputeNode cn = new ComputeNode(beId, workerHost, workerHeartbeatPort + 1);
            cn.setStarletPort(0);
            GlobalStateMgr.getCurrentSystemInfo().addComputeNode(cn);
            // empty result
            Assert.assertTrue(starosAgent.getBackendIdsByShard(shardId, 0).isEmpty());
            GlobalStateMgr.getCurrentSystemInfo().dropComputeNode(cn);
            workerToBackend.clear();
        }
        { // No starlet port in backend, correct heartbeat port, can find the correct computeNode!
            ComputeNode cn = new ComputeNode(beId, workerHost, workerHeartbeatPort);
            cn.setStarletPort(0);
            GlobalStateMgr.getCurrentSystemInfo().addComputeNode(cn);
            Assert.assertEquals(Sets.newHashSet(beId), starosAgent.getBackendIdsByShard(shardId, 0));
            GlobalStateMgr.getCurrentSystemInfo().dropComputeNode(cn);
            workerToBackend.clear();
        }
        { // No starlet port in backend, correct heartbeat port, can find the correct computeNode for the workerGroup!
            ComputeNode cn = new ComputeNode(beId, workerHost, workerHeartbeatPort);
            cn.setStarletPort(0);
            GlobalStateMgr.getCurrentSystemInfo().addComputeNode(cn);
            Assert.assertEquals(Lists.newArrayList(beId), starosAgent.getWorkersByWorkerGroup(0));
            GlobalStateMgr.getCurrentSystemInfo().dropComputeNode(cn);
            workerToBackend.clear();
        }
    }
}
