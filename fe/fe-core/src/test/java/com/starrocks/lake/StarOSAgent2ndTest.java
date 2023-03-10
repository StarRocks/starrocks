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
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerState;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        int workerBePort = 9060;
        long shardId = 10;

        ReplicaInfo replica = ReplicaInfo.newBuilder()
                .setReplicaRole(ReplicaRole.PRIMARY)
                .setWorkerInfo(WorkerInfo.newBuilder()
                        .setIpPort(String.format("%s:%d", workerHost, workerStarletPort))
                        .setWorkerId(1L)
                        .setWorkerState(WorkerState.ON)
                        .putWorkerProperties("be_port", String.valueOf(workerBePort))
                        .putWorkerProperties("be_brpc_port", "8060")
                        .build())
                .build();

        ShardInfo shardInfo = ShardInfo.newBuilder().setShardId(shardId)
                .addReplicaInfo(replica)
                .build();

        new Expectations() {
            {
                client.getShardInfo("1", Lists.newArrayList(shardId));
                minTimes = 0;
                result = Lists.newArrayList(shardInfo);
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Map<Long, Long> workerToBackend = Maps.newHashMap();
        Deencapsulation.setField(starosAgent, "workerToBackend", workerToBackend);

        { // give a correct starlet port, wrong bePort
            Backend backend = new Backend(beId, workerHost, 0);
            backend.setStarletPort(workerStarletPort);
            backend.setBePort(workerBePort + 1);
            GlobalStateMgr.getCurrentSystemInfo().addBackend(backend);
            Assert.assertEquals(Sets.newHashSet(beId), starosAgent.getBackendIdsByShard(shardId));
            GlobalStateMgr.getCurrentSystemInfo().dropBackend(backend);
            workerToBackend.clear();
        }
        { // No starlet port in backend, be port mismatch
            Backend backend = new Backend(beId, workerHost, 0);
            backend.setStarletPort(0);
            backend.setBePort(workerBePort + 1);
            GlobalStateMgr.getCurrentSystemInfo().addBackend(backend);
            // empty result
            Assert.assertTrue(starosAgent.getBackendIdsByShard(shardId).isEmpty());
            GlobalStateMgr.getCurrentSystemInfo().dropBackend(backend);
            workerToBackend.clear();
        }
        { // No starlet port in backend, correct be port, can find the correct backend!
            Backend backend = new Backend(beId, workerHost, 0);
            backend.setStarletPort(0);
            backend.setBePort(workerBePort);
            GlobalStateMgr.getCurrentSystemInfo().addBackend(backend);
            Assert.assertEquals(Sets.newHashSet(beId), starosAgent.getBackendIdsByShard(shardId));
            GlobalStateMgr.getCurrentSystemInfo().dropBackend(backend);
            workerToBackend.clear();
        }
    }
}
