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
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ShardInfo;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerState;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * compared to StarOSAgentTest, only mock the StarClient, others keep real
 */
public class StarOSAgent2ndTest {
    private StarOSAgent starosAgent;

    @Before
    public void setUp() throws Exception {
        starosAgent = new StarOSAgent();
        starosAgent.init(null);
    }

    @Test
    public void testGetBackendIdsByShardMissingStarletPort(@Mocked StarClient client) throws StarClientException,
            StarRocksException {
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
        Map<Long, Long> workerToNode = Maps.newHashMap();
        Deencapsulation.setField(starosAgent, "workerToNode", workerToNode);

        // Test Backend
        { // give a correct starlet port, wrong heartbeat port
            Backend backend = new Backend(beId, workerHost, workerHeartbeatPort + 1);
            backend.setStarletPort(workerStarletPort);
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend);
            Assert.assertEquals(Sets.newHashSet(beId), getBackendIdsByShard(shardId, 0));
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackend(backend);
            workerToNode.clear();
        }
        { // No starlet port in backend, mismatch heartbeat port
            Backend backend = new Backend(beId, workerHost, workerHeartbeatPort + 1);
            backend.setStarletPort(0);
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend);
            // empty result
            Assert.assertTrue(getBackendIdsByShard(shardId, 0).isEmpty());
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackend(backend);
            workerToNode.clear();
        }
        { // No starlet port in backend, correct heartbeat port, can find the correct backend!
            Backend backend = new Backend(beId, workerHost, workerHeartbeatPort);
            backend.setStarletPort(0);
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend);
            Assert.assertEquals(Sets.newHashSet(beId), getBackendIdsByShard(shardId, 0));
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackend(backend);
            workerToNode.clear();
        }
        { // No starlet port in backend, correct heartbeat port, can find the correct backend for the workerGroup!
            Backend backend = new Backend(beId, workerHost, workerHeartbeatPort);
            backend.setStarletPort(0);
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend);
            Assert.assertEquals(Lists.newArrayList(beId), starosAgent.getWorkersByWorkerGroup(0));
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackend(backend);
            workerToNode.clear();
        }

        // Test ComputeNode
        { // give a correct starlet port, wrong heartbeat port
            ComputeNode cn = new ComputeNode(beId, workerHost, workerHeartbeatPort + 1);
            cn.setStarletPort(workerStarletPort);
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(cn);
            Assert.assertEquals(Sets.newHashSet(beId), getBackendIdsByShard(shardId, 0));
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropComputeNode(cn);
            workerToNode.clear();
        }
        { // No starlet port in backend, mismatch heartbeat port
            ComputeNode cn = new ComputeNode(beId, workerHost, workerHeartbeatPort + 1);
            cn.setStarletPort(0);
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(cn);
            // empty result
            Assert.assertTrue(getBackendIdsByShard(shardId, 0).isEmpty());
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropComputeNode(cn);
            workerToNode.clear();
        }
        { // No starlet port in backend, correct heartbeat port, can find the correct computeNode!
            ComputeNode cn = new ComputeNode(beId, workerHost, workerHeartbeatPort);
            cn.setStarletPort(0);
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(cn);
            Assert.assertEquals(Sets.newHashSet(beId), getBackendIdsByShard(shardId, 0));
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropComputeNode(cn);
            workerToNode.clear();
        }
        { // No starlet port in backend, correct heartbeat port, can find the correct computeNode for the workerGroup!
            ComputeNode cn = new ComputeNode(beId, workerHost, workerHeartbeatPort);
            cn.setStarletPort(0);
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(cn);
            Assert.assertEquals(Lists.newArrayList(beId), starosAgent.getWorkersByWorkerGroup(0));
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropComputeNode(cn);
            workerToNode.clear();
        }
    }

    @Test
    public void testGetPrimaryComputeNodeIdByShard(@Mocked StarClient client) throws StarClientException,
            StarRocksException {
        String workerHost = "127.0.0.1";
        int workerStarletPort = 9070;
        int workerHeartbeatPort = 9050;
        long shardId = 10L;

        UtFrameUtils.mockInitWarehouseEnv();

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

        ShardInfo shardInfo0 = ShardInfo.newBuilder().setShardId(shardId)
                .addReplicaInfo(replica)
                .build();

        ShardInfo shardInfo1 = ShardInfo.newBuilder().setShardId(shardId)
                .build();

        new Expectations() {
            {
                client.getShardInfo("1", Lists.newArrayList(shardId), StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 0;
                returns(Lists.newArrayList(shardInfo0), Lists.newArrayList(shardInfo1));
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Map<Long, Long> workerToNode = Maps.newHashMap();
        workerToNode.put(1L, 2L);
        Deencapsulation.setField(starosAgent, "workerToNode", workerToNode);

        Assert.assertEquals(2, starosAgent.getPrimaryComputeNodeIdByShard(shardId));
        StarRocksException exception =
                Assert.assertThrows(StarRocksException.class, () -> starosAgent.getPrimaryComputeNodeIdByShard(shardId));
        Assert.assertEquals(InternalErrorCode.REPLICA_FEW_ERR, exception.getErrorCode());
    }

    @Test
    public void allocatePartitionFilePathInfo() {
        FilePathInfo.Builder fsPathBuilder = FilePathInfo.newBuilder();
        // set S3FsInfo
        fsPathBuilder.getFsInfoBuilder().getS3FsInfoBuilder()
                .setBucket("bucket")
                .setPathPrefix("app1");
        // set FsInfo
        fsPathBuilder.getFsInfoBuilder()
                .setFsName("test-name")
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.S3)
                .addAllLocations(new ArrayList<>());
        fsPathBuilder.setFullPath("s3://bucket/app1/service_id_balabala/db111/table222");

        long partitionId = 10086; // 0x2766

        // enablePartitioned prefix = false
        {
            FilePathInfo info = StarOSAgent.allocatePartitionFilePathInfo(fsPathBuilder.build(), partitionId);
            String expectedFullPath = String.format("%s/%d", fsPathBuilder.getFullPath(), partitionId);
            Assert.assertEquals(expectedFullPath, info.getFullPath());
            // Compare the info without the fullpath info, should be identical
            Assert.assertEquals(info.toBuilder().clearFullPath().toString(),
                    fsPathBuilder.build().toBuilder().clearFullPath().toString());
        }

        // enablePartitioned prefix = true
        fsPathBuilder.getFsInfoBuilder().getS3FsInfoBuilder().clearPathPrefix().setPartitionedPrefixEnabled(true)
                .setNumPartitionedPrefix(1024);
        fsPathBuilder.setFullPath("s3://bucket/service_id_balabala/db111/table222");

        {
            FilePathInfo info = StarOSAgent.allocatePartitionFilePathInfo(fsPathBuilder.build(), partitionId);
            // prefix: 10086 % 1024 = 870 (0x366) -> reverse order: 663
            String expectedFullPath =
                    String.format("s3://bucket/663/service_id_balabala/db111/table222/%d", partitionId);
            Assert.assertEquals(expectedFullPath, info.getFullPath());
            // Compare the info without the fullpath info, should be identical
            Assert.assertEquals(info.toBuilder().clearFullPath().toString(),
                    fsPathBuilder.build().toBuilder().clearFullPath().toString());
        }
    }

    private Set<Long> getBackendIdsByShard(long shardId, long workerGroupId) throws StarRocksException {
        return starosAgent.getAllNodeIdsByShard(shardId, workerGroupId, false);
    }
}
