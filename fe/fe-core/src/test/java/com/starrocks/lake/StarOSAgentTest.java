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
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.StatusCode;
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerState;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StarOSAgentTest {
    private StarOSAgent starosAgent;
    public static final String SERVICE_NAME = "starrocks";

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    SystemInfoService service;

    @Mocked
    StarClient client;

    @Before
    public void setUp() throws Exception {
        starosAgent = new StarOSAgent();
        starosAgent.init(null);
    }

    @Test
    public void testRegisterAndBootstrapService() throws Exception {
        new Expectations() {
            {
                client.registerService(SERVICE_NAME);
                minTimes = 0;
                result = null;

                client.bootstrapService("starrocks", SERVICE_NAME);
                minTimes = 0;
                result = "1";
            }
        };

        starosAgent.registerAndBootstrapService();
        Assert.assertEquals("1", Deencapsulation.getField(starosAgent, "serviceId"));
    }

    @Test
    public void testRegisterServiceException() throws Exception {
        new Expectations() {
            {
                client.registerService(SERVICE_NAME);
                minTimes = 0;
                result = new StarClientException(StatusCode.ALREADY_EXIST,
                        "service already exists!");

                client.bootstrapService("starrocks", SERVICE_NAME);
                minTimes = 0;
                result = "3";
            }
        };

        starosAgent.registerAndBootstrapService();
        Assert.assertEquals("3", Deencapsulation.getField(starosAgent, "serviceId"));
    }

    @Test
    public void testBootstrapServiceException() throws Exception {
        new Expectations() {
            {
                client.bootstrapService("starrocks", SERVICE_NAME);
                minTimes = 0;
                result = new StarClientException(StatusCode.ALREADY_EXIST,
                        "service already exists!");

                client.getServiceInfoByName(SERVICE_NAME).getServiceId();
                minTimes = 0;
                result = "4";
            }
        };

        starosAgent.registerAndBootstrapService();
        Assert.assertEquals("4", Deencapsulation.getField(starosAgent, "serviceId"));
    }

    @Test
    public void testGetServiceId() throws Exception {
        new Expectations() {
            {
                client.getServiceInfoByName(SERVICE_NAME).getServiceId();
                minTimes = 0;
                result = "2";
            }
        };

        starosAgent.getServiceId();
        Assert.assertEquals("2", Deencapsulation.getField(starosAgent, "serviceId"));
    }

    @Test
    public void testAllocateFilePath() throws StarClientException, DdlException {
        new Expectations() {
            {
                FilePathInfo pathInfo = client.allocateFilePath("1", FileStoreType.S3, "123");
                result = pathInfo;
                minTimes = 0;
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
    }

    @Test
    public void testAddAndRemoveWorker() throws Exception {
        new Expectations() {
            {
                client.addWorker("1", "127.0.0.1:8090");
                minTimes = 0;
                result = 10;

                client.removeWorker("1", 10);
                minTimes = 0;
                result = null;
            }
        };

        String workerHost = "127.0.0.1:8090";
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        starosAgent.addWorker(5, workerHost);
        Assert.assertEquals(10, starosAgent.getWorkerId(workerHost));

        starosAgent.removeWorker(workerHost);
        Assert.assertEquals(-1, starosAgent.getWorkerIdByBackendId(5));
    }

    @Test
    public void testAddWillRemovePreviousWorker() throws Exception {
        final String workerHost = "127.0.0.1:8090";
        final long workerId1 = 10;
        final long workerId2 = 11;
        new Expectations() {
            {
                client.addWorker("1", workerHost);
                minTimes = 1;
                result = workerId1;
            }
        };

        long backendId = 5;
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        starosAgent.addWorker(backendId, workerHost);
        Assert.assertEquals(workerId1, starosAgent.getWorkerIdByBackendId(backendId));

        final String workerHost2 = "127.0.0.1:8091";
        new Expectations() {
            {
                client.addWorker("1", workerHost2);
                minTimes = 1;
                result = workerId2;

                client.removeWorker("1", workerId1);
                minTimes = 1;
                result = null;
            }
        };
        starosAgent.addWorker(backendId, workerHost2);
        Assert.assertEquals(workerId2, starosAgent.getWorkerIdByBackendId(backendId));
    }

    @Test
    public void testAddWorkerException() throws Exception {
        new Expectations() {
            {
                client.addWorker("1", "127.0.0.1:8090");
                minTimes = 0;
                result = new StarClientException(StatusCode.ALREADY_EXIST, "worker already exists");

                client.getWorkerInfo("1", "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = 6;
            }
        };

        String workerHost = "127.0.0.1:8090";
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        starosAgent.addWorker(5, workerHost);
        Assert.assertEquals(6, starosAgent.getWorkerId(workerHost));
        Assert.assertEquals(6, starosAgent.getWorkerIdByBackendId(5));

        new Expectations() {
            {
                client.addWorker("1", "127.0.0.1:8091");
                minTimes = 0;
                result = new StarClientException(StatusCode.ALREADY_EXIST,
                        "worker already exists");

                client.getWorkerInfo("1", "127.0.0.1:8091").getWorkerId();
                minTimes = 0;
                result = new StarClientException(StatusCode.GRPC,
                        "network error");
            }
        };
        starosAgent.addWorker(10, "127.0.0.1:8091");
        ExceptionChecker.expectThrows(NullPointerException.class,
                () -> starosAgent.getWorkerId("127.0.0.1:8091"));
    }

    @Test
    public void testRemoveWorkerException() throws Exception {
        new Expectations() {
            {
                client.getWorkerInfo("1", "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = new StarClientException(StatusCode.GRPC, "network error");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to get worker id from starMgr.",
                () -> starosAgent.removeWorker("127.0.0.1:8090"));

        new Expectations() {
            {
                client.getWorkerInfo("1", "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = 10;

                client.removeWorker("1", 10);
                minTimes = 0;
                result = new StarClientException(StatusCode.GRPC, "network error");
            }
        };

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to remove worker.",
                () -> starosAgent.removeWorker("127.0.0.1:8090"));
    }

    @Test
    public void testCreateAndListShardGroup() throws StarClientException, DdlException {
        ShardInfo shard1 = ShardInfo.newBuilder().setShardId(10L).build();
        ShardInfo shard2 = ShardInfo.newBuilder().setShardId(11L).build();
        List<ShardInfo> shards = Lists.newArrayList(shard1, shard2);

        long groupId = 333;
        ShardGroupInfo info = ShardGroupInfo.newBuilder().setGroupId(groupId).build();
        List<ShardGroupInfo> groups = new ArrayList<>(1);
        groups.add(info);

        new MockUp<StarClient>() {
            @Mock
            public List<ShardInfo> createShard(String serviceId, List<CreateShardInfo> createShardInfos)
                    throws StarClientException {
                return shards;
            }

            @Mock
            public List<ShardGroupInfo> createShardGroup(String serviceId, List<CreateShardGroupInfo> createShardGroupInfos)
                    throws StarClientException {
                return groups;
            }

            @Mock
            public List<ShardGroupInfo> listShardGroup(String serviceId) throws StarClientException {
                return groups;
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        // test create shard group
        ExceptionChecker.expectThrowsNoException(() -> starosAgent.createShardGroup(0, 0, 1));
        // test create shards
        FilePathInfo pathInfo = FilePathInfo.newBuilder().build();
        FileCacheInfo cacheInfo = FileCacheInfo.newBuilder().build();
        Assert.assertEquals(Lists.newArrayList(10L, 11L), starosAgent.createShards(2, pathInfo, cacheInfo, 333));

        // list shard group
        List<ShardGroupInfo> realGroupIds = starosAgent.listShardGroup();
        Assert.assertEquals(1, realGroupIds.size());
        Assert.assertEquals(groupId, realGroupIds.get(0).getGroupId());
    }

    @Test
    public void testDeleteShardGroup() throws StarClientException, DdlException {
        new Expectations() {
            {
                client.deleteShardGroup("1", (List<Long>) any, true);
                minTimes = 0;
                result = null;
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");

        // test delete shard group
        ExceptionChecker.expectThrowsNoException(() -> starosAgent.deleteShardGroup(Lists.newArrayList(1L, 2L)));
    }


    @Test
    public void testGetBackendByShard() throws StarClientException, UserException {
        ReplicaInfo replica1 = ReplicaInfo.newBuilder()
                .setReplicaRole(ReplicaRole.PRIMARY)
                .setWorkerInfo(WorkerInfo.newBuilder().setWorkerId(1L).setWorkerState(WorkerState.ON).build())
                .build();
        ReplicaInfo replica2 = ReplicaInfo.newBuilder()
                .setReplicaRole(ReplicaRole.SECONDARY)
                .setWorkerInfo(WorkerInfo.newBuilder().setWorkerId(2L).setWorkerState(WorkerState.ON).build())
                .build();
        ReplicaInfo replica3 = ReplicaInfo.newBuilder()
                .setReplicaRole(ReplicaRole.SECONDARY)
                .setWorkerInfo(WorkerInfo.newBuilder().setWorkerId(3L).setWorkerState(WorkerState.OFF).build())
                .build();
        List<ReplicaInfo> replicas = Lists.newArrayList(replica1, replica2, replica3);

        ShardInfo shard = ShardInfo.newBuilder().setShardId(10L).addAllReplicaInfo(replicas).build();
        List<ShardInfo> shards = Lists.newArrayList(shard);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public long getBackendIdWithStarletPort(String host, int starletPort) {
                return -1L;
            }
        };

        new MockUp<WorkerInfo>() {
            @Mock
            public String getIpPort() {
                return "127.0.0.1:8090";
            }
        };

        new Expectations() {
            {
                client.getShardInfo("1", Lists.newArrayList(10L));
                minTimes = 0;
                result = shards;
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Map<Long, Long> workerToBackend = Maps.newHashMap();
        Deencapsulation.setField(starosAgent, "workerToBackend", workerToBackend);

        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Failed to get backend by worker. worker id",
                () -> starosAgent.getPrimaryBackendIdByShard(10L));

        Assert.assertEquals(Sets.newHashSet(), starosAgent.getBackendIdsByShard(10L));

        workerToBackend.put(1L, 10001L);
        workerToBackend.put(2L, 10002L);
        workerToBackend.put(3L, 10003L);
        Deencapsulation.setField(starosAgent, "workerToBackend", workerToBackend);

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Assert.assertEquals(10001L, starosAgent.getPrimaryBackendIdByShard(10L));
        Assert.assertEquals(Sets.newHashSet(10001L, 10002L, 10003L), starosAgent.getBackendIdsByShard(10L));
    }

    @Test
    public void testRemoveWorkerFromMap() {
        String workerHost = "127.0.0.1:8090";
        Map<String, Long> mockWorkerToId = Maps.newHashMap();
        mockWorkerToId.put(workerHost, 5L);
        Deencapsulation.setField(starosAgent, "workerToId", mockWorkerToId);
        Assert.assertEquals(5L, starosAgent.getWorkerId(workerHost));

        starosAgent.removeWorkerFromMap(5L, workerHost);
        ExceptionChecker.expectThrows(NullPointerException.class, () -> starosAgent.getWorkerId(workerHost));
    }

}
