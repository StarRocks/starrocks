// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import com.staros.proto.AllocateStorageInfo;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.ObjectStorageInfo;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardStorageInfo;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        starosAgent.init();
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
    public void testGetServiceStorageUri() throws StarClientException, DdlException {
        new Expectations() {
            {
                client.allocateStorage("1", (AllocateStorageInfo) any);
                minTimes = 0;
                result = ShardStorageInfo.newBuilder().setObjectStorageInfo(
                        ObjectStorageInfo.newBuilder().setObjectUri("s3://bucket/1/").build()).build();
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Assert.assertEquals("s3://bucket/1/",
                starosAgent.getServiceShardStorageInfo().getObjectStorageInfo().getObjectUri());
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
    public void testCreateShards() throws StarClientException, DdlException {
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
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        // test create shard group
        ExceptionChecker.expectThrowsNoException(() -> starosAgent.createShardGroup(groupId));
        // test create shards
        Assert.assertEquals(Lists.newArrayList(10L, 11L), starosAgent.createShards(2, null, groupId));
    }


    @Test
    public void testDeleteShards() throws StarClientException, DdlException {
        Set<Long> shardIds = new HashSet<>();
        shardIds.add(1L);
        shardIds.add(2L);
        new Expectations() {
            {
                client.deleteShard("1", shardIds);
                minTimes = 0;
                result = new StarClientException(StatusCode.GRPC, "network error");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        // test delete shard
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to delete shards.",
                () -> starosAgent.deleteShards(shardIds));
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
