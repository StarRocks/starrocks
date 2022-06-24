// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

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
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class StarOSAgentTest {
    private StarOSAgent starosAgent;

    @Mocked
    StarClient client;

    @Before
    public void setUp() throws Exception {
        starosAgent = new StarOSAgent();
    }

    @Test
    public void testRegisterAndBootstrapService() throws Exception {
        new Expectations() {
            {
                client.registerService("starrocks");
                minTimes = 0;
                result = null;

                client.bootstrapService("starrocks", "starrocks");
                minTimes = 0;
                result = 1;
            }
        };
        starosAgent.registerAndBootstrapService();
        Assert.assertEquals(1, starosAgent.getServiceIdForTest());
    }

    @Test
    public void testRegisterServiceException() throws Exception {
        new Expectations() {
            {
                client.registerService("starrocks");
                minTimes = 0;
                result = new StarClientException(StarClientException.ExceptionCode.ALREADY_EXIST,
                        "service already exists!");

                client.bootstrapService("starrocks", "starrocks");
                minTimes = 0;
                result = 3;
            }
        };
        starosAgent.registerAndBootstrapService();
        Assert.assertEquals(3L, starosAgent.getServiceIdForTest());
    }

    @Test
    public void testBootstrapServiceException() throws Exception {
        new Expectations() {
            {
                client.bootstrapService("starrocks", "starrocks");
                minTimes = 0;
                result = new StarClientException(StarClientException.ExceptionCode.ALREADY_EXIST,
                        "service already exists!");

                client.getServiceInfo("123").getServiceId();
                minTimes = 0;
                result = 4;
            }
        };
        starosAgent.registerAndBootstrapService();
        Assert.assertEquals(4, starosAgent.getServiceIdForTest());
    }

    @Test
    public void testGetServiceId() throws Exception {
        new Expectations() {
            {
                client.getServiceInfo("starrocks").getServiceId();
                minTimes = 0;
                result = 2;
            }
        };

        starosAgent.getServiceId();
        Assert.assertEquals(2L, starosAgent.getServiceIdForTest());
    }

    @Test
    public void testAddAndRemoveWorker() throws Exception {
         new Expectations() {
             {
                 client.addWorker(1, "127.0.0.1:8090");
                 minTimes = 0;
                 result = 10;

                 client.removeWorker(1, 10);
                 minTimes = 0;
                 result = null;
             }
         };

        String workerHost = "127.0.0.1:8090";
        starosAgent.setServiceId(1);
        starosAgent.addWorker(5, workerHost);
        Assert.assertEquals(10, starosAgent.getWorkerId(workerHost));

        starosAgent.removeWorker(workerHost);
        Assert.assertEquals(-1, starosAgent.getWorkerIdByBackendId(5));
    }

    @Test
    public void testAddWorkerException() throws Exception  {
        new Expectations() {
            {
                client.addWorker(1, "127.0.0.1:8090");
                minTimes = 0;
                result = new StarClientException(StarClientException.ExceptionCode.ALREADY_EXIST,
                        "worker already exists");

                client.getWorkerInfo(1, "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = 6;
            }
        };

        String workerHost = "127.0.0.1:8090";
        starosAgent.setServiceId(1);
        starosAgent.addWorker(5, workerHost);
        Assert.assertEquals(6, starosAgent.getWorkerId(workerHost));
        Assert.assertEquals(6, starosAgent.getWorkerIdByBackendId(5));
    }

    @Test
    public void testRemoveWorkerException() throws Exception {
        new Expectations() {
            {
                client.getWorkerInfo(1, "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = new StarClientException(StarClientException.ExceptionCode.GRPC,
                        "network error");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", 1L);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to get worker id from starMgr.",
                () -> starosAgent.removeWorker("127.0.0.1:8090"));

        new Expectations() {
            {
                client.getWorkerInfo(1, "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = 10;

                client.removeWorker(1, 10);
                minTimes = 0;
                result = new StarClientException(StarClientException.ExceptionCode.GRPC,
                        "network error");
            }
        };
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to remove worker.",
                () -> starosAgent.removeWorker("127.0.0.1:8090"));
    }

    public void testCreateShards() throws StarClientException, DdlException {
        new Expectations() {
            {
                client.createShard(1L, 2);
                minTimes = 0;
                result = Lists.newArrayList(ShardInfo.newBuilder().setShardId(10L).build(),
                        ShardInfo.newBuilder().setShardId(11L).build());
            }
        };

        starosAgent.setServiceId(1L);
        Assert.assertEquals(Lists.newArrayList(10L, 11L), starosAgent.createShards(2, null));
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

        new Expectations() {
            {
                client.getShardInfo(1L, Lists.newArrayList(10L));
                minTimes = 0;
                result = shards;
            }
        };

        Map<Long, Long> workerToBackend = Maps.newHashMap();
        workerToBackend.put(1L, 10001L);
        workerToBackend.put(2L, 10002L);
        workerToBackend.put(3L, 10003L);
        Deencapsulation.setField(starosAgent, "workerToBackend", workerToBackend);

        starosAgent.setServiceId(1L);
        Assert.assertEquals(10001L, starosAgent.getPrimaryBackendIdByShard(10L));
        Assert.assertEquals(Sets.newHashSet(10001L, 10002L, 10003L), starosAgent.getBackendIdsByShard(10L));
    }

    @Test
    public void testRemoveWorkerfromMap() {
        String workerHost = "127.0.0.1:8090";
        Map<String, Long> mockWorkerToId = Maps.newHashMap();
        mockWorkerToId.put(workerHost, 5L);
        Deencapsulation.setField(starosAgent, "workerToId", mockWorkerToId);
        Assert.assertEquals(5L, starosAgent.getWorkerId(workerHost));

        starosAgent.removeWorkerFromMap(5L, workerHost);
        ExceptionChecker.expectThrows(NullPointerException.class, () -> starosAgent.getWorkerId(workerHost));
    }
}
