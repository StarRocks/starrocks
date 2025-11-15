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

package com.staros.client;

import com.google.common.collect.ImmutableMap;
import com.staros.credential.AwsCredentialMgr;
import com.staros.credential.AwsSimpleCredential;
import com.staros.exception.StarException;
import com.staros.manager.StarManager;
import com.staros.manager.StarManagerServer;
import com.staros.proto.ADLS2CredentialInfo;
import com.staros.proto.ADLS2FileStoreInfo;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.AzBlobCredentialInfo;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.CacheEnableState;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.DeleteMetaGroupInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.HDFSFileStoreInfo;
import com.staros.proto.JoinMetaGroupInfo;
import com.staros.proto.LeaderInfo;
import com.staros.proto.MetaGroupInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ReplicationType;
import com.staros.proto.S3FileStoreInfo;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ServiceState;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardInfoList;
import com.staros.proto.StatusCode;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.UpdateShardGroupInfo;
import com.staros.proto.UpdateShardInfo;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerGroupState;
import com.staros.proto.WorkerInfo;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.util.Constant;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StarClientTest {
    private String serverIpPort;
    private String serviceTemplateName = "StarClientTest";
    private StarManagerServer server;
    private Thread serverThread;

    private StarManagerServer follower;
    private Thread followerThread;
    private String followerIpPort;

    private static boolean zeroGroupDefaultValue;
    private StarClient client;

    @BeforeClass
    public static void prepare() {
        Config.S3_BUCKET = "bucket";
        Config.S3_REGION = "us-west-1";
        Config.S3_ENDPOINT = "https://bucket.us-west-1.com";
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_ID = "ak";
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_SECRET = "sk";
        // Force StarClient UT running in compatibility mode.
        zeroGroupDefaultValue = Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY;
        Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY = true;
        Config.HDFS_URL = "url";
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
        Config.LIST_SHARD_GROUP_BATCH_SIZE = 1;
        Config.STARMGR_REPLACE_FILESTORE_ENABLED = true;
    }

    @AfterClass
    public static void tearDown() {
        Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY = zeroGroupDefaultValue;
    }

    @Before
    public void startServer() {
        server = new StarManagerServer();
        serverThread = new Thread() {
            public void run() {
                try {
                    // let server itself detect available port
                    server.start(0);
                    serverIpPort = String.format("127.0.0.1:%d", server.getServerPort());
                    server.getStarManager().setListenAddressInfo("127.0.0.1", server.getServerPort());
                    server.getStarManager().becomeLeader();
                    server.blockUntilShutdown();
                } catch (Exception e) {
                    Assert.assertNull(e);
                }
            }
        };
        serverThread.start();
        server.blockUntilStart();

        client = new StarClient();
        client.connectServer(serverIpPort);
    }

    public void startFollower() {
        follower = new StarManagerServer();
        followerThread = new Thread() {
            public void run() {
                try {
                    // let server itself detect available port
                    follower.start(0);
                    followerIpPort = String.format("127.0.0.1:%d", follower.getServerPort());
                    follower.getStarManager().setListenAddressInfo("127.0.0.1", follower.getServerPort());
                    follower.getStarManager().becomeFollower();
                    follower.blockUntilShutdown();
                } catch (Exception e) {
                    Assert.assertNull(e);
                }
            }
        };
        followerThread.start();
        follower.blockUntilStart();
    }

    @After
    public void shutdownServer() {
        client.stop();
        try {
            server.shutdown();
            server.blockUntilShutdown();
            serverThread.join();
        } catch (Exception e) {
            Assert.assertNull(e);
        }

        if (follower != null) {
            try {
                follower.shutdown();
                follower.blockUntilShutdown();
                followerThread.join();
            } catch (Exception e) {
                Assert.assertNull(e);
            }
        }
    }

    @Test
    public void testStarClientRegisterService() {
        try {
            client.registerService(serviceTemplateName);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        try {
            client.registerService(serviceTemplateName);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.ALREADY_EXIST);
        }
    }

    @Test
    public void testStarClientDeregisterService() {
        try {
            client.registerService(serviceTemplateName);
            client.deregisterService(serviceTemplateName);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        try {
            client.deregisterService(serviceTemplateName);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarClientBootstrapService() {
        String serviceName = "StarClientTest-1";
        try {
            client.registerService(serviceTemplateName);

            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        try {
            client.bootstrapService(serviceTemplateName, serviceName);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.ALREADY_EXIST);
        }
    }

    @Test
    public void testStarClientShutdownService() {
        String serviceName = "StarClientTest-1";
        String serviceId = "0";
        try {
            client.registerService(serviceTemplateName);
            serviceId = client.bootstrapService(serviceTemplateName, serviceName);
            client.shutdownService(serviceId);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        try {
            client.shutdownService(serviceId + 1);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarClientGetServiceInfo() {
        String serviceId = "0";
        String serviceName = "StarClientTest-1";
        try {
            client.registerService(serviceTemplateName);

            serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            ServiceInfo serviceInfo = client.getServiceInfoById(serviceId);
            Assert.assertEquals(serviceInfo.getServiceTemplateName(), serviceTemplateName);
            Assert.assertEquals(serviceInfo.getServiceName(), serviceName);
            Assert.assertEquals(serviceInfo.getServiceId(), serviceId);
            Assert.assertEquals(serviceInfo.getServiceState(), ServiceState.RUNNING);

            serviceInfo = client.getServiceInfoByName(serviceName);
            Assert.assertEquals(serviceInfo.getServiceTemplateName(), serviceTemplateName);
            Assert.assertEquals(serviceInfo.getServiceName(), serviceName);
            Assert.assertEquals(serviceInfo.getServiceId(), serviceId);
            Assert.assertEquals(serviceInfo.getServiceState(), ServiceState.RUNNING);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        try {
            client.getServiceInfoById(serviceId + 1);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }

        try {
            client.getServiceInfoByName(serviceName + "aaa");
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarClientAddWorker() {
        String ipPort = "127.0.0.1:1234";
        String serviceId = "0";

        try {
            client.registerService(serviceTemplateName);
            String serviceName = "StarClientTest-1";
            serviceId = client.bootstrapService(serviceTemplateName, serviceName);
            client.addWorker(serviceId, ipPort);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        failToAddWorker(client, serviceId + "x", ipPort, StatusCode.NOT_EXIST);
        failToAddWorker(client, serviceId, ipPort, StatusCode.ALREADY_EXIST);
        // no port info
        failToAddWorker(client, serviceId, "1.2.3.4", StatusCode.INVALID_ARGUMENT);
        // empty port info
        failToAddWorker(client, serviceId, "1.2.3.4:", StatusCode.INVALID_ARGUMENT);
        // no ip address
        failToAddWorker(client, serviceId, ":1345", StatusCode.INVALID_ARGUMENT);
        // invalid ip address
        failToAddWorker(client, serviceId, "1.2.3.256:1345", StatusCode.INVALID_ARGUMENT);
        // invalid domain name
        failToAddWorker(client, serviceId, "invalid-domain-name.xx:1111", StatusCode.INVALID_ARGUMENT);

        try {
            // valid hostname
            Assert.assertNotEquals(0L, client.addWorker(serviceId, "localhost:1345"));
            // Assert.assertNotEquals(0L, client.addWorker(serviceId, "localhost.localdomain:1345"));
        } catch (StarClientException e) {
            Assert.fail(String.format("don't expect throw exceptions. Exception: %s", e.getMessage()));
        }
    }

    private void failToAddWorker(StarClient client, String serviceId, String workerAddress, StatusCode expectedCode) {
        try {
            client.addWorker(serviceId, workerAddress);
            Assert.fail("Expect exception throw before reaching here!");
        } catch (StarClientException e) {
            Assert.assertEquals(expectedCode, e.getCode());
        }
    }

    @Test
    public void testStarClientRemoveWorker() {
        String serviceId = "0";
        long workerId = 0;
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            String ipPort = "127.0.0.1:1234";
            workerId = client.addWorker(serviceId, ipPort);
            client.removeWorker(serviceId, workerId);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        try {
            client.removeWorker(serviceId + 1, workerId);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }

        try {
            client.removeWorker(serviceId, workerId + 1);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }

        // test remove worker from a given workerGroup
        long groupId = 0;
        try {
            String ipPort = "127.0.0.1:1235";
            WorkerGroupDetailInfo workerGroupInfo = client.createWorkerGroup(serviceId, "StarClientTest",
                    WorkerGroupSpec.newBuilder().setSize("X").build(), Collections.emptyMap(), Collections.emptyMap(),
                    1 /* replicaNumber */, ReplicationType.SYNC);
            groupId = workerGroupInfo.getGroupId();
            workerId = client.addWorker(serviceId, ipPort, groupId);
            client.removeWorker(serviceId, workerId, groupId);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        String finalServiceId = serviceId;
        long finalWorkerId = workerId;
        long finalGroupId = groupId;
        StarClientException exception = Assert.assertThrows(StarClientException.class, () -> client.removeWorker(
                finalServiceId, finalWorkerId + 1, finalGroupId));
        Assert.assertEquals(exception.getCode(), StatusCode.NOT_EXIST);
    }

    @Test
    public void testStarClientGetWorkerInfo() {
        String serviceId = "0";
        long workerId = 0;
        String ipPort = "127.0.0.1:1234";

        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            workerId = client.addWorker(serviceId, ipPort);
            WorkerInfo workerInfo = client.getWorkerInfo(serviceId, workerId);
            Assert.assertEquals(workerInfo.getServiceId(), serviceId);
            Assert.assertEquals(workerInfo.getWorkerId(), workerId);
            Assert.assertEquals(workerInfo.getIpPort(), ipPort);

            workerInfo = client.getWorkerInfo(serviceId, ipPort);
            Assert.assertEquals(workerInfo.getServiceId(), serviceId);
            Assert.assertEquals(workerInfo.getWorkerId(), workerId);
            Assert.assertEquals(workerInfo.getIpPort(), ipPort);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        try {
            client.getWorkerInfo(serviceId + 1, workerId);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }

        try {
            client.getWorkerInfo(serviceId, workerId + 1);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }

        try {
            client.getWorkerInfo(serviceId, ipPort + "aaa");
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarClientCreateShard() {
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            String ipPort = "127.0.0.1:1234";
            long workerId = client.addWorker(serviceId, ipPort);

            Map<String, String> properties = new HashMap<>();
            properties.put("hello", "world");

            int shardCount = 2;
            List<Long> shardIds = new ArrayList<>(shardCount);
            shardIds.add(10001L);
            shardIds.add(10002L);
            List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder b = CreateShardInfo.newBuilder()
                        .setReplicaCount(1)
                        .setShardId(shardIds.get(i))
                        .putAllShardProperties(properties)
                        .setScheduleToWorkerGroup(0)
                        .setPathInfo(newFilePathInfo());
                createShardInfos.add(b.build());
            }
            List<ShardInfo> shardInfos = client.createShard(serviceId, createShardInfos);
            Assert.assertEquals(shardInfos.size(), shardCount);
            for (int i = 0; i < shardCount; ++i) {
                Assert.assertEquals(shardInfos.get(i).getServiceId(), serviceId);
                Assert.assertEquals((long) shardInfos.get(i).getShardId(), (long) shardIds.get(i));
                Assert.assertEquals(properties, shardInfos.get(i).getShardPropertiesMap());
                Assert.assertEquals("s3://bucket/key", shardInfos.get(i).getFilePath().getFullPath());

                Assert.assertEquals(shardInfos.get(i).getReplicaInfoList().size(), 1);
                Assert.assertEquals(shardInfos.get(i).getReplicaInfoList().get(0).getWorkerInfo().getWorkerId(),
                        workerId);
            }

            // test empty info
            Assert.assertThrows(StarClientException.class, () -> {
                createShardInfos.clear();
                client.createShard(serviceId, createShardInfos);
            });
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }
    }

    private FilePathInfo newFilePathInfo() {
        AwsSimpleCredential credential = new AwsSimpleCredential("ak", "sk");
        S3FileStoreInfo s3Fs = S3FileStoreInfo.newBuilder()
                .setBucket("bucket")
                .setRegion("us-west-1")
                .setEndpoint("https://bucket.us-west-1.com")
                .setCredential(AwsCredentialMgr.toProtobuf(credential)).build();

        FileStoreInfo fsInfo = FileStoreInfo.newBuilder()
                .setFsType(FileStoreType.S3)
                .setFsKey(Constant.S3_FSKEY_FOR_CONFIG)
                .setFsName(Constant.S3_FSNAME_FOR_CONFIG)
                .setS3FsInfo(s3Fs)
                .build();
        return FilePathInfo.newBuilder().setFsInfo(fsInfo).setFullPath("s3://bucket/key").build();
    }

    @Test
    public void testCreateShardScheduleToNonDefaultWorkerGroup() throws StarClientException {
        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-1";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

        WorkerGroupDetailInfo workerGroupInfo = client.createWorkerGroup(serviceId, "StarClientTest",
                WorkerGroupSpec.newBuilder().setSize("X").build(), Collections.emptyMap(), Collections.emptyMap(),
                1 /* replicaNumber */, ReplicationType.SYNC);
        long workerGroupId = workerGroupInfo.getGroupId();

        // workerId0 in Default-WorkerGroup (id: 0)
        long workerId0 = client.addWorker(serviceId, "127.0.0.1:1234");
        // workerId1 in non-default-WorkerGroup (id: workerGroupId)
        long workerId1 = client.addWorker(serviceId, "127.0.0.1:1235", workerGroupId);

        Map<String, String> properties = new HashMap<>();
        properties.put("hello", "world");

        long shardId = 123456789L;
        CreateShardInfo request = CreateShardInfo.newBuilder()
                .setReplicaCount(1)
                .setShardId(shardId)
                .putAllShardProperties(properties)
                .setScheduleToWorkerGroup(workerGroupId) // request schedule to workerGroupId
                .setPathInfo(newFilePathInfo())
                .build();

        { // create shards and request to schedule to `workerGroupId`
            List<ShardInfo> shardInfoList = client.createShard(serviceId, Collections.singletonList(request));
            Assert.assertEquals(1L, shardInfoList.size());
            ShardInfo info = shardInfoList.get(0);
            Assert.assertEquals(serviceId, info.getServiceId());
            Assert.assertEquals(shardId, info.getShardId());
            Assert.assertEquals(properties, info.getShardPropertiesMap());
            Assert.assertEquals("s3://bucket/key", info.getFilePath().getFullPath());
            Assert.assertEquals(1L, info.getReplicaInfoList().size());
            // shard scheduled to workerId1 in workerGroupId
            Assert.assertEquals(workerId1, info.getReplicaInfoList().get(0).getWorkerInfo().getWorkerId());
        }

        { // try to get shard distribution from default worker group
            List<ShardInfo> shardInfoList =
                    client.getShardInfo(serviceId, Collections.singletonList(shardId), StarClient.DEFAULT_ID);
            Assert.assertEquals(1L, shardInfoList.size());
            ShardInfo info = shardInfoList.get(0);
            Assert.assertEquals(serviceId, info.getServiceId());
            Assert.assertEquals(shardId, info.getShardId());
            Assert.assertEquals(properties, info.getShardPropertiesMap());
            Assert.assertEquals("s3://bucket/key", info.getFilePath().getFullPath());
            Assert.assertEquals(1L, info.getReplicaInfoList().size());
            // shard scheduled to workerId0 in default worker group
            Assert.assertEquals(workerId0, info.getReplicaInfoList().get(0).getWorkerInfo().getWorkerId());
        }
    }

    @Test
    public void testStarClientDeleteShard() {
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            String ipPort = "127.0.0.1:1234";
            long workerId = client.addWorker(serviceId, ipPort);

            int shardCount = 2;
            List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                builder.setReplicaCount(1);
                createShardInfos.add(builder.build());
            }
            List<ShardInfo> shardInfos = client.createShard(serviceId, createShardInfos);

            Set<Long> shardIds = new HashSet<>();
            for (ShardInfo shardInfo : shardInfos) {
                shardIds.add(shardInfo.getShardId());
            }

            client.deleteShard(serviceId, shardIds);

            // test empty id
            Assert.assertThrows(StarClientException.class, () -> {
                shardIds.clear();
                client.deleteShard(serviceId, shardIds);
            });
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testStarClientUpdateShard() {
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            String ipPort = "127.0.0.1:1234";
            long workerId = client.addWorker(serviceId, ipPort);

            int shardCount = 2;
            List<Long> shardIds = new ArrayList<>(shardCount);
            shardIds.add(10001L);
            shardIds.add(10002L);
            List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder()
                        .setReplicaCount(1)
                        .setShardId(shardIds.get(i));
                createShardInfos.add(builder.build());
            }
            List<ShardInfo> shardInfos = client.createShard(serviceId, createShardInfos);

            List<UpdateShardInfo> updateShardInfos = new ArrayList<>(shardCount);
            updateShardInfos.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(0))
                    .setEnableCache(CacheEnableState.ENABLED).build());
            updateShardInfos.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(1))
                    .setEnableCache(CacheEnableState.ENABLED).build());
            client.updateShard(serviceId, updateShardInfos);
            shardInfos = client.getShardInfo(serviceId, shardIds);
            for (ShardInfo shardInfo : shardInfos) {
                Assert.assertTrue(shardInfo.getFileCache().getEnableCache());
            }

            List<UpdateShardInfo> updateShardInfos1 = new ArrayList<>(shardCount);
            updateShardInfos1.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(0))
                    .setEnableCache(CacheEnableState.DISABLED).build());
            updateShardInfos1.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(1))
                    .setEnableCache(CacheEnableState.DISABLED).build());
            client.updateShard(serviceId, updateShardInfos1);
            shardInfos = client.getShardInfo(serviceId, shardIds);
            for (ShardInfo shardInfo : shardInfos) {
                Assert.assertFalse(shardInfo.getFileCache().getEnableCache());
            }

            Set<Long> shardIdsSet = new HashSet<>();
            for (ShardInfo shardInfo : shardInfos) {
                shardIdsSet.add(shardInfo.getShardId());
            }
            client.deleteShard(serviceId, shardIdsSet);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }
    }

    public void testStarClientGetShardInfo(StarManagerServer svr) {
        client.stop();
        // recreate StarClient
        client = new StarClient(svr);
        client.connectServer(serverIpPort);

        String serviceId = "0";
        List<ShardInfo> shardInfos = null;
        int shardCount = 2;

        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            String ipPort = "127.0.0.1:1234";
            long workerId = client.addWorker(serviceId, ipPort);

            List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                builder.setReplicaCount(1);
                createShardInfos.add(builder.build());
            }
            shardInfos = client.createShard(serviceId, createShardInfos);
            Assert.assertEquals(shardInfos.size(), shardCount);

            List<Long> shardIds = new ArrayList<>();
            for (ShardInfo shardInfo : shardInfos) {
                shardIds.add(shardInfo.getShardId());
            }

            shardInfos = client.getShardInfo(serviceId, shardIds);
            for (ShardInfo shardInfo : shardInfos) {
                Assert.assertEquals(shardInfo.getServiceId(), serviceId);

                Assert.assertEquals(shardInfo.getReplicaInfoList().size(), 1);
                Assert.assertEquals(shardInfo.getReplicaInfoList().get(0).getWorkerInfo().getWorkerId(), workerId);
            }
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        try {
            List<Long> shardIds3 = new ArrayList<>();
            client.getShardInfo(serviceId, shardIds3);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.INVALID_ARGUMENT);
        }

        try {
            List<Long> shardIds2 = new ArrayList<>();
            for (ShardInfo shardInfo : shardInfos) {
                shardIds2.add(shardInfo.getShardId() + shardCount);
            }

            client.getShardInfo(serviceId, shardIds2);
        } catch (StarClientException e) {
            Assert.assertEquals(e.getCode(), StatusCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarClientGetShardInfo() {
        testStarClientGetShardInfo(null);
    }

    @Test
    public void testStarClientGetShardInfoDirectAccess() {
        testStarClientGetShardInfo(server);
    }

    @Test
    public void testStarClientListShardInfo() {
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            int shardCount = 2;
            List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
            List<Long> groupIds = new ArrayList<>();
            groupIds.add(StarClient.DEFAULT_ID);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                builder.setReplicaCount(1);
                builder.addAllGroupIds(groupIds);
                createShardInfos.add(builder.build());
            }
            List<ShardInfo> shardInfos = client.createShard(serviceId, createShardInfos);
            List<Long> shardIds = shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());

            List<List<ShardInfo>> shardInfos2 = client.listShard(serviceId, groupIds);
            Assert.assertEquals(shardInfos2.size(), 1);
            List<Long> shardIds2 = shardInfos2.get(0).stream().map(ShardInfo::getShardId).collect(Collectors.toList());

            Assert.assertEquals(shardIds, shardIds2);

            // test empty group id
            Assert.assertThrows(StarClientException.class, () -> {
                groupIds.clear();
                client.listShard(serviceId, groupIds);
            });
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testStarClientListShardInfoNonDefaultWorkerGroup() throws StarClientException {
        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-1";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

        // create a worker group with no worker yet
        WorkerGroupDetailInfo workerGroupInfo = client.createWorkerGroup(serviceId, "StarClientTest",
                WorkerGroupSpec.newBuilder().setSize("X").build(), Collections.emptyMap(), Collections.emptyMap(),
                1 /* replicaNumber */, ReplicationType.SYNC);
        long workerGroupId = workerGroupInfo.getGroupId();

        // create a shard group
        CreateShardGroupInfo shardGroupRequest =
                CreateShardGroupInfo.newBuilder().setPolicy(PlacementPolicy.SPREAD).build();
        List<ShardGroupInfo> shardGroupResponse = client.createShardGroup(serviceId,
                Collections.singletonList(shardGroupRequest));
        Assert.assertEquals(1L, shardGroupResponse.size());
        long shardGroupId = shardGroupResponse.get(0).getGroupId();

        // create a shard in the shard group
        long shardId = 1357924680L;
        CreateShardInfo shardRequest = CreateShardInfo.newBuilder()
                .setPathInfo(newFilePathInfo())
                .setReplicaCount(1)
                .setShardId(shardId)
                .addGroupIds(shardGroupId)
                .build();
        List<ShardInfo> shardResponse = client.createShard(serviceId, Collections.singletonList(shardRequest));
        Assert.assertEquals(1L, shardResponse.size());

        { // list shard info in workerGroupId, expect empty replica info
            List<List<ShardInfo>> shardInfoLists =
                    client.listShard(serviceId, Collections.singletonList(shardGroupId), workerGroupId,
                            false /* withoutReplicaInfo */);
            Assert.assertEquals(1L, shardInfoLists.size());
            Assert.assertEquals(1L, shardInfoLists.get(0).size());
            ShardInfo info = shardInfoLists.get(0).get(0);
            Assert.assertEquals(shardId, info.getShardId());
            Assert.assertEquals(0L, info.getReplicaInfoCount());
        }
        // Add a worker to the worker group
        long workerId = client.addWorker(serviceId, "127.0.0.1:1234", workerGroupId);

        { // list again, should have replica info on workerId
            List<List<ShardInfo>> shardInfoLists =
                    client.listShard(serviceId, Collections.singletonList(shardGroupId), workerGroupId,
                            false /* withoutReplicaInfo */);
            Assert.assertEquals(1L, shardInfoLists.size());
            Assert.assertEquals(1L, shardInfoLists.get(0).size());
            ShardInfo info = shardInfoLists.get(0).get(0);
            Assert.assertEquals(shardId, info.getShardId());
            Assert.assertEquals(1L, info.getReplicaInfoCount());
            // the replica is scheduled to worker:workerId
            Assert.assertEquals(workerId, info.getReplicaInfoList().get(0).getWorkerInfo().getWorkerId());
        }

        { // list without worker info
            List<List<ShardInfo>> shardInfoLists =
                    client.listShard(serviceId, Collections.singletonList(shardGroupId), workerGroupId,
                            true /* withoutReplicaInfo */);
            Assert.assertEquals(1L, shardInfoLists.size());
            Assert.assertEquals(1L, shardInfoLists.get(0).size());
            ShardInfo info = shardInfoLists.get(0).get(0);
            Assert.assertEquals(shardId, info.getShardId());
            Assert.assertEquals(0L, info.getReplicaInfoCount());
        }
    }

    @Test
    public void testStarClientShardGroup() {
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            // test create shard group
            List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();

            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder()
                    .setPolicy(PlacementPolicy.NONE)
                    .putLabels("labelkey", "labelvalue")
                    .build());
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder()
                    .setPolicy(PlacementPolicy.RANDOM)
                    .putProperties("partitionid", "123456")
                    .build());

            List<ShardGroupInfo> shardGroupInfos = client.createShardGroup(serviceId, createShardGroupInfos);

            Assert.assertEquals(createShardGroupInfos.size(), shardGroupInfos.size());
            Assert.assertEquals(shardGroupInfos.get(0).getServiceId(), serviceId);
            Assert.assertEquals(shardGroupInfos.get(0).getPolicy(), PlacementPolicy.NONE);
            Assert.assertEquals(shardGroupInfos.get(0).getLabelsCount(), 1);
            Assert.assertEquals(shardGroupInfos.get(0).getLabelsMap().get("labelkey"), "labelvalue");
            Assert.assertEquals(shardGroupInfos.get(1).getServiceId(), serviceId);
            Assert.assertEquals(shardGroupInfos.get(1).getPolicy(), PlacementPolicy.RANDOM);
            Assert.assertEquals(shardGroupInfos.get(1).getPropertiesCount(), 1);
            Assert.assertEquals(shardGroupInfos.get(1).getPropertiesMap().get("partitionid"), "123456");
        } catch (StarClientException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testStarClientUpdateShardGroup() {
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder()
                    .setPolicy(PlacementPolicy.NONE)
                    .putLabels("labelkey", "labelvalue")
                    .build());
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder()
                    .setPolicy(PlacementPolicy.RANDOM)
                    .putProperties("partitionid", "123456")
                    .build());

            List<ShardGroupInfo> shardGroupInfos = client.createShardGroup(serviceId, createShardGroupInfos);
            Assert.assertEquals(createShardGroupInfos.size(), shardGroupInfos.size());

            // test update shard group
            List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());
            List<UpdateShardGroupInfo> updateShardGroupInfos = new ArrayList<>();
            updateShardGroupInfos.add(UpdateShardGroupInfo.newBuilder().setGroupId(groupIds.get(0))
                    .setEnableCache(CacheEnableState.ENABLED).build());
            updateShardGroupInfos.add(UpdateShardGroupInfo.newBuilder().setGroupId(groupIds.get(1))
                    .setEnableCache(CacheEnableState.DISABLED).build());
            client.updateShardGroup(serviceId, updateShardGroupInfos);

            client.deleteShardGroup(serviceId, groupIds, true);
        } catch (StarClientException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testStarClientListShardGroup() {
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            // test create shard group
            List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();

            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().setPolicy(PlacementPolicy.SPREAD).build());
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().setPolicy(PlacementPolicy.PACK).build());

            List<ShardGroupInfo> shardGroupInfos = client.createShardGroup(serviceId, createShardGroupInfos);
            List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());

            List<ShardGroupInfo> shardGroupInfos2 = client.listShardGroup(serviceId);
            List<Long> groupIds2 =
                    shardGroupInfos2.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());

            Assert.assertEquals(shardGroupInfos.size() + 1 /* plus default group */, shardGroupInfos2.size());
            for (Long gid : groupIds) {
                Assert.assertTrue(groupIds2.contains(gid));
            }
        } catch (StarClientException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testStarClientGetShardGroup() {
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            // test create shard group
            List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());

            List<ShardGroupInfo> shardGroupInfos = client.createShardGroup(serviceId, createShardGroupInfos);
            List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());

            List<ShardGroupInfo> shardGroupInfos2 = client.getShardGroup(serviceId, groupIds);
            Assert.assertEquals(shardGroupInfos.size(), shardGroupInfos2.size());

            for (int i = 0; i < 2; ++i) {
                Assert.assertEquals(shardGroupInfos.get(i).getGroupId(), shardGroupInfos2.get(i).getGroupId());
            }
        } catch (StarClientException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testStarClientMetaGroup() {
        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-MetaGroup";
            String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            // test list meta group
            Assert.assertEquals(client.listMetaGroup(serviceId).size(), 0);

            // create shard group
            List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
            List<ShardGroupInfo> shardGroupInfos = client.createShardGroup(serviceId, createShardGroupInfos);

            // create a shard in this shard group
            List<Long> groupIds = new ArrayList<>();
            groupIds.add(shardGroupInfos.get(0).getGroupId());
            List<CreateShardInfo> createShardInfos = new ArrayList<>();
            createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds).build());
            client.createShard(serviceId, createShardInfos);

            // test create meta group
            CreateMetaGroupInfo createInfo = CreateMetaGroupInfo.newBuilder()
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .build();
            MetaGroupInfo metaGroupInfo = client.createMetaGroup(serviceId, createInfo);
            long metaGroupId = metaGroupInfo.getMetaGroupId();
            Assert.assertEquals(metaGroupInfo.getServiceId(), serviceId);
            Assert.assertNotEquals(0, metaGroupId);
            Assert.assertEquals(metaGroupInfo.getShardGroupIdsList().size(), 0);
            Assert.assertEquals(metaGroupInfo.getPlacementPolicy(), PlacementPolicy.PACK);
            StarClientException thrown1 = Assert.assertThrows(StarClientException.class, () -> {
                client.createMetaGroup(serviceId + "aaa", createInfo);
            });
            Assert.assertEquals(StatusCode.NOT_EXIST, thrown1.getCode());

            // test update meta group
            JoinMetaGroupInfo joinInfo = JoinMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                    .build();
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setJoinInfo(joinInfo)
                    .addAllShardGroupIds(groupIds)
                    .build();
            client.updateMetaGroup(serviceId, updateInfo);
            StarClientException thrown2 = Assert.assertThrows(StarClientException.class, () -> {
                client.updateMetaGroup(serviceId + "aaa", updateInfo);
            });
            Assert.assertEquals(StatusCode.NOT_EXIST, thrown2.getCode());

            // test meta group after update
            metaGroupInfo = client.getMetaGroupInfo(serviceId, metaGroupId);
            Assert.assertEquals(metaGroupInfo.getShardGroupIdsList().size(), 1);
            // test list meta group
            List<MetaGroupInfo> metaGroupInfos = client.listMetaGroup(serviceId);
            Assert.assertEquals(metaGroupInfos.size(), 1);
            Assert.assertEquals(metaGroupInfos.get(0).getMetaGroupId(), metaGroupId);

            // Check if MetaGroup is stable. The target metaGroup only has one shardGroup & one shard, expect to be always stable.
            boolean isStable = client.queryMetaGroupStable(serviceId, metaGroupId);
            Assert.assertTrue(isStable);

            // Test queryMetaGroupStable exception cases
            { // metaGroupId not exist
                StarClientException notExist = Assert.assertThrows(StarClientException.class,
                        () -> client.queryMetaGroupStable(serviceId, metaGroupId + 10));
                Assert.assertEquals(StatusCode.NOT_EXIST, notExist.getCode());
            }
            { // workerGroupId not exist
                StarClientException notExist = Assert.assertThrows(StarClientException.class,
                        () -> client.queryMetaGroupStable(serviceId, metaGroupId, 123456L));
                Assert.assertEquals(StatusCode.NOT_EXIST, notExist.getCode());
            }
            { // serviceId not exist
                StarClientException notExist = Assert.assertThrows(StarClientException.class,
                        () -> client.queryMetaGroupStable(serviceId + "aa", metaGroupId));
                Assert.assertEquals(StatusCode.NOT_EXIST, notExist.getCode());
            }

            DeleteMetaGroupInfo deleteInfo = DeleteMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                    .build();
            // test delete meta group
            client.deleteMetaGroup(serviceId, deleteInfo);
            // test service not exist
            StarClientException thrown3 = Assert.assertThrows(StarClientException.class, () -> {
                client.deleteMetaGroup(serviceId + "aaa", deleteInfo);
            });
            Assert.assertEquals(StatusCode.NOT_EXIST, thrown3.getCode());

            // test get meta group
            StarClientException thrown4 = Assert.assertThrows(StarClientException.class, () -> {
                client.getMetaGroupInfo(serviceId + "aaa", 0);
            });
            Assert.assertEquals(StatusCode.NOT_EXIST, thrown4.getCode());

            // test list meta group
            StarClientException thrown5 = Assert.assertThrows(StarClientException.class, () -> {
                client.listMetaGroup(serviceId + "aaa");
            });
            Assert.assertEquals(StatusCode.NOT_EXIST, thrown5.getCode());
        } catch (StarClientException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testStarClientAllocateFilePath() {
        {

            try {
                client.registerService(serviceTemplateName);

                String serviceName = "StarClientTest-1";
                String serviceId = client.bootstrapService(serviceTemplateName, serviceName);
                // allocate by fstype
                FilePathInfo filePath = client.allocateFilePath(serviceId, FileStoreType.S3, "123");
                Assert.assertEquals(filePath.getFsInfo().getFsType(), FileStoreType.S3);
                // allocate by fskey
                filePath = client.allocateFilePath(serviceId, Constant.S3_FSKEY_FOR_CONFIG, "123");
                Assert.assertEquals(filePath.getFsInfo().getFsKey(), Constant.S3_FSKEY_FOR_CONFIG);
            } catch (StarClientException e) {
                Assert.assertNull(e);
            }
        }
    }

    @Test
    public void testCreateWorkerGroup() throws StarClientException {
        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-WorkerGroupCreation";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

        String owner = "TestOwner";
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("XL").build();
        Map<String, String> labels = ImmutableMap.of("Label1", "value1", "label2", "value2");
        Map<String, String> properties = ImmutableMap.of("prop1", "A", "prop2", "B");
        WorkerGroupDetailInfo result = null;
        int replicaNumber = 2;
        try {
            result = client.createWorkerGroup(serviceId, owner, spec, labels, properties, replicaNumber,
                    ReplicationType.ASYNC);
        } catch (StarClientException exception) {
            Assert.fail("Don't expect any exception throw");
        }
        Assert.assertEquals(serviceId, result.getServiceId());
        Assert.assertEquals(owner, result.getOwner());
        Assert.assertEquals(spec, result.getSpec());
        Assert.assertEquals(labels, result.getLabelsMap());
        Assert.assertEquals(properties, result.getPropertiesMap());
        Assert.assertEquals(WorkerGroupState.PENDING, result.getState());
        Assert.assertEquals(replicaNumber, result.getReplicaNumber());
        Assert.assertEquals(ReplicationType.ASYNC, result.getReplicationType());
        // default to WARMUP_NOTHING
        Assert.assertEquals(WarmupLevel.WARMUP_NOTHING, result.getWarmupLevel());

        try {
            result = client.createWorkerGroup(serviceId, owner, spec, labels, properties);
        } catch (StarClientException exception) {
            Assert.fail("Don't expect any exception throw");
        }
        Assert.assertEquals(serviceId, result.getServiceId());
        Assert.assertEquals(owner, result.getOwner());
        Assert.assertEquals(spec, result.getSpec());
        Assert.assertEquals(labels, result.getLabelsMap());
        Assert.assertEquals(properties, result.getPropertiesMap());
        Assert.assertEquals(WorkerGroupState.PENDING, result.getState());
        Assert.assertEquals(1, result.getReplicaNumber());

        // serviceId not exist
        StarClientException exception = Assert.assertThrows(StarClientException.class,
                () -> client.createWorkerGroup(serviceId + "xxx", owner, spec, labels, properties,
                        1 /* replicaNumber */, ReplicationType.SYNC));
        Assert.assertEquals(StatusCode.NOT_EXIST, exception.getCode());

        { // create worker group with a different warmup level
            WarmupLevel levelAll = WarmupLevel.WARMUP_ALL;
            WorkerGroupDetailInfo result2 = null;
            try {
                result2 = client.createWorkerGroup(serviceId, owner, spec, labels, properties, replicaNumber,
                        ReplicationType.ASYNC, levelAll);
            } catch (StarClientException e) {
                Assert.fail("Don't expect any exception throw");
            }
            Assert.assertEquals(levelAll, result2.getWarmupLevel());
        }
    }

    @Test
    public void testListWorkerGroups() throws StarClientException {
        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-ListWorkerGroupOperation";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);
        String owner = "TestOwner";
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("XL").build();

        Map<Long, WorkerGroupDetailInfo> expectedResult = new HashMap<>();

        { // create worker group1
            Map<String, String> labels = ImmutableMap.of("BU", "CTO", "group", "developer");
            Map<String, String> properties = ImmutableMap.of("prop1", "A", "prop2", "B");
            try {
                WorkerGroupDetailInfo result = client.createWorkerGroup(serviceId, owner, spec, labels, properties,
                        1 /* replicaNumber */, ReplicationType.ASYNC);
                expectedResult.put(result.getGroupId(), result);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
        }
        { // create worker group 2
            Map<String, String> labels = ImmutableMap.of("BU", "CTO", "group", "marketing");
            Map<String, String> properties = ImmutableMap.of("prop1", "X", "prop2", "Y");
            try {
                WorkerGroupDetailInfo result = client.createWorkerGroup(serviceId, owner, spec, labels, properties,
                        1 /* replicaNumber */, ReplicationType.ASYNC);
                expectedResult.put(result.getGroupId(), result);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
        }
        { // create worker group 3, with no labels
            try {
                WorkerGroupDetailInfo result = client.createWorkerGroup(serviceId, owner, spec, Collections.emptyMap(),
                        Collections.emptyMap(), 1 /* replicaNumber */, ReplicationType.ASYNC);
                expectedResult.put(result.getGroupId(), result);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
        }

        List<Long> groupIds = new ArrayList<>(expectedResult.keySet());
        // list by id
        {
            // list single
            try {
                List<WorkerGroupDetailInfo> results = client.listWorkerGroup(serviceId, groupIds.subList(0, 1), false);
                Assert.assertEquals(1L, results.size());
                Assert.assertEquals(expectedResult.get(groupIds.get(0)), results.get(0));
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }

            // list multiple
            try {
                List<WorkerGroupDetailInfo> results = client.listWorkerGroup(serviceId, groupIds.subList(0, 2), false);
                Assert.assertEquals(2L, results.size());
                results.forEach(x -> {
                    Assert.assertEquals(expectedResult.get(x.getGroupId()), x);
                });
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }

            // list non-exist group
            StarClientException ex = Assert.assertThrows(StarClientException.class,
                    () -> client.listWorkerGroup(serviceId, Collections.singletonList(13579L), true));
            Assert.assertEquals(StatusCode.NOT_EXIST, ex.getCode());
        }

        // list by labels
        {
            // matching single result
            try {
                List<WorkerGroupDetailInfo> results =
                        client.listWorkerGroup(serviceId, ImmutableMap.of("group", "developer"));
                Assert.assertEquals(1L, results.size());
                Assert.assertEquals(expectedResult.get(results.get(0).getGroupId()), results.get(0));
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }

            // matching multiple results
            try {
                List<WorkerGroupDetailInfo> results = client.listWorkerGroup(serviceId, ImmutableMap.of("BU", "CTO"));
                Assert.assertEquals(2L, results.size());
                results.forEach(x -> {
                    Assert.assertEquals(expectedResult.get(x.getGroupId()), x);
                });
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }

            // empty label means listing all
            try {
                List<WorkerGroupDetailInfo> results = client.listWorkerGroup(serviceId, Collections.emptyMap());
                if (Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY) {
                    // with one additional 0-group
                    Assert.assertEquals(expectedResult.size() + 1, results.size());
                } else {
                    Assert.assertEquals(expectedResult.size(), results.size());
                }
                results.forEach(x -> {
                    if (x.getGroupId() != 0) {
                        Assert.assertEquals(expectedResult.get(x.getGroupId()), x);
                    }
                });
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }

            // matching nothing
            StarClientException ex = Assert.assertThrows(StarClientException.class,
                    () -> client.listWorkerGroup(serviceId, ImmutableMap.of("BU", "cTO")));
            Assert.assertEquals(StatusCode.NOT_EXIST, ex.getCode());
        }

        { // list with worker info as well
            try {
                // get the first workerGroupId
                long workerGroupId = expectedResult.keySet().iterator().next();
                String ipPort = "127.0.0.1:17538";

                long workerId = client.addWorker(serviceId, ipPort, workerGroupId);
                List<WorkerGroupDetailInfo> results = client.listWorkerGroup(serviceId,
                        Collections.singletonList(workerGroupId), true);
                Assert.assertEquals(1L, results.size());
                WorkerGroupDetailInfo groupInfo = results.get(0);
                Assert.assertEquals(1L, groupInfo.getWorkersInfoCount());
                WorkerInfo workerInfo = groupInfo.getWorkersInfo(0);
                Assert.assertEquals(workerId, workerInfo.getWorkerId());
                Assert.assertEquals(workerGroupId, workerInfo.getGroupId());
                Assert.assertEquals(ipPort, workerInfo.getIpPort());
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception!");
            }
        }
    }

    @Test
    public void testUpdateWorkerGroupInfo() throws StarClientException {
        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-UpdateWorkerGroupOperation";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);
        String owner = "TestOwner";
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("XL").build();
        Map<String, String> labels = ImmutableMap.of("BU", "CTO", "group", "developer");
        Map<String, String> properties = ImmutableMap.of("prop1", "A", "prop2", "B");
        int replicaNumber = 3;

        WorkerGroupDetailInfo originGroup = null;
        { // create worker group
            try {
                originGroup = client.createWorkerGroup(serviceId, owner, spec, labels, properties, replicaNumber,
                        ReplicationType.SYNC);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
        }
        long groupId = originGroup.getGroupId();

        { // update properties
            WorkerGroupDetailInfo updatedInfo = null;
            Map<String, String> newProps = ImmutableMap.of("prop1", "AA");
            try {
                updatedInfo = client.updateWorkerGroup(serviceId, groupId, null, newProps, replicaNumber,
                        ReplicationType.NO_SET);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
            Assert.assertEquals(serviceId, updatedInfo.getServiceId());
            Assert.assertEquals(owner, updatedInfo.getOwner());
            Assert.assertEquals(spec, updatedInfo.getSpec());
            Assert.assertEquals(labels, updatedInfo.getLabelsMap());
            Assert.assertNotEquals(properties, updatedInfo.getPropertiesMap());
            Assert.assertEquals(newProps, updatedInfo.getPropertiesMap());
        }

        { // update label
            WorkerGroupDetailInfo updatedInfo = null;
            Map<String, String> newLabels = ImmutableMap.of("CXO", "XX");
            try {
                updatedInfo = client.updateWorkerGroup(serviceId, groupId, newLabels, properties, replicaNumber,
                        ReplicationType.NO_SET);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
            Assert.assertEquals(serviceId, updatedInfo.getServiceId());
            Assert.assertEquals(owner, updatedInfo.getOwner());
            Assert.assertEquals(spec, updatedInfo.getSpec());
            Assert.assertNotEquals(labels, updatedInfo.getLabelsMap());
            Assert.assertEquals(newLabels, updatedInfo.getLabelsMap());
            Assert.assertEquals(properties, updatedInfo.getPropertiesMap());
        }

        { // update spec
            WorkerGroupDetailInfo updatedInfo = null;
            WorkerGroupSpec newSpec = WorkerGroupSpec.newBuilder().setSize("XXXL").build();
            try {
                updatedInfo = client.alterWorkerGroupSpec(serviceId, groupId, newSpec);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
            Assert.assertEquals(serviceId, updatedInfo.getServiceId());
            Assert.assertEquals(owner, updatedInfo.getOwner());
            Assert.assertEquals(newSpec, updatedInfo.getSpec());
            Assert.assertNotEquals(spec, updatedInfo.getSpec());
        }
        { // update replica number and replication type
            WorkerGroupDetailInfo updatedInfo = null;
            int updateReplicaNumber = 2;
            try {
                updatedInfo =
                        client.updateWorkerGroup(serviceId, groupId, Collections.emptyMap(), Collections.emptyMap(),
                                updateReplicaNumber, ReplicationType.ASYNC);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
            Assert.assertEquals(serviceId, updatedInfo.getServiceId());
            Assert.assertEquals(updateReplicaNumber, updatedInfo.getReplicaNumber());
            Assert.assertEquals(ReplicationType.ASYNC, updatedInfo.getReplicationType());
        }
        { // update WarmupLevel
            WorkerGroupDetailInfo updatedInfo = null;
            WarmupLevel updateWarmupLevel = WarmupLevel.WARMUP_META;
            // before update, the warmup level is WARMUP_NOTHING
            Assert.assertEquals(WarmupLevel.WARMUP_NOTHING, originGroup.getWarmupLevel());
            try {
                updatedInfo =
                        client.updateWorkerGroup(serviceId, groupId, Collections.emptyMap(), Collections.emptyMap(),
                                0, ReplicationType.NO_SET, updateWarmupLevel);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
            Assert.assertEquals(serviceId, updatedInfo.getServiceId());
            Assert.assertEquals(originGroup.getGroupId(), updatedInfo.getGroupId());
            // after the update, the warmup level set to WARMUP_META
            Assert.assertEquals(updateWarmupLevel, updatedInfo.getWarmupLevel());
        }
    }

    @Test
    public void testDeleteWorkerGroup() throws StarClientException {
        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-UpdateWorkerGroupOperation";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);
        String owner = "TestOwner";
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("XL").build();
        Map<String, String> labels = ImmutableMap.of("BU", "CTO", "group", "developer");
        Map<String, String> properties = ImmutableMap.of("prop1", "A", "prop2", "B");

        WorkerGroupDetailInfo originGroup = null;
        { // create worker group
            try {
                originGroup =
                        client.createWorkerGroup(serviceId, owner, spec, labels, properties, 1 /* replicaNumber */,
                                ReplicationType.SYNC);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
        }
        long groupId = originGroup.getGroupId();
        { // Can list the worker group correctly
            try {
                List<WorkerGroupDetailInfo> results =
                        client.listWorkerGroup(serviceId, Collections.singletonList(groupId), false);
                Assert.assertEquals(1L, results.size());
                Assert.assertEquals(originGroup, results.get(0));
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
        }
        { // Delete it
            try {
                client.deleteWorkerGroup(serviceId, groupId);
            } catch (StarClientException exception) {
                Assert.fail("Don't expect any exception throw");
            }
        }
        { // List again but not exist
            StarClientException ex = Assert.assertThrows(StarClientException.class,
                    () -> client.listWorkerGroup(serviceId, Collections.singletonList(groupId), true));
            Assert.assertEquals(StatusCode.NOT_EXIST, ex.getCode());
        }
    }

    @Test
    public void testStarClientFileStore() throws StarClientException {
        AwsCredentialInfo.Builder awsCredentialInfo = AwsCredentialInfo.newBuilder();
        AwsDefaultCredentialInfo.Builder defaultCredentialInfo = AwsDefaultCredentialInfo.newBuilder();
        awsCredentialInfo.setDefaultCredential(defaultCredentialInfo.build());
        S3FileStoreInfo s3FileStoreInfo = S3FileStoreInfo.newBuilder().setBucket("test-bucket")
                .setRegion("region").setEndpoint("endpoint").setPathPrefix("prefix")
                .setCredential(awsCredentialInfo).build();

        Map<String, String> properties = new HashMap<>();
        properties.put("property1", "value1");
        List<String> locations = Arrays.asList("s3://test-bucket/prefix");
        final FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsType(FileStoreType.S3)
                .setFsName("test-name")
                .setS3FsInfo(s3FileStoreInfo)
                .addAllLocations(locations)
                .putAllProperties(properties).build();
        { // Can not add file store
            StarClientException ex =
                    Assert.assertThrows(StarClientException.class, () -> client.addFileStore(info, "0"));
            Assert.assertEquals(StatusCode.NOT_EXIST, ex.getCode());
        }

        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-UpdateWorkerGroupOperation";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

        {
            // "test-name" can not be empty.
            StarClientException ex = Assert.assertThrows(StarClientException.class,
                    () -> client.getFileStoreByName("", serviceId));
            Assert.assertEquals(StatusCode.INVALID_ARGUMENT, ex.getCode());
        }

        {
            // "test-name" not exists.
            StarClientException ex = Assert.assertThrows(StarClientException.class,
                    () -> client.getFileStoreByName("test-name", serviceId));
            Assert.assertEquals(StatusCode.NOT_EXIST, ex.getCode());
        }

        {
            // Can not add file store which does not have fsname
            final FileStoreInfo missingFsNameInfo = FileStoreInfo.newBuilder()
                    .setFsType(FileStoreType.S3)
                    .setS3FsInfo(s3FileStoreInfo).build();
            StarClientException ex =
                    Assert.assertThrows(StarClientException.class, () -> client.addFileStore(missingFsNameInfo, "0"));
            Assert.assertEquals(StatusCode.INVALID_ARGUMENT, ex.getCode());
        }

        {
            // Can not add s3 file store which has empty bucket
            final S3FileStoreInfo s3FileStoreInfo1 = s3FileStoreInfo.toBuilder().setBucket("test-bucket").build();
            final FileStoreInfo fsInfo1 = FileStoreInfo.newBuilder()
                    .setFsType(FileStoreType.S3)
                    .setS3FsInfo(s3FileStoreInfo).build();
            StarClientException ex =
                    Assert.assertThrows(StarClientException.class, () -> client.addFileStore(fsInfo1, "0"));
            Assert.assertEquals(StatusCode.INVALID_ARGUMENT, ex.getCode());
        }

        String fsKey = null;
        { // Can add file store
            try {
                fsKey = client.addFileStore(info, serviceId);
            } catch (StarClientException e) {
                Assert.assertNull(e);
            }
            Assert.assertNotNull(fsKey);
        }

        {
            // "test name" exists.
            try {
                FileStoreInfo fsInfo = client.getFileStoreByName(info.getFsName(), serviceId);
                Assert.assertEquals(fsKey, fsInfo.getFsKey());
                Assert.assertNotNull(fsInfo.getPropertiesMap());
                Assert.assertEquals("value1", fsInfo.getPropertiesMap().get("property1"));
            } catch (StarClientException e) {
                Assert.assertNull(e);
            }
        }

        {
            try {
                FileStoreInfo fsInfo = client.getFileStore(fsKey, serviceId);
                Assert.assertEquals(fsKey, fsInfo.getFsKey());
            } catch (StarClientException e) {
                Assert.assertNull(e);
            }
        }

        { // File store can be added and return different fsKey;
            String fsKey1 = null;
            List<FileStoreInfo> fileStoreInfos = new ArrayList<>();
            try {
                fsKey1 = client.addFileStore(info, serviceId);
                fileStoreInfos = client.listFileStore(serviceId);
            } catch (StarClientException e) {
                Assert.assertNull(e);
            }
            Assert.assertNotEquals(fsKey, fsKey1);
            // 2 configured fskey + 2fskey.
            Assert.assertEquals(4, fileStoreInfos.size());

            try {
                fileStoreInfos = client.listFileStore(serviceId, FileStoreType.S3);
            } catch (StarClientException e) {
                Assert.assertNull(e);
            }
            Assert.assertEquals(3, fileStoreInfos.size());
        }

        locations = new ArrayList<>(Arrays.asList("hdfs://url"));
        HDFSFileStoreInfo hdfs = HDFSFileStoreInfo.newBuilder().setUrl("url").build();
        final FileStoreInfo info1 = FileStoreInfo.newBuilder().setFsType(FileStoreType.HDFS).setFsName("test-hdfs")
                .setFsKey("url").setHdfsFsInfo(hdfs).addAllLocations(locations).build();
        { // Non-exist file store can not be updated
            StarClientException ex = Assert.assertThrows(StarClientException.class,
                    () -> client.updateFileStore(info1, serviceId));
            Assert.assertEquals(StatusCode.NOT_EXIST, ex.getCode());
        }

        { // Non-exist file store can not be replaced
            StarClientException ex = Assert.assertThrows(StarClientException.class,
                    () -> client.replaceFileStore(info1, serviceId));
            Assert.assertEquals(StatusCode.NOT_EXIST, ex.getCode());
        }

        s3FileStoreInfo = S3FileStoreInfo.newBuilder().setBucket("test-bucket")
                .setRegion("region1").setEndpoint("endpoint").setPathPrefix("prefix")
                .setCredential(awsCredentialInfo).build();
        final FileStoreInfo info2 = FileStoreInfo.newBuilder()
                .setFsType(FileStoreType.S3)
                .setFsKey(fsKey)
                .setFsName("test-name")
                .setS3FsInfo(s3FileStoreInfo)
                .addAllLocations(locations).build();
        { // File store can be updated
            try {
                client.updateFileStore(info2, serviceId);
            } catch (StarClientException e) {
                Assert.assertNull(e);
            }
        }

        { // File store can be replaced
            try {
                client.replaceFileStore(info2, serviceId);
            } catch (StarClientException e) {
                Assert.assertNull(e);
            }
        }

        { // Not-exist file store can not be removed
            StarClientException ex = Assert.assertThrows(StarClientException.class,
                    () -> client.removeFileStoreByName(info1.getFsName(), serviceId));
            Assert.assertEquals(StatusCode.NOT_EXIST, ex.getCode());
        }

        { // Remove successfully
            try {
                client.removeFileStoreByName(info2.getFsName(), serviceId);
            } catch (StarClientException e) {
                Assert.assertNull(e);
            }
        }
    }

    @Test
    public void testFollowerDoNotSendWriteToLeader() {
        startFollower();
        StarClient myClient = new StarClient();
        // connect to follower, no known of leader's location
        myClient.connectServer(followerIpPort);
        String template = "xyz-not-exist";
        StarClientException exception =
                Assert.assertThrows(StarClientException.class, () -> myClient.registerService(template));
        Assert.assertEquals(StatusCode.NOT_LEADER, exception.getCode());

        // now let follower knows then leader address
        LeaderInfo info = LeaderInfo.newBuilder()
                .setHost(serverIpPort.split(":")[0])
                .setPort(Integer.parseInt(serverIpPort.split(":")[1]))
                .build();
        follower.getStarManager().replayLeaderChange(info);

        // write operation do not send to leader
        exception = Assert.assertThrows(StarClientException.class, () -> myClient.registerService(template));
        Assert.assertEquals(StatusCode.NOT_LEADER, exception.getCode());

        myClient.stop();
    }

    @Test
    public void testAllocateFilePathInfoNoPartitionedPrefix() {
        String pathSuffix = "user_suffix";
        String bucket = "test-bucket";
        String pathPrefix = "path_prefix";

        S3FileStoreInfo.Builder s3builder = S3FileStoreInfo.newBuilder()
                .setBucket(bucket)
                .setEndpoint("http://localhost")
                .setPathPrefix(pathPrefix);

        FileStoreInfo.Builder fsBuilder = FileStoreInfo.newBuilder()
                .setS3FsInfo(s3builder)
                .setFsKey("fskey")
                .setFsName("name")
                .setFsType(FileStoreType.S3);

        FilePathInfo fsInfo = FilePathInfo.newBuilder()
                .setFsInfo(fsBuilder)
                .setFullPath(String.format("s3://%s/%s/%s", bucket, pathPrefix, pathSuffix))
                .build();

        {
            int intValue = 48346; // 0xbcda
            String allocPath = StarClient.allocateFilePath(fsInfo, intValue);
            // no change
            Assert.assertEquals(fsInfo.getFullPath(), allocPath);
        }

        {
            long longValue = 1234567890;
            String allocPath = StarClient.allocateFilePath(fsInfo, Long.hashCode(longValue));
            // no change
            Assert.assertEquals(fsInfo.getFullPath(), allocPath);
        }

        {
            String stringValue = "bcda";
            String allocPath = StarClient.allocateFilePath(fsInfo, stringValue.hashCode());
            // no change
            Assert.assertEquals(fsInfo.getFullPath(), allocPath);
        }
    }

    private String calcPrefix(int hashCode, int module) {
        // NOTE: make sure this implementation matches the implementation in `StarClient.allocateFilePathInfo()`
        StringBuilder stringBuilder = new StringBuilder(Integer.toHexString(hashCode % module));
        stringBuilder.reverse();
        return stringBuilder.toString();
    }

    @Test
    public void testAllocateFilePathInfoWithPartitionedPrefix() {
        String pathSuffix = "user_suffix";
        String bucket = "test-bucket";
        // must be empty for file store that enables partitioned prefix
        String pathPrefix = "";
        int numPartitionedPrefix = 128;

        S3FileStoreInfo.Builder s3builder = S3FileStoreInfo.newBuilder()
                .setBucket(bucket)
                .setEndpoint("http://localhost")
                .setPathPrefix(pathPrefix)
                .setPartitionedPrefixEnabled(true)
                .setNumPartitionedPrefix(numPartitionedPrefix);

        FileStoreInfo.Builder fsBuilder = FileStoreInfo.newBuilder()
                .setS3FsInfo(s3builder)
                .setFsKey("fskey")
                .setFsName("name")
                .setFsType(FileStoreType.S3);

        FilePathInfo fsInfo = FilePathInfo.newBuilder()
                .setFsInfo(fsBuilder)
                .setFullPath(String.format("s3://%s/%s", bucket, pathSuffix))
                .build();

        {
            int intValue = 48346; // 0xbcda
            String allocatePath = StarClient.allocateFilePath(fsInfo, intValue);
            String partitionedStr = calcPrefix(intValue, numPartitionedPrefix);
            String expectedFullPath = String.format("s3://%s/%s/%s", bucket, partitionedStr, pathSuffix);
            Assert.assertEquals(expectedFullPath, allocatePath);
        }

        {
            long longValue = 1234567890;
            String allocPath = StarClient.allocateFilePath(fsInfo, Long.hashCode(longValue));
            String partitionedStr = calcPrefix(Long.hashCode(longValue), numPartitionedPrefix);
            String expectedFullPath = String.format("s3://%s/%s/%s", bucket, partitionedStr, pathSuffix);
            Assert.assertEquals(expectedFullPath, allocPath);
        }

        {
            String stringValue = "bcda";
            String allocPath = StarClient.allocateFilePath(fsInfo, stringValue.hashCode());
            String partitionedStr = calcPrefix(stringValue.hashCode(), numPartitionedPrefix);
            String expectedFullPath = String.format("s3://%s/%s/%s", bucket, partitionedStr, pathSuffix);
            Assert.assertEquals(expectedFullPath, allocPath);
        }
    }

    @Test
    public void testUpdateHdfsFileStore() throws StarClientException {
        HDFSFileStoreInfo hdfsInfo = HDFSFileStoreInfo.newBuilder()
                .setUsername("starrocks")
                .setUrl("hdfs://localhost:7090/starrocks")
                .putConfiguration("hadoop.security.authentication", "simple")
                .putConfiguration("hadoop.client.abc", "123")
                .build();

        FileStoreInfo fsInfo = FileStoreInfo.newBuilder()
                .setFsName("hdfs-test-name")
                .setHdfsFsInfo(hdfsInfo)
                .setFsType(FileStoreType.HDFS)
                .build();

        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-testUpdateHdfsFileStore";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

        String fsKey = client.addFileStore(fsInfo, serviceId);
        {
            FileStoreInfo getInfo = client.getFileStore(fsKey, serviceId);
            Assert.assertEquals(getInfo.getHdfsFsInfo(), hdfsInfo);
        }

        { // update the hdfsInfo
            HDFSFileStoreInfo newHdfsInfo = HDFSFileStoreInfo.newBuilder()
                    .setUsername("root")
                    .putConfiguration("hadoop.security.authentication", "kerberos")
                    .putConfiguration("hadoop.ipc.retry", "3")
                    .build();

            FileStoreInfo updateInfo = fsInfo.toBuilder().setFsKey(fsKey).setHdfsFsInfo(newHdfsInfo).build();
            client.updateFileStore(updateInfo, serviceId);

            // get info again,  the info should be updated
            FileStoreInfo getInfo = client.getFileStore(fsKey, serviceId);
            HDFSFileStoreInfo getHdfsInfo = getInfo.getHdfsFsInfo();
            Assert.assertEquals(newHdfsInfo.getUsername(), getHdfsInfo.getUsername());
            Map<String, String> configMap = getHdfsInfo.getConfigurationMap();
            // add 1 item, replace one item, total 3 items
            Assert.assertEquals(3L, configMap.size());
            // an old item
            Assert.assertEquals("123", configMap.get("hadoop.client.abc"));
            // a new added item
            Assert.assertEquals("3", configMap.get("hadoop.ipc.retry"));
            // a replaced item
            Assert.assertEquals("kerberos", configMap.get("hadoop.security.authentication"));
        }
    }

    @Test
    public void testUpdateAzblobFileStore() throws StarClientException {
        AzBlobCredentialInfo credInfo = AzBlobCredentialInfo.newBuilder()
                .setClientId("clientId1")
                .setClientSecret("secret1")
                .build();

        AzBlobFileStoreInfo azFsInfo = AzBlobFileStoreInfo.newBuilder()
                .setEndpoint("https://server-endpoint")
                .setCredential(credInfo)
                .build();

        FileStoreInfo fsInfo = FileStoreInfo.newBuilder()
                .setFsName("azblob-test-name")
                .setAzblobFsInfo(azFsInfo)
                .setFsType(FileStoreType.AZBLOB)
                .addLocations("azblob://bucket/path")
                .build();

        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-testUpdateAzblobFileStore";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

        String fsKey = client.addFileStore(fsInfo, serviceId);
        {
            FileStoreInfo getInfo = client.getFileStore(fsKey, serviceId);
            Assert.assertEquals("bucket/path", getInfo.getAzblobFsInfo().getPath());
            // reset the path field
            Assert.assertEquals(getInfo.getAzblobFsInfo().toBuilder().clearPath().build(), azFsInfo);
        }

        { // update the azFsInfo
            AzBlobCredentialInfo newCredInfo = AzBlobCredentialInfo.newBuilder()
                    .setSharedKey("sharedkey2")
                    .setClientSecret("secret2")
                    .build();

            AzBlobFileStoreInfo newAzFsInfo = AzBlobFileStoreInfo.newBuilder()
                    .setEndpoint("azblob://new-endpoint")
                    .setCredential(newCredInfo)
                    .build();

            FileStoreInfo updateInfo = fsInfo.toBuilder().setFsKey(fsKey).setAzblobFsInfo(newAzFsInfo).build();
            client.updateFileStore(updateInfo, serviceId);

            // get info again,  the info should be updated
            FileStoreInfo getInfo = client.getFileStore(fsKey, serviceId);
            AzBlobFileStoreInfo getAzFsInfo = getInfo.getAzblobFsInfo();
            // the endpoint is updated
            Assert.assertEquals(newAzFsInfo.getEndpoint(), getAzFsInfo.getEndpoint());
            // credentials are overwritten as a whole entity
            Assert.assertEquals(newAzFsInfo.getCredential(), newCredInfo);
        }
    }

    @Test
    public void testUpdateADLS2FileStore() throws StarClientException {
        ADLS2CredentialInfo credInfo = ADLS2CredentialInfo.newBuilder()
                .setClientId("clientId1")
                .setClientSecret("secret1")
                .build();

        ADLS2FileStoreInfo azFsInfo = ADLS2FileStoreInfo.newBuilder()
                .setEndpoint("https://server-endpoint")
                .setCredential(credInfo)
                .build();

        FileStoreInfo fsInfo = FileStoreInfo.newBuilder()
                .setFsName("adls2-test-name")
                .setAdls2FsInfo(azFsInfo)
                .setFsType(FileStoreType.ADLS2)
                .addLocations("adls2://bucket/path")
                .build();

        client.registerService(serviceTemplateName);
        String serviceName = "StarClientTest-testUpdateADLS2FileStore";
        String serviceId = client.bootstrapService(serviceTemplateName, serviceName);

        String fsKey = client.addFileStore(fsInfo, serviceId);
        {
            FileStoreInfo getInfo = client.getFileStore(fsKey, serviceId);
            Assert.assertEquals("bucket/path", getInfo.getAdls2FsInfo().getPath());
            // reset the path field
            Assert.assertEquals(getInfo.getAdls2FsInfo().toBuilder().clearPath().build(), azFsInfo);
        }

        { // update the azFsInfo
            ADLS2CredentialInfo newCredInfo = ADLS2CredentialInfo.newBuilder()
                    .setSharedKey("sharedkey2")
                    .setClientSecret("secret2")
                    .build();

            ADLS2FileStoreInfo newAzFsInfo = ADLS2FileStoreInfo.newBuilder()
                    .setEndpoint("adls2://new-endpoint")
                    .setCredential(newCredInfo)
                    .build();

            FileStoreInfo updateInfo = fsInfo.toBuilder().setFsKey(fsKey).setAdls2FsInfo(newAzFsInfo).build();
            client.updateFileStore(updateInfo, serviceId);

            // get info again, the info should be updated
            FileStoreInfo getInfo = client.getFileStore(fsKey, serviceId);
            ADLS2FileStoreInfo getAzFsInfo = getInfo.getAdls2FsInfo();
            // the endpoint is updated
            Assert.assertEquals(newAzFsInfo.getEndpoint(), getAzFsInfo.getEndpoint());
            // credentials are overwritten as a whole entity
            Assert.assertEquals(newAzFsInfo.getCredential(), newCredInfo);
        }
    }

    @Test
    public void testReadTimeout() {
        String serviceId = "timeout";
        List<ShardInfo> shardInfos = new ArrayList<>();
        int shardCount = 2;

        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            String ipPort = "127.0.0.1:1234";
            long workerId = client.addWorker(serviceId, ipPort);

            List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                builder.setReplicaCount(1);
                createShardInfos.add(builder.build());
            }
            shardInfos = client.createShard(serviceId, createShardInfos);
            Assert.assertEquals(shardInfos.size(), shardCount);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        List<Long> shardIds = new ArrayList<>();
        for (ShardInfo shardInfo : shardInfos) {
            shardIds.add(shardInfo.getShardId());
        }
        final String serviceIdCopy = serviceId;
        final List<Long> shardIdsCopy = shardIds;

        {
            client.stop();
            client = new StarClient();
            client.setClientReadTimeoutSec(1);
            client.setClientListTimeoutSec(1);
            client.setClientReadMaxRetryCount(0);
            client.connectServer(serverIpPort);

            new MockUp<StarManager>() {
                @Mock
                public List<ShardInfo> getShardInfo(String serviceId, List<Long> shardIds, long workerGroupId)
                        throws StarException, InterruptedException {
                    Thread.sleep(2000);
                    return new ArrayList<ShardInfo>();
                }

                @Mock
                public List<ShardInfoList> listShardInfo(String serviceId, List<Long> groupIds, long workerGroupId,
                                                         boolean withoutReplicaInfo)
                        throws StarException, InterruptedException {
                    Thread.sleep(2000);
                    return new ArrayList<ShardInfoList>();
                }
            };

            StarClientException exception = Assert.assertThrows(StarClientException.class, () -> client.getShardInfo(
                    serviceIdCopy, shardIdsCopy));
            Assert.assertEquals(exception.getCode(), StatusCode.GRPC);
            Assert.assertTrue(exception.getMessage().contains("DEADLINE_EXCEEDED"));

            List<Long> groupIds = new ArrayList<>();
            groupIds.add(100L);
            StarClientException exception2 = Assert.assertThrows(StarClientException.class, () -> client.listShard(
                    serviceIdCopy, groupIds));
            Assert.assertEquals(exception2.getCode(), StatusCode.GRPC);
            Assert.assertTrue(exception2.getMessage().contains("DEADLINE_EXCEEDED"));
        }
    }

    @Test
    public void testRetry() {
        String serviceId = "timeout";
        List<ShardInfo> shardInfos = new ArrayList<>();
        int shardCount = 2;

        try {
            client.registerService(serviceTemplateName);

            String serviceName = "StarClientTest-1";
            serviceId = client.bootstrapService(serviceTemplateName, serviceName);

            String ipPort = "127.0.0.1:1234";
            long workerId = client.addWorker(serviceId, ipPort);

            List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                builder.setReplicaCount(1);
                createShardInfos.add(builder.build());
            }
            shardInfos = client.createShard(serviceId, createShardInfos);
            Assert.assertEquals(shardInfos.size(), shardCount);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        List<Long> shardIds = new ArrayList<>();
        for (ShardInfo shardInfo : shardInfos) {
            shardIds.add(shardInfo.getShardId());
        }

        try {
            client.getShardInfo(serviceId, shardIds);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }

        client.setClientReadMaxRetryCount(10);

        try {
            client.getShardInfo(serviceId, shardIds);
        } catch (StarClientException e) {
            Assert.assertNull(e);
        }
    }
}
