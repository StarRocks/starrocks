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

package com.staros.worker;

import com.staros.common.HijackConfig;
import com.staros.common.TestHelper;
import com.staros.common.TestUtils;
import com.staros.exception.ExceptionCode;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.proto.AddResourceNodeRequest;
import com.staros.proto.AddResourceNodeResponse;
import com.staros.proto.ReplicationType;
import com.staros.proto.TestResourceManageServiceGrpc;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerGroupState;
import com.staros.provisioner.StarProvisionServer;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Leverage WorkerManager's workerGroup related operation to trigger DefaultResourceManager method call.
 */
public class DefaultResourceManagerTest {
    private static final Logger LOG = LogManager.getLogger(DefaultResourceManagerTest.class);
    private static Thread serverThread;
    private static int serverPort = -1;
    private static StarProvisionServer provisionServer;
    private static Server rpcServer;
    private static HijackConfig hijackConfigVar;

    private TestResourceManageServiceGrpc.TestResourceManageServiceBlockingStub mgrRpcClient;
    private ManagedChannel channel;
    private WorkerManager workerManager;
    private String serviceId;

    @BeforeClass
    public static void setUpForClass() {
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
        startServer();
        hijackConfigVar = new HijackConfig("RESOURCE_PROVISIONER_ADDRESS", String.format("127.0.0.1:%d", serverPort));
    }

    @AfterClass
    public static void tearDownForClass() {
        hijackConfigVar.reset();
        stopServer();
    }

    @Before
    public void setUp() {
        workerManager = WorkerManager.createWorkerManagerForTest(null);
        workerManager.start();
        serviceId = this.getClass().getName() + "-serviceId";
        workerManager.bootstrapService(serviceId);

        channel = ManagedChannelBuilder.forTarget(Config.RESOURCE_PROVISIONER_ADDRESS).usePlaintext().build();
        mgrRpcClient = TestResourceManageServiceGrpc.newBlockingStub(channel);
        provisionServer.clear();
    }

    @After
    public void tearDown() {
        channel.shutdownNow();
        workerManager.stop();
    }

    private static void startServer() {
        provisionServer = new StarProvisionServer();
        ServerBuilder<?> builder = ServerBuilder.forPort(0);
        StarProvisionServer.getServices(provisionServer).forEach(builder::addService);
        rpcServer = builder.build();

        serverThread = new Thread(() -> {
            try {
                rpcServer.start();
                serverPort = rpcServer.getPort();
                LOG.info("Starting StarProvisionServer on port " + serverPort + " ...");
                rpcServer.awaitTermination();
            } catch (Exception e) {
                LOG.error("Fail to start testing resource provisioner service!", e);
            }
        });
        serverThread.start();
        // wait until the serverPort is available
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> serverPort != -1L);
    }

    private static void stopServer() {
        rpcServer.shutdownNow();
        try {
            serverThread.join();
        } catch (InterruptedException exception) {
            // do nothing
        }
    }

    @Test
    public void testStartTestProvisionerService() {
        int expectFree = 0;
        // size: "invalidSize"
        Assert.assertThrows(InvalidArgumentStarException.class, () -> createTestWorkerGroupBySpec("invalidSize"));
        { // size: "x1", NotEnoughResource
            Assert.assertEquals(0L, provisionServer.getFreeNodeCount());
            Assert.assertEquals(0L, provisionServer.getAssignedPoolsSize());
            long workerGroupId = createTestWorkerGroupBySpec("x1");
            waitWorkerManagerExecutorIdle();
            Assert.assertEquals(0L, provisionServer.getAssignedPoolsSize());
            Assert.assertEquals(WorkerGroupState.PENDING, workerManager.getWorkerGroup(serviceId, workerGroupId).getState());
        }
        // Add 6 free nodes
        addNodes(6);
        expectFree += 6;
        Assert.assertEquals(0L, provisionServer.getAssignedPoolsSize());
        Assert.assertEquals(expectFree, provisionServer.getFreeNodeCount());
        { // size: "x2", good
            long workerGroupId = createTestWorkerGroupBySpec("x2");
            expectFree -= 2;
            waitWorkerManagerExecutorIdle();
            // Two nodes are assigned to the new workerGroup
            Assert.assertEquals(1L, provisionServer.getAssignedPoolsSize());
            Assert.assertEquals(expectFree, provisionServer.getFreeNodeCount());
            Assert.assertEquals(2, workerManager.getWorkerGroup(serviceId, workerGroupId).getWorkerCount());
            Assert.assertEquals(WorkerGroupState.READY, workerManager.getWorkerGroup(serviceId, workerGroupId).getState());

            // Scale-out x2 -> x4
            // alter workerGroup spec from x2 -> x4
            workerManager.updateWorkerGroup(serviceId, workerGroupId,
                    WorkerGroupSpec.newBuilder().setSize("x4").build(), null, null, 1 /* replicaNumber */,
                    ReplicationType.NO_SET, WarmupLevel.WARMUP_NOT_SET);
            expectFree -= 2;
            waitWorkerManagerExecutorIdle();
            // Two nodes are assigned to the new workerGroup
            Assert.assertEquals(1L, provisionServer.getAssignedPoolsSize());
            // assigned additional 2 nodes, 2 nodes remain in free
            Assert.assertEquals(expectFree, provisionServer.getFreeNodeCount());
            Assert.assertEquals(WorkerGroupState.READY, workerManager.getWorkerGroup(serviceId, workerGroupId).getState());
            Assert.assertEquals(4, workerManager.getWorkerGroup(serviceId, workerGroupId).getWorkerCount());

            // scale-out x4 -> x8, NotEnoughResource
            workerManager.updateWorkerGroup(serviceId, workerGroupId,
                    WorkerGroupSpec.newBuilder().setSize("x8").build(), null, null, 1 /* replicaNumber */,
                    ReplicationType.NO_SET, WarmupLevel.WARMUP_NOT_SET);
            waitWorkerManagerExecutorIdle();
            // Two nodes are assigned to the new workerGroup
            Assert.assertEquals(1L, provisionServer.getAssignedPoolsSize());
            Assert.assertEquals(expectFree, provisionServer.getFreeNodeCount());
            Assert.assertEquals(WorkerGroupState.PENDING, workerManager.getWorkerGroup(serviceId, workerGroupId).getState());
            // worker node number not changed
            Assert.assertEquals(4, workerManager.getWorkerGroup(serviceId, workerGroupId).getWorkerCount());

            // Scale-in x4 -> x1
            workerManager.updateWorkerGroup(serviceId, workerGroupId,
                    WorkerGroupSpec.newBuilder().setSize("x1").build(), null, null, 1 /* replicaNumber */,
                    ReplicationType.NO_SET, WarmupLevel.WARMUP_NOT_SET);
            expectFree += 3;
            waitWorkerManagerExecutorIdle();
            // Two nodes are assigned to the new workerGroup
            Assert.assertEquals(1L, provisionServer.getAssignedPoolsSize());
            Assert.assertEquals(expectFree, provisionServer.getFreeNodeCount());
            Assert.assertEquals(WorkerGroupState.READY, workerManager.getWorkerGroup(serviceId, workerGroupId).getState());
            Assert.assertEquals(1, workerManager.getWorkerGroup(serviceId, workerGroupId).getWorkerCount());

            // create a second workerGroup with x4
            long workerGroupId2 = createTestWorkerGroupBySpec("x4");
            expectFree -= 4;
            waitWorkerManagerExecutorIdle();
            // Two nodes are assigned to the new workerGroup
            Assert.assertEquals(2L, provisionServer.getAssignedPoolsSize());
            Assert.assertEquals(expectFree, provisionServer.getFreeNodeCount());
            Assert.assertEquals(4, workerManager.getWorkerGroup(serviceId, workerGroupId2).getWorkerCount());
            Assert.assertEquals(WorkerGroupState.READY, workerManager.getWorkerGroup(serviceId, workerGroupId).getState());

            // "x1"
            workerManager.deleteWorkerGroup(serviceId, workerGroupId);
            expectFree += 1;
            waitWorkerManagerExecutorIdle();
            Assert.assertEquals(1L, provisionServer.getAssignedPoolsSize());
            Assert.assertEquals(expectFree, provisionServer.getFreeNodeCount());

            // "x4"
            workerManager.deleteWorkerGroup(serviceId, workerGroupId2);
            expectFree += 4;
            waitWorkerManagerExecutorIdle();
            Assert.assertEquals(0L, provisionServer.getAssignedPoolsSize());
            Assert.assertEquals(expectFree, provisionServer.getFreeNodeCount());
        }
    }

    @Test
    public void repeatAddSameNodeError() {
        String node = TestHelper.generateMockWorkerIpAddress();
        AddResourceNodeRequest request = AddResourceNodeRequest.newBuilder()
                .setHost(node)
                .build();
        { // Add node
            AddResourceNodeResponse response = mgrRpcClient.addNode(request);
            Assert.assertEquals(0L, response.getStatus().getCode());
            Assert.assertEquals(1, provisionServer.getFreeNodeCount());
        }
        { // Add again. Failed
            AddResourceNodeResponse response = mgrRpcClient.addNode(request);
            Assert.assertEquals(ExceptionCode.ALREADY_EXIST.ordinal(), response.getStatus().getCode());
            Assert.assertEquals(1, provisionServer.getFreeNodeCount());
        }
    }

    private long createTestWorkerGroupBySpec(String size) {
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize(size).build();
        return workerManager.createWorkerGroup(serviceId, "TestOwner", spec, null, null, 1 /* replicaNumber */,
                ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);
    }

    private void addNodes(int n) {
        for (int i = 0; i < n; ++i) {
            AddResourceNodeRequest request = AddResourceNodeRequest.newBuilder()
                    .setHost(TestHelper.generateMockWorkerIpAddress())
                    .build();
            AddResourceNodeResponse response = mgrRpcClient.addNode(request);
            Assert.assertEquals(0L, response.getStatus().getCode());
        }
    }

    private void waitWorkerManagerExecutorIdle() {
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> TestUtils.workerManagerExecutorsIdle(workerManager));
    }
}
