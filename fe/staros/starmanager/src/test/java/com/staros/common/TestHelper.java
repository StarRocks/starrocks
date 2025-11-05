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


package com.staros.common;

import com.staros.journal.DummyJournalSystem;
import com.staros.journal.JournalSystem;
import com.staros.manager.StarManager;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.MetaGroupInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ReplicationType;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerState;
import com.staros.service.ServiceManager;
import com.staros.shard.Shard;
import com.staros.shard.ShardManager;
import com.staros.starlet.StarletAgent;
import com.staros.util.IdGenerator;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import com.staros.worker.WorkerManager;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestHelper {
    private static final Logger LOG = LogManager.getLogger(TestHelper.class);

    private final ServiceManager serviceManager;
    private final WorkerManager workerManager;
    private MockResourceManager resourceManager;
    private static int mockWorkerPort = 59000;

    private String defaultServiceId = null;
    private long defaultWorkerGroupId = 0;
    private String serviceTemplateName;

    public TestHelper(String serviceTemplateName) {
        JournalSystem journal = new DummyJournalSystem();
        IdGenerator generator = new IdGenerator(journal);
        this.serviceTemplateName = serviceTemplateName;
        this.serviceManager = new ServiceManager(journal, generator);
        this.workerManager = new WorkerManager(journal, generator);
        commonInitialization();
        this.workerManager.start();
    }

    public TestHelper(String serviceTemplateName, StarManager starManager) {
        this.serviceTemplateName = serviceTemplateName;
        this.serviceManager = starManager.getServiceManager();
        this.workerManager = starManager.getWorkerManager();
        commonInitialization();
        // turn off auto provision by default
        turnOffWorkerGroupAutoProvision();
    }

    public void stop() {
        this.workerManager.stop();
    }

    public void commonInitialization() {
        this.resourceManager = new MockResourceManager(this.workerManager);
        try {
            // Use REFLECTION to replace workerManager.resourceManager with our MockResourceManager
            FieldUtils.writeField(workerManager, "resourceManager", resourceManager, true);
        } catch (IllegalAccessException exception) {
            LOG.error("Fail to use reflection to set MockResourceManager to WorkerManager in TestHelper!");
        }

        if (serviceTemplateName.isEmpty()) {
            serviceTemplateName = this.getClass().getName() + "-serviceTemplateName-0";
        }
        this.serviceManager.registerService(this.serviceTemplateName, null);
    }

    public void turnOnWorkerGroupAutoProvision() {
        resourceManager.setAutoProvision(true);
    }

    public void turnOffWorkerGroupAutoProvision() {
        resourceManager.setAutoProvision(false);
    }

    public void setDefaultWorkerGroupReplicaNumber(int replicaNumber) {
        WorkerGroup wg = workerManager.getWorkerGroup(defaultServiceId, defaultWorkerGroupId);
        wg.setReplicaNumber(replicaNumber);
    }

    public void setDefaultWorkerGroupReplication(ReplicationType replicationType) {
        setWorkerGroupReplicationType(defaultWorkerGroupId, replicationType);
    }

    public void setWorkerGroupReplicationType(long workerGroupId, ReplicationType replicationType) {
        WorkerGroup wg = workerManager.getWorkerGroup(defaultServiceId, workerGroupId);
        wg.setReplicationType(replicationType);
    }

    public void setDefaultWorkerGroupWarmupLevel(WarmupLevel warmupLevel) {
        setWorkerGroupWarmupLevel(defaultWorkerGroupId, warmupLevel);
    }

    public void setWorkerGroupWarmupLevel(long workerGroupId, WarmupLevel warmupLevel) {
        WorkerGroup wg = workerManager.getWorkerGroup(defaultServiceId, workerGroupId);
        wg.setWarmupLevel(warmupLevel);
    }

    /**
     * Create default service and worker group with given service name.
     * Expect to call only once, to init `defaultServiceId` and `defaultWorkerGroupId`
     * @param serviceName target service name
     */
    public void createDefaultServiceAndWorkerGroup(String serviceName) {
        if (serviceName == null || serviceName.isEmpty()) {
            serviceName = this.getClass().getName() + "-serviceName-0";
        }
        defaultServiceId = bootstrapService(serviceName);
        defaultWorkerGroupId = TestUtils.createWorkerGroupForTest(workerManager, defaultServiceId, 1);
    }

    /**
     * bootstrap a new service with given service name, create the ServiceWorkerGroup as well.
     * @param serviceName target service name
     * @return service id
     */
    public String bootstrapService(String serviceName) {
        String serviceId = serviceManager.bootstrapService(serviceTemplateName, serviceName);
        workerManager.bootstrapService(serviceId);
        return serviceId;
    }

    public ServiceManager getServiceManager() {
        return serviceManager;
    }

    public WorkerManager getWorkerManager() {
        return workerManager;
    }

    /**
     * get ShardManager of defaultServiceId
     * @return shard manager of the default service id
     */
    public ShardManager getDefaultShardManager() {
        return serviceManager.getShardManager(defaultServiceId);
    }

    public String getDefaultServiceId() {
        return defaultServiceId;
    }

    public void setDefaultServiceId(String serviceId) {
        this.defaultServiceId = serviceId;
    }

    public long getDefaultWorkerGroupId() {
        return defaultWorkerGroupId;
    }

    /**
     * Create a single shard with defaultServiceId in shardGroup
     * @param shardGroup target shard group
     * @return shard id created
     */
    public long createTestShard(long shardGroup) {
        return createTestShard(Arrays.asList(shardGroup));
    }

    public long createTestShard(List<Long> shardGroups) {
        return createTestShards(1, shardGroups).get(0);
    }

    public List<Long> createTestShards(int nShards, long shardGroup) {
        return createTestShards(nShards, Arrays.asList(shardGroup));
    }

    public List<Long> createTestShards(int nShards, List<Long> shardGroups) {
        CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
        if (!shardGroups.isEmpty()) {
            // give shardGroup <= 0, means don't add into any groups
            builder.addAllGroupIds(shardGroups.stream().filter(x -> x > 0).collect(Collectors.toList()));
        }

        List<ShardInfo> shardInfoList = serviceManager.getShardManager(defaultServiceId)
                .createShard(Collections.nCopies(nShards, builder.build()),
                        serviceManager.getFileStoreMgr(defaultServiceId));
        Assert.assertEquals(shardInfoList.size(), nShards);
        return shardInfoList.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
    }

    /**
     * Create a worker group in default service
     * @return worker group id created
     */
    public long createWorkerGroup() {
        return createWorkerGroup(defaultServiceId, 1);
    }

    public long createWorkerGroup(int replicaNumber) {
        return createWorkerGroup(defaultServiceId, replicaNumber);
    }

    public long createWorkerGroup(String serviceId) {
        return TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);
    }

    public long createWorkerGroup(String serviceId, int replicaNumber) {
        return TestUtils.createWorkerGroupForTest(workerManager, serviceId, replicaNumber);
    }

    public long createWorkerGroup(String serviceId, String size, int replicaNumber) {
        return workerManager.createWorkerGroup(serviceId, "commonUtilsForTest",
                WorkerGroupSpec.newBuilder().setSize(size).build(),
                null, null, replicaNumber, ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);
    }

    public List<Long> getWorkerIdsOfShardReplicaFromWorkerGroup(long shardId, long workerGroupId) {
        Shard shard = serviceManager.getShardManager(defaultServiceId).getShard(shardId);
        Assert.assertNotNull(shard);
        return shard.getReplicaWorkerIds().stream()
                .map(workerManager::getWorker)
                .filter(y -> y != null && y.getGroupId() == workerGroupId)
                .map(Worker::getWorkerId)
                .collect(Collectors.toList());
    }

    public static String generateMockWorkerIpAddress() {
        final String mockWorkerIp = "127.1.1.0";
        return String.format("%s:%d", mockWorkerIp, mockWorkerPort++);
    }

    public long createTestWorkerToWorkerGroup(long workerGroupId) {
        return createTestWorkersToWorkerGroup(1, workerGroupId).get(0);
    }

    public List<Long> createTestWorkersToWorkerGroup(int nWorkers, long workerGroupId) {
        return createTestWorkersToWorkerGroup(defaultServiceId, nWorkers, workerGroupId);
    }

    public List<Long> createTestWorkersToWorkerGroup(String serviceId, int nWorkers, long workerGroupId) {
        List<Long> workerIds = new ArrayList<>(nWorkers);
        for (int i = 0; i < nWorkers; ++i) {
            long workerId = workerManager.addWorker(serviceId, workerGroupId, generateMockWorkerIpAddress());
            Worker worker = workerManager.getWorker(workerId);
            worker.updateLastSeenTime(System.currentTimeMillis());
            workerIds.add(workerId);
            Awaitility.await()
                    .pollInterval(1, TimeUnit.MILLISECONDS)
                    .atMost(1, TimeUnit.SECONDS)
                    .until(worker::isAlive);
        }
        return workerIds;
    }

    /**
     * Create a shard group with specified policy in default service
     * @param policy PlacementPolicy of the shard group
     * @return shard group id created.
     */
    public long createTestShardGroup(PlacementPolicy policy) {
        CreateShardGroupInfo request = CreateShardGroupInfo.newBuilder()
                .setPolicy(policy)
                .build();
        List<ShardGroupInfo> response = serviceManager.getShardManager(defaultServiceId)
                .createShardGroup(Arrays.asList(request));
        return response.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList()).get(0);
    }

    /**
     * Collect the shard replica distribution in specified worker group.
     * @param shardId the shard id to be collected
     * @param workerGroupId target worker group id
     * @return statistic info saved in map of workerId -> number of replicas
     */
    public Map<Long, Integer> collectShardReplicaDistributionStats(long shardId, long workerGroupId) {
        return collectShardReplicaDistributionStats(Arrays.asList(shardId), workerGroupId);
    }

    /**
     * Create a new MetaGroup with specified `policy`, shardGroupIds is the list of initial members.
     *
     * @param policy        Placement policy
     * @param shardGroupIds list of shard group ids to put into meta group
     * @return meta group id
     */
    public long createMetaGroup(PlacementPolicy policy, List<Long> shardGroupIds) {
        CreateMetaGroupInfo request = CreateMetaGroupInfo.newBuilder()
                .setPlacementPolicy(policy)
                .addAllShardGroupIds(shardGroupIds)
                .build();
        MetaGroupInfo response = serviceManager.getShardManager(defaultServiceId).createMetaGroup(request);
        return response.getMetaGroupId();
    }

    public Map<Long, Integer> collectShardReplicaDistributionStats(List<Long> shardIds, long workerGroupId) {
        Map<Long, Integer> stats = new HashMap<>();
        for (Long id : shardIds) {
            serviceManager.getShardManager(defaultServiceId).getShard(id).getReplicaWorkerIds()
                    .stream()
                    .map(workerManager::getWorker)
                    .filter(x -> x.getGroupId() == workerGroupId)
                    .forEach(key -> stats.compute(key.getWorkerId(), (k, val) -> val == null ? 1 : val + 1));
        }
        return stats;
    }

    public void setWorkerStatus(long workerId, WorkerState state) {
        workerManager.getWorker(workerId).setState(state);
    }

    public StarletAgent getMockWorkerStarletAgent(long workerId) {
        Worker worker = workerManager.getWorker(workerId);
        if (worker == null) {
            return null;
        }
        try {
            return (StarletAgent) FieldUtils.readDeclaredField(worker, "starletAgent", true);
        } catch (IllegalAccessException exception) {
            return null;
        }
    }
}
