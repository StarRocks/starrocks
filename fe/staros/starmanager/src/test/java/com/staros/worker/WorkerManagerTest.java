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

import com.google.common.collect.ImmutableMap;
import com.staros.common.HijackConfig;
import com.staros.common.TestHelper;
import com.staros.common.TestUtils;
import com.staros.exception.AlreadyExistsStarException;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerGroupState;
import com.staros.proto.WorkerInfo;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.util.Utils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerManagerTest {
    private WorkerManager workerManager;
    private String serviceId;

    @BeforeClass
    public static void prepare() {
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
    }

    @Before
    public void setUp() {
        workerManager = WorkerManager.createWorkerManagerForTest(null);
        serviceId = this.getClass().getName() + "-serviceId";
        workerManager.bootstrapService(serviceId);
    }

    @Test
    public void testWorkerManager() {
        String ipPort = "127.0.0.1:1234";
        long groupId = TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);

        Assert.assertThrows(NotExistStarException.class, () -> workerManager.addWorker(serviceId, groupId + 1, ipPort));
        long workerId = workerManager.addWorker(serviceId, groupId, ipPort);

        Assert.assertThrows(AlreadyExistsStarException.class, () -> workerManager.addWorker(serviceId, groupId, ipPort));
        Assert.assertThrows(NotExistStarException.class, () -> workerManager.removeWorker(serviceId, groupId + 1, workerId));
        Assert.assertThrows(NotExistStarException.class, () -> workerManager.removeWorker(serviceId, groupId, workerId + 1));

        { // test get worker by id
            WorkerInfo workerInfo = workerManager.getWorkerInfo(workerId);
            Assert.assertEquals(workerInfo.getServiceId(), serviceId);
            Assert.assertEquals(workerInfo.getGroupId(), groupId);
            Assert.assertEquals(workerInfo.getWorkerId(), workerId);
            Assert.assertEquals(workerInfo.getIpPort(), ipPort);
            Assert.assertThrows(NotExistStarException.class, () -> workerManager.getWorkerInfo(workerId + 1));
        }
        { // test get worker by ip port
            WorkerInfo workerInfo = workerManager.getWorkerInfo(ipPort);
            Assert.assertEquals(workerInfo.getServiceId(), serviceId);
            Assert.assertEquals(workerInfo.getGroupId(), groupId);
            Assert.assertEquals(workerInfo.getWorkerId(), workerId);
            Assert.assertEquals(workerInfo.getIpPort(), ipPort);
            Assert.assertThrows(NotExistStarException.class, () -> workerManager.getWorkerInfo(ipPort + "aaa"));
        }
        workerManager.removeWorker(serviceId, groupId, workerId);
    }

    @Test
    public void testCreateWorkerGroupServiceNotExist() {
        // ServiceId not bootstrapped yet.
        String fakeServiceId = "fake-service-id";
        Assert.assertThrows(NotExistStarException.class,
                () -> TestUtils.createWorkerGroupForTest(workerManager, fakeServiceId, 1));
        Assert.assertThrows(NotExistStarException.class, () -> workerManager.getDefaultWorkerGroup(fakeServiceId));
        Assert.assertThrows(NotExistStarException.class, () -> workerManager.getWorkerGroup(fakeServiceId, 0));

        workerManager.bootstrapService(fakeServiceId);
        // after bootstrap, no exception doing worker group creation.
        long groupId = TestUtils.createWorkerGroupForTest(workerManager, fakeServiceId, 1);
        Assert.assertTrue(groupId > 0);
    }

    @Test
    public void testRemoveWorkerInconsistentInfo() {
        long groupId = TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);
        long groupId2 = TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);
        String ipPort = "127.0.0.1:1234";
        long workerId = workerManager.addWorker(serviceId, groupId, ipPort);
        Assert.assertNotNull(workerManager.getWorker(workerId));

        // try to remove worker with inconsistent info
        Assert.assertThrows(InvalidArgumentStarException.class, () ->
                workerManager.removeWorker(serviceId, groupId2, workerId));

        workerManager.removeWorker(serviceId, groupId, workerId);
        Assert.assertNull(workerManager.getWorker(workerId));
    }

    @Test
    public void testGetWorkerGroupExceptions() {
        long group1 = TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);
        long group2NotExist = group1 + 1000;

        Assert.assertNotNull(workerManager.getWorkerGroupNoException(serviceId, group1));
        Assert.assertNotNull(workerManager.getWorkerGroup(serviceId, group1));

        Assert.assertNull(workerManager.getWorkerGroupNoException(serviceId, group2NotExist));
        Assert.assertThrows(NotExistStarException.class, () -> workerManager.getWorkerGroup(serviceId, group2NotExist));

        List<Long> groupIds = workerManager.getAllWorkerGroupIds(serviceId);
        Assert.assertEquals(1L, groupIds.size());
        Assert.assertTrue(groupIds.contains(group1));
    }

    @Test
    public void testListWorkerGroupById() {
        // listWorkerGroupsById
        // empty worker group id list
        Assert.assertThrows(InvalidArgumentStarException.class,
                () -> workerManager.listWorkerGroupsById(serviceId, Collections.emptyList(), false));
        // invalid service id
        Assert.assertThrows(NotExistStarException.class,
                () -> workerManager.listWorkerGroupsById(serviceId + "xxx", Collections.singletonList(12345L), false));
        // not exist worker group id
        Assert.assertThrows(NotExistStarException.class,
                () -> workerManager.listWorkerGroupsById(serviceId, Collections.singletonList(12345L), true));

        long groupId = TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);
        // add two workers into group 1
        List<Long> workerIds = new ArrayList<>();
        workerIds.add(workerManager.addWorker(serviceId, groupId, TestHelper.generateMockWorkerIpAddress()));
        workerIds.add(workerManager.addWorker(serviceId, groupId, TestHelper.generateMockWorkerIpAddress()));
        { // assert the workers in the workerGroup
            WorkerGroup workerGroup = workerManager.getWorkerGroup(serviceId, groupId);
            for (long id : workerIds) {
                Worker w = workerGroup.getWorker(id);
                Assert.assertNotNull(w);
            }
        }
        { // List one group without worker info
            List<WorkerGroupDetailInfo> listInfo = workerManager.listWorkerGroupsById(
                    serviceId, Collections.singletonList(groupId), false);
            Assert.assertEquals(1L, listInfo.size());
            Assert.assertEquals(0L, listInfo.get(0).getWorkersInfoCount());
            WorkerGroup group = workerManager.getWorkerGroup(serviceId, groupId);
            Assert.assertEquals(group.toProtobuf().toString(), listInfo.get(0).toString());
        }

        { // List one group with worker info
            List<WorkerGroupDetailInfo> listInfo = workerManager.listWorkerGroupsById(
                    serviceId, Collections.singletonList(groupId), true);
            Assert.assertEquals(1L, listInfo.size());
            WorkerGroupDetailInfo detailInfo = listInfo.get(0);
            // have two workers
            Assert.assertEquals(workerIds.size(), detailInfo.getWorkersInfoCount());
            detailInfo.getWorkersInfoList().forEach(x -> {
                // verify workerInfo
                Assert.assertTrue(workerIds.contains(x.getWorkerId()));
                Assert.assertEquals(groupId, x.getGroupId());
            });
            WorkerGroup group = workerManager.getWorkerGroup(serviceId, groupId);
            // clear the workersInfo and do the comparison
            WorkerGroupDetailInfo copyInfo =
                    WorkerGroupDetailInfo.newBuilder().mergeFrom(detailInfo).clearWorkersInfo().build();
            Assert.assertEquals(group.toProtobuf().toString(), copyInfo.toString());
        }

        long groupId2 = TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);
        { // list two groups
            List<WorkerGroupDetailInfo> listInfo = workerManager.listWorkerGroupsById(
                    serviceId, Arrays.asList(groupId, groupId2), false);
            Assert.assertEquals(2L, listInfo.size());
        }
    }

    @Test
    public void testListWorkerGroupByLabel() {
        // service Id not exist
        Assert.assertThrows(NotExistStarException.class,
                () -> workerManager.listWorkerGroups(serviceId + "xxx", Collections.emptyMap(), false));
        // No result
        Assert.assertThrows(NotExistStarException.class,
                () -> workerManager.listWorkerGroups(serviceId, Collections.emptyMap(), true));

        String owner = "WorkerManagerTestOwner";
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("M").build();
        long groupId1 = workerManager.createWorkerGroup(serviceId, owner, spec,
                ImmutableMap.of("BU", "CTO", "Team", "Query"),
                ImmutableMap.of("createTime", "1", "updateTime", "1"), 1 /* replicaNumber */,
                ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);
        long groupId2 = workerManager.createWorkerGroup(serviceId, owner, spec,
                ImmutableMap.of("BU", "CTO", "Team", "Cloud"),
                ImmutableMap.of("createTime", "2", "updateTime", "2"), 1 /* replicaNumber */,
                ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);

        // add two workers into group 1
        List<Long> workerIds = new ArrayList<>();
        workerIds.add(workerManager.addWorker(serviceId, groupId1, TestHelper.generateMockWorkerIpAddress()));
        workerIds.add(workerManager.addWorker(serviceId, groupId1, TestHelper.generateMockWorkerIpAddress()));

        Map<Long, String> expectedResult = new HashMap<>();
        expectedResult.put(groupId1, workerManager.getWorkerGroup(serviceId, groupId1).toProtobuf().toString());
        expectedResult.put(groupId2, workerManager.getWorkerGroup(serviceId, groupId2).toProtobuf().toString());

        { // list all
            List<WorkerGroupDetailInfo> info = workerManager.listWorkerGroups(serviceId, Collections.emptyMap(), false);
            if (Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY) {
                Assert.assertEquals(expectedResult.size() + 1, info.size());
            } else {
                Assert.assertEquals(expectedResult.size(), info.size());
            }
            info.forEach(x -> Assert.assertEquals(expectedResult.get(x.getGroupId()), x.toString()));
            info.forEach(x -> Assert.assertEquals(0L, x.getWorkersInfoCount()));
        }
        { // list only matches (2 matches)
            List<WorkerGroupDetailInfo> info = workerManager.listWorkerGroups(serviceId, ImmutableMap.of("BU", "CTO"), false);
            Assert.assertEquals(expectedResult.size(), info.size());
            info.forEach(x -> Assert.assertEquals(expectedResult.get(x.getGroupId()), x.toString()));
            info.forEach(x -> Assert.assertEquals(0L, x.getWorkersInfoCount()));
        }
        { // list only matches (1 match)
            List<WorkerGroupDetailInfo> info = workerManager.listWorkerGroups(serviceId, ImmutableMap.of("Team", "Cloud"), false);
            Assert.assertEquals(1L, info.size());
            info.forEach(x -> Assert.assertEquals(expectedResult.get(x.getGroupId()), x.toString()));
            info.forEach(x -> Assert.assertEquals(0L, x.getWorkersInfoCount()));
        }
        { // list only matches (0 matches)
            Assert.assertThrows(NotExistStarException.class,
                    () -> workerManager.listWorkerGroups(serviceId, ImmutableMap.of("BU", "EXO"), true));
        }
        { // list matches group1 with workerInfo
            List<WorkerGroupDetailInfo> info = workerManager.listWorkerGroups(serviceId, ImmutableMap.of("Team", "Query"), true);
            Assert.assertEquals(1L, info.size());
            WorkerGroupDetailInfo groupInfo = info.get(0);
            Assert.assertEquals(workerIds.size(), groupInfo.getWorkersInfoCount());
            {
                WorkerGroupDetailInfo groupInfoWithoutWorker =
                        WorkerGroupDetailInfo.newBuilder().mergeFrom(groupInfo).clearWorkersInfo().build();
                Assert.assertEquals(expectedResult.get(groupInfoWithoutWorker.getGroupId()), groupInfoWithoutWorker.toString());
            }
            groupInfo.getWorkersInfoList().forEach(x -> {
                Assert.assertEquals(groupId1, x.getGroupId());
                Assert.assertTrue(workerIds.contains(x.getWorkerId()));
            });
        }
        { // list all worker group with worker info
            List<WorkerGroupDetailInfo> info = workerManager.listWorkerGroups(serviceId, Collections.emptyMap(), true);
            Assert.assertEquals(2L, info.size());
            WorkerGroupDetailInfo groupInfo1;
            WorkerGroupDetailInfo groupInfo2;
            if (info.get(0).getGroupId() == groupId1) {
                groupInfo1 = info.get(0);
                groupInfo2 = info.get(1);
            } else {
                groupInfo1 = info.get(1);
                groupInfo2 = info.get(0);
            }
            // validate groupInfo1 which has two workers
            Assert.assertEquals(workerIds.size(), groupInfo1.getWorkersInfoCount());
            {
                WorkerGroupDetailInfo groupInfoWithoutWorker =
                        WorkerGroupDetailInfo.newBuilder().mergeFrom(groupInfo1).clearWorkersInfo().build();
                Assert.assertEquals(expectedResult.get(groupInfoWithoutWorker.getGroupId()), groupInfoWithoutWorker.toString());
            }
            groupInfo1.getWorkersInfoList().forEach(x -> {
                Assert.assertEquals(groupId1, x.getGroupId());
                Assert.assertTrue(workerIds.contains(x.getWorkerId()));
            });
            // validate groupInfo2 which has no workers
            Assert.assertEquals(expectedResult.get(groupInfo2.getGroupId()), groupInfo2.toString());
            Assert.assertEquals(0L, groupInfo2.getWorkersInfoCount());
        }
    }

    @Test
    public void testUpdateWorkerGroup() {
        long workerGroupId = TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);
        // service Id not exist
        Assert.assertThrows(NotExistStarException.class,
                () -> workerManager.updateWorkerGroup(serviceId + "xxx", workerGroupId, null, null, null, 0 /* replicaNumber */,
                                                      ReplicationType.NO_SET, WarmupLevel.WARMUP_NOT_SET));
        // worker group id not exist
        Assert.assertThrows(NotExistStarException.class,
                () -> workerManager.updateWorkerGroup(serviceId, workerGroupId + 10086, null, null, null, 0 /* replicaNumber */,
                                                      ReplicationType.NO_SET, WarmupLevel.WARMUP_NOT_SET));

        WorkerGroupDetailInfo expectedInfo = workerManager.getWorkerGroup(serviceId, workerGroupId).toProtobuf();
        // nothing changed.
        workerManager.updateWorkerGroup(serviceId, workerGroupId, null, null, null, 0 /* replicaNumber */,
                ReplicationType.NO_SET, WarmupLevel.WARMUP_NOT_SET);
        Assert.assertEquals(expectedInfo.toString(),
                workerManager.getWorkerGroup(serviceId, workerGroupId).toProtobuf().toString());

        { // update
            WorkerGroupSpec newSpec = WorkerGroupSpec.newBuilder().setSize("XXXXXL").build();
            Map<String, String> newLabels = ImmutableMap.of("A1", "A2");
            Map<String, String> newProps = ImmutableMap.of("P1", "P2");
            int newReplicaNumber = 3;
            WarmupLevel newLevel = WarmupLevel.WARMUP_ALL;
            Assert.assertNotEquals(expectedInfo.getSpec(), newSpec);
            Assert.assertNotEquals(expectedInfo.getLabelsMap(), newLabels);
            Assert.assertNotEquals(expectedInfo.getPropertiesMap(), newProps);
            Assert.assertNotEquals(expectedInfo.getReplicaNumber(), newReplicaNumber);
            Assert.assertNotEquals(expectedInfo.getWarmupLevel(), newLevel);

            workerManager.updateWorkerGroup(serviceId, workerGroupId, newSpec, newLabels, newProps, newReplicaNumber,
                    ReplicationType.ASYNC, newLevel);
            WorkerGroupDetailInfo info = workerManager.getWorkerGroup(serviceId, workerGroupId).toProtobuf();
            Assert.assertEquals(newSpec, info.getSpec());
            Assert.assertEquals(newLabels, info.getLabelsMap());
            Assert.assertEquals(newProps, info.getPropertiesMap());
            Assert.assertEquals(WorkerGroupState.PENDING, info.getState());
            Assert.assertEquals(newReplicaNumber, info.getReplicaNumber());
            Assert.assertEquals(ReplicationType.ASYNC, info.getReplicationType());
            Assert.assertEquals(newLevel, info.getWarmupLevel());
        }
    }

    @Test
    public void testDeleteWorkerGroup() {
        long workerGroupId = TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);
        // service Id not exist
        Assert.assertThrows(NotExistStarException.class,
                () -> workerManager.deleteWorkerGroup(serviceId + "xxx", workerGroupId));
        // worker group id not exist
        Assert.assertThrows(NotExistStarException.class,
                () -> workerManager.deleteWorkerGroup(serviceId, workerGroupId + 10086));

        long workerId = workerManager.addWorker(serviceId, workerGroupId, TestHelper.generateMockWorkerIpAddress());
        Assert.assertNotNull(workerManager.getWorker(workerId));
        Assert.assertEquals(1L, workerManager.getWorkerGroup(serviceId, workerGroupId).getWorkerCount());

        workerManager.deleteWorkerGroup(serviceId, workerGroupId);
        Assert.assertThrows(NotExistStarException.class, () -> workerManager.getWorkerGroup(serviceId, workerGroupId));
        Assert.assertThrows(NotExistStarException.class, () -> workerManager.getWorkerInfo(workerId));
    }

    @Test
    public void testWorkerGroupProvision() {
        TestHelper helper = new TestHelper("testWorkerGroupProvision");
        helper.turnOnWorkerGroupAutoProvision();
        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        String serviceId = helper.getDefaultServiceId();
        WorkerManager workerManager = helper.getWorkerManager();

        // create a workerGroup with 2 nodes
        long workerGroupId = helper.createWorkerGroup(serviceId, "x2", 1);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> TestUtils.workerManagerExecutorsIdle(workerManager));
        {
            WorkerGroup group = workerManager.getWorkerGroup(serviceId, workerGroupId);
            Assert.assertEquals(2L, group.getWorkerCount());
            Assert.assertEquals(WorkerGroupState.READY, group.getState());
        }
        // change to 6 nodes
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("x6").build();
        workerManager.updateWorkerGroup(serviceId, workerGroupId, spec, null, null, 0 /* replicaNumber */,
                ReplicationType.NO_SET, WarmupLevel.WARMUP_NOT_SET);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> TestUtils.workerManagerExecutorsIdle(workerManager));
        {
            WorkerGroup group = workerManager.getWorkerGroup(serviceId, workerGroupId);
            Assert.assertEquals(6L, group.getWorkerCount());
            Assert.assertEquals(WorkerGroupState.READY, group.getState());
        }
        // change to 4 nodes
        WorkerGroupSpec spec2 = WorkerGroupSpec.newBuilder().setSize("x4").build();
        workerManager.updateWorkerGroup(serviceId, workerGroupId, spec2, null, null, 0 /* replicaNumber */,
                ReplicationType.NO_SET, WarmupLevel.WARMUP_NOT_SET);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> TestUtils.workerManagerExecutorsIdle(workerManager));
        {
            WorkerGroup group = workerManager.getWorkerGroup(serviceId, workerGroupId);
            Assert.assertEquals(4L, group.getWorkerCount());
            Assert.assertEquals(WorkerGroupState.READY, group.getState());
        }
        // trigger resource deletion. Nothing to do for now in MockResourceManager
        workerManager.deleteWorkerGroup(serviceId, workerGroupId);
    }

    @Test
    public void testWorkerGroupDumpAndLoadCompatibilityBasic() {
        // dump and restore works in both enable/disable Zero worker group.
        testWorkerGroupDumpAndLoadCompatibility(true);
        testWorkerGroupDumpAndLoadCompatibility(false);
    }

    private void testWorkerGroupDumpAndLoadCompatibility(boolean enableZeroWorkerGroup) {
        HijackConfig configVar =
                new HijackConfig("ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY", String.valueOf(enableZeroWorkerGroup));
        // create 3 service worker group
        // service worker group 1 : empty
        // service worker group 2 : one worker group with empty worker
        // service worker group 3 : two worker groups with one and two workers respectively
        WorkerManager workerManager = WorkerManager.createWorkerManagerForTest(null);
        List<String> serviceIds = Arrays.asList(
                "testWorkerGroupDumpAndRestore-0",
                "testWorkerGroupDumpAndRestore-1",
                "testWorkerGroupDumpAndRestore-2");
        serviceIds.forEach(workerManager::bootstrapService);
        {
            Map<String, String> labels = ImmutableMap.of("serviceId", serviceIds.get(1));
            Map<String, String> props = ImmutableMap.of("P1", "P2");
            workerManager.createWorkerGroup(serviceIds.get(1), "myOwner", WorkerGroupSpec.newBuilder().setSize("x0").build(),
                    labels, props, 1 /* replicaNumber */, ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);
        }
        {
            Map<String, String> labels = ImmutableMap.of("serviceId", serviceIds.get(2));
            Map<String, String> props = ImmutableMap.of("P1", "P2");
            String serviceId = serviceIds.get(2);
            long workerGroupId1 = workerManager.createWorkerGroup(serviceId, "myOwner",
                    WorkerGroupSpec.newBuilder().setSize("x4").build(),
                    labels, props, 1 /* replicaNumber */, ReplicationType.SYNC, WarmupLevel.WARMUP_META);
            long workerGroupId2 = workerManager.createWorkerGroup(serviceId, "myOwner",
                    WorkerGroupSpec.newBuilder().setSize("x4").build(),
                    labels, props, 1 /* replicaNumber */, ReplicationType.SYNC, WarmupLevel.WARMUP_ALL);
            // one worker to workerGroupId1
            workerManager.addWorker(serviceId, workerGroupId1, TestHelper.generateMockWorkerIpAddress());
            // two workers to workerGroupId2
            workerManager.addWorker(serviceId, workerGroupId2, TestHelper.generateMockWorkerIpAddress());
            workerManager.addWorker(serviceId, workerGroupId2, TestHelper.generateMockWorkerIpAddress());
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            workerManager.dumpMeta(out);
        } catch (Exception e) {
            Assert.fail();
        }
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        WorkerManager newManager = WorkerManager.createWorkerManagerForTest(null);
        try {
            newManager.loadMeta(in);
        } catch (Exception e) {
            Assert.fail();
        }
        // sanity validation
        Assert.assertEquals(workerManager.getAllWorkerIds(), newManager.getAllWorkerIds());
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        try {
            workerManager.dumpMeta(out2);
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertArrayEquals(out.toByteArray(), out2.toByteArray());
        configVar.reset();
    }

    @Test
    public void testWorkerManagerMetaDumpAndLoadException() {
        { // prepare data
            Map<String, String> labels = ImmutableMap.of("serviceId", serviceId);
            Map<String, String> props = ImmutableMap.of("P1", "P2");
            long workerGroup = workerManager.createWorkerGroup(serviceId, "myOwner",
                    WorkerGroupSpec.newBuilder().setSize("x0").build(), labels, props, 1 /* replicaNumber */,
                    ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);
            workerManager.addWorker(serviceId, workerGroup, TestHelper.generateMockWorkerIpAddress());
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            workerManager.dumpMeta(out);
        } catch (Exception e) {
            Assert.fail();
        }
        { // remove the last byte
            byte[] data = out.toByteArray();
            ByteArrayInputStream in = new ByteArrayInputStream(data, 0, data.length - 1);
            WorkerManager newManager = WorkerManager.createWorkerManagerForTest(null);
            Assert.assertThrows(IOException.class, () -> newManager.loadMeta(in));
        }
        { // corrupt the first byte.
            byte[] data = out.toByteArray();
            data[0] = (byte) (data[0] + (byte) 64);
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            WorkerManager newManager = WorkerManager.createWorkerManagerForTest(null);
            Assert.assertThrows(IOException.class, () -> newManager.loadMeta(in));
        }
    }

    /**
     * Convenient help class to change the WorkerManager static private variable through Reflection together.
     */
    private static class WorkerManagerThreadPoolParamKeeper {
        private final int interval;
        private final int initDelay;
        private final String intervalVarName = "ADJUST_THREAD_POOL_INTERVAL_MS";
        private final String delayVarName = "ADJUST_THREAD_POOL_INIT_DELAY_MS";

        public WorkerManagerThreadPoolParamKeeper(int newInterval, int newDelay) {
            this.interval = readStaticField(intervalVarName);
            writeStaticField(intervalVarName, newInterval);

            this.initDelay = readStaticField(delayVarName);
            writeStaticField(delayVarName, newDelay);
        }

        public void release() {
            writeStaticField(intervalVarName, interval);
            writeStaticField(delayVarName, initDelay);
        }

        private int readStaticField(String fieldName) {
            try {
                return (int) FieldUtils.readStaticField(WorkerManager.class, fieldName, true);
            } catch (IllegalAccessException exception) {
                Assert.fail(String.format("Reflecting read WorkerManager field %s failed with exception: %s",
                        fieldName, exception.getMessage()));
                return -1;
            }
        }

        private void writeStaticField(String fieldName, int value) {
            try {
                FieldUtils.writeStaticField(WorkerManager.class, fieldName, value, true);
            } catch (IllegalAccessException exception) {
                Assert.fail(String.format("Reflecting write WorkerManager field %s failed with exception: %s",
                        fieldName, exception.getMessage()));
            }
        }
    }

    @Test
    public void testIdleAdjustWorkerManagerGenericPoolSize() {
        // setup test env
        workerManager.stop();
        WorkerManagerThreadPoolParamKeeper keeper = new WorkerManagerThreadPoolParamKeeper(100, 100);

        // Manually set the ThreadPool coreSize to a large one, wait for a few round until the coreSize dropped
        workerManager.start();
        ThreadPoolExecutor executor = TestUtils.getWorkerManagerExecutor(workerManager);
        Assert.assertNotNull(executor);
        Utils.adjustFixedThreadPoolExecutors(executor, 16);
        Assert.assertEquals(16, executor.getCorePoolSize());
        // check every round, each time decrease 1 thread until coreSize to 4 and stopped, need about 1.2seconds
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> executor.getCorePoolSize() == 4);
        Assert.assertEquals(4, executor.getCorePoolSize());

        // clean up test env
        workerManager.stop();
        keeper.release();
    }

    @Test
    public void testPendingTasksAdjustWorkerManagerGenericPoolSize() {
        // setup test env
        workerManager.stop();
        WorkerManagerThreadPoolParamKeeper keeper = new WorkerManagerThreadPoolParamKeeper(100, 100);
        workerManager.start();

        CountDownLatch runLatch = new CountDownLatch(1);
        AtomicInteger doneCount = new AtomicInteger();
        ThreadPoolExecutor executor = TestUtils.getWorkerManagerExecutor(workerManager);
        Assert.assertNotNull(executor);

        int nTasks = 500;
        for (int i = 0; i < nTasks; ++i) {
            Runnable run = () -> {
                try {
                    runLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                doneCount.incrementAndGet();
            };
            executor.execute(run);
        }
        // with 100ms per round, 5 secs allows running with 50 rounds. enough to adjust to MAX_CORE_SIZE
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> executor.getCorePoolSize() == 64);
        Assert.assertEquals(64, executor.getCorePoolSize());
        // allow all tasks to be completed
        runLatch.countDown();
        // wait for all tasks done
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> doneCount.get() == nTasks);
        // thread pool size cooldown
        // with 100ms per round, 5 secs allows running with 50 rounds. enough to adjust to MIN_CORE_SIZE * 2
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> executor.getCorePoolSize() == 4);

        // clean up test env
        workerManager.stop();
        keeper.release();
    }

    @Test
    public void testWorkerManagerGenericThreadPoolCalculateThreadPoolSize() {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        int loop = 4096;
        for (int i = 0; i < loop; ++i) { // pending tasks <= numWorkers, scale-in or no change
            int numWorkers = rand.nextInt(0, 1024);
            int pendingTasks = rand.nextInt(-numWorkers - 1, numWorkers);
            int currentSize = rand.nextInt(1, 128);
            int newSize = WorkerManager.calculateThreadPoolSize(currentSize, numWorkers, pendingTasks);
            String msg = "TestCase: " + "numWorkers=" + numWorkers + ", pendingTasks=" + pendingTasks;
            if (currentSize > 4) { // scale-in
                Assert.assertEquals(msg, currentSize - 1, newSize);
            } else { // no change
                Assert.assertEquals(msg, newSize, currentSize);
            }
        }

        for (int i = 0; i < loop; ++i) { // pending tasks > numWorkers
            int numWorkers = rand.nextInt(0, 1024);
            int pendingTasks = rand.nextInt(1, 65536) + numWorkers;
            int currentSize = rand.nextInt(1, 128);

            int multiplex = (int) Math.ceil(Math.log10(pendingTasks));
            int expectSize = Math.min(currentSize + multiplex * 2, 64);
            String msg = "TestCase: " + "numWorkers=" + numWorkers + ", pendingTasks=" +
                    pendingTasks + ", currentSize=" + currentSize;
            int newSize = WorkerManager.calculateThreadPoolSize(currentSize, numWorkers, pendingTasks);
            Assert.assertEquals(msg, newSize, expectSize);
        }
    }
}
