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

import com.staros.common.TestHelper;
import com.staros.exception.AlreadyExistsStarException;
import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerGroupState;
import com.staros.proto.WorkerState;
import com.staros.starlet.StarletAgentFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkerGroupTest {
    private final String serviceId = WorkerGroupTest.class.getName() + "-serviceId";

    @BeforeClass
    public static void setMockStarletAgent() {
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
    }

    @Test
    public void testCompatibleWorkerGroup() {
        long groupId = 2;
        long workerId = 3;
        WorkerGroup workerGroup = new WorkerGroup(serviceId, groupId);

        Assert.assertEquals(workerGroup.getServiceId(), serviceId);
        Assert.assertEquals(workerGroup.getGroupId(), groupId);
        Assert.assertTrue(workerGroup.getLabels().isEmpty());
        Assert.assertTrue(workerGroup.getProperties().isEmpty());

        // read-only labels instance
        Assert.assertThrows(UnsupportedOperationException.class, () -> workerGroup.getLabels().put("A", "B"));

        // read-only properties instance
        Assert.assertThrows(UnsupportedOperationException.class, () -> workerGroup.getProperties().put("P1", "P2"));

        Worker worker = new Worker(serviceId, groupId, workerId, TestHelper.generateMockWorkerIpAddress());
        workerGroup.addWorker(worker);
        Assert.assertEquals(1L, workerGroup.getWorkerCount());

        // add Again, causes AlreadyExistsStarException
        Assert.assertThrows(AlreadyExistsStarException.class, () -> workerGroup.addWorker(worker));

        Assert.assertTrue(workerGroup.removeWorker(worker));
        Assert.assertEquals(workerGroup.getWorkerCount(), 0);

        // remove again, return false
        Assert.assertFalse(workerGroup.removeWorker(worker));
    }

    @Test
    public void testWorkerGroupBackwardsCompatibility() {
        long groupId = 2;
        WorkerGroup workerGroup = new WorkerGroup(serviceId, groupId);
        WorkerGroupDetailInfo.Builder builder = workerGroup.toProtobuf().toBuilder();
        builder.clearReplicationType()
                .clearReplicaNumber()
                .clearWarmupLevel();

        WorkerGroup group2 = WorkerGroup.fromProtobuf(builder.build());
        Assert.assertEquals(groupId, group2.getGroupId());
        // make sure the following new added fields are properly set to default values.
        Assert.assertEquals(ReplicationType.NO_REPLICATION, group2.getReplicationType());
        Assert.assertEquals(1L, group2.getReplicaNumber());
        Assert.assertEquals(WarmupLevel.WARMUP_NOTHING, group2.getWarmupLevel());
    }

    @Test
    public void testWorkerGroupUpdate() {
        long groupId = 2022;
        String owner = "TestOwner";
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("S").build();
        Map<String, String> labels = new HashMap<>();
        labels.put("l1", "v1");
        Map<String, String> props = new HashMap<>();
        labels.put("p1", "v2");
        int replicaNumber = 2;

        WorkerGroup group = new WorkerGroup(serviceId, groupId, owner, spec, labels, props, replicaNumber,
                ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);
        Assert.assertEquals(serviceId, group.getServiceId());
        Assert.assertEquals(groupId, group.getGroupId());
        Assert.assertEquals(owner, group.getOwner());
        Assert.assertEquals(spec, group.getSpec());
        Assert.assertNotSame(spec, group.getSpec());
        Assert.assertEquals(labels, group.getLabels());
        Assert.assertNotSame(labels, group.getLabels());
        Assert.assertEquals(props, group.getProperties());
        Assert.assertNotSame(props, group.getProperties());
        Assert.assertEquals(WorkerGroupState.PENDING, group.getState());
        Assert.assertEquals(2, group.getReplicaNumber());
        Assert.assertEquals(ReplicationType.SYNC, group.getReplicationType());

        // update spec
        WorkerGroupSpec newSpec = WorkerGroupSpec.newBuilder().setSize("L").build();
        group.updateSpec(newSpec);
        Assert.assertEquals(newSpec, group.getSpec());
        Assert.assertNotSame(newSpec, group.getSpec());

        // update state
        group.updateState(WorkerGroupState.READY);
        Assert.assertEquals(WorkerGroupState.READY, group.getState());

        // update Labels
        labels.put("l2", "ll2");
        Assert.assertNotEquals(labels, group.getLabels());
        group.setLabels(labels);
        Assert.assertEquals(labels, group.getLabels());
        Assert.assertNotSame(labels, group.getLabels());
        // nothing changed
        group.setLabels(null);
        Assert.assertEquals(labels, group.getLabels());

        // update Properties
        props.put("p2", "pp2");
        Assert.assertNotEquals(props, group.getProperties());
        group.setProperties(props);
        Assert.assertEquals(props, group.getProperties());
        Assert.assertNotSame(props, group.getProperties());
        // nothing changed
        group.setProperties(null);
        Assert.assertEquals(props, group.getProperties());

        // update replica number
        StarException thrown = Assert.assertThrows(StarException.class, () -> {
            group.setReplicaNumber(0);
        });
        Assert.assertEquals(ExceptionCode.INVALID_ARGUMENT, thrown.getExceptionCode());
        group.setReplicaNumber(3);
        Assert.assertEquals(3, group.getReplicaNumber());

        // update replication type
        group.setReplicationType(ReplicationType.ASYNC);
        Assert.assertEquals(ReplicationType.ASYNC, group.getReplicationType());

        // update warmup level
        group.setWarmupLevel(WarmupLevel.WARMUP_INDEX);
        Assert.assertEquals(WarmupLevel.WARMUP_INDEX, group.getWarmupLevel());
    }

    @Test
    public void testUpdateWorker() {
        long groupId = 2;
        long workerId = 3;

        WorkerGroup workerGroup = new WorkerGroup(serviceId, groupId);
        Worker worker1 = new Worker(serviceId, groupId, workerId, TestHelper.generateMockWorkerIpAddress());
        worker1.setState(WorkerState.ON);
        Worker worker2 = new Worker(serviceId, groupId, workerId, TestHelper.generateMockWorkerIpAddress());
        worker2.setState(WorkerState.DOWN);

        Assert.assertNotEquals(worker1, worker2);

        workerGroup.addWorker(worker1);
        // worker1 information updated, but the two are still referenced to different objects
        Assert.assertTrue(workerGroup.updateWorker(worker2));
        Assert.assertEquals(WorkerState.DOWN, worker1.getState());
        Assert.assertNotSame(worker1, worker2);

        Worker worker3 = new Worker(serviceId, groupId, workerId + 1000, TestHelper.generateMockWorkerIpAddress());
        // worker3 not exist
        Assert.assertFalse(workerGroup.updateWorker(worker3));

        // add worker3 into the workerGroup
        workerGroup.addWorker(worker3);
        // worker's replication type will be updated too
        workerGroup.setReplicationType(ReplicationType.ASYNC);
        Assert.assertEquals(ReplicationType.ASYNC, worker1.getReplicationType());
        Assert.assertEquals(ReplicationType.ASYNC, worker3.getReplicationType());

        Assert.assertEquals(WarmupLevel.WARMUP_NOTHING, workerGroup.getWarmupLevel());
        Assert.assertEquals(WarmupLevel.WARMUP_NOTHING, worker1.getWarmupLevel());
        Assert.assertEquals(WarmupLevel.WARMUP_NOTHING, worker3.getWarmupLevel());

        workerGroup.setWarmupLevel(WarmupLevel.WARMUP_INDEX);
        Assert.assertEquals(WarmupLevel.WARMUP_INDEX, workerGroup.getWarmupLevel());
        Assert.assertEquals(WarmupLevel.WARMUP_INDEX, worker1.getWarmupLevel());
        Assert.assertEquals(WarmupLevel.WARMUP_INDEX, worker3.getWarmupLevel());
    }

    @Test
    public void testWorkerGroupAllWorkerIds() {
        long groupId = 2;
        WorkerGroup workerGroup = new WorkerGroup(serviceId, groupId);
        Assert.assertEquals(0, workerGroup.getWorkerCount());
        {
            long workerId = 3;
            String ipPort = TestHelper.generateMockWorkerIpAddress();
            Worker worker = new Worker(serviceId, groupId, workerId, ipPort);
            worker.setState(WorkerState.ON);
            workerGroup.addWorker(worker);
        }
        {
            long workerId = 4;
            String ipPort = TestHelper.generateMockWorkerIpAddress();
            Worker worker = new Worker(serviceId, groupId, workerId, ipPort);
            worker.setState(WorkerState.DOWN);
            workerGroup.addWorker(worker);
        }

        Assert.assertEquals(2, workerGroup.getWorkerCount());

        List<Long> allIds = workerGroup.getAllWorkerIds(false /*onlyAlive*/);
        Assert.assertEquals(Arrays.asList(3L, 4L), allIds);

        List<Long> aliveIds = workerGroup.getAllWorkerIds(true /*onlyAlive*/);
        Assert.assertEquals(Arrays.asList(3L), aliveIds);
    }
}
