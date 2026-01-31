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

import com.staros.exception.WorkerNotHealthyStarException;
import com.staros.proto.AddShardInfo;
import com.staros.proto.AddShardRequest;
import com.staros.proto.RemoveShardRequest;
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerState;
import com.staros.starlet.StarletAgentFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class WorkerTest {
    private String serviceId = "1";
    private long groupId = 2;
    private long workerId = 3;
    private String ipPort = "127.0.0.1:1234";

    @BeforeClass
    public static void setMockStarletAgent() {
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
    }

    public static Worker getTestWorker(String serviceId, long groupId, long workerId, String ipPort) {
        Worker worker = new Worker(serviceId, groupId, workerId, ipPort);
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("hello", "world");
        worker.updateInfo(0, workerProps, 0);
        return worker;
    }

    @Test
    public void testWorker() {
        Worker worker = getTestWorker(serviceId, groupId, workerId, ipPort);

        Assert.assertEquals(worker.getServiceId(), serviceId);
        Assert.assertEquals(worker.getGroupId(), groupId);
        Assert.assertEquals(worker.getWorkerId(), workerId);
        Assert.assertEquals(worker.getIpPort(), ipPort);
        Assert.assertEquals(worker.getState(), WorkerState.DOWN);
        // init downtime set to 0
        Assert.assertEquals(0, worker.getLastDownTime());
        // already in DOWN state, the timestamp won't be updated
        Assert.assertFalse(worker.setState(WorkerState.DOWN));
        Assert.assertEquals(0, worker.getLastDownTime());

        Assert.assertTrue(worker.setState(WorkerState.ON));
        Assert.assertFalse(worker.setState(WorkerState.ON));
        Assert.assertEquals(worker.getState(), WorkerState.ON);
        // downtime will not be touched.
        Assert.assertEquals(0, worker.getLastDownTime());

        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("hello", "world");
        Assert.assertEquals(worker.getProperties(), workerProps);

        Assert.assertEquals(0L, worker.getLastSeenTime());
        long current = System.currentTimeMillis();
        worker.updateLastSeenTime(current);
        Assert.assertEquals(current, worker.getLastSeenTime());
        // worker's lastSeenTime can't be updated to backwards
        worker.updateLastSeenTime(current - 10);
        Assert.assertEquals(current, worker.getLastSeenTime());

        // the worker is not associated with a workerGroup, so the warmupLevel and replicationType can't be obtained.
        Assert.assertThrows(IllegalStateException.class, worker::getWarmupLevel);
        Assert.assertThrows(IllegalStateException.class, worker::getReplicationType);

        long beforeSetDown = System.currentTimeMillis();
        Assert.assertTrue(worker.setState(WorkerState.DOWN));
        Assert.assertTrue(beforeSetDown <= worker.getLastDownTime());
    }

    @Test
    public void testWorkerNumOfShards() {
        Worker worker = getTestWorker(serviceId, groupId, workerId, ipPort);
        worker.updateInfo(System.currentTimeMillis(), null, 2);

        // NumOfShards will be overwritten by worker.updateInfo()
        Assert.assertEquals(2L, worker.getNumOfShards());

        // NumOfShards will be increased by addShard()
        AddShardInfo info = AddShardInfo.newBuilder()
                .setShardId(1)
                .build();
        // fake addRequest with 3 identical AddShardInfo
        AddShardRequest addRequest = AddShardRequest.newBuilder()
                .addAllShardInfo(Collections.nCopies(3, info))
                .build();

        Assert.assertEquals(WorkerState.DOWN, worker.getState());
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> worker.addShard(addRequest));
        // Add failure, nothing changed!
        Assert.assertEquals(2L, worker.getNumOfShards());

        worker.setState(WorkerState.ON);
        try {
            worker.addShard(addRequest);
        } catch (Exception e) {
            Assert.fail("should not throw exception!");
        }
        // expected result, because the NumOfShards is designed to be an approximate value
        Assert.assertEquals(5L, worker.getNumOfShards());

        // NumOfShards can be decreased by removeShard()
        RemoveShardRequest removeRequest = RemoveShardRequest.newBuilder()
                .addAllShardIds(Collections.nCopies(2, 135L))
                .build();

        worker.setState(WorkerState.DOWN);
        Assert.assertThrows(WorkerNotHealthyStarException.class, () -> worker.removeShard(removeRequest));
        // Remove failure, nothing changed.
        Assert.assertEquals(5L, worker.getNumOfShards());

        worker.setState(WorkerState.ON);
        try {
            worker.removeShard(removeRequest);
        } catch (Exception e) {
            Assert.fail("should not throw exception!");
        }        // expected result, because the NumOfShards is designed to be an approximate value
        Assert.assertEquals(3L, worker.getNumOfShards());

        // NumOfShards will be overwritten back by worker's heartbeat
        worker.updateInfo(System.currentTimeMillis(), null, 7);
        Assert.assertEquals(7L, worker.getNumOfShards());
    }

    @Test
    public void testSerialization() {
        Worker worker1 = getTestWorker(serviceId, groupId, workerId, ipPort);
        Assert.assertEquals(WorkerState.DOWN, worker1.getState());
        Assert.assertTrue(worker1.setState(WorkerState.ON));
        Assert.assertTrue(worker1.setState(WorkerState.DOWN));
        Assert.assertTrue(worker1.getLastDownTime() > 0);

        WorkerInfo info = worker1.toProtobuf();

        Worker worker2 = Worker.fromProtobuf(info);

        Assert.assertEquals(worker1.getServiceId(), worker2.getServiceId());
        Assert.assertEquals(worker1.getGroupId(), worker2.getGroupId());
        Assert.assertEquals(worker1.getWorkerId(), worker2.getWorkerId());
        Assert.assertEquals(worker1.getIpPort(), worker2.getIpPort());
        Assert.assertEquals(worker1.getProperties(), worker2.getProperties());
        Assert.assertEquals(worker1.getState(), worker2.getState());
        Assert.assertEquals(worker1.getLastDownTime(), worker2.getLastDownTime());
        // NumOfShards are not serialized
        Assert.assertEquals(0L, worker2.getNumOfShards());
    }

    @Test
    public void testCheckWorkerExceptions() {
        Worker worker1 = getTestWorker(serviceId, groupId, workerId, ipPort);
        Worker worker2 = getTestWorker(serviceId + "1", groupId, workerId, ipPort);
        Worker worker3 = getTestWorker(serviceId, groupId + 1, workerId, ipPort);

        Assert.assertEquals(worker1.getWorkerId(), worker2.getWorkerId());
        Assert.assertThrows(IllegalStateException.class, () -> worker1.match(worker2));
        Assert.assertThrows(IllegalStateException.class, () -> worker1.update(worker2));

        Assert.assertEquals(worker1.getWorkerId(), worker3.getWorkerId());
        Assert.assertThrows(IllegalStateException.class, () -> worker1.match(worker3));
        Assert.assertThrows(IllegalStateException.class, () -> worker1.update(worker3));
    }

    @Test
    public void testUpdateWorkerInfo() {
        // startTime = 0
        Worker worker = getTestWorker(serviceId, groupId, workerId, ipPort);
        Assert.assertEquals(0L, worker.getNumOfShards());
        Assert.assertEquals(1L, worker.getProperties().size());

        Map<String, String> props = new HashMap<>();
        props.put("keyX", "valueX");

        long startTime = 0;
        {
            // Invalid startTime
            Pair<Boolean, Boolean> result = worker.updateInfo(startTime, null, 0);
            Assert.assertFalse(result.getKey()); // restarted
            Assert.assertFalse(result.getValue()); // needPersist
        }
        {
            // Invalid startTime, valid properties
            Pair<Boolean, Boolean> result = worker.updateInfo(startTime, props, 0);
            Assert.assertFalse(result.getKey()); // restarted
            Assert.assertTrue(result.getValue()); // needPersist
        }
        {
            // valid startTime, first time update sTime, the same properties, simulate the first heartbeat
            ++startTime;
            Pair<Boolean, Boolean> result = worker.updateInfo(startTime, props, 0);
            Assert.assertFalse(result.getKey()); // restarted
            Assert.assertFalse(result.getValue()); // needPersist
        }
        {
            // valid startTime, update startTime again, simulate restart event
            ++startTime;
            Pair<Boolean, Boolean> result = worker.updateInfo(startTime, null, 0);
            Assert.assertTrue(result.getKey()); // restarted
            Assert.assertTrue(result.getValue()); // needPersist
        }
        {
            // same startTime, same props, simulate regular non-first heartbeat
            Pair<Boolean, Boolean> result = worker.updateInfo(startTime, props, 0);
            Assert.assertFalse(result.getKey()); // restarted
            Assert.assertFalse(result.getValue()); // needPersist
        }
        {
            // same startTime, different props, simulate regular non-first heartbeat
            props.put("keyA", "valueA");
            Pair<Boolean, Boolean> result = worker.updateInfo(startTime, props, 0);
            Assert.assertFalse(result.getKey()); // restarted
            Assert.assertTrue(result.getValue()); // needPersist
        }
        {
            //  startTime backward, same props, simulate invalid startTime
            --startTime;
            Pair<Boolean, Boolean> result = worker.updateInfo(startTime, props, 0);
            Assert.assertFalse(result.getKey()); // restarted
            Assert.assertFalse(result.getValue()); // needPersist
        }
    }
}
