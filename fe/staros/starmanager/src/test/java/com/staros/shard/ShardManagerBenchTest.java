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

package com.staros.shard;

import com.google.common.collect.Lists;
import com.staros.common.HijackConfig;
import com.staros.common.TestHelper;
import com.staros.proto.JoinMetaGroupInfo;
import com.staros.proto.MetaGroupInfo;
import com.staros.proto.MetaGroupJournalInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.schedule.ShardSchedulerV2;
import com.staros.service.ServiceManager;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.worker.WorkerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ShardManagerBenchTest {

    private static final Logger LOG = LogManager.getLogger(ShardManagerBenchTest.class);

    private WorkerManager workerManager;
    private String serviceId;

    private ShardSchedulerV2 scheduler;
    private HijackConfig hijackTriggerScheduling;
    private TestHelper helper;

    @BeforeClass
    public static void setupForClass() {
        // Turn On MockStarletAgent
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
    }

    @Before
    public void setup() {
        Config.S3_BUCKET = "test-bucket";
        // disable auto schedule
        hijackTriggerScheduling = new HijackConfig("SCHEDULER_TRIGGER_SCHEDULE_WHEN_CREATE_SHARD", "false");
        helper = new TestHelper(this.getClass().getName() + "-serviceTemplateName-0");
        workerManager = helper.getWorkerManager();

        // Setup SchedulerV2
        ServiceManager serviceManager = helper.getServiceManager();
        scheduler = new ShardSchedulerV2(serviceManager, workerManager);
        serviceManager.setShardScheduler(scheduler);
        scheduler.start();

        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        serviceId = helper.getDefaultServiceId();
    }

    @After
    public void tearDown() {
        scheduler.stop();
        hijackTriggerScheduling.reset();
        Config.S3_BUCKET = "";
    }

    @Test
    public void testLargeScaleMetaGroupReplay() {
        int nShards = 16;
        int nShardGroups = 32768;
        // create an empty metaGroup
        long metaGroupId = helper.createMetaGroup(PlacementPolicy.PACK, new ArrayList<>());
        List<Long> groups = new ArrayList<>();
        for (int i = 0; i < nShardGroups; ++i) {
            long groupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
            groups.add(groupId);
            helper.createTestShards(nShards, groupId);
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setJoinInfo(JoinMetaGroupInfo.newBuilder().setMetaGroupId(metaGroupId).build())
                    .addShardGroupIds(groupId)
                    .build();
            helper.getDefaultShardManager().updateMetaGroup(updateInfo);
        }
        // the metaGroup reaches its final state, now test the replayOperation
        MetaGroupInfo metaGroupInfo = helper.getDefaultShardManager().getMetaGroupInfo(metaGroupId);
        Assert.assertEquals(PlacementPolicy.PACK, metaGroupInfo.getPlacementPolicy());
        List<Long> groupsInMetaGroup = metaGroupInfo.getShardGroupIdsList();
        Assert.assertEquals(nShards, groupsInMetaGroup.size());

        int nRepeats = 10;
        UpdateMetaGroupInfo replayUpdateInfo = UpdateMetaGroupInfo.newBuilder()
                .setJoinInfo(JoinMetaGroupInfo.newBuilder().setMetaGroupId(metaGroupId).build())
                .addAllShardGroupIds(groups)
                .build();
        MetaGroupJournalInfo updateJournalInfo = MetaGroupJournalInfo.newBuilder()
                .setMetaGroupInfo(metaGroupInfo)
                .setUpdateInfo(replayUpdateInfo)
                .build();
        long total = 0;
        for (int i = 0; i < nRepeats; ++i) {
            long start = System.currentTimeMillis();
            helper.getDefaultShardManager().replayUpdateMetaGroup(updateJournalInfo);
            total += System.currentTimeMillis() - start;
        }
        long average = total / nRepeats;
        LOG.warn("[TestParams] repeats={} numOfShardGroups={} numOfShardsPerGroup={} [Result] average={}", nRepeats,
                nShardGroups, nShards, average);
        // average time for the operation should be under 500ms
        Assert.assertTrue(average < 500);
    }

    public void testBenchListHashSetInternal(int capacity, double hitRatio) {
        double range = (double) capacity / hitRatio;
        List<Long> list = new ArrayList<>();
        List<Long> allNums = new ArrayList<>();
        for (long i = 0; i < range; ++i) {
            allNums.add(i);
        }
        Collections.shuffle(allNums);
        for (int i = 0; i < capacity; ++i) {
            list.add(allNums.get(i));
        }
        Set<Long> set = new HashSet<>(list);
        int repeats = 1000 * 100;
        int size = allNums.size();

        // start the testing
        long p1 = System.nanoTime();
        for (int i = 0; i < repeats; ++i) {
            list.contains(allNums.get(i % size));
        }
        long p2 = System.nanoTime();
        for (int i = 0; i < repeats; ++i) {
            set.contains(allNums.get(i % size));
        }
        long p3 = System.nanoTime();
        LOG.warn("[Params] capacity={}, fillRatio={}, [Result] listAverage={}us, setAverage={}us ratio={}", capacity,
                hitRatio, (p2 - p1) / repeats, (p3 - p2) / repeats, (p2 - p1) / (p3 - p2));
    }

    @Ignore("Just for performance comparison")
    @Test
    public void testBenchListHashSet() {
        // 1024, 4096, 16384
        List<Integer> capacities = Lists.newArrayList(1 << 14, 1 << 12, 1 << 10);
        List<Double> fillRatio = Lists.newArrayList(0.9, 0.99, 0.999);
        for (int capacity : capacities) {
            for (double f : fillRatio) {
                testBenchListHashSetInternal(capacity, f);
            }
        }
    }
}
