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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/clone/ColocateTableBalancerTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.clone;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.leader.TabletCollector;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@TestMethodOrder(MethodName.class)
public class ColocateTableBalancerTest {
    private static ColocateTableBalancer balancer = ColocateTableBalancer.getInstance();

    private Backend backend1;
    private Backend backend2;
    private Backend backend3;
    private Backend backend4;
    private Backend backend5;
    private Backend backend6;
    private Backend backend7;
    private Backend backend8;
    private Backend backend9;

    private Map<Long, Double> mixLoadScores;

    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        balancer.setStop();
        GlobalStateMgr.getCurrentState().getAlterJobMgr().stop();
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        GlobalStateMgr.getCurrentState().getHeartbeatMgr().setStop();
        GlobalStateMgr.getCurrentState().getTabletScheduler().setStop();
        TabletCollector collector = (TabletCollector) Deencapsulation.getField(GlobalStateMgr.getCurrentState(),
                "tabletCollector");
        collector.setStop();
        ColocateTableBalancer.getInstance().setStop();
    }

    @BeforeEach
    public void setUp() throws Exception {
        backend1 = new Backend(1L, "192.168.1.1", 9050);
        backend2 = new Backend(2L, "192.168.1.2", 9050);
        backend3 = new Backend(3L, "192.168.1.3", 9050);
        backend4 = new Backend(4L, "192.168.1.4", 9050);
        backend5 = new Backend(5L, "192.168.1.5", 9050);
        backend6 = new Backend(6L, "192.168.1.6", 9050);
        // 7,8,9 are on same host
        backend7 = new Backend(7L, "192.168.1.8", 9050);
        backend8 = new Backend(8L, "192.168.1.8", 9050);
        backend9 = new Backend(9L, "192.168.1.8", 9050);

        mixLoadScores = Maps.newHashMap();
        mixLoadScores.put(1L, 0.1);
        mixLoadScores.put(2L, 0.5);
        mixLoadScores.put(3L, 0.4);
        mixLoadScores.put(4L, 0.2);
        mixLoadScores.put(5L, 0.3);
        mixLoadScores.put(6L, 0.6);
        mixLoadScores.put(7L, 0.8);
        mixLoadScores.put(8L, 0.7);
        mixLoadScores.put(9L, 0.9);
    }

    private ColocateTableIndex createColocateIndex(GroupId groupId, List<Long> flatList, int replicationNum) {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();
        List<List<Long>> backendsPerBucketSeq = Lists.partition(flatList, replicationNum);
        colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
        return colocateTableIndex;
    }

    private void addTabletsToScheduler(String dbName, String tableName, boolean setGroupId) {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        OlapTable table =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableName);
        // add its tablet to TabletScheduler
        TabletScheduler tabletScheduler = GlobalStateMgr.getCurrentState().getTabletScheduler();
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex materializedIndex = partition.getDefaultPhysicalPartition().getBaseIndex();
            for (Tablet tablet : materializedIndex.getTablets()) {
                TabletSchedCtx ctx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                            database.getId(),
                            table.getId(),
                            partition.getId(),
                            materializedIndex.getId(),
                            tablet.getId(),
                            System.currentTimeMillis());
                ctx.setOrigPriority(TabletSchedCtx.Priority.LOW);
                if (setGroupId) {
                    ctx.setColocateGroupId(
                                GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(table.getId()));
                }
                tabletScheduler.addTablet(ctx, false);
            }
        }
    }

    @Test
    public void test1MatchGroup() throws Exception {
        starRocksAssert.withDatabase("db1").useDatabase("db1")
                    .withTable("CREATE TABLE db1.tbl(id INT NOT NULL) " +
                                "distributed by hash(`id`) buckets 3 " +
                                "properties('replication_num' = '1', 'colocate_with' = 'group1');");

        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db1");
        OlapTable table =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "tbl");
        addTabletsToScheduler("db1", "tbl", false);

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        List<List<Long>> bl = Lists.newArrayList();
        bl.add(new ArrayList<>(Arrays.asList(1L, 2L, 3L)));
        bl.add(new ArrayList<>(Arrays.asList(1L, 2L, 3L)));
        bl.add(new ArrayList<>(Arrays.asList(1L, 2L, 3L)));
        colocateIndex.addBackendsPerBucketSeq(colocateIndex.getGroup(table.getId()), Lists.newArrayList(bl));

        // test if group is unstable when all its tablets are in TabletScheduler
        long tableId = table.getId();
        ColocateTableBalancer colocateTableBalancer = ColocateTableBalancer.getInstance();
        colocateTableBalancer.runAfterCatalogReady();
        GroupId groupId = GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(tableId);
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getColocateTableIndex().isGroupUnstable(groupId));

        // clean
        colocateIndex.removeTable(table.getId(), table, false);
    }

    @Test
    public void test3RepairWithBadReplica() throws Exception {
        Config.tablet_sched_disable_colocate_overall_balance = true;
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        starRocksAssert.withDatabase("db3").useDatabase("db3")
                    .withTable("CREATE TABLE db3.tbl3(id INT NOT NULL) " +
                                "distributed by hash(`id`) buckets 1 " +
                                "properties('replication_num' = '1', 'colocate_with' = 'group3');");

        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db3");
        OlapTable table =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "tbl3");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();

        List<Partition> partitions = Lists.newArrayList(table.getPartitions());
        LocalTablet tablet = (LocalTablet) partitions.get(0).getDefaultPhysicalPartition().getBaseIndex().getTablets().get(0);
        tablet.getImmutableReplicas().get(0).setBad(true);
        ColocateTableBalancer colocateTableBalancer = ColocateTableBalancer.getInstance();
        long oldVal = Config.tablet_sched_repair_delay_factor_second;
        try {
            Config.tablet_sched_repair_delay_factor_second = -1;
            // the created pseudo min cluster can only have backend on the same host, so we can only create table with
            // single replica, we need to open this test switch to test the behavior of bad replica balance
            ColocateTableBalancer.ignoreSingleReplicaCheck = true;
            // call twice to trigger the real balance action
            colocateTableBalancer.runAfterCatalogReady();
            colocateTableBalancer.runAfterCatalogReady();
            TabletScheduler tabletScheduler = GlobalStateMgr.getCurrentState().getTabletScheduler();
            List<List<String>> result = tabletScheduler.getPendingTabletsInfo(100);
            System.out.println(result);
            Assertions.assertEquals(result.get(0).get(0), Long.toString(tablet.getId()));
            Assertions.assertEquals(result.get(0).get(3), "COLOCATE_REDUNDANT");
        } finally {
            Config.tablet_sched_repair_delay_factor_second = oldVal;
            Config.tablet_sched_disable_colocate_overall_balance = false;
        }
    }

    @Test
    public void testRepairPrecedeBalance(@Mocked SystemInfoService infoService,
                                         @Mocked ClusterLoadStatistic statistic,
                                         @Mocked Backend myBackend1,
                                         @Mocked Backend myBackend2,
                                         @Mocked Backend myBackend3,
                                         @Mocked Backend myBackend4,
                                         @Mocked Backend myBackend5) {
        new Expectations() {
            {
                // backend1 is available
                infoService.getBackend(1L);
                result = myBackend1;
                minTimes = 0;
                myBackend1.isAvailable();
                result = true;
                minTimes = 0;
                myBackend1.getHost();
                result = "192.168.0.111";

                // backend2 is available
                infoService.getBackend(2L);
                result = myBackend2;
                minTimes = 0;
                myBackend2.isAvailable();
                result = true;
                minTimes = 0;
                myBackend2.getHost();
                result = "192.168.0.112";

                // backend3 is available
                infoService.getBackend(3L);
                result = myBackend3;
                minTimes = 0;
                myBackend3.isAvailable();
                result = true;
                minTimes = 0;
                myBackend3.getHost();
                result = "192.168.0.113";

                // backend4 is available
                infoService.getBackend(4L);
                result = myBackend4;
                minTimes = 0;
                myBackend4.isAvailable();
                result = true;
                minTimes = 0;
                myBackend4.getHost();
                result = "192.168.0.114";

                // backend5 not available, and dead for a long time
                infoService.getBackend(5L);
                result = myBackend5;
                minTimes = 0;
                myBackend5.isAvailable();
                result = false;
                minTimes = 0;
                myBackend5.isAlive();
                result = false;
                minTimes = 0;
                myBackend5.getLastUpdateMs();
                result = System.currentTimeMillis() - Config.tablet_sched_colocate_be_down_tolerate_time_s * 1000 * 2;
                minTimes = 0;
                myBackend5.getHost();
                result = "192.168.0.115";
            }
        };

        FeConstants.runningUnitTest = true;

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        GroupId groupId = new GroupId(10005, 10006);
        short replicationNUm = 3;
        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(1L, 2L, 3L, 1L, 2L, 3L, 1L, 2L, 4L, 1L, 2L, 5L), replicationNUm);
        setGroup2Schema(groupId, colocateTableIndex, 4, replicationNUm);

        Set<Long> unavailableBeIds = Sets.newHashSet(5L);
        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        List<Long> availBackendIds = Lists.newArrayList(1L, 2L, 3L, 4L);
        boolean changed = false;
        ColocateTableBalancer.disableRepairPrecedence = true;
        changed = (Boolean) Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, unavailableBeIds, availBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        Assertions.assertTrue(changed);
        System.out.println(balancedBackendsPerBucketSeq);
        List<List<Long>> expected = Lists.partition(
                    Lists.newArrayList(4L, 2L, 3L, 1L, 2L, 3L, 1L, 3L, 4L, 1L, 2L, 4L), 3);
        Assertions.assertEquals(expected, balancedBackendsPerBucketSeq);

        ColocateTableBalancer.disableRepairPrecedence = false;
        balancedBackendsPerBucketSeq.clear();
        changed = (Boolean) Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, unavailableBeIds, availBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        Assertions.assertTrue(changed);
        System.out.println(balancedBackendsPerBucketSeq);
        expected = Lists.partition(
                    Lists.newArrayList(1L, 2L, 3L, 1L, 2L, 3L, 1L, 2L, 4L, 1L, 2L, 4L), 3);
        Assertions.assertEquals(expected, balancedBackendsPerBucketSeq);
    }

    @Test
    public void testPerGroupBalance(@Mocked SystemInfoService infoService,
                                    @Mocked ClusterLoadStatistic statistic) throws InterruptedException {
        new Expectations() {
            {
                // try to fix the unstable test
                // java.util.ConcurrentModificationException
                //    at mockit.internal.expectations.state.ExecutingTest.isInjectableMock(ExecutingTest.java:142)
                //    at mockit.internal.expectations.state.ExecutingTest.addInjectableMock(ExecutingTest.java:137)
                //    at com.starrocks.clone.ColocateTableBalancerTest$2.<init>(ColocateTableBalancerTest.java:342)
                //    at com.starrocks.clone.ColocateTableBalancerTest.testPerGroupBalance(ColocateTableBalancerTest.java:340)
                //
                // this exception happens at the internal of the mockit, probably a bug of mockit
                // need to use concurrent safe list for mockit.internal.expectations.state.ExecutingTest#injectableMocks
                // because mockit itself will start some background thread to clean `injectableMocks` which will have
                // conflict with our test thread, here is just a workaround, wait for a while to avoid that.
                Thread.sleep(2000);
                infoService.getBackend(1L);
                result = backend1;
                minTimes = 0;
                infoService.getBackend(2L);
                result = backend2;
                minTimes = 0;
                infoService.getBackend(3L);
                result = backend3;
                minTimes = 0;
                infoService.getBackend(4L);
                result = backend4;
                minTimes = 0;
                infoService.getBackend(5L);
                result = backend5;
                minTimes = 0;
                infoService.getBackend(6L);
                result = backend6;
                minTimes = 0;
                infoService.getBackend(7L);
                result = backend7;
                minTimes = 0;
                infoService.getBackend(8L);
                result = backend8;
                minTimes = 0;
                infoService.getBackend(9L);
                result = backend9;
                minTimes = 0;

                statistic.getBackendLoadStatistic(anyLong);
                result = null;
                minTimes = 0;
            }
        };

        GroupId groupId = new GroupId(10000, 10001);
        List<Column> distributionCols = Lists.newArrayList();
        distributionCols.add(new Column("k1", Type.INT));
        ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId, distributionCols, 5, (short) 3);
        Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
        group2Schema.put(groupId, groupSchema);

        // 1. balance an imbalanced group
        // [[1, 2, 3], [4, 1, 2], [3, 4, 1], [2, 3, 4], [1, 2, 3]]
        FeConstants.runningUnitTest = true;
        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L), 3);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);

        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        List<Long> allAvailBackendIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        boolean changed = (Boolean) Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, new HashSet<Long>(), allAvailBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        List<List<Long>> expected = Lists.partition(
                    Lists.newArrayList(9L, 5L, 3L, 4L, 6L, 8L, 7L, 6L, 1L, 2L, 9L, 4L, 1L, 2L, 3L), 3);
        Assertions.assertTrue(changed);
        Assertions.assertEquals(expected, balancedBackendsPerBucketSeq);

        // 2. balance an already balanced group
        colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(9L, 8L, 7L, 8L, 6L, 5L, 9L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L), 3);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);
        balancedBackendsPerBucketSeq.clear();
        changed = (Boolean) Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, new HashSet<Long>(), allAvailBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        System.out.println(balancedBackendsPerBucketSeq);
        Assertions.assertFalse(changed);
        Assertions.assertTrue(balancedBackendsPerBucketSeq.isEmpty());

        try {
            Thread.sleep(1000L);
        } catch (Exception e) {
            e.printStackTrace();
        }

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds();
    }

    @Test
    public void testOverallGroupBalance(@Mocked SystemInfoService infoService,
                                        @Mocked ClusterLoadStatistic statistic) throws InterruptedException {
        new Expectations() {
            {
                infoService.getBackend(1L);
                result = backend1;
                minTimes = 0;
                infoService.getBackend(2L);
                result = backend2;
                minTimes = 0;
                infoService.getBackend(3L);
                result = backend3;
                minTimes = 0;
                infoService.getBackend(4L);
                result = backend4;
                minTimes = 0;
                infoService.getBackend(5L);
                result = backend5;
                minTimes = 0;

                statistic.getBackendLoadStatistic(anyLong);
                result = null;
                minTimes = 0;

                infoService.getAvailableBackendIds();
                result = Lists.newArrayList(1L, 2L, 3L, 4L, 5L);
                minTimes = 0;

                infoService.getBackendHostById(1L);
                result = backend1.getHost();
                minTimes = 0;

                infoService.getBackendHostById(2L);
                result = backend2.getHost();
                minTimes = 0;

                infoService.getBackendHostById(3L);
                result = backend3.getHost();
                minTimes = 0;

                infoService.getBackendHostById(4L);
                result = backend4.getHost();
                minTimes = 0;

                infoService.getBackendHostById(5L);
                result = backend5.getHost();
                minTimes = 0;
            }
        };

        new MockUp<ColocateTableIndex>() {
            @Mock
            public int getNumOfTabletsPerBucket(GroupId groupId) {
                return 1;
            }
        };

        new MockUp<TabletScheduler>() {
            @Mock
            public synchronized Map<GroupId, Long> getTabletsNumInScheduleForEachCG() {
                return Maps.newHashMap();
            }
        };

        GroupId groupId1 = new GroupId(10000, 10001);
        GroupId groupId2 = new GroupId(10010, 10011);
        // init group1
        List<GroupId> groupIds = Arrays.asList(groupId1, groupId2);
        Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
        for (GroupId groupId : groupIds) {
            List<Column> distributionCols = Lists.newArrayList();
            distributionCols.add(new Column("k1", Type.INT));
            ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId, distributionCols, 3, (short) 1);
            group2Schema.put(groupId, groupSchema);
        }
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        // For group 1, bucket: 3, replication_num: 1, backend list per bucket seq: [[1], [2], [3]]
        colocateTableIndex.addBackendsPerBucketSeq(groupId1,
                    Lists.partition(Lists.newArrayList(1L, 2L, 3L), 1));
        // For group 2, bucket: 3, replication_num: 1, backend list per bucket seq: [[1], [2], [3]]
        colocateTableIndex.addBackendsPerBucketSeq(groupId2,
                    Lists.partition(Lists.newArrayList(1L, 2L, 3L), 1));
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);
        Multimap<GroupId, Long> group2Tables = ArrayListMultimap.create();
        group2Tables.put(groupId1, 20001L);
        group2Tables.put(groupId2, 20002L);
        Deencapsulation.setField(colocateTableIndex, "group2Tables", group2Tables);

        // replicas of group 1 and group 2 only distribute on backends 1, 2, 3, and each backend of these backends
        // has one replica of the two groups, so from every group's view, it's already balanced, but from overview,
        // backend 4 and 5 has zero replica distributed, here we test overall balance process works
        List<Long> availableBackends = infoService.getAvailableBackendIds();
        System.out.println(availableBackends);
        Deencapsulation.invoke(balancer, "relocateAndBalanceAllGroups");
        List<List<Long>> list1 = colocateTableIndex.getBackendsPerBucketSeq(groupId1);
        List<List<Long>> list2 = colocateTableIndex.getBackendsPerBucketSeq(groupId2);
        System.out.println(list1);
        System.out.println(list2);
        // TODO: find the reason why the mock record doesn't take effect
        if (!availableBackends.isEmpty()) {
            // backend id -> count
            Map<Long, Integer> result = Maps.newHashMap();
            list1.forEach(e -> result.merge(e.get(0), 1, Integer::sum));
            list2.forEach(e -> result.merge(e.get(0), 1, Integer::sum));
            System.out.println(result);
            // totally 6 replicas, after adding 2 backends and overall balance,
            // every backend should have 1 replica, except one
            Assertions.assertEquals(Lists.newArrayList(1, 1, 1, 1, 2),
                        result.values().stream().sorted().collect(Collectors.toList()));
        }
    }

    private void setGroup2Schema(GroupId groupId, ColocateTableIndex colocateTableIndex,
                                 int bucketNum, short replicationNum) {
        List<Column> distributionCols = Lists.newArrayList();
        distributionCols.add(new Column("k1", Type.INT));
        ColocateGroupSchema groupSchema =
                    new ColocateGroupSchema(groupId, distributionCols, bucketNum, replicationNum);
        Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
        group2Schema.put(groupId, groupSchema);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);
    }

    @Test
    public void testBalanceWithOnlyOneAvailableBackend(@Mocked SystemInfoService infoService,
                                                       @Mocked ClusterLoadStatistic statistic,
                                                       @Mocked Backend myBackend2,
                                                       @Mocked Backend myBackend3,
                                                       @Mocked Backend myBackend4) {
        new Expectations() {
            {
                // backend2 is available
                infoService.getBackend(2L);
                result = myBackend2;
                minTimes = 0;
                myBackend2.isAvailable();
                result = true;
                minTimes = 0;

                // backend3 not available, and dead for a long time
                infoService.getBackend(3L);
                result = myBackend3;
                minTimes = 0;
                myBackend3.isAvailable();
                result = false;
                minTimes = 0;
                myBackend3.isAlive();
                result = false;
                minTimes = 0;
                myBackend3.getLastUpdateMs();
                result = System.currentTimeMillis() - Config.tablet_sched_colocate_be_down_tolerate_time_s * 1000 * 2;
                minTimes = 0;

                // backend4 not available, and dead for a short time
                infoService.getBackend(4L);
                result = myBackend4;
                minTimes = 0;
                myBackend4.isAvailable();
                result = false;
                minTimes = 0;
                myBackend4.isAlive();
                result = false;
                minTimes = 0;
                myBackend4.getLastUpdateMs();
                result = System.currentTimeMillis();
                minTimes = 0;
            }
        };

        // To avoid "missing x invocation to..." error
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        GroupId groupId = new GroupId(10000, 10001);
        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(4L, 2L, 3L, 4L, 2L, 3L, 4L, 2L, 3L), 3);
        setGroup2Schema(groupId, colocateTableIndex, 3, (short) 3);

        Set<Long> unavailableBeIds = Sets.newHashSet(3L, 4L);
        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        List<Long> availBackendIds = Lists.newArrayList(2L);

        boolean changed = (Boolean) Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, unavailableBeIds, availBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        // in this case, there is only on available backend, no need to make balancing decision.
        Assertions.assertFalse(changed);
    }

    @Test
    public void testBalanceWithSingleReplica(@Mocked SystemInfoService infoService,
                                             @Mocked ClusterLoadStatistic statistic,
                                             @Mocked Backend myBackend2,
                                             @Mocked Backend myBackend3,
                                             @Mocked Backend myBackend4) {
        new Expectations() {
            {
                // backend2 is available
                infoService.getBackend(2L);
                result = myBackend2;
                minTimes = 0;
                myBackend2.isAvailable();
                result = true;
                minTimes = 0;
                myBackend2.getHost();
                result = "192.168.0.112";
                minTimes = 0;

                // backend3 is available
                infoService.getBackend(3L);
                result = myBackend3;
                minTimes = 0;
                myBackend3.isAvailable();
                result = true;
                minTimes = 0;
                myBackend3.getHost();
                result = "192.168.0.113";
                minTimes = 0;

                // backend4 not available, and dead for a short time
                infoService.getBackend(4L);
                result = myBackend4;
                minTimes = 0;
                myBackend4.isAvailable();
                result = false;
                minTimes = 0;
                myBackend4.isAlive();
                result = true;
                minTimes = 0;
                myBackend4.getLastUpdateMs();
                result = System.currentTimeMillis();
                minTimes = 0;
                myBackend4.getHost();
                result = "192.168.0.114";
                minTimes = 0;
            }
        };

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        GroupId groupId = new GroupId(10000, 10001);
        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(4L, 2L, 3L, 4L, 2L, 3L, 4L, 2L, 3L), 1);
        setGroup2Schema(groupId, colocateTableIndex, 9, (short) 1);

        Set<Long> unavailableBeIds = Sets.newHashSet(4L);
        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        List<Long> availBackendIds = Lists.newArrayList(2L, 3L);
        boolean changed = false;

        changed = (Boolean) Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, unavailableBeIds, availBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        // there is unavailable backend, but the replication number is 1 and replicas on available backends are
        // already balanced, so the bucket sequence will remain unchanged.
        Assertions.assertFalse(changed);

        colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(2L, 2L, 4L, 2L, 2L, 4L, 3L, 2L, 4L), 1);
        setGroup2Schema(groupId, colocateTableIndex, 9, (short) 1);
        changed = (Boolean) Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, unavailableBeIds, availBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        Assertions.assertTrue(changed);
        System.out.println(balancedBackendsPerBucketSeq);
        List<List<Long>> expected = Lists.partition(
                    Lists.newArrayList(3L, 3L, 4L, 2L, 2L, 4L, 3L, 2L, 4L), 1);
        // there is unavailable backend, but the replication number is 1 and replicas on available backends are
        // not balanced, check the balancer actually working.
        Assertions.assertEquals(expected, balancedBackendsPerBucketSeq);

        new Expectations() {
            {
                myBackend4.isDecommissioned();
                result = true;
                minTimes = 0;
            }
        };
        balancedBackendsPerBucketSeq = Lists.newArrayList();
        colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(2L, 2L, 4L, 2L, 2L, 4L, 3L, 2L, 4L), 1);
        setGroup2Schema(groupId, colocateTableIndex, 9, (short) 1);
        changed = (Boolean) Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, unavailableBeIds, availBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        Assertions.assertTrue(changed);
        System.out.println(balancedBackendsPerBucketSeq);
        List<List<Long>> expected3 = Lists.partition(
                    Lists.newArrayList(2L, 2L, 3L, 2L, 2L, 3L, 3L, 2L, 3L), 1);
        // there is unavailable backend, but the replication number is 1 and there is decommissioned backend,
        // so we need to do relocation first.
        Assertions.assertEquals(expected3, balancedBackendsPerBucketSeq);
    }

    @Test
    public void testFixBalanceEndlessLoop(@Mocked SystemInfoService infoService,
                                          @Mocked ClusterLoadStatistic statistic) {
        new Expectations() {
            {
                infoService.getBackend(1L);
                result = backend1;
                minTimes = 0;
                infoService.getBackend(2L);
                result = backend2;
                minTimes = 0;
                infoService.getBackend(3L);
                result = backend3;
                minTimes = 0;
                infoService.getBackend(4L);
                result = backend4;
                minTimes = 0;
                infoService.getBackend(5L);
                result = backend5;
                minTimes = 0;
                infoService.getBackend(6L);
                result = backend6;
                minTimes = 0;
                infoService.getBackend(7L);
                result = backend7;
                minTimes = 0;
                infoService.getBackend(8L);
                result = backend8;
                minTimes = 0;
                infoService.getBackend(9L);
                result = backend9;
                minTimes = 0;
                statistic.getBackendLoadStatistic(anyLong);
                result = null;
                minTimes = 0;
            }
        };
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        GroupId groupId = new GroupId(10000, 10001);
        List<Column> distributionCols = Lists.newArrayList();
        distributionCols.add(new Column("k1", Type.INT));
        ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId, distributionCols, 5, (short) 1);
        Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
        group2Schema.put(groupId, groupSchema);

        // 1. only one available backend
        // [[7], [7], [7], [7], [7]]
        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(7L, 7L, 7L, 7L, 7L), 3);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);

        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        List<Long> allAvailBackendIds = Lists.newArrayList(7L);
        boolean changed =
                    Deencapsulation.invoke(balancer, "doRelocateAndBalance",
                                groupId, new HashSet<Long>(), allAvailBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        Assertions.assertFalse(changed);

        // 2. all backends are checked but this round is not changed
        // [[7], [7], [7], [7], [7]]
        // and add new backends 8, 9 that are on the same host with 7
        colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(7L, 7L, 7L, 7L, 7L), 3);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);

        balancedBackendsPerBucketSeq = Lists.newArrayList();
        allAvailBackendIds = Lists.newArrayList(7L, 8L, 9L);
        changed =
                    Deencapsulation.invoke(balancer, "doRelocateAndBalance",
                                groupId, new HashSet<Long>(), allAvailBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        Assertions.assertFalse(changed);

        // 3. all backends are not available
        colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(7L, 7L, 7L, 7L, 7L), 3);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);
        balancedBackendsPerBucketSeq = Lists.newArrayList();
        allAvailBackendIds = Lists.newArrayList();
        Set<Long> unAvailableBackendIds = Sets.newHashSet(7L);
        changed = Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, unAvailableBackendIds, allAvailBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        Assertions.assertFalse(changed);
    }

    @Test
    public void testGetSortedBackendReplicaNumPairs(@Mocked ClusterLoadStatistic statistic) {
        new Expectations() {
            {
                statistic.getBackendLoadStatistic(anyLong);
                result = new Delegate<BackendLoadStatistic>() {
                    BackendLoadStatistic delegate(Long beId) {
                        return new FakeBackendLoadStatistic(beId, null, null, null);
                    }
                };
                minTimes = 0;
            }
        };

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        // all buckets are on different be
        List<Long> allAvailBackendIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        Set<Long> unavailBackendIds = Sets.newHashSet(9L);
        List<Long> flatBackendsPerBucketSeq = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        List<Map.Entry<Long, Long>> backends = Deencapsulation.invoke(balancer, "getSortedBackendReplicaNumPairs",
                    allAvailBackendIds, unavailBackendIds, statistic, flatBackendsPerBucketSeq);
        long[] backendIds = backends.stream().mapToLong(Map.Entry::getKey).toArray();
        Assertions.assertArrayEquals(new long[] {7L, 8L, 6L, 2L, 3L, 5L, 4L, 1L}, backendIds);

        // 0,1 bucket on same be and 5, 6 on same be
        flatBackendsPerBucketSeq = Lists.newArrayList(1L, 1L, 3L, 4L, 5L, 6L, 7L, 7L, 9L);
        backends = Deencapsulation
                    .invoke(balancer, "getSortedBackendReplicaNumPairs", allAvailBackendIds, unavailBackendIds,
                                statistic, flatBackendsPerBucketSeq);
        backendIds = backends.stream().mapToLong(Map.Entry::getKey).toArray();
        Assertions.assertArrayEquals(new long[] {7L, 1L, 6L, 3L, 5L, 4L, 8L, 2L}, backendIds);
    }

    public final class FakeBackendLoadStatistic extends BackendLoadStatistic {
        public FakeBackendLoadStatistic(long beId, String clusterName, SystemInfoService infoService,
                                        TabletInvertedIndex invertedIndex) {
            super(beId, clusterName, infoService, invertedIndex);
        }

        @Override
        public double getMixLoadScore() {
            return mixLoadScores.get(getBeId());
        }
    }

    @Test
    public void testGetBeSeqIndexes() {
        List<Long> flatBackendsPerBucketSeq = Lists.newArrayList(1L, 2L, 2L, 3L, 4L, 2L);
        List<Integer> indexes = Deencapsulation.invoke(balancer,
                    "getBeSeqIndexes", flatBackendsPerBucketSeq, 2L);
        Assertions.assertArrayEquals(new int[] {1, 2, 5}, indexes.stream().mapToInt(i -> i).toArray());
        System.out.println("backend1 id is " + backend1.getId());
    }

    @Test
    public void testGetUnavailableBeIdsInGroup() {
        GroupId groupId = new GroupId(10000, 10001);
        List<Long> allBackendsInGroup = Lists.newArrayList(1L, 2L, 3L, 4L, 5L);
        List<List<Long>> backendsPerBucketSeq = new ArrayList<>();
        backendsPerBucketSeq.add(allBackendsInGroup);
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();
        SystemInfoService infoService = new SystemInfoService();
        colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);

        Backend be2 = new Backend(2L, "", 2002);
        be2.setAlive(true);
        infoService.replayAddBackend(be2);

        Backend be3 = new Backend(3L, "", 3003);
        be3.setAlive(false);
        be3.setLastUpdateMs(System.currentTimeMillis() - Config.tablet_sched_colocate_be_down_tolerate_time_s * 1000 * 2);
        infoService.replayAddBackend(be3);

        Backend be4 = new Backend(4L, "", 4004);
        be4.setAlive(false);
        be4.setLastUpdateMs(System.currentTimeMillis());
        infoService.replayAddBackend(be4);

        Backend be5 = new Backend(5L, "", 5005);
        be5.setDecommissioned(true);
        infoService.replayAddBackend(be5);

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();

        Set<Long> unavailableBeIds = Deencapsulation
                    .invoke(balancer, "getUnavailableBeIdsInGroup", infoService, colocateTableIndex, groupId);
        Assertions.assertArrayEquals(new long[] {1L, 3L, 5L},
                    unavailableBeIds.stream().mapToLong(i -> i).sorted().toArray());
    }

    @Test
    public void testGetAvailableBeIds(@Mocked SystemInfoService infoService,
                                      @Mocked Backend myBackend2,
                                      @Mocked Backend myBackend3,
                                      @Mocked Backend myBackend4,
                                      @Mocked Backend myBackend5) {
        List<Long> clusterBackendIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L);
        new Expectations() {
            {
                infoService.getBackendIds(false);
                result = clusterBackendIds;
                minTimes = 0;

                infoService.getBackend(1L);
                result = null;
                minTimes = 0;

                // backend2 is available
                infoService.getBackend(2L);
                result = myBackend2;
                minTimes = 0;
                myBackend2.isAvailable();
                result = true;
                minTimes = 0;

                // backend3 not available, and dead for a long time
                infoService.getBackend(3L);
                result = myBackend3;
                minTimes = 0;
                myBackend3.isAvailable();
                result = false;
                minTimes = 0;
                myBackend3.isAlive();
                result = false;
                minTimes = 0;
                myBackend3.getLastUpdateMs();
                result = System.currentTimeMillis() - Config.tablet_sched_colocate_be_down_tolerate_time_s * 1000 * 2;
                minTimes = 0;

                // backend4 available, not alive but dead for a short time
                infoService.getBackend(4L);
                result = myBackend4;
                minTimes = 0;
                myBackend4.isAvailable();
                result = false;
                minTimes = 0;
                myBackend4.isAlive();
                result = false;
                minTimes = 0;
                myBackend4.getLastUpdateMs();
                result = System.currentTimeMillis();
                minTimes = 0;

                // backend5 not available, and in decommission
                infoService.getBackend(5L);
                result = myBackend5;
                minTimes = 0;
                myBackend5.isAvailable();
                result = false;
                minTimes = 0;
                myBackend5.isAlive();
                result = true;
                minTimes = 0;
                myBackend5.isDecommissioned();
                result = true;
                minTimes = 0;
            }
        };

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        List<Long> availableBeIds = Deencapsulation.invoke(balancer, "getAvailableBeIds", infoService);
        Assertions.assertArrayEquals(new long[] {2L, 4L}, availableBeIds.stream().mapToLong(i -> i).sorted().toArray());
    }

    @Test
    public void testDropBackend(@Mocked SystemInfoService infoService, @Mocked ClusterLoadStatistic statistic) {
        new Expectations() {
            {
                infoService.getBackend(1L);
                result = backend1;
                minTimes = 0;
                infoService.getBackend(2L);
                result = backend2;
                minTimes = 0;
                infoService.getBackend(3L);
                result = backend3;
                minTimes = 0;
                infoService.getBackend(4L);
                result = backend4;
                minTimes = 0;
                infoService.getBackend(5L);
                result = backend5;
                minTimes = 0;
                infoService.getBackend(6L);
                result = backend6;
                minTimes = 0;
                infoService.getBackend(7L);
                result = backend7;
                minTimes = 0;
                infoService.getBackend(8L);
                result = backend8;
                minTimes = 0;
                // backend is dropped
                infoService.getBackend(9L);
                result = null;
                minTimes = 0;

                statistic.getBackendLoadStatistic(anyLong);
                result = new Delegate<BackendLoadStatistic>() {
                    BackendLoadStatistic delegate(Long beId) {
                        return new FakeBackendLoadStatistic(beId, null, null, null);
                    }
                };
                minTimes = 0;
            }
        };
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        GroupId groupId = new GroupId(10000, 10001);
        List<Column> distributionCols = Lists.newArrayList();
        distributionCols.add(new Column("k1", Type.INT));
        ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId, distributionCols, 5, (short) 3);
        Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
        group2Schema.put(groupId, groupSchema);

        // group is balanced before backend 9 is dropped
        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId,
                    Lists.newArrayList(9L, 8L, 7L, 8L, 6L, 5L, 9L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L), 3);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);
        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        Set<Long> unavailableBeIds = Sets.newHashSet(9L);
        List<Long> allAvailBackendIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        boolean changed = (Boolean) Deencapsulation
                    .invoke(balancer, "doRelocateAndBalance", groupId, unavailableBeIds, allAvailBackendIds,
                                colocateTableIndex, infoService, statistic, balancedBackendsPerBucketSeq);
        System.out.println(balancedBackendsPerBucketSeq);
        Assertions.assertTrue(changed);
        List<List<Long>> expected = Lists.partition(
                    Lists.newArrayList(5L, 8L, 7L, 8L, 6L, 5L, 6L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L), 3);
        Assertions.assertEquals(expected, balancedBackendsPerBucketSeq);
    }

    @Test
    public void testSystemStable() throws Exception {
        ColocateTableBalancer balancer = ColocateTableBalancer.getInstance();
        Backend backend1 = new Backend(100001L, "192.168.0.1", 9050);
        backend1.setAlive(true);
        Backend backend2 = new Backend(100002L, "192.168.0.2", 9050);
        backend2.setAlive(true);
        SystemInfoService infoService = new SystemInfoService();
        infoService.replayAddBackend(backend1);
        infoService.replayAddBackend(backend2);

        Assertions.assertFalse(balancer.isSystemStable(infoService));
        Assertions.assertFalse(balancer.isSystemStable(infoService));
        // set stable last time to 1s, and sleep 1s, the system becomes to stable
        Config.tablet_sched_colocate_balance_wait_system_stable_time_s = 1;
        System.out.println("before sleep, time: " + System.currentTimeMillis()
                    + "alive backend is: " + infoService.getBackendIds(true));
        Thread.sleep(2000L);
        System.out.println("after sleep, time: " + System.currentTimeMillis()
                    + "alive backend is: " + infoService.getBackendIds(true));
        Assertions.assertTrue(balancer.isSystemStable(infoService));
        Assertions.assertTrue(balancer.isSystemStable(infoService));

        // one backend is changed to not alive, the system becomes to unstable
        backend1.setAlive(false);
        Assertions.assertFalse(balancer.isSystemStable(infoService));
        Assertions.assertFalse(balancer.isSystemStable(infoService));
        System.out.println("before sleep, time: " + System.currentTimeMillis()
                    + "alive backend is: " + infoService.getBackendIds(true));
        Thread.sleep(2000L);
        System.out.println("after sleep, time: " + System.currentTimeMillis()
                    + "alive backend is: " + infoService.getBackendIds(true));
        Assertions.assertTrue(balancer.isSystemStable(infoService));
    }
}
