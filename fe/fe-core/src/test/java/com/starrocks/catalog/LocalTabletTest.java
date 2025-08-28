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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/TabletTest.java

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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.clone.TabletChecker;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class LocalTabletTest {

    private LocalTablet tablet;
    private Replica replica1;
    private Replica replica2;
    private Replica replica3;

    private TabletInvertedIndex invertedIndex;

    @Mocked
    private SystemInfoService infoService;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private NodeMgr nodeMgr;

    @BeforeEach
    public void makeTablet() {
        invertedIndex = new TabletInvertedIndex();
        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                GlobalStateMgr.isCheckpointThread();
                minTimes = 0;
                result = false;

                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = infoService;
            }
        };

        tablet = new LocalTablet(1);
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, TStorageMedium.HDD);
        invertedIndex.addTablet(1, tabletMeta);
        replica1 = new Replica(1L, 1L, 100L, 0, 200000L, 3000L, ReplicaState.NORMAL, 0, 0);
        replica2 = new Replica(2L, 2L, 100L, 0, 200001L, 3001L, ReplicaState.NORMAL, 0, 0);
        replica3 = new Replica(3L, 3L, 100L, 0, 200002L, 3002L, ReplicaState.NORMAL, 0, 0);
        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        infoService = globalStateMgr.getNodeMgr().getClusterInfo();
        infoService.addBackend(new Backend(10001L, "host1", 9050));
        infoService.addBackend(new Backend(10002L, "host2", 9050));
    }

    @Test
    public void getMethodTest() {
        Assertions.assertEquals(replica1, tablet.getReplicaById(replica1.getId()));
        Assertions.assertEquals(replica2, tablet.getReplicaById(replica2.getId()));
        Assertions.assertEquals(replica3, tablet.getReplicaById(replica3.getId()));

        Assertions.assertEquals(3, tablet.getImmutableReplicas().size());
        Assertions.assertEquals(replica1, tablet.getReplicaByBackendId(replica1.getBackendId()));
        Assertions.assertEquals(replica2, tablet.getReplicaByBackendId(replica2.getBackendId()));
        Assertions.assertEquals(replica3, tablet.getReplicaByBackendId(replica3.getBackendId()));

        Assertions.assertEquals(600003L, tablet.getDataSize(false));
        Assertions.assertEquals(200002L, tablet.getDataSize(true));
        Assertions.assertEquals(3002L, tablet.getRowCount(100));
    }

    @Test
    public void deleteReplicaTest() {
        // delete replica1
        Assertions.assertTrue(tablet.deleteReplicaByBackendId(replica1.getBackendId()));
        Assertions.assertNull(tablet.getReplicaById(replica1.getId()));

        // err: re-delete replica1
        Assertions.assertFalse(tablet.deleteReplicaByBackendId(replica1.getBackendId()));
        Assertions.assertFalse(tablet.deleteReplica(replica1));
        Assertions.assertNull(tablet.getReplicaById(replica1.getId()));

        // delete replica2
        Assertions.assertTrue(tablet.deleteReplica(replica2));
        Assertions.assertEquals(1, tablet.getImmutableReplicas().size());

        // clear replicas
        tablet.clearReplica();
        Assertions.assertEquals(0, tablet.getImmutableReplicas().size());
    }

    @Test
    public void testGetColocateHealthStatus() throws Exception {
        LocalTablet tablet = new LocalTablet();
        Replica versionIncompleteReplica = new Replica(1L, 10001L, 8,
                -1, 10, 10, ReplicaState.NORMAL, 9, 8);
        Replica normalReplica = new Replica(1L, 10002L, 9,
                -1, 10, 10, ReplicaState.NORMAL, -1, 9);
        tablet.addReplica(versionIncompleteReplica, false);
        tablet.addReplica(normalReplica, false);
        Assertions.assertEquals(LocalTablet.TabletHealthStatus.COLOCATE_REDUNDANT,
                TabletChecker.getColocateTabletHealthStatus(
                        tablet, 9, 1, Sets.newHashSet(10002L)));
    }


    @Test
    public void testGetBackends() throws Exception {
        LocalTablet tablet = new LocalTablet();

        Replica replica1 = new Replica(1L, 10001L, 8,
                -1, 10, 10, ReplicaState.NORMAL, 9, 8);
        Replica replica2 = new Replica(1L, 10002L, 9,
                -1, 10, 10, ReplicaState.NORMAL, -1, 9);
        tablet.addReplica(replica1, false);
        tablet.addReplica(replica2, false);

        Assertions.assertEquals(tablet.getBackends().size(), 2);

    }

    @Test
    public void testGetReplicaInfos() {
        LocalTablet tablet = new LocalTablet();

        Replica replica1 = new Replica(1L, 10001L, 8,
                -1, 10, 10, ReplicaState.NORMAL, 9, 8);
        Replica replica2 = new Replica(1L, 10002L, 9,
                -1, 10, 10, ReplicaState.NORMAL, -1, 9);
        tablet.addReplica(replica1, false);
        tablet.addReplica(replica2, false);

        String infos = tablet.getReplicaInfos();
        System.out.println(infos);
    }

    @Test
    public void testGetQuorumVersion() {
        List<Replica> replicas = Lists.newArrayList(new Replica(10001, 20001, ReplicaState.NORMAL, 10, -1),
                new Replica(10002, 20002, ReplicaState.NORMAL, 10, -1),
                new Replica(10003, 20003, ReplicaState.NORMAL, 9, -1));
        LocalTablet tablet = new LocalTablet(10004, replicas);

        Assertions.assertEquals(-1L, tablet.getQuorumVersion(3));
        Assertions.assertEquals(10L, tablet.getQuorumVersion(2));

        Replica replica = tablet.getReplicaByBackendId(20001L);
        replica.setBad(true);
        Assertions.assertEquals(-1L, tablet.getQuorumVersion(2));
        replica.setBad(false);

        replica.setState(ReplicaState.DECOMMISSION);
        Assertions.assertEquals(-1L, tablet.getQuorumVersion(2));
        replica.setState(ReplicaState.NORMAL);
    }

    @Test
    public void testGetQueryableReplicaWithErrorState() {
        List<Replica> replicas = Lists.newArrayList(new Replica(10001, 20001, ReplicaState.NORMAL, 10, -1),
                new Replica(10002, 20002, ReplicaState.NORMAL, 10, -1),
                new Replica(10003, 20003, ReplicaState.NORMAL, 10, -1));
        LocalTablet tablet = new LocalTablet(10004, replicas);
        Assertions.assertTrue(tablet.getQueryableReplicasSize(10, -1) == 3);
        replicas.get(0).setIsErrorState(true);
        Assertions.assertTrue(tablet.getQueryableReplicasSize(10, -1) == 2);
        replicas.get(1).setIsErrorState(true);
        Assertions.assertTrue(tablet.getQueryableReplicasSize(10, -1) == 1);
    }

    @Test
    public void testGetNormalReplicaBackendPathMapFilterBlackListNode() {
        List<Replica> replicas = Lists.newArrayList(new Replica(10001, 20001, ReplicaState.NORMAL, 10, -1),
                new Replica(10002, 20002, ReplicaState.NORMAL, 10, -1),
                new Replica(10003, 20003, ReplicaState.NORMAL, 10, -1));
        LocalTablet tablet = new LocalTablet(10004, replicas);
        new MockUp<SimpleScheduler>() {
            @Mock
            public boolean isInBlocklist(long id) {
                if (id == 20002) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public boolean checkBackendAlive(long id) {
                return true;
            }
        };

        Multimap<Replica, Long> map = tablet.getNormalReplicaBackendPathMap(infoService, false);
        Assertions.assertTrue(map.size() == 2);
        for (Map.Entry<Replica, Long> entry : map.entries()) {
            Assertions.assertTrue(entry.getKey().getBackendId() != 20002);
        }
    }

    @Test
    public void testGetNormalReplicaBackendPathMapFilterDecommission() {
        List<Replica> replicas = Lists.newArrayList(new Replica(10001, 20001, ReplicaState.NORMAL, 10, -1),
                new Replica(10002, 20002, ReplicaState.NORMAL, 10, -1),
                new Replica(10003, 20003, ReplicaState.DECOMMISSION, 10, -1));
        LocalTablet tablet = new LocalTablet(10004, replicas);
        new MockUp<SimpleScheduler>() {
            @Mock
            public boolean isInBlocklist(long id) {
                return false;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public boolean checkBackendAlive(long id) {
                return true;
            }
        };

        Multimap<Replica, Long> map = tablet.getNormalReplicaBackendPathMap(infoService, false);
        Assertions.assertTrue(map.size() == 2);
        for (Map.Entry<Replica, Long> entry : map.entries()) {
            Assertions.assertTrue(entry.getKey().getBackendId() != 20003);
        }
        Multimap<Replica, Long> map2 = tablet.getNormalReplicaBackendPathMap(infoService, true);
        Assertions.assertTrue(map2.size() == 3);
    }
}
