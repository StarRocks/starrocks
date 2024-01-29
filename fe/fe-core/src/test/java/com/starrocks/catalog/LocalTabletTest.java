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

import com.google.common.collect.Sets;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

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

    @Before
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
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, 1, TStorageMedium.HDD);
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
        Assert.assertEquals(replica1, tablet.getReplicaById(replica1.getId()));
        Assert.assertEquals(replica2, tablet.getReplicaById(replica2.getId()));
        Assert.assertEquals(replica3, tablet.getReplicaById(replica3.getId()));

        Assert.assertEquals(3, tablet.getImmutableReplicas().size());
        Assert.assertEquals(replica1, tablet.getReplicaByBackendId(replica1.getBackendId()));
        Assert.assertEquals(replica2, tablet.getReplicaByBackendId(replica2.getBackendId()));
        Assert.assertEquals(replica3, tablet.getReplicaByBackendId(replica3.getBackendId()));

        Assert.assertEquals(600003L, tablet.getDataSize(false));
        Assert.assertEquals(200002L, tablet.getDataSize(true));
        Assert.assertEquals(3002L, tablet.getRowCount(100));
    }

    @Test
    public void deleteReplicaTest() {
        // delete replica1
        Assert.assertTrue(tablet.deleteReplicaByBackendId(replica1.getBackendId()));
        Assert.assertNull(tablet.getReplicaById(replica1.getId()));

        // err: re-delete replica1
        Assert.assertFalse(tablet.deleteReplicaByBackendId(replica1.getBackendId()));
        Assert.assertFalse(tablet.deleteReplica(replica1));
        Assert.assertNull(tablet.getReplicaById(replica1.getId()));

        // delete replica2
        Assert.assertTrue(tablet.deleteReplica(replica2));
        Assert.assertEquals(1, tablet.getImmutableReplicas().size());

        // clear replicas
        tablet.clearReplica();
        Assert.assertEquals(0, tablet.getImmutableReplicas().size());
    }

    @Test
    public void testSerialization() throws Exception {
        File file = new File("./olapTabletTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        tablet.write(dos);
        dos.flush();
        dos.close();

        // Read an object from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        LocalTablet rTablet1 = LocalTablet.read(dis);
        Assert.assertEquals(1, rTablet1.getId());
        Assert.assertEquals(3, rTablet1.getImmutableReplicas().size());
        Assert.assertEquals(rTablet1.getImmutableReplicas().get(0).getVersion(),
                rTablet1.getImmutableReplicas().get(1).getVersion());

        Assert.assertTrue(rTablet1.equals(tablet));
        Assert.assertTrue(rTablet1.equals(rTablet1));
        Assert.assertFalse(rTablet1.equals(this));

        LocalTablet tablet2 = new LocalTablet(1);
        Replica replica1 = new Replica(1L, 1L, 100L, 0, 200000L, 3000L, ReplicaState.NORMAL, 0, 0);
        Replica replica2 = new Replica(2L, 2L, 100L, 0, 200001L, 3001L, ReplicaState.NORMAL, 0, 0);
        Replica replica3 = new Replica(3L, 3L, 100L, 0, 200002L, 3002L, ReplicaState.NORMAL, 0, 0);
        tablet2.addReplica(replica1);
        tablet2.addReplica(replica2);
        Assert.assertFalse(tablet2.equals(tablet));
        tablet2.addReplica(replica3);
        Assert.assertTrue(tablet2.equals(tablet));

        LocalTablet tablet3 = new LocalTablet(1);
        tablet3.addReplica(replica1);
        tablet3.addReplica(replica2);
        tablet3.addReplica(new Replica(4L, 4L, 100L, 0, 200002L, 3002L, ReplicaState.NORMAL, 0, 0));
        Assert.assertFalse(tablet3.equals(tablet));

        dis.close();
        file.delete();

        // Read an object from json
        String jsonStr = GsonUtils.GSON.toJson(tablet);
        LocalTablet jTablet = GsonUtils.GSON.fromJson(jsonStr, LocalTablet.class);
        Assert.assertEquals(1, jTablet.getId());
        Assert.assertEquals(3, jTablet.getImmutableReplicas().size());
        Assert.assertEquals(jTablet.getImmutableReplicas().get(0).getVersion(),
                jTablet.getImmutableReplicas().get(1).getVersion());
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
        Assert.assertEquals(LocalTablet.TabletStatus.COLOCATE_REDUNDANT,
                tablet.getColocateHealthStatus(9, 1, Sets.newHashSet(10002L)));
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

        Assert.assertEquals(tablet.getBackends().size(), 2);

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
}
