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


package com.starrocks.leader;

import com.google.common.collect.Sets;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletVersionPair;
import com.starrocks.thrift.TUpdateTabletVersionRequest;
import com.starrocks.thrift.TUpdateTabletVersionResult;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.starrocks.catalog.Replica.ReplicaState.NORMAL;

public class LeaderImplTest {

    private long dbId;
    private String dbName;
    private String tableName;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long replicaId;
    private long backendId;

    private final LeaderImpl leader = new LeaderImpl();

    @Before
    public void setUp() {
        dbId = 1L;
        dbName = "database0";
        tableName = "table0";
        tableId = 10L;
        partitionId = 11L;
        indexId = 12L;
        tabletId = 13L;
        replicaId = 14L;
        backendId = 15L;
    }

    @Test
    public void testFindRelatedReplica(@Mocked OlapTable olapTable, @Mocked LakeTable lakeTable,
                                       @Mocked PhysicalPartition physicalPartition, @Mocked MaterializedIndex index
                                       ) throws Exception {

        // olap table
        new Expectations() {
            {
                physicalPartition.getIndex(indexId);
                result = index;
                index.getTablet(tabletId);
                result = new LocalTablet(tabletId);
            }
        };
        
        Assert.assertNull(Deencapsulation.invoke(leader, "findRelatedReplica",
                olapTable, physicalPartition, backendId, tabletId, indexId));
        // lake table
        new MockUp<LakeTablet>() {
            @Mock
            public Set<Long> getBackendIds() {
                return Sets.newHashSet();
            }
        };

        new Expectations() {
            {
                physicalPartition.getIndex(indexId);
                result = index;
                index.getTablet(tabletId);
                result = new LakeTablet(tabletId);
            }
        };

        Assert.assertEquals(new Replica(tabletId, backendId, -1, NORMAL), Deencapsulation.invoke(leader, "findRelatedReplica",
                olapTable, physicalPartition, backendId, tabletId, indexId));
    }

    @Test
    public void testUpdateTabletVersion() throws Exception {
        TUpdateTabletVersionRequest request = new TUpdateTabletVersionRequest();
        TUpdateTabletVersionResult result = new TUpdateTabletVersionResult();
        result = leader.updateTabletVersion(request);
        Assert.assertEquals(result.status.getStatus_code(), TStatusCode.CANCELLED);
        Assert.assertEquals("current fe is not leader", result.status.getError_msgs().get(0));

        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        TBackend tBackend = new TBackend("host2", 8000, 1);
        request.setBackend(tBackend);
        result = leader.updateTabletVersion(request);
        Assert.assertEquals(result.status.getStatus_code(), TStatusCode.CANCELLED);
        Assert.assertEquals("backend not exist.", result.status.getError_msgs().get(0));

        Backend cn = new Backend(10002, "host2", 8000);
        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackendWithBePort(String host, int bePort) {
                return cn;
            }
        };

        List<TTabletVersionPair> tabletVersions = new ArrayList<>();
        TTabletVersionPair pair1 = new TTabletVersionPair();
        pair1.setTablet_id(10001L);
        pair1.setVersion(4L);
        tabletVersions.add(pair1);
        
        TTabletVersionPair pair2 = new TTabletVersionPair();
        pair2.setTablet_id(10002L);
        pair2.setVersion(5L);
        tabletVersions.add(pair2);
        request.setTablet_versions(tabletVersions);

        result = leader.updateTabletVersion(request);
        Assert.assertEquals(result.status.getStatus_code(), TStatusCode.CANCELLED);
        Assert.assertEquals("no replicas on backend", result.status.getError_msgs().get(0));

        List<TabletMeta> metaList = new ArrayList<>();
        List<Replica> replicas = new ArrayList<>();
        Replica replica1 = new Replica(1L, cn.getId(), ReplicaState.NORMAL, 3, 0);
        Replica replica2 = new Replica(2L, cn.getId(), ReplicaState.NORMAL, 3, 0);
        replicas.add(replica1);
        replicas.add(replica2);

        new MockUp<TabletInvertedIndex>() {
            @Mock
            public List<Replica> getReplicasOnBackendByTabletIds(List<Long> tabletIds, long backendId) {
                return replicas;
            }
        
            @Mock
            public List<TabletMeta> getTabletMetaList(List<Long> tabletIds) {
                return metaList;
            }
        };

        result = leader.updateTabletVersion(request);
        Assert.assertEquals(TStatusCode.CANCELLED, result.status.getStatus_code());
        Assert.assertEquals("no tabletMeta found", result.status.getError_msgs().get(0));

        TabletMeta meta1 = new TabletMeta(1L, 1L, 1L, 1L, 1, TStorageMedium.HDD, false);
        TabletMeta meta2 = new TabletMeta(1L, 1L, 1L, 2L, 1, TStorageMedium.HDD, false);
        metaList.add(meta1);
        metaList.add(meta2);

        new MockUp<Locker>() {
            @Mock
            public void lockTableWithIntensiveDbLock(Long dbId, Long tabletId, LockType lockType) {
                return;
            }

            @Mock
            public void unLockTableWithIntensiveDbLock(Long dbId, Long tabletId, LockType lockType) {
                return;
            }
        };

        result = leader.updateTabletVersion(request);
        Assert.assertEquals(TStatusCode.OK, result.status.getStatus_code());
    
        Assert.assertEquals(4L, replica1.getVersion());
        Assert.assertEquals(5L, replica2.getVersion());

        TabletMeta wrongMeta = new TabletMeta(2L, 1L, 1L, 1L, 1, TStorageMedium.HDD, false);
        List<TabletMeta> wrongMetaList = new ArrayList<>();
        wrongMetaList.add(meta1);
        wrongMetaList.add(wrongMeta);
    
        new MockUp<TabletInvertedIndex>() {
            @Mock
            public List<TabletMeta> getTabletMetaList(List<Long> tabletIds) {
                return wrongMetaList;
            }
        };
    
        result = leader.updateTabletVersion(request);
        Assert.assertEquals(TStatusCode.CANCELLED, result.status.getStatus_code());
        Assert.assertEquals("tablets in request from different db or table", 
                          result.status.getError_msgs().get(0));
        
    }
}
