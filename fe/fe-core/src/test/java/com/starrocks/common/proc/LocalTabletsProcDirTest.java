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


package com.starrocks.common.proc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LocalTabletsProcDirTest {

    @Test
    public void testFetchResultWithLocalTablet(@Mocked GlobalStateMgr globalStateMgr,
                                               @Mocked SystemInfoService systemInfoService) {
        Map<Long, Backend> idToBackend = Maps.newHashMap();
        long backendId = 20L;
        Backend b1 = new Backend(backendId, "127.0.0.1", 9050);
        Map<String, DiskInfo> disks1 = Maps.newHashMap();
        DiskInfo d1 = new DiskInfo("/home/disk1");
        d1.setPathHash(1L);
        disks1.put("/home/disk1", d1);
        ImmutableMap<String, DiskInfo> immutableMap1 = ImmutableMap.copyOf(disks1);
        b1.setDisks(immutableMap1);

        Backend b2 = new Backend(backendId + 1, "127.0.0.2", 9050);
        Map<String, DiskInfo> disks2 = Maps.newHashMap();
        DiskInfo d2 = new DiskInfo("/home/disk2");
        d2.setPathHash(2L);
        disks2.put("/home/disk2", d2);
        ImmutableMap<String, DiskInfo> immutableMap2 = ImmutableMap.copyOf(disks2);
        b2.setDisks(immutableMap2);

        idToBackend.put(backendId, b1);
        idToBackend.put(backendId + 1, b2);

        VariableMgr variableMgr = new VariableMgr();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = ImmutableMap.copyOf(idToBackend);

                GlobalStateMgr.getCurrentState().getVariableMgr();
                minTimes = 0;
                result = variableMgr;
            }
        };

        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tablet1Id = 5L;
        long tablet2Id = 6L;
        long replicaId = 10L;
        long physicalPartitionId = 11L;

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", IntegerType.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", IntegerType.BIGINT, true, null, "", ""));
        columns.add(new Column("v", IntegerType.BIGINT, false, AggregateType.SUM, "0", ""));

        // Replica
        Replica replica1 = new Replica(replicaId, backendId, Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica2 = new Replica(replicaId + 1, backendId + 1, Replica.ReplicaState.NORMAL, 1, 0);
        replica1.setPathHash(1L);
        replica2.setPathHash(2L);

        // Tablet
        LocalTablet tablet1 = new LocalTablet(tablet1Id);
        tablet1.addReplica(replica1);
        tablet1.addReplica(replica2);
        LocalTablet tablet2 = new LocalTablet(tablet2Id);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.SSD));
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.SSD);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexMetaId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        // Db
        Database db = new Database(dbId, "test_db");
        db.registerTableUnlocked(table);

        // Check
        LocalTabletsProcDir tabletsProcDir = new LocalTabletsProcDir(db, table, index);
        List<List<Comparable>> result = tabletsProcDir.fetchComparableResult(-1, -1, null, null, false);
        System.out.println(result);
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals((long) result.get(0).get(0), tablet1Id);
        Assertions.assertEquals(result.get(0).get(21), "/home/disk1");
        Assertions.assertEquals(result.get(0).get(22), true);
        Assertions.assertEquals((long) result.get(0).get(23), -1);
        Assertions.assertEquals((long) result.get(1).get(0), tablet1Id);
        if ((long) result.get(0).get(1) == replicaId) {
            Assertions.assertEquals((long) result.get(0).get(2), backendId);
        } else if ((long) result.get(0).get(1) == replicaId + 1) {
            Assertions.assertEquals((long) result.get(0).get(2), backendId + 1);
        }
        Assertions.assertEquals(result.get(1).get(21), "/home/disk2");
        Assertions.assertEquals((long) result.get(2).get(0), tablet2Id);
        Assertions.assertEquals(result.get(2).get(1), -1);
        Assertions.assertEquals(result.get(2).get(2), -1);
    }
}