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
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LocalTabletsProcDirTest {

    @Test
    public void testFetchResultWithLocalTablet(@Mocked GlobalStateMgr globalStateMgr,
                                               @Mocked SystemInfoService systemInfoService) {
        Map<Long, Backend> idToBackend = Maps.newHashMap();
        long backendId = 20L;
        idToBackend.put(backendId, new Backend(backendId, "127.0.0.1", 9050));
        idToBackend.put(backendId, new Backend(backendId + 1, "127.0.0.2", 9050));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = ImmutableMap.copyOf(idToBackend);
            }
        };

        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tablet1Id = 5L;
        long tablet2Id = 6L;
        long replicaId = 10L;

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Replica
        Replica replica1 = new Replica(replicaId, backendId, Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica2 = new Replica(replicaId + 1, backendId + 1, Replica.ReplicaState.NORMAL, 1, 0);

        // Tablet
        LocalTablet tablet1 = new LocalTablet(tablet1Id);
        tablet1.addReplica(replica1);
        tablet1.addReplica(replica2);
        LocalTablet tablet2 = new LocalTablet(tablet2Id);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.SSD));
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.SSD);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        // Db
        Database db = new Database(dbId, "test_db");
        db.createTable(table);

        // Check
        LocalTabletsProcDir tabletsProcDir = new LocalTabletsProcDir(db, table, index);
        List<List<Comparable>> result = tabletsProcDir.fetchComparableResult(-1, -1, null, false);
        System.out.println(result);
        Assert.assertEquals(3, result.size());
        Assert.assertEquals((long) result.get(0).get(0), tablet1Id);
        Assert.assertEquals((long) result.get(0).get(1), replicaId);
        Assert.assertEquals((long) result.get(0).get(2), backendId);
        Assert.assertEquals((long) result.get(1).get(0), tablet1Id);
        Assert.assertEquals((long) result.get(1).get(1), replicaId + 1);
        Assert.assertEquals((long) result.get(1).get(2), backendId + 1);
        Assert.assertEquals((long) result.get(2).get(0), tablet2Id);
        Assert.assertEquals(result.get(2).get(1), -1);
        Assert.assertEquals(result.get(2).get(2), -1);
    }
}