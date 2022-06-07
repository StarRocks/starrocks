// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletStat;
import com.starrocks.thrift.TTabletStatResult;
import com.starrocks.thrift.TTabletType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TabletStatMgrTest {
    @Test
    public void testUpdateTabletStat(@Mocked GlobalStateMgr globalStateMgr) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tablet2Id = 11L;
        long backendId = 20L;
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet2 is LocalTablet
        TabletMeta tabletMeta2 = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD);
        invertedIndex.addTablet(tablet2Id, tabletMeta2);
        Replica replica = new Replica(tablet2Id + 1, backendId, 0, Replica.ReplicaState.NORMAL);
        invertedIndex.addReplica(tablet2Id, replica);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.HDD));
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Table
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        // Db
        Database db = new Database();
        db.createTable(table);

        TTabletStatResult result = new TTabletStatResult();
        Map<Long, TTabletStat> tabletsStats = Maps.newHashMap();
        result.setTablets_stats(tabletsStats);
        TTabletStat tablet2Stat = new TTabletStat(tablet2Id);
        tablet2Stat.setData_size(200L);
        tablet2Stat.setRow_num(201L);
        tabletsStats.put(tablet2Id, tablet2Stat);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentInvertedIndex();
                result = invertedIndex;
            }
        };

        // Check
        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateTabletStat", backendId, result);

        Assert.assertEquals(200L, replica.getDataSize());
        Assert.assertEquals(201L, replica.getRowCount());
    }
}