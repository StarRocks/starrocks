package com.starrocks.consistency;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ConsistencyCheckerTest {

    @Test
    public void testChooseTablets(@Mocked Catalog catalog) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long replicaId = 6L;
        long backendId = 7L;
        TStorageMedium medium = TStorageMedium.HDD;

        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Replica replica = new Replica(replicaId, backendId, 2L, -1L, 1111, 10,
                1000, Replica.ReplicaState.NORMAL, -1, -1L, 2, -1L);

        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 1111, medium);
        Tablet tablet = new Tablet(tabletId, Lists.newArrayList(replica));
        materializedIndex.addTablet(tablet, tabletMeta, true);
        PartitionInfo partitionInfo = new PartitionInfo();
        DataProperty dataProperty = new DataProperty(medium);
        partitionInfo.addPartition(partitionId, dataProperty, (short) 3, false);
        DistributionInfo distributionInfo = new HashDistributionInfo(1, Lists.newArrayList());
        Partition partition = new Partition(partitionId, "partition", materializedIndex, distributionInfo);
        partition.setVisibleVersion(2L, System.currentTimeMillis(), -1);
        OlapTable table = new OlapTable(tableId, "table", Lists.newArrayList(), KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        Database database = new Database(dbId, "database");
        database.createTable(table);

        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                result = catalog;
                minTimes = 0;

                catalog.getDbIds();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                catalog.getDb(dbId);
                result = database;
                minTimes = 0;
            }
        };

        Assert.assertEquals(1, new ConsistencyChecker().chooseTablets().size());

        // set table state to RESTORE, we will make sure checker will not choose its tablets.
        table.setState(OlapTable.OlapTableState.RESTORE);
        Assert.assertEquals(0, new ConsistencyChecker().chooseTablets().size());
    }
}
