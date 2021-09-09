// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.thrift.TStorageMedium;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CatalogRecycleBinTest {

    @Test
    public void testGetDb() {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        Database database = new Database(1, "db");
        bin.recycleDatabase(database, Sets.newHashSet());
        Database database2 = new Database(2, "db");
        bin.recycleDatabase(database2, Sets.newHashSet());

        Database recycledDb = bin.getDatabase(1);
        Assert.assertNull(recycledDb);
        recycledDb = bin.getDatabase(2);
        Assert.assertEquals(2L, recycledDb.getId());
        Assert.assertEquals("db", recycledDb.getFullName());

        List<Long> dbIds = bin.getAllDbIds();
        Assert.assertEquals(Lists.newArrayList(2L), dbIds);
    }

    @Test
    public void testGetTable() {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        Table table = new Table(1L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11L, table);
        Table table2 = new Table(2L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11L, table2);

        Table recycledTable = bin.getTable(1L);
        Assert.assertNull(recycledTable);
        recycledTable = bin.getTable(2L);
        Assert.assertEquals(2L, recycledTable.getId());

        List<Table> tables = bin.getTables(11L);
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(2L, tables.get(0).getId());
    }

    @Test
    public void testGetPartition() throws Exception {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        List<Column> columns = Lists.newArrayList(new Column("k1", ScalarType.createVarcharType(10)));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Partition partition = new Partition(1L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(11L, 22L, partition, range, dataProperty, (short) 1, false);
        Partition partition2 = new Partition(2L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(11L, 22L, partition2, range, dataProperty, (short) 1, false);

        Partition recycledPart = bin.getPartition(1L);
        Assert.assertNull(recycledPart);
        recycledPart = bin.getPartition(2L);
        Assert.assertEquals(2L, recycledPart.getId());
        Assert.assertEquals(range, bin.getPartitionRange(2L));
        Assert.assertEquals(dataProperty, bin.getPartitionDataProperty(2L));
        Assert.assertEquals((short) 1, bin.getPartitionReplicationNum(2L));
        Assert.assertFalse(bin.getPartitionIsInMemory(2L));

        List<Partition> partitions = bin.getPartitions(22L);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(2L, partitions.get(0).getId());
    }
}
