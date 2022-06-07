// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.PartitionKeyDesc;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.FastByteArrayOutputStream;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTabletType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.LinkedList;
import java.util.List;

public class MaterializedViewTest {

    private static List<Column> columns;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private Database database;

    private OlapTable olapTable1;
    private OlapTable olapTable2;
    private OlapTable olapTable3;

    @Before
    public void setUp() {
        columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.SUM, "", ""));
    }

    @Test
    public void testInit() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getClusterId();
                result = 1024;
            }
        };
        MaterializedView mv = new MaterializedView();
        Assert.assertEquals(1024, mv.getClusterId());
        Assert.assertEquals(Table.TableType.MATERIALIZED_VIEW, mv.getType());
        Assert.assertEquals(null, mv.getTableProperty());

        MaterializedView mv2 = new MaterializedView(1000, 100, "mv2", columns, KeysType.AGG_KEYS,
                null, null, null);
        Assert.assertEquals(1024, mv2.getClusterId());
        Assert.assertEquals(100, mv2.getDbId());
        Assert.assertEquals(Table.TableType.MATERIALIZED_VIEW, mv2.getType());
        Assert.assertEquals(null, mv2.getTableProperty());
        Assert.assertEquals("mv2", mv2.getName());
        Assert.assertEquals(KeysType.AGG_KEYS, mv2.getKeysType());
        mv2.setBaseIndexId(10003);
        Assert.assertEquals(10003, mv2.getBaseIndexId());
        Assert.assertFalse(mv2.isPartitioned());
        mv2.setState(OlapTable.OlapTableState.ROLLUP);
        Assert.assertEquals(OlapTable.OlapTableState.ROLLUP, mv2.getState());
        Assert.assertEquals(null, mv2.getDefaultDistributionInfo());
        Assert.assertEquals(null, mv2.getPartitionInfo());
        mv2.setReplicationNum((short)3);
        Assert.assertEquals(3, mv2.getDefaultReplicationNum().shortValue());
        mv2.setStorageMedium(TStorageMedium.SSD);
        Assert.assertEquals("SSD", mv2.getStorageMedium());
        Assert.assertEquals(true, mv2.isActive());
        mv2.setActive(false);
        Assert.assertEquals(false, mv2.isActive());
        mv2.setBaseTableIds(Sets.newHashSet(10L, 20L));
        Assert.assertEquals(Sets.newHashSet(10L, 20L), mv2.getBaseTableIds());

        String mvDefinition = "create materialized view mv2 select col1, col2 from table1";
        mv2.setViewDefineSql(mvDefinition);
        Assert.assertEquals(mvDefinition, mv2.getViewDefineSql());
    }

    @Test
    public void testSchema() {
        MaterializedView mv = new MaterializedView(1000, 100, "mv2", columns, KeysType.AGG_KEYS,
                null, null, null);
        mv.setBaseIndexId(1L);
        mv.setIndexMeta(1L, "mv_name", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        Assert.assertEquals(1, mv.getBaseIndexId());
        mv.rebuildFullSchema();
        Assert.assertEquals("mv_name", mv.getIndexNameById(1L));
        List<Column> indexColumns = Lists.newArrayList(columns.get(0), columns.get(2));
        mv.setIndexMeta(2L, "index_name", indexColumns, 0,
                222, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        mv.rebuildFullSchema();
        Assert.assertEquals("index_name", mv.getIndexNameById(2L));
    }

    @Test
    public void testPartition() {
        // distribute
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        partitionInfo.setIsInMemory(1, false);
        partitionInfo.setTabletType(1, TTabletType.TABLET_TYPE_DISK);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();

        MaterializedView mv = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(1L, "mv_name", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        Assert.assertEquals("mv_name", mv.getName());
        mv.setName("new_name");
        Assert.assertEquals("new_name", mv.getName());
        PartitionInfo pInfo1 = mv.getPartitionInfo();
        Assert.assertTrue(pInfo1 instanceof SinglePartitionInfo);

        MaterializedIndex index = new MaterializedIndex(3, IndexState.NORMAL);
        Partition partition = new Partition(2, "mv_name", index, distributionInfo);
        mv.addPartition(partition);
        Partition tmpPartition = mv.getPartition("mv_name");
        Assert.assertTrue(tmpPartition != null);
        Assert.assertEquals(2L, tmpPartition.getId());
        Assert.assertEquals(1, mv.getPartitions().size());
        Assert.assertEquals(1, mv.getPartitionNames().size());
        Assert.assertEquals(0, mv.getPartitionColumnNames().size());
        Assert.assertTrue(mv.isPartitioned());

        PartitionInfo rangePartitionInfo = new RangePartitionInfo(Lists.newArrayList(columns.get(0)));
        rangePartitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        rangePartitionInfo.setReplicationNum(1, (short) 3);
        rangePartitionInfo.setIsInMemory(1, false);
        rangePartitionInfo.setTabletType(1, TTabletType.TABLET_TYPE_DISK);

        MaterializedView mv2 = new MaterializedView(1000, 100, "mv_name_2", columns, KeysType.AGG_KEYS,
                rangePartitionInfo, distributionInfo, refreshScheme);
        mv2.setIndexMeta(1L, "mv_name_2", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        Assert.assertEquals("mv_name_2", mv2.getName());
        mv2.setName("new_name_2");
        Assert.assertEquals("new_name_2", mv2.getName());
        PartitionInfo pInfo2 = mv2.getPartitionInfo();
        Assert.assertTrue(pInfo2 instanceof RangePartitionInfo);
        Partition partition2 = new Partition(3, "p1", index, distributionInfo);
        mv2.addPartition(partition2);
        Partition tmpPartition2 = mv2.getPartition("p1");
        Assert.assertTrue(tmpPartition2 != null);
        Assert.assertEquals(3L, tmpPartition2.getId());
        Assert.assertEquals(1, mv2.getPartitions().size());
        Assert.assertEquals(1, mv2.getPartitionNames().size());
        Assert.assertEquals(1, mv2.getPartitionColumnNames().size());
        Assert.assertTrue(mv2.isPartitioned());
    }

    @Test
    public void testDistribution() {
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        partitionInfo.setIsInMemory(1, false);
        partitionInfo.setTabletType(1, TTabletType.TABLET_TYPE_DISK);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();

        MaterializedView mv = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(1L, "mv_name", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        DistributionInfo distributionInfo1 = mv.getDefaultDistributionInfo();
        Assert.assertTrue(distributionInfo1 instanceof RandomDistributionInfo);
        Assert.assertEquals(0, mv.getDistributionColumnNames().size());

        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(10, Lists.newArrayList(columns.get(0)));
        MaterializedView mv2 = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, hashDistributionInfo, refreshScheme);
        DistributionInfo distributionInfo2 = mv2.getDefaultDistributionInfo();
        Assert.assertTrue(distributionInfo2 instanceof HashDistributionInfo);
        Assert.assertEquals(1, mv2.getDistributionColumnNames().size());
    }

    @Test
    public void testToThrift() {
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        partitionInfo.setIsInMemory(1, false);
        partitionInfo.setTabletType(1, TTabletType.TABLET_TYPE_DISK);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();

        MaterializedView mv = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(1L, "mv_name", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        TTableDescriptor tableDescriptor = mv.toThrift(null);
        Assert.assertEquals(TTableType.MATERIALIZED_VIEW, tableDescriptor.getTableType());
        Assert.assertEquals(1000, tableDescriptor.getId());
        Assert.assertEquals("mv_name", tableDescriptor.getTableName());
    }

    @Test
    public void testSinglePartitionSerialization() throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getDb(100);
                result = database;

                database.getTable(10L);
                result = olapTable1;

                database.getTable(20L);
                result = olapTable2;

                database.getTable(30L);
                result = olapTable3;
            }
        };
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        partitionInfo.setIsInMemory(1, false);
        partitionInfo.setTabletType(1, TTabletType.TABLET_TYPE_DISK);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(10, Lists.newArrayList(columns.get(0)));
        MaterializedView mv = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, hashDistributionInfo, refreshScheme);
        mv.setBaseIndexId(1);
        mv.setIndexMeta(1L, "mv_name", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        MaterializedIndex index = new MaterializedIndex(3, IndexState.NORMAL);
        Partition partition = new Partition(2, "mv_name", index, hashDistributionInfo);
        mv.addPartition(partition);
        mv.setBaseTableIds(Sets.newHashSet(10L, 20L, 30L));

        FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
        mv.write(out);

        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream());

        Table table = Table.read(in);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView materializedView = (MaterializedView) table;
        Assert.assertTrue(mv.equals(materializedView));
        Assert.assertEquals(mv.getName(), materializedView.getName());
        PartitionInfo partitionInfo1 = materializedView.getPartitionInfo();
        Assert.assertTrue(partitionInfo1 != null);
        Assert.assertEquals(PartitionType.UNPARTITIONED, partitionInfo1.getType());
        DistributionInfo distributionInfo = materializedView.getDefaultDistributionInfo();
        Assert.assertTrue(distributionInfo != null);
        Assert.assertTrue(distributionInfo instanceof HashDistributionInfo);
        Assert.assertEquals(10, ((HashDistributionInfo) distributionInfo).getBucketNum());
        Assert.assertEquals(1, ((HashDistributionInfo) distributionInfo).getDistributionColumns().size());
    }

    public RangePartitionInfo generateRangePartitionInfo() throws DdlException, AnalysisException {
        //add columns
        List<Column> partitionColumns = Lists.newArrayList();
        List<SingleRangePartitionDesc> singleRangePartitionDescs = Lists.newArrayList();
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20180101"), new PartitionValue("10")),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

        RangePartitionInfo partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20001L, true);
        }

        return partitionInfo;
    }

    @Test
    public void testRangePartitionSerialization() throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getDb(100);
                result = database;

                database.getTable(10L);
                result = olapTable1;

                database.getTable(20L);
                result = olapTable2;

                database.getTable(30L);
                result = olapTable3;
            }
        };
        RangePartitionInfo partitionInfo = generateRangePartitionInfo();
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(10, Lists.newArrayList(columns.get(0)));
        MaterializedView mv = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, hashDistributionInfo, refreshScheme);
        mv.setBaseIndexId(1);
        mv.setIndexMeta(1L, "mv_name", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        MaterializedIndex index = new MaterializedIndex(3, IndexState.NORMAL);
        Partition partition = new Partition(2, "mv_name", index, hashDistributionInfo);
        mv.addPartition(partition);
        mv.setBaseTableIds(Sets.newHashSet(10L, 20L, 30L));

        FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
        mv.write(out);

        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream());

        Table table = Table.read(in);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView materializedView2 = (MaterializedView) table;
        Assert.assertTrue(mv.equals(materializedView2));
        Assert.assertEquals(mv.getName(), materializedView2.getName());
        PartitionInfo partitionInfo2 = materializedView2.getPartitionInfo();
        Assert.assertTrue(partitionInfo2 != null);
        Assert.assertEquals(PartitionType.RANGE, partitionInfo2.getType());
        RangePartitionInfo rangePartitionInfo2 = (RangePartitionInfo) partitionInfo2;
        Assert.assertTrue(partitionInfo.getRange(20000L).equals(rangePartitionInfo2.getRange(20000L)));
        DistributionInfo distributionInfo2 = materializedView2.getDefaultDistributionInfo();
        Assert.assertTrue(distributionInfo2 != null);
        Assert.assertTrue(distributionInfo2 instanceof HashDistributionInfo);
        Assert.assertEquals(10, distributionInfo2.getBucketNum());
        Assert.assertEquals(1, ((HashDistributionInfo) distributionInfo2).getDistributionColumns().size());
    }
}
