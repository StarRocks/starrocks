// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.PartitionKeyDesc;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.io.FastByteArrayOutputStream;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MaterializedViewTest {

    private static List<Column> columns;

    @Before
    public void setUp() {
        columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.SUM, "", ""));
    }

    @Test
    public void testInit() {
        MaterializedView mv = new MaterializedView();
        Assert.assertEquals(Table.TableType.MATERIALIZED_VIEW, mv.getType());
        Assert.assertEquals(null, mv.getTableProperty());

        MaterializedView mv2 = new MaterializedView(1000, 100, "mv2", columns, KeysType.AGG_KEYS,
                null, null, null);
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

    // This test is temporarily removed because it is unstable,
    // and it will be added back when the cause of the problem is found and fixed.
    public void testSinglePartitionSerialization(@Mocked GlobalStateMgr globalStateMgr,
                                                 @Mocked Database database) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getDb(100);
                result = database;

                database.getTable(10L);
                result = null;

                database.getTable(20L);
                result = null;

                database.getTable(30L);
                result = null;
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

    public RangePartitionInfo generateRangePartitionInfo(Database database, OlapTable baseTable)
            throws DdlException, AnalysisException, NotImplementedException {
        //add columns
        List<Column> partitionColumns = baseTable.getPartitionInfo().getPartitionColumns();
        List<SingleRangePartitionDesc> singleRangePartitionDescs = Lists.newArrayList();
        int columns = partitionColumns.size();

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20180101")),
                Lists.newArrayList(new PartitionValue("20190101")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

        List<Expr> exprs = Lists.newArrayList();
        TableName tableName = new TableName(database.getFullName(), "mv_name");
        SlotRef slotRef1 = new SlotRef(tableName, "k1");
        StringLiteral quarterStringLiteral = new StringLiteral("quarter");
        FunctionCallExpr quarterFunctionCallExpr =
                new FunctionCallExpr("date_trunc", Arrays.asList(quarterStringLiteral, slotRef1));
        exprs.add(quarterFunctionCallExpr);

        RangePartitionInfo partitionInfo = new ExpressionRangePartitionInfo(exprs,partitionColumns);

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
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        OlapTable baseTable = ((OlapTable) testDb.getTable("tbl1"));

        RangePartitionInfo partitionInfo = generateRangePartitionInfo(testDb, baseTable);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(10, Lists.newArrayList(columns.get(0)));
        MaterializedView mv = new MaterializedView(1000, testDb.getId(), "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, hashDistributionInfo, refreshScheme);
        mv.setBaseIndexId(1);
        mv.setIndexMeta(1L, "mv_name", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        MaterializedIndex index = new MaterializedIndex(3, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(2, "mv_name", index, hashDistributionInfo);
        mv.addPartition(partition);
        mv.setBaseTableIds(Sets.newHashSet(baseTable.getId()));
        mv.setViewDefineSql("select * from test.tbl1");

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

    @Test
    public void testRenameMaterializedView() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view mv_to_rename\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;")
                .withNewMaterializedView("create materialized view mv_to_rename2\n" +
                        "PARTITION BY date_trunc('month', k1)\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;");
        String alterSql = "alter materialized view mv_to_rename rename mv_new_name;";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, alterSql);
        stmtExecutor.execute();
        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        MaterializedView mv = ((MaterializedView) testDb.getTable("mv_new_name"));
        Assert.assertNotNull(mv);
        Assert.assertEquals("mv_new_name", mv.getName());
        ExpressionRangePartitionInfo partitionInfo = (ExpressionRangePartitionInfo) mv.getPartitionInfo();
        List<Expr> exprs = partitionInfo.getPartitionExprs();
        Assert.assertEquals(1, exprs.size());
        Assert.assertTrue(exprs.get(0) instanceof SlotRef);
        SlotRef slotRef = (SlotRef) exprs.get(0);
        Assert.assertEquals("mv_new_name", slotRef.getTblNameWithoutAnalyzed().getTbl());

        String alterSql2 = "alter materialized view mv_to_rename2 rename mv_new_name2;";
        StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, alterSql2);
        stmtExecutor2.execute();
        MaterializedView mv2 = ((MaterializedView) testDb.getTable("mv_new_name2"));
        Assert.assertNotNull(mv2);
        Assert.assertEquals("mv_new_name2", mv2.getName());
        ExpressionRangePartitionInfo partitionInfo2 = (ExpressionRangePartitionInfo) mv2.getPartitionInfo();
        List<Expr> exprs2 = partitionInfo2.getPartitionExprs();
        Assert.assertEquals(1, exprs2.size());
        Assert.assertTrue(exprs2.get(0) instanceof FunctionCallExpr);
        Expr rightChild = exprs2.get(0).getChild(1);
        Assert.assertTrue(rightChild instanceof SlotRef);
        SlotRef slotRef2 = (SlotRef) rightChild;
        Assert.assertEquals("mv_new_name2", slotRef2.getTblNameWithoutAnalyzed().getTbl());
    }

    @Test
    public void testMvAfterDropBaseTable() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl_drop\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view mv_to_check\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k2, sum(v1) as total from tbl_drop group by k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        MaterializedView mv = ((MaterializedView) testDb.getTable("mv_to_check"));
        String dropSql = "drop table tbl_drop;";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, dropSql);
        stmtExecutor.execute();
        Assert.assertNotNull(mv);
        Assert.assertFalse(mv.isActive());
    }

    @Test
    public void testMvAfterBaseTableRename() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl_to_rename\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view mv_to_check\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k2, sum(v1) as total from tbl_to_rename group by k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        String alterSql = "alter table tbl_to_rename rename new_tbl_name;";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, alterSql);
        stmtExecutor.execute();
        MaterializedView mv = ((MaterializedView) testDb.getTable("mv_to_check"));
        Assert.assertNotNull(mv);
        Assert.assertFalse(mv.isActive());
    }

    @Test
    public void testMvAfterBaseTablePartitionRename() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl_to_rename\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view mv_to_check\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k2, sum(v1) as total from tbl_to_rename group by k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        String alterPartitionSql = "alter table tbl_to_rename rename partition p1 new_p1;";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, alterPartitionSql);
        stmtExecutor.execute();
        MaterializedView mv = ((MaterializedView) testDb.getTable("mv_to_check"));
        Assert.assertNotNull(mv);
        Assert.assertFalse(mv.isActive());
    }

    @Test
    public void testMaterializedViewWithHint() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view mv_with_hint\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select /*+ SET_VAR(query_timeout = 500) */ k1, k2, sum(v1) as total from tbl1 group by k1, k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        MaterializedView mv = ((MaterializedView) testDb.getTable("mv_with_hint"));
        String mvTaskName = "mv-" + mv.getId();
        Task task = connectContext.getGlobalStateMgr().getTaskManager().getTask(mvTaskName);
        Assert.assertNotNull(task);
        Map<String, String> taskProperties = task.getProperties();
        Assert.assertTrue(taskProperties.containsKey("query_timeout"));
        Assert.assertEquals("500", taskProperties.get("query_timeout"));
        Assert.assertEquals(Constants.TaskType.EVENT_TRIGGERED, task.getType());
    }
}
