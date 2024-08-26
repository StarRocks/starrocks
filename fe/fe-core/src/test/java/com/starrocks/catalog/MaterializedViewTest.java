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


package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.planner.MaterializedViewTestBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.MVTestUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.sql.optimizer.MVTestUtils.waitForSchemaChangeAlterJobFinish;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MaterializedViewTest {

    private static List<Column> columns = new LinkedList<Column>();
    private ConnectContext connectContext;
    private StarRocksAssert starRocksAssert;

    @Before
    public void setUp() {
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        UtFrameUtils.createMinStarRocksCluster();

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
        mv2.setReplicationNum((short) 3);
        Assert.assertEquals(3, mv2.getDefaultReplicationNum().shortValue());
        mv2.setStorageMedium(TStorageMedium.SSD);
        Assert.assertEquals("SSD", mv2.getStorageMedium());
        Assert.assertEquals(true, mv2.isActive());
        mv2.setInactiveAndReason("");
        Assert.assertEquals(false, mv2.isActive());

        List<BaseTableInfo> baseTableInfos = Lists.newArrayList();
        BaseTableInfo baseTableInfo1 = new BaseTableInfo(100L, "db", "tbl1", 10L);
        baseTableInfos.add(baseTableInfo1);
        BaseTableInfo baseTableInfo2 = new BaseTableInfo(100L, "db", "tbl2", 20L);
        baseTableInfos.add(baseTableInfo2);
        mv2.setBaseTableInfos(baseTableInfos);
        List<BaseTableInfo> baseTableInfosCheck = mv2.getBaseTableInfos();

        Assert.assertEquals(10L, baseTableInfosCheck.get(0).getTableId());
        Assert.assertEquals(20L, baseTableInfosCheck.get(1).getTableId());

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
    public void testRenameMaterializedView() throws Exception {
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
                .withMaterializedView("create materialized view mv_to_rename\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;")
                .withMaterializedView("create materialized view mv_to_rename2\n" +
                        "PARTITION BY date_trunc('month', k1)\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;");

        Database db = connectContext.getGlobalStateMgr().getDb("test");
        Assert.assertNotNull(db);
        Table table = db.getTable("mv_to_rename");
        Assert.assertNotNull(table);
        // test partition related info
        MaterializedView oldMv = (MaterializedView) table;
        Map<Table, Column> partitionMap = oldMv.getRefBaseTablePartitionColumns();
        Table table1 = db.getTable("tbl1");
        Assert.assertTrue(partitionMap.containsKey(table1));
        List<Table.TableType> baseTableType = oldMv.getBaseTableTypes();
        Assert.assertEquals(1, baseTableType.size());
        Assert.assertEquals(table1.getType(), baseTableType.get(0));
        connectContext.executeSql("refresh materialized view mv_to_rename with sync mode");
        Optional<Long> maxTime = oldMv.maxBaseTableRefreshTimestamp();
        Assert.assertTrue(maxTime.isPresent());
        Pair<Table, Column> pair = MaterializedViewTestBase.getRefBaseTablePartitionColumn(oldMv);
        Assert.assertEquals("tbl1", pair.first.getName());

        String alterSql = "alter materialized view mv_to_rename rename mv_new_name;";
        StatementBase statement = SqlParser.parseSingleStatement(alterSql, connectContext.getSessionVariable().getSqlMode());

        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView mv = ((MaterializedView) testDb.getTable("mv_new_name"));
        Assert.assertNotNull(mv);
        Assert.assertEquals("mv_new_name", mv.getName());
        ExpressionRangePartitionInfo partitionInfo = (ExpressionRangePartitionInfo) mv.getPartitionInfo();
        List<Expr> exprs = partitionInfo.getPartitionExprs(mv.getIdToColumn());
        Assert.assertEquals(1, exprs.size());
        Assert.assertTrue(exprs.get(0) instanceof SlotRef);
        SlotRef slotRef = (SlotRef) exprs.get(0);
        Assert.assertEquals("mv_new_name", slotRef.getTblNameWithoutAnalyzed().getTbl());
        starRocksAssert.dropMaterializedView("mv_new_name");

        String alterSql2 = "alter materialized view mv_to_rename2 rename mv_new_name2;";
        statement = SqlParser.parseSingleStatement(alterSql2, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, statement);
        stmtExecutor2.execute();
        MaterializedView mv2 = ((MaterializedView) testDb.getTable("mv_new_name2"));
        Assert.assertNotNull(mv2);
        Assert.assertEquals("mv_new_name2", mv2.getName());
        ExpressionRangePartitionInfo partitionInfo2 = (ExpressionRangePartitionInfo) mv2.getPartitionInfo();
        List<Expr> exprs2 = partitionInfo2.getPartitionExprs(mv2.getIdToColumn());
        Assert.assertEquals(1, exprs2.size());
        Assert.assertTrue(exprs2.get(0) instanceof FunctionCallExpr);
        Expr rightChild = exprs2.get(0).getChild(1);
        Assert.assertTrue(rightChild instanceof SlotRef);
        SlotRef slotRef2 = (SlotRef) rightChild;
        Assert.assertEquals("mv_new_name2", slotRef2.getTblNameWithoutAnalyzed().getTbl());
        starRocksAssert.dropMaterializedView("mv_new_name2");
    }

    @Test
    public void testReplay() throws Exception {
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
                .withMaterializedView("create materialized view mv_replay\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;");
        connectContext.executeSql("insert into test.tbl1 values('2022-02-01', 2, 3)");
        connectContext.executeSql("insert into test.tbl1 values('2022-02-16', 3, 5)");
        connectContext.executeSql("refresh materialized view mv_replay with sync mode");

        Database db = connectContext.getGlobalStateMgr().getDb("test");
        Assert.assertNotNull(db);
        Table table = db.getTable("mv_replay");
        MaterializedView mv = (MaterializedView) table;
        AlterMaterializedViewBaseTableInfosLog log = new AlterMaterializedViewBaseTableInfosLog(db.getId(), mv.getId(), null,
                mv.getBaseTableInfos(), mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap());
        mv.replayAlterMaterializedViewBaseTableInfos(log);

        starRocksAssert.dropMaterializedView("mv_replay");
    }

    @Test
    public void testMvAfterDropBaseTable() throws Exception {
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
                .withMaterializedView("create materialized view mv_to_check\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k2, sum(v1) as total from tbl_drop group by k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView mv = ((MaterializedView) testDb.getTable("mv_to_check"));
        String dropSql = "drop table tbl_drop;";
        StatementBase statement = SqlParser.parseSingleStatement(dropSql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        Assert.assertNotNull(mv);
        Assert.assertFalse(mv.isActive());
    }

    @Test
    public void testMvAfterBaseTableRename() throws Exception {
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
                .withMaterializedView("create materialized view mv_to_check\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k2, sum(v1) as total from tbl_to_rename group by k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        String alterSql = "alter table tbl_to_rename rename new_tbl_name;";
        StatementBase statement = SqlParser.parseSingleStatement(alterSql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        MaterializedView mv = ((MaterializedView) testDb.getTable("mv_to_check"));
        Assert.assertNotNull(mv);
        Assert.assertFalse(mv.isActive());
    }

    @Test
    public void testMaterializedViewWithHint() throws Exception {
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
                .withMaterializedView("create materialized view mv_with_hint\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select /*+ SET_VAR(query_timeout = 500) */ k1, k2, sum(v1) as total from tbl1 group by k1, k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView mv = ((MaterializedView) testDb.getTable("mv_with_hint"));
        String mvTaskName = "mv-" + mv.getId();
        Task task = connectContext.getGlobalStateMgr().getTaskManager().getTask(mvTaskName);
        Assert.assertNotNull(task);
        Map<String, String> taskProperties = task.getProperties();
        Assert.assertTrue(taskProperties.containsKey("query_timeout"));
        Assert.assertEquals("500", taskProperties.get("query_timeout"));
        Assert.assertEquals(Constants.TaskType.EVENT_TRIGGERED, task.getType());
        Assert.assertTrue(task.getDefinition(), task.getDefinition().contains("query_timeout = 500"));
    }

    @Test
    public void testRollupMaterializedViewWithScalarFunction() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE `part_with_mv` (\n" +
                        "  `p_partkey` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `p_name` varchar(55) NOT NULL COMMENT \"\",\n" +
                        "  `p_mfgr` varchar(25) NOT NULL COMMENT \"\",\n" +
                        "  `p_brand` varchar(10) NOT NULL COMMENT \"\",\n" +
                        "  `p_type` varchar(25) NOT NULL COMMENT \"\",\n" +
                        "  `p_size` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `p_container` varchar(10) NOT NULL COMMENT \"\",\n" +
                        "  `p_retailprice` decimal64(15, 2) NOT NULL COMMENT \"\",\n" +
                        "  `p_comment` varchar(23) NOT NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`p_partkey`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 24\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\");");
        String createMvSql = "create materialized view mv1 as select p_partkey, p_name, length(p_brand) as v1 " +
                "from part_with_mv;";
        StatementBase statement = SqlParser.parseSingleStatement(createMvSql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        Assert.assertTrue(Strings.isNullOrEmpty(connectContext.getState().getErrorMessage()));
    }

    @Test(expected = SemanticException.class)
    public void testNonPartitionMvSupportedProperties() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE goods(\n" +
                        "item_id1 INT,\n" +
                        "item_name STRING,\n" +
                        "price FLOAT\n" +
                        ") DISTRIBUTED BY HASH(item_id1)\n" +
                        "PROPERTIES(\"replication_num\" = \"1\");");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW order_mv\n" +
                "DISTRIBUTED BY HASH(item_id1) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"partition_refresh_number\" = \"10\"\n" +
                ")\n" +
                "REFRESH ASYNC\n" +
                "AS SELECT\n" +
                "item_id1,\n" +
                "sum(price) as total\n" +
                "FROM goods\n" +
                "GROUP BY item_id1;");
    }

    @Test
    public void testCreateMaterializedViewWithInactiveMaterializedView() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE base_table\n" +
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
                .withMaterializedView("CREATE MATERIALIZED VIEW base_mv\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "REFRESH manual\n" +
                        "as select k1,k2,v1 from base_table;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView baseMv = ((MaterializedView) testDb.getTable("base_mv"));
        baseMv.setInactiveAndReason("");

        SinglePartitionInfo singlePartitionInfo = new SinglePartitionInfo();
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(3, Lists.newArrayList(columns.get(0)));
        MaterializedView mv = new MaterializedView(1000, testDb.getId(), "mv", columns, KeysType.AGG_KEYS,
                singlePartitionInfo, hashDistributionInfo, refreshScheme);
        List<BaseTableInfo> baseTableInfos = Lists.newArrayList();
        BaseTableInfo baseTableInfo = new BaseTableInfo(testDb.getId(), testDb.getFullName(), baseMv.getName(), baseMv.getId());
        baseTableInfos.add(baseTableInfo);
        mv.setBaseTableInfos(baseTableInfos);
        mv.onReload();

        Assert.assertFalse(mv.isActive());
    }

    @Test
    public void testMvMysqlType() {
        MaterializedView mv = new MaterializedView();
        String mysqlType = mv.getMysqlType();
        Assert.assertEquals("VIEW", mysqlType);
    }

    @Test
    public void testShouldRefreshBy() {
        MaterializedView mv = new MaterializedView();
        MaterializedView.MvRefreshScheme mvRefreshScheme = new MaterializedView.MvRefreshScheme();
        mvRefreshScheme.setType(MaterializedView.RefreshType.ASYNC);
        mv.setRefreshScheme(mvRefreshScheme);
        boolean shouldRefresh = mv.shouldTriggeredRefreshBy(null, null);
        Assert.assertTrue(shouldRefresh);
        mv.setTableProperty(new TableProperty(Maps.newConcurrentMap()));
        shouldRefresh = mv.shouldTriggeredRefreshBy(null, null);
        Assert.assertTrue(shouldRefresh);
    }

    @Test
    public void testShowSyncMV() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl_sync_mv\n" +
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
                .withMaterializedView("create materialized view sync_mv_to_check\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "as select k2, sum(v1) as total from tbl_sync_mv group by k2;");
        String showSql = "show create materialized view sync_mv_to_check;";
        StatementBase statement = SqlParser.parseSingleStatement(showSql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        Assert.assertEquals(connectContext.getState().getStateType(), QueryState.MysqlStateType.EOF);
    }

    @Test
    public void testAlterMVWithIndex() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.table1\n" +
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
                .withMaterializedView("create materialized view index_mv_to_check\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "as select k2, sum(v1) as total from table1 group by k2;");

        Database db = connectContext.getGlobalStateMgr().getDb("test");
        Assert.assertNotNull(db);
        Table table = db.getTable("index_mv_to_check");
        Assert.assertNotNull(table);

        String bitmapSql = "create index index1 ON test.index_mv_to_check (k2) USING BITMAP COMMENT 'balabala'";
        AlterTableStmt alterMVStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(bitmapSql, connectContext);
        DDLStmtExecutor.execute(alterMVStmt, connectContext);
        Optional<AlterJobV2> job = MVTestUtils.findAlterJobV2(db.getId(), table.getId());
        Assert.assertTrue("Alter job should be present", job.isPresent());
        waitForSchemaChangeAlterJobFinish(job.get());

        String bloomfilterSql = "alter table test.index_mv_to_check set (\"bloom_filter_columns\"=\"k2\")";
        alterMVStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(bloomfilterSql, connectContext);
        DDLStmtExecutor.execute(alterMVStmt, connectContext);
        job = MVTestUtils.findAlterJobV2(db.getId(), table.getId());
        waitForSchemaChangeAlterJobFinish(job.get());

        Assert.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testShowMVWithIndex() throws Exception {
        testAlterMVWithIndex();
        String showCreateSql = "show create materialized view test.index_mv_to_check;";
        ShowCreateTableStmt showCreateTableStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateSql, connectContext);
        ShowResultSet showResultSet = ShowExecutor.execute(showCreateTableStmt, connectContext);
        System.out.println(showResultSet.getMetaData().toString());
        System.out.println(showResultSet.getResultRows());
    }

    @Test
    public void testAlterViewWithIndex() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.table1\n" +
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
                .withView("create view index_view_to_check\n" +
                        "as select k2, sum(v1) as total from table1 group by k2;");
        String bitmapSql = "create index index1 ON test.index_view_to_check (k2) USING BITMAP COMMENT 'balabala'";
        AlterTableStmt alterViewStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(bitmapSql, connectContext);
        Assert.assertThrows("Do not support alter non-native table/materialized-view[index_view_to_check]",
                SemanticException.class,
                () -> DDLStmtExecutor.execute(alterViewStmt, connectContext));
    }

    public void testCreateMV(String mvSql) throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.table1\n" +
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
        starRocksAssert.withMaterializedView(mvSql);
        String showCreateSql = "show create materialized view test.index_mv_to_check;";
        ShowCreateTableStmt showCreateTableStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateSql, connectContext);
        ShowResultSet showResultSet = ShowExecutor.execute(showCreateTableStmt, connectContext);
        System.out.println(showResultSet.getResultRows());
    }

    @Test
    public void testCreateMVWithIndex() throws Exception {
        String mvSqlWithBitMapAndBloomfilter = "create materialized view index_mv_to_check " +
                "(k2 ," +
                " total ," +
                "INDEX index1 (`k2`) USING BITMAP COMMENT 'balabala' " +
                ")" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES " +
                "("
                + "\"replicated_storage\" = \"true\","
                + "\"replication_num\" = \"1\","
                + "\"storage_medium\" = \"HDD\","
                + "\"bloom_filter_columns\" = \"k2\""
                + ")" +
                "as select k2, sum(v1) as total from table1 group by k2;";
        String mvSqlWithBitMap = "create materialized view index_mv_to_check " +
                "(k2 ," +
                " total ," +
                "INDEX index1 (`k2`) USING BITMAP COMMENT 'balabala' " +
                ")" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES " +
                "("
                + "\"replicated_storage\" = \"true\","
                + "\"replication_num\" = \"1\","
                + "\"storage_medium\" = \"HDD\""
                + ")" +
                "as select k2, sum(v1) as total from table1 group by k2;";
        String mvSqlWithBloomFilter = "create materialized view index_mv_to_check " +
                "(k2 ," +
                " total" +
                ")" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES " +
                "("
                + "\"replicated_storage\" = \"true\","
                + "\"replication_num\" = \"1\","
                + "\"storage_medium\" = \"HDD\","
                + "\"bloom_filter_columns\" = \"k2\""
                + ")" +
                "as select k2, sum(v1) as total from table1 group by k2;";
        testCreateMV(mvSqlWithBitMapAndBloomfilter);
        testCreateMV(mvSqlWithBitMap);
        testCreateMV(mvSqlWithBloomFilter);
    }


    @Test
    public void testCreateMVWithDuplicateIndexOrDuplicateColumn() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.table1\n" +
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
        String mvSql = "create materialized view index_mv_to_check " +
                "(k2 ," +
                " total ," +
                "INDEX index1 (`k2`) USING BITMAP COMMENT 'balabala', " +
                "INDEX index1 (`total`) USING BITMAP COMMENT 'balabala' " +
                ")" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES " +
                "("
                + "\"replicated_storage\" = \"true\","
                + "\"replication_num\" = \"1\","
                + "\"storage_medium\" = \"HDD\","
                + "\"bloom_filter_columns\" = \"k2\""
                + ")" +
                "as select k2, sum(v1) as total from table1 group by k2;";
        Assert.assertThrows("Duplicate index name 'index1'",
                UserException.class,
                () -> starRocksAssert.withMaterializedView(mvSql));

        String mvSql2 = "create materialized view index_mv_to_check " +
                "(k2 ," +
                " total ," +
                "INDEX index1 (`k2`) USING BITMAP COMMENT 'balabala', " +
                "INDEX index2 (`k2`) USING BITMAP COMMENT 'balabala' " +
                ")" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES " +
                "("
                + "\"replicated_storage\" = \"true\","
                + "\"replication_num\" = \"1\","
                + "\"storage_medium\" = \"HDD\","
                + "\"bloom_filter_columns\" = \"k2\""
                + ")" +
                "as select k2, sum(v1) as total from table1 group by k2;";
        Assert.assertThrows("Duplicate column name 'k2' in index",
                UserException.class,
                () -> starRocksAssert.withMaterializedView(mvSql2));
    }
}
