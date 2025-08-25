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
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.PropertyAnalyzer;
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
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.sql.optimizer.MVTestUtils.waitForSchemaChangeAlterJobFinish;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestMethodOrder(MethodName.class)
public class MaterializedViewTest extends StarRocksTestBase {

    private static List<Column> columns = new LinkedList<Column>();
    private ConnectContext connectContext;
    private StarRocksAssert starRocksAssert;

    @BeforeEach
    public void setUp() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        UtFrameUtils.createMinStarRocksCluster();

        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.SUM, "", ""));

        super.before();

        starRocksAssert
                .withDatabase("test")
                .useDatabase("test")
                .withTable("CREATE TABLE base_t1\n" +
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
    }

    @Test
    public void testInit() {
        MaterializedView mv = new MaterializedView();
        Assertions.assertEquals(Table.TableType.MATERIALIZED_VIEW, mv.getType());
        Assertions.assertEquals(null, mv.getTableProperty());

        MaterializedView mv2 = new MaterializedView(1000, 100, "mv2", columns, KeysType.AGG_KEYS,
                null, null, null);
        Assertions.assertEquals(100, mv2.getDbId());
        Assertions.assertEquals(Table.TableType.MATERIALIZED_VIEW, mv2.getType());
        Assertions.assertEquals(null, mv2.getTableProperty());
        Assertions.assertEquals("mv2", mv2.getName());
        Assertions.assertEquals(KeysType.AGG_KEYS, mv2.getKeysType());
        mv2.setBaseIndexId(10003);
        Assertions.assertEquals(10003, mv2.getBaseIndexId());
        Assertions.assertFalse(mv2.isPartitioned());
        mv2.setState(OlapTable.OlapTableState.ROLLUP);
        Assertions.assertEquals(OlapTable.OlapTableState.ROLLUP, mv2.getState());
        Assertions.assertEquals(null, mv2.getDefaultDistributionInfo());
        Assertions.assertEquals(null, mv2.getPartitionInfo());
        mv2.setReplicationNum((short) 3);
        Assertions.assertEquals(3, mv2.getDefaultReplicationNum().shortValue());
        mv2.setStorageMedium(TStorageMedium.SSD);
        Assertions.assertEquals("SSD", mv2.getStorageMedium());
        Assertions.assertEquals(true, mv2.isActive());
        mv2.setInactiveAndReason("");
        Assertions.assertEquals(false, mv2.isActive());

        List<BaseTableInfo> baseTableInfos = Lists.newArrayList();
        BaseTableInfo baseTableInfo1 = new BaseTableInfo(100L, "db", "tbl1", 10L);
        baseTableInfos.add(baseTableInfo1);
        BaseTableInfo baseTableInfo2 = new BaseTableInfo(100L, "db", "tbl2", 20L);
        baseTableInfos.add(baseTableInfo2);
        mv2.setBaseTableInfos(baseTableInfos);
        List<BaseTableInfo> baseTableInfosCheck = mv2.getBaseTableInfos();

        Assertions.assertEquals(10L, baseTableInfosCheck.get(0).getTableId());
        Assertions.assertEquals(20L, baseTableInfosCheck.get(1).getTableId());

        String mvDefinition = "create materialized view mv2 select col1, col2 from table1";
        mv2.setViewDefineSql(mvDefinition);
        Assertions.assertEquals(mvDefinition, mv2.getViewDefineSql());
    }

    @Test
    public void testSchema() {
        MaterializedView mv = new MaterializedView(1000, 100, "mv2", columns, KeysType.AGG_KEYS,
                null, null, null);
        mv.setBaseIndexId(1L);
        mv.setIndexMeta(1L, "mv_name", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        Assertions.assertEquals(1, mv.getBaseIndexId());
        mv.rebuildFullSchema();
        Assertions.assertEquals("mv_name", mv.getIndexNameById(1L));
        List<Column> indexColumns = Lists.newArrayList(columns.get(0), columns.get(2));
        mv.setIndexMeta(2L, "index_name", indexColumns, 0,
                222, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        mv.rebuildFullSchema();
        Assertions.assertEquals("index_name", mv.getIndexNameById(2L));
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
        Assertions.assertEquals("mv_name", mv.getName());
        mv.setName("new_name");
        Assertions.assertEquals("new_name", mv.getName());
        PartitionInfo pInfo1 = mv.getPartitionInfo();
        Assertions.assertTrue(pInfo1 instanceof SinglePartitionInfo);

        MaterializedIndex index = new MaterializedIndex(3, IndexState.NORMAL);
        Partition partition = new Partition(2, 22, "mv_name", index, distributionInfo);
        mv.addPartition(partition);
        Partition tmpPartition = mv.getPartition("mv_name");
        Assertions.assertTrue(tmpPartition != null);
        Assertions.assertEquals(2L, tmpPartition.getId());
        Assertions.assertEquals(1, mv.getPartitions().size());
        Assertions.assertEquals(1, mv.getPartitionNames().size());
        Assertions.assertEquals(0, mv.getPartitionColumnNames().size());
        Assertions.assertTrue(mv.isPartitioned());

        PartitionInfo rangePartitionInfo = new RangePartitionInfo(Lists.newArrayList(columns.get(0)));
        rangePartitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        rangePartitionInfo.setReplicationNum(1, (short) 3);
        rangePartitionInfo.setIsInMemory(1, false);
        rangePartitionInfo.setTabletType(1, TTabletType.TABLET_TYPE_DISK);

        MaterializedView mv2 = new MaterializedView(1000, 100, "mv_name_2", columns, KeysType.AGG_KEYS,
                rangePartitionInfo, distributionInfo, refreshScheme);
        mv2.setIndexMeta(1L, "mv_name_2", columns, 0,
                111, (short) 2, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        Assertions.assertEquals("mv_name_2", mv2.getName());
        mv2.setName("new_name_2");
        Assertions.assertEquals("new_name_2", mv2.getName());
        PartitionInfo pInfo2 = mv2.getPartitionInfo();
        Assertions.assertTrue(pInfo2 instanceof RangePartitionInfo);
        Partition partition2 = new Partition(3, 33, "p1", index, distributionInfo);
        mv2.addPartition(partition2);
        Partition tmpPartition2 = mv2.getPartition("p1");
        Assertions.assertTrue(tmpPartition2 != null);
        Assertions.assertEquals(3L, tmpPartition2.getId());
        Assertions.assertEquals(1, mv2.getPartitions().size());
        Assertions.assertEquals(1, mv2.getPartitionNames().size());
        Assertions.assertEquals(1, mv2.getPartitionColumnNames().size());
        Assertions.assertTrue(mv2.isPartitioned());
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
        Assertions.assertTrue(distributionInfo1 instanceof RandomDistributionInfo);
        Assertions.assertEquals(0, mv.getDistributionColumnNames().size());

        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(10, Lists.newArrayList(columns.get(0)));
        MaterializedView mv2 = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, hashDistributionInfo, refreshScheme);
        DistributionInfo distributionInfo2 = mv2.getDefaultDistributionInfo();
        Assertions.assertTrue(distributionInfo2 instanceof HashDistributionInfo);
        Assertions.assertEquals(1, mv2.getDistributionColumnNames().size());
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
        Assertions.assertEquals(TTableType.MATERIALIZED_VIEW, tableDescriptor.getTableType());
        Assertions.assertEquals(1000, tableDescriptor.getId());
        Assertions.assertEquals("mv_name", tableDescriptor.getTableName());
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

        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        Assertions.assertNotNull(db);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv_to_rename");
        Assertions.assertNotNull(table);
        // test partition related info
        MaterializedView oldMv = (MaterializedView) table;
        Assertions.assertTrue(oldMv.getRefreshScheme().isAsync());
        Assertions.assertTrue(oldMv.getRefreshScheme().toString().contains("MvRefreshScheme"));
        Map<Table, List<Column>> partitionMap = oldMv.getRefBaseTablePartitionColumns();
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");
        Assertions.assertTrue(partitionMap.containsKey(table1));
        List<Table.TableType> baseTableType = oldMv.getBaseTableTypes();
        Assertions.assertEquals(1, baseTableType.size());
        Assertions.assertEquals(table1.getType(), baseTableType.get(0));
        connectContext.executeSql("refresh materialized view mv_to_rename with sync mode");
        Optional<Long> maxTime = oldMv.maxBaseTableRefreshTimestamp();
        Assertions.assertTrue(maxTime.isPresent());
        Pair<Table, Column> pair = MaterializedViewTestBase.getRefBaseTablePartitionColumn(oldMv);
        Assertions.assertEquals("tbl1", pair.first.getName());

        String alterSql = "alter materialized view mv_to_rename rename mv_new_name;";
        StatementBase statement = SqlParser.parseSingleStatement(alterSql, connectContext.getSessionVariable().getSqlMode());

        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv_new_name"));
        Assertions.assertNotNull(mv);
        Assertions.assertEquals("mv_new_name", mv.getName());
        ExpressionRangePartitionInfo partitionInfo = (ExpressionRangePartitionInfo) mv.getPartitionInfo();
        List<Expr> exprs = partitionInfo.getPartitionExprs(mv.getIdToColumn());
        Assertions.assertEquals(1, exprs.size());
        Assertions.assertTrue(exprs.get(0) instanceof SlotRef);
        SlotRef slotRef = (SlotRef) exprs.get(0);
        Assertions.assertEquals("mv_new_name", slotRef.getTblNameWithoutAnalyzed().getTbl());
        starRocksAssert.dropMaterializedView("mv_new_name");

        String alterSql2 = "alter materialized view mv_to_rename2 rename mv_new_name2;";
        statement = SqlParser.parseSingleStatement(alterSql2, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, statement);
        stmtExecutor2.execute();
        MaterializedView mv2 = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv_new_name2"));
        Assertions.assertNotNull(mv2);
        Assertions.assertEquals("mv_new_name2", mv2.getName());
        ExpressionRangePartitionInfo partitionInfo2 = (ExpressionRangePartitionInfo) mv2.getPartitionInfo();
        List<Expr> exprs2 = partitionInfo2.getPartitionExprs(mv2.getIdToColumn());
        Assertions.assertEquals(1, exprs2.size());
        Assertions.assertTrue(exprs2.get(0) instanceof FunctionCallExpr);
        Expr rightChild = exprs2.get(0).getChild(1);
        Assertions.assertTrue(rightChild instanceof SlotRef);
        SlotRef slotRef2 = (SlotRef) rightChild;
        Assertions.assertEquals("mv_new_name2", slotRef2.getTblNameWithoutAnalyzed().getTbl());
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

        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        Assertions.assertNotNull(db);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv_replay");
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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv_to_check"));
        String dropSql = "drop table tbl_drop;";
        StatementBase statement = SqlParser.parseSingleStatement(dropSql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        Assertions.assertNotNull(mv);
        Assertions.assertFalse(mv.isActive());
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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        String alterSql = "alter table tbl_to_rename rename new_tbl_name;";
        StatementBase statement = SqlParser.parseSingleStatement(alterSql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv_to_check"));
        Assertions.assertNotNull(mv);
        Assertions.assertFalse(mv.isActive());
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
                        "as select /*+ SET_VAR(query_timeout = 500) */ k1, k2, sum(v1) " +
                        "as total from tbl1 group by k1, k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv_with_hint"));
        String mvTaskName = "mv-" + mv.getId();
        Task task = connectContext.getGlobalStateMgr().getTaskManager().getTask(mvTaskName);
        Assertions.assertNotNull(task);
        Map<String, String> taskProperties = task.getProperties();
        Assertions.assertTrue(taskProperties.containsKey("query_timeout"));
        Assertions.assertEquals("500", taskProperties.get("query_timeout"));
        Assertions.assertEquals(Constants.TaskType.EVENT_TRIGGERED, task.getType());
        Assertions.assertTrue(task.getDefinition().contains("query_timeout = 500"), task.getDefinition());
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
        Assertions.assertTrue(Strings.isNullOrEmpty(connectContext.getState().getErrorMessage()));
    }

    @Test
    public void testNonPartitionMvSupportedProperties() {
        assertThrows(SemanticException.class, () -> {
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
        });
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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView baseMv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "base_mv"));
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

        Assertions.assertFalse(mv.isActive());
    }

    @Test
    public void testMvMysqlType() {
        MaterializedView mv = new MaterializedView();
        String mysqlType = mv.getMysqlType();
        Assertions.assertEquals("VIEW", mysqlType);
    }

    @Test
    public void testShouldRefreshBy() {
        MaterializedView mv = new MaterializedView();
        MaterializedView.MvRefreshScheme mvRefreshScheme = new MaterializedView.MvRefreshScheme();
        mvRefreshScheme.setType(MaterializedViewRefreshType.ASYNC);
        mv.setRefreshScheme(mvRefreshScheme);
        boolean shouldRefresh = mv.shouldTriggeredRefreshBy(null, null);
        Assertions.assertTrue(shouldRefresh);
        mv.setTableProperty(new TableProperty(Maps.newConcurrentMap()));
        shouldRefresh = mv.shouldTriggeredRefreshBy(null, null);
        Assertions.assertTrue(shouldRefresh);
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
        Assertions.assertEquals(connectContext.getState().getStateType(), QueryState.MysqlStateType.EOF);
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

        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        Assertions.assertNotNull(db);
        Table table = db.getTable("index_mv_to_check");
        Assertions.assertNotNull(table);

        String bitmapSql = "create index index1 ON test.index_mv_to_check (k2) USING BITMAP COMMENT 'balabala'";
        AlterTableStmt alterMVStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(bitmapSql, connectContext);
        DDLStmtExecutor.execute(alterMVStmt, connectContext);
        Optional<AlterJobV2> job = MVTestUtils.findAlterJobV2(db.getId(), table.getId());
        Assertions.assertTrue(job.isPresent(), "Alter job should be present");
        waitForSchemaChangeAlterJobFinish(job.get());

        String bloomfilterSql = "alter table test.index_mv_to_check set (\"bloom_filter_columns\"=\"k2\")";
        alterMVStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(bloomfilterSql, connectContext);
        DDLStmtExecutor.execute(alterMVStmt, connectContext);
        job = MVTestUtils.findAlterJobV2(db.getId(), table.getId());
        waitForSchemaChangeAlterJobFinish(job.get());

        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());

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
        Assertions.assertThrows(SemanticException.class,
                () -> DDLStmtExecutor.execute(alterViewStmt, connectContext),
                "Do not support alter non-native table/materialized-view[index_view_to_check]");
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

    private String getShowMVResult(String mvName) throws Exception {
        String showCreateSql = "show create materialized view " + mvName + ";";
        ShowCreateTableStmt showCreateTableStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateSql, connectContext);
        ShowResultSet showResultSet = ShowExecutor.execute(showCreateTableStmt, connectContext);
        System.out.println(showResultSet.getResultRows());
        List<List<String>> result = showResultSet.getResultRows();
        Assertions.assertEquals(1, result.size());
        String actual = result.get(0).get(1);
        System.out.println(actual);
        return actual;
    }

    private void assertShowMVContains(String mvName, String expect) throws Exception {
        String actual = getShowMVResult(mvName);
        Assertions.assertTrue(actual.contains(expect));
    }

    private void assertShowMVNotContains(String mvName, String expect) throws Exception {
        String actual = getShowMVResult(mvName);
        Assertions.assertFalse(actual.contains(expect));
    }

    @Test
    public void testAlterMVBloomFilterIndexes1() throws Exception {
        String sql = "create materialized view test_mv1 " +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES " +
                "("
                + "\"replication_num\" = \"1\""
                + ")" +
                "as select k2, sum(v1) as total from base_t1 group by k2;";
        starRocksAssert.withMaterializedView(sql);
        assertShowMVNotContains("test_mv1", "bloom_filter_columns");
        starRocksAssert.ddl("ALTER MATERIALIZED VIEW test_mv1 SET (" +
                "\"bloom_filter_columns\" = \"k2\");");
        assertShowMVContains("test_mv1", "\"bloom_filter_columns\" = \"k2\"");
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
        Assertions.assertThrows(StarRocksException.class,
                () -> starRocksAssert.withMaterializedView(mvSql),
                "Duplicate index name 'index1'");

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
        Assertions.assertThrows(StarRocksException.class,
                () -> starRocksAssert.withMaterializedView(mvSql2),
                "Duplicate column name 'k2' in index");
    }

    @Test
    public void testBasePartitionInfo() {
        MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(-1L, -1L, 123456L);
        Assertions.assertEquals(-1, basePartitionInfo.getExtLastFileModifiedTime());
        Assertions.assertEquals(-1, basePartitionInfo.getFileNumber());
        basePartitionInfo.setExtLastFileModifiedTime(100);
        basePartitionInfo.setFileNumber(10);
        Assertions.assertEquals(100, basePartitionInfo.getExtLastFileModifiedTime());
        Assertions.assertEquals(10, basePartitionInfo.getFileNumber());
        Assertions.assertTrue(basePartitionInfo.toString().contains(
                "BasePartitionInfo{id=-1, version=-1, lastRefreshTime=123456, lastFileModifiedTime=100, fileNumber=10}"));
    }

    @Test
    public void testCreateMVWithCoolDownTime() throws Exception {
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
        String mvSql = "create materialized view mv_cooldowun_check " +
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
                + "\"storage_medium\" = \"SSD\","
                + "\"storage_cooldown_time\" = \"9999-12-31 23:59:59\""
                + ")" +
                "as select k2, sum(v1) as total from table1 group by k2;";
        // correct behavior
        starRocksAssert.withMaterializedView(mvSql);
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView baseMv = ((MaterializedView) testDb.getTable("mv_cooldowun_check"));
        Assertions.assertEquals(4, baseMv.getTableProperty().getProperties().size());
        Assertions.assertEquals("253402271999000", baseMv.getTableProperty().getProperties().get("storage_cooldown_time"));

        // Mock add partition
        Map<String, String> propertiesAddPartitionCase = MvUtils.getPartitionProperties(baseMv);
        Assertions.assertEquals(3, propertiesAddPartitionCase.size());
        Assertions.assertTrue(propertiesAddPartitionCase.containsKey("storage_cooldown_time"));

        Config.tablet_sched_storage_cooldown_second = 2592000;
        DataProperty mockDataProperty = PropertyAnalyzer.analyzeDataProperty(propertiesAddPartitionCase,
                DataProperty.getInferredDefaultDataProperty(), false);
        Assertions.assertTrue(mockDataProperty.getCooldownTimeMs() == 253402271999000L);
        // correct behavior

        // misbehavior
        starRocksAssert.dropMaterializedView("mv_cooldowun_check");
        starRocksAssert.withMaterializedView(mvSql);
        testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        baseMv = ((MaterializedView) testDb.getTable("mv_cooldowun_check"));
        Assertions.assertEquals(4, baseMv.getTableProperty().getProperties().size());
        Assertions.assertEquals("253402271999000", baseMv.getTableProperty().getProperties().get("storage_cooldown_time"));

        //Mock add partition
        propertiesAddPartitionCase = MvUtils.getPartitionProperties(baseMv);
        Assertions.assertEquals(3, propertiesAddPartitionCase.size());
        Assertions.assertTrue(propertiesAddPartitionCase.containsKey("storage_cooldown_time"));
        propertiesAddPartitionCase.remove("storage_cooldown_time");

        Config.tablet_sched_storage_cooldown_second = 2592000;
        mockDataProperty = PropertyAnalyzer.analyzeDataProperty(propertiesAddPartitionCase,
                DataProperty.getInferredDefaultDataProperty(), false);
        Assertions.assertTrue(mockDataProperty.getCooldownTimeMs() < 253402271999000L);
        // misbehavior
    }

    @Test
    public void testMaterializedViewReloadNotPostLoadImage() throws Exception {
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
                        "as select k1,k2,v1 from base_table;")
                .withMaterializedView("CREATE MATERIALIZED VIEW mv1\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "REFRESH manual\n" +
                        "as select k1,k2,v1 from base_mv;")
                .withMaterializedView("CREATE MATERIALIZED VIEW mv2\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "REFRESH manual\n" +
                        "as select k1,k2,v1 from base_mv;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table baseTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "base_table");
        MaterializedView baseMv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "base_mv"));
        MaterializedView mv1 = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv1"));
        MaterializedView mv2 = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv2"));

        {
            Config.enable_mv_post_image_reload_cache = false;
            boolean postLoadImage = false;

            baseMv.setReloaded(false);
            baseTable.removeRelatedMaterializedView(baseMv.getMvId());
            baseMv.removeRelatedMaterializedView(mv1.getMvId());
            baseMv.removeRelatedMaterializedView(mv2.getMvId());

            mv1.onReload(postLoadImage);
            mv2.onReload(postLoadImage);

            Assertions.assertFalse(baseMv.hasReloaded());
            Assertions.assertEquals(1, baseTable.getRelatedMaterializedViews().size());
            Assertions.assertEquals(2, baseMv.getRelatedMaterializedViews().size());
            Assertions.assertTrue(baseMv.getRelatedMaterializedViews().contains(mv1.getMvId()));
            Assertions.assertTrue(baseMv.getRelatedMaterializedViews().contains(mv2.getMvId()));
        }

        {
            Config.enable_mv_post_image_reload_cache = true;
            boolean postLoadImage = true;

            baseMv.setReloaded(false);
            baseTable.removeRelatedMaterializedView(baseMv.getMvId());
            baseMv.removeRelatedMaterializedView(mv1.getMvId());
            baseMv.removeRelatedMaterializedView(mv2.getMvId());

            mv1.onReload(postLoadImage);
            mv2.onReload(postLoadImage);

            Assertions.assertTrue(baseMv.hasReloaded());
            Assertions.assertEquals(1, baseTable.getRelatedMaterializedViews().size());
            Assertions.assertEquals(2, baseMv.getRelatedMaterializedViews().size());
            Assertions.assertTrue(baseMv.getRelatedMaterializedViews().contains(mv1.getMvId()));
            Assertions.assertTrue(baseMv.getRelatedMaterializedViews().contains(mv2.getMvId()));
        }

        {
            Config.enable_mv_post_image_reload_cache = true;
            boolean postLoadImage = false;

            baseMv.setReloaded(false);
            baseTable.removeRelatedMaterializedView(baseMv.getMvId());
            baseMv.removeRelatedMaterializedView(mv1.getMvId());
            baseMv.removeRelatedMaterializedView(mv2.getMvId());

            mv1.onReload(postLoadImage);
            mv2.onReload(postLoadImage);

            Assertions.assertFalse(baseMv.hasReloaded());
            Assertions.assertEquals(1, baseTable.getRelatedMaterializedViews().size());
            Assertions.assertEquals(2, baseMv.getRelatedMaterializedViews().size());
            Assertions.assertTrue(baseMv.getRelatedMaterializedViews().contains(mv1.getMvId()));
            Assertions.assertTrue(baseMv.getRelatedMaterializedViews().contains(mv2.getMvId()));
        }
    }


    @Test
    public void testMaterializedViewReloadPostLoadImage() throws Exception {
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
                        "as select k1,k2,v1 from base_table;")
                .withMaterializedView("CREATE MATERIALIZED VIEW mv1\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "REFRESH manual\n" +
                        "as select k1,k2,v1 from base_mv;")
                .withMaterializedView("CREATE MATERIALIZED VIEW mv2\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "REFRESH manual\n" +
                        "as select k1,k2,v1 from base_mv;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table baseTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "base_table");
        MaterializedView baseMv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "base_mv"));

        List<Table> baseTables = baseMv.getBaseTables();
        Assertions.assertEquals(1, baseTables.size());
        Assertions.assertEquals(baseTable.getId(), baseTables.get(0).getId());
        List<Table.TableType> baseTableTypes = baseMv.getBaseTableTypes();
        Assertions.assertEquals(1, baseTableTypes.size());
        Assertions.assertEquals(Table.TableType.OLAP, baseTableTypes.get(0));

        MaterializedView mv1 = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv1"));
        MaterializedView mv2 = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv2"));

        baseTable.removeRelatedMaterializedView(baseMv.getMvId());
        baseMv.removeRelatedMaterializedView(mv1.getMvId());
        baseMv.removeRelatedMaterializedView(mv2.getMvId());

        Assertions.assertFalse(mv1.hasReloaded());
        Assertions.assertFalse(mv2.hasReloaded());
        Assertions.assertFalse(baseMv.hasReloaded());

        Config.enable_mv_post_image_reload_cache = true;
        // do post image reload
        GlobalStateMgr.getCurrentState().processMvRelatedMeta();

        // after post image reload, all materialized views should have `reloaded` flag reset to false
        Assertions.assertFalse(mv1.hasReloaded());
        Assertions.assertFalse(mv2.hasReloaded());
        Assertions.assertFalse(baseMv.hasReloaded());
        Assertions.assertEquals(1, baseTable.getRelatedMaterializedViews().size());
        Assertions.assertEquals(2, baseMv.getRelatedMaterializedViews().size());
        Assertions.assertTrue(baseMv.getRelatedMaterializedViews().contains(mv1.getMvId()));
        Assertions.assertTrue(baseMv.getRelatedMaterializedViews().contains(mv2.getMvId()));
    }

    @Test
    public void testGsonPrePostProcess() throws Exception {
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
                        "as select k1,k2,v1 from base_table;")
                .withMaterializedView("CREATE MATERIALIZED VIEW mv1\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "REFRESH manual\n" +
                        "as select k1,k2,v1 from base_mv;")
                .withMaterializedView("CREATE MATERIALIZED VIEW mv2\n" +
                        "PARTITION BY k1\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "REFRESH manual\n" +
                        "as select k1,k2,v1 from base_mv;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView baseMv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "base_mv"));
        Assertions.assertTrue(baseMv.getPartitionExprMaps().size() == 1);
        baseMv.gsonPreProcess();
        baseMv.gsonPostProcess();
        Assertions.assertTrue(baseMv.getPartitionExprMaps().size() == 1);

        MaterializedView mv1 = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "mv1"));
        Assertions.assertTrue(mv1.getPartitionExprMaps().size() == 1);
        baseMv.gsonPreProcess();
        baseMv.gsonPostProcess();
        Assertions.assertTrue(mv1.getPartitionExprMaps().size() == 1);
    }

    @Test
    public void testMVWithView() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.base_table1 \n" +
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
                .withView("create view base_view1 \n" +
                        "as select k2, sum(v1) as total from base_table1 group by k2;")
                .withMaterializedView("create materialized view test_mv1 \n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select * from base_view1;");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_mv1"));
        Assertions.assertNotNull(mv);
        Assertions.assertTrue(mv.isActive());

        Table baseTable = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "base_table1");
        Table baseView = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "base_view1");
        Assertions.assertNotNull(baseView);

        List<Table> baseTables = mv.getBaseTables();
        Assertions.assertEquals(2, baseTables.size());
        Assertions.assertEquals("base_view1", baseTables.get(0).getName());
        Assertions.assertEquals(baseView, baseTables.get(0));
        Assertions.assertEquals("base_table1", baseTables.get(1).getName());
        Assertions.assertEquals(baseTable, baseTables.get(1));

        List<BaseTableInfo> baseTablesWithView = mv.getBaseTableInfosWithoutView();
        Assertions.assertEquals(1, baseTablesWithView.size());
        BaseTableInfo baseTableInfo = baseTablesWithView.get(0);
        Assertions.assertEquals(db.getId(), baseTableInfo.getDbId());
        Assertions.assertEquals(db.getFullName(), baseTableInfo.getDbName());
        Assertions.assertEquals(baseTable.getName(), baseTableInfo.getTableName());
        Assertions.assertEquals(baseTable.getId(), baseTableInfo.getTableId());
    }

    @Test
    public void testPartitionRefreshStrategy() {
        Assertions.assertEquals(MaterializedView.PartitionRefreshStrategy.defaultValue(),
                MaterializedView.PartitionRefreshStrategy.valueOf("STRICT"));
        String sql = "create materialized view test_mv1 " +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES " +
                "("
                + "\"replication_num\" = \"1\""
                + ")" +
                "as select k2, sum(v1) as total from base_t1 group by k2;";
        starRocksAssert.withMaterializedView(sql, (obj) -> {
            String mvName = (String) obj;
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "test_mv1"));
            Assertions.assertEquals(mv.getPartitionRefreshStrategy(), MaterializedView.PartitionRefreshStrategy.STRICT);
        });
    }
}
