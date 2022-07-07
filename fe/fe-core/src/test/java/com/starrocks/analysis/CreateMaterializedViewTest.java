// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RefreshType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.ViewDefBuilder;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.util.ThreadUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateMaterializedViewTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database testDb;
    private static GlobalStateMgr currentState;

    @BeforeClass
    public static void beforeClass() throws Exception {

        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE `aggregate_table_with_null` (\n" +
                        "`k1` date,\n" +
                        "`v2` datetime MAX,\n" +
                        "`v3` char(20) MIN,\n" +
                        "`v4` bigint SUM,\n" +
                        "`v8` bigint SUM,\n" +
                        "`v5` HLL HLL_UNION,\n" +
                        "`v6` BITMAP BITMAP_UNION,\n" +
                        "`v7` PERCENTILE PERCENTILE_UNION\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`k1`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_format\" = \"v2\"\n" +
                        ");")
                .withView("CREATE VIEW v1 AS SELECT * FROM aggregate_table_with_null;")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k2)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('10'),\n" +
                        "    PARTITION p2 values less than('20')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl4\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    k3 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k2,k3)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('20','30'),\n" +
                        "    PARTITION p2 values less than('40','50')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE `t1` (\n" +
                        "  `c_1_0` decimal128(30, 4) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_1` boolean NOT NULL COMMENT \"\",\n" +
                        "  `c_1_2` date NULL COMMENT \"\",\n" +
                        "  `c_1_3` date NOT NULL COMMENT \"\",\n" +
                        "  `c_1_4` double NULL COMMENT \"\",\n" +
                        "  `c_1_5` double NULL COMMENT \"\",\n" +
                        "  `c_1_6` datetime NULL COMMENT \"\",\n" +
                        "  `c_1_7` ARRAY<int(11)> NULL COMMENT \"\",\n" +
                        "  `c_1_8` smallint(6) NULL COMMENT \"\",\n" +
                        "  `c_1_9` bigint(20) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_10` varchar(31) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_11` decimal128(22, 18) NULL COMMENT \"\",\n" +
                        "  `c_1_12` boolean NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(`c_1_3`)\n" +
                        "(PARTITION p20000101 VALUES [('2000-01-01'), ('2010-12-31')),\n" +
                        "PARTITION p20101231 VALUES [('2010-12-31'), ('2021-12-30')),\n" +
                        "PARTITION p20211230 VALUES [('2021-12-30'), ('2032-12-29')))\n" +
                        "DISTRIBUTED BY HASH(`c_1_3`, `c_1_2`, `c_1_0`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");")
                .withTable("CREATE EXTERNAL TABLE mysql_external_table\n" +
                        "(\n" +
                        "    k1 DATE,\n" +
                        "    k2 INT,\n" +
                        "    k3 SMALLINT,\n" +
                        "    k4 VARCHAR(2048),\n" +
                        "    k5 DATETIME\n" +
                        ")\n" +
                        "ENGINE=mysql\n" +
                        "PROPERTIES\n" +
                        "(\n" +
                        "    \"host\" = \"127.0.0.1\",\n" +
                        "    \"port\" = \"3306\",\n" +
                        "    \"user\" = \"mysql_user\",\n" +
                        "    \"password\" = \"mysql_passwd\",\n" +
                        "    \"database\" = \"mysql_db_test\",\n" +
                        "    \"table\" = \"mysql_table_test\"\n" +
                        ");")
                .withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE test2.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2021-02-01'),\n" +
                        "    PARTITION p2 values less than('2021-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl_for_count\n" +
                        "(\n" +
                        "   c_0_0 BIGINT NULL ,\n" +
                        "   c_0_1 DATE NOT NULL ,\n" +
                        "   c_0_2 DECIMAL(37, 5)  NOT NULL,\n" +
                        "   c_0_3 INT MAX NOT NULL ,\n" +
                        "   c_0_4 DATE REPLACE_IF_NOT_NULL NOT NULL ,\n" +
                        "   c_0_5 PERCENTILE PERCENTILE_UNION NOT NULL\n" +
                        ")\n" +
                        "AGGREGATE KEY (c_0_0,c_0_1,c_0_2)\n" +
                        "PARTITION BY RANGE(c_0_1)\n" +
                        "(\n" +
                        "   START (\"2010-01-01\") END (\"2021-12-31\") EVERY (INTERVAL 219 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH (c_0_2,c_0_1) BUCKETS 3\n" +
                        "properties('replication_num'='1');")
                .useDatabase("test");
        currentState = GlobalStateMgr.getCurrentState();
        testDb = currentState.getDb("default_cluster:test");
    }

    private void dropMv(String mvName) throws Exception {
        String sql ="drop materialized view " + mvName;
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statementBase);
        stmtExecutor.execute();
    }

    // ========== full test ==========

    @Test
    public void testFullCreate() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {}
        };
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2)\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            Table mv1 = testDb.getTable("mv1");
            Assert.assertTrue(mv1 instanceof MaterializedView);
            // test partition
            MaterializedView materializedView = (MaterializedView) mv1;
            PartitionInfo partitionInfo = materializedView.getPartitionInfo();
            Assert.assertEquals(partitionInfo.getPartitionColumns().size(),1);
            Assert.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs().get(0);
            Assert.assertTrue(partitionExpr instanceof FunctionCallExpr);
            FunctionCallExpr partitionFunctionCallExpr = (FunctionCallExpr) partitionExpr;
            Assert.assertEquals(partitionFunctionCallExpr.getFnName().getFunction(),"date_trunc");
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionFunctionCallExpr.collect(SlotRef.class, slotRefs);
            SlotRef partitionSlotRef = slotRefs.get(0);
            Assert.assertEquals(partitionSlotRef.getColumnName(), "k1");
            Set<Long> baseTableIds = materializedView.getBaseTableIds();
            Assert.assertEquals(baseTableIds.size(), 1);
            Expr partitionRefTableExpr = materializedView.getPartitionRefTableExprs().get(0);
            List<SlotRef> tableSlotRefs = Lists.newArrayList();
            partitionRefTableExpr.collect(SlotRef.class,tableSlotRefs);
            SlotRef slotRef = tableSlotRefs.get(0);
            TableName baseTableName = slotRef.getTblNameWithoutAnalyzed();
            Assert.assertEquals(baseTableName.getDb(),testDb.getFullName());
            Table baseTable = testDb.getTable(baseTableName.getTbl());
            Assert.assertTrue(baseTable != null);
            Assert.assertTrue(baseTableIds.contains(baseTable.getId()));
            Assert.assertEquals(((OlapTable) baseTable).getRelatedMaterializedViews().size(), 1);
            Column baseColumn = baseTable.getColumn(slotRef.getColumnName());
            Assert.assertTrue(baseColumn != null);
            Assert.assertEquals(baseColumn.getName(), "k1");
            // test sql
            Assert.assertEquals(materializedView.getViewDefineSql(),
                    "SELECT `tb1`.`k1` AS `k1`, `tb1`.`k2` AS `s2` FROM `test`.`tbl1` AS `tb1`");
            // test property
            TableProperty tableProperty = materializedView.getTableProperty();
            Assert.assertEquals(tableProperty.getReplicationNum().shortValue(), 1);
            Assert.assertEquals(materializedView.getState(), OlapTable.OlapTableState.NORMAL);
            Assert.assertEquals(materializedView.getKeysType(), KeysType.DUP_KEYS);
            Assert.assertEquals(materializedView.getType(), Table.TableType.MATERIALIZED_VIEW); //TableTypeMATERIALIZED_VIEW
            Assert.assertEquals(materializedView.getRelatedMaterializedViews().size(), 0);
            Assert.assertEquals(materializedView.getBaseSchema().size(), 2);
            Assert.assertTrue(materializedView.isActive());
            // test sync
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);
            Assert.assertEquals(Constants.TaskRunState.SUCCESS, taskRuns.get(0).getState());
            Assert.assertEquals(((ExpressionRangePartitionInfo) partitionInfo).getIdToRange(false).size(),2);
            Assert.assertEquals(materializedView.getPartitions().size(),2);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    @Test
    public void testFullCreateMultiTables() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {}
        };
        String sql = "create materialized view mv1\n" +
                "partition by s1\n" +
                "distributed by hash(s2)\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select date_trunc('month',tb1.k1) s1, tb2.k2 s2 from tbl1 tb1 join tbl2 tb2 on tb1.k2 = tb2.k2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            Table mv1 = testDb.getTable("mv1");
            Assert.assertTrue(mv1 instanceof MaterializedView);
            // test partition
            MaterializedView materializedView = (MaterializedView) mv1;
            PartitionInfo partitionInfo = materializedView.getPartitionInfo();
            Assert.assertEquals(partitionInfo.getPartitionColumns().size(),1);
            Assert.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs().get(0);
            Assert.assertTrue(partitionExpr instanceof SlotRef);
            SlotRef partitionSlotRef = (SlotRef) partitionExpr;
            Assert.assertEquals(partitionSlotRef.getColumnName(), "s1");
            Set<Long> baseTableIds = materializedView.getBaseTableIds();
            Assert.assertEquals(baseTableIds.size(), 2);
            Expr partitionRefTableExpr = materializedView.getPartitionRefTableExprs().get(0);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionRefTableExpr.collect(SlotRef.class,slotRefs);
            SlotRef slotRef = slotRefs.get(0);
            TableName baseTableName = slotRef.getTblNameWithoutAnalyzed();
            Assert.assertEquals(baseTableName.getDb(),testDb.getFullName());
            Table baseTable = testDb.getTable(baseTableName.getTbl());
            Assert.assertTrue(baseTable != null);
            Assert.assertTrue(baseTableIds.contains(baseTable.getId()));
            Assert.assertEquals(((OlapTable) baseTable).getRelatedMaterializedViews().size(), 1);
            Column baseColumn = baseTable.getColumn(slotRef.getColumnName());
            Assert.assertTrue(baseColumn != null);
            Assert.assertEquals(baseColumn.getName(), "k1");
            // test sql
            Assert.assertEquals(materializedView.getViewDefineSql(),
                    "SELECT date_trunc('month', `tb1`.`k1`) AS `s1`, `tb2`.`k2` AS `s2` " +
                            "FROM `test`.`tbl1` AS `tb1` " +
                            "INNER JOIN `test`.`tbl2` AS `tb2` " +
                            "ON `tb1`.`k2` = `tb2`.`k2`");
            // test property
            TableProperty tableProperty = materializedView.getTableProperty();
            Assert.assertEquals(tableProperty.getReplicationNum().shortValue(), 1);
            Assert.assertEquals(materializedView.getState(), OlapTable.OlapTableState.NORMAL);
            Assert.assertEquals(materializedView.getKeysType(), KeysType.DUP_KEYS);
            Assert.assertEquals(materializedView.getType(), Table.TableType.MATERIALIZED_VIEW); //TableTypeMATERIALIZED_VIEW
            Assert.assertEquals(materializedView.getRelatedMaterializedViews().size(), 0);
            Assert.assertEquals(materializedView.getBaseSchema().size(), 2);
            Assert.assertTrue(materializedView.isActive());
            // test sync
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);
            Assert.assertEquals(Constants.TaskRunState.SUCCESS, taskRuns.get(0).getState());
            Assert.assertEquals(((ExpressionRangePartitionInfo) partitionInfo).getIdToRange(false).size(),2);
            Assert.assertEquals(materializedView.getPartitions().size(),2);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    @Test
    public void testFullCreateNoPartition() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {}
        };
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            Table mv1 = testDb.getTable("mv1");
            Assert.assertTrue(mv1 instanceof MaterializedView);
            // test partition
            MaterializedView materializedView = (MaterializedView) mv1;
            PartitionInfo partitionInfo = materializedView.getPartitionInfo();
            Assert.assertTrue(partitionInfo instanceof SinglePartitionInfo);
            Assert.assertEquals(materializedView.getPartitions().size(),1);
            Partition partition = materializedView.getPartitions().iterator().next();
            Assert.assertTrue(partition != null);
            Assert.assertEquals(partition.getName(),"mv1");
            Set<Long> baseTableIds = materializedView.getBaseTableIds();
            Assert.assertEquals(baseTableIds.size(), 1);
            Table baseTable = testDb.getTable(baseTableIds.iterator().next());
            Assert.assertEquals(((OlapTable) baseTable).getRelatedMaterializedViews().size(), 1);
            // test sql
            Assert.assertEquals(materializedView.getViewDefineSql(),
                    "SELECT `test`.`tbl1`.`k1` AS `k1`, `test`.`tbl1`.`k2` AS `k2` FROM `test`.`tbl1`");
            // test property
            TableProperty tableProperty = materializedView.getTableProperty();
            Assert.assertEquals(tableProperty.getReplicationNum().shortValue(), 1);
            Assert.assertEquals(materializedView.getState(), OlapTable.OlapTableState.NORMAL);
            Assert.assertEquals(materializedView.getKeysType(), KeysType.DUP_KEYS);
            Assert.assertEquals(materializedView.getType(), Table.TableType.MATERIALIZED_VIEW); //TableTypeMATERIALIZED_VIEW
            Assert.assertEquals(materializedView.getRelatedMaterializedViews().size(), 0);
            Assert.assertEquals(materializedView.getBaseSchema().size(), 2);
            MaterializedView.AsyncRefreshContext asyncRefreshContext =
                    materializedView.getRefreshScheme().getAsyncRefreshContext();
            Assert.assertTrue(asyncRefreshContext.getStartTime() > 0);
            Assert.assertEquals(asyncRefreshContext.getTimeUnit(), "HOUR");
            Assert.assertEquals(asyncRefreshContext.getStep(), 1);
            Assert.assertTrue(materializedView.isActive());
            // test sync
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);
            Assert.assertEquals(Constants.TaskRunState.SUCCESS, taskRuns.get(0).getState());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    // ========== partition test ==========

    @Test
    public void testPartitionByTableAlias(){
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1 tb1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionNoDataBase() {
        starRocksAssert.withoutUseDatabase();
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from test.tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "No database selected");
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionHasDataBase() {
        starRocksAssert.withoutUseDatabase();
        String sql = "create materialized view test.mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from test.tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionNoNeed() {
        String sql = "create materialized view mv1 " +
                "partition by (a+b) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Partition exp not supports:a + b");
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionWithFunctionIn() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',tbl1.k1) ss, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            Assert.assertFalse(partitionExpDesc.isFunction());
            Assert.assertTrue(partitionExpDesc.getExpr() instanceof SlotRef);
            Assert.assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "ss");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithFunction() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            Assert.assertTrue(partitionExpDesc.isFunction());
            Assert.assertTrue(partitionExpDesc.getExpr() instanceof FunctionCallExpr);
            Assert.assertEquals(partitionExpDesc.getExpr().getChild(1), partitionExpDesc.getSlotRef());
            Assert.assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "ss");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithFunctionNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',k1) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            Assert.assertTrue(partitionExpDesc.isFunction());
            Assert.assertTrue(partitionExpDesc.getExpr() instanceof FunctionCallExpr);
            Assert.assertEquals(partitionExpDesc.getExpr().getChild(1), partitionExpDesc.getSlotRef());
            Assert.assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithoutFunction() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            Assert.assertTrue(!partitionExpDesc.isFunction());
            Assert.assertTrue(partitionExpDesc.getExpr() instanceof SlotRef);
            Assert.assertEquals(partitionExpDesc.getExpr(), partitionExpDesc.getSlotRef());
            Assert.assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithFunctionIncludeFunction() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',date_trunc('month',ss)) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Partition exp not supports:date_trunc('month', date_trunc('month', ss))");
        }
    }

    @Test
    public void testPartitionWithFunctionIncludeFunctionInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view partition function date_trunc must related with column");
        }
    }

    @Test
    public void testPartitionColumnNoBaseTablePartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s2 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view partition column in partition exp must be base table partition column");
        }
    }

    @Test
    public void testPartitionColumnBaseTableHasMultiPartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s2 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl4;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view related base table partition columns only supports single column");
        }
    }

    @Test
    public void testBaseTableNoPartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl3;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view partition column in partition exp must be base table partition column");
        }
    }

    @Test
    public void testPartitionByColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            CreateMaterializedViewStatement statementBase =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Set<Long> baseTableIds = statementBase.getBaseTableIds();
            Assert.assertEquals(baseTableIds.size(), 1);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumnNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumnMixAlias1() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1, tbl1.k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumnMixAlias2() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumnNotInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by s8 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k2,sqrt(tbl1.k1) s1 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Materialized view partition exp column:s8 is not found in query statement");
        }
    }

    @Test
    public void testPartitionByFunctionNotInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',s8) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view partition exp column:s8 is not found in query statement");
        }
    }

    @Test
    public void testPartitionByFunctionColumnNoExists() {
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',tb2.k1)\n" +
                "distributed by hash(s2)\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, tb2.k2 s2 from tbl1 tb1 join tbl2 tb2 on tb1.k2 = tb2.k2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view partition exp: `tb2`.`k1` must related to column");
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoNeedParams() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc(tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "No matching function with signature: date_trunc(date).");
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoCorrParams() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('%y%m',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "date_trunc function can't support argument other than year|quarter|month|week|day");
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoCorrParams1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',k2) ss, k2 from tbl2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view partition function date_trunc check failed");
        }
    }

    @Test
    public void testPartitionByNoAllowedFunction() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k2, sqrt(tbl1.k1) ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Materialized view partition function sqrt is not support");
        }
    }

    @Test
    public void testPartitionByNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',k1) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Partition exp date_trunc('month', k1) must be alias of select item");
        }
    }

    // ========== distributed test  ==========
    @Test
    public void testDistributeKeyIsNotKey() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";

        try {
            FeConstants.shortkey_max_column_count = 1;
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            FeConstants.shortkey_max_column_count = 3;
        }
    }

    @Test
    public void testDistributeByIsNull1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Materialized view should contain distribution desc");
        }
    }

    @Test
    public void testDistributeByIsNull2() {
        connectContext.getSessionVariable().setAllowDefaultPartition(true);
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            connectContext.getSessionVariable().setAllowDefaultPartition(false);
        }
    }

    // ========== refresh test ==========
    @Test
    public void testRefreshAsyncOnlyEvery() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async EVERY(INTERVAL 2 MINUTE)" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;
            RefreshSchemeDesc refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            Assert.assertEquals(refreshSchemeDesc.getType(), RefreshType.ASYNC);
            Assert.assertTrue(asyncRefreshSchemeDesc.getStartTime() != null);
            Assert.assertEquals(((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getValue(), 2);
            Assert.assertEquals(asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription(), "MINUTE");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshAsyncStartBeforeCurr() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2016-12-31') EVERY(INTERVAL 1 HOUR)" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Refresh start must be after current time");
        }
    }

    @Test
    public void testRefreshManual() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh manual " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;
            RefreshSchemeDesc refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
            Assert.assertEquals(refreshSchemeDesc.getType(), RefreshType.MANUAL);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshSync() {
        String sql = "create materialized view mv1 " +
                "refresh sync " +
                "as select k2 from tbl1 group by k2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assert.assertTrue(statementBase instanceof CreateMaterializedViewStmt);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshSyncHasPartition() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "refresh sync " +
                "as select tbl1.k1 ss, k2 from tbl1 group by k2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Partition by is not supported by SYNC refresh type int materialized view");
        }
    }

    @Test
    public void testRefreshSyncHasDistribution() {
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) " +
                "refresh sync " +
                "as select tbl1.k1 ss, k2 from tbl1 group by k2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Distribution by is not supported by SYNC refresh type in materialized view");
        }
    }

    @Test
    public void testNoRefresh() {
        String sql = "create materialized view mv1 " +
                "as select tbl1.k1 ss, k2 from tbl1 group by k1, k2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assert.assertTrue(statementBase instanceof CreateMaterializedViewStmt);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNoRefreshNoSelectStmt() {
        String sql = "create materialized view mv1 " +
                "as select t1.k1 ss, t1.k2 from tbl1 t1 union select k1, k2 from tbl1 group by tbl1.k1, tbl1.k2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Materialized view query statement only support select");
        }
    }

    // ========== as test ==========
    @Test
    public void testAsNoSelectRelation() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select t1.k1 ss, t1.k2 from tbl1 t1 union select * from tbl2 t2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Materialized view query statement only support select");
        }
    }

    @Test
    public void testAsTableNotInOneDatabase() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select t1.k1 ss, t1.k2 from test2.tbl3 t1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Materialized view do not support table: tbl3 " +
                    "do not exist in database: default_cluster:test",e.getMessage());
        }
    }

    @Test
    public void testAsTableNoOlapTable() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select tbl1.k1 ss, tbl1.k2 from mysql_external_table tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Materialized view only supports olap table, " +
                    "but the type of table: mysql_external_table is: MYSQL", e.getMessage());
        }
    }

    @Test
    public void testAsHasStar() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(ss) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 ss, *  from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Select * is not supported in materialized view");
        }
    }

    @Test
    public void testAsSelectItemNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',tbl1.k1), k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view query statement select item date_trunc('month', `tbl1`.`k1`) must has an alias");
        }
    }

    @Test
    public void testAsSelectItemHasNonDeterministicFunction1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select rand() s1, date_trunc('month',tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view query statement select item rand() not supported nondeterministic function");
        }
    }

    @Test
    public void testAsSelectItemHasNonDeterministicFunction2() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select k2, rand()+rand() s1, date_trunc('month',tbl1.k1) ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Materialized view query statement select item rand() not supported nondeterministic function");
        }
    }

    // ========== other test ==========
    @Test
    public void testDisabled() {
        Config.enable_materialized_view = false;
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "The experimental mv is disabled");
        } finally {
            Config.enable_materialized_view = true;
        }
    }

    @Test
    public void testExists() {
        String sql = "create materialized view tbl1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Table 'tbl1' already exists");
        }
    }

    @Test
    public void testIfNotExists() {
        String sql = "create materialized view if not exists mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSupportedProperties() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_time\" = \"2122-12-31 23:59:59\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testUnSupportedProperties() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"short_key\" = \"20\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Unknown properties: {short_key=20}");
        }
    }

    @Test
    public void testNoDuplicateKey() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";

        try {
            FeConstants.shortkey_max_column_count = 0;
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Data type of first column cannot be DATE");
        } finally {
            FeConstants.shortkey_max_column_count = 3;
        }
    }

    @Test
    public void testCreateMvWithHll() {
        String sql = "CREATE MATERIALIZED VIEW mv_function\n" +
                "AS SELECT k1,MAX(v2),MIN(v3),SUM(v4),HLL_UNION(v5),BITMAP_UNION(v6),PERCENTILE_UNION(v7)\n" +
                "FROM test.aggregate_table_with_null GROUP BY k1\n" +
                "ORDER BY k1 DESC";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMvBaseOnView() {
        String sql = "CREATE MATERIALIZED VIEW mv1\n" +
                "AS SELECT k1,v2 FROM test.v1";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Do not support alter non-OLAP table[v1]");
        }
    }

    @Test
    public void testAggregateTableWithCount() {
        String sql = "CREATE MATERIALIZED VIEW v0 AS SELECT t0_57.c_0_1," +
                " COUNT(t0_57.c_0_0) , MAX(t0_57.c_0_2) , MAX(t0_57.c_0_3) , MIN(t0_57.c_0_4)" +
                " FROM tbl_for_count AS t0_57 GROUP BY t0_57.c_0_1 ORDER BY t0_57.c_0_1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Aggregate type table do not support count function in materialized view");
        }
    }

    @Test
    public void testNoExistDb() {
        String sql = "create materialized view db1.mv1\n" +
                "partition by s1\n" +
                "distributed by hash(s2)\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Can not find database:default_cluster:db1");
        }
    }
}

