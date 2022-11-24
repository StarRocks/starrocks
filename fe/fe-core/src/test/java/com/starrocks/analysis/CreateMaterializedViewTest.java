// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.RefreshType;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CreateMaterializedViewTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

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
                        "                        `k1` date,\n" +
                        "                        `v2` datetime MAX,\n" +
                        "                        `v3` char(20) MIN,\n" +
                        "                        `v4` tinyint SUM,\n" +
                        "                        `v8` tinyint SUM,\n" +
                        "                        `v5` HLL HLL_UNION,\n" +
                        "                        `v6` BITMAP BITMAP_UNION,\n" +
                        "                        `v7` PERCENTILE PERCENTILE_UNION\n" +
                        "                    ) ENGINE=OLAP\n" +
                        "                    AGGREGATE KEY(`k1`)\n" +
                        "                    COMMENT \"OLAP\"\n" +
                        "                    DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                        "                    PROPERTIES (\n" +
                        "                        \"replication_num\" = \"1\",\n" +
                        "                        \"storage_format\" = \"v2\"\n" +
                        "                    );")
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
                .useDatabase("test");
<<<<<<< HEAD
=======
        starRocksAssert.withView("create view test.view_to_tbl1 as select * from test.tbl1;");
        currentState = GlobalStateMgr.getCurrentState();
        testDb = currentState.getDb("test");
    }

    private void dropMv(String mvName) throws Exception {
        String sql = "drop materialized view " + mvName;
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statementBase);
        stmtExecutor.execute();
    }

    private void dropTableForce(String tableName) throws Exception {
        String sql = "drop table " + tableName +" force";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statementBase);
        stmtExecutor.execute();
    }

    private void dropTable(String tableName) throws Exception {
        String sql = "drop table " + tableName;
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statementBase);
        stmtExecutor.execute();
    }

    private List<TaskRunStatus> waitingTaskFinish() {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);
        int retryCount = 0, maxRetry = 5;
        while (retryCount < maxRetry) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            Constants.TaskRunState state = taskRuns.get(0).getState();
            if (state == Constants.TaskRunState.FAILED || state == Constants.TaskRunState.SUCCESS) {
                break;
            }
            retryCount++;
            LOG.info("waiting for TaskRunState retryCount:" + retryCount);
        }
        return taskRuns;
    }

    private void waitingRollupJobV2Finish() throws Exception{
        // waiting alterJobV2 finish
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        //Assert.assertEquals(1, alterJobs.size());

        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            if (alterJobV2.getType() != AlterJobV2.JobType.ROLLUP) {
                continue;
            }
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "rollup job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                ThreadUtil.sleepAtLeastIgnoreInterrupts(1000L);
            }
        }
    }

    // ========== full test ==========

    @Test
    public void testFullCreate() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 SECOND)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            ThreadUtil.sleepAtLeastIgnoreInterrupts(4000L);
            Table mv1 = testDb.getTable("mv1");
            Assert.assertTrue(mv1 instanceof MaterializedView);
            // test partition
            MaterializedView materializedView = (MaterializedView) mv1;
            PartitionInfo partitionInfo = materializedView.getPartitionInfo();
            Assert.assertEquals(1, partitionInfo.getPartitionColumns().size());
            Assert.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs().get(0);
            Assert.assertTrue(partitionExpr instanceof FunctionCallExpr);
            FunctionCallExpr partitionFunctionCallExpr = (FunctionCallExpr) partitionExpr;
            Assert.assertEquals("date_trunc", partitionFunctionCallExpr.getFnName().getFunction());
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionFunctionCallExpr.collect(SlotRef.class, slotRefs);
            SlotRef partitionSlotRef = slotRefs.get(0);
            Assert.assertEquals("k1", partitionSlotRef.getColumnName());
            List<MaterializedView.BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
            Assert.assertEquals(1, baseTableInfos.size());
            Expr partitionRefTableExpr = materializedView.getPartitionRefTableExprs().get(0);
            List<SlotRef> tableSlotRefs = Lists.newArrayList();
            partitionRefTableExpr.collect(SlotRef.class, tableSlotRefs);
            SlotRef slotRef = tableSlotRefs.get(0);
            TableName baseTableName = slotRef.getTblNameWithoutAnalyzed();
            Assert.assertEquals(baseTableName.getDb(), testDb.getFullName());
            Table baseTable = testDb.getTable(baseTableName.getTbl());
            Assert.assertNotNull(baseTable);
            Assert.assertEquals(baseTableInfos.get(0).getTableId(), baseTable.getId());
            Assert.assertEquals(1, baseTable.getRelatedMaterializedViews().size());
            Column baseColumn = baseTable.getColumn(slotRef.getColumnName());
            Assert.assertNotNull(baseColumn);
            Assert.assertEquals("k1", baseColumn.getName());
            // test sql
            Assert.assertEquals("SELECT `test`.`tb1`.`k1`, `test`.`tb1`.`k2` AS `s2`\n" +
                            "FROM `test`.`tbl1` AS `tb1`",
                    materializedView.getViewDefineSql());
            // test property
            TableProperty tableProperty = materializedView.getTableProperty();
            Assert.assertEquals(1, tableProperty.getReplicationNum().shortValue());
            Assert.assertEquals(OlapTable.OlapTableState.NORMAL, materializedView.getState());
            Assert.assertEquals(KeysType.DUP_KEYS, materializedView.getKeysType());
            Assert.assertEquals(Table.TableType.MATERIALIZED_VIEW,
                    materializedView.getType()); //TableTypeMATERIALIZED_VIEW
            Assert.assertEquals(0, materializedView.getRelatedMaterializedViews().size(), 0);
            Assert.assertEquals(2, materializedView.getBaseSchema().size());
            Assert.assertTrue(materializedView.isActive());
            // test sync
            testFullCreateSync(materializedView, baseTable);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    public void testFullCreateSync(MaterializedView materializedView, Table baseTable) throws Exception {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
        List<TaskRunStatus> taskRuns = waitingTaskFinish();
        Assert.assertEquals(Constants.TaskRunState.SUCCESS, taskRuns.get(0).getState());
        Collection<Partition> baseTablePartitions = ((OlapTable) baseTable).getPartitions();
        Collection<Partition> mvPartitions = materializedView.getPartitions();
        Assert.assertEquals(2, mvPartitions.size());
        Assert.assertEquals(baseTablePartitions.size(), mvPartitions.size());

        // add partition p3
        String addPartitionSql = "ALTER TABLE test.tbl1 ADD PARTITION p3 values less than('2020-04-01');";
        new StmtExecutor(connectContext, addPartitionSql).execute();
        taskManager.executeTask(mvTaskName);
        waitingTaskFinish();
        Assert.assertEquals(3, baseTablePartitions.size());
        Assert.assertEquals(baseTablePartitions.size(), mvPartitions.size());

        // delete partition p3
        String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p3\n";
        new StmtExecutor(connectContext, dropPartitionSql).execute();
        taskManager.executeTask(mvTaskName);
        waitingTaskFinish();
        Assert.assertEquals(2, mvPartitions.size());
        Assert.assertEquals(baseTablePartitions.size(), mvPartitions.size());
    }

    @Test
    public void testCreateAsync() {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 MONTH)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        Assert.assertThrows(IllegalArgumentException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
    }

    @Test
    public void testCreateAsyncNormal() throws Exception {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testCreateAsyncLowercase() throws Exception {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 day)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testCreateAsyncWithSingleTable() throws Exception {
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2)\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement)
                UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        RefreshSchemeDesc refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
        Assert.assertEquals(MaterializedView.RefreshType.MANUAL,refreshSchemeDesc.getType());
    }

    @Test
    public void testCreateSyncWithSingleTable() throws Exception {
        String sql = "create materialized view mv1\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assert.assertTrue(statementBase instanceof CreateMaterializedViewStmt);
    }

    @Test
    public void testFullCreateMultiTables() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };
        String sql = "create materialized view mv1\n" +
                "partition by s1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 SECOND)\n" +
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
            Assert.assertEquals(1, partitionInfo.getPartitionColumns().size());
            Assert.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs().get(0);
            Assert.assertTrue(partitionExpr instanceof SlotRef);
            SlotRef partitionSlotRef = (SlotRef) partitionExpr;
            Assert.assertEquals("s1", partitionSlotRef.getColumnName());
            List<MaterializedView.BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
            Assert.assertEquals(2, baseTableInfos.size());
            Expr partitionRefTableExpr = materializedView.getPartitionRefTableExprs().get(0);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionRefTableExpr.collect(SlotRef.class, slotRefs);
            SlotRef slotRef = slotRefs.get(0);
            TableName baseTableName = slotRef.getTblNameWithoutAnalyzed();
            Assert.assertEquals(baseTableName.getDb(), testDb.getFullName());
            Table baseTable = testDb.getTable(baseTableName.getTbl());
            Assert.assertNotNull(baseTable);
            Assert.assertTrue(baseTableInfos.stream().anyMatch(baseTableInfo ->
                    baseTableInfo.getTableId() == baseTable.getId()));
            Assert.assertTrue(1 <= baseTable.getRelatedMaterializedViews().size());
            Column baseColumn = baseTable.getColumn(slotRef.getColumnName());
            Assert.assertNotNull(baseColumn);
            Assert.assertEquals("k1", baseColumn.getName());
            // test sql
            Assert.assertEquals(
                    "SELECT date_trunc('month', `test`.`tb1`.`k1`) AS `s1`, `test`.`tb2`.`k2` AS `s2`\n" +
                            "FROM `test`.`tbl1` AS `tb1` INNER JOIN `test`.`tbl2` AS `tb2` ON `test`.`tb1`.`k2` = `test`.`tb2`.`k2`",
                    materializedView.getViewDefineSql());
            // test property
            TableProperty tableProperty = materializedView.getTableProperty();
            Assert.assertEquals(1, tableProperty.getReplicationNum().shortValue(), 1);
            Assert.assertEquals(OlapTable.OlapTableState.NORMAL, materializedView.getState());
            Assert.assertEquals(KeysType.DUP_KEYS, materializedView.getKeysType());
            Assert.assertEquals(Table.TableType.MATERIALIZED_VIEW,
                    materializedView.getType()); //TableTypeMATERIALIZED_VIEW
            Assert.assertEquals(0, materializedView.getRelatedMaterializedViews().size());
            Assert.assertEquals(2, materializedView.getBaseSchema().size());
            Assert.assertTrue(materializedView.isActive());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
>>>>>>> 1427b8a4b ([BugFix] Fix column name resolved ignore resolve db name (#13504))
    }

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
            assertEquals(e.getMessage(), "The experimental mv is disabled");
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
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Table 'tbl1' already exists");
        }
    }

    @Test
    public void testIfNotExists() {
        String sql = "create materialized view if not exists tbl1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionNoDataBase() {
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
            assertEquals(e.getMessage(), "No database selected");
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testFullDescPartitionHasDataBase() {
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
            assertEquals(e.getMessage(),
                    "Partition exp not supports:a + b");
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testCreateMVWithExplainQuery() {
        String sql = "create materialized view mv1 " +
                "as explain select k1, v2 from aggregate_table_with_null;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Materialized view does not support explain query", e.getMessage());
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
            assertTrue(partitionExpDesc.isFunction());
            assertTrue(partitionExpDesc.getExpr() instanceof FunctionCallExpr);
            assertEquals(partitionExpDesc.getExpr().getChild(1), partitionExpDesc.getSlotRef());
            assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
            assertEquals(partitionExpDesc.getSlotRef().getTblNameWithoutAnalyzed().getTbl(), "tbl1");
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
            assertTrue(partitionExpDesc.isFunction());
            assertTrue(partitionExpDesc.getExpr() instanceof FunctionCallExpr);
            assertEquals(partitionExpDesc.getExpr().getChild(1), partitionExpDesc.getSlotRef());
            assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
            assertEquals(partitionExpDesc.getSlotRef().getTblNameWithoutAnalyzed().getTbl(), "tbl1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
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
            assertTrue(partitionExpDesc.isFunction());
            assertTrue(partitionExpDesc.getExpr() instanceof FunctionCallExpr);
            assertEquals(partitionExpDesc.getExpr().getChild(1), partitionExpDesc.getSlotRef());
            assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
            assertEquals(partitionExpDesc.getSlotRef().getTblNameWithoutAnalyzed().getTbl(), "tbl1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
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
            assertTrue(!partitionExpDesc.isFunction());
            assertTrue(partitionExpDesc.getExpr() instanceof SlotRef);
            assertEquals(partitionExpDesc.getExpr(), partitionExpDesc.getSlotRef());
            assertEquals(partitionExpDesc.getSlotRef().getColumnName(), "k1");
            assertEquals(partitionExpDesc.getSlotRef().getTblNameWithoutAnalyzed().getTbl(), "tbl1");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
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
            assertEquals(e.getMessage(),
                    "Partition exp not supports:date_trunc('month', date_trunc('month', ss))");
        } finally {
            starRocksAssert.useDatabase("test");
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
            assertEquals(e.getMessage(),
                    "Materialized view partition function date_trunc must related with column");
        } finally {
            starRocksAssert.useDatabase("test");
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
            assertEquals(e.getMessage(),
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
            assertEquals(e.getMessage(),
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
            assertEquals(e.getMessage(),
                    "Materialized view partition column in partition exp must be base table partition column");
        }
    }

    @Test
    public void testFullDescPartitionByColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionByColumnNoAlias() {
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
    public void testFullDescPartitionByColumnMixAlias1() {
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
    public void testFullDescPartitionByColumnMixAlias2() {
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
    public void testNoPartitionExp() {
        String sql = "create materialized view mv1 " +
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
    public void testSelectHasStar() {
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
            assertEquals(e.getMessage(), "Select * is not supported in materialized view");
        }
    }

    @Test
    public void testPartitionByFunctionNotInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by s8 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select sqrt(tbl1.k1) s1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view partition exp column is not found in query statement");
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
            assertEquals(e.getMessage(), "No matching function with signature: date_trunc(date).");
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
            assertEquals(e.getMessage(),
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
            assertEquals(e.getMessage(),
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
                "as select sqrt(tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view partition function sqrt is not support");
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
            assertEquals(e.getMessage(), "Partition exp date_trunc('month', k1) must be alias of select item");
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
            assertEquals(e.getMessage(), "Materialized view should contain distribution desc");
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
            assertEquals(refreshSchemeDesc.getType(), RefreshType.ASYNC);
            assertNotNull(asyncRefreshSchemeDesc.getStartTime());
            assertEquals(((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getValue(), 2);
            assertEquals(asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription(), "MINUTE");
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
            assertEquals(e.getMessage(), "Refresh start must be after current time");
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
            assertEquals(refreshSchemeDesc.getType(), RefreshType.MANUAL);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshSync() throws Exception {
        String sql = "create materialized view mv1 " +
                "refresh sync " +
                "as select k2 from tbl1 group by k2;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
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
            assertEquals(e.getMessage(), "Partition by is not supported by SYNC refresh type int materialized view");
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
            assertEquals(e.getMessage(), "Distribution by is not supported by SYNC refresh type in materialized view");
        }
    }

    @Test
    public void testNoRefresh() {
        String sql = "create materialized view mv1 " +
                "as select tbl1.k1 ss, k2 from tbl1 group by k1, k2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            assertTrue(statementBase instanceof CreateMaterializedViewStmt);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNoRefreshNoSelectStmt() {
        String sql = "create materialized view mv1 " +
                "as select t1.k1 ss, t1.k2 from tbl1 t1 union select k1, k2 from tbl1 group by tbl1.k1, tbl1.k2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            assertTrue(statementBase instanceof CreateMaterializedViewStmt);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view query statement only support select");
        }
    }

    @Test
    public void testQueryStatementNoSelectRelation() {
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
            assertEquals(e.getMessage(), "Materialized view query statement only support select");
        }
    }

    @Test
    public void testTableNotInOneDatabase() {
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
            assertEquals(e.getMessage(),
                    "Materialized view do not support table which is in other database:default_cluster:test2");
        }
    }

    @Test
    public void testTableNoOlapTable() {
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
            assertEquals(e.getMessage(), "Materialized view only support olap table:mysql_external_table type:MYSQL");
        }

    }

    @Test
    public void testSelectItemNoAlias() {
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
            assertEquals(e.getMessage(),
                    "Materialized view query statement select item date_trunc('month', `tbl1`.`k1`) must has an alias");
        }
    }

    @Test
    public void testSelectItemHasNonDeterministicFunction1() {
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
            assertEquals(e.getMessage(),
                    "Materialized view query statement select item rand() not supported nondeterministic function");
        }
    }

    @Test
    public void testSelectItemHasNonDeterministicFunction2() {
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
            assertEquals(e.getMessage(),
                    "Materialized view query statement select item rand() not supported nondeterministic function");
        }
    }

    @Test
    public void testCreateMVWithHll() throws Exception {
        String sql = "CREATE MATERIALIZED VIEW mv_function\n" +
                "                             AS SELECT\n" +
                "                               k1,\n" +
                "                               MAX(v2),\n" +
                "                               MIN(v3),\n" +
                "                               SUM(v4),\n" +
                "                               HLL_UNION(v5),\n" +
                "                               BITMAP_UNION(v6),\n" +
                "                               PERCENTILE_UNION(v7)\n" +
                "                             FROM test.aggregate_table_with_null GROUP BY k1\n" +
                "                             ORDER BY k1 DESC";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testCreateMVBaseOnView() throws Exception {
        String sql = "CREATE MATERIALIZED VIEW mv1\n" +
                "                                                  AS\n" +
                "                                                  SELECT\n" +
                "                                                    k1,\n" +
                "                                                    v2\n" +
                "                                                  FROM test.v1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Do not support alter non-OLAP table[v1]");
        }
    }

    @Test
    public void createViewBadName() {
        String longLongName = "view___123456789012345678901234567890123456789012345678901234567890";
        String sql = "create view db1." + longLongName + " as select 1,2,3";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assert.fail(); // should raise Exception
        } catch (Exception e) {
            Assert.assertEquals("Incorrect table name '" + longLongName + "'", e.getMessage());
        }
    }

    @Test
    public void testCreateSyncMvFromSubquery() {
        String sql = "create materialized view sync_mv_1 as" +
                " select k1, sum(k2) from (select k1, k2 from tbl1 group by k1, k2) a group by k1";
        try {
            starRocksAssert.withMaterializedView(sql);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Materialized view query statement only support direct query from table"));
        }
    }
}

