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
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class CreateMaterializedViewTest {

    private static final Logger LOG = LogManager.getLogger(CreateMaterializedViewTest.class);

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
                        "    PARTITION p1 values [('2020-01-01'),('2020-02-01')),\n" +
                        "    PARTITION p2 values [('2020-02-01'),('2020-03-01'))\n" +
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
                .withTable("CREATE TABLE test.tbl5\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    k3 int,\n" +
                        "    v1 int,\n" +
                        "    v2 int\n" +
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
        testDb = currentState.getDb("test");
    }

    private void dropMv(String mvName) throws Exception {
        String sql = "drop materialized view " + mvName;
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
                "distributed by hash(s2)\n" +
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
            Set<Long> baseTableIds = materializedView.getBaseTableIds();
            Assert.assertEquals(1, baseTableIds.size());
            Expr partitionRefTableExpr = materializedView.getPartitionRefTableExprs().get(0);
            List<SlotRef> tableSlotRefs = Lists.newArrayList();
            partitionRefTableExpr.collect(SlotRef.class, tableSlotRefs);
            SlotRef slotRef = tableSlotRefs.get(0);
            TableName baseTableName = slotRef.getTblNameWithoutAnalyzed();
            Assert.assertEquals(baseTableName.getDb(), testDb.getFullName());
            Table baseTable = testDb.getTable(baseTableName.getTbl());
            Assert.assertNotNull(baseTable);
            Assert.assertTrue(baseTableIds.contains(baseTable.getId()));
            Assert.assertEquals(1, ((OlapTable) baseTable).getRelatedMaterializedViews().size());
            Column baseColumn = baseTable.getColumn(slotRef.getColumnName());
            Assert.assertNotNull(baseColumn);
            Assert.assertEquals("k1", baseColumn.getName());
            // test sql
            Assert.assertEquals("SELECT `test`.`tb1`.`k1` AS `k1`, `test`.`tb1`.`k2` AS `s2` FROM `test`.`tbl1` AS `tb1`",
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
                "distributed by hash(s2)\n" +
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
                "distributed by hash(s2)\n" +
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
                "distributed by hash(s2)\n" +
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
                "distributed by hash(s2)\n" +
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
            Set<Long> baseTableIds = materializedView.getBaseTableIds();
            Assert.assertEquals(2, baseTableIds.size());
            Expr partitionRefTableExpr = materializedView.getPartitionRefTableExprs().get(0);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionRefTableExpr.collect(SlotRef.class, slotRefs);
            SlotRef slotRef = slotRefs.get(0);
            TableName baseTableName = slotRef.getTblNameWithoutAnalyzed();
            Assert.assertEquals(baseTableName.getDb(), testDb.getFullName());
            Table baseTable = testDb.getTable(baseTableName.getTbl());
            Assert.assertNotNull(baseTable);
            Assert.assertTrue(baseTableIds.contains(baseTable.getId()));
            Assert.assertEquals(1, ((OlapTable) baseTable).getRelatedMaterializedViews().size());
            Column baseColumn = baseTable.getColumn(slotRef.getColumnName());
            Assert.assertNotNull(baseColumn);
            Assert.assertEquals("k1", baseColumn.getName());
            // test sql
            Assert.assertEquals(
                    "SELECT date_trunc('month', `test`.`tb1`.`k1`) AS `s1`, `test`.`tb2`.`k2` AS `s2` FROM `test`.`tbl1` AS `tb1` INNER JOIN `test`.`tbl2` AS `tb2` ON `test`.`tb1`.`k2` = `test`.`tb2`.`k2`",
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
    }

    @Test
    public void testFullCreateNoPartition() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) " +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 SECOND) " +
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
            Assert.assertEquals(1, materializedView.getPartitions().size());
            Partition partition = materializedView.getPartitions().iterator().next();
            Assert.assertNotNull(partition);
            Assert.assertEquals("mv1", partition.getName());
            Set<Long> baseTableIds = materializedView.getBaseTableIds();
            Assert.assertEquals(1, baseTableIds.size());
            Table baseTable = testDb.getTable(baseTableIds.iterator().next());
            Assert.assertEquals(1, ((OlapTable) baseTable).getRelatedMaterializedViews().size());
            // test sql
            Assert.assertEquals("SELECT `test`.`tbl1`.`k1` AS `k1`, `test`.`tbl1`.`k2` " +
                    "AS `k2` FROM `test`.`tbl1`", materializedView.getViewDefineSql());
            // test property
            TableProperty tableProperty = materializedView.getTableProperty();
            Assert.assertEquals(1, tableProperty.getReplicationNum().shortValue());
            Assert.assertEquals(OlapTable.OlapTableState.NORMAL, materializedView.getState());
            Assert.assertEquals(KeysType.DUP_KEYS, materializedView.getKeysType());
            Assert.assertEquals(Table.TableType.MATERIALIZED_VIEW,
                    materializedView.getType()); //TableTypeMATERIALIZED_VIEW
            Assert.assertEquals(0, materializedView.getRelatedMaterializedViews().size());
            Assert.assertEquals(2, materializedView.getBaseSchema().size());
            MaterializedView.AsyncRefreshContext asyncRefreshContext =
                    materializedView.getRefreshScheme().getAsyncRefreshContext();
            Assert.assertTrue(asyncRefreshContext.getStartTime() > 0);
            Assert.assertEquals("SECOND", asyncRefreshContext.getTimeUnit());
            Assert.assertEquals(3, asyncRefreshContext.getStep());
            Assert.assertTrue(materializedView.isActive());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    @Test
    public void testPartitionByTableAlias() {
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
            Assert.assertEquals("No database selected", e.getMessage());
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
            Assert.assertEquals("Partition exp not supports:a + b", e.getMessage());
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
            Assert.assertFalse(partitionExpDesc.isFunction());
            Assert.assertTrue(partitionExpDesc.getExpr() instanceof SlotRef);
            Assert.assertEquals("ss", partitionExpDesc.getSlotRef().getColumnName());
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
            Assert.assertEquals("ss", partitionExpDesc.getSlotRef().getColumnName());
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
            Assert.assertEquals("k1", partitionExpDesc.getSlotRef().getColumnName());
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
            Assert.assertEquals("k1", partitionExpDesc.getSlotRef().getColumnName());
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
            Assert.assertEquals("Partition exp not supports:date_trunc" +
                    "('month', date_trunc('month', ss))", e.getMessage());
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
            Assert.assertEquals("Materialized view partition function date_trunc " +
                    "must related with column", e.getMessage());
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
            Assert.assertEquals("Materialized view partition column in partition exp " +
                    "must be base table partition column", e.getMessage());
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
            Assert.assertEquals("Materialized view related base table partition columns " +
                    "only supports single column", e.getMessage());
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
            Assert.assertEquals("Materialized view partition column in partition exp " +
                    "must be base table partition column", e.getMessage());
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
            Assert.assertEquals(1, baseTableIds.size());
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
            Assert.assertEquals("Materialized view partition exp column:s8 is " +
                    "not found in query statement", e.getMessage());
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
            Assert.assertEquals("Materialized view partition exp column:s8 is " +
                    "not found in query statement", e.getMessage());
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
            Assert.assertEquals("Materialized view partition exp: `tb2`.`k1` must related to column", e.getMessage());
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
            Assert.assertEquals("No matching function with signature: date_trunc(date).", e.getMessage());
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
            Assert.assertEquals("date_trunc function can't support argument other than " +
                    "year|quarter|month|week|day", e.getMessage());
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
            Assert.assertEquals("Materialized view partition function date_trunc check failed", e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionUseWeek() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('week',k2) ss, k2 from tbl2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(
                    "The function date_trunc used by the materialized view for partition does not support week formatting",
                    e.getMessage());
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
            Assert.assertEquals("Materialized view partition function sqrt is not support", e.getMessage());
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
            Assert.assertEquals("Partition exp date_trunc('month', k1) must be alias of select item", e.getMessage());
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
            Assert.assertEquals("Materialized view should contain distribution desc", e.getMessage());
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
            Assert.assertEquals(MaterializedView.RefreshType.ASYNC, refreshSchemeDesc.getType());
            Assert.assertNotNull(asyncRefreshSchemeDesc.getStartTime());
            Assert.assertEquals(2, ((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getValue());
            Assert.assertEquals("MINUTE",
                    asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription());
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
            Assert.assertEquals("Refresh start must be after current time", e.getMessage());
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
            Assert.assertEquals(MaterializedView.RefreshType.MANUAL, refreshSchemeDesc.getType());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
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
            Assert.assertEquals("Materialized view query statement only support select", e.getMessage());
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
            Assert.assertEquals("Materialized view query statement only support select", e.getMessage());
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
                    "do not exist in database: test", e.getMessage());
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
    public void testAsTableOnMV() {
        String sql1 = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        String sql2 = "create materialized view mv2 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from mv1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.assertEquals("Creating a materialized view from materialized view is not supported now." +
                    " The type of table: mv1 is: Materialized View", e.getMessage());
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
            Assert.assertEquals("Select * is not supported in materialized view", e.getMessage());
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
            Assert.assertEquals("Materialized view query statement select item " +
                    "date_trunc('month', `tbl1`.`k1`) must has an alias", e.getMessage());
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
                    "Materialized view query statement select item rand() " +
                            "not supported nondeterministic function", e.getMessage());
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
            Assert.assertEquals("Materialized view query statement select item rand() " +
                    "not supported nondeterministic function", e.getMessage());
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
            Assert.assertEquals("The experimental mv is disabled", e.getMessage());
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
            Assert.assertEquals("Table 'tbl1' already exists", e.getMessage());
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
            Assert.assertEquals("Unknown properties: {short_key=20}", e.getMessage());
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
            Assert.assertEquals("Data type of first column cannot be DATE", e.getMessage());
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
            Assert.assertEquals("Do not support alter non-OLAP table[v1]", e.getMessage());
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
            Assert.assertEquals("Aggregate type table do not support count function in materialized view",
                    e.getMessage());
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
            Assert.assertEquals("Can not find database:db1", e.getMessage());
        }
    }

    @Test
    public void testMvNameInvalid() {
        String sql = "create materialized view mvklajksdjksjkjfksdlkfgkllksdjkgjsdjfjklsdjkfgjkldfkljgljkljklgja\n" +
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
            Assert.assertEquals("Incorrect table name " +
                    "'mvklajksdjksjkjfksdlkfgkllksdjkgjsdjfjklsdjkfgjkldfkljgljkljklgja'", e.getMessage());
        }
    }

    @Test
    public void testMvNameTooLong() {
        String sql = "create materialized view 22mv\n" +
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
            Assert.assertEquals("Incorrect table name '22mv'", e.getMessage());
        }
    }

    @Test
    public void testPartitionAndDistributionByColumnNameIgnoreCase() {
        String sql = "create materialized view mv1 " +
                "partition by K1 " +
                "distributed by hash(K2) " +
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
    public void testDuplicateColumn() {
        String sql = "create materialized view mv1 " +
                "partition by K1 " +
                "distributed by hash(K2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, K1 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Duplicate column name 'K1'", e.getMessage());
        }
    }

    @Test
    public void testNoBaseTable() {
        String sql = "create materialized view mv1 " +
                "partition by K1 " +
                "distributed by hash(K2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select 1 as k1, 2 as k2";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Can not find base table in query statement", e.getMessage());
        }
    }

    @Test
    public void testUseCte() {
        String sql = "create materialized view mv1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH ASYNC\n" +
                "AS with tbl as\n" +
                "(select * from tbl1)\n" +
                "SELECT k1,k2\n" +
                "FROM tbl;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Materialized view query statement not support cte", e.getMessage());
        }
    }

    @Test
    public void testUseSubQuery() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from (select * from tbl1) tbl";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Materialized view query statement not support subquery", e.getMessage());
        }
    }

    @Test
    public void testPartitionByNotFirstColumn() throws Exception {
        starRocksAssert.withNewMaterializedView("create materialized view mv_with_partition_by_not_first_column" +
                " partition by k1" +
                " distributed by hash(k3) buckets 10" +
                " as select k3, k1, sum(v1) as total from tbl5 group by k3, k1");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table = db.getTable("mv_with_partition_by_not_first_column");
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assert.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        List<Expr> partitionExpr = expressionRangePartitionInfo.getPartitionExprs();
        Assert.assertEquals(1, partitionExpr.size());
        Assert.assertTrue(partitionExpr.get(0) instanceof SlotRef);
        SlotRef slotRef = (SlotRef) partitionExpr.get(0);
        Assert.assertNotNull(slotRef.getSlotDescriptorWithoutCheck());
        SlotDescriptor slotDescriptor = slotRef.getSlotDescriptorWithoutCheck();
        Assert.assertEquals(1, slotDescriptor.getId().asInt());
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

    @Test
    public void testCreateAsyncMv() {
        Config.enable_experimental_mv = true;
        String sql = "create materialized view async_mv_1 distributed by hash(c_1_9) as" +
                " select c_1_9, c_1_4 from t1";
        try {
            starRocksAssert.withNewMaterializedView(sql);
            MaterializedView mv = (MaterializedView) testDb.getTable("async_mv_1");
            Assert.assertTrue(mv.getFullSchema().get(0).isKey());
            Assert.assertFalse(mv.getFullSchema().get(1).isKey());
        } catch (Exception e) {
            Assert.assertTrue(false);
        }

        String sql2 = "create materialized view async_mv_1 distributed by hash(c_1_4) as" +
                " select c_1_4 from t1";
        try {
            starRocksAssert.withNewMaterializedView(sql2);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Data type of first column cannot be DOUBLE"));
        }
    }
}

