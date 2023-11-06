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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.MVTestUtils.waitingRollupJobV2Finish;

public class CreateMaterializedViewTest {
    private static final Logger LOG = LogManager.getLogger(CreateMaterializedViewTest.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TestName name = new TestName();

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database testDb;
    private static GlobalStateMgr currentState;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        Config.alter_scheduler_interval_millisecond = 100;
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
                .withTable("CREATE TABLE test.TBL1 \n" +
                        "(\n" +
                        "    K1 date,\n" +
                        "    K2 int,\n" +
                        "    V1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(K1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2020-01-01'),('2020-02-01')),\n" +
                        "    PARTITION p2 values [('2020-02-01'),('2020-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(K2) BUCKETS 3\n" +
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
                        "\"replication_num\" = \"1\"\n" +
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
                        "\"in_memory\" = \"false\"\n" +
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
                .withTable("CREATE TABLE test.mocked_cloud_table\n" +
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
                .useDatabase("test");
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
            List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
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
        Collection<Partition> baseTablePartitions = baseTable.getPartitions();
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
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
    }

    @Test
    public void testCreateAsyncMVWithDuplicatedProperty() {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));

        String sql2 = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql2, connectContext));

        String sql3 = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql3, connectContext));
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

        sql = "create materialized view mv1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "partition by date_trunc('month',k1)\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        sql = "create materialized view mv1\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "partition by date_trunc('month',k1)\n" +
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
        Assert.assertEquals(MaterializedView.RefreshType.MANUAL, refreshSchemeDesc.getType());
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
            List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
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
    }

    @Test
    public void testFullCreateNoPartition() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) buckets 10 " +
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
            List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
            Assert.assertEquals(1, baseTableInfos.size());
            Table baseTable = testDb.getTable(baseTableInfos.iterator().next().getTableId());
            Assert.assertTrue(1 <= baseTable.getRelatedMaterializedViews().size());
            // test sql
            Assert.assertEquals("SELECT `test`.`tbl1`.`k1`, `test`.`tbl1`.`k2`\nFROM `test`.`tbl1`",
                    materializedView.getViewDefineSql());
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
    public void testCreateWithoutBuckets() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2)" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 SECOND) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    @Test
    public void testPartitionByTableAlias() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from test.tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("No database selected"));
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionHasDataBase() {
        starRocksAssert.withoutUseDatabase();
        String sql = "create materialized view test.mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(),
                    e.getMessage().contains("Unsupported expr 'a + b' in PARTITION BY clause"));
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
            Assert.assertEquals("Creating materialized view does not support explain query", e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionWithFunctionIn() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
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
    public void testPartitionWithFunctionInUseStr2Date() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(a) buckets 10 " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select str2date(d,'%Y%m%d') ss, a, b, c from jdbc0.partitioned_db0.tbl1;";
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
    public void testPartitionWithFunctionInUseStr2DateForError() {
        String sql = "create materialized view mv_error " +
                "partition by ss " +
                "distributed by hash(a) buckets 10 " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select str2date(d,'%Y%m%d') ss, a, b, c from jdbc0.partitioned_db0.tbl0;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Materialized view partition function str2date check failed"));
        }
    }

    @Test
    public void testPartitionWithFunction() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',ss) " +
                "distributed by hash(k2) buckets 10 " +
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
    public void testPartitionWithFunctionUseStr2Date() throws Exception {
        // basic
        {
            String sql = "create materialized view mv1 " +
                    "partition by str2date(d,'%Y%m%d') " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select a, b, c, d from jdbc0.partitioned_db0.tbl1;";
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            Assert.assertTrue(partitionExpDesc.isFunction());
            Assert.assertTrue(partitionExpDesc.getExpr() instanceof FunctionCallExpr);
            Assert.assertEquals(partitionExpDesc.getExpr().getChild(0), partitionExpDesc.getSlotRef());
            Assert.assertEquals("d", partitionExpDesc.getSlotRef().getColumnName());
        }

        // slot
        {
            String sql = "create materialized view mv_str2date " +
                    "partition by p " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select str2date(d,'%Y%m%d') as p,  a, b, c, d from jdbc0.partitioned_db0.tbl1;";
            starRocksAssert.withMaterializedView(sql);
        }

        // rollup
        {
            String sql = "create materialized view mv_date_trunc_str2date " +
                    "partition by date_trunc('month', p) " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select str2date(d,'%Y%m%d') as p,  a, b, c, d from jdbc0.partitioned_db0.tbl1;";
            starRocksAssert.withMaterializedView(sql);
        }
    }

    @Test
    public void testPartitionWithFunctionNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',k1) " +
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ExpressionPartitionDesc partitionExpDesc = createMaterializedViewStatement.getPartitionExpDesc();
            Assert.assertFalse(partitionExpDesc.isFunction());
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
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unsupported expr 'date_trunc('month', " +
                    "date_trunc('month', ss))' in PARTITION BY clause"));
        }
    }

    @Test
    public void testPartitionWithFunctionIncludeFunctionInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',ss) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 1, column 42 to line 1, column 63. " +
                            "Detail message: Materialized view partition function date_trunc must related with column.",
                    e.getMessage());
        }
    }

    @Test
    public void testPartitionColumnNoBaseTablePartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s2 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Materialized view partition column " +
                    "in partition exp must be base table partition column.", e.getMessage());
        }
    }

    @Test
    public void testPartitionColumnBaseTableHasMultiPartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s2 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl4;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Materialized view related base table " +
                    "partition columns only supports single column.", e.getMessage());
        }
    }

    @Test
    public void testBaseTableNoPartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl3;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Materialized view partition column" +
                    " in partition exp must be base table partition column.", e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            CreateMaterializedViewStatement statementBase =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            List<BaseTableInfo> baseTableInfos = statementBase.getBaseTableInfos();
            Assert.assertEquals(1, baseTableInfos.size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumnNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k2,sqrt(tbl1.k1) s1 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Materialized view partition exp " +
                    "column:s8 is not found in query statement.", e.getMessage());
        }
    }

    @Test
    public void testPartitionByFunctionNotInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',s8) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Materialized view partition exp " +
                    "column:s8 is not found in query statement.", e.getMessage());
        }
    }

    @Test
    public void testPartitionByFunctionColumnNoExists() {
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',tb2.k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, tb2.k2 s2 from tbl1 tb1 join tbl2 tb2 on tb1.k2 = tb2.k2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 2, column 13 to line 2, column 38. " +
                            "Detail message: Materialized view partition exp: `tb2`.`k1` must related to column.",
                    e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoNeedParams() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc(tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 29. " +
                    "Detail message: No matching function with signature: date_trunc(date).", e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoCorrParams() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('%y%m',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error at line 3, column 29. " +
                            "Detail message: date_trunc function can't support argument other than year|quarter|month|week|day.",
                    e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoCorrParams1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',k2) ss, k2 from tbl2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 32. " +
                    "Detail message: Materialized view partition function date_trunc check failed: " +
                    "date_trunc('month', `k2`).", e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionUseWeek() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('week',k2) ss, k2 from tbl2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 31. " +
                    "Detail message: The function date_trunc used by the materialized view for partition " +
                    "does not support week formatting.", e.getMessage());
        }
    }

    @Test
    public void testPartitionByNoAllowedFunction() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k2, sqrt(tbl1.k1) ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 3, column 16 to line 3, column 28. " +
                            "Detail message: Materialized view partition function sqrt is not support: sqrt(`k1`).",
                    e.getMessage());
        }
    }

    @Test
    public void testPartitionByNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',k1) " +
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(s2) buckets 10 " +
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
    public void testDistributeByIsNull1() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss from tbl1;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
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
    public void testRefreshAsyncOnlyEvery() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
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
        } finally {
            dropMv("mv1");
        }
    }

    @Test
    public void testRefreshAsyncStartBeforeCurr() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2016-12-31') EVERY(INTERVAL 1 HOUR)" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;
            RefreshSchemeDesc refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
            Assert.assertEquals(MaterializedView.RefreshType.ASYNC, refreshSchemeDesc.getType());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshManual() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
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
            Assert.assertTrue(e.getMessage().contains("Materialized view query statement only support select"));
        }
    }

    // ========== as test ==========
    @Test
    public void testSetOperation() throws Exception {
        for (String setOp : Arrays.asList("UNION", "UNION ALL", "INTERSECT", "EXCEPT")) {
            String sql = String.format("create materialized view mv1 " +
                    "partition by ss " +
                    "distributed by hash(k2) buckets 10 " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")" +
                    "as select t1.k1 ss, t1.k2 from tbl1 t1 %s select k1, k2 from tbl2 t2;", setOp);
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        // All select list must be validated
        Assert.assertThrows("hehe", AnalysisException.class, () -> {
            String sql1 = "create materialized view mv1 " +
                    "partition by ss " +
                    "distributed by hash(k2) buckets 10 " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")" +
                    "as select t1.k1 ss, t1.k2 from tbl1 t1 union select * from tbl2 t2;";
            UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
        });

        Assert.assertThrows("hehe", AnalysisException.class, () -> {
            String sql1 = "create materialized view mv1 " +
                    "partition by ss " +
                    "distributed by hash(k2) buckets 10 " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")" +
                    "as select t1.k1 ss, t1.k2 from tbl1 t1 union select k1, k2 from tbl2 t2 union select * from tbl2 t3";
            UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
        });
    }

    @Test
    public void testAsTableNotInOneDatabase() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
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
    public void testMySQLTable() throws Exception {
        String sql1 = "create materialized view mv1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select tbl1.k1 ss, tbl1.k2 from mysql_external_table tbl1;";
        UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);

        String sql2 = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select tbl1.k1 ss, tbl1.k2 from mysql_external_table tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage()
                    .contains("Materialized view with partition does not support base table type : MYSQL"));
        }
    }

    @Test
    public void testCreateMvFromMv() {
        String sql1 = "create materialized view base_mv " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
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
        String sql2 = "create materialized view mv_from_base_mv " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from base_mv;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMvFromMv2() throws Exception {
        String sql1 = "create materialized view base_mv2 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        }

        String sql2 = "create materialized view mv_from_base_mv2 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from base_mv2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMvFromInactiveMv() {
        String sql1 = "create materialized view base_inactive_mv " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
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

        MaterializedView baseInactiveMv = ((MaterializedView) testDb.getTable("base_inactive_mv"));
        baseInactiveMv.setInactiveAndReason("");

        String sql2 = "create materialized view mv_from_base_inactive_mv " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from base_inactive_mv;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error at line 3, column 24. Detail message: " +
                            "Create/Rebuild materialized view from inactive materialized view: base_inactive_mv.",
                    e.getMessage());
        }
    }

    @Test
    public void testAsHasStar() throws Exception {
        String sql = "create materialized view testAsHasStar " +
                "partition by ss " +
                "distributed by hash(ss) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 ss, *  from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            MaterializedView mv = ((MaterializedView) testDb.getTable("testAsHasStar"));
            mv.setInactiveAndReason("");
            List<Column> mvColumns = mv.getFullSchema();

            Table baseTable = testDb.getTable("tbl1");
            List<Column> baseColumns = baseTable.getFullSchema();

            Assert.assertEquals(mvColumns.size(), baseColumns.size() + 1);
            Assert.assertEquals("ss", mvColumns.get(0).getName());
            for (int i = 1; i < mvColumns.size(); i++) {
                Assert.assertEquals(mvColumns.get(i).getName(),
                        baseColumns.get(i - 1).getName());
            }
        } catch (Exception e) {
            Assert.fail("Select * should be supported in materialized view");
        } finally {
            dropMv("testAsHasStar");
        }
    }

    @Test
    public void testAsHasStarWithSameColumns() throws Exception {
        String sql = "create materialized view testAsHasStar " +
                "partition by ss " +
                "distributed by hash(ss) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select a.k1 ss, a.*, b.* from tbl1 as a join tbl1 as b on a.k1=b.k1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Duplicate column name 'k1'"));
        } finally {
            dropMv("testAsHasStar");
        }
    }

    @Test
    public void testMVWithSameColumns() throws Exception {
        String sql = "create materialized view testAsHasStar " +
                "partition by ss " +
                "distributed by hash(ss) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select a.k1 ss, a.k2, b.k2 from tbl1 as a join tbl1 as b on a.k1=b.k1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Duplicate column name 'k2'"));
        } finally {
            dropMv("testAsHasStar");
        }
    }

    @Test
    public void testAsHasStarWithNondeterministicFunction() {
        String sql = "create materialized view mv1 " +
                "distributed by hash(ss) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 ss, *  from (select *, rand(), current_date() from tbl1) as t;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 3, column 38 to line 3, column 43." +
                    " Detail message: Materialized view query statement select item rand()" +
                    " not supported nondeterministic function.", e.getMessage());
        }
    }

    @Test
    public void testAsSelectItemAlias1() throws Exception {
        String sql = "create materialized view testAsSelectItemAlias1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            MaterializedView mv = ((MaterializedView) testDb.getTable("testAsSelectItemAlias1"));
            mv.setInactiveAndReason("");
            List<Column> mvColumns = mv.getFullSchema();

            Assert.assertEquals("date_trunc('month', tbl1.k1)", mvColumns.get(0).getName());
            Assert.assertEquals("k1", mvColumns.get(1).getName());
            Assert.assertEquals("k2", mvColumns.get(2).getName());

        } catch (Exception e) {
            Assert.fail("Materialized view query statement select item " +
                    "date_trunc('month', `tbl1`.`k1`) should be supported");
        } finally {
            dropMv("testAsSelectItemAlias1");
        }
    }

    @Test
    public void testAsSelectItemAlias2() throws Exception {
        String sql = "create materialized view testAsSelectItemAlias2 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as " +
                "select date_trunc('month',tbl1.k1), k1, k2 from tbl1 union all " +
                "select date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            MaterializedView mv = ((MaterializedView) testDb.getTable("testAsSelectItemAlias2"));
            mv.setInactiveAndReason("");
            List<Column> mvColumns = mv.getFullSchema();

            Assert.assertEquals("date_trunc('month', tbl1.k1)", mvColumns.get(0).getName());
            Assert.assertEquals("k1", mvColumns.get(1).getName());
            Assert.assertEquals("k2", mvColumns.get(2).getName());

        } finally {
            dropMv("testAsSelectItemAlias2");
        }
    }

    @Test
    // partition by expr is still not supported.
    public void testAsSelectItemAlias3() {
        String sql = "create materialized view testAsSelectItemAlias3 " +
                "partition by date_trunc('month',tbl1.k1) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Materialized view partition exp: " +
                    "`tbl1`.`k1` must related to column"));
        }
    }

    @Test
    // distribution by expr is still not supported.
    public void testAsSelectItemAlias4() {
        String sql = "create materialized view testAsSelectItemAlias4 " +
                "partition by k1 " +
                "distributed by hash(date_trunc('month',tbl1.k1)) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage()
                    .contains("No viable statement for input 'distributed by hash(date_trunc('."));
        }
    }

    @Test
    public void testAsSelectItemNoAliasWithNondeterministicFunction1() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select rand(), date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 16. " +
                            "Detail message: Materialized view query statement select item rand() not supported " +
                            "nondeterministic function.",
                    e.getMessage());
        }
    }

    @Test
    public void testAsSelectItemHasNonDeterministicFunction1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select rand() s1, date_trunc('month',tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 16. " +
                    "Detail message: Materialized view query statement " +
                    "select item rand() not supported nondeterministic function.", e.getMessage());
        }
    }

    @Test
    public void testAsSelectItemHasNonDeterministicFunction2() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select k2, rand()+rand() s1, date_trunc('month',tbl1.k1) ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 3, column 15 to line 3, column 20. " +
                    "Detail message: Materialized view query statement " +
                    "select item rand() not supported nondeterministic function.", e.getMessage());
        }
    }

    // ========== test colocate mv ==========
    @Test
    public void testCreateColocateMvNormal() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.colocateTable\n" +
                "(\n" +
                "    k1 int,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        String sql = "create materialized view colocateMv\n" +
                "PROPERTIES (\n" +
                "\"colocate_mv\" = \"true\"\n" +
                ")\n" +
                "as select k1, k2 from colocateTable;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStmt) statementBase);
            waitingRollupJobV2Finish();
            ColocateTableIndex colocateTableIndex = currentState.getColocateTableIndex();
            String fullGroupName = testDb.getId() + "_" + testDb.getFullName() + ":" + "colocateMv";
            System.out.println(fullGroupName);
            long tableId = colocateTableIndex.getTableIdByGroup(fullGroupName);
            Assert.assertNotEquals(-1, tableId);

            OlapTable table = (OlapTable) testDb.getTable("colocateTable");
            Assert.assertEquals(1, table.getColocateMaterializedViewNames().size());
            ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
            Assert.assertEquals(1, colocateTableIndex.getAllTableIds(groupId).size());

            dropMv("colocateMv");
            Assert.assertFalse(currentState.getColocateTableIndex().isColocateTable(tableId));
            Assert.assertEquals(0, table.getColocateMaterializedViewNames().size());

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            currentState.getColocateTableIndex().clear();
        }
    }

    @Test
    public void testCreateColocateMvToExitGroup() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.colocateTable2\n" +
                "(\n" +
                "    k1 int,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"group2\"\n" +
                ");");

        String sql = "create materialized view colocateMv2\n" +
                "PROPERTIES (\n" +
                "\"colocate_mv\" = \"true\"\n" +
                ")\n" +
                "as select k1, k2 from colocateTable2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStmt) statementBase);

            waitingRollupJobV2Finish();
            ColocateTableIndex colocateTableIndex = currentState.getColocateTableIndex();
            String fullGroupName = testDb.getId() + "_" + "group2";
            long tableId = colocateTableIndex.getTableIdByGroup(fullGroupName);
            Assert.assertNotEquals(-1, tableId);

            OlapTable table = (OlapTable) testDb.getTable("colocateTable2");
            Assert.assertEquals(1, table.getColocateMaterializedViewNames().size());
            ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
            Assert.assertEquals(1, colocateTableIndex.getAllTableIds(groupId).size());

            dropMv("colocateMv2");
            Assert.assertTrue(currentState.getColocateTableIndex().isColocateTable(tableId));
            Assert.assertEquals(0, table.getColocateMaterializedViewNames().size());

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            currentState.getColocateTableIndex().clear();
        }

    }

    @Test
    public void testColocateMvAlterGroup() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.colocateTable3\n" +
                "(\n" +
                "    k1 int,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"group3\"\n" +
                ");");

        String sql = "create materialized view colocateMv3\n" +
                "PROPERTIES (\n" +
                "\"colocate_mv\" = \"true\"\n" +
                ")\n" +
                "as select k1, k2 from colocateTable3;";
        String sql2 = "create materialized view colocateMv4\n" +
                "PROPERTIES (\n" +
                "\"colocate_mv\" = \"true\"\n" +
                ")\n" +
                "as select k1, k2 from colocateTable3;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStmt) statementBase);
            waitingRollupJobV2Finish();
            statementBase = UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStmt) statementBase);
            waitingRollupJobV2Finish();

            ColocateTableIndex colocateTableIndex = currentState.getColocateTableIndex();
            String fullGroupName = testDb.getId() + "_" + "group3";
            System.out.println(fullGroupName);
            long tableId = colocateTableIndex.getTableIdByGroup(fullGroupName);
            Assert.assertNotEquals(-1, tableId);

            OlapTable table = (OlapTable) testDb.getTable("colocateTable3");
            Assert.assertFalse(table.isInColocateMvGroup());
            Assert.assertEquals(2, table.getColocateMaterializedViewNames().size());
            ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
            Assert.assertEquals(1, colocateTableIndex.getAllTableIds(groupId).size());

            sql = "alter table colocateTable3 set (\"colocate_with\" = \"groupNew\")";
            statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statementBase);
            stmtExecutor.execute();

            Assert.assertEquals(2, table.getColocateMaterializedViewNames().size());
            Assert.assertEquals("groupNew", table.getColocateGroup());
            Assert.assertFalse(table.isInColocateMvGroup());
            Assert.assertTrue(colocateTableIndex.isColocateTable(tableId));

            sql = "alter table colocateTable3 set (\"colocate_with\" = \"\")";
            statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            stmtExecutor = new StmtExecutor(connectContext, statementBase);
            stmtExecutor.execute();

            Assert.assertTrue(colocateTableIndex.isColocateTable(tableId));
            Assert.assertTrue(table.isInColocateMvGroup());
            groupId = colocateTableIndex.getGroup(tableId);
            Assert.assertNotEquals("group1", table.getColocateGroup());

            dropMv("colocateMv4");
            Assert.assertEquals(1, table.getColocateMaterializedViewNames().size());
            dropMv("colocateMv3");
            Assert.assertFalse(colocateTableIndex.isColocateTable(tableId));

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            currentState.getColocateTableIndex().clear();
        }

    }

    @Test
    public void testRandomColocate() {
        String sql = "create materialized view mv1 " +
                "distributed by random " +
                "refresh async " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n," +
                "'colocate_with' = 'hehe' " +
                ")" +
                "as select k2, date_trunc('month',tbl1.k1) ss from tbl1;";
        Assert.assertThrows(SemanticException.class, () -> starRocksAssert.withMaterializedView(sql));
    }

    // ========== other test ==========
    @Test
    public void testDisabled() {
        Config.enable_materialized_view = false;
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(k2) buckets 10 " +
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
                "distributed by hash(k2) buckets 10 " +
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

    private void assertParseFailWithException(String sql, String msg) {
        CreateMaterializedViewStatement stmt = null;
        try {
            stmt = (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                    connectContext);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(msg));
        }
    }

    private void assertCreateFailWithException(String sql, String msg) {
        CreateMaterializedViewStatement stmt = null;
        try {
            stmt = (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                    connectContext);
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            currentState.createMaterializedView(stmt);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(msg));
        }
    }

    @Test
    public void testUnSupportedProperties() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"short_key\" = \"20\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        assertCreateFailWithException(sql, "Invalid parameter Analyze materialized properties failed because unknown " +
                "properties");
    }

    @Test
    public void testCreateMVWithSessionProperties1() {
        String sql = "create materialized view mv_with_property1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"session.query_timeout\" = \"10000\"" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";

        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            starRocksAssert.getCtx());
            currentState.createMaterializedView(stmt);
            Table mv1 = testDb.getTable("mv_with_property1");
            Assert.assertTrue(mv1 instanceof MaterializedView);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCreateMVWithSessionProperties2() {
        String sql = "create materialized view mv_with_property2 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"query_timeout\" = \"10000\"" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        assertCreateFailWithException(sql, "Invalid parameter Analyze materialized properties failed because unknown " +
                "properties");
    }

    @Test
    public void testCreateMVWithSessionProperties3() {
        String sql = "create materialized view mv_with_property3 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"session.query_timeout1\" = \"10000\"" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        assertCreateFailWithException(sql, "Unknown system variable 'query_timeout1'");
    }

    @Test
    public void testNoDuplicateKey() {
        String sql = "create materialized view testNoDuplicateKey " +
                "partition by s1 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";

        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            connectContext);
            currentState.createMaterializedView(stmt);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCreateMvWithImplicitColumnReorder() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view mv_column_reorder refresh async as " +
                "select c_1_3, c_1_4, c_1_0, c_1_1, c_1_2 from t1");
        MaterializedView mv = starRocksAssert.getMv("test", "mv_column_reorder");
        List<String> keys = mv.getKeyColumns().stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertEquals(Lists.newArrayList("c_1_3"), keys);
        Assert.assertEquals(Lists.newArrayList(), mv.getQueryOutputIndices());
        String ddl = mv.getMaterializedViewDdlStmt(false);
        Assert.assertTrue(ddl, ddl.contains("(`c_1_3`, `c_1_4`, `c_1_0`, `c_1_1`, `c_1_2`)"));
        Assert.assertTrue(ddl, ddl.contains(" SELECT `test`.`t1`.`c_1_3`, `test`.`t1`.`c_1_4`, " +
                "`test`.`t1`.`c_1_0`, `test`.`t1`.`c_1_1`, `test`.`t1`.`c_1_2`"));
    }

    @Test
    public void testCreateMvWithSortCols() throws Exception {
        starRocksAssert.dropMaterializedView("mv1");
        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`k1`, `s2`)\n" +
                    "as select tb1.k1, k2 s2 from tbl1 tb1;";
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement)
                    UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            List<String> keyColumns = createMaterializedViewStatement.getMvColumnItems().stream()
                    .filter(Column::isKey).map(Column::getName)
                    .collect(Collectors.toList());
            Assert.assertEquals(2, createMaterializedViewStatement.getSortKeys().size());
            Assert.assertEquals(Arrays.asList("k1", "s2"), keyColumns);

            starRocksAssert.withMaterializedView(sql);
            String ddl = starRocksAssert.getMv("test", "mv1")
                    .getMaterializedViewDdlStmt(false);
            Assert.assertTrue(ddl, ddl.contains("(`k1`, `s2`)"));
            Assert.assertTrue(ddl, ddl.contains("SELECT `test`.`tb1`.`k1`, `test`.`tb1`.`k2` AS `s2`"));
            starRocksAssert.dropMaterializedView("mv1");
        }

        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`s2`)\n" +
                    "as select tb1.k1, k2 s2 from tbl1 tb1;";
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement)
                    UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            List<String> keyColumns = createMaterializedViewStatement.getMvColumnItems().stream()
                    .filter(Column::isKey).map(Column::getName)
                    .collect(Collectors.toList());
            Assert.assertEquals(Arrays.asList("s2"), keyColumns);

            starRocksAssert.withMaterializedView(sql);
            String ddl = starRocksAssert.getMv("test", "mv1")
                    .getMaterializedViewDdlStmt(false);
            Assert.assertTrue(ddl, ddl.contains("(`k1`, `s2`)"));
            Assert.assertTrue(ddl, ddl.contains("SELECT `test`.`tb1`.`k1`, `test`.`tb1`.`k2` AS `s2`"));
            starRocksAssert.dropMaterializedView("mv1");
        }
        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`k1`)\n" +
                    "as select tb1.k1, k2 s2 from tbl1 tb1;";
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement)
                    UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            List<String> keyColumns = createMaterializedViewStatement.getMvColumnItems().stream()
                    .filter(Column::isKey).map(Column::getName)
                    .collect(Collectors.toList());
            Assert.assertEquals(Arrays.asList("k1"), keyColumns);

            starRocksAssert.withMaterializedView(sql);
            String ddl = starRocksAssert.getMv("test", "mv1")
                    .getMaterializedViewDdlStmt(false);
            Assert.assertTrue(ddl, ddl.contains("(`k1`, `s2`)"));
            Assert.assertTrue(ddl, ddl.contains("SELECT `test`.`tb1`.`k1`, `test`.`tb1`.`k2` AS `s2`"));
            starRocksAssert.dropMaterializedView("mv1");
        }
        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`k3`)\n" +
                    "as select tb1.k1, k2 s2 from tbl1 tb1;";
            Assert.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }
        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`c_1_7`)\n" +
                    "as select * from t1;";
            Assert.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }
    }

    @Test
    public void testCreateMvWithInvalidSortCols() throws Exception {
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2)\n" +
                "order by (`s2`, `k1`)\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        Assert.assertThrows("Sort columns should be a ordered prefix of select cols", Exception.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
    }

    @Test
    public void testCreateMvWithColocateGroup() throws Exception {
        String groupName = name.getMethodName();
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "'colocate_with' = '" + groupName + "'" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        String fullGroupName = testDb.getId() + "_" + groupName;
        long tableId = currentState.getColocateTableIndex().getTableIdByGroup(fullGroupName);
        Assert.assertTrue(tableId > 0);
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
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Do not support alter non-OLAP table[v1].",
                    e.getMessage());
        }
    }

    @Test
    public void testAggregateTableWithCount() {
        String sql = "CREATE MATERIALIZED VIEW v0 AS SELECT t0_57.c_0_1," +
                " COUNT(t0_57.c_0_0) , MAX(t0_57.c_0_2) , MAX(t0_57.c_0_3) , MIN(t0_57.c_0_4)" +
                " FROM tbl_for_count AS t0_57 GROUP BY t0_57.c_0_1 ORDER BY t0_57.c_0_1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Aggregate type table do not " +
                            "support count function in materialized view.",
                    e.getMessage());
        }
    }

    @Test
    public void testNoExistDb() {
        String sql = "create materialized view unexisted_db1.mv1\n" +
                "partition by s1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        assertParseFailWithException(sql, "Can not find database:unexisted_db1.");
    }

    @Test
    public void testMvNameInvalid() {
        String sql = "create materialized view mvklajksdjksjkjfksdlkfgkllksdjkgjsdjfjklsdjkfgjkldfkljgljkljklgja\n" +
                "partition by s1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            connectContext);
            currentState.createMaterializedView(stmt);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testMvName1() {
        String sql = "create materialized view 22mv\n" +
                "partition by s1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            connectContext);
            currentState.createMaterializedView(stmt);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testPartitionAndDistributionByColumnNameIgnoreCase() {
        String sql = "create materialized view mv1 " +
                "partition by K1 " +
                "distributed by hash(K2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            connectContext);
            currentState.createMaterializedView(stmt);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDuplicateColumn() {
        String sql = "create materialized view mv1 " +
                "partition by K1 " +
                "distributed by hash(K2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, K1 from tbl1;";
        assertParseFailWithException(sql, "Getting analyzing error. Detail message: Duplicate column name 'K1'.");
    }

    @Test
    public void testNoBaseTable() {
        String sql = "create materialized view mv1 " +
                "partition by K1 " +
                "distributed by hash(K2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select 1 as k1, 2 as k2";
        assertParseFailWithException(sql, "Getting analyzing error. Detail message: Can not find base " +
                "table in query statement.");
    }

    @Test
    public void testUseCte() throws Exception {
        String sql = "create materialized view mv1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH ASYNC\n" +
                "AS with tbl as\n" +
                "(select * from tbl1)\n" +
                "SELECT k1,k2\n" +
                "FROM tbl;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        sql = "create materialized view mv1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH ASYNC AS " +
                "WITH cte1 AS (select k1, k2 from tbl1),\n" +
                "     cte2 AS (select count(*) cnt from tbl1)\n" +
                "SELECT cte1.k1, cte2.cnt\n" +
                "FROM cte1, cte2;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testUseSubQuery() throws Exception {
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from (select * from tbl1) tbl";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testUseSubQueryWithPartition() throws Exception {
        String sql1 = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from (select * from tbl1) tbl";

        String sql2 = "create materialized view mv2 " +
                "partition by kk " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('day', k1) as kk, k2 from (select * from tbl1) tbl";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
            UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testJoinWithPartition() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('day', k1) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tb1.kk as k1, tb2.k2 as k2 from (select k1 as kk, k2 from tbl1) tb1 join (select * from tbl2) tb2 on tb1.kk = tb2.k1";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testPartitionByNotFirstColumn() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view mv_with_partition_by_not_first_column" +
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
    public void testHiveMVWithoutPartition() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_hive_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS select     s_suppkey,     s_nationkey," +
                "sum(s_acctbal) as total_s_acctbal,      count(s_phone) as s_phone_count from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table = db.getTable("supplier_hive_mv");
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assert.assertTrue(partitionInfo instanceof SinglePartitionInfo);
        Assert.assertEquals(1, mv.getAllPartitions().size());
        starRocksAssert.dropMaterializedView("supplier_hive_mv");
    }

    @Test
    public void testHiveMVJoinWithoutPartition() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_nation_hive_mv DISTRIBUTED BY " +
                "HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS select     s_suppkey,     n_name,      sum(s_acctbal) " +
                "as total_s_acctbal,      count(s_phone) as s_phone_count from " +
                "hive0.tpch.supplier as supp join hive0.tpch.nation group by s_suppkey, n_name order by s_suppkey;");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table = db.getTable("supplier_nation_hive_mv");
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assert.assertTrue(partitionInfo instanceof SinglePartitionInfo);
        Assert.assertEquals(1, mv.getAllPartitions().size());
        starRocksAssert.dropMaterializedView("supplier_nation_hive_mv");
    }

    @Test
    public void testHiveMVWithPartition() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW lineitem_supplier_hive_mv \n" +
                "partition by l_shipdate\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH MANUAL\n" +
                "AS \n" +
                "select l_shipdate, l_orderkey, l_quantity, l_linestatus, s_name from " +
                "hive0.partitioned_db.lineitem_par join hive0.tpch.supplier where l_suppkey = s_suppkey\n");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table = db.getTable("lineitem_supplier_hive_mv");
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assert.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        Assert.assertEquals(1, expressionRangePartitionInfo.getPartitionColumns().size());
        Column partColumn = expressionRangePartitionInfo.getPartitionColumns().get(0);
        Assert.assertEquals("l_shipdate", partColumn.getName());
        Assert.assertTrue(partColumn.getType().isDate());
        starRocksAssert.dropMaterializedView("lineitem_supplier_hive_mv");
    }

    @Test
    public void testHiveMVAsyncRefresh() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_hive_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH ASYNC  START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "AS select     s_suppkey,     s_nationkey, sum(s_acctbal) as total_s_acctbal,      " +
                "count(s_phone) as s_phone_count from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table = db.getTable("supplier_hive_mv");
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assert.assertTrue(partitionInfo instanceof SinglePartitionInfo);
        Assert.assertEquals(1, mv.getAllPartitions().size());
        MaterializedView.MvRefreshScheme mvRefreshScheme = mv.getRefreshScheme();
        Assert.assertEquals(mvRefreshScheme.getType(), MaterializedView.RefreshType.ASYNC);
        MaterializedView.AsyncRefreshContext asyncRefreshContext = mvRefreshScheme.getAsyncRefreshContext();
        Assert.assertEquals(asyncRefreshContext.getTimeUnit(), "HOUR");
        starRocksAssert.dropMaterializedView("supplier_hive_mv");
    }

    @Test
    public void testHiveMVAsyncRefreshWithException() throws Exception {
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("Materialized view which type is ASYNC need to specify refresh interval " +
                "for external table");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_hive_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH ASYNC AS select     s_suppkey,     s_nationkey," +
                "sum(s_acctbal) as total_s_acctbal,      count(s_phone) as s_phone_count from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;");
    }

    /**
     * Create MV on external catalog should report the correct error message
     */
    @Test
    public void testExternalCatalogException() throws Exception {
        // 1. create mv with full database name
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW default_catalog.test.supplier_hive_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS " +
                "select s_suppkey, s_nationkey, sum(s_acctbal) as total_s_acctbal " +
                "from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;");
        starRocksAssert.dropMaterializedView("default_catalog.test.supplier_hive_mv");

        // create mv with database.table
        starRocksAssert.useCatalog("hive0");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.supplier_hive_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS " +
                "select s_suppkey, s_nationkey, sum(s_acctbal) as total_s_acctbal " +
                "from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;");
        starRocksAssert.dropMaterializedView("default_catalog.test.supplier_hive_mv");

        // create mv with table name
        AnalysisException ex = Assert.assertThrows(AnalysisException.class, () ->
                starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_hive_mv " +
                        "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS " +
                        "select s_suppkey, s_nationkey, sum(s_acctbal) as total_s_acctbal " +
                        "from hive0.tpch.supplier as supp " +
                        "group by s_suppkey, s_nationkey order by s_suppkey;")
        );
        Assert.assertEquals("Getting analyzing error. Detail message: No database selected. " +
                        "You could set the database name through `<database>.<table>` or `use <database>` statement.",
                ex.getMessage());

        // create mv with wrong catalog
        ex = Assert.assertThrows(AnalysisException.class, () ->
                starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW hive0.tpch.supplier_hive_mv " +
                        "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS " +
                        "select s_suppkey, s_nationkey, sum(s_acctbal) as total_s_acctbal " +
                        "from hive0.tpch.supplier as supp " +
                        "group by s_suppkey, s_nationkey order by s_suppkey;")
        );
        Assert.assertEquals("Getting analyzing error from line 1, column 25 to line 1, column 36. " +
                        "Detail message: Materialized view can only be created in default_catalog. " +
                        "You could either create it with default_catalog.<database>.<mv>, " +
                        "or switch to default_catalog through `set catalog <default_catalog>` statement.",
                ex.getMessage());

        // reset state
        starRocksAssert.useCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        starRocksAssert.useDatabase(testDb.getFullName());
    }

    @Test
    public void testJdbcTable() throws Exception {
        starRocksAssert.withResource("create external resource jdbc0\n" +
                "properties (\n" +
                "    \"type\"=\"jdbc\",\n" +
                "    \"user\"=\"postgres\",\n" +
                "    \"password\"=\"changeme\",\n" +
                "    \"jdbc_uri\"=\"jdbc:postgresql://127.0.0.1:5432/jdbc_test\",\n" +
                "    \"driver_url\"=\"https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar\",\n" +
                "    \"driver_class\"=\"org.postgresql.Driver\"\n" +
                "); ");
        starRocksAssert.withTable("create external table jdbc_tbl (\n" +
                "     `id` bigint NULL,\n" +
                "     `data` varchar(200) NULL\n" +
                " ) ENGINE=jdbc\n" +
                " properties (\n" +
                "     \"resource\"=\"jdbc0\",\n" +
                "     \"table\"=\"dest_tbl\"\n" +
                " );");
        starRocksAssert.withMaterializedView("create materialized view mv_jdbc " +
                "distributed by hash(id) refresh deferred manual " +
                "as select * from jdbc_tbl;");
    }

    @Test
    public void testCreateRealtimeMV() throws Exception {
        String sql = "create materialized view rtmv \n" +
                "refresh incremental " +
                "distributed by hash(l_shipdate) " +
                " as select l_shipdate, l_orderkey, l_quantity, l_linestatus, s_name from " +
                "hive0.partitioned_db.lineitem_par join hive0.tpch.supplier where l_suppkey = s_suppkey\n";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testCreateSyncMvFromSubquery() {
        String sql = "create materialized view sync_mv_1 as" +
                " select k1, sum(k2) from (select k1, k2 from tbl1 group by k1, k2) a group by k1";
        try {
            starRocksAssert.withMaterializedView(sql);
        } catch (Exception e) {
            Assert.assertTrue(
                    e.getMessage().contains("Materialized view query statement only support direct query from table"));
        }
    }

    @Test
    public void testCreateAsyncMv() {
        Config.enable_experimental_mv = true;
        String sql = "create materialized view async_mv_1 distributed by hash(c_1_9) as" +
                " select c_1_9, c_1_4 from t1";
        try {
            starRocksAssert.withMaterializedView(sql);
            MaterializedView mv = (MaterializedView) testDb.getTable("async_mv_1");
            Assert.assertTrue(mv.getFullSchema().get(0).isKey());
            Assert.assertFalse(mv.getFullSchema().get(1).isKey());
        } catch (Exception e) {
            Assert.fail();
        }

        String sql2 = "create materialized view async_mv_1 distributed by hash(c_1_4) as" +
                " select c_1_4 from t1";
        try {
            starRocksAssert.withMaterializedView(sql2);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Data type of first column cannot be DOUBLE"));
        }
    }

    @Test
    public void testCollectAllTableAndView() {
        String sql = "select k2,v1 from test.tbl1 where k2 > 0 and v1 not in (select v1 from test.tbl2 where k2 > 0);";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Map<TableName, Table> result = AnalyzerUtils.collectAllTableAndView(statementBase);
            Assert.assertEquals(result.size(), 2);
        } catch (Exception e) {
            LOG.error("Test CollectAllTableAndView failed", e);
            Assert.fail();
        }
    }

    @Test
    public void testCreateMVWithDifferentDB() {
        try {
            ConnectContext newConnectContext = UtFrameUtils.createDefaultCtx();
            StarRocksAssert newStarRocksAssert = new StarRocksAssert(newConnectContext);
            newStarRocksAssert.withDatabase("test_mv_different_db")
                    .useDatabase("test_mv_different_db");
            String sql = "create materialized view test.test_mv_use_different_tbl " +
                    "as select k1, sum(v1), min(v2) from test.tbl5 group by k1;";
            CreateMaterializedViewStmt stmt =
                    (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, newStarRocksAssert.getCtx());
            Assert.assertEquals(stmt.getDBName(), "test");
            Assert.assertEquals(stmt.getMVName(), "test_mv_use_different_tbl");
            currentState.createMaterializedView(stmt);
            waitingRollupJobV2Finish();

            Table table = testDb.getTable("tbl5");
            Assert.assertNotNull(table);
            OlapTable olapTable = (OlapTable) table;
            Assert.assertTrue(olapTable.getIndexIdToMeta().size() >= 2);
            Assert.assertTrue(olapTable.getIndexIdToMeta().entrySet().stream()
                    .anyMatch(x -> x.getValue().getKeysType().isAggregationFamily()));
            newStarRocksAssert.dropDatabase("test_mv_different_db");
            starRocksAssert.dropMaterializedView("test_mv_use_different_tbl");
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCreateAsyncMVWithDifferentDB() {
        try {
            ConnectContext newConnectContext = UtFrameUtils.createDefaultCtx();
            StarRocksAssert newStarRocksAssert = new StarRocksAssert(newConnectContext);
            newStarRocksAssert.withDatabase("test_mv_different_db")
                    .useDatabase("test_mv_different_db");
            String sql = "create materialized view test.test_mv_use_different_tbl " +
                    "distributed by hash(k1) " +
                    "as select k1, sum(v1), min(v2) from test.tbl5 group by k1;";
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            newStarRocksAssert.getCtx());
            Assert.assertEquals(stmt.getTableName().getDb(), "test");
            Assert.assertEquals(stmt.getTableName().getTbl(), "test_mv_use_different_tbl");

            currentState.createMaterializedView(stmt);
            newStarRocksAssert.dropDatabase("test_mv_different_db");
            Table mv1 = testDb.getTable("test_mv_use_different_tbl");
            Assert.assertTrue(mv1 instanceof MaterializedView);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCreateAsyncMVWithDifferentDB2() {
        try {
            ConnectContext newConnectContext = UtFrameUtils.createDefaultCtx();
            StarRocksAssert newStarRocksAssert = new StarRocksAssert(newConnectContext);
            newStarRocksAssert.withDatabase("test_mv_different_db")
                    .useDatabase("test_mv_different_db");
            String sql = "create materialized view test_mv_different_db.test_mv_use_different_tbl " +
                    "distributed by hash(k1) " +
                    "as select k1, sum(v1), min(v2) from test.tbl5 group by k1;";
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            newStarRocksAssert.getCtx());
            Assert.assertEquals(stmt.getTableName().getDb(), "test_mv_different_db");
            Assert.assertEquals(stmt.getTableName().getTbl(), "test_mv_use_different_tbl");

            currentState.createMaterializedView(stmt);

            Database differentDb = currentState.getDb("test_mv_different_db");
            Table mv1 = differentDb.getTable("test_mv_use_different_tbl");
            Assert.assertTrue(mv1 instanceof MaterializedView);

            newStarRocksAssert.dropDatabase("test_mv_different_db");
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCreateSyncMVWithCaseWhenComplexExpression1() {
        try {
            String t1 = "CREATE TABLE case_when_t1 (\n" +
                    "    k1 INT,\n" +
                    "    k2 char(20))\n" +
                    "DUPLICATE KEY(k1)\n" +
                    "DISTRIBUTED BY HASH(k1)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n";
            starRocksAssert.withTable(t1);
            String mv1 = "create materialized view case_when_mv1 AS SELECT k1, " +
                    "(CASE k2 WHEN 'beijing' THEN 'bigcity' ELSE 'smallcity' END) as city FROM case_when_t1;\n";
            starRocksAssert.withMaterializedView(mv1);
            waitingRollupJobV2Finish();

            Table table = testDb.getTable("case_when_t1");
            Assert.assertNotNull(table);
            OlapTable olapTable = (OlapTable) table;
            Assert.assertTrue(olapTable.getIndexIdToMeta().size() >= 2);
            Assert.assertTrue(olapTable.getIndexIdToMeta().entrySet().stream()
                    .noneMatch(x -> x.getValue().getKeysType().isAggregationFamily()));
            List<Column> fullSchemas = table.getFullSchema();
            Assert.assertTrue(fullSchemas.size() == 3);
            Column mvColumn = fullSchemas.get(2);
            Assert.assertTrue(mvColumn.getName().equals("mv_city"));
            Assert.assertTrue(mvColumn.getType().isVarchar());
            Assert.assertTrue(mvColumn.getType().getColumnSize() == 1048576);
            starRocksAssert.dropTable("case_when_t1");
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCreateAsync_Deferred(@Mocked TaskManager taskManager) throws Exception {
        new Expectations() {
            {
                taskManager.executeTask((String) any);
                times = 0;
            }
        };
        starRocksAssert.withMaterializedView(
                "create materialized view deferred_async " +
                        "refresh deferred async distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view deferred_manual " +
                        "refresh deferred manual distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view deferred_scheduled " +
                        "refresh deferred async every(interval 1 day) distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
    }

    @Test
    public void testCreateAsync_Immediate(@Mocked TaskManager taskManager) throws Exception {
        new Expectations() {
            {
                taskManager.executeTask((String) any);
                times = 3;
            }
        };
        starRocksAssert.withMaterializedView(
                "create materialized view async_immediate " +
                        "refresh immediate async distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view manual_immediate " +
                        "refresh immediate manual distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view schedule_immediate " +
                        "refresh immediate async every(interval 1 day) distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
    }

    @Test
    public void testCreateAsync_Immediate_Implicit(@Mocked TaskManager taskManager) throws Exception {
        new Expectations() {
            {
                taskManager.executeTask((String) any);
                times = 3;
            }
        };
        starRocksAssert.withMaterializedView(
                "create materialized view async_immediate_implicit " +
                        "refresh async distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view manual_immediate_implicit " +
                        "refresh manual distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view schedule_immediate_implicit " +
                        "refresh async every(interval 1 day) distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
    }

    private void testMVColumnAlias(String expr) throws Exception {
        String mvName = "mv_alias";
        try {
            String createMvExpr =
                    String.format("create materialized view %s " +
                            "refresh deferred manual distributed by hash(c_1_9) as" +
                            " select c_1_9, %s from t1", mvName, expr);
            starRocksAssert.withMaterializedView(createMvExpr);
            Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
            Table table = db.getTable(mvName);
            List<String> columnNames = table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());
            Assert.assertTrue(columnNames.toString(), columnNames.contains(expr));
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testExprAlias() throws Exception {
        testMVColumnAlias("c_1_9 + 1");
        testMVColumnAlias("char_length(c_1_9)");
        testMVColumnAlias("(char_length(c_1_9)) + 1");
        testMVColumnAlias("(char_length(c_1_9)) + '$'");
        testMVColumnAlias("c_1_9 + c_1_10");
    }

    private Table getTable(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(mvName);
        Assert.assertNotNull(table);
        return table;
    }

    private MaterializedView getMv(String dbName, String mvName) {
        Table table = getTable(dbName, mvName);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        return mv;
    }

    @Test
    public void testMvNullable() throws Exception {
        starRocksAssert.withTable("create table emps (\n" +
                        "    empid int not null,\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null,\n" +
                        "    salary double\n" +
                        ")\n" +
                        "distributed by hash(`empid`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("create table depts (\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null\n" +
                        ")\n" +
                        "distributed by hash(`deptno`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
        {
            starRocksAssert.withMaterializedView("create materialized view mv_nullable" +
                    " distributed by hash(`empid`) as" +
                    " select empid, d.deptno, d.name" +
                    " from emps e left outer join depts d on e.deptno = d.deptno");
            MaterializedView mv = getMv("test", "mv_nullable");
            Assert.assertFalse(mv.getColumn("empid").isAllowNull());
            Assert.assertTrue(mv.getColumn("deptno").isAllowNull());
            Assert.assertTrue(mv.getColumn("name").isAllowNull());
            starRocksAssert.dropMaterializedView("mv_nullable");
        }

        {
            starRocksAssert.withMaterializedView("create materialized view mv_nullable" +
                    " distributed by hash(`empid`) as" +
                    " select empid, d.deptno, d.name" +
                    " from emps e right outer join depts d on e.deptno = d.deptno");
            MaterializedView mv = getMv("test", "mv_nullable");
            Assert.assertTrue(mv.getColumn("empid").isAllowNull());
            Assert.assertFalse(mv.getColumn("deptno").isAllowNull());
            Assert.assertFalse(mv.getColumn("name").isAllowNull());
            starRocksAssert.dropMaterializedView("mv_nullable");
        }

        {
            starRocksAssert.withMaterializedView("create materialized view mv_nullable" +
                    " distributed by hash(`empid`) as" +
                    " select empid, d.deptno, d.name" +
                    " from emps e full outer join depts d on e.deptno = d.deptno");
            MaterializedView mv = getMv("test", "mv_nullable");
            Assert.assertTrue(mv.getColumn("empid").isAllowNull());
            Assert.assertTrue(mv.getColumn("deptno").isAllowNull());
            Assert.assertTrue(mv.getColumn("name").isAllowNull());
            starRocksAssert.dropMaterializedView("mv_nullable");
        }

        starRocksAssert.dropTable("emps");
        starRocksAssert.dropTable("depts");
    }

    @Test
    public void testCreateAsyncDateTruncAndTimeSLice() throws Exception {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k11)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                    "') EVERY(INTERVAL 3 DAY)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select time_slice(tb1.k1, interval 5 minute) as k11, k2 s2 from tbl1 tb1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k11)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                    "') EVERY(INTERVAL 3 DAY)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select time_slice(tb1.k1, interval 5 year) as k11, k2 s2 from tbl1 tb1;";
            Assert.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k11)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                    "') EVERY(INTERVAL 3 DAY)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select time_slice(tb1.k1, interval 5 month) as k11, k2 s2 from tbl1 tb1;";
            Assert.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k11)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                    "') EVERY(INTERVAL 3 DAY)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select time_slice(tb1.k1, interval 5 month, 'ceil') as k11, k2 s2 from tbl1 tb1;";
            Assert.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }
    }

    @Test
    public void testMVWithMaxRewriteStaleness() throws Exception {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv_with_rewrite_staleness \n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 SECOND)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"," +
                "\"mv_rewrite_staleness_second\" = \"60\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            ThreadUtil.sleepAtLeastIgnoreInterrupts(4000L);
            Table mv1 = testDb.getTable("mv_with_rewrite_staleness");
            Assert.assertTrue(mv1 instanceof MaterializedView);

            // test partition
            MaterializedView materializedView = (MaterializedView) mv1;
            Assert.assertEquals(materializedView.getMaxMVRewriteStaleness(), 60);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            dropMv("mv_with_rewrite_staleness");
        }
    }

    @Test
    public void testCreateMvWithView() throws Exception {
        starRocksAssert.withView("create view view_1 as select tb1.k1, k2 s2 from tbl1 tb1;");
        starRocksAssert.withView("create view view_2 as select v1.k1, v1.s2 from view_1 v1;");
        starRocksAssert.withView("create view view_3 as select date_trunc('month',k1) d1, v1.s2 from view_1 v1;");

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select * from view_1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select k1, s2 from view_1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select v1.k1, v1.s2 from view_1 v1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by d1\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select date_trunc('month',k1) d1, v1.s2 from view_1 v1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by d1\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select v3.d1, v3.s2 from view_3 v3;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select v2.k1, v2.s2 from view_2 v2;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }
        starRocksAssert.dropView("view_1");
        starRocksAssert.dropView("view_2");
        starRocksAssert.dropView("view_3");
    }

    @Test
    public void testMvOnUnion() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `customer_nullable_1` (\n" +
                "  `c_custkey` int(11) NULL COMMENT \"\",\n" +
                "  `c_name` varchar(26) NULL COMMENT \"\",\n" +
                "  `c_address` varchar(41) NULL COMMENT \"\",\n" +
                "  `c_city` varchar(11) NULL COMMENT \"\",\n" +
                "  `c_nation` varchar(16) NULL COMMENT \"\",\n" +
                "  `c_region` varchar(13) NULL COMMENT \"\",\n" +
                "  `c_phone` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `c_mktsegment` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `customer_nullable_2` (\n" +
                "  `c_custkey` int(11) NULL COMMENT \"\",\n" +
                "  `c_name` varchar(26) NULL COMMENT \"\",\n" +
                "  `c_address` varchar(41) NULL COMMENT \"\",\n" +
                "  `c_city` varchar(11) NULL COMMENT \"\",\n" +
                "  `c_nation` varchar(16) NULL COMMENT \"\",\n" +
                "  `c_region` varchar(13) NULL COMMENT \"\",\n" +
                "  `c_phone` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `c_mktsegment` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("\n" +
                "CREATE TABLE `customer_nullable_3` (\n" +
                "  `c_custkey` int(11)  NULL COMMENT \"\",\n" +
                "  `c_name` varchar(26)  NULL COMMENT \"\",\n" +
                "  `c_address` varchar(41)  NULL COMMENT \"\",\n" +
                "  `c_city` varchar(11)  NULL COMMENT \"\",\n" +
                "  `c_nation` varchar(16)  NULL COMMENT \"\",\n" +
                "  `c_region` varchar(13)  NULL COMMENT \"\",\n" +
                "  `c_phone` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `c_mktsegment` varchar(11) NOT NULL COMMENT \"\",\n" +
                "  `c_total` decimal(19,6) null default \"0.0\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withMaterializedView("\n" +
                "create materialized view customer_mv\n" +
                "distributed by hash(`custkey`)\n" +
                "as\n" +
                "\n" +
                "select\n" +
                "\tc_custkey custkey,\n" +
                "\tc_name name,\n" +
                "\tc_phone phone,\n" +
                "\t0 total,\n" +
                "\t c_mktsegment segment\n" +
                "from customer_nullable_1\n" +
                "\n" +
                "union all\n" +
                "\n" +
                "select\n" +
                "\tc_custkey custkey,\n" +
                "\tnull name,\n" +
                "\tnull phone,\n" +
                "\t0 total,\n" +
                "\t c_mktsegment segment\n" +
                "from customer_nullable_2\n" +
                "\n" +
                "union all\n" +
                "\n" +
                "select\n" +
                "\tc_custkey custkey,\n" +
                "\tnull name,\n" +
                "\tnull phone,\n" +
                "\tc_total total,\n" +
                "\t c_mktsegment segment\n" +
                "from customer_nullable_3;");

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");

        MaterializedView mv = (MaterializedView) db.getTable("customer_mv");
        Assert.assertTrue(mv.getColumn("total").getType().isDecimalOfAnyVersion());
        Assert.assertFalse(mv.getColumn("segment").isAllowNull());
    }

    @Test
    public void testRandomizeStart() throws Exception {
        // NOTE: if the test case execute super slow, the delta would not be so stable
        final long FIXED_DELTA = 5;
        String sql = "create materialized view mv_test_randomize \n" +
                "distributed by hash(k1) buckets 10\n" +
                "refresh async every(interval 1 minute) " +
                "PROPERTIES (\n" +
                "'replication_num' = '1'" +
                ")\n" +
                "as " +
                "select tb1.k1, k2, " +
                "array<int>[1,2,3] as type_array, " +
                "map<int, int>{1:2} as type_map, " +
                "parse_json('{\"a\": 1}') as type_json, " +
                "row('c') as type_struct, " +
                "array<json>[parse_json('{}')] as type_array_json " +
                "from tbl1 tb1;";
        long currentSecond = Utils.getLongFromDateTime(LocalDateTime.now());
        starRocksAssert.withMaterializedView(sql);
        MaterializedView mv = getMv(testDb.getFullName(), "mv_test_randomize");
        long startTime = mv.getRefreshScheme().getAsyncRefreshContext().getStartTime();
        long delta = startTime - currentSecond;
        Assert.assertTrue("delta is " + delta, delta >= 0 && delta <= 60);
        starRocksAssert.dropMaterializedView("mv_test_randomize");

        // manual disable it
        sql = "create materialized view mv_test_randomize \n" +
                "distributed by hash(k1) buckets 10\n" +
                "refresh async every(interval 1 minute) " +
                "PROPERTIES (\n" +
                "'replication_num' = '1', " +
                "'mv_randomize_start' = '-1'" +
                ")\n" +
                "as " +
                "select tb1.k1, k2, " +
                "array<int>[1,2,3] as type_array, " +
                "map<int, int>{1:2} as type_map, " +
                "parse_json('{\"a\": 1}') as type_json, " +
                "row('c') as type_struct, " +
                "array<json>[parse_json('{}')] as type_array_json " +
                "from tbl1 tb1;";
        currentSecond = Utils.getLongFromDateTime(LocalDateTime.now());
        starRocksAssert.withMaterializedView(sql);
        mv = getMv(testDb.getFullName(), "mv_test_randomize");
        startTime = mv.getRefreshScheme().getAsyncRefreshContext().getStartTime();
        delta = startTime - currentSecond;
        Assert.assertTrue("delta is " + delta, delta >= 0 && delta < FIXED_DELTA);
        starRocksAssert.dropMaterializedView("mv_test_randomize");

        // manual specify it
        sql = "create materialized view mv_test_randomize \n" +
                "distributed by hash(k1) buckets 10\n" +
                "refresh async every(interval 1 minute) " +
                "PROPERTIES (\n" +
                "'replication_num' = '1', " +
                "'mv_randomize_start' = '2'" +
                ")\n" +
                "as " +
                "select tb1.k1, k2, " +
                "array<int>[1,2,3] as type_array, " +
                "map<int, int>{1:2} as type_map, " +
                "parse_json('{\"a\": 1}') as type_json, " +
                "row('c') as type_struct, " +
                "array<json>[parse_json('{}')] as type_array_json " +
                "from tbl1 tb1;";
        currentSecond = Utils.getLongFromDateTime(LocalDateTime.now());
        starRocksAssert.withMaterializedView(sql);
        mv = getMv(testDb.getFullName(), "mv_test_randomize");
        startTime = mv.getRefreshScheme().getAsyncRefreshContext().getStartTime();
        delta = startTime - currentSecond;
        Assert.assertTrue("delta is " + delta, delta >= 0 && delta < (2 + FIXED_DELTA));
        starRocksAssert.dropMaterializedView("mv_test_randomize");
    }

    @Test
    public void testCreateMvWithTypes() throws Exception {
        String sql = "create materialized view mv_test_types \n" +
                "distributed by hash(k1) buckets 10\n" +
                "PROPERTIES (\n" +
                "'replication_num' = '1'" +
                ")\n" +
                "as " +
                "select tb1.k1, k2, " +
                "array<int>[1,2,3] as type_array, " +
                "map<int, int>{1:2} as type_map, " +
                "parse_json('{\"a\": 1}') as type_json, " +
                "row('c') as type_struct, " +
                "array<json>[parse_json('{}')] as type_array_json " +
                "from tbl1 tb1;";
        starRocksAssert.withMaterializedView(sql);
    }

    @Test
    public void testCreateMaterializedViewWithTableAlias1() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select t0.k1, t0.k2, t0.sum as sum0 " +
                "from (select k1, k2, sum(v1) as sum from tbl1 group by k1, k2) t0 where t0.k2 > 10";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMaterializedViewWithTableAlias2() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select t0.k1, t0.k2, t0.sum as sum0, t1.sum as sum1, t2.sum as sum2 " +
                "from (select k1, k2, sum(v1) as sum from tbl1 group by k1, k2) t0 " +
                "left join (select  k1, k2, sum(v1) as sum from tbl1 group by k1, k2) t1 on t0.k1=t1.k2 " +
                "left join (select k1, k2, sum(v1) as sum from tbl1 group by k1, k2) t2 on t0.k1=t2.k1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMvWithViewAndSubQuery() throws Exception {
        starRocksAssert.withView("create view view_1 as " +
                "select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10;");
        starRocksAssert.withView("create view view_2 as " +
                "select k1, s2 from (select v1.k1, v1.s2 from view_1 v1) t where t.k1 > 10;");
        starRocksAssert.withView("create view view_3 as " +
                "select d1, s2 from (select date_trunc('month',k1) d1, v1.s2 from view_1 v1)t where d1 is not null;");
        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select k1, s2 from view_1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select view_1.k1, view_2.s2 from view_1 join view_2 on view_1.k1=view_2.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by d1\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select v3.d1, v3.s2 from view_3 v3;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select view_1.k1, view_2.s2 from view_1 join view_2 on view_1.k1=view_2.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        starRocksAssert.dropView("view_1");
        starRocksAssert.dropView("view_2");
        starRocksAssert.dropView("view_3");
    }

    @Test
    public void testCreateSynchronousMVOnLakeTable() throws Exception {
        String sql = "create materialized view sync_mv1 as select k1, sum(v1) from mocked_cloud_table group by k1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        Table table = getTable("test", "mocked_cloud_table");
        // Change table type to cloud native table
        Deencapsulation.setField(table, "type", Table.TableType.CLOUD_NATIVE);
        DdlException e = Assert.assertThrows(DdlException.class, () -> {
            GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);
        });
        Assert.assertTrue(e.getMessage().contains("Creating synchronous materialized view(rollup) is not supported in " +
                "shared data clusters.\nPlease use asynchronous materialized view instead.\n" +
                "Refer to https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements" +
                "/data-definition/CREATE%20MATERIALIZED%20VIEW#asynchronous-materialized-view for details."));
    }

    List<Column> getMaterializedViewKeysChecked(String sql) {
        String mvName = null;
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;

            currentState.createMaterializedView(createMaterializedViewStatement);
            ThreadUtil.sleepAtLeastIgnoreInterrupts(4000L);

            TableName mvTableName = createMaterializedViewStatement.getTableName();
            mvName = mvTableName.getTbl();

            Table table = testDb.getTable(mvName);
            Assert.assertNotNull(table);
            Assert.assertTrue(table instanceof MaterializedView);
            MaterializedView mv = (MaterializedView) table;

            return mv.getFullSchema().stream().filter(Column::isKey).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (!Objects.isNull(mvName)) {
                try {
                    starRocksAssert.dropMaterializedView(mvName);
                } catch (Exception e) {
                    Assert.fail();
                }
            }
        }
        return Lists.newArrayList();
    }

    @Test
    public void testCreateSynchronousMVOnAnotherMV() throws Exception {
        String sql = "create materialized view sync_mv1 as select k1, sum(v1) from mocked_cloud_table group by k1;";
        CreateMaterializedViewStmt createTableStmt = (CreateMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser(sql, connectContext);
        Table table = getTable("test", "mocked_cloud_table");
        // Change table type to materialized view
        Deencapsulation.setField(table, "type", Table.TableType.MATERIALIZED_VIEW);
        DdlException e = Assert.assertThrows(DdlException.class, () -> {
            GlobalStateMgr.getCurrentState().getMetadata().createMaterializedView(createTableStmt);
        });
        Assert.assertTrue(e.getMessage().contains("Do not support create synchronous materialized view(rollup) on"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar1() {
        // sort key by default
        String sql = "create materialized view test_mv1 " +
                "partition by c_1_3 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assert.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assert.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assert.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar2() {
        // sort key by default
        String sql = "create materialized view test_mv2 " +
                "partition by c_1_3 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1 union all select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assert.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assert.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assert.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar3() {
        // sort key by default
        String sql = "create materialized view test_mv1 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assert.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assert.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assert.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar4() {
        // sort key by default
        String sql = "create materialized view test_mv2 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1 union all select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assert.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assert.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assert.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar5() {
        // sort key by default
        String sql = "create materialized view test_mv2 " +
                "partition by date_trunc('day', c_1_3) " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1 union all select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assert.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assert.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assert.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar6() {
        // sort key by default
        String sql = "create materialized view test_mv2 " +
                "partition by date_trunc('month', c_1_3) " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1 union all select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assert.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assert.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assert.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
    }

    @Test
    public void testCreateMvWithUnsupportedStr2date() {
        {
            String sql = "create materialized view mv1 " +
                    "partition by ss " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select str2date(d, '%m-%d-%Y') ss, a, b, c from jdbc0.partitioned_db0.tbl1;";
            Assert.assertThrows("Materialized view partition function date_trunc check failed",
                    AnalysisException.class, () -> starRocksAssert.useDatabase("test").withMaterializedView(sql));
        }

        {
            String sql = "create materialized view mv1 " +
                    "partition by date_trunc('month', ss) " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select str2date(d, '%m-%d-%Y') ss, a, b, c from jdbc0.partitioned_db0.tbl1;";
            Assert.assertThrows("Materialized view partition function date_trunc check failed",
                    AnalysisException.class, () -> starRocksAssert.useDatabase("test").withMaterializedView(sql));
        }
    }

    @Test
    public void testCreateMvWithCTE() throws Exception {
        starRocksAssert.withView("create view view_1 as select tb1.k1, k2 s2 from tbl1 tb1;");
        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select k1, s2 from cte1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select a.k1, a.s2 from cte1 as a;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10), " +
                    " cte2 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select cte1.k1, cte2.s2 from cte1 join cte2 on cte1.k1=cte2.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by d1\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte3 as (select d1, s2 from (select date_trunc('month',k1) d1" +
                    ", v1.s2 from view_1 v1)t where d1 is not null) " +
                    " select v3.d1, v3.s2 from cte3 v3;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10), " +
                    " cte2 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select cte1.k1, cte2.s2 from cte1 join cte2 on cte1.k1=cte2.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10), " +
                    " cte2 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select a.k1, b.s2 from cte1 a join cte2 b on a.k1=b.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',b.k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10), " +
                    " cte2 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select a.k1, b.s2 from cte1 a join cte2 b on a.k1=b.k1;";
            Assert.assertThrows("Materialized view partition exp: `b`.`k1` must related to column.",
                    AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }
        starRocksAssert.dropView("view_1");
    }
}