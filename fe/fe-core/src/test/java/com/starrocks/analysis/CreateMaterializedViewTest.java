// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

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
                .withTable("CREATE TABLE test.tbl_for_count " +
                        "(" +
                        "   c_0_0 BIGINT NULL ," +
                        "   c_0_1 DATE NOT NULL ," +
                        "   c_0_2 DECIMAL(37, 5)  NOT NULL," +
                        "   c_0_3 INT MAX NOT NULL , " +
                        "   c_0_4 DATE REPLACE_IF_NOT_NULL NOT NULL ," +
                        "   c_0_5 PERCENTILE PERCENTILE_UNION NOT NULL" +
                        ") " +
                        "AGGREGATE KEY (c_0_0,c_0_1,c_0_2) " +
                        "PARTITION BY RANGE(c_0_1) " +
                        "(" +
                        "   START (\"2010-01-01\") END (\"2021-12-31\") EVERY (INTERVAL 219 day) " +
                        ") " +
                        "DISTRIBUTED BY HASH (c_0_2,c_0_1) BUCKETS 3 " +
                        "properties(\"replication_num\"=\"1\") ;")
                .useDatabase("test");
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
        Database testDb = null;
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            testDb = currentState.getDb("default_cluster:test");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (testDb != null) {
                testDb.dropTable("mv1");
            }
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
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Unknown properties: {short_key=20}");
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
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Data type of first column cannot be DATE");
        } finally {
            FeConstants.shortkey_max_column_count = 3;
        }
    }

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
    public void testFullCreate() {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {}
        };
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 5 SECOND) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        Database testDb = null;
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            testDb = currentState.getDb("default_cluster:test");
            Table mv1 = testDb.getTable("mv1");
            assertTrue(mv1 instanceof MaterializedView);

            MaterializedView materializedView = (MaterializedView) mv1;
            PartitionInfo partitionInfo = materializedView.getPartitionInfo();
            assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
            assertEquals(partitionInfo.getPartitionColumns().size(),1);
            Column partitionColumn = partitionInfo.getPartitionColumns().get(0);
            Set<Long> baseTableIds = materializedView.getBaseTableIds();
            assertEquals(baseTableIds.size(), 1);
            OlapTable baseTable = ((OlapTable) testDb.getTable(baseTableIds.iterator().next()));
            assertTrue(baseTable.getColumn(partitionColumn.getName()) != null);
            assertEquals(baseTable.getRelatedMaterializedViews().size(), 1);
            assertEquals(materializedView.getViewDefineSql(),
                    "SELECT date_trunc('month', `test`.`tbl1`.`k1`) AS `s1`, `test`.`tbl1`.`k2` AS `s2` FROM `test`.`tbl1`");
            TableProperty tableProperty = materializedView.getTableProperty();
            assertEquals(tableProperty.getReplicationNum().shortValue(), 1);
            assertEquals(materializedView.getState(), OlapTable.OlapTableState.NORMAL);
            assertEquals(materializedView.getKeysType(), KeysType.DUP_KEYS);
            assertEquals(materializedView.getType(), Table.TableType.MATERIALIZED_VIEW); //TableTypeMATERIALIZED_VIEW
            assertEquals(materializedView.getRelatedMaterializedViews().size(), 0);
            assertEquals(materializedView.getBaseSchema().size(), 2);
            MaterializedView.AsyncRefreshContext asyncRefreshContext =
                    materializedView.getRefreshScheme().getAsyncRefreshContext();
            assertTrue(asyncRefreshContext.getStartTime() > 0);
            assertEquals(asyncRefreshContext.getTimeUnit(), "SECOND");
            assertEquals(asyncRefreshContext.getStep(), 5);
            assertTrue(materializedView.isActive());
            // test sync
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);
            assertEquals(Constants.TaskRunState.SUCCESS, taskRuns.get(0).getState());
            assertEquals(((ExpressionRangePartitionInfo) partitionInfo).getIdToRange(false).size(),2);
            assertEquals(materializedView.getPartitions().size(),2);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (testDb != null) {
                testDb.dropTable("mv1");
            }
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
            CreateMaterializedViewStatement statementBase =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Set<Long> baseTableIds = statementBase.getBaseTableIds();
            assertEquals(baseTableIds.size(), 1);
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
        Database testDb = null;
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
            testDb = currentState.getDb("default_cluster:test");
            Table mv1 = testDb.getTable("mv1");
            assertTrue(mv1 instanceof MaterializedView);

            MaterializedView materializedView = (MaterializedView) mv1;
            PartitionInfo partitionInfo = materializedView.getPartitionInfo();
            assertTrue(partitionInfo instanceof SinglePartitionInfo);
            assertEquals(materializedView.getPartitions().size(),1);
            Partition partition = materializedView.getPartitions().iterator().next();
            assertTrue(partition != null);
            assertEquals(partition.getName(),"mv1");
            // test sync
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (testDb != null) {
                testDb.dropTable("mv1");
            }
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
                "as select k2,sqrt(tbl1.k1) s1 from tbl1;";
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
                "as select k2, sqrt(tbl1.k1) ss from tbl1;";
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
    public void testAggregateTableWithCount() {
        String sql = "CREATE MATERIALIZED VIEW v0 AS SELECT t0_57.c_0_1," +
                " COUNT(t0_57.c_0_0) , MAX(t0_57.c_0_2) , MAX(t0_57.c_0_3) , MIN(t0_57.c_0_4)" +
                " FROM tbl_for_count AS t0_57 GROUP BY t0_57.c_0_1 ORDER BY t0_57.c_0_1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Aggregate type table do not support count function in materialized view");
        }
    }
}

