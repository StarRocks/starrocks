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

package com.starrocks.scheduler;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.externalcooldown.ExternalCooldownConfig;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateExternalCooldownStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;

import java.time.Instant;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionBasedCooldownProcessorTest {
    protected static ConnectContext connectContext;
    protected static PseudoCluster cluster;
    protected static StarRocksAssert starRocksAssert;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    protected static long startSuiteTime = 0;
    protected long startCaseTime = 0;

    protected static final String TEST_DB_NAME = "test";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        starRocksAssert.withDatabase(TEST_DB_NAME);
        starRocksAssert.useDatabase(TEST_DB_NAME);

        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);


        starRocksAssert.withDatabase(TEST_DB_NAME).useDatabase(TEST_DB_NAME)
                .withTable("CREATE TABLE tbl1\n" +
                        "(\n" +
                        "    id int,\n" +
                        "    ts datetime,\n" +
                        "    data string\n" +
                        ")\n" +
                        "DUPLICATE KEY(`id`, `ts`)\n" +
                        "PARTITION BY RANGE(`ts`)\n" +
                        "(\n" +
                        "    PARTITION p20200101 VALUES [('2020-01-01 00:00:00'),('2020-01-02 00:00:00')),\n" +
                        "    PARTITION p20200102 VALUES [('2020-01-02 00:00:00'),('2020-01-03 00:00:00')),\n" +
                        "    PARTITION p20200103 VALUES [('2020-01-03 00:00:00'),('2020-01-04 00:00:00')),\n" +
                        "    PARTITION p20200104 VALUES [('2020-01-04 00:00:00'),('2020-01-05 00:00:00')),\n" +
                        "    PARTITION p20200105 VALUES [('2020-01-05 00:00:00'),('2020-01-06 00:00:00'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                        "PROPERTIES(\n" +
                        "'external_cooldown_target'='iceberg0.partitioned_transforms_db.t0_day',\n" +
                        "'external_cooldown_schedule'='START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE',\n" +
                        "'external_cooldown_wait_second'='3600',\n" +
                        "'replication_num' = '1'\n" +
                        ");")
                .withTable("CREATE TABLE tbl2\n" +
                "(\n" +
                "    id int,\n" +
                "    ts datetime,\n" +
                "    data string\n" +
                ")\n" +
                "DUPLICATE KEY(`id`, `ts`)\n" +
                "PARTITION BY RANGE(`ts`)\n" +
                "(\n" +
                "    PARTITION p20200101 VALUES [('2020-01-01 00:00:00'),('2020-01-02 00:00:00')),\n" +
                "    PARTITION p20200102 VALUES [('2020-01-02 00:00:00'),('2020-01-03 00:00:00')),\n" +
                "    PARTITION p20200103 VALUES [('2020-01-03 00:00:00'),('2020-01-04 00:00:00')),\n" +
                "    PARTITION p20200104 VALUES [('2020-01-04 00:00:00'),('2020-01-05 00:00:00')),\n" +
                "    PARTITION p20200105 VALUES [('2020-01-05 00:00:00'),('2020-01-06 00:00:00'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "'external_cooldown_target'='iceberg0.partitioned_transforms_db.t0_day',\n" +
                "'external_cooldown_schedule'='START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE',\n" +
                "'external_cooldown_wait_second'='3600',\n" +
                "'replication_num' = '1'\n" +
                ");");
    }

    @Before
    public void before() {
        startCaseTime = Instant.now().getEpochSecond();
    }

    protected static void initAndExecuteTaskRun(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());

        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() throws Exception {
                return;
            }
        };
        taskRun.executeTaskRun();
    }

    protected static void initAndExecuteTaskRunFail(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());

        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() throws Exception {
                throw new UserException("test");
            }
        };
        taskRun.executeTaskRun();
    }

    private static void triggerExternalCooldown(Database testDb, OlapTable table, Partition partition, boolean success)
            throws Exception {
        Task task = TaskBuilder.buildExternalCooldownTask(testDb, table, partition);
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        if (success) {
            initAndExecuteTaskRun(taskRun);
        } else {
            initAndExecuteTaskRunFail(taskRun);
        }
    }

    private static void triggerExternalCooldown(Database testDb, Table table, boolean success) throws Exception {
        TableName tableName;
        if (testDb.getCatalogName() == null) {
            tableName = new TableName(testDb.getOriginName(), table.getName());
        } else {
            tableName = new TableName(testDb.getCatalogName(), testDb.getOriginName(), table.getName());
        }
        CreateExternalCooldownStmt stmt = new CreateExternalCooldownStmt(
                tableName, null, false, null);
        Task task = TaskBuilder.buildExternalCooldownTask(stmt);
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        if (success) {
            initAndExecuteTaskRun(taskRun);
        } else {
            initAndExecuteTaskRunFail(taskRun);
        }
    }

    protected void assertPlanContains(ExecPlan execPlan, String... explain) {
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    @Test
    public void testCooldownSinglePartition() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        OlapTable olapTable = ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(TEST_DB_NAME, "tbl1"));

        Partition partition = olapTable.getPartition("p20200101");
        partition.getDefaultPhysicalPartition().setVisibleVersion(
                partition.getDefaultPhysicalPartition().getVisibleVersion() + 1,
                System.currentTimeMillis() - 3600 * 1000);
        triggerExternalCooldown(testDb, olapTable, partition, true);

        Partition partition1 = olapTable.getPartition("p20200102");
        partition1.getDefaultPhysicalPartition().setVisibleVersion(
                partition1.getDefaultPhysicalPartition().getVisibleVersion() + 1,
                System.currentTimeMillis() - 3600 * 1000);
        Assert.assertThrows(DdlException.class, () -> triggerExternalCooldown(testDb, olapTable, partition1, false));

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        Long syncedTimeMs = partitionInfo.getExternalCoolDownSyncedTimeMs(partition.getId());
        Assert.assertNotNull(syncedTimeMs);
        Assert.assertEquals((Long) partition.getDefaultPhysicalPartition().getVisibleVersionTime(), syncedTimeMs);

        partition = olapTable.getPartition("p20200101");
        partition.getDefaultPhysicalPartition().setVisibleVersion(
                partition.getDefaultPhysicalPartition().getVisibleVersion() + 1,
                System.currentTimeMillis() - 3600 * 1000);
        partition = olapTable.getPartition("p20200102");
        partition.getDefaultPhysicalPartition().setVisibleVersion(
                partition.getDefaultPhysicalPartition().getVisibleVersion() + 1,
                System.currentTimeMillis() - 3600 * 1000);
        partition = olapTable.getPartition("p20200103");
        partition.getDefaultPhysicalPartition().setVisibleVersion(
                partition.getDefaultPhysicalPartition().getVisibleVersion() + 1,
                System.currentTimeMillis() - 3600 * 1000);
        triggerExternalCooldown(testDb, olapTable, true);
    }

    @Test
    public void testExecutorIsNull() {
        new MockUp<StatementBase>() {
            @Mock
            public void setOrigStmt(OriginStatement origStmt) {
                throw new RuntimeException("test");
            }
        };
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        OlapTable olapTable = ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(TEST_DB_NAME, "tbl1"));

        Partition partition = olapTable.getPartition("p20200101");
        partition.getDefaultPhysicalPartition().setVisibleVersion(
                partition.getDefaultPhysicalPartition().getVisibleVersion() + 1,
                System.currentTimeMillis() - 3600 * 1000);
        Assert.assertThrows(RuntimeException.class, () -> triggerExternalCooldown(testDb, olapTable, partition, false));
    }

    @Test
    public void testStateErr() {
        new MockUp<QueryState>() {
            @Mock
            public QueryState.MysqlStateType getStateType() {
                return QueryState.MysqlStateType.ERR;
            }
        };
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        OlapTable olapTable = ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(TEST_DB_NAME, "tbl1"));

        Partition partition = olapTable.getPartition("p20200101");
        partition.getDefaultPhysicalPartition().setVisibleVersion(
                partition.getDefaultPhysicalPartition().getVisibleVersion() + 1,
                System.currentTimeMillis() - 3600 * 1000);
        Assert.assertThrows(DdlException.class, () -> triggerExternalCooldown(testDb, olapTable, partition, true));
    }

    @Test
    public void testPrepare() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        OlapTable olapTable1 = ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(TEST_DB_NAME, "tbl2"));
        Assert.assertThrows(DmlException.class, () -> triggerExternalCooldown(testDb, olapTable1, true));

        String sql2 = "ALTER TABLE tbl2 SET(\"external_cooldown_target\" = \"\");";
        AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, alterTableStmt2);
        Assert.assertThrows(DmlException.class, () -> triggerExternalCooldown(testDb, olapTable1, true));

        String sql3 = "ALTER TABLE tbl2 SET(\"external_cooldown_target\" = \"iceberg0.partitioned_transforms_db.t0_day\");";
        AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql3, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, alterTableStmt3);

        String sql4 = "ALTER TABLE tbl2 SET(\"external_cooldown_wait_second\" = \"0\");";
        AlterTableStmt alterTableStmt4 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql4, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, alterTableStmt4);
        Assert.assertThrows(DmlException.class, () -> triggerExternalCooldown(testDb, olapTable1, true));

        String sql5 = "ALTER TABLE tbl2 SET(\"external_cooldown_wait_second\" = \"3600\");";
        AlterTableStmt alterTableStmt5 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql5, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, alterTableStmt5);

        new MockUp<LocalMetastore>() {
            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return new IcebergTable();
            }
        };
        Assert.assertThrows(DmlException.class, () -> triggerExternalCooldown(testDb, olapTable1, true));

        new MockUp<MockIcebergMetadata>() {
            @Mock
            public com.starrocks.catalog.Table getTable(String dbName, String tblName) {
                return new OlapTable();
            }
        };
        ExternalCooldownConfig config = olapTable1.getCurExternalCoolDownConfig();
        config.setTarget("iceberg0.partitioned_transforms_db.xxxx");
        olapTable1.setCurExternalCoolDownConfig(config);
        Assert.assertThrows(DmlException.class, () -> triggerExternalCooldown(testDb, olapTable1, true));

        config.setTarget("default_catalog.test.tbl1");
        olapTable1.setCurExternalCoolDownConfig(config);
        Assert.assertThrows(DmlException.class, () -> triggerExternalCooldown(testDb, olapTable1, true));
    }

    @Test
    public void testPrepare1() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        OlapTable olapTable1 = ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(TEST_DB_NAME, "tbl2"));

        new MockUp<OlapTable>() {
            @Mock
            public Table getExternalCoolDownTable() {
                return null;
            }
        };
        Assert.assertThrows(DmlException.class, () -> triggerExternalCooldown(testDb, olapTable1, true));
    }

    @Test
    public void testPrepare2() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        OlapTable olapTable1 = ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(TEST_DB_NAME, "tbl2"));

        new MockUp<LocalMetastore>() {
            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return null;
            }
        };
        Assert.assertThrows(DmlException.class, () -> triggerExternalCooldown(testDb, olapTable1, true));
    }

    @Test
    public void testPrepare3() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        OlapTable olapTable1 = ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(TEST_DB_NAME, "tbl2"));

        new MockUp<OlapTable>() {
            @Mock
            public Table getExternalCoolDownTable() {
                return new OlapTable();
            }
        };
        Assert.assertThrows(DmlException.class, () -> triggerExternalCooldown(testDb, olapTable1, true));
    }
}