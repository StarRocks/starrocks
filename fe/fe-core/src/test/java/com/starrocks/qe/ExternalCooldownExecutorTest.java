// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
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

package com.starrocks.qe;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CancelExternalCooldownStmt;
import com.starrocks.sql.ast.CreateExternalCooldownStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ExternalCooldownExecutorTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    protected static PseudoCluster cluster;

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
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 int,\n" +
                        "    k2 datetime,\n" +
                        "    v1 int\n" +
                        ")\n" +
                        "DUPLICATE KEY(`k1`, `k2`)\n" +
                        "PARTITION BY RANGE(`k2`)\n" +
                        "(\n" +
                        "    PARTITION p1 VALUES [('2020-01-01 00:00:00'),('2020-01-02 00:00:00')),\n" +
                        "    PARTITION p2 VALUES [('2020-01-02 00:00:00'),('2020-01-03 00:00:00'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES(\n" +
                        "'external_cooldown_target'='iceberg0.partitioned_transforms_db.t0_day',\n" +
                        "'external_cooldown_schedule'='START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE',\n" +
                        "'external_cooldown_wait_second'='3600',\n" +
                        "'replication_num' = '1'\n" +
                        ");");
    }

    @Test
    public void testCooldownTable() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("cooldown table test.tbl1", connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, stmt);
        executor.execute();

        Table table = starRocksAssert.getTable(TEST_DB_NAME, "tbl1");
        String taskName = TaskBuilder.getExternalCooldownTaskName(table.getId());
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(taskName);
        Assert.assertNotNull(task);

        StatementBase stmt1 = UtFrameUtils.parseStmtWithNewParser("cancel cooldown table test.tbl1", connectContext);
        StmtExecutor executor1 = new StmtExecutor(connectContext, stmt1);
        executor1.execute();

        CreateExternalCooldownStmt stmt2 = (CreateExternalCooldownStmt) UtFrameUtils.parseStmtWithNewParser(
                "cooldown table test.tbl1", connectContext);
        stmt2.setTableName(TableName.fromString("iceberg0.partitioned_transforms_db.t0_day"));
        Assert.assertThrows(SemanticException.class, () -> DDLStmtExecutor.execute(stmt2, connectContext));
    }

    @Test
    public void testDDLStmtExecutorCancel() throws Exception {
        StatementBase stmt1 = UtFrameUtils.parseStmtWithNewParser("cancel cooldown table test.tbl1", connectContext);
        try {
            DDLStmtExecutor.execute(stmt1, connectContext);
        } catch (Exception ex) {
            Assert.fail();
        }

        // no exist table
        CancelExternalCooldownStmt stmt2 = (CancelExternalCooldownStmt) UtFrameUtils.parseStmtWithNewParser(
                "cancel cooldown table test.tbl1", connectContext);
        stmt2.setTableName(TableName.fromString("test.tbl001"));
        Assert.assertThrows(DdlException.class, () -> DDLStmtExecutor.execute(stmt2, connectContext));

        // non olap table
        CancelExternalCooldownStmt stmt3 = (CancelExternalCooldownStmt) UtFrameUtils.parseStmtWithNewParser(
                "cancel cooldown table test.tbl1", connectContext);
        stmt3.setTableName(TableName.fromString("iceberg0.partitioned_transforms_db.t0_day"));
        Assert.assertThrows(DdlException.class, () -> DDLStmtExecutor.execute(stmt3, connectContext));
    }
}
