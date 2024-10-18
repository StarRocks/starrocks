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

package com.starrocks.qe;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
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

    @BeforeClass
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(temp.newFolder().toURI().toString());
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }

        new MockUp<MetadataMgr>() {
            @Mock
            public Table getTable(String catalogName, String dbName, String tblName) {
                return new IcebergTable();
            }
        };
        starRocksAssert.withDatabase("test").useDatabase("test")
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
                        "'external_cooldown_target'='iceberg.db1.tbl1',\n" +
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

        Table table = starRocksAssert.getTable("test", "tbl1");
        String taskName = TaskBuilder.getExternalCooldownTaskName(table.getId());
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(taskName);
        Assert.assertNotNull(task);

        StatementBase stmt1 = UtFrameUtils.parseStmtWithNewParser("cancel cooldown table test.tbl1", connectContext);
        StmtExecutor executor1 = new StmtExecutor(connectContext, stmt1);
        executor1.execute();
    }
}
