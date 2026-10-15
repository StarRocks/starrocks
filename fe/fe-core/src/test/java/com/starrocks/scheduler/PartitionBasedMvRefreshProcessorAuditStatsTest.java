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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PartitionBasedMvRefreshProcessorAuditStatsTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE test.audit_stats_tbl\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
    }

    /**
     * Regression test: async MV refresh runs the INSERT OVERWRITE through
     * StmtExecutor.handleDMLStmtWithProfile() directly and bypasses StmtExecutor.execute(), where
     * recordExecStatsIntoContext() is invoked. Before the fix the execution statistics were never
     * flushed into the audit event builder, so the audit log lost CpuCostNs/MemCostBytes/ScanRows
     * for every MV refresh. This verifies they are recorded again.
     *
     * Backport of StarRocks/starrocks#74954; on this branch the refresh processor is
     * PartitionBasedMvRefreshProcessor rather than MVTaskRunProcessor.
     */
    @Test
    public void testMVRefreshRecordsAuditStats() throws Exception {
        // Insert data so the refresh issues a real INSERT OVERWRITE that produces exec statistics.
        executeInsertSql(connectContext, "insert into test.audit_stats_tbl partition(p1) values('2022-01-02', 1, 10)");

        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test_mv_audit_stats` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        "AS SELECT k1, k2, v1 FROM test.audit_stats_tbl;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "test_mv_audit_stats");
        Assertions.assertNotNull(mv);

        TaskRun taskRun = buildMVTaskRun(mv, testDb.getFullName());
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        // The refresh uses its own ConnectContext; read the audit event built on that context.
        ConnectContext runCtx = taskRun.getRunCtx();
        Assertions.assertNotNull(runCtx, "MV refresh ConnectContext should be available");
        AuditEvent auditEvent = runCtx.getAuditEventBuilder().build();

        // Default value is -1 (dropped from the audit line by AuditLogBuilder). A non-default value
        // proves the stats were flushed into the builder on the MV refresh path.
        Assertions.assertNotEquals(-1L, auditEvent.cpuCostNs,
                "MV refresh audit log should record CpuCostNs");
        Assertions.assertNotEquals(-1L, auditEvent.memCostBytes,
                "MV refresh audit log should record MemCostBytes");
        Assertions.assertNotEquals(-1L, auditEvent.scanRows,
                "MV refresh audit log should record ScanRows");

        starRocksAssert.dropMaterializedView("test_mv_audit_stats");
    }
}
