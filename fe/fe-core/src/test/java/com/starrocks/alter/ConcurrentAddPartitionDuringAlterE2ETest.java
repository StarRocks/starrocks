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

package com.starrocks.alter;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;

/**
 * End-to-end coverage (shared-data cluster) for the G1 guard relaxation in
 * {@link AlterJobExecutor#visitAlterTableStatement}: a manual {@code ALTER TABLE ... ADD PARTITION}
 * may proceed while the table is in the transient {@code UPDATING_META} state of fast schema
 * evolution, instead of failing with {@code InvalidOlapTableStateException}.
 *
 * <p>The UPDATING_META branch needs no alter job (Path 1 of the design), so this test exercises
 * the executor wiring — the {@code addPartitionOnly} clause check, the config gate, and the
 * downstream {@code addPartitions} call against a non-NORMAL table — without racing the alter
 * scheduler daemon. The SCHEMA_CHANGE + lake-index-job branch (Path 2) is covered by
 * {@link ConcurrentAddPartitionDuringAlterTest} (decision logic) and the shared-data SQL test.
 */
public class ConcurrentAddPartitionDuringAlterE2ETest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "db_concurrent_add_partition_test";

    private static int savedAlterInterval;

    @BeforeAll
    public static void setUp() throws Exception {
        // Quiesce the alter-scheduler daemon for the whole class so a manually-registered fast-path
        // job stays in PENDING (the daemon would otherwise pick it up). Set before cluster creation
        // so the handler's daemon starts with this interval.
        savedAlterInterval = Config.alter_scheduler_interval_millisecond;
        Config.alter_scheduler_interval_millisecond = 86400000; // 1 day
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @AfterAll
    public static void tearDown() {
        Config.alter_scheduler_interval_millisecond = savedAlterInterval;
    }

    @BeforeEach
    public void before() throws Exception {
        Config.enable_concurrent_add_partition_during_alter = true;
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
    }

    @AfterEach
    public void after() throws Exception {
        Config.enable_concurrent_add_partition_during_alter = true;
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    private static LakeTable createRangeTable(String name) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE TABLE " + name + " (" +
                        "  k DATE NOT NULL," +
                        "  v BIGINT NOT NULL" +
                        ") DUPLICATE KEY(k)" +
                        " PARTITION BY RANGE(k) (" +
                        "  PARTITION p1 VALUES LESS THAN ('2020-02-01')" +
                        " )" +
                        " DISTRIBUTED BY HASH(k) BUCKETS 1" +
                        " PROPERTIES('replication_num'='1')",
                connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        return (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), name);
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    @Test
    public void testAddPartitionAllowedDuringUpdatingMeta() throws Exception {
        LakeTable table = createRangeTable("t_allow");
        int before = table.getPartitions().size();
        table.setState(OlapTable.OlapTableState.UPDATING_META);
        try {
            alterTable("ALTER TABLE t_allow ADD PARTITION p2 VALUES LESS THAN ('2020-03-01')");
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
        }
        Assertions.assertEquals(before + 1, table.getPartitions().size());
        Assertions.assertNotNull(table.getPartition("p2"));
    }

    @Test
    public void testAddColumnStillRejectedDuringUpdatingMeta() throws Exception {
        // Only ADD PARTITION is relaxed; any other clause keeps the legacy rejection.
        LakeTable table = createRangeTable("t_reject_col");
        table.setState(OlapTable.OlapTableState.UPDATING_META);
        try {
            Assertions.assertThrows(Exception.class, () ->
                    alterTable("ALTER TABLE t_reject_col ADD COLUMN c BIGINT"));
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
        }
    }

    @Test
    public void testAddPartitionRejectedWhenConfigDisabled() throws Exception {
        // The mutable config is the kill switch: with it off, legacy exclusive behavior returns.
        LakeTable table = createRangeTable("t_config_off");
        Config.enable_concurrent_add_partition_during_alter = false;
        table.setState(OlapTable.OlapTableState.UPDATING_META);
        try {
            Assertions.assertThrows(Exception.class, () ->
                    alterTable("ALTER TABLE t_config_off ADD PARTITION p2 VALUES LESS THAN ('2020-03-01')"));
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
            Config.enable_concurrent_add_partition_during_alter = true;
        }
        Assertions.assertEquals(1, table.getPartitions().size());
    }

    private static long nextJobId = 90_000_001L;

    private static long dbId() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME).getId();
    }

    private static SchemaChangeHandler schemaChangeHandler() {
        return GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
    }

    /** Register a real (un-run) lake ADD INDEX fast-path job so the table is "under" a safe alter job. */
    private static void registerSafeJob(LakeTable table) {
        schemaChangeHandler().addAlterJobV2(new LakeTableAddIndexJob(
                nextJobId++, dbId(), table.getId(), table.getName(), 60_000L, new ArrayList<>(), new ArrayList<>()));
    }

    /** Register a real (un-run) full-rewrite schema-change job (does NOT tolerate concurrent ADD PARTITION). */
    private static void registerUnsafeJob(LakeTable table) {
        schemaChangeHandler().addAlterJobV2(new LakeTableSchemaChangeJob(
                nextJobId++, dbId(), table.getId(), table.getName(), 60_000L));
    }

    @Test
    public void testAddPartitionAllowedDuringSchemaChangeWithSafeJob() throws Exception {
        // Path 2: a running lake ADD/DROP INDEX fast-path job holds the table in SCHEMA_CHANGE, but it
        // declares allowConcurrentPartitionCreation() -> ADD PARTITION proceeds (G1 + addPartitions relax).
        LakeTable table = createRangeTable("t_sc_safe");
        registerSafeJob(table);
        int before = table.getPartitions().size();
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            alterTable("ALTER TABLE t_sc_safe ADD PARTITION p2 VALUES LESS THAN ('2020-03-01')");
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
        }
        Assertions.assertEquals(before + 1, table.getPartitions().size());
        Assertions.assertNotNull(table.getPartition("p2"));
    }

    @Test
    public void testAddPartitionRejectedDuringSchemaChangeWithUnsafeJob() throws Exception {
        // A full-rewrite schema-change job does NOT tolerate concurrent partition creation -> rejected.
        LakeTable table = createRangeTable("t_sc_unsafe");
        registerUnsafeJob(table);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            Assertions.assertThrows(Exception.class, () ->
                    alterTable("ALTER TABLE t_sc_unsafe ADD PARTITION p2 VALUES LESS THAN ('2020-03-01')"));
        } finally {
            table.setState(OlapTable.OlapTableState.NORMAL);
        }
        Assertions.assertEquals(1, table.getPartitions().size());
    }

    @Test
    public void testCancelConflictingAlterJobsSkipsSafeJobButCancelsUnsafe() throws Exception {
        // G2: the automatic-create-partition path skips cancelling a safe job, but still routes an
        // unsafe one to the legacy cancel branch. Drive the private static helper directly.
        Method m = FrontendServiceImpl.class.getDeclaredMethod("cancelConflictingAlterJobs",
                GlobalStateMgr.class, Database.class, OlapTable.class, long.class, boolean.class, long.class);
        m.setAccessible(true);
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        Database db = gsm.getLocalMetastore().getDb(DB_NAME);

        // (a) safe job -> skip branch: job is NOT cancelled.
        LakeTable safe = createRangeTable("t_g2_safe");
        registerSafeJob(safe);
        safe.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            m.invoke(null, gsm, db, safe, 1001L, true, 5000L);
            Assertions.assertFalse(
                    schemaChangeHandler().getUnfinishedAlterJobV2ByTableId(safe.getId()).isEmpty(),
                    "safe job must not be cancelled by automatic partition creation");
        } finally {
            safe.setState(OlapTable.OlapTableState.NORMAL);
        }

        // (b) unsafe job -> legacy else branch (cancelAlterJob) executes; the helper swallows any
        // cancel error internally, so the call returns normally.
        LakeTable unsafe = createRangeTable("t_g2_unsafe");
        registerUnsafeJob(unsafe);
        unsafe.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            m.invoke(null, gsm, db, unsafe, 1002L, true, 5000L);
        } finally {
            unsafe.setState(OlapTable.OlapTableState.NORMAL);
        }
    }
}
