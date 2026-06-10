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
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
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
}
