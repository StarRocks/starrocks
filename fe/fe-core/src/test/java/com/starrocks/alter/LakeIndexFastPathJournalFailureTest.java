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
import com.starrocks.persist.EditLog;
import com.starrocks.persist.EditLogException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * A lake index fast-path ALTER whose job fails to be journaled must leave the table as it was.
 *
 * <p>The table state is set by {@link SchemaChangeHandler#process} in the WAL applier, together with
 * registering the job. If the state were changed before the journal write, a failed write would leave
 * the table in SCHEMA_CHANGE with no job behind it: every later ALTER is rejected, CANCEL ALTER has
 * nothing to cancel, and lake compaction skips the table, until the FE stops being the leader. Closing
 * the leader WAL gate makes {@link EditLog#logAlterJob} throw at exactly that point.
 */
public class LakeIndexFastPathJournalFailureTest {
    private static final String DB_NAME = "db_lake_index_fast_path_journal_failure";
    private static ConnectContext connectContext;
    private static int savedAlterInterval;

    @BeforeAll
    public static void setUp() throws Exception {
        // Keep the jobs of the retried ALTERs in PENDING: only their registration matters here.
        savedAlterInterval = Config.alter_scheduler_interval_millisecond;
        Config.alter_scheduler_interval_millisecond = 86400000;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database " + DB_NAME, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
    }

    @AfterAll
    public static void tearDown() {
        Config.alter_scheduler_interval_millisecond = savedAlterInterval;
    }

    private static OlapTable createTable(String name, String extraColumns, String extraProperties) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE TABLE " + name + " (k BIGINT NOT NULL, v1 BIGINT, v2 VARCHAR(64)" + extraColumns + ")"
                        + " PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 2"
                        + " PROPERTIES('replication_num'='1'" + extraProperties + ")",
                connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), name);
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    private static List<AlterJobV2> unfinishedJobs(OlapTable table) {
        return GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                .getUnfinishedAlterJobV2ByTableId(table.getId());
    }

    private static void assertJournalFailureLeavesTableUsable(OlapTable table, String sql,
                                                              Class<? extends AlterJobV2> fastPathJobClass)
            throws Exception {
        EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
        editLog.closeWalGate();
        try {
            Assertions.assertThrows(EditLogException.class, () -> alterTable(sql));
        } finally {
            editLog.openWalGate();
        }
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertTrue(unfinishedJobs(table).isEmpty());

        // Once the journal accepts writes again, the same ALTER takes the fast path and registers its job.
        alterTable(sql);
        List<AlterJobV2> jobs = unfinishedJobs(table);
        Assertions.assertEquals(1, jobs.size());
        Assertions.assertInstanceOf(fastPathJobClass, jobs.get(0));
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
    }

    @Test
    public void testAddIndex() throws Exception {
        OlapTable table = createTable("t_add_index", "", "");
        assertJournalFailureLeavesTableUsable(table, "ALTER TABLE t_add_index ADD INDEX idx_v1 (v1) USING BITMAP",
                LakeTableAddIndexJob.class);
    }

    @Test
    public void testDropIndex() throws Exception {
        OlapTable table = createTable("t_drop_index", ", INDEX idx_v1 (v1) USING BITMAP", "");
        assertJournalFailureLeavesTableUsable(table, "ALTER TABLE t_drop_index DROP INDEX idx_v1",
                LakeTableDropIndexJob.class);
    }

    @Test
    public void testAddBloomFilter() throws Exception {
        OlapTable table = createTable("t_add_bf", "", "");
        assertJournalFailureLeavesTableUsable(table, "ALTER TABLE t_add_bf SET (\"bloom_filter_columns\" = \"v1\")",
                LakeTableAddIndexJob.class);
    }

    @Test
    public void testDropBloomFilter() throws Exception {
        OlapTable table = createTable("t_drop_bf", "", ", 'bloom_filter_columns'='v1'");
        assertJournalFailureLeavesTableUsable(table, "ALTER TABLE t_drop_bf SET (\"bloom_filter_columns\" = \"\")",
                LakeTableDropIndexJob.class);
    }
}
