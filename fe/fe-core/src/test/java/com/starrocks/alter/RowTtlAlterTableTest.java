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

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WALApplier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Tests ALTER TABLE ... SET for the row TTL properties: the three keys travel together, they cannot
 * be mixed with anything else, and the admission gate lets an already configured table keep changing.
 */
public class RowTtlAlterTableTest {
    private static final String EXPIRE_AT = PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT;
    private static final String CHECK_INTERVAL = PropertyAnalyzer.PROPERTIES_ROW_TTL_CHECK_INTERVAL_SECOND;
    private static final String TIME_ZONE = PropertyAnalyzer.PROPERTIES_ROW_TTL_TIME_ZONE;

    private static ConnectContext connectContext;

    private String dbName;
    private boolean savedEnableRowTtl;

    @BeforeAll
    public static void setUpClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @BeforeEach
    public void setUp() throws Exception {
        savedEnableRowTtl = Config.enable_row_ttl;
        Config.enable_row_ttl = true;
        dbName = "row_ttl_alter_" + UUID.randomUUID().toString().replace("-", "");
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database " + dbName, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(dbName);
        createTable("CREATE TABLE t(k INT NOT NULL, expire_at DATETIME, other_time DATETIME, created_at BIGINT) "
                + "PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1");
    }

    @AfterEach
    public void tearDown() {
        Config.enable_row_ttl = savedEnableRowTtl;
    }

    private void createTable(String sql) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
    }

    private void alter(String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    private String rejectionOf(String sql) {
        Exception e = assertThrows(Exception.class, () -> alter(sql));
        return e.getMessage() == null ? "" : e.getMessage();
    }

    private OlapTable table() {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbName, "t");
    }

    @Test
    public void testFirstConfigurationSetsAllThreeKeysInOneStatement() throws Exception {
        alter("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"expire_at + INTERVAL 7 DAY\", "
                + "\"" + CHECK_INTERVAL + "\" = \"3600\", \"" + TIME_ZONE + "\" = \"UTC\")");
        assertEquals("expire_at + INTERVAL 7 DAY", table().getProperties().get(EXPIRE_AT));
        assertEquals("3600", table().getProperties().get(CHECK_INTERVAL));
        assertEquals("UTC", table().getProperties().get(TIME_ZONE));
    }

    @Test
    public void testConfiguredTableKeepsChanging() throws Exception {
        alter("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"expire_at\")");

        alter("ALTER TABLE t SET (\"" + CHECK_INTERVAL + "\" = \"600\")");
        assertEquals("600", table().getProperties().get(CHECK_INTERVAL));

        // The gate guards the entrance only, so a configured table can still be repointed while it
        // is off, and DROP ROW TTL stays available too.
        Config.enable_row_ttl = false;
        alter("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"other_time\")");
        assertEquals("other_time", table().getProperties().get(EXPIRE_AT));
    }

    @Test
    public void testRejections() {
        assertTrue(rejectionOf("ALTER TABLE t SET (\"" + CHECK_INTERVAL + "\" = \"600\")").contains(EXPIRE_AT));
        assertTrue(rejectionOf("ALTER TABLE t SET (\"" + TIME_ZONE + "\" = \"UTC\")").contains(EXPIRE_AT));
        assertTrue(rejectionOf("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"expire_at\", "
                + "\"replication_num\" = \"1\")").contains("one table property"));
        assertTrue(rejectionOf("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"created_at\")")
                .contains("to_datetime"));
        assertTrue(rejectionOf("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"to_datetime(created_at, 2)\")")
                .contains("scale"));

        Config.enable_row_ttl = false;
        assertTrue(rejectionOf("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"expire_at\")")
                .contains("enable_row_ttl"));
    }

    @Test
    public void testMaterializedViewDoesNotTakeRowTtl() {
        String sql = "CREATE MATERIALIZED VIEW mv REFRESH MANUAL PROPERTIES(\"" + EXPIRE_AT
                + "\" = \"expire_at\") AS SELECT k, expire_at FROM t";
        Exception e = assertThrows(Exception.class, () -> {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(stmt);
        });
        String message = e.getMessage() == null ? "" : e.getMessage();
        // The message has to name materialized views. Saying "only primary key tables" would send
        // the user looking for a primary key materialized view that would not work either, and the
        // untouched path would ask for a `session.` prefix that no prefix would make valid.
        assertTrue(message.contains("Row TTL is not supported on materialized views"), message);
    }
    @Test
    public void testDropRowTtlRemovesEveryKey() throws Exception {
        alter("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"expire_at\", "
                + "\"" + CHECK_INTERVAL + "\" = \"600\", \"" + TIME_ZONE + "\" = \"UTC\")");

        alter("ALTER TABLE t DROP ROW TTL");

        // Removed outright rather than blanked, so the table is back to having no row TTL at all
        // and the DDL keeps no trace of it.
        Map<String, String> properties = table().getTableProperty().getProperties();
        assertFalse(properties.containsKey(EXPIRE_AT));
        assertFalse(properties.containsKey(CHECK_INTERVAL));
        assertFalse(properties.containsKey(TIME_ZONE));

        // No IF EXISTS: running it again on a table that has nothing is quiet, not an error.
        alter("ALTER TABLE t DROP ROW TTL");
    }

    @Test
    public void testDropRowTtlSurvivesReplay() throws Exception {
        alter("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"expire_at\", "
                + "\"" + CHECK_INTERVAL + "\" = \"600\")");

        // Capture what the leader writes, running the applier the way the real journal would.
        AtomicReference<ModifyTablePropertyOperationLog> written = new AtomicReference<>();
        EditLog realEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(realEditLog);
        doAnswer(invocation -> {
            written.set(invocation.getArgument(0));
            ((WALApplier) invocation.getArgument(1)).apply(null);
            return null;
        }).when(spyEditLog).logAlterTableProperties(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        try {
            alter("ALTER TABLE t DROP ROW TTL");
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(realEditLog);
        }

        ModifyTablePropertyOperationLog log = written.get();
        assertNotNull(log);
        assertTrue(log.getProperties().isEmpty());
        assertEquals(Set.of(EXPIRE_AT, CHECK_INTERVAL), log.getRemovedProperties());

        // A follower still carrying the keys has to end up without them after replaying that entry.
        table().getTableProperty().modifyTableProperties(
                Map.of(EXPIRE_AT, "expire_at", CHECK_INTERVAL, "600"));
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, log);
        assertFalse(table().getTableProperty().getProperties().containsKey(EXPIRE_AT));
        assertFalse(table().getTableProperty().getProperties().containsKey(CHECK_INTERVAL));
    }

    @Test
    public void testDropRowTtlOnAnIneligibleTable() throws Exception {
        createTable("CREATE TABLE dup(k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1");
        // A table that cannot carry row TTL is named as such rather than waved through as
        // "nothing to remove".
        assertTrue(rejectionOf("ALTER TABLE dup DROP ROW TTL").contains("primary key"));
    }

    @Test
    public void testTtlStaysUsableAsAnIdentifier() throws Exception {
        // TTL is a new lexer token, so it has to remain spellable as an ordinary name.
        createTable("CREATE TABLE ttl(k INT NOT NULL, ttl DATETIME) PRIMARY KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1");
        alter("ALTER TABLE ttl SET (\"" + EXPIRE_AT + "\" = \"ttl\")");
        assertEquals("ttl", ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(dbName, "ttl")).getProperties().get(EXPIRE_AT));
    }
    @Test
    public void testTimeColumnIsLocked() throws Exception {
        alter("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"expire_at + INTERVAL 7 DAY\")");

        assertTrue(rejectionOf("ALTER TABLE t DROP COLUMN expire_at").contains("dropped"));
        assertTrue(rejectionOf("ALTER TABLE t RENAME COLUMN expire_at TO renamed_at").contains("renamed"));
        assertTrue(rejectionOf("ALTER TABLE t MODIFY COLUMN expire_at DATE").contains("modified"));
        // The message has to name a way out, or the column looks permanently stuck.
        assertTrue(rejectionOf("ALTER TABLE t DROP COLUMN expire_at").contains("DROP ROW TTL"));

        // Every other column is untouched by the lock.
        alter("ALTER TABLE t DROP COLUMN created_at");

        // Repointing the expression moves the lock rather than adding a second one.
        alter("ALTER TABLE t SET (\"" + EXPIRE_AT + "\" = \"other_time\")");
        alter("ALTER TABLE t DROP COLUMN expire_at");
        assertTrue(rejectionOf("ALTER TABLE t DROP COLUMN other_time").contains("dropped"));

        // And dropping row TTL releases it.
        alter("ALTER TABLE t DROP ROW TTL");
        alter("ALTER TABLE t DROP COLUMN other_time");
    }
}
