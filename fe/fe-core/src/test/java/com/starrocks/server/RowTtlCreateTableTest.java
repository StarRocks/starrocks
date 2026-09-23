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

package com.starrocks.server;

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that CREATE TABLE accepts, rejects and prints back the row TTL properties, on the only kind
 * of table that may carry them: a shared-data primary key table.
 */
public class RowTtlCreateTableTest {
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
        dbName = "row_ttl_" + UUID.randomUUID().toString().replace("-", "");
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database " + dbName, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(dbName);
    }

    @AfterEach
    public void tearDown() {
        Config.enable_row_ttl = savedEnableRowTtl;
    }

    private OlapTable createTable(String sql) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(dbName, stmt.getTableName());
    }

    private OlapTable createTableLike(String sql) throws Exception {
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTableLike(stmt);
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(dbName, stmt.getTableName());
    }

    private static String ddlOf(OlapTable table) {
        List<String> ddl = Lists.newArrayList();
        AstToStringBuilder.getDdlStmt(table, ddl, null, null, false, false);
        return ddl.get(0);
    }

    private static String primaryKeyTable(String name, String properties) {
        return "CREATE TABLE " + name + "(k INT NOT NULL, expire_at DATETIME, created_at BIGINT) "
                + "PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1"
                + (properties.isEmpty() ? "" : " PROPERTIES(" + properties + ")");
    }

    private static String property(String key, String value) {
        return "\"" + key + "\" = \"" + value + "\"";
    }

    @Test
    public void testCreateTableCarriesAndPrintsRowTtl() throws Exception {
        OlapTable all = createTable(primaryKeyTable("t_all",
                String.join(", ", property(EXPIRE_AT, "expire_at + INTERVAL 7 DAY"),
                        property(CHECK_INTERVAL, "3600"), property(TIME_ZONE, "UTC"))));
        // Stored exactly as written, so the printed DDL replays into the same table.
        assertEquals("expire_at + INTERVAL 7 DAY", all.getProperties().get(EXPIRE_AT));
        assertEquals("3600", all.getProperties().get(CHECK_INTERVAL));
        assertEquals("UTC", all.getProperties().get(TIME_ZONE));
        String ddl = ddlOf(all);
        assertTrue(ddl.contains(EXPIRE_AT), ddl);
        assertTrue(ddl.contains(CHECK_INTERVAL), ddl);
        assertTrue(ddl.contains(TIME_ZONE), ddl);

        // Only the keys the table actually set are printed.
        OlapTable expireOnly = createTable(primaryKeyTable("t_expire_only",
                property(EXPIRE_AT, "to_datetime(created_at, 3)")));
        String expireOnlyDdl = ddlOf(expireOnly);
        assertTrue(expireOnlyDdl.contains(EXPIRE_AT), expireOnlyDdl);
        assertFalse(expireOnlyDdl.contains(CHECK_INTERVAL), expireOnlyDdl);
        assertFalse(expireOnlyDdl.contains(TIME_ZONE), expireOnlyDdl);

        // A table without row TTL shows no trace of it.
        String plainDdl = ddlOf(createTable(primaryKeyTable("t_plain", "")));
        assertFalse(plainDdl.contains("row_ttl"), plainDdl);
    }

    private String rejectionOf(String sql) {
        Exception e = assertThrows(Exception.class, () -> createTable(sql));
        return e.getMessage() == null ? "" : e.getMessage();
    }

    @Test
    public void testCreateTableRejectsIneligibleTableAndBadValues() {
        assertTrue(rejectionOf("CREATE TABLE t_dup(k INT, expire_at DATETIME) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(" + property(EXPIRE_AT, "expire_at") + ")")
                .contains("primary key"));
        assertTrue(rejectionOf(primaryKeyTable("t_empty", property(EXPIRE_AT, ""))).contains("empty"));
        assertTrue(rejectionOf(primaryKeyTable("t_interval_only", property(CHECK_INTERVAL, "3600")))
                .contains(EXPIRE_AT));
        assertTrue(rejectionOf(primaryKeyTable("t_bad_column", property(EXPIRE_AT, "no_such_column")))
                .contains("does not exist"));
        String bareBigint = rejectionOf(primaryKeyTable("t_bare_bigint", property(EXPIRE_AT, "created_at")));
        assertTrue(bareBigint.contains("to_datetime"), bareBigint);
        assertTrue(bareBigint.contains("Unix timestamp"), bareBigint);

        // An engine other than OLAP keeps the properties it recognises and drops the rest, so the
        // rejection has to happen before the statement reaches that engine's factory.
        String mysqlEngine = rejectionOf("CREATE TABLE t_mysql(k INT, expire_at DATETIME) ENGINE=mysql "
                + "PROPERTIES(\"host\" = \"127.0.0.1\", \"port\" = \"3306\", \"user\" = \"u\", "
                + "\"password\" = \"p\", \"database\" = \"d\", \"table\" = \"t\", "
                + property(EXPIRE_AT, "expire_at") + ")");
        assertTrue(mysqlEngine.contains("not supported on mysql tables"), mysqlEngine);
    }

    @Test
    public void testCreateTableSkipsRowTtlWhileReplayingQueryDump() throws Exception {
        // A dump carries the DDL of a table from another cluster, so this cluster's switch says
        // nothing about it and the replay has to get as far as planning.
        Config.enable_row_ttl = false;
        boolean savedReplay = FeConstants.isReplayFromQueryDump;
        FeConstants.isReplayFromQueryDump = true;
        try {
            OlapTable table = createTable(primaryKeyTable("t_dump", property(EXPIRE_AT, "expire_at")));
            String ddl = ddlOf(table);
            assertFalse(ddl.contains("row_ttl"), ddl);
        } finally {
            FeConstants.isReplayFromQueryDump = savedReplay;
        }
    }

    @Test
    public void testCreateTableIsGatedByEnableRowTtl() {
        Config.enable_row_ttl = false;
        // A brand new table is by definition a table that does not have row TTL yet, so it faces
        // the same admission check as enabling row TTL on an existing table.
        assertTrue(rejectionOf(primaryKeyTable("t_gated", property(EXPIRE_AT, "expire_at")))
                .contains("enable_row_ttl"));
    }

    @Test
    public void testCreateTableLikeDoesNotInheritRowTtl() throws Exception {
        createTable(primaryKeyTable("t_source",
                String.join(", ", property(EXPIRE_AT, "expire_at"), property(CHECK_INTERVAL, "600"))));

        OlapTable copy = createTableLike("CREATE TABLE t_copy LIKE t_source");
        String copyDdl = ddlOf(copy);
        assertFalse(copyDdl.contains("row_ttl"), copyDdl);

        // Only the silent inheritance is dropped: a copy that asks for row TTL still gets it.
        OlapTable explicit = createTableLike("CREATE TABLE t_copy_explicit PROPERTIES("
                + property(EXPIRE_AT, "expire_at + INTERVAL 1 DAY") + ") LIKE t_source");
        assertEquals("expire_at + INTERVAL 1 DAY", explicit.getProperties().get(EXPIRE_AT));
    }
}
