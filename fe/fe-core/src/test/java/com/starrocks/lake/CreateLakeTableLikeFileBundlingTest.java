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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CREATE TABLE LIKE replays the DDL that SHOW CREATE TABLE prints. A shared-data table's file_bundling has to
 * survive that replay whichever way it is set and whatever the cluster default is.
 */
public class CreateLakeTableLikeFileBundlingTest {
    private static final String[] KEY_CLAUSES = {"PRIMARY KEY(k)", "DUPLICATE KEY(k)"};

    private static ConnectContext connectContext;

    private String dbName;
    private boolean savedEnableFileBundling;

    @BeforeAll
    public static void setUpClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @BeforeEach
    public void setUp() throws Exception {
        savedEnableFileBundling = Config.enable_file_bundling;
        dbName = "file_bundling_" + UUID.randomUUID().toString().replace("-", "");
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database " + dbName, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(dbName);
    }

    @AfterEach
    public void tearDown() {
        Config.enable_file_bundling = savedEnableFileBundling;
    }

    private OlapTable getTable(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbName, name);
    }

    private OlapTable createTable(String name, String sql) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
        return getTable(name);
    }

    private OlapTable createSource(String name, String keyClause, boolean fileBundling) throws Exception {
        return createTable(name, "CREATE TABLE " + name + " (k BIGINT NOT NULL, v BIGINT) " + keyClause
                + " DISTRIBUTED BY HASH(k) BUCKETS 2 PROPERTIES('replication_num' = '1', '"
                + PropertyAnalyzer.PROPERTIES_FILE_BUNDLING + "' = '" + fileBundling + "')");
    }

    private OlapTable createTableLike(String name, String sql) throws Exception {
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt.getCreateTableStmt());
        return getTable(name);
    }

    private static String ddlOf(OlapTable table) {
        List<String> ddl = Lists.newArrayList();
        AstToStringBuilder.getDdlStmt(table, ddl, null, null, false, true /* hide password */);
        return ddl.get(0);
    }

    private static String printed(boolean fileBundling) {
        return "\"" + PropertyAnalyzer.PROPERTIES_FILE_BUNDLING + "\" = \"" + fileBundling + "\"";
    }

    private void assertLikeKeepsSourceValue(boolean fileBundling) throws Exception {
        // The value a dropped property would fall back to is the opposite of the source's.
        Config.enable_file_bundling = !fileBundling;
        for (int i = 0; i < KEY_CLAUSES.length; ++i) {
            String source = "t_like_src_" + i;
            String copy = "t_like_copy_" + i;
            assertEquals(fileBundling, createSource(source, KEY_CLAUSES[i], fileBundling).isFileBundling());

            OlapTable copied = createTableLike(copy, "CREATE TABLE " + copy + " LIKE " + source);

            assertEquals(fileBundling, copied.isFileBundling(), KEY_CLAUSES[i]);
            String ddl = ddlOf(copied);
            assertTrue(ddl.contains(printed(fileBundling)), ddl);
        }
    }

    @Test
    public void testLikeKeepsFalseWhenTheClusterDefaultIsTrue() throws Exception {
        assertLikeKeepsSourceValue(false);
    }

    @Test
    public void testLikeKeepsTrueWhenTheClusterDefaultIsFalse() throws Exception {
        assertLikeKeepsSourceValue(true);
    }

    @Test
    public void testShowCreateTableRebuildsTheSameValue() throws Exception {
        for (boolean fileBundling : new boolean[] {false, true}) {
            Config.enable_file_bundling = !fileBundling;
            for (int i = 0; i < KEY_CLAUSES.length; ++i) {
                String source = "t_ddl_" + fileBundling + "_" + i;
                String rebuilt = source + "_rebuilt";
                String ddl = ddlOf(createSource(source, KEY_CLAUSES[i], fileBundling));
                assertTrue(ddl.contains(printed(fileBundling)), ddl);

                OlapTable table = createTable(rebuilt, ddl.replace("`" + source + "`", "`" + rebuilt + "`"));

                assertEquals(fileBundling, table.isFileBundling(), KEY_CLAUSES[i]);
            }
        }
    }

    @Test
    public void testLikePropertiesOverrideTheSource() throws Exception {
        for (boolean sourceValue : new boolean[] {false, true}) {
            // The copy asks for the opposite of the source, which is also the opposite of the cluster default.
            Config.enable_file_bundling = sourceValue;
            boolean requested = !sourceValue;
            for (int i = 0; i < KEY_CLAUSES.length; ++i) {
                String source = "t_override_" + sourceValue + "_" + i;
                String copy = source + "_copy";
                createSource(source, KEY_CLAUSES[i], sourceValue);

                OlapTable copied = createTableLike(copy, "CREATE TABLE " + copy + " PROPERTIES('"
                        + PropertyAnalyzer.PROPERTIES_FILE_BUNDLING + "' = '" + requested + "') LIKE " + source);

                assertEquals(requested, copied.isFileBundling(), KEY_CLAUSES[i]);
            }
        }
    }

    @Test
    public void testTableWithoutThePropertyIsCopiedAsFalse() throws Exception {
        Config.enable_file_bundling = true;
        OlapTable source = createSource("t_no_entry", KEY_CLAUSES[0], false);
        // A table created before the property existed has no entry for it, and reads as false.
        source.getTableProperty().getProperties().remove(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING);
        assertFalse(source.isFileBundling());
        String ddl = ddlOf(source);
        assertTrue(ddl.contains(printed(false)), ddl);

        OlapTable copied = createTableLike("t_no_entry_copy", "CREATE TABLE t_no_entry_copy LIKE t_no_entry");

        assertFalse(copied.isFileBundling());
    }
}
