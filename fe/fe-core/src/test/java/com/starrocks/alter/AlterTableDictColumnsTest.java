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
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

// End-to-end coverage for the ALTER TABLE ... DISABLE/ENABLE DICTIONARY entry point, exercising the full
// path: grammar -> AstBuilder (AlterTableDictColumnsClause) -> AlterTableClauseAnalyzer -> AlterJobExecutor
// -> LocalMetastore.updateNoDictColumns, plus the SHOW CREATE TABLE rendering of no_dict_columns.
public class AlterTableDictColumnsTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_dict").useDatabase("test_dict")
                .withTable("CREATE TABLE test_dict.t (\n" +
                        "  id bigint,\n" +
                        "  c1 varchar(64),\n" +
                        "  c2 varchar(64),\n" +
                        "  c3 varchar(64)\n" +
                        ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3 " +
                        "PROPERTIES('replication_num' = '1');");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static OlapTable getTable() {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("test_dict").getTable("t");
    }

    @Test
    public void testDisableThenEnableDictionary() throws Exception {
        // DISABLE two columns.
        starRocksAssert.alterTable("ALTER TABLE test_dict.t DISABLE DICTIONARY (c1, c2)");
        OlapTable table = getTable();
        Assertions.assertEquals(Set.of("c1", "c2"), table.getNoDictColumns());
        Assertions.assertTrue(table.isNoDictColumn("c1"));

        // no_dict_columns is an auto-managed guard property and is intentionally NOT rendered in
        // SHOW CREATE TABLE (it would not round-trip through CREATE); it is surfaced via the
        // GET /api/global_dict/table/no_dict_columns endpoint instead.
        String ddl = starRocksAssert.showCreateTable("SHOW CREATE TABLE test_dict.t");
        Assertions.assertFalse(ddl.contains("no_dict_columns"),
                "SHOW CREATE TABLE must not render no_dict_columns, got:\n" + ddl);

        // DISABLE a third column: union semantics, existing ones preserved.
        starRocksAssert.alterTable("ALTER TABLE test_dict.t DISABLE DICTIONARY (c3)");
        Assertions.assertEquals(Set.of("c1", "c2", "c3"), getTable().getNoDictColumns());

        // ENABLE one column: only that one is removed.
        starRocksAssert.alterTable("ALTER TABLE test_dict.t ENABLE DICTIONARY (c1)");
        Assertions.assertEquals(Set.of("c2", "c3"), getTable().getNoDictColumns());
        Assertions.assertFalse(getTable().isNoDictColumn("c1"));

        // ENABLE the rest: set becomes empty and no_dict_columns disappears from SHOW CREATE.
        starRocksAssert.alterTable("ALTER TABLE test_dict.t ENABLE DICTIONARY (c2, c3)");
        Assertions.assertTrue(getTable().getNoDictColumns().isEmpty());
        String ddlAfter = starRocksAssert.showCreateTable("SHOW CREATE TABLE test_dict.t");
        Assertions.assertFalse(ddlAfter.contains("no_dict_columns"),
                "no_dict_columns should not be rendered once the set is empty, got:\n" + ddlAfter);
    }

    @Test
    public void testDisableRejectsNonStringColumn() {
        // id is bigint -> analyzer must reject.
        Assertions.assertThrows(Exception.class,
                () -> starRocksAssert.alterTable("ALTER TABLE test_dict.t DISABLE DICTIONARY (id)"));
    }

    @Test
    public void testDisableRejectsUnknownColumn() {
        Assertions.assertThrows(Exception.class,
                () -> starRocksAssert.alterTable("ALTER TABLE test_dict.t DISABLE DICTIONARY (not_a_col)"));
    }

    // codex #2: DISABLE/ENABLE DICTIONARY must be case-insensitive. The persisted set stores the canonical
    // column name so isNoDictColumn (canonical id) sees it, and a differently cased ENABLE fully clears it.
    @Test
    public void testDisableEnableDictionaryIsCaseInsensitive() throws Exception {
        starRocksAssert.alterTable("ALTER TABLE test_dict.t DISABLE DICTIONARY (C3)");
        OlapTable table = getTable();
        Assertions.assertTrue(table.getNoDictColumns().contains("c3"),
                "persisted set must use the canonical column name, got: " + table.getNoDictColumns());
        Assertions.assertTrue(table.isNoDictColumn("c3"));

        starRocksAssert.alterTable("ALTER TABLE test_dict.t ENABLE DICTIONARY (c3)");
        Assertions.assertFalse(getTable().isNoDictColumn("c3"));
        Assertions.assertTrue(getTable().getNoDictColumns().isEmpty());
    }
}
