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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.type.PrimitiveType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FilesSchemaPlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable(
                "CREATE TABLE t_sink (x BIGINT, y VARCHAR(64)) " +
                "DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    private static TableFunctionTable parseAndExtractFilesTable(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        FileTableFunctionRelation rel = findFileRelation(stmt);
        assertNotNull(rel, "FileTableFunctionRelation not found in parsed statement");
        return (TableFunctionTable) rel.getTable();
    }

    // Walk the statement to find the first FileTableFunctionRelation.
    private static FileTableFunctionRelation findFileRelation(StatementBase stmt) {
        QueryStatement qs;
        if (stmt instanceof InsertStmt) {
            qs = ((InsertStmt) stmt).getQueryStatement();
        } else if (stmt instanceof CreateTableAsSelectStmt) {
            qs = ((CreateTableAsSelectStmt) stmt).getInsertStmt().getQueryStatement();
        } else if (stmt instanceof QueryStatement) {
            qs = (QueryStatement) stmt;
        } else {
            return null;
        }
        if (qs.getQueryRelation() instanceof SelectRelation) {
            SelectRelation sr = (SelectRelation) qs.getQueryRelation();
            if (sr.getRelation() instanceof FileTableFunctionRelation) {
                return (FileTableFunctionRelation) sr.getRelation();
            }
        }
        return null;
    }

    @Test
    public void testSelectFromFilesWithExplicitSchema() throws Exception {
        String sql = "SELECT * FROM FILES(" +
                "  'path' = 'fake://bucket/dir/'," +
                "  'format' = 'parquet'," +
                "  'schema' = 'x BIGINT, y VARCHAR(64)')";
        TableFunctionTable t = parseAndExtractFilesTable(sql);
        assertEquals(2, t.getFullSchema().size());
        Column x = t.getColumn("x");
        Column y = t.getColumn("y");
        assertNotNull(x);
        assertNotNull(y);
        assertEquals(PrimitiveType.BIGINT, x.getType().getPrimitiveType());
        assertEquals(PrimitiveType.VARCHAR, y.getType().getPrimitiveType());
    }

    @Test
    public void testInsertFromFilesWithExplicitSchema() throws Exception {
        String sql = "INSERT INTO t_sink SELECT x, y FROM FILES(" +
                "  'path' = 'fake://bucket/dir/'," +
                "  'format' = 'parquet'," +
                "  'schema' = 'x BIGINT, y VARCHAR(64)')";
        TableFunctionTable t = parseAndExtractFilesTable(sql);
        assertEquals(2, t.getFullSchema().size());
        assertNotNull(t.getColumn("x"));
        assertEquals(PrimitiveType.BIGINT, t.getColumn("x").getType().getPrimitiveType());
        assertNotNull(t.getColumn("y"));
        assertEquals(PrimitiveType.VARCHAR, t.getColumn("y").getType().getPrimitiveType());
    }

    @Test
    public void testCtasFromFilesWithExplicitSchema() throws Exception {
        String sql = "CREATE TABLE ctas_target " +
                "PROPERTIES('replication_num'='1') " +
                "AS SELECT * FROM FILES(" +
                "  'path' = 'fake://bucket/dir/'," +
                "  'format' = 'parquet'," +
                "  'schema' = 'a BIGINT, b VARCHAR(32)')";
        TableFunctionTable t = parseAndExtractFilesTable(sql);
        assertEquals(2, t.getFullSchema().size());
        assertEquals("a", t.getFullSchema().get(0).getName());
        assertEquals("b", t.getFullSchema().get(1).getName());
        assertEquals(PrimitiveType.BIGINT, t.getColumn("a").getType().getPrimitiveType());
        assertEquals(PrimitiveType.VARCHAR, t.getColumn("b").getType().getPrimitiveType());
    }
}
