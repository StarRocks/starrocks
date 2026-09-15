// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InsertAnalyzerFilesSchemaPushDownTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable(
                "CREATE TABLE t_sink (x BIGINT, y VARCHAR(64)) " +
                "DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable(
                "CREATE TABLE t_files_str (col_int INT, col_string STRING) " +
                "DISTRIBUTED BY HASH(col_int) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable(
                "CREATE TABLE t_files_tiny (col_int TINYINT, col_string STRING) " +
                "DISTRIBUTED BY HASH(col_int) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @Test
    public void testConfigPushDownSilentlySkippedWithExplicitSchema() throws Exception {
        String sql = "INSERT INTO t_sink SELECT x, y FROM FILES(" +
                "  'path' = 'fake://bucket/dir/'," +
                "  'format' = 'parquet'," +
                "  'schema' = 'x TINYINT, y VARCHAR(64)')";
        // Analysis runs the pushDownSchemaFunc via the FileTableFunctionRelation wiring.
        // If the gate works, the explicit schema (TINYINT) survives; if not, the config-level
        // type push-down would overwrite x with BIGINT (target column type).
        InsertStmt insertStmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        SelectRelation selectRelation = (SelectRelation) insertStmt.getQueryStatement().getQueryRelation();
        FileTableFunctionRelation fileRelation = (FileTableFunctionRelation) selectRelation.getRelation();
        TableFunctionTable fileTable = (TableFunctionTable) fileRelation.getTable();

        assertTrue(fileTable.hasExplicitSchema(), "FILES() should have explicit schema");
        assertEquals(PrimitiveType.TINYINT,
                fileTable.getColumn("x").getType().getPrimitiveType(),
                "Column 'x' must remain TINYINT (schema-declared); push-down would have made it BIGINT");
    }

    @Test
    public void testInsertPushDownSchemaPropertyConflicts() {
        String sql = "INSERT INTO t_sink PROPERTIES('enable_push_down_schema' = 'true') " +
                "SELECT x, y FROM FILES(" +
                "  'path' = 'fake://bucket/dir/'," +
                "  'format' = 'parquet'," +
                "  'schema' = 'x TINYINT, y VARCHAR(64)')";
        // Analyzer runs during parseStmtWithNewParser — that is where the gate fires.
        // UtFrameUtils wraps the thrown SemanticException in AnalysisException (see Task 8).
        AnalysisException e = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx()));
        assertInstanceOf(SemanticException.class, e.getCause());
        assertTrue(e.getMessage().contains("'enable_push_down_schema'")
                && e.getMessage().contains("'schema'"));
    }

    @Test
    public void testInsertPushDownSchemaPropertyConflictsThroughSubquery() {
        // Regression for the shape-dependent bypass: FILES() reached through a subquery
        // (or CTE / join) must still trigger the conflict. The check now walks all
        // FileTableFunctionRelation instances under the InsertStmt rather than only
        // looking at the top-level fromRelation.
        String sql = "INSERT INTO t_sink PROPERTIES('enable_push_down_schema' = 'true') " +
                "SELECT x, y FROM (SELECT x, y FROM FILES(" +
                "  'path' = 'fake://bucket/dir/'," +
                "  'format' = 'parquet'," +
                "  'schema' = 'x TINYINT, y VARCHAR(64)')) t";
        AnalysisException e = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx()));
        assertInstanceOf(SemanticException.class, e.getCause());
        assertTrue(e.getMessage().contains("'enable_push_down_schema'")
                && e.getMessage().contains("'schema'"));
    }

    @Test
    public void testInsertPushDownSchemaPropertyConflictsThroughCte() {
        // Same shape-independent guarantee for CTE-wrapped FILES().
        String sql = "INSERT INTO t_sink PROPERTIES('enable_push_down_schema' = 'true') " +
                "WITH cte AS (SELECT x, y FROM FILES(" +
                "  'path' = 'fake://bucket/dir/'," +
                "  'format' = 'parquet'," +
                "  'schema' = 'x TINYINT, y VARCHAR(64)')) " +
                "SELECT x, y FROM cte";
        AnalysisException e = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx()));
        assertInstanceOf(SemanticException.class, e.getCause());
        assertTrue(e.getMessage().contains("'enable_push_down_schema'")
                && e.getMessage().contains("'schema'"));
    }

    @Test
    public void testInsertPushDownDoesNotShrinkInferredVarcharLength() throws Exception {
        // fake:// infers col_string as wildcard VARCHAR; STRING on the sink is VARCHAR(65533).
        // Push-down must not shrink the FILES() slot to 65533 (issue #78208).
        String sql = "INSERT INTO t_files_str SELECT * FROM FILES(" +
                "  'path' = 'fake://bucket/wide.csv'," +
                "  'format' = 'csv')";
        InsertStmt insertStmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        SelectRelation selectRelation = (SelectRelation) insertStmt.getQueryStatement().getQueryRelation();
        FileTableFunctionRelation fileRelation = (FileTableFunctionRelation) selectRelation.getRelation();
        TableFunctionTable fileTable = (TableFunctionTable) fileRelation.getTable();

        ScalarType fileStringType = (ScalarType) fileTable.getColumn("col_string").getType();
        assertEquals(PrimitiveType.VARCHAR, fileStringType.getPrimitiveType());
        assertEquals(StringType.MAX_STRING_LENGTH, fileStringType.getLength(),
                "FILES() varchar length must stay the inferred width, not the STRING sink's 65533");
    }

    @Test
    public void testInsertPushDownStillNarrowsIntegerTypes() throws Exception {
        String sql = "INSERT INTO t_files_tiny SELECT * FROM FILES(" +
                "  'path' = 'fake://bucket/wide.csv'," +
                "  'format' = 'csv')";
        InsertStmt insertStmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        SelectRelation selectRelation = (SelectRelation) insertStmt.getQueryStatement().getQueryRelation();
        FileTableFunctionRelation fileRelation = (FileTableFunctionRelation) selectRelation.getRelation();
        TableFunctionTable fileTable = (TableFunctionTable) fileRelation.getTable();

        assertEquals(PrimitiveType.TINYINT,
                fileTable.getColumn("col_int").getType().getPrimitiveType(),
                "Integer type push-down must still rewrite inferred INT to TINYINT");
        ScalarType fileStringType = (ScalarType) fileTable.getColumn("col_string").getType();
        assertEquals(StringType.MAX_STRING_LENGTH, fileStringType.getLength());
    }
}
