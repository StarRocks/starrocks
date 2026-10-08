// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.type.PrimitiveType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InsertAnalyzerFilesSchemaPushDownTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable(
                "CREATE TABLE t_sink (x BIGINT, y VARCHAR(64)) " +
                "DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES('replication_num'='1')");
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
    public void testPushedDownStrictModeReachesAPreResolvedFilesTable() {
        // Reproduces the sequence the Sample-Based Tablet Pre-Split hook (and StatementPlanner's
        // lock-free pre-analysis for an INSERT that mixes FILES() with locked tables) puts the
        // statement through: FILES() is resolved BEFORE InsertAnalyzer#analyzeProperties pushes
        // strict_mode down into the relation's property map, and QueryAnalyzer#resolveTableRef then
        // reuses that instance instead of rebuilding it from the map. Without the re-apply step the
        // scan would run non-strict no matter what the statement asked for.
        String sql = "INSERT INTO t_sink PROPERTIES('strict_mode' = 'true') SELECT x, y FROM FILES(" +
                "  'path' = 'fake://bucket/dir/'," +
                "  'format' = 'parquet'," +
                "  'schema' = 'x BIGINT, y VARCHAR(64)')";
        ConnectContext context = starRocksAssert.getCtx();
        InsertStmt insertStmt = (InsertStmt) SqlParser.parseSingleStatement(
                sql, context.getSessionVariable().getSqlMode());

        new QueryAnalyzer(context).analyzeFilesOnly(insertStmt.getQueryStatement());
        SelectRelation selectRelation = (SelectRelation) insertStmt.getQueryStatement().getQueryRelation();
        FileTableFunctionRelation fileRelation = (FileTableFunctionRelation) selectRelation.getRelation();
        TableFunctionTable preResolved = (TableFunctionTable) fileRelation.getTable();
        assertFalse(preResolved.isStrictMode(),
                "the pre-resolution runs before the push-down, so strict_mode is not visible yet");

        Analyzer.analyze(insertStmt, context);

        assertSame(preResolved, fileRelation.getTable(),
                "the analyzer must reuse the pre-resolved FILES table rather than rebuild it");
        assertTrue(preResolved.isStrictMode(),
                "the pushed-down strict_mode must be re-applied to the reused FILES table");
    }
}
