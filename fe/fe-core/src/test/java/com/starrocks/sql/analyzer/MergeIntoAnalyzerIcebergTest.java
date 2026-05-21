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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

import static com.starrocks.sql.plan.ConnectorPlanTestBase.newFolder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeIntoAnalyzerIcebergTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());
    }

    private static MergeIntoStmt parseMerge(String sql) {
        return (MergeIntoStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);
    }

    private static JoinRelation analyzeAndGetSyntheticJoin(String sql) {
        MergeIntoStmt stmt = parseMerge(sql);
        MergeIntoAnalyzer.analyze(stmt, connectContext);
        SelectRelation selectRelation = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        return (JoinRelation) selectRelation.getRelation();
    }

    // ---- Error cases ----

    @Test
    public void testMergeIntoNonV2Table() {
        // t0 is format version 3, so MERGE INTO should be rejected
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0 AS t " +
                "USING (SELECT 1 AS id, 'x' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(exception.getMessage().contains("V2"),
                "Error should mention V2: " + exception.getMessage());
    }

    @Test
    public void testMergeIntoUpdatePartitionColumn() {
        // t1_v2 is partitioned by 'date'; updating it should be rejected
        String sql = "MERGE INTO iceberg0.partitioned_db.t1_v2 AS t " +
                "USING (SELECT 1 AS id, '2024-02-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET date = s.date";
        MergeIntoStmt stmt = parseMerge(sql);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(exception.getMessage().contains("partition column"),
                "Error should mention partition column: " + exception.getMessage());
    }

    @Test
    public void testMergeIntoUpdateHiddenColumn() {
        // Updating the hidden _file column should be rejected
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'path' AS _file) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET _file = s._file";
        MergeIntoStmt stmt = parseMerge(sql);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(exception.getMessage().contains("metadata column") || exception.getMessage().contains("_file"),
                "Error should mention metadata column or _file: " + exception.getMessage());
    }

    @Test
    public void testMergeUpdateLastAssignmentWinsOverridesDefault() {
        // last-assignment-wins (matches StarRocks native OLAP UPDATE and MySQL's
        // left-to-right rule, documented on getMatchedColumnValue): "SET data =
        // DEFAULT, data = s.data" is valid because the surviving final
        // assignment is `s.data`. Iceberg V2's DEFAULT rejection must only fire
        // on the surviving expr, not on overwritten earlier ones.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'x' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = DEFAULT, data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);
        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertNotNull(stmt.getQueryStatement());
    }

    @Test
    public void testMergeRejectsDefaultInUpdate() {
        // Iceberg V2 has no supported column defaults; ExpressionAnalyzer types
        // DefaultValueExpr as varchar so the placeholder otherwise rides into the
        // sink as a wrongly-typed value. Reject up front, matching UpdateAnalyzer.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'x' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = DEFAULT";
        MergeIntoStmt stmt = parseMerge(sql);
        SemanticException ex = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(ex.getMessage().contains("DEFAULT"),
                "Error should mention DEFAULT: " + ex.getMessage());
    }

    @Test
    public void testSingleConditionalMatchedGatesValueWithCase() {
        // For a single conditional MATCHED clause, the data SELECT must gate the
        // update value on the clause condition (so rows that fail the AND
        // predicate do not evaluate the update expression even though they reach
        // the join). The same wrapping also surfaces source columns referenced
        // in the condition to ColumnPrivilege via the rewritten queryStatement.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date, 1 AS flag) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.flag = 1 THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);
        MergeIntoAnalyzer.analyze(stmt, connectContext);

        // Without the fix, `s.flag` lives only in routingExpr; the data SELECT
        // emits `s.data` unconditionally. With the fix, `s.flag` shows up in the
        // data column's SELECT expression because the value is wrapped in a CASE.
        boolean flagInSelectList = stmt.getQueryStatement().getQueryRelation()
                .getOutputExpression().stream()
                .anyMatch(e -> exprReferencesColumn(e, "flag"));
        assertTrue(flagInSelectList,
                "Single conditional MATCHED must gate the update value with a CASE on the condition");
    }

    private static boolean exprReferencesColumn(Expr expr, String columnName) {
        if (expr instanceof SlotRef slot && columnName.equalsIgnoreCase(slot.getColumnName())) {
            return true;
        }
        for (Expr child : expr.getChildren()) {
            if (exprReferencesColumn(child, columnName)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testSingleConditionalNotMatchedGatesValueWithCase() {
        // Symmetric to the matched-side check above: a single conditional NOT
        // MATCHED clause must gate the insert value on the clause condition so
        // it does not evaluate on rows routed to NO_OP, and the condition column
        // must enter the data SELECT for ColumnPrivilege.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 2 AS id, 'new' AS data, '2024-01-01' AS date, 1 AS flag) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED AND s.flag = 1 THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        MergeIntoStmt stmt = parseMerge(sql);
        MergeIntoAnalyzer.analyze(stmt, connectContext);

        boolean flagInSelectList = stmt.getQueryStatement().getQueryRelation()
                .getOutputExpression().stream()
                .anyMatch(e -> exprReferencesColumn(e, "flag"));
        assertTrue(flagInSelectList,
                "Single conditional NOT MATCHED must gate the insert value with a CASE on the condition");
    }

    @Test
    public void testMergeIntoNoTargetAliasAnalyzes() {
        // Target without an alias must use a fully-qualified TableName for its
        // generated SlotRefs so the analyzer does not bind them to a same-named
        // source table living in a different db/catalog. Happy-path: ensure the
        // unaliased form still analyzes cleanly.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON iceberg0.unpartitioned_db.t0_v2.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);
        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertNotNull(stmt.getQueryStatement());
    }

    @Test
    public void testMergeIntoInsertHiddenColumn() {
        // INSERT (_file, id, data, date) VALUES (...) — listing the hidden _file
        // metadata column must be rejected, mirroring the UPDATE-side check above.
        // Without this validation getNotMatchedColumnValue silently drops the value
        // because dataColumns filters out hidden columns.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'p' AS f, 'd' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (_file, id, data, date) " +
                "VALUES (s.f, s.id, s.data, s.date)";
        MergeIntoStmt stmt = parseMerge(sql);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(exception.getMessage().contains("metadata column") || exception.getMessage().contains("_file"),
                "Error should mention metadata column or _file: " + exception.getMessage());
    }

    @Test
    public void testMergeIntoInsertColumnCountMismatch() {
        // INSERT (id, data) VALUES (1) — 2 columns but 1 value
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'x' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id)";
        MergeIntoStmt stmt = parseMerge(sql);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(exception.getMessage().contains("column count") || exception.getMessage().contains("count"),
                "Error should mention column count mismatch: " + exception.getMessage());
    }

    @Test
    public void testMergeIntoInsertDuplicateColumnRejected() {
        // WHEN NOT MATCHED THEN INSERT (id, data, data) VALUES (...) — 'data'
        // listed twice. Must reject with ERR_DUP_FIELDNAME to match plain
        // INSERT (InsertAnalyzer rejects the same pattern at line ~287).
        // Without this check the emission layer silently kept the FIRST
        // value for the duplicated column.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'x' AS data) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, data, data) VALUES (s.id, 'a', 'b')";
        MergeIntoStmt stmt = parseMerge(sql);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(exception.getMessage().toLowerCase().contains("duplicate")
                        || exception.getMessage().toLowerCase().contains("column name"),
                "Error should mention duplicate column name: " + exception.getMessage());
    }

    @Test
    public void testMergeIntoRejectsNonLastUnconditionalMatchedClause() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = 'updated' " +
                "WHEN MATCHED AND s.data = 'delete' THEN DELETE";
        MergeIntoStmt stmt = parseMerge(sql);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(exception.getMessage().contains("Unconditional WHEN MATCHED clause must be the last"),
                "Error should mention non-last unconditional MATCHED clause: " + exception.getMessage());
    }

    @Test
    public void testMergeIntoRejectsNonLastUnconditionalNotMatchedClause() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date) " +
                "WHEN NOT MATCHED AND s.data = 'special' " +
                "    THEN INSERT (id, data, date) VALUES (s.id, 'special', s.date)";
        MergeIntoStmt stmt = parseMerge(sql);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(exception.getMessage().contains("Unconditional WHEN NOT MATCHED clause must be the last"),
                "Error should mention non-last unconditional NOT MATCHED clause: " + exception.getMessage());
    }

    @Test
    public void testMergeInsertStarRequiresAliasedSourceRelation() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 2 AS id, 'new' AS data, '2024-01-01' AS date) " +
                "ON t.id = id " +
                "WHEN NOT MATCHED THEN INSERT *";
        MergeIntoStmt stmt = parseMerge(sql);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(exception.getMessage().contains("INSERT * requires an aliased source relation"),
                "Error should mention aliased source requirement: " + exception.getMessage());
    }

    // ---- Success cases ----

    @Test
    public void testBasicMergeMatchedUpdate() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertTrue(stmt.getTable() instanceof IcebergTable);
        assertNotNull(stmt.getQueryStatement());
        assertNotNull(stmt.getOutputColumnNames());
    }

    @Test
    public void testBasicMergeMatchedDelete() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN DELETE";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertTrue(stmt.getTable() instanceof IcebergTable);
        assertNotNull(stmt.getQueryStatement());
    }

    @Test
    public void testBasicMergeNotMatchedInsert() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 2 AS id, 'inserted' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertTrue(stmt.getTable() instanceof IcebergTable);
        assertNotNull(stmt.getQueryStatement());
    }

    @Test
    public void testMergeAllClauses() {
        // All three clause types: MATCHED UPDATE, MATCHED DELETE, NOT MATCHED INSERT
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.data = 'delete' THEN DELETE " +
                "WHEN MATCHED THEN UPDATE SET data = s.data " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertNotNull(stmt.getQueryStatement());
        // _file, _pos, id, data, date = 5 user columns. The op_code routing column is
        // sink-private and injected by the planner — it must not appear here.
        List<String> colNames = stmt.getOutputColumnNames();
        assertNotNull(colNames);
        assertEquals(5, colNames.size());
        assertEquals(IcebergTable.FILE_PATH, colNames.get(0));
        assertEquals(IcebergTable.ROW_POSITION, colNames.get(1));
        assertFalse(colNames.contains("op_code"), "op_code must not appear in user-visible output");
        assertNotNull(stmt.getRoutingExpr(), "analyzer must hand the planner a routing expression");
    }

    @Test
    public void testMergeMultipleMatchedClauses() {
        // Two WHEN MATCHED clauses with different conditions
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND t.data = 'old' THEN UPDATE SET data = 'updated' " +
                "WHEN MATCHED THEN DELETE";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertNotNull(stmt.getQueryStatement());
        assertEquals(2, stmt.getWhenClauses().size());
    }

    @Test
    public void testMergeMultipleNotMatchedClauses() {
        // Two WHEN NOT MATCHED clauses with conditions
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED AND s.data = 'special' " +
                "    THEN INSERT (id, data, date) VALUES (s.id, 'special', s.date) " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertNotNull(stmt.getQueryStatement());
    }

    @Test
    public void testMergeInsertStar() {
        // WHEN NOT MATCHED THEN INSERT * — star insert
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 2 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT *";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertNotNull(stmt.getQueryStatement());
        assertNotNull(stmt.getOutputColumnNames());
    }

    @Test
    public void testMergeInsertStarSourceWithDifferentColumnNames() {
        // INSERT * resolves source columns by NAME. If none of the source
        // aliases match the target column names, every target column's
        // SlotRef fails to resolve and analysis errors out.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS x, 'a' AS y, '2024-01-01' AS z) AS s " +
                "ON t.id = s.x " +
                "WHEN NOT MATCHED THEN INSERT *";
        MergeIntoStmt stmt = parseMerge(sql);

        Exception ex = assertThrows(Exception.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
        assertTrue(msg.contains("column") || msg.contains("resolve") || msg.contains("unknown"),
                "Error should mention unresolved column: " + ex.getMessage());
    }

    @Test
    public void testMergeInsertStarSourceMissingTargetColumn() {
        // Source missing one target column ('date'). INSERT * still expands
        // SlotRef(s, "date") for the target column; resolution fails.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'a' AS data) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT *";
        MergeIntoStmt stmt = parseMerge(sql);

        Exception ex = assertThrows(Exception.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
        assertTrue(msg.contains("date") || msg.contains("column") || msg.contains("resolve"),
                "Error should mention missing column: " + ex.getMessage());
    }

    @Test
    public void testMergeInsertStarSourceWithExtraColumns() {
        // Source has columns beyond the target schema ('bonus'). By-name
        // resolution simply ignores the extras — analysis must succeed.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'a' AS data, '2024-01-01' AS date, 'extra' AS bonus) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT *";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertNotNull(stmt.getQueryStatement());
    }

    @Test
    public void testMergeSourceSubquery() {
        // Source is a subquery with an alias
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT id, data, date FROM iceberg0.unpartitioned_db.t0_v2 WHERE id > 100) AS src " +
                "ON t.id = src.id " +
                "WHEN MATCHED THEN UPDATE SET data = src.data";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertTrue(stmt.getTable() instanceof IcebergTable);
        assertNotNull(stmt.getQueryStatement());
        // _file, _pos, id, data, date = 5 user columns (op_code is sink-private).
        List<String> colNames = stmt.getOutputColumnNames();
        assertNotNull(colNames);
        assertEquals(5, colNames.size());
        assertNotNull(stmt.getRoutingExpr());
    }

    @Test
    public void testMergeSourceMergeLevelAlias() {
        // Parenthesizing a non-subquery relation forces ANTLR to route the trailing
        // identifier to the mergeIntoStatement-level sourceAlias slot rather than the
        // relation's own alias slot. The AstBuilder must apply that alias back onto
        // the source relation so that "s.*" in ON/WHEN resolves during QueryAnalyzer.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (iceberg0.unpartitioned_db.t0_v2) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertNotNull(stmt.getQueryStatement());
    }

    @Test
    public void testMergeDerivesTargetPredicateFromSourceSubquery() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT id, data, date FROM iceberg0.unpartitioned_db.t0_v2 WHERE id % 20 = 0) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        MergeIntoAnalyzer.analyze(stmt, connectContext);

        SelectRelation selectRelation = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        JoinRelation joinRelation = (JoinRelation) selectRelation.getRelation();
        String onPredicate = ExprToSql.toSql(joinRelation.getOnPredicate()).toLowerCase();
        assertTrue(onPredicate.contains("t.`id` % 20 = 0")
                        || onPredicate.contains("t.id % 20 = 0")
                        || onPredicate.contains("`t`.`id` % 20 = 0"),
                "target-side predicate should be derived from source predicate: " + onPredicate);
    }

    @Test
    public void testMergeDoesNotDeriveTargetPredicateFromAliasedSourceProjection() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT id + 1 AS id, data, date FROM iceberg0.unpartitioned_db.t0_v2 WHERE id % 20 = 0) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        MergeIntoAnalyzer.analyze(stmt, connectContext);

        SelectRelation selectRelation = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        JoinRelation joinRelation = (JoinRelation) selectRelation.getRelation();
        String onPredicate = ExprToSql.toSql(joinRelation.getOnPredicate()).toLowerCase();
        assertFalse(onPredicate.contains("t.`id` % 20 = 0")
                        || onPredicate.contains("t.id % 20 = 0")
                        || onPredicate.contains("`t`.`id` % 20 = 0"),
                "target-side predicate should not be derived from a non-slot source projection: " + onPredicate);
    }

    @Test
    public void testMergeColumnOutputNamesOrder() {
        // User-visible output layout: _file, _pos, data_cols... — op_code is sink-private
        // and added by the planner.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        MergeIntoAnalyzer.analyze(stmt, connectContext);

        List<String> colNames = stmt.getOutputColumnNames();
        assertNotNull(colNames);
        assertEquals(5, colNames.size());
        assertEquals(IcebergTable.FILE_PATH, colNames.get(0));
        assertEquals(IcebergTable.ROW_POSITION, colNames.get(1));
        assertEquals("id", colNames.get(2));
        assertEquals("data", colNames.get(3));
        assertEquals("date", colNames.get(4));
        assertFalse(colNames.contains("op_code"));
        assertNotNull(stmt.getRoutingExpr());
    }

    @Test
    public void testMergePartitionedTableSuccess() {
        // t1_v2 is partitioned by 'date'; updating non-partition column should succeed
        String sql = "MERGE INTO iceberg0.partitioned_db.t1_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertTrue(stmt.getTable() instanceof IcebergTable);
        assertNotNull(stmt.getQueryStatement());
        assertNotNull(stmt.getOutputColumnNames());
    }

    @Test
    public void testUnpartitionedMergeForcesShuffleJoinHint() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";

        JoinRelation joinRelation = analyzeAndGetSyntheticJoin(sql);

        assertEquals(HintNode.HINT_JOIN_SHUFFLE, joinRelation.getJoinHint(),
                "Unpartitioned Iceberg MERGE must not broadcast the target side");
    }

    @Test
    public void testPartitionedMergeKeepsDefaultJoinHint() {
        String sql = "MERGE INTO iceberg0.partitioned_db.t1_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";

        JoinRelation joinRelation = analyzeAndGetSyntheticJoin(sql);

        assertEquals("", joinRelation.getJoinHint(),
                "Partitioned Iceberg MERGE already has sink-side partition shuffle requirements");
    }

    // ---- Positional INSERT VALUES tests ----

    @Test
    public void testMergePositionalInsertValuesSuccess() {
        // INSERT VALUES without column list — values in schema order (id, data, date)
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 99 AS id, 'new' AS data, '2024-06-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.data, s.date)";
        MergeIntoStmt stmt = parseMerge(sql);
        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));
    }

    @Test
    public void testMergePositionalInsertValuesTooFew() {
        // INSERT VALUES with fewer values than target columns — should fail
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 99 AS id, 'new' AS data) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.data)";
        MergeIntoStmt stmt = parseMerge(sql);
        SemanticException ex = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(ex.getMessage().contains("does not match target table column count"),
                "Error should mention column count mismatch: " + ex.getMessage());
    }

    @Test
    public void testMergePositionalInsertValuesTooMany() {
        // INSERT VALUES with more values than target columns — should fail
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 99 AS id, 'new' AS data, '2024-01-01' AS date, 'extra' AS x) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.data, s.date, s.x)";
        MergeIntoStmt stmt = parseMerge(sql);
        SemanticException ex = assertThrows(SemanticException.class,
                () -> MergeIntoAnalyzer.analyze(stmt, connectContext));
        assertTrue(ex.getMessage().contains("does not match target table column count"),
                "Error should mention column count mismatch: " + ex.getMessage());
    }

    // ---- Self-merge test ----

    @Test
    public void testSelfMergeSuccess() {
        // MERGE INTO t USING t — self-merge should analyze successfully
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING iceberg0.unpartitioned_db.t0_v2 AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);
        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));
    }
}
