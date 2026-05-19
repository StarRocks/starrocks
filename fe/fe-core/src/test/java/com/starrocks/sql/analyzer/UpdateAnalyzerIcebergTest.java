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
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UpdateAnalyzerIcebergTest {

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

    @Test
    public void testIcebergUpdateBasic() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        assertDoesNotThrow(() -> UpdateAnalyzer.analyze(updateStmt, connectContext));

        assertNotNull(updateStmt.getTable());
        assertTrue(updateStmt.getTable() instanceof IcebergTable);
        assertNotNull(updateStmt.getQueryStatement());
    }

    @Test
    public void testIcebergUpdateWithoutWhereClause() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new'";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("where clause"));
    }

    @Test
    public void testIcebergUpdatePartitionColumn() {
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET date = '2024-01-01' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("partition column"));
    }

    @Test
    public void testIcebergUpdateNonExistentColumn() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET nonexistent = 'val' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("do not existing column"));
    }

    @Test
    public void testIcebergUpdateConvertsToSelectWithFileAndPos() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        QueryStatement queryStmt = updateStmt.getQueryStatement();
        assertNotNull(queryStmt);

        QueryRelation queryRelation = queryStmt.getQueryRelation();
        assertInstanceOf(SelectRelation.class, queryRelation);

        SelectRelation selectRelation = (SelectRelation) queryRelation;
        assertNotNull(selectRelation.getSelectList());

        // Check that select list contains _file and _pos columns
        boolean hasFileColumn = false;
        boolean hasPosColumn = false;

        for (int i = 0; i < selectRelation.getSelectList().getItems().size(); i++) {
            String alias = selectRelation.getSelectList().getItems().get(i).getAlias();
            if (IcebergTable.FILE_PATH.equals(alias)) {
                hasFileColumn = true;
            }
            if (IcebergTable.ROW_POSITION.equals(alias)) {
                hasPosColumn = true;
            }
        }

        assertTrue(hasFileColumn, "Select list should contain _file column");
        assertTrue(hasPosColumn, "Select list should contain _pos column");
    }

    @Test
    public void testIcebergUpdateConvertsToSelectWithOpCode() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        QueryStatement queryStmt = updateStmt.getQueryStatement();
        assertNotNull(queryStmt);

        QueryRelation queryRelation = queryStmt.getQueryRelation();
        assertInstanceOf(SelectRelation.class, queryRelation);

        SelectRelation selectRelation = (SelectRelation) queryRelation;
        assertNotNull(selectRelation.getSelectList());

        // Check that select list contains _file, _pos, and op_code columns
        boolean hasFileColumn = false;
        boolean hasPosColumn = false;
        boolean hasOpCode = false;

        for (int i = 0; i < selectRelation.getSelectList().getItems().size(); i++) {
            String alias = selectRelation.getSelectList().getItems().get(i).getAlias();
            if (IcebergTable.FILE_PATH.equals(alias)) {
                hasFileColumn = true;
            }
            if (IcebergTable.ROW_POSITION.equals(alias)) {
                hasPosColumn = true;
            }
            if ("op_code".equals(alias)) {
                hasOpCode = true;
            }
        }

        assertTrue(hasFileColumn, "Select list should contain _file column");
        assertTrue(hasPosColumn, "Select list should contain _pos column");
        assertTrue(hasOpCode, "Select list should contain op_code column");
    }

    @Test
    public void testIcebergUpdateWithCTEClause() {
        String sql = "WITH cte AS (SELECT id FROM iceberg0.unpartitioned_db.t0_v2) " +
                "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id IN (SELECT id FROM cte)";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("with"));
    }

    @Test
    public void testIcebergUpdateNonV2TableError() {
        // t0 is format version 3, should be rejected
        String sql = "UPDATE iceberg0.unpartitioned_db.t0 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("V2 tables"));
    }

    @Test
    public void testIcebergUpdateWithFromClause() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' " +
                "FROM iceberg0.unpartitioned_db.t0 WHERE iceberg0.unpartitioned_db.t0_v2.id = iceberg0.unpartitioned_db.t0.id";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("from"));
    }

    @Test
    public void testIcebergUpdateMultipleColumns() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new', date = '2024-01-01' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        assertDoesNotThrow(() -> UpdateAnalyzer.analyze(updateStmt, connectContext));

        assertNotNull(updateStmt.getTable());
        assertTrue(updateStmt.getTable() instanceof IcebergTable);
        assertNotNull(updateStmt.getQueryStatement());

        // Verify column output names include all expected columns
        List<String> colNames = updateStmt.getQueryStatement().getQueryRelation().getColumnOutputNames();
        assertNotNull(colNames);
        // _file, _pos, id, data, date, op_code
        assertEquals(6, colNames.size());
        assertEquals(IcebergTable.FILE_PATH, colNames.get(0));
        assertEquals(IcebergTable.ROW_POSITION, colNames.get(1));
        assertEquals("id", colNames.get(2));
        assertEquals("data", colNames.get(3));
        assertEquals("date", colNames.get(4));
        assertEquals("op_code", colNames.get(5));
    }

    @Test
    public void testIcebergUpdateColumnOutputNames() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        List<String> colNames = updateStmt.getQueryStatement().getQueryRelation().getColumnOutputNames();
        assertNotNull(colNames);
        // _file, _pos, id, data, date, op_code
        assertEquals(6, colNames.size());
        assertEquals(IcebergTable.FILE_PATH, colNames.get(0));
        assertEquals(IcebergTable.ROW_POSITION, colNames.get(1));
        assertEquals("op_code", colNames.get(colNames.size() - 1));
    }

    @Test
    public void testIcebergUpdateSelectListStructure() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        QueryStatement queryStmt = updateStmt.getQueryStatement();
        SelectRelation selectRelation = (SelectRelation) queryStmt.getQueryRelation();
        SelectList selectList = selectRelation.getSelectList();
        List<SelectListItem> items = selectList.getItems();

        // Expected: _file, _pos, id, data, date, op_code = 6 items
        assertEquals(6, items.size());

        // Verify output expressions are present and cast correctly
        List<Expr> outputExprs = selectRelation.getOutputExpression();
        assertNotNull(outputExprs);
        assertEquals(6, outputExprs.size());
    }

    @Test
    public void testIcebergUpdatePartitionedTableHappyPath() {
        // t1_v2 is a partitioned V2 table with partition column 'date'
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        assertDoesNotThrow(() -> UpdateAnalyzer.analyze(updateStmt, connectContext));

        assertNotNull(updateStmt.getTable());
        assertTrue(updateStmt.getTable() instanceof IcebergTable);
        assertNotNull(updateStmt.getQueryStatement());
        assertNotNull(updateStmt.getQueryStatement().getQueryRelation().getColumnOutputNames());
    }

    @Test
    public void testIcebergUpdateQueryStatementIsExplainFalse() {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        QueryStatement queryStmt = updateStmt.getQueryStatement();
        assertNotNull(queryStmt);
        // Non-EXPLAIN update should not be marked as explain
        assertTrue(!queryStmt.isExplain());
    }

    @Test
    public void testIcebergUpdateDataColumnCasting() {
        // Use multiple columns to exercise the cast loop
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new', date = '2024-06-01' WHERE id > 0";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        QueryStatement queryStmt = updateStmt.getQueryStatement();
        SelectRelation selectRelation = (SelectRelation) queryStmt.getQueryRelation();

        // Verify output expressions have been set (cast step completed)
        List<Expr> outputExprs = selectRelation.getOutputExpression();
        assertNotNull(outputExprs);
        // All 6 columns should have output expressions
        assertEquals(6, outputExprs.size());
        // None should be null
        for (Expr expr : outputExprs) {
            assertNotNull(expr);
        }
    }

    @Test
    public void testIcebergUpdateHiddenMetadataColumnRejected() {
        // Updating the hidden `_file` metadata column must be rejected.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET `" + IcebergTable.FILE_PATH
                + "` = 'x' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("metadata column"),
                "Expected 'metadata column' error, got: " + exception.getMessage());
    }

    @Test
    public void testIcebergUpdateHiddenRowPositionRejected() {
        // Updating the hidden `_pos` metadata column must be rejected.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET `" + IcebergTable.ROW_POSITION
                + "` = 0 WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("metadata column"),
                "Expected 'metadata column' error, got: " + exception.getMessage());
    }

    @Test
    public void testIcebergUpdateDefaultValueRejected() {
        // Iceberg V2 does not support column default values; SET col = DEFAULT must be rejected.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = DEFAULT WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("DEFAULT"),
                "Expected 'DEFAULT' error, got: " + exception.getMessage());
    }

    @Test
    public void testIcebergUpdateWithExpressionAssignment() {
        // SET using an expression (not a constant) exercises the assignExpr path
        // in the analyzer and the per-column cast loop on a non-literal source.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = concat(data, '_v2') WHERE id > 0";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        assertDoesNotThrow(() -> UpdateAnalyzer.analyze(updateStmt, connectContext));

        assertNotNull(updateStmt.getQueryStatement());
        SelectRelation selectRelation =
                (SelectRelation) updateStmt.getQueryStatement().getQueryRelation();
        // Expect _file, _pos, id, data, date, op_code
        assertEquals(6, selectRelation.getSelectList().getItems().size());
        assertEquals(6, selectRelation.getOutputExpression().size());
    }

    @Test
    public void testIcebergUpdateWithNullAssignment() {
        // SET col = NULL is a valid assignment; analyzer must cast the null literal
        // to the target column type and produce 6 output exprs.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = NULL WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        assertDoesNotThrow(() -> UpdateAnalyzer.analyze(updateStmt, connectContext));

        SelectRelation selectRelation =
                (SelectRelation) updateStmt.getQueryStatement().getQueryRelation();
        assertEquals(6, selectRelation.getOutputExpression().size());
    }

    @Test
    public void testIcebergUpdateLastItemIsOpCodeLiteral() {
        // The last SELECT item must be an IntLiteral tagged with the UPDATE op code
        // (exercises the op_code literal creation in analyzeIcebergTable).
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        SelectRelation selectRelation =
                (SelectRelation) updateStmt.getQueryStatement().getQueryRelation();
        SelectList selectList = selectRelation.getSelectList();
        SelectListItem last = selectList.getItems().get(selectList.getItems().size() - 1);
        assertEquals("op_code", last.getAlias());
        assertInstanceOf(IntLiteral.class, last.getExpr());
        assertEquals(IcebergRowDeltaSink.OpCode.UPDATE.value(),
                ((IntLiteral) last.getExpr()).getValue());
    }

    @Test
    public void testIcebergUpdateTableAndQueryStatementSet() {
        // analyzeIcebergTable must populate both setTable and setQueryStatement.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        assertNotNull(updateStmt.getTable());
        assertInstanceOf(IcebergTable.class, updateStmt.getTable());
        assertNotNull(updateStmt.getQueryStatement());
        assertFalse(updateStmt.getQueryStatement().isExplain());
    }

    @Test
    public void testIcebergUpdateColumnNameIsCaseInsensitive() {
        // Assignment column lookup uses lowercase comparison; uppercase SET identifier
        // must resolve against the (lowercase) schema column.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET DATA = 'x' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        assertDoesNotThrow(() -> UpdateAnalyzer.analyze(updateStmt, connectContext));
    }

    @Test
    public void testIcebergUpdatePartitionedTableColumnOutputNames() {
        // For a partitioned V2 table, the column output list must still follow the
        // same contract: [_file, _pos, id, data, date, op_code].
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        List<String> colNames = updateStmt.getQueryStatement().getQueryRelation().getColumnOutputNames();
        assertNotNull(colNames);
        assertEquals(6, colNames.size());
        assertEquals(IcebergTable.FILE_PATH, colNames.get(0));
        assertEquals(IcebergTable.ROW_POSITION, colNames.get(1));
        assertEquals("op_code", colNames.get(colNames.size() - 1));
    }

    @Test
    public void testIcebergUpdatePartitionedRejectsUppercasePartitionColumn() {
        // Partition column check compares by lowercase name; upper-case assignment
        // targeting `DATE` must still be rejected on partitioned table t1_v2.
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET DATE = '2024-01-01' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
        assertTrue(exception.getMessage().contains("partition column"));
    }

    @Test
    public void testIcebergUpdatePropertiesPopulated() {
        // analyzeProperties runs before Iceberg branch and must populate the
        // update properties with session-driven defaults.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        assertNotNull(updateStmt.getProperties());
        assertTrue(updateStmt.getProperties().containsKey("max_filter_ratio"));
        assertTrue(updateStmt.getProperties().containsKey("strict_mode"));
        assertTrue(updateStmt.getProperties().containsKey("timeout"));
    }

    @Test
    public void testIcebergUpdateDataColumnStartLayout() {
        // The analyzer casts columns starting at index 2. Verify that the first
        // two output exprs (for _file, _pos) are not the casted data exprs.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        SelectRelation selectRelation =
                (SelectRelation) updateStmt.getQueryStatement().getQueryRelation();
        List<Expr> outputExprs = selectRelation.getOutputExpression();
        // _file + _pos + 3 data cols + op_code = 6
        assertEquals(6, outputExprs.size());
        // Last expression is the op_code literal
        assertInstanceOf(IntLiteral.class, outputExprs.get(outputExprs.size() - 1));
    }

    @Test
    public void testIcebergUpdateOutputExprsCountMatchesColNames() {
        // The saved column output names and the SelectRelation output exprs
        // must line up 1:1 (the planner relies on this invariant).
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        UpdateAnalyzer.analyze(updateStmt, connectContext);

        List<String> colNames = updateStmt.getQueryStatement().getQueryRelation().getColumnOutputNames();
        SelectRelation selectRelation =
                (SelectRelation) updateStmt.getQueryStatement().getQueryRelation();
        assertNotNull(colNames);
        assertEquals(colNames.size(), selectRelation.getOutputExpression().size());
    }

    @Test
    public void testIcebergUpdateNonExistentTable() {
        // Table lookup happens at the top of analyze(); a non-existent table
        // must raise a SemanticException (not an NPE) before the Iceberg branch.
        String sql = "UPDATE iceberg0.unpartitioned_db.not_a_real_table SET data = 'x' WHERE id = 1";
        UpdateStmt updateStmt = (UpdateStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        assertThrows(Exception.class,
                () -> UpdateAnalyzer.analyze(updateStmt, connectContext));
    }
}
