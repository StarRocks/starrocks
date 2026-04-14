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
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.expression.Expr;
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
        List<String> colNames = updateStmt.getIcebergColumnOutputNames();
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

        List<String> colNames = updateStmt.getIcebergColumnOutputNames();
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
        assertNotNull(updateStmt.getIcebergColumnOutputNames());
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
}
