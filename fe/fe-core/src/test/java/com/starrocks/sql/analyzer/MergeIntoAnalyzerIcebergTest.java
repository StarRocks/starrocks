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
import com.starrocks.sql.ast.MergeIntoStmt;
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

    // ---- Error cases ----

    @Test
    public void testMergeIntoNonV2Table() {
        // t0 is format version 3, so MERGE INTO should be rejected
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0 AS t " +
                "USING (SELECT 1 AS id, 'x' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET t.data = s.data";
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
                "WHEN MATCHED THEN UPDATE SET t.date = s.date";
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
                "WHEN MATCHED THEN UPDATE SET t._file = s._file";
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

    // ---- Success cases ----

    @Test
    public void testBasicMergeMatchedUpdate() {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET t.data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertTrue(stmt.getTable() instanceof IcebergTable);
        assertNotNull(stmt.getQueryStatement());
        assertNotNull(stmt.getIcebergColumnOutputNames());
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
                "WHEN MATCHED THEN UPDATE SET t.data = s.data " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertNotNull(stmt.getQueryStatement());
        // _file, _pos, id, data, date, op_code = 6 columns
        List<String> colNames = stmt.getIcebergColumnOutputNames();
        assertNotNull(colNames);
        assertEquals(6, colNames.size());
        assertEquals(IcebergTable.FILE_PATH, colNames.get(0));
        assertEquals(IcebergTable.ROW_POSITION, colNames.get(1));
        assertEquals("op_code", colNames.get(colNames.size() - 1));
    }

    @Test
    public void testMergeMultipleMatchedClauses() {
        // Two WHEN MATCHED clauses with different conditions
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND t.data = 'old' THEN UPDATE SET t.data = 'updated' " +
                "WHEN MATCHED THEN DELETE";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertNotNull(stmt.getQueryStatement());
        assertEquals(3, stmt.getWhenClauses().size());
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
        assertNotNull(stmt.getIcebergColumnOutputNames());
    }

    @Test
    public void testMergeSourceSubquery() {
        // Source is a subquery with an alias
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT id, data, date FROM iceberg0.unpartitioned_db.t0_v2 WHERE id > 100) AS src " +
                "ON t.id = src.id " +
                "WHEN MATCHED THEN UPDATE SET t.data = src.data";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertTrue(stmt.getTable() instanceof IcebergTable);
        assertNotNull(stmt.getQueryStatement());
        // _file, _pos, id, data, date, op_code = 6 columns
        List<String> colNames = stmt.getIcebergColumnOutputNames();
        assertNotNull(colNames);
        assertEquals(6, colNames.size());
    }

    @Test
    public void testMergeColumnOutputNamesOrder() {
        // Verify the output column layout: _file, _pos, data_cols..., op_code
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET t.data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        MergeIntoAnalyzer.analyze(stmt, connectContext);

        List<String> colNames = stmt.getIcebergColumnOutputNames();
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
    public void testMergePartitionedTableSuccess() {
        // t1_v2 is partitioned by 'date'; updating non-partition column should succeed
        String sql = "MERGE INTO iceberg0.partitioned_db.t1_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET t.data = s.data";
        MergeIntoStmt stmt = parseMerge(sql);

        assertDoesNotThrow(() -> MergeIntoAnalyzer.analyze(stmt, connectContext));

        assertNotNull(stmt.getTable());
        assertTrue(stmt.getTable() instanceof IcebergTable);
        assertNotNull(stmt.getQueryStatement());
        assertNotNull(stmt.getIcebergColumnOutputNames());
    }
}
