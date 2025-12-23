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
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static com.starrocks.sql.plan.ConnectorPlanTestBase.newFolder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeleteAnalyzerTest {

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
    public void testIcebergDeleteBasic() {
        String sql = "DELETE FROM iceberg0.unpartitioned_db.t0 WHERE id = 1";
        DeleteStmt deleteStmt = (DeleteStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        assertDoesNotThrow(() -> DeleteAnalyzer.analyze(deleteStmt, connectContext));

        assertNotNull(deleteStmt.getTable());
        assertTrue(deleteStmt.getTable() instanceof IcebergTable);
        assertNotNull(deleteStmt.getQueryStatement());
    }

    @Test
    public void testIcebergDeleteWithoutWhereClause() {
        String sql = "DELETE FROM iceberg0.unpartitioned_db.t0";
        DeleteStmt deleteStmt = (DeleteStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> DeleteAnalyzer.analyze(deleteStmt, connectContext));
        assertTrue(exception.getMessage().contains("Delete must specify where clause"));
    }

    @Test
    public void testIcebergDeleteWithPartition() {
        String sql = "DELETE FROM iceberg0.partitioned_db.t1 PARTITION(p_date = '2023-01-01') WHERE id = 1";
        DeleteStmt deleteStmt = (DeleteStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> DeleteAnalyzer.analyze(deleteStmt, connectContext));
        assertTrue(exception.getMessage().contains("Delete for Iceberg table do not support specifying partitions"));
    }

    @Test
    public void testIcebergDeleteWithUsingClause() {
        String sql = "DELETE FROM iceberg0.unpartitioned_db.t0 USING other_table WHERE t0.id = other_table.id";
        DeleteStmt deleteStmt = (DeleteStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> DeleteAnalyzer.analyze(deleteStmt, connectContext));
        assertTrue(exception.getMessage().contains("Delete for Iceberg table do not support `using` clause"));
    }

    @Test
    public void testIcebergDeleteWithCTEs() {
        String sql = "WITH cte AS (SELECT id FROM iceberg0.unpartitioned_db.t0) DELETE FROM iceberg0.unpartitioned_db.t0 " +
                "WHERE id IN (SELECT id FROM cte)";
        DeleteStmt deleteStmt = (DeleteStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> DeleteAnalyzer.analyze(deleteStmt, connectContext));
        assertTrue(exception.getMessage().contains("Delete for Iceberg table do not support `with` clause"));
    }

    @Test
    public void testIcebergDeleteConvertsToSelectFileAndPos() {
        String sql = "DELETE FROM iceberg0.unpartitioned_db.t0 WHERE id = 1";
        DeleteStmt deleteStmt = (DeleteStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        DeleteAnalyzer.analyze(deleteStmt, connectContext);

        QueryStatement queryStmt = deleteStmt.getQueryStatement();
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
    public void testUnsupportedCatalogDelete() {
        String sql = "DELETE FROM hive0.`partitioned_db`.`lineitem_par` WHERE l_orderkey = 1";
        DeleteStmt deleteStmt = (DeleteStmt) SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);

        SemanticException exception = assertThrows(SemanticException.class,
                () -> DeleteAnalyzer.analyze(deleteStmt, connectContext));
        assertTrue(exception.getMessage().contains("doesn't support"));
    }
}
