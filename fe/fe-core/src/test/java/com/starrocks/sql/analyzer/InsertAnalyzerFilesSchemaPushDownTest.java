// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
