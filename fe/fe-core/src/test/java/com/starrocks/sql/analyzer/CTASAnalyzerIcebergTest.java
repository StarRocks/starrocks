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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

import static com.starrocks.sql.plan.ConnectorPlanTestBase.newFolder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CTASAnalyzerIcebergTest {
    private static ConnectContext connectContext;

    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        new StarRocksAssert(connectContext);
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());
    }

    private static List<ColumnDef> analyzeCtasColumnDefs(String sql) throws Exception {
        CreateTableAsSelectStmt stmt =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        CreateTableStmt createTableStmt = stmt.getCreateTableStmt();
        assertEquals("iceberg", createTableStmt.getEngineName());
        return createTableStmt.getColumnDefs();
    }

    @Test
    public void testIcebergCtasKeepsPartitionColumnNullable() throws Exception {
        List<ColumnDef> columnDefs = analyzeCtasColumnDefs(
                "create table iceberg0.partitioned_db.ctas_null_part (a, b, c) "
                        + "partition by (c) as select 1, 2, null");

        // Nullability must come from the source expression only. Iceberg allows NULL in an
        // identity partition, so being the partition column must not tighten `c` to NOT NULL.
        assertFalse(columnDefs.get(0).isAllowNull());
        assertFalse(columnDefs.get(1).isAllowNull());
        assertTrue(columnDefs.get(2).isAllowNull());
    }

    @Test
    public void testIcebergCtasKeepsNonNullPartitionColumnNotNull() throws Exception {
        List<ColumnDef> columnDefs = analyzeCtasColumnDefs(
                "create table iceberg0.partitioned_db.ctas_notnull_part (a, b, c) "
                        + "partition by (c) as select 1, null, 3");

        assertFalse(columnDefs.get(0).isAllowNull());
        assertTrue(columnDefs.get(1).isAllowNull());
        assertFalse(columnDefs.get(2).isAllowNull());
    }
}
