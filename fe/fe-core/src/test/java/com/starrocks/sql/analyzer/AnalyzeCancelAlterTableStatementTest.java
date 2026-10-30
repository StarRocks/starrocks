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

import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeCancelAlterTableStatementTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();

        starRocksAssert = getStarRocksAssert();

        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testCancelRollup()  {
        CancelAlterTableStmt stmt = (CancelAlterTableStmt) analyzeSuccess(
                "cancel alter table rollup from db.tbl (1, 2, 3)");
        Assertions.assertEquals("db", stmt.getDbName());
        Assertions.assertEquals(ShowAlterStmt.AlterType.ROLLUP, stmt.getAlterType());
        Assertions.assertEquals("tbl", stmt.getTableName());
        Assertions.assertEquals(List.of(1L, 2L, 3L), stmt.getAlterJobIdList());
    }

    @Test
    public void testCancelRollupWithoutJobIds() {
        // Without a job id list the handler must cancel every unfinished rollup job of the table,
        // which it only does when the list is null rather than empty.
        CancelAlterTableStmt stmt = (CancelAlterTableStmt) analyzeSuccess(
                "cancel alter table rollup from db.tbl");
        Assertions.assertEquals(ShowAlterStmt.AlterType.ROLLUP, stmt.getAlterType());
        Assertions.assertNull(stmt.getAlterJobIdList());
    }

    @Test
    public void testCancelAlterColumn() {
        CancelAlterTableStmt stmt = (CancelAlterTableStmt) analyzeSuccess(
                "CANCEL ALTER TABLE COLUMN FROM t0");
        Assertions.assertEquals("test", stmt.getDbName());
        Assertions.assertEquals(ShowAlterStmt.AlterType.COLUMN, stmt.getAlterType());
        Assertions.assertEquals("t0", stmt.getTableName());
    }

    @Test
    public void testCancelMaterializedView() {
        CancelAlterTableStmt stmt = (CancelAlterTableStmt) analyzeSuccess(
                "cancel alter materialized view from materialized_view_test");
        Assertions.assertEquals("test", stmt.getDbName());
        Assertions.assertEquals("materialized_view_test", stmt.getTableName());
        Assertions.assertEquals(ShowAlterStmt.AlterType.MATERIALIZED_VIEW, stmt.getAlterType());
    }
}
