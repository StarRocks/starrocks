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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeCancelAlterTableStatementTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
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
        Assert.assertEquals("db", stmt.getDbName());
        Assert.assertEquals(ShowAlterStmt.AlterType.ROLLUP, stmt.getAlterType());
        Assert.assertEquals("tbl", stmt.getTableName());
    }

    @Test
    public void testCancelAlterColumn() {
        CancelAlterTableStmt stmt = (CancelAlterTableStmt) analyzeSuccess(
                "CANCEL ALTER TABLE COLUMN FROM t0");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals(ShowAlterStmt.AlterType.COLUMN, stmt.getAlterType());
        Assert.assertEquals("t0", stmt.getTableName());
    }

    @Test
    public void testCancelMaterializedView() {
        CancelAlterTableStmt stmt = (CancelAlterTableStmt) analyzeSuccess(
                "cancel alter materialized view from materialized_view_test");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("materialized_view_test", stmt.getTableName());
        Assert.assertEquals(ShowAlterStmt.AlterType.MATERIALIZED_VIEW, stmt.getAlterType());
    }
}
