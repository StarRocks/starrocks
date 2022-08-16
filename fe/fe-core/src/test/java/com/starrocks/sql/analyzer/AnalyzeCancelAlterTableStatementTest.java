// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CancelAlterTableStmt;
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
        Assert.assertEquals("CANCEL ALTER ROLLUP FROM `db`.`tbl` (1,2,3)", stmt.toSql());
    }

    @Test
    public void testCancelAlterColumn() {
        CancelAlterTableStmt stmt = (CancelAlterTableStmt) analyzeSuccess(
                "CANCEL ALTER TABLE COLUMN FROM tbl");
        Assert.assertEquals("CANCEL ALTER COLUMN FROM `test`.`tbl`", stmt.toSql());
    }

    @Test
    public void testCancelMaterializedView() {
        CancelAlterTableStmt stmt = (CancelAlterTableStmt) analyzeSuccess(
                "cancel alter materialized view from materialized_view_test");
        Assert.assertEquals("CANCEL ALTER MATERIALIZED_VIEW FROM `test`.`materialized_view_test`", stmt.toSql());
    }
}
