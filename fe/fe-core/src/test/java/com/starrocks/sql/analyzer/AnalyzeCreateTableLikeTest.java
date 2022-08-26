// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeCreateTableLikeTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();

        starRocksAssert = getStarRocksAssert();

        String createTblStmtStr = "create table db.tbl1(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testNormal() {
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) analyzeSuccess(
                "CREATE TABLE tbl2 LIKE tbl1");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("tbl2", stmt.getTableName());
        Assert.assertEquals("test", stmt.getExistedDbName());
        Assert.assertEquals("tbl1", stmt.getExistedTableName());
    }

    @Test
    public void testAnalyzeSuccess() {
        analyzeSuccess("CREATE TABLE test1.table2 LIKE test1.table1;");
        analyzeSuccess("CREATE TABLE test2.table2 LIKE test1.table1;");
        analyzeSuccess("CREATE TABLE table2 LIKE table1;");
    }

    @Test
    public void testAnalyzeFail() {
        analyzeFail("CREATE TABLE `XX_AA*B` LIKE tbl3",
                "Incorrect table name 'XX_AA*B'");
    }
}
