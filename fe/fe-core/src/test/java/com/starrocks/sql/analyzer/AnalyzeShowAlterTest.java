// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeShowAlterTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();

        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testShowAlter1() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess("SHOW ALTER TABLE COLUMN FROM db");
        Assert.assertEquals("SHOW ALTER TABLE COLUMN FROM `db`", statement.toSql());
    }

    @Test
    public void testShowAlter2() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess(
                "SHOW ALTER TABLE COLUMN FROM db WHERE `TableName` = \'abc\' LIMIT 1, 2");
        Assert.assertEquals("SHOW ALTER TABLE COLUMN FROM `db` WHERE TableName = 'abc' LIMIT 1, 2",
                statement.toSql());
    }

    @Test
    public void testShowAlter3() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess(
                "SHOW ALTER TABLE COLUMN FROM db ORDER BY CreateTime");
        Assert.assertEquals("SHOW ALTER TABLE COLUMN FROM `db` ORDER BY CreateTime ASC",
                statement.toSql());
    }

    @Test
    public void testShowAlter4() {
        ShowAlterStmt statement = (ShowAlterStmt) analyzeSuccess(
                "SHOW ALTER TABLE COLUMN FROM db WHERE `CreateTime` > '2019-12-04 00:00:00'");
        Assert.assertEquals("SHOW ALTER TABLE COLUMN FROM `db` " +
                        "WHERE CreateTime > '2019-12-04 00:00:00'",
                statement.toSql());
    }

    @Test
    public void normalTest() {
        analyzeSuccess("SHOW ALTER TABLE COLUMN ORDER BY CreateTime DESC LIMIT 1;");
        analyzeFail("SHOW ALTER TABLE COLUMN FROM errordb",
                "Unknown database 'errordb'");
        analyzeFail("SHOW ALTER TABLE COLUMN FROM db WHERE `CreateTime` > '2019-12-04 00:00:00' " +
                        "AND `bad_column` < '2010-12-04 00:00:00'",
                "The columns of TableName/CreateTime/FinishTime/State are supported");
        analyzeFail("SHOW ALTER TABLE COLUMN FROM db WHERE `CreateTime` > '2019-12-04 00:00:00' " +
                        "OR `FinishTime` < '2022-12-04 00:00:00'",
                "Only allow compound predicate with operator AND");
        analyzeSuccess("SHOW ALTER MATERIALIZED VIEW");
        analyzeSuccess("SHOW ALTER MATERIALIZED VIEW FROM db WHERE `TableName` = \'abc\' LIMIT 1, 2");
        analyzeSuccess("SHOW ALTER MATERIALIZED VIEW FROM db ORDER BY CreateTime");        
    }
}
