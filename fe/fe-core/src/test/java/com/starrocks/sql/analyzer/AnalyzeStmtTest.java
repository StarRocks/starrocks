// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeStmtTest {
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
    public void testAllColumns() {
        String sql = "analyze table db.tbl";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);

        Assert.assertEquals(4, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testSelectedColumns() {
        String sql = "analyze table db.tbl (kk1, kk2)";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);

        Assert.assertTrue(analyzeStmt.isSample());
        Assert.assertEquals(2, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testProperties() {
        String sql = "analyze full table db.tbl properties('expire_sec' = '30')";
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) analyzeSuccess(sql);

        Assert.assertFalse(analyzeStmt.isSample());
        Assert.assertEquals(1, analyzeStmt.getProperties().size());
        Assert.assertEquals("30", analyzeStmt.getProperties().getOrDefault("expire_sec", "2"));
    }

    @Test
    public void testTypeKeys() throws Exception {
        analyzeSuccess("select count(*) from tarray group by v4");
        analyzeSuccess("select distinct v4 from tarray");
        analyzeSuccess("select * from tarray order by v4");
        analyzeSuccess("select DENSE_RANK() OVER(partition by v3 order by v4) from tarray");
        analyzeFail("select avg(v4) from tarray");
        analyzeFail("select count(*) from tarray group by v5");
        analyzeFail("select distinct v5 from tarray");
        analyzeFail("select * from tarray join tarray y using(v4)");
        analyzeFail("select * from tarray join tarray y using(v5)");
        analyzeFail("select * from tarray order by v5");
        analyzeFail("select DENSE_RANK() OVER(partition by v5 order by v4) from tarray");
    }
}
