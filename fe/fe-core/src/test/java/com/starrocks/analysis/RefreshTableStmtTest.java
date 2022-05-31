// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RefreshTableStmtTest {
    private static StarRocksAssert starRocksAssert;
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createTbl = "create table db1.tbl1(k1 varchar(32), catalog varchar(32), external varchar(32), k4 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTbl);
    }

    @Test
    public void testRefreshTableParserAndAnalyzer() {
        String sql_1 = "REFRESH EXTERNAL TABLE db1.table1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof RefreshTableStmt);
        sql_1 = "REFRESH EXTERNAL TABLE catalog1.db1.table1";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof RefreshTableStmt);
        sql_1 = "REFRESH EXTERNAL TABLE catalog1.db1.table1.test";
        AnalyzeTestUtil.analyzeFail(sql_1);
        sql_1 = "REFRESH EXTERNAL TABLE catalog1.db1.table1 PARTITION(\"p1\", \"p2\")";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof RefreshTableStmt);
        Assert.assertEquals(((RefreshTableStmt) stmt).getPartitions().size(), 2);
        Assert.assertEquals(((RefreshTableStmt) stmt).getTableName(), "table1");
        sql_1 = "REFRESH EXTERNAL TABLE catalog1.db1.table1 PARTITION(\"k1=0\\/k2=1\", \"k1=1\\/k2=2\")";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertEquals(((RefreshTableStmt) stmt).getPartitions().size(), 2);
    }

}
