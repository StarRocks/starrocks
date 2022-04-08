// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.UUID;

public class CreateAnalyzeJobStmtTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testAllDB() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "create analyze all";

        SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        com.starrocks.sql.analyzer.Analyzer analyzer =
                new com.starrocks.sql.analyzer.Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

        analyzer.analyze(statementBase);

        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) statementBase;

        Assert.assertEquals(AnalyzeJob.DEFAULT_ALL_ID, analyzeStmt.getDbId());
        Assert.assertEquals(AnalyzeJob.DEFAULT_ALL_ID, analyzeStmt.getTableId());
        Assert.assertTrue(analyzeStmt.getColumnNames().isEmpty());
    }

    @Test
    public void testAllTable() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "create analyze full database db";

        SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        com.starrocks.sql.analyzer.Analyzer analyzer =
                new com.starrocks.sql.analyzer.Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

        analyzer.analyze(statementBase);

        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) statementBase;

        Assert.assertEquals(10002, analyzeStmt.getDbId());
        Assert.assertEquals(AnalyzeJob.DEFAULT_ALL_ID, analyzeStmt.getTableId());
        Assert.assertTrue(analyzeStmt.getColumnNames().isEmpty());
    }

    @Test
    public void testColumn() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "create analyze table db.tbl(kk1, kk2)";

        SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        com.starrocks.sql.analyzer.Analyzer analyzer =
                new com.starrocks.sql.analyzer.Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

        analyzer.analyze(statementBase);

        CreateAnalyzeJobStmt analyzeStmt = (CreateAnalyzeJobStmt) statementBase;

        Assert.assertEquals(10002, analyzeStmt.getDbId());
        Assert.assertEquals(10004, analyzeStmt.getTableId());
        Assert.assertEquals(2, analyzeStmt.getColumnNames().size());
    }
}
