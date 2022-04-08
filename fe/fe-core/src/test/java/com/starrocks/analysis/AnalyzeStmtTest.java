// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.UUID;

public class AnalyzeStmtTest {
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
    public void testAllColumns() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "analyze table db.tbl";

        SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        com.starrocks.sql.analyzer.Analyzer analyzer =
                new com.starrocks.sql.analyzer.Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = null;
        try {
            statementBase = SqlParserUtils.getFirstStmt(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(sql);
            System.err.println("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        }

        analyzer.analyze(statementBase);

        AnalyzeStmt analyzeStmt = (AnalyzeStmt) statementBase;
        Assert.assertEquals(4, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testSelectedColumns() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "analyze table db.tbl (kk1, kk2)";

        SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        com.starrocks.sql.analyzer.Analyzer analyzer =
                new com.starrocks.sql.analyzer.Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = null;
        try {
            statementBase = SqlParserUtils.getFirstStmt(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(sql);
            System.err.println("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        }
        analyzer.analyze(statementBase);
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) statementBase;

        Assert.assertEquals(2, analyzeStmt.getColumnNames().size());
    }

    @Test
    public void testProperties() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "analyze full table db.tbl properties('expire_sec' = '30')";

        SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        com.starrocks.sql.analyzer.Analyzer analyzer =
                new com.starrocks.sql.analyzer.Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = null;
        statementBase = SqlParserUtils.getFirstStmt(parser);
        analyzer.analyze(statementBase);
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) statementBase;

        Assert.assertEquals(1, analyzeStmt.getProperties().size());

        Assert.assertEquals("30", analyzeStmt.getProperties().getOrDefault("expire_sec", "2"));
    }
}
