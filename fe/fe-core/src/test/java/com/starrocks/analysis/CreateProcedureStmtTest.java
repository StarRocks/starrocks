package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateProcedureStmtTest {
    private static StarRocksAssert starRocksAssert;
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    @Test
    public void testNormal() throws Exception {
        ConnectContext defaultCtx = starRocksAssert.getCtx();
        defaultCtx.setDatabase("testDb");
        String sql = "create procedure xxx (IN v1 int, IN v2 int, OUT v3 string) \n PROPERTIES (\"\"=\"\")";
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, ConnectContext.get());

        sql = "call xxx(2, @test)";
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, ConnectContext.get());

        sql = "call xxx(@test)";
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, ConnectContext.get());
    }

}
