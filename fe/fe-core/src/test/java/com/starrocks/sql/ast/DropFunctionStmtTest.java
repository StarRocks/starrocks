package com.starrocks.sql.ast;

import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class DropFunctionStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {
        String dropFunctionSql = "DROP FUNCTION ABC.MY_UDF_JSON_GET(string, string)";
        DropFunctionStmt stmt = (DropFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                dropFunctionSql, 32).get(0);
//        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        Assert.assertEquals("ABC", stmt.getFunctionName().getDb());
        Assert.assertEquals("my_udf_json_get", stmt.getFunctionName().getFunction());
    }
}
