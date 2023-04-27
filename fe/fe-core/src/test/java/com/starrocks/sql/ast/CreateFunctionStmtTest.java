package com.starrocks.sql.ast;

import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class CreateFunctionStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {
        String createFunctionSql = "CREATE FUNCTION ABC.MY_UDF_JSON_GET(string, string) \n"
                + "RETURNS string \n"
                + "properties (\n"
                + "    \"symbol\" = \"com.starrocks.udf.sample.UDFJsonGet\",\n"
                + "    \"type\" = \"StarrocksJar\",\n"
                + "    \"file\" = \"http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar\"\n"
                + ");";
        CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);
//        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        Assert.assertEquals("ABC", stmt.getFunctionName().getDb());
        Assert.assertEquals("my_udf_json_get", stmt.getFunctionName().getFunction());
    }
}
