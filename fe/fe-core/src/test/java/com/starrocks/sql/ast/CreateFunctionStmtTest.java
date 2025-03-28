// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.starrocks.sql.ast;

import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.CreateFunctionAnalyzer;
import mockit.Expectations;
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

        Assert.assertEquals("ABC", stmt.getFunctionName().getDb());
        Assert.assertEquals("my_udf_json_get", stmt.getFunctionName().getFunction());
        Assert.assertFalse(stmt.shouldReplaceIfExists());
    }

    @Test(expected = Throwable.class)
    public void testDisableUDF() throws Exception {
        boolean val = Config.enable_udf;
        try {
            Config.enable_udf = false;
            String createFunctionSql = "CREATE FUNCTION ABC.MY_UDF_JSON_GET(string, string) \n"
                    + "RETURNS string \n"
                    + "properties (\n"
                    + "    \"symbol\" = \"com.starrocks.udf.sample.UDFJsonGet\",\n"
                    + "    \"type\" = \"StarrocksJar\",\n"
                    + "    \"file\" = \"http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar\"\n"
                    + ");";
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                    createFunctionSql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, ctx);
        } finally {
            Config.enable_udf = val;
        }
    }

    @Test
    public void testInlinePropertiesUDF() throws Exception {
        String createFunctionSql = "CREATE FUNCTION get_typeb(INT) RETURNS \n" +
                "STRING\n" +
                " type = 'Python'\n" +
                " symbol = 'echo'\n" +
                "AS  \n" +
                "$$ \n" +
                "def echo(x):\n" +
                "    return str(type(x))  \n" +
                "$$;";
        CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);
        Assert.assertEquals("Python", stmt.getProperties().get("type"));
        Assert.assertEquals("echo", stmt.getProperties().get("symbol"));

        createFunctionSql = "CREATE FUNCTION get_type(INT) RETURNS\n" +
                "STRING\n" +
                " type = 'Python'\n" +
                " symbol = 'echo'\n" +
                " file = 'http://localhost:8000/echo.py.zip';";
        stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);
        Assert.assertEquals(stmt.getProperties().get("file"), "http://localhost:8000/echo.py.zip");

    }

    @Test
    public void testCreateUDFWithContent() {
        String createFunctionSql = "CREATE FUNCTION echo(int) \n"
                + "RETURNS int \n"
                + "properties (\n"
                + "    \"symbol\" = \"echo\",\n"
                + "    \"type\" = \"Python\"\n"
                + ") AS $$ \n"
                + "def a(b):\n" +
                "   return b \n" +
                "$$;";
        CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);
        Assert.assertTrue(stmt.getContent().contains("\n"));
        Assert.assertTrue(stmt.getContent().contains("def a(b):"));
    }

    @Test(expected = Throwable.class)
    public void testGetIfDisableUDF() throws Exception {
        boolean val = Config.enable_udf;
        try {
            Config.enable_udf = false;
            FunctionName mock = FunctionName.createFnName("mock");
            new Expectations(AnalyzerUtils.class) {
                {
                    AnalyzerUtils.getDBUdfFunction(ctx, mock, new Type[0]);
                }
            };
            AnalyzerUtils.getUdfFunction(ctx, mock, new Type[0]);

        } finally {
            Config.enable_udf = val;
        }
    }

    @Test
    public void testCreateOrReplaceUDF() {
        String createFunctionSql = "CREATE OR REPLACE FUNCTION ABC.MY_UDF_JSON_GET(string, string) \n"
                + "RETURNS string \n"
                + "properties (\n"
                + "    \"symbol\" = \"com.starrocks.udf.sample.UDFJsonGet\",\n"
                + "    \"type\" = \"StarrocksJar\",\n"
                + "    \"file\" = \"http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar\"\n"
                + ");";
        CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);

        Assert.assertEquals("ABC", stmt.getFunctionName().getDb());
        Assert.assertEquals("my_udf_json_get", stmt.getFunctionName().getFunction());
        Assert.assertTrue(stmt.shouldReplaceIfExists());
    }

    @Test
    public void testCreateIfNotExistsUDF() throws Exception {
        String createFunctionSql = "CREATE FUNCTION IF NOT EXISTS ABC.MY_UDF_JSON_GET(string, string) \n"
                + "RETURNS string \n"
                + "properties (\n"
                + "    \"symbol\" = \"com.starrocks.udf.sample.UDFJsonGet\",\n"
                + "    \"type\" = \"StarrocksJar\",\n"
                + "    \"file\" = \"http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar\"\n"
                + ");";
        CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);

        Assert.assertEquals("ABC", stmt.getFunctionName().getDb());
        Assert.assertEquals("my_udf_json_get", stmt.getFunctionName().getFunction());
        Assert.assertFalse(stmt.shouldReplaceIfExists());
        Assert.assertTrue(stmt.createIfNotExists());

        createFunctionSql = "CREATE OR REPLACE FUNCTION IF NOT EXISTS ABC.MY_UDF_JSON_GET(string, string) \n"
                + "RETURNS string \n"
                + "properties (\n"
                + "    \"symbol\" = \"com.starrocks.udf.sample.UDFJsonGet\",\n"
                + "    \"type\" = \"StarrocksJar\",\n"
                + "    \"file\" = \"http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar\"\n"
                + ");";
        try {
            stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                    createFunctionSql, 32).get(0);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("\"IF NOT EXISTS\" and \"OR REPLACE\" cannot be used together"));
        }
    }
}
