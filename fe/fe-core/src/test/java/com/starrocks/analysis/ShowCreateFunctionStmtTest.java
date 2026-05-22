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

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowCreateFunctionStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShowCreateFunctionStmtTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test_udf_db").useDatabase("test_udf_db");
        starRocksAssert.withFunction(
                "CREATE FUNCTION test_udf_db.echo(string) RETURNS string PROPERTIES " +
                        "(\"symbol\" = \"Echo\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        starRocksAssert.withFunction(
                "CREATE FUNCTION test_udf_db.echo(int) RETURNS int PROPERTIES " +
                        "(\"symbol\" = \"Echo\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        starRocksAssert.withFunction(
                "CREATE GLOBAL FUNCTION echo_python(int) RETURNS int PROPERTIES " +
                        "(\"symbol\" = \"Echo\", \"type\" = \"python\", \"file\" = \"xxx\", \"cust_prop\" = \"cust_val\");");
        starRocksAssert.withFunction(
                "CREATE FUNCTION test_udf_db.python_add(BIGINT) RETURNS BIGINT PROPERTIES (" +
                        "\"type\" = \"Python\", \"file\" = \"inline\", " +
                        "\"symbol\" = \"add\", \"input\" = \"arrow\") " +
                        "AS $$\n" +
                        "import pyarrow.compute as pc\n" +
                        "def add(x):\n" +
                        "    return pc.add(x, 1)\n" +
                        "$$;");
        starRocksAssert.withFunction(
                "CREATE AGGREGATE FUNCTION test_udf_db.my_agg(int) RETURNS bigint PROPERTIES " +
                        "(\"symbol\" = \"com.example.MyAgg\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        starRocksAssert.withFunction(
                "CREATE AGGREGATE FUNCTION test_udf_db.my_agg_analytic(int) RETURNS bigint PROPERTIES " +
                        "(\"symbol\" = \"com.example.MyWindow\", \"type\" = \"StarrocksJar\", " +
                        "\"file\" = \"xxx\", \"analytic\" = \"true\");");
        starRocksAssert.withFunction(
                "CREATE AGGREGATE FUNCTION test_udf_db.my_agg_shared(int) RETURNS bigint PROPERTIES " +
                        "(\"symbol\" = \"com.example.MyShared\", \"type\" = \"StarrocksJar\", " +
                        "\"file\" = \"xxx\", \"isolation\" = \"shared\");");
        starRocksAssert.withFunction(
                "CREATE TABLE FUNCTION IF NOT EXISTS test_udf_db.my_table_fn(int) RETURNS bigint PROPERTIES " +
                        "(\"symbol\" = \"com.example.MyTableFn\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        starRocksAssert.withFunction(
                "CREATE GLOBAL FUNCTION my_global_udf(int) RETURNS bigint PROPERTIES " +
                        "(\"symbol\" = \"com.example.MyGlobalUdf\", \"type\" = \"StarrocksJar\", " +
                        "\"file\" = \"file:///tmp/global.jar\");");
        starRocksAssert.withFunction(
                "CREATE FUNCTION test_udf_db.sql_add_one(x INT) RETURNS x + 1;");
    }

    @Test
    public void testParseWithArgs() throws Exception {
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function test_udf_db.echo(string)", ctx);
        Assertions.assertNotNull(stmt.getFunctionRef());
        Assertions.assertNotNull(stmt.getArgsDef());
        Assertions.assertEquals("echo", stmt.getFunctionRef().getFunctionName());
        Assertions.assertFalse(stmt.isGlobalFunction());
    }

    @Test
    public void testParseWithArgsCustProp() throws Exception {
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create global function echo_python(string)", ctx);
        Assertions.assertNotNull(stmt.getFunctionRef());
        Assertions.assertNotNull(stmt.getArgsDef());
        Assertions.assertEquals("echo_python", stmt.getFunctionRef().getFunctionName());
        Assertions.assertTrue(stmt.isGlobalFunction());
    }

    @Test
    public void testParseGlobal() throws Exception {
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create global function my_global(int)", ctx);
        Assertions.assertTrue(stmt.isGlobalFunction());
    }

    @Test
    public void testParseGlobalRejectsDbQualifier() {
        assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "show create global function some_db.my_global(int)", ctx));
    }

    @Test
    public void testMetaDataColumns() throws Exception {
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function echo(int)", ctx);
        ShowResultMetaFactory factory = new ShowResultMetaFactory();
        Assertions.assertEquals(1, factory.getMetadata(stmt).getColumnCount());
        Assertions.assertEquals("Create Function", factory.getMetadata(stmt).getColumn(0).getName());
    }

    @Test
    public void testAnalyzerRequiresDatabaseForNonGlobal() {
        // No current database and no qualifier — analyzer should fail.
        ConnectContext bareCtx = UtFrameUtils.createDefaultCtx();
        bareCtx.setDatabase(null);
        assertThrows(AnalysisException.class, () -> {
            ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                    "show create function echo(int)", bareCtx);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, bareCtx);
        });
    }

    @Test
    public void testExecuteSpecificOverload() throws Exception {
        ctx.setDatabase("test_udf_db");
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function echo(string)", ctx);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals(1, rs.getResultRows().size());
        String createSql = rs.getResultRows().get(0).get(0);
        Assertions.assertTrue(createSql.contains("CREATE FUNCTION"), createSql);
        Assertions.assertTrue(createSql.toUpperCase().contains("VARCHAR"), createSql);
    }

    @Test
    public void testExecuteUnsizedVarcharMatchesStringOverload() throws Exception {
        ctx.setDatabase("test_udf_db");
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function echo(string)", ctx);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals(1, rs.getResultRows().size());
        String createSql = rs.getResultRows().get(0).get(0);
        Assertions.assertTrue(createSql.toUpperCase().contains("VARCHAR"), createSql);
    }

    @Test
    public void testExecuteUnknownFunctionFails() {
        ctx.setDatabase("test_udf_db");
        assertThrows(SemanticException.class, () -> {
            ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                    "show create function does_not_exist(int)", ctx);
            ShowExecutor.execute(stmt, ctx);
        });
    }

    @Test
    public void testExecuteShowCreateAggregateFunction() throws Exception {
        ctx.setDatabase("test_udf_db");
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function my_agg(int)", ctx);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        String createSql = rs.getResultRows().get(0).get(0);
        Assertions.assertTrue(createSql.startsWith("CREATE AGGREGATE FUNCTION"), createSql);
        Assertions.assertTrue(createSql.contains("\"type\" = \"StarrocksJar\""), createSql);
        Assertions.assertTrue(createSql.contains("\"file\" = \"xxx\""), createSql);
        Assertions.assertTrue(createSql.contains("\"intermediate\""), createSql);
        Assertions.assertFalse(createSql.contains("\"isolation\""), createSql);
        Assertions.assertFalse(createSql.contains("\"analytic\""), createSql);
    }

    @Test
    public void testExecuteShowCreateAggregateFunctionAnalytic() throws Exception {
        ctx.setDatabase("test_udf_db");
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function my_agg_analytic(int)", ctx);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        String createSql = rs.getResultRows().get(0).get(0);
        Assertions.assertTrue(createSql.contains("\"analytic\" = \"true\""), createSql);
    }

    @Test
    public void testExecuteShowCreateAggregateFunctionShared() throws Exception {
        ctx.setDatabase("test_udf_db");
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function my_agg_shared(int)", ctx);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        String createSql = rs.getResultRows().get(0).get(0);
        Assertions.assertTrue(createSql.contains("\"isolation\" = \"shared\""), createSql);
    }

    @Test
    public void testExecuteShowCreateTableFunction() throws Exception {
        ctx.setDatabase("test_udf_db");
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function my_table_fn(int)", ctx);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        String createSql = rs.getResultRows().get(0).get(0);
        Assertions.assertTrue(createSql.startsWith("CREATE TABLE FUNCTION"), createSql);
        Assertions.assertTrue(createSql.contains("\"type\" = \"StarrocksJar\""), createSql);
        Assertions.assertTrue(createSql.contains("\"file\" = \"xxx\""), createSql);
    }

    @Test
    public void testExecuteShowCreatePythonInlineFunction() throws Exception {
        ctx.setDatabase("test_udf_db");
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function python_add(bigint)", ctx);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals(1, rs.getResultRows().size());
        String createSql = rs.getResultRows().get(0).get(0);
        Assertions.assertTrue(createSql.startsWith("CREATE FUNCTION"), createSql);
        Assertions.assertTrue(createSql.contains("\"type\" = \"Python\""), createSql);
        Assertions.assertTrue(createSql.contains("\"file\" = \"inline\""), createSql);
        Assertions.assertTrue(createSql.contains("\"symbol\" = \"add\""), createSql);
        Assertions.assertTrue(createSql.contains("\"input\" = \"arrow\""), createSql);
        Assertions.assertTrue(createSql.contains("AS $$"), createSql);
        Assertions.assertTrue(createSql.contains("import pyarrow.compute"), createSql);
        Assertions.assertTrue(createSql.contains("def add(x)"), createSql);
    }

    @Test
    public void testExecuteShowCreateGlobalFunctionRoundTripsWithoutDbPrefix() throws Exception {
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create global function my_global_udf(int)", ctx);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        String createSql = rs.getResultRows().get(0).get(0);
        Assertions.assertTrue(createSql.startsWith("CREATE GLOBAL FUNCTION"), createSql);
        Assertions.assertFalse(createSql.contains("__global_udf_db__"), createSql);
        Assertions.assertTrue(createSql.contains("my_global_udf("), createSql);
    }

    @Test
    public void testExecuteShowCreateSqlFunction() throws Exception {
        ctx.setDatabase("test_udf_db");
        ShowCreateFunctionStmt stmt = (ShowCreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(
                "show create function sql_add_one(int)", ctx);
        ShowResultSet rs = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals(1, rs.getResultRows().size());
        String createSql = rs.getResultRows().get(0).get(0);
        Assertions.assertTrue(createSql.startsWith("CREATE FUNCTION"), createSql);
        Assertions.assertTrue(createSql.contains("sql_add_one("), createSql);
        Assertions.assertTrue(createSql.contains("RETURNS"), createSql);
    }

}
