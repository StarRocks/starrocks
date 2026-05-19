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
}
