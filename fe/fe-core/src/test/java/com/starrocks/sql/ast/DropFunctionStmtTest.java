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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class DropFunctionStmtTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("drop_fn_db").useDatabase("drop_fn_db");

        starRocksAssert.withFunction(
                "CREATE FUNCTION drop_fn_db.str_udf(VARCHAR(65533)) RETURNS INT PROPERTIES " +
                        "(\"symbol\" = \"com.example.StrUdf\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        // char_udf registered with an explicit CHAR(10)
        starRocksAssert.withFunction(
                "CREATE FUNCTION drop_fn_db.char_udf(CHAR(10)) RETURNS INT PROPERTIES " +
                        "(\"symbol\" = \"com.example.CharUdf\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        // multi_udf has a non-string first arg and a sized VARCHAR second arg
        starRocksAssert.withFunction(
                "CREATE FUNCTION drop_fn_db.multi_udf(INT, VARCHAR(65533)) RETURNS INT PROPERTIES " +
                        "(\"symbol\" = \"com.example.MultiUdf\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        // Nested complex type functions — registered with explicit sizes.
        starRocksAssert.withFunction(
                "CREATE FUNCTION drop_fn_db.array_udf(ARRAY<VARCHAR(65533)>) RETURNS INT PROPERTIES " +
                        "(\"symbol\" = \"com.example.ArrayUdf\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        starRocksAssert.withFunction(
                "CREATE FUNCTION drop_fn_db.map_udf(MAP<VARCHAR(65533), INT>) RETURNS INT PROPERTIES " +
                        "(\"symbol\" = \"com.example.MapUdf\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        starRocksAssert.withFunction(
                "CREATE FUNCTION drop_fn_db.struct_udf(STRUCT<a VARCHAR(65533)>) RETURNS INT PROPERTIES " +
                        "(\"symbol\" = \"com.example.StructUdf\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
        // string_udf registered with the STRING type — internally VARCHAR(MAX_SIZE).
        starRocksAssert.withFunction(
                "CREATE FUNCTION drop_fn_db.string_udf(STRING) RETURNS INT PROPERTIES " +
                        "(\"symbol\" = \"com.example.StringUdf\", \"type\" = \"StarrocksJar\", \"file\" = \"xxx\");");
    }

    @Test
    public void testNormal() throws Exception {
        String dropFunctionSql = "DROP FUNCTION ABC.MY_UDF_JSON_GET(string, string)";
        DropFunctionStmt stmt = (DropFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                dropFunctionSql, 32).get(0);

        Assertions.assertEquals("ABC", stmt.getFunctionRef().getDbName());
        Assertions.assertEquals("my_udf_json_get", stmt.getFunctionRef().getFunctionName());
    }

    @Test
    public void testDropIfExists() throws Exception {
        String dropFunctionSql = "DROP FUNCTION IF EXISTS ABC.MY_UDF_JSON_GET(string, string)";
        DropFunctionStmt stmt = (DropFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                dropFunctionSql, 32).get(0);

        Assertions.assertEquals("ABC", stmt.getFunctionRef().getDbName());
        Assertions.assertEquals("my_udf_json_get", stmt.getFunctionRef().getFunctionName());
        Assertions.assertTrue(stmt.dropIfExists());
    }

    // DROP accepts unsized string types
    // A function registered with VARCHAR(n)/CHAR(n) must be referenceable by
    // DROP FUNCTION f(VARCHAR)/DROP FUNCTION f(CHAR).
    @Test
    public void testDropAcceptsUnsizedVarcharMatchesSizedRegistration() throws Exception {
        // str_udf was created with VARCHAR(65533); bare VARCHAR must resolve it.
        UtFrameUtils.parseStmtWithNewParser(
                "DROP FUNCTION IF EXISTS drop_fn_db.str_udf(VARCHAR)", ctx);
    }

    @Test
    public void testDropAcceptsUnsizedCharMatchesSizedRegistration() throws Exception {
        // char_udf was created with CHAR(10); bare CHAR must resolve it.
        UtFrameUtils.parseStmtWithNewParser(
                "DROP FUNCTION IF EXISTS drop_fn_db.char_udf(CHAR)", ctx);
    }

    @Test
    public void testDropAcceptsUnsizedVarcharInMultiArgSignature() throws Exception {
        // multi_udf(INT, VARCHAR(65533)): second arg referenced with bare VARCHAR.
        UtFrameUtils.parseStmtWithNewParser(
                "DROP FUNCTION IF EXISTS drop_fn_db.multi_udf(INT, VARCHAR)", ctx);
    }

    // Nested complex types follow the same rule as TypeDefAnalyzer propagates
    // requireExplicitSize=false recursively into array, map, and struct type arguments.
    @Test
    public void testDropAcceptsUnsizedVarcharInsideArray() throws Exception {
        UtFrameUtils.parseStmtWithNewParser(
                "DROP FUNCTION IF EXISTS drop_fn_db.array_udf(ARRAY<VARCHAR>)", ctx);
    }

    @Test
    public void testDropAcceptsUnsizedVarcharAsMapKey() throws Exception {
        UtFrameUtils.parseStmtWithNewParser(
                "DROP FUNCTION IF EXISTS drop_fn_db.map_udf(MAP<VARCHAR, INT>)", ctx);
    }

    @Test
    public void testDropAcceptsUnsizedVarcharInsideStruct() throws Exception {
        // Field name must match — StructType.matchesType checks names case-insensitively.
        UtFrameUtils.parseStmtWithNewParser(
                "DROP FUNCTION IF EXISTS drop_fn_db.struct_udf(STRUCT<a VARCHAR>)", ctx);
    }

    @Test
    public void testDropStringFunctionWithUnsizedVarchar() throws Exception {
        // STRING is VARCHAR(MAX_SIZE) internally — PrimitiveType.VARCHAR with max length.
        // matchesType returns true for any two string types regardless of size, so bare
        // VARCHAR must resolve a function registered with the STRING keyword.
        UtFrameUtils.parseStmtWithNewParser(
                "DROP FUNCTION IF EXISTS drop_fn_db.string_udf(VARCHAR)", ctx);
    }

    @Test
    public void testDropStringFunctionWithString() throws Exception {
        // Confirms the symmetric case: STRING also resolves STRING (no size required).
        UtFrameUtils.parseStmtWithNewParser(
                "DROP FUNCTION IF EXISTS drop_fn_db.string_udf(STRING)", ctx);
    }

    // Invalid explicit sizes are still rejected
    // The lenient path only permits the unsized form (len == -1).  An explicit
    // size of 0 is always invalid and must be rejected even in DROP/GRANT/REVOKE.
    @Test
    public void testDropRejectsVarcharCharSizeZero() {
        AnalysisException ex = assertThrows(AnalysisException.class, () ->
                UtFrameUtils.parseStmtWithNewParser(
                        "DROP FUNCTION IF EXISTS drop_fn_db.str_udf(VARCHAR(0))", ctx));
        Assertions.assertTrue(ex.getMessage().contains("Varchar size must be > 0"), ex.getMessage());
    }

    @Test
    public void testDropRejectsCharSizeZero() {
        AnalysisException ex = assertThrows(AnalysisException.class, () ->
                UtFrameUtils.parseStmtWithNewParser(
                        "DROP FUNCTION IF EXISTS drop_fn_db.char_udf(CHAR(0))", ctx));
        Assertions.assertTrue(ex.getMessage().contains("Char size must be > 0"), ex.getMessage());
    }

    // Overload / duplicate semantics
    // matchesType() treats any two string types as equivalent regardless of size,
    // so the function registry cannot hold two overloads that differ only in
    // VARCHAR/CHAR length or in VARCHAR vs CHAR. CREATE FUNCTION should
    // reject the second registration with "function already exists".
    private static ScalarFunction buildScalarFn(String fnName, com.starrocks.type.Type argType) {
        FunctionName name = new FunctionName("drop_fn_db", fnName);
        return ScalarFunction.createUdf(name, new Type[] {argType},
                IntegerType.INT, false, TFunctionBinaryType.SRJAR,
                "xxx", "com.example.Fn", "", "", true, null);
    }

    @Test
    public void testVarcharDifferentSizesCannotCoexist() {
        // str_udf(VARCHAR(65533)) was registered in @BeforeAll via utCreateFunctionMock
        // (allowExists=true). Attempting to add str_udf(VARCHAR(10)) via the strict
        // production path must be rejected because VARCHAR(65533).matchesType(VARCHAR(10)).
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("drop_fn_db");
        ScalarFunction fn = buildScalarFn("str_udf", TypeFactory.createVarcharType(10));
        StarRocksException ex = assertThrows(StarRocksException.class,
                () -> db.addFunction(fn, false, false));
        Assertions.assertTrue(ex.getMessage().contains("function already exists"), ex.getMessage());

        ScalarFunction fn1 = buildScalarFn("string_udf", TypeFactory.createVarcharType(10));
        StarRocksException ex1 = assertThrows(StarRocksException.class,
                () -> db.addFunction(fn1, false, false));
        Assertions.assertTrue(ex1.getMessage().contains("function already exists"), ex1.getMessage());

    }

    @Test
    public void testCharCannotCoexistWithVarchar() {
        // CHAR and VARCHAR are both in STRING_TYPE_LIST, so matchesType returns true
        // for any CHAR/VARCHAR pair. CHAR(10) is therefore a duplicate of VARCHAR(65533).
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("drop_fn_db");
        ScalarFunction fn = buildScalarFn("str_udf", TypeFactory.createCharType(10));
        StarRocksException ex = assertThrows(StarRocksException.class,
                () -> db.addFunction(fn, false, false));
        Assertions.assertTrue(ex.getMessage().contains("function already exists"), ex.getMessage());
    }

    @Test
    public void testDifferentBaseTypeCanCoexist() throws Exception {
        // A genuinely different overload — same name, INT arg instead of VARCHAR — is
        // not a duplicate and must be accepted.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("drop_fn_db");
        ScalarFunction fn = buildScalarFn("str_udf", IntegerType.INT);
        db.addFunction(fn, false, false);
        // Clean up so the registry is not polluted for other tests.
        db.dropFunction(new FunctionSearchDesc(
                new FunctionName("drop_fn_db", "str_udf"),
                new Type[] {IntegerType.INT}, false), false);
    }
}
