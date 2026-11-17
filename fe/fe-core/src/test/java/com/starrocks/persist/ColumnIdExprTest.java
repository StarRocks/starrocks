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

package com.starrocks.persist;

import com.starrocks.catalog.Column;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.AnyMapType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.NullType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ColumnIdExprTest {

    private static ColumnIdExpr roundTrip(String sql) {
        ColumnIdExpr expr = ColumnIdExpr.create(SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT));
        return ExpressionSerializedObject.create(expr).deserialize();
    }

    private static Type firstArgType(ColumnIdExpr expr) {
        return expr.getExpr().getChild(0).getType();
    }

    @Test
    public void testTypedArrayKeepsType() {
        String sql = "array_contains(ARRAY<DATE>['2020-01-01'], d)";
        ColumnIdExpr expr = ColumnIdExpr.create(SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT));
        Assertions.assertTrue(expr.toSql().contains("ARRAY<DATE>['2020-01-01']"), expr.toSql());

        ColumnIdExpr replayed = roundTrip(sql);
        Assertions.assertInstanceOf(ArrayExpr.class, replayed.getExpr().getChild(0));
        Assertions.assertEquals(new ArrayType(DateType.DATE), firstArgType(replayed));
        // serialization is stable after replay
        Assertions.assertEquals(expr.toSql(), replayed.toSql());
    }

    @Test
    public void testNestedAndEmptyTypedArray() {
        ColumnIdExpr replayed = roundTrip("array_length(ARRAY<ARRAY<INT>>[[1, 2]])");
        Assertions.assertEquals(new ArrayType(new ArrayType(IntegerType.INT)), firstArgType(replayed));

        replayed = roundTrip("array_length(ARRAY<INT>[])");
        Assertions.assertEquals(new ArrayType(IntegerType.INT), firstArgType(replayed));
    }

    @Test
    public void testUntypedArrayUnchanged() {
        ColumnIdExpr expr = ColumnIdExpr.create(
                SqlParser.parseSqlToExpr("array_contains(['2020-01-01'], d)", SqlModeHelper.MODE_DEFAULT));
        Assertions.assertTrue(expr.toSql().startsWith("array_contains(['2020-01-01']"), expr.toSql());
        Assertions.assertNull(firstArgType(roundTrip("array_contains(['2020-01-01'], d)")));
    }

    @Test
    public void testTypedMapKeepsType() {
        String sql = "map_keys(MAP<INT,DATE>{1:'2020-01-01'})";
        ColumnIdExpr expr = ColumnIdExpr.create(SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT));
        Assertions.assertTrue(expr.toSql().contains("MAP<INT,DATE>{1:'2020-01-01'}"), expr.toSql());

        ColumnIdExpr replayed = roundTrip(sql);
        Assertions.assertInstanceOf(MapExpr.class, replayed.getExpr().getChild(0));
        Assertions.assertEquals(new MapType(IntegerType.INT, DateType.DATE), firstArgType(replayed));
        Assertions.assertEquals(expr.toSql(), replayed.toSql());
    }

    @Test
    public void testUntypedMapUnchanged() {
        String sql = "map_keys(map{1:'2020-01-01'})";
        ColumnIdExpr expr = ColumnIdExpr.create(SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT));
        Assertions.assertTrue(expr.toSql().startsWith("map_keys(map{1:'2020-01-01'}"), expr.toSql());
        Assertions.assertEquals(AnyMapType.ANY_MAP, firstArgType(roundTrip(sql)));
    }

    @Test
    public void testNullTypeLiteralHasNoPrefix() {
        // An analyzed [] / [NULL] carries an inferred ARRAY<NULL_TYPE>, which has no SQL spelling and must not be
        // frozen into another type; the bare literal is printed and re-infers the same type.
        ColumnIdExpr emptyArray = ColumnIdExpr.create(new ArrayExpr(ArrayType.ARRAY_NULL, new ArrayList<>()));
        Assertions.assertEquals("[]", emptyArray.toSql());

        List<Expr> nulls = new ArrayList<>();
        nulls.add(new NullLiteral());
        ColumnIdExpr nestedNull = ColumnIdExpr.create(
                new ArrayExpr(new ArrayType(ArrayType.ARRAY_NULL), List.of(new ArrayExpr(ArrayType.ARRAY_NULL, nulls))));
        Assertions.assertEquals("[[NULL]]", nestedNull.toSql());

        List<Expr> kv = new ArrayList<>();
        kv.add(SqlParser.parseSqlToExpr("1", SqlModeHelper.MODE_DEFAULT));
        kv.add(new NullLiteral());
        ColumnIdExpr nullValueMap = ColumnIdExpr.create(new MapExpr(new MapType(IntegerType.TINYINT, NullType.NULL), kv));
        Assertions.assertEquals("map{1:NULL}", nullValueMap.toSql());
    }

    @Test
    public void testGeneratedColumnGsonRoundTrip() {
        Column d = new Column("d", DateType.DATE);
        Column x = new Column("x", BooleanType.BOOLEAN);
        Expr expr = SqlParser.parseSqlToExpr("array_contains(ARRAY<DATE>['2020-01-01'], d)",
                SqlModeHelper.MODE_DEFAULT);
        x.setGeneratedColumnExpr(ColumnIdExpr.create(expr));

        String json = GsonUtils.GSON.toJson(x);
        Column replayed = GsonUtils.GSON.fromJson(json, Column.class);
        Assertions.assertTrue(replayed.generatedColumnExprToString().contains("ARRAY<DATE>['2020-01-01']"),
                replayed.generatedColumnExprToString());

        List<Column> schema = new ArrayList<>();
        schema.add(d);
        schema.add(replayed);
        Expr replayedExpr = replayed.getGeneratedColumnExpr(schema);
        Assertions.assertEquals(new ArrayType(DateType.DATE), replayedExpr.getChild(0).getType());
    }
}
