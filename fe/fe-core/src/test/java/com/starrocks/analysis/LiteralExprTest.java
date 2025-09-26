// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.TimeZone;

public class LiteralExprTest {

    @BeforeAll
    public static void setUp() {
        TimeZone tz = TimeZone.getTimeZone("ETC/GMT-0");
        TimeZone.setDefault(tz);
    }

    @Test
    public void testCreateDefaultNullType() throws Exception {
        Type type = Type.NULL;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof NullLiteral);
    }

    @Test
    public void testCreateDefaultBooleanType() throws Exception {
        Type type = Type.BOOLEAN;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof BoolLiteral);
        Assertions.assertEquals(false, ((BoolLiteral) expr).getValue());
    }

    @Test
    public void testCreateDefaultIntType() throws Exception {
        Type type = Type.INT;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof IntLiteral);
        Assertions.assertEquals(0, ((IntLiteral) expr).getLongValue());
    }

    @Test
    public void testCreateDefaultBigIntType() throws Exception {
        Type type = Type.BIGINT;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof IntLiteral);
        Assertions.assertEquals(0, ((IntLiteral) expr).getLongValue());
    }

    @Test
    public void testCreateDefaultLargeIntType() throws Exception {
        Type type = Type.LARGEINT;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof LargeIntLiteral);
        Assertions.assertEquals(BigInteger.ZERO, ((LargeIntLiteral) expr).getValue());
    }

    @Test
    public void testCreateDefaultFloatType() throws Exception {
        Type type = Type.FLOAT;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof FloatLiteral);
        Assertions.assertEquals(0.0, ((FloatLiteral) expr).getValue());
    }

    @Test
    public void testCreateDefaultDoubleType() throws Exception {
        Type type = Type.DOUBLE;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof FloatLiteral);
        Assertions.assertEquals(0.0, ((FloatLiteral) expr).getValue());
    }

    @Test
    public void testCreateDefaultDecimalType() throws Exception {
        Type type = Type.DECIMALV2;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof DecimalLiteral);
        Assertions.assertEquals("0", ((DecimalLiteral) expr).getStringValue());
    }

    @Test
    public void testCreateDefaultVarcharType() throws Exception {
        Type type = Type.VARCHAR;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof StringLiteral);
        Assertions.assertEquals("", ((StringLiteral) expr).getValue());
    }

    @Test
    public void testCreateDefaultCharType() throws Exception {
        Type type = Type.CHAR;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof StringLiteral);
        Assertions.assertEquals("", ((StringLiteral) expr).getValue());
    }

    @Test
    public void testCreateDefaultDateType() throws Exception {
        Type type = Type.DATE;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof DateLiteral);
        Assertions.assertEquals("1970-01-01", ((DateLiteral) expr).getStringValue());
    }

    @Test
    public void testCreateDefaultDateTimeType() throws Exception {
        Type type = Type.DATETIME;
        LiteralExpr expr = LiteralExpr.createDefault(type);
        Assertions.assertTrue(expr instanceof DateLiteral);
        Assertions.assertEquals("1970-01-01 00:00:00", ((DateLiteral) expr).getStringValue());
    }

}
