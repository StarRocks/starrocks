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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.VarBinaryLiteral;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConstantOperatorConvertorTest {

    @Test
    public void testNullConvertsToNullLiteral() {
        ConstantOperator op = ConstantOperator.createNull(BooleanType.BOOLEAN);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(NullLiteral.class, result);
        assertEquals(BooleanType.BOOLEAN, result.getType());
    }

    @Test
    public void testNullWithIntegerType() {
        ConstantOperator op = ConstantOperator.createNull(IntegerType.INT);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(NullLiteral.class, result);
        assertEquals(IntegerType.INT, result.getType());
    }

    @Test
    public void testBooleanTrue() {
        ConstantOperator op = ConstantOperator.createBoolean(true);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(BoolLiteral.class, result);
        assertEquals(true, ((BoolLiteral) result).getValue());
    }

    @Test
    public void testBooleanFalse() {
        ConstantOperator op = ConstantOperator.createBoolean(false);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(BoolLiteral.class, result);
        assertEquals(false, ((BoolLiteral) result).getValue());
    }

    @Test
    public void testTinyInt() {
        ConstantOperator op = ConstantOperator.createTinyInt((byte) 42);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(IntLiteral.class, result);
        assertEquals(IntegerType.TINYINT, result.getType());
        assertEquals(42L, ((IntLiteral) result).getValue());
    }

    @Test
    public void testSmallInt() {
        ConstantOperator op = ConstantOperator.createSmallInt((short) 1000);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(IntLiteral.class, result);
        assertEquals(IntegerType.SMALLINT, result.getType());
        assertEquals(1000L, ((IntLiteral) result).getValue());
    }

    @Test
    public void testInt() {
        ConstantOperator op = ConstantOperator.createInt(123456);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(IntLiteral.class, result);
        assertEquals(IntegerType.INT, result.getType());
        assertEquals(123456L, ((IntLiteral) result).getValue());
    }

    @Test
    public void testBigint() {
        ConstantOperator op = ConstantOperator.createBigint(9876543210L);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(IntLiteral.class, result);
        assertEquals(IntegerType.BIGINT, result.getType());
        assertEquals(9876543210L, ((IntLiteral) result).getValue());
    }

    @Test
    public void testLargeInt() {
        BigInteger bigVal = new BigInteger("123456789012345678901234567890");
        ConstantOperator op = ConstantOperator.createLargeInt(bigVal);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(LargeIntLiteral.class, result);
        assertEquals(bigVal, ((LargeIntLiteral) result).getValue());
    }

    @Test
    public void testFloat() throws Exception {
        ConstantOperator op = ConstantOperator.createFloat(3.14);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(FloatLiteral.class, result);
        assertEquals(FloatType.FLOAT, result.getType());
        assertEquals(3.14, ((FloatLiteral) result).getValue(), 1e-9);
    }

    @Test
    public void testDouble() throws Exception {
        ConstantOperator op = ConstantOperator.createDouble(2.718281828);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(FloatLiteral.class, result);
        assertEquals(FloatType.DOUBLE, result.getType());
        assertEquals(2.718281828, ((FloatLiteral) result).getValue(), 1e-9);
    }

    @Test
    public void testDate() throws Exception {
        LocalDateTime date = LocalDateTime.of(2024, 3, 15, 0, 0, 0);
        ConstantOperator op = ConstantOperator.createDate(date);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(DateLiteral.class, result);
        DateLiteral dateLiteral = (DateLiteral) result;
        assertEquals(DateType.DATE, result.getType());
        assertEquals(2024L, dateLiteral.getYear());
        assertEquals(3L, dateLiteral.getMonth());
        assertEquals(15L, dateLiteral.getDay());
    }

    @Test
    public void testDatetime() throws Exception {
        LocalDateTime dt = LocalDateTime.of(2024, 6, 20, 14, 30, 45);
        ConstantOperator op = ConstantOperator.createDatetime(dt);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(DateLiteral.class, result);
        DateLiteral dateLiteral = (DateLiteral) result;
        assertEquals(DateType.DATETIME, result.getType());
        assertEquals(2024L, dateLiteral.getYear());
        assertEquals(6L, dateLiteral.getMonth());
        assertEquals(20L, dateLiteral.getDay());
        assertEquals(14L, dateLiteral.getHour());
        assertEquals(30L, dateLiteral.getMinute());
        assertEquals(45L, dateLiteral.getSecond());
    }

    @Test
    public void testDecimalV2() {
        BigDecimal decimal = new BigDecimal("123.456");
        ConstantOperator op = ConstantOperator.createDecimal(decimal, DecimalType.DECIMALV2);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(DecimalLiteral.class, result);
    }

    @Test
    public void testDecimalV3() {
        BigDecimal decimal = new BigDecimal("99.99");
        ScalarType decimalType = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        ConstantOperator op = ConstantOperator.createDecimal(decimal, decimalType);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(DecimalLiteral.class, result);
    }

    @Test
    public void testVarchar() {
        ConstantOperator op = ConstantOperator.createVarchar("hello world");
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(StringLiteral.class, result);
        assertEquals("hello world", ((StringLiteral) result).getValue());
    }

    @Test
    public void testChar() {
        ConstantOperator op = ConstantOperator.createChar("abc");
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(StringLiteral.class, result);
        assertEquals("abc", ((StringLiteral) result).getValue());
    }

    @Test
    public void testVarcharWithType() {
        ConstantOperator op = ConstantOperator.createChar("test", new VarcharType(20));
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(StringLiteral.class, result);
        assertEquals("test", ((StringLiteral) result).getValue());
    }

    @Test
    public void testVarbinary() {
        byte[] bytes = new byte[] {0x01, 0x02, 0x03};
        ConstantOperator op = ConstantOperator.createBinary(bytes, com.starrocks.type.VarbinaryType.VARBINARY);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(VarBinaryLiteral.class, result);
        assertArrayEquals(bytes, ((VarBinaryLiteral) result).getValue());
    }

    @Test
    public void testUnsupportedTypeThrows() {
        // HLL is not a supported constant type for literal conversion
        ConstantOperator op = ConstantOperator.createObject("dummy", com.starrocks.type.HLLType.HLL);
        assertThrows(UnsupportedOperationException.class, () -> ConstantOperatorConvertor.toLiteralExpr(op));
    }
}
