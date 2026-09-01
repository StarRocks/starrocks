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


package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReduceCastRuleTest {

    @Test
    public void testDuplicateCastReduceFail() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator =
                new CastOperator(IntegerType.BIGINT, new CastOperator(IntegerType.INT, ConstantOperator.createChar("hello")));

        ScalarOperator result = rule.apply(operator, null);

        assertEquals(OperatorType.CALL, result.getOpType());
        assertTrue(result instanceof CastOperator);

        assertTrue(result.getType().isBigint());
        assertEquals(OperatorType.CALL, result.getChild(0).getOpType());
        assertTrue(result.getChild(0).getChild(0).getType().isChar());
    }

    @Test
    public void testDuplicateCastReduceSuccess() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator = new CastOperator(IntegerType.BIGINT,
                new CastOperator(IntegerType.INT, ConstantOperator.createSmallInt((short) 3)));

        ScalarOperator result = rule.apply(operator, null);

        assertEquals(OperatorType.CALL, result.getOpType());
        assertTrue(result instanceof CastOperator);

        assertTrue(result.getType().isBigint());
        assertEquals(OperatorType.CONSTANT, result.getChild(0).getOpType());
        assertTrue(result.getChild(0).getType().isNumericType());
    }

    @Test
    public void testBooleanWithDecreasingCast() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator =
                new CastOperator(BooleanType.BOOLEAN, new CastOperator(IntegerType.INT,
                        ConstantOperator.createLargeInt(new BigInteger("1000000000000000000"))));

        ScalarOperator result = rule.apply(operator, null);

        assertEquals(OperatorType.CALL, result.getOpType());
        assertTrue(result instanceof CastOperator);

        assertTrue(result.getType().isBoolean());
        ScalarOperator child = result.getChild(0);
        assertEquals(OperatorType.CALL, child.getOpType());
        assertTrue(child instanceof CastOperator);
        ScalarOperator grandChild = child.getChild(0);
        assertEquals(OperatorType.CONSTANT, grandChild.getOpType());
    }

    @Test
    public void testFloatingPointToIntegerCastNotReduced() {
        // see issue #51545: cast(cast(<double> as bigint) as string) returned the un-truncated
        // double (e.g. "3.6666666666666665") because the intermediate bigint cast was dropped.
        // The fractional part must be truncated by the bigint cast, so the chain cannot be reduced.
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator = new CastOperator(VarcharType.VARCHAR,
                new CastOperator(IntegerType.BIGINT, ConstantOperator.createDouble(3.6666666666666665)));

        ScalarOperator result = rule.apply(operator, null);

        assertTrue(result instanceof CastOperator);
        assertTrue(result.getType().isVarchar());
        // the intermediate cast(... as bigint) must be preserved
        assertTrue(result.getChild(0) instanceof CastOperator);
        assertTrue(result.getChild(0).getType().isBigint());
        assertEquals(OperatorType.CONSTANT, result.getChild(0).getChild(0).getOpType());
        assertTrue(result.getChild(0).getChild(0).getType().isFloatingPointType());
    }

    @Test
    public void testFloatingPointToIntegerToNarrowerIntegerStillReduced() {
        // when the outer cast is an integer no wider than the intermediate integer, the outer
        // cast re-applies the truncation, so float -> integer -> integer is safe to reduce.
        // cast(cast(<double> as bigint) as int) -> cast(<double> as int)
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator = new CastOperator(IntegerType.INT,
                new CastOperator(IntegerType.BIGINT, ConstantOperator.createDouble(3.6666666666666665)));

        ScalarOperator result = rule.apply(operator, null);

        assertTrue(result instanceof CastOperator);
        assertTrue(result.getType().isInt());
        // the intermediate cast(... as bigint) is removed, leaving cast(<double> as int)
        assertEquals(OperatorType.CONSTANT, result.getChild(0).getOpType());
        assertTrue(result.getChild(0).getType().isFloatingPointType());
    }

    @Test
    public void testFloatingPointToIntegerToWiderIntegerNotReduced() {
        // the outer integer is wider than the intermediate one, so dropping the intermediate cast
        // would skip its overflow boundary, e.g. cast(cast(<double> as int) as bigint) differs from
        // cast(<double> as bigint) for values outside the int range. The chain must not be reduced.
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator = new CastOperator(IntegerType.BIGINT,
                new CastOperator(IntegerType.INT, ConstantOperator.createDouble(3.0e9)));

        ScalarOperator result = rule.apply(operator, null);

        assertTrue(result instanceof CastOperator);
        assertTrue(result.getType().isBigint());
        assertTrue(result.getChild(0) instanceof CastOperator);
        assertTrue(result.getChild(0).getType().isInt());
    }

    @Test
    public void testLossyFloatingPointMiddleCastNotReduced() {
        // a lossy floating-point intermediate cast must be preserved, otherwise the outer cast sees
        // the un-narrowed value. double -> float and bigint -> float are both lossy at the same slot
        // size, so neither the slot-size nor a type-shape check catches them - only the lossless gate.
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        // cast(cast(16777217.5 as float) as int): float rounds to 16777218.0 then truncates to
        // 16777218; dropping the float cast (cast(16777217.5 as int)) would wrongly give 16777217.
        ScalarOperator doubleToFloatToInt = new CastOperator(IntegerType.INT,
                new CastOperator(FloatType.FLOAT, ConstantOperator.createDouble(16777217.5)));
        ScalarOperator r1 = rule.apply(doubleToFloatToInt, null);
        assertTrue(r1.getChild(0) instanceof CastOperator);
        assertTrue(r1.getChild(0).getType().isFloat());

        // cast(cast(<bigint> as float) as int): bigint -> float loses precision for large values.
        ScalarOperator bigintToFloatToInt = new CastOperator(IntegerType.INT,
                new CastOperator(FloatType.FLOAT, ConstantOperator.createBigint(9007199254740993L)));
        ScalarOperator r2 = rule.apply(bigintToFloatToInt, null);
        assertTrue(r2.getChild(0) instanceof CastOperator);
        assertTrue(r2.getChild(0).getType().isFloat());
    }

    @Test
    public void testLosslessWideningMiddleCastStillReduced() {
        // lossless widening intermediate casts can still be reduced: float -> double and int -> bigint.
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        // cast(cast(<float> as double) as varchar) -> cast(<float> as varchar)
        ScalarOperator floatToDoubleToVarchar = new CastOperator(VarcharType.VARCHAR,
                new CastOperator(FloatType.DOUBLE, ConstantOperator.createFloat(1.5)));
        ScalarOperator r1 = rule.apply(floatToDoubleToVarchar, null);
        assertEquals(OperatorType.CONSTANT, r1.getChild(0).getOpType());
        assertTrue(r1.getChild(0).getType().isFloat());

        // cast(cast(<int> as bigint) as varchar) -> cast(<int> as varchar)
        ScalarOperator intToBigintToVarchar = new CastOperator(VarcharType.VARCHAR,
                new CastOperator(IntegerType.BIGINT, ConstantOperator.createInt(7)));
        ScalarOperator r2 = rule.apply(intToBigintToVarchar, null);
        assertEquals(OperatorType.CONSTANT, r2.getChild(0).getOpType());
        assertTrue(r2.getChild(0).getType().isInt());
    }

    @Test
    public void testWideIntegerToDoubleMiddleCastNotReduced() {
        // BIGINT/LARGEINT -> DOUBLE loses integer precision beyond 2^53, but isImplicitlyCastable
        // reports it as lossless because the compatibility matrix widens to DOUBLE. The middle cast
        // must be preserved, otherwise cast(cast(9007199254740993 as double) as varchar) would print
        // the exact integer instead of the rounded double 9007199254740992.
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator bigintToDoubleToVarchar = new CastOperator(VarcharType.VARCHAR,
                new CastOperator(FloatType.DOUBLE, ConstantOperator.createBigint(9007199254740993L)));
        ScalarOperator r1 = rule.apply(bigintToDoubleToVarchar, null);
        assertTrue(r1.getChild(0) instanceof CastOperator);
        assertTrue(r1.getChild(0).getType().isDouble());

        ScalarOperator largeintToDoubleToVarchar = new CastOperator(VarcharType.VARCHAR,
                new CastOperator(FloatType.DOUBLE, ConstantOperator.createLargeInt(new BigInteger("9007199254740993"))));
        ScalarOperator r2 = rule.apply(largeintToDoubleToVarchar, null);
        assertTrue(r2.getChild(0) instanceof CastOperator);
        assertTrue(r2.getChild(0).getType().isDouble());
    }

    @Test
    public void testNarrowIntegerToDoubleMiddleCastStillReduced() {
        // INT -> DOUBLE is exact (int fits in the 53-bit significand), so the chain can be reduced.
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator intToDoubleToVarchar = new CastOperator(VarcharType.VARCHAR,
                new CastOperator(FloatType.DOUBLE, ConstantOperator.createInt(123456789)));
        ScalarOperator result = rule.apply(intToDoubleToVarchar, null);
        assertEquals(OperatorType.CONSTANT, result.getChild(0).getOpType());
        assertTrue(result.getChild(0).getType().isInt());
    }

    @Test
    public void testSameTypeCast() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator =
                new CastOperator(CharType.CHAR, ConstantOperator.createChar("hello"));

        ScalarOperator result = rule.apply(operator, null);

        assertTrue(result.getType().isChar());
        assertEquals(OperatorType.CONSTANT, result.getOpType());
    }

    @Test
    public void testVarcharLengthIsNotInheritedByDefault() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();
        ScalarOperator child =
                new ColumnRefOperator(0, TypeFactory.createVarcharType(3), "id_varchar", false);
        ScalarOperator operator = new CastOperator(TypeFactory.createVarcharType(10), child);

        ScalarOperator result = rule.apply(operator, null);

        assertTrue(result instanceof ColumnRefOperator);
        assertEquals(3, ((ScalarType) result.getType()).getLength());
        assertEquals(3, ((ScalarType) child.getType()).getLength());
    }

    @Test
    public void testVarcharLengthCanBeInheritedAfterReduceCast() {
        boolean prevInheritance = GlobalVariable.isEnableReduceCastVarcharLengthInheritance();
        GlobalVariable.setEnableReduceCastVarcharLengthInheritance(true);

        try {
            ScalarOperatorRewriteRule rule = new ReduceCastRule();
            ScalarOperator child =
                    new ColumnRefOperator(0, TypeFactory.createVarcharType(3), "id_varchar", false);
            ScalarOperator operator = new CastOperator(TypeFactory.createVarcharType(10), child);

            ScalarOperator result = rule.apply(operator, null);

            assertTrue(result instanceof ColumnRefOperator);
            assertEquals(10, ((ScalarType) result.getType()).getLength());
            assertEquals(10, ((ScalarType) child.getType()).getLength());
            Assertions.assertSame(child, result);
        } finally {
            GlobalVariable.setEnableReduceCastVarcharLengthInheritance(prevInheritance);
        }
    }

    @Test
    public void testReduceCastToVarcharInDatetimeCast() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();
        {
            // cast(cast(id_date as varchar) as datetime) -> cast(id_date as datetime)
            ScalarOperator operator = new CastOperator(DateType.DATETIME,
                    new CastOperator(VarcharType.VARCHAR, new ColumnRefOperator(0, DateType.DATE, "id_date", false)));

            ScalarOperator result = rule.apply(operator, null);

            assertTrue(result.getType().isDatetime());
            assertEquals(DateType.DATE, result.getChild(0).getType());
        }
        {
            // cast(cast(id_datetime as varchar) as date) -> cast(id_datetime as date)
            ScalarOperator operator = new CastOperator(DateType.DATE,
                    new CastOperator(VarcharType.VARCHAR,
                            new ColumnRefOperator(0, DateType.DATETIME, "id_datetime", false)));

            ScalarOperator result = rule.apply(operator, null);

            assertTrue(result.getType().isDate());
            assertEquals(DateType.DATETIME, result.getChild(0).getType());
        }
        {
            // cast(cast(id_datetime as varchar) as datetime) -> id_datetime
            ScalarOperator operator = new CastOperator(DateType.DATETIME,
                    new CastOperator(VarcharType.VARCHAR,
                            new ColumnRefOperator(0, DateType.DATETIME, "id_datetime", false)));

            ScalarOperator result = rule.apply(operator, null);

            assertTrue(result.getType().isDatetime());
            assertTrue(result instanceof ColumnRefOperator);
        }
        {
            // cast(cast(id_date as varchar) as date) -> id_date
            ScalarOperator operator = new CastOperator(DateType.DATE,
                    new CastOperator(VarcharType.VARCHAR, new ColumnRefOperator(0, DateType.DATE, "id_date", false)));

            ScalarOperator result = rule.apply(operator, null);

            assertTrue(result.getType().isDate());
            assertTrue(result instanceof ColumnRefOperator);
        }
    }

    private ConstantOperator createConstOperatorFromType(Type type) {
        if (type.isTinyint()) {
            return ConstantOperator.createTinyInt(Byte.MAX_VALUE);
        }
        if (type.isSmallint()) {
            return ConstantOperator.createSmallInt(Short.MAX_VALUE);
        }
        if (type.isInt()) {
            return ConstantOperator.createInt(Integer.MAX_VALUE);
        }
        if (type.isBigint()) {
            return ConstantOperator.createBigint(Long.MAX_VALUE);
        }
        if (type.isLargeint()) {
            return ConstantOperator.createLargeInt(BigInteger.ONE);
        }
        if (type.isDecimalV3()) {
            return ConstantOperator.createDecimal(BigDecimal.ONE, type);
        }
        return ConstantOperator.createNull(type);
    }

    @Test
    public void testBinaryPredicateInvolvingDecimalSuccess() {
        Type[][] typeListList = new Type[][] {
                {IntegerType.TINYINT, IntegerType.SMALLINT, TypeFactory.createDecimalV3NarrowestType(16, 9)},
                {IntegerType.TINYINT, IntegerType.SMALLINT, TypeFactory.createDecimalV3NarrowestType(9, 0)},
                {TypeFactory.createDecimalV3NarrowestType(4, 0), IntegerType.SMALLINT, IntegerType.BIGINT},
                {TypeFactory.createDecimalV3NarrowestType(18, 0), IntegerType.BIGINT, IntegerType.LARGEINT},
                {TypeFactory.createDecimalV3NarrowestType(2, 0), IntegerType.TINYINT, IntegerType.INT},
        };

        ScalarOperatorRewriteRule reduceCastRule = new ReduceCastRule();
        ScalarOperatorRewriteRule foldConstantsRule = new FoldConstantsRule();
        for (Type[] types : typeListList) {
            ScalarOperator binPredRhs = createConstOperatorFromType(types[0]);
            ScalarOperator castChild = createConstOperatorFromType(types[1]);
            CastOperator binPredLhs = new CastOperator(types[2], castChild);
            BinaryPredicateOperator binPred = new BinaryPredicateOperator(
                    BinaryType.GE, binPredLhs, binPredRhs);
            ScalarOperator result = reduceCastRule.apply(binPred, null);
            result = foldConstantsRule.apply(result, null);
            Assertions.assertTrue(result instanceof ConstantOperator);
        }
    }

    @Test
    public void testBinaryPredicateInvolvingDecimalFail() {
        Type[][] typeListList = new Type[][] {
                {IntegerType.TINYINT, IntegerType.SMALLINT, TypeFactory.createDecimalV3NarrowestType(13, 9)},
                {IntegerType.INT, IntegerType.SMALLINT, TypeFactory.createDecimalV3NarrowestType(9, 0)},
        };

        ScalarOperatorRewriteRule reduceCastRule = new ReduceCastRule();
        ScalarOperatorRewriteRule foldConstantsRule = new FoldConstantsRule();
        for (Type[] types : typeListList) {
            ScalarOperator binPredRhs = createConstOperatorFromType(types[0]);
            ScalarOperator castChild = createConstOperatorFromType(types[1]);
            CastOperator binPredLhs = new CastOperator(types[2], castChild);
            BinaryPredicateOperator binPred = new BinaryPredicateOperator(
                    BinaryType.GE, binPredLhs, binPredRhs);
            ScalarOperator result = reduceCastRule.apply(binPred, null);
            result = foldConstantsRule.apply(result, null);
            Assertions.assertFalse(result instanceof ConstantOperator, Arrays.toString(types));
        }

        typeListList = new Type[][] {
                {TypeFactory.createDecimalV3NarrowestType(6, 0), IntegerType.SMALLINT, IntegerType.BIGINT},
                {TypeFactory.createDecimalV3NarrowestType(19, 0), IntegerType.BIGINT, IntegerType.LARGEINT},
                {TypeFactory.createDecimalV3NarrowestType(10, 0), IntegerType.TINYINT, IntegerType.INT},
        };

        for (Type[] types : typeListList) {
            ScalarOperator binPredRhs = createConstOperatorFromType(types[0]);
            ScalarOperator castChild = createConstOperatorFromType(types[1]);
            CastOperator binPredLhs = new CastOperator(types[2], castChild);
            BinaryPredicateOperator binPred = new BinaryPredicateOperator(
                    BinaryType.GE, binPredLhs, binPredRhs);
            ScalarOperator result = reduceCastRule.apply(binPred, null);
            result = foldConstantsRule.apply(result, null);
            Assertions.assertTrue(result instanceof ConstantOperator, Arrays.toString(types));
        }
    }

    @Test
    public void testReduceDateToDatetimeCast() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        ReduceCastRule reduceCastRule = new ReduceCastRule();
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATETIME, new ColumnRefOperator(0, DateType.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 11:11:11", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.GE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.GE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-29",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATETIME, new ColumnRefOperator(0, DateType.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 00:00:00", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.GE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.GE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-28",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATETIME, new ColumnRefOperator(0, DateType.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 00:00:00", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.LE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.LE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-28",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATETIME, new ColumnRefOperator(0, DateType.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 11:11:11", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.LT, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.LE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-28",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATETIME, new ColumnRefOperator(0, DateType.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 00:00:00", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.LT, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.LE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-27",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATETIME, new ColumnRefOperator(0, DateType.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 00:00:00", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.EQ, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.EQ,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-28",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATETIME, new ColumnRefOperator(0, DateType.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-29 11:11:11", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.EQ, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertSame(beforeOptimize, afterOptimize);
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATETIME, new ColumnRefOperator(0, DateType.DATE, "id_date", false));
            ConstantOperator constantOperator = ConstantOperator.createNull(DateType.DATETIME);
            BinaryPredicateOperator beforeOptimize = BinaryPredicateOperator.ge(castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);
            Assertions.assertSame(beforeOptimize, afterOptimize);
        }
    }

    @Test
    public void testReduceDatetimeToDateCast() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        ReduceCastRule reduceCastRule = new ReduceCastRule();
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATE, new ColumnRefOperator(0, DateType.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.GE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.GE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-28 00:00:00",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATE, new ColumnRefOperator(0, DateType.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.GT, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.GE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-29 00:00:00",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATE, new ColumnRefOperator(0, DateType.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.LE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.LT,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-29 00:00:00",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATE, new ColumnRefOperator(0, DateType.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.LT, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.LT,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assertions.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-28 00:00:00",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATE, new ColumnRefOperator(0, DateType.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryType.EQ, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assertions.assertTrue(afterOptimize instanceof CompoundPredicateOperator);
            Assertions.assertTrue(((CompoundPredicateOperator) afterOptimize).isAnd());
            ScalarOperator left = afterOptimize.getChild(0);
            ScalarOperator right = afterOptimize.getChild(1);
            Assertions.assertTrue(left instanceof BinaryPredicateOperator);
            Assertions.assertTrue(right instanceof BinaryPredicateOperator);
            Assertions.assertEquals(BinaryType.GE,
                    ((BinaryPredicateOperator) left).getBinaryType());
            Assertions.assertTrue(left.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(left.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-28 00:00:00",
                    ((ConstantOperator) left.getChild(1)).getDatetime().format(formatter));
            Assertions.assertEquals(BinaryType.LT,
                    ((BinaryPredicateOperator) right).getBinaryType());
            Assertions.assertTrue(right.getChild(0) instanceof ColumnRefOperator);
            Assertions.assertTrue(right.getChild(1) instanceof ConstantOperator);
            Assertions.assertEquals("2021-12-29 00:00:00",
                    ((ConstantOperator) right.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(DateType.DATE, new ColumnRefOperator(0, DateType.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator = ConstantOperator.createNull(DateType.DATE);
            BinaryPredicateOperator beforeOptimize = BinaryPredicateOperator.lt(castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(beforeOptimize, null);
            Assertions.assertSame(beforeOptimize, afterOptimize);
        }
    }

    @Test
    public void testPrecisionLoss() {
        ReduceCastRule rule = new ReduceCastRule();
        // cast(96.1) as int = 96, we can't change it into 96.1 = cast(96) as double
        ScalarOperator castOperator = new CastOperator(IntegerType.INT, ConstantOperator.createDouble(96.1));
        BinaryPredicateOperator beforeOptimize =
                BinaryPredicateOperator.eq(castOperator, ConstantOperator.createInt(96));
        ScalarOperator result = rule.apply(beforeOptimize, null);
        Assertions.assertTrue(result.getChild(0) instanceof CastOperator);
    }
}
