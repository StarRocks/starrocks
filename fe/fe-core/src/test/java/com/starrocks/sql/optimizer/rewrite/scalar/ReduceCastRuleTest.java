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

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReduceCastRuleTest {

    @Test
    public void testDuplicateCastReduceFail() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator =
                new CastOperator(Type.BIGINT, new CastOperator(Type.INT, ConstantOperator.createChar("hello")));

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

        ScalarOperator operator =
                new CastOperator(Type.BIGINT, new CastOperator(Type.INT, ConstantOperator.createSmallInt((short) 3)));

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
                new CastOperator(Type.BOOLEAN, new CastOperator(Type.INT,
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
    public void testSameTypeCast() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator =
                new CastOperator(Type.CHAR, ConstantOperator.createChar("hello"));

        ScalarOperator result = rule.apply(operator, null);

        assertTrue(result.getType().isChar());
        assertEquals(OperatorType.CONSTANT, result.getOpType());
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
                {Type.TINYINT, Type.SMALLINT, ScalarType.createDecimalV3NarrowestType(16, 9)},
                {Type.TINYINT, Type.SMALLINT, ScalarType.createDecimalV3NarrowestType(9, 0)},
                {ScalarType.createDecimalV3NarrowestType(4, 0), Type.SMALLINT, Type.BIGINT},
                {ScalarType.createDecimalV3NarrowestType(18, 0), Type.BIGINT, Type.LARGEINT},
                {ScalarType.createDecimalV3NarrowestType(2, 0), Type.TINYINT, Type.INT},
        };

        ScalarOperatorRewriteRule reduceCastRule = new ReduceCastRule();
        ScalarOperatorRewriteRule foldConstantsRule = new FoldConstantsRule();
        for (Type[] types : typeListList) {
            ScalarOperator binPredRhs = createConstOperatorFromType(types[0]);
            ScalarOperator castChild = createConstOperatorFromType(types[1]);
            CastOperator binPredLhs = new CastOperator(types[2], castChild);
            BinaryPredicateOperator binPred = new BinaryPredicateOperator(
                    BinaryPredicateOperator.BinaryType.GE, binPredLhs, binPredRhs);
            ScalarOperator result = reduceCastRule.apply(binPred, null);
            result = foldConstantsRule.apply(result, null);
            Assert.assertTrue(result instanceof ConstantOperator);
        }
    }

    @Test
    public void testBinaryPredicateInvolvingDecimalFail() {
        Type[][] typeListList = new Type[][] {
                {Type.TINYINT, Type.SMALLINT, ScalarType.createDecimalV3NarrowestType(13, 9)},
                {Type.INT, Type.SMALLINT, ScalarType.createDecimalV3NarrowestType(9, 0)},
                {ScalarType.createDecimalV3NarrowestType(6, 0), Type.SMALLINT, Type.BIGINT},
                {ScalarType.createDecimalV3NarrowestType(19, 0), Type.BIGINT, Type.LARGEINT},
                {ScalarType.createDecimalV3NarrowestType(10, 0), Type.TINYINT, Type.INT},
        };

        ScalarOperatorRewriteRule reduceCastRule = new ReduceCastRule();
        ScalarOperatorRewriteRule foldConstantsRule = new FoldConstantsRule();
        for (Type[] types : typeListList) {
            ScalarOperator binPredRhs = createConstOperatorFromType(types[0]);
            ScalarOperator castChild = createConstOperatorFromType(types[1]);
            CastOperator binPredLhs = new CastOperator(types[2], castChild);
            BinaryPredicateOperator binPred = new BinaryPredicateOperator(
                    BinaryPredicateOperator.BinaryType.GE, binPredLhs, binPredRhs);
            ScalarOperator result = reduceCastRule.apply(binPred, null);
            result = foldConstantsRule.apply(result, null);
            Assert.assertTrue(!(result instanceof ConstantOperator));
        }
    }

    @Test
    public void testReduceDateToDatetimeCast() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        ReduceCastRule reduceCastRule = new ReduceCastRule();
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATETIME, new ColumnRefOperator(0, Type.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 11:11:11", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.GE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-29",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATETIME, new ColumnRefOperator(0, Type.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 00:00:00", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.GE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-28",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATETIME, new ColumnRefOperator(0, Type.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 00:00:00", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.LE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-28",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATETIME, new ColumnRefOperator(0, Type.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 11:11:11", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.LE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-28",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATETIME, new ColumnRefOperator(0, Type.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 00:00:00", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.LE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-27",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATETIME, new ColumnRefOperator(0, Type.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-28 00:00:00", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.EQ,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-28",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDate().format(dateFormat));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATETIME, new ColumnRefOperator(0, Type.DATE, "id_date", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDatetime(LocalDateTime.parse("2021-12-29 11:11:11", dateTimeFormatter));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertSame(beforeOptimize, afterOptimize);
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATETIME, new ColumnRefOperator(0, Type.DATE, "id_date", false));
            ConstantOperator constantOperator = ConstantOperator.createNull(Type.DATETIME);
            BinaryPredicateOperator beforeOptimize = BinaryPredicateOperator.ge(castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);
            Assert.assertSame(beforeOptimize, afterOptimize);
        }
    }

    @Test
    public void testReduceDatetimeToDateCast() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        ReduceCastRule reduceCastRule = new ReduceCastRule();
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATE, new ColumnRefOperator(0, Type.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.GE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-28 00:00:00",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATE, new ColumnRefOperator(0, Type.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.GE,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-29 00:00:00",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATE, new ColumnRefOperator(0, Type.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.LT,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-29 00:00:00",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATE, new ColumnRefOperator(0, Type.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.LT,
                    ((BinaryPredicateOperator) afterOptimize).getBinaryType());
            Assert.assertTrue(afterOptimize.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(afterOptimize.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-28 00:00:00",
                    ((ConstantOperator) afterOptimize.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATE, new ColumnRefOperator(0, Type.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator =
                    ConstantOperator.createDate(LocalDate.parse("2021-12-28").atTime(0, 0, 0, 0));
            BinaryPredicateOperator beforeOptimize =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(
                    beforeOptimize,
                    null);

            Assert.assertTrue(afterOptimize instanceof CompoundPredicateOperator);
            Assert.assertTrue(((CompoundPredicateOperator) afterOptimize).isAnd());
            ScalarOperator left = afterOptimize.getChild(0);
            ScalarOperator right = afterOptimize.getChild(1);
            Assert.assertTrue(left instanceof BinaryPredicateOperator);
            Assert.assertTrue(right instanceof BinaryPredicateOperator);
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.GE,
                    ((BinaryPredicateOperator) left).getBinaryType());
            Assert.assertTrue(left.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(left.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-28 00:00:00",
                    ((ConstantOperator) left.getChild(1)).getDatetime().format(formatter));
            Assert.assertEquals(BinaryPredicateOperator.BinaryType.LT,
                    ((BinaryPredicateOperator) right).getBinaryType());
            Assert.assertTrue(right.getChild(0) instanceof ColumnRefOperator);
            Assert.assertTrue(right.getChild(1) instanceof ConstantOperator);
            Assert.assertEquals("2021-12-29 00:00:00",
                    ((ConstantOperator) right.getChild(1)).getDatetime().format(formatter));
        }
        {
            CastOperator castOperator =
                    new CastOperator(Type.DATE, new ColumnRefOperator(0, Type.DATETIME, "id_datetime", false));
            ConstantOperator constantOperator = ConstantOperator.createNull(Type.DATE);
            BinaryPredicateOperator beforeOptimize = BinaryPredicateOperator.lt(castOperator, constantOperator);
            ScalarOperator afterOptimize = reduceCastRule.apply(beforeOptimize, null);
            Assert.assertSame(beforeOptimize, afterOptimize);
        }
    }
}
