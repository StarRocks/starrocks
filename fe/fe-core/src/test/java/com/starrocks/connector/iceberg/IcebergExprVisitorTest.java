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

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rule.tree.VariantPathRewriteRule;
import com.starrocks.type.AnyStructType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IcebergExprVisitorTest {
    private static final Schema SCHEMA =
            new Schema(
                    Types.NestedField.optional(1, "k1", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "k2", Types.IntegerType.get()),
                    Types.NestedField.optional(3, "k3", Types.DateType.get()),
                    Types.NestedField.optional(4, "k4", Types.TimestampType.withoutZone()),
                    Types.NestedField.optional(5, "k5", Types.BooleanType.get()),
                    Types.NestedField.optional(6, "k6", Types.StringType.get()),
                    Types.NestedField.optional(7, "k7", Types.LongType.get()),
                    Types.NestedField.optional(8, "k8", Types.FloatType.get()),
                    Types.NestedField.optional(9, "k9", Types.DoubleType.get()),
                    Types.NestedField.optional(10, "k10", Types.StructType.of(
                            Types.NestedField.optional(11, "k11", Types.IntegerType.get()),
                            Types.NestedField.optional(12, "k12", Types.DateType.get()),
                            Types.NestedField.optional(13, "k13", Types.TimestampType.withZone()),
                            Types.NestedField.optional(14, "k14", Types.BooleanType.get()),
                            Types.NestedField.optional(15, "k15", Types.StringType.get()),
                            Types.NestedField.optional(16, "k16", Types.FloatType.get())
                    )),
                    Types.NestedField.optional(17, "k17.double", Types.DoubleType.get()));

    private static final ColumnRefOperator K1 = new ColumnRefOperator(3, IntegerType.INT, "k1", true, false);
    private static final ColumnRefOperator K2 = new ColumnRefOperator(4, IntegerType.INT, "k2", true, false);
    private static final ColumnRefOperator K3 = new ColumnRefOperator(5, DateType.DATE, "k3", true, false);
    private static final ColumnRefOperator K4 = new ColumnRefOperator(6, DateType.DATETIME, "k4", true, false);
    private static final ColumnRefOperator K5 = new ColumnRefOperator(7, BooleanType.BOOLEAN, "k5", true, false);
    private static final ColumnRefOperator K6 = new ColumnRefOperator(8, StringType.STRING, "k6", true, false);
    private static final ColumnRefOperator K7 = new ColumnRefOperator(9, IntegerType.BIGINT, "k7", true, false);
    private static final ColumnRefOperator K8 = new ColumnRefOperator(10, FloatType.FLOAT, "k8", true, false);
    private static final ColumnRefOperator K9 = new ColumnRefOperator(11, FloatType.DOUBLE, "k9", true, false);
    private static final ColumnRefOperator K10 = new ColumnRefOperator(12, AnyStructType.ANY_STRUCT, "k10", true, false);
    private static final SubfieldOperator K11 = new SubfieldOperator(K10, IntegerType.INT, ImmutableList.of("k11"));
    private static final SubfieldOperator K12 = new SubfieldOperator(K10, DateType.DATE, ImmutableList.of("k12"));
    private static final SubfieldOperator K13 = new SubfieldOperator(K10, DateType.DATETIME, ImmutableList.of("k13"));
    private static final SubfieldOperator K14 = new SubfieldOperator(K10, BooleanType.BOOLEAN, ImmutableList.of("k14"));
    private static final SubfieldOperator K15 = new SubfieldOperator(K10, StringType.STRING, ImmutableList.of("k15"));
    private static final SubfieldOperator K16 = new SubfieldOperator(K10, FloatType.FLOAT, ImmutableList.of("k16"));
    private static final ColumnRefOperator K17 = new ColumnRefOperator(17, FloatType.DOUBLE, "k17.double", true, false);
    private static final ColumnRefOperator LAST_UPDATED_SEQUENCE_NUMBER = new ColumnRefOperator(
            18, IntegerType.BIGINT, IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER, true, false);

    @Test
    public void testToIcebergExpression() {
        ScalarOperatorToIcebergExpr.IcebergContext context = new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());
        ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

        Expression convertedExpr;
        Expression expectedExpr;

        // isNull
        convertedExpr = converter.convert(Lists.newArrayList(new IsNullPredicateOperator(false, K1)), context);
        expectedExpr = Expressions.isNull("k1");
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated isNull expression should be correct");

        // notNUll
        convertedExpr = converter.convert(Lists.newArrayList(new IsNullPredicateOperator(true, K1)), context);
        expectedExpr = Expressions.notNull("k1");
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated notNull expression should be correct");

        // equal date
        ConstantOperator value = ConstantOperator.createDate(LocalDate.parse("2022-11-11").atTime(0, 0, 0, 0));
        long epochDay = value.getDatetime().toLocalDate().toEpochDay();
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K3, value)), context);
        expectedExpr = Expressions.equal("k3", epochDay);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated equal expression should be correct");

        // equal datetime
        value = ConstantOperator.createDatetime(LocalDateTime.of(2022, 11, 11, 11, 11, 11));
        long epochSec = value.getDatetime().toEpochSecond(ZoneOffset.UTC);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K4, value)), context);
        expectedExpr = Expressions.equal("k4", TimeUnit.MICROSECONDS.convert(epochSec, TimeUnit.SECONDS));
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated equal expression should be correct");

        // equal timestamp
        value = ConstantOperator.createDatetime(LocalDateTime.of(2023, 8, 18, 15, 13, 12, 634297000));
        long secs = value.getDatetime().atZone(ZoneOffset.UTC).toEpochSecond() * 1000
                * 1000 * 1000 + value.getDatetime().getNano();
        epochSec = TimeUnit.MICROSECONDS.convert(secs, TimeUnit.NANOSECONDS);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K4, value)), context);
        expectedExpr = Expressions.equal("k4", epochSec);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated equal expression should be correct");

        // notEqual
        value = ConstantOperator.createBoolean(true);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.NE, K5, value)), context);
        expectedExpr = Expressions.notEqual("k5", true);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated notEqual expression should be correct");

        // lessThan
        value = ConstantOperator.createInt(5);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LT, K2, value)), context);
        expectedExpr = Expressions.lessThan("k2", value.getInt());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated lessThan expression should be correct");

        // lessThanOrEqual
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LE, K2, value)), context);
        expectedExpr = Expressions.lessThanOrEqual("k2", value.getInt());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated lessThanOrEqual expression should be correct");

        // greaterThan
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GT, K2, value)), context);
        expectedExpr = Expressions.greaterThan("k2", value.getInt());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated greaterThan expression should be correct");

        // greaterThanOrEqual
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GE, K2, value)), context);
        expectedExpr = Expressions.greaterThanOrEqual("k2", value.getInt());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated greaterThanOrEqual expression should be correct");

        List<ScalarOperator> inOp = Lists.newArrayList();
        inOp.add(K6);
        inOp.add(ConstantOperator.createVarchar("123"));
        inOp.add(ConstantOperator.createVarchar("456"));
        inOp.add(ConstantOperator.createVarchar("789"));
        inOp.add(ConstantOperator.createVarchar("jqk"));
        List<String> inList = inOp.stream()
                .filter(x -> !(x instanceof ColumnRefOperator))
                .map(x -> (ConstantOperator) x)
                .map(ConstantOperator::getVarchar)
                .collect(Collectors.toList());

        InPredicateOperator predicate = new InPredicateOperator(false, inOp);
        convertedExpr = converter.convert(Lists.newArrayList(predicate), context);
        expectedExpr = Expressions.in("k6", inList);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(), "Generated in expression should be correct");

        // notIn
        convertedExpr = converter.convert(Lists.newArrayList(new InPredicateOperator(true, inOp)), context);
        expectedExpr = Expressions.notIn("k6", inList);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated notIn expression should be correct");

        // like
        value = ConstantOperator.createVarchar("a%");
        convertedExpr = converter.convert(Lists.newArrayList(
                new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, K6, value)), context);
        expectedExpr = Expressions.startsWith("k6", "a");
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(), "Generated like expression should be correct");

        // or
        BinaryPredicateOperator op1 = new BinaryPredicateOperator(
                BinaryType.GT, K1, ConstantOperator.createInt(10));
        BinaryPredicateOperator op2 = new BinaryPredicateOperator(
                BinaryType.LT, K1, ConstantOperator.createInt(5));

        Expression expression1 = converter.convert(Lists.newArrayList(op1), context);
        Expression expression2 = converter.convert(Lists.newArrayList(op2), context);
        convertedExpr = converter.convert(Lists.newArrayList(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, op1, op2)), context);
        expectedExpr = Expressions.or(expression1, expression2);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(), "Generated or expression should be correct");
    }

    @Test
    public void testToIcebergExpressionStructSubColumn() {
        ScalarOperatorToIcebergExpr.IcebergContext context = new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());
        ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

        Expression convertedExpr;
        Expression expectedExpr;

        // isNull
        convertedExpr = converter.convert(Lists.newArrayList(new IsNullPredicateOperator(false, K11)), context);
        expectedExpr = Expressions.isNull("k10.k11");
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString());

        // notNUll
        convertedExpr = converter.convert(Lists.newArrayList(new IsNullPredicateOperator(true, K11)), context);
        expectedExpr = Expressions.notNull("k10.k11");
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated notNull expression should be correct");

        // equal date
        ConstantOperator value = ConstantOperator.createDate(LocalDate.parse("2022-11-11").atTime(0, 0, 0, 0));
        long epochDay = value.getDatetime().toLocalDate().toEpochDay();
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K12, value)), context);
        expectedExpr = Expressions.equal("k10.k12", epochDay);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated equal expression should be correct");

        // equal datetime
        value = ConstantOperator.createDatetime(LocalDateTime.of(2022, 11, 11, 11, 11, 11));
        long epochSec = value.getDatetime().toEpochSecond(OffsetDateTime.now().getOffset());
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K13, value)), context);
        expectedExpr = Expressions.equal("k10.k13", TimeUnit.MICROSECONDS.convert(epochSec, TimeUnit.SECONDS));
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated equal expression should be correct");

        // equal timestamp
        value = ConstantOperator.createDatetime(LocalDateTime.of(2023, 8, 18, 15, 13, 12, 634297000));
        epochSec = 1692342792634297L;
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K13, value)), context);
        expectedExpr = Expressions.equal("k10.k13", TimeUnit.MICROSECONDS.convert(epochSec, TimeUnit.MICROSECONDS));
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated equal expression should be correct");

        // notEqual
        value = ConstantOperator.createBoolean(true);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.NE, K14, value)), context);
        expectedExpr = Expressions.notEqual("k10.k14", true);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated notEqual expression should be correct");

        // lessThan
        value = ConstantOperator.createInt(5);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LT, K11, value)), context);
        expectedExpr = Expressions.lessThan("k10.k11", value.getInt());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated lessThan expression should be correct");

        // lessThanOrEqual
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LE, K11, value)), context);
        expectedExpr = Expressions.lessThanOrEqual("k10.k11", value.getInt());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated lessThanOrEqual expression should be correct");

        // greaterThan
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GT, K11, value)), context);
        expectedExpr = Expressions.greaterThan("k10.k11", value.getInt());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated greaterThan expression should be correct");

        // greaterThanOrEqual
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GE, K11, value)), context);
        expectedExpr = Expressions.greaterThanOrEqual("k10.k11", value.getInt());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated greaterThanOrEqual expression should be correct");

        List<ScalarOperator> inOp = Lists.newArrayList();
        inOp.add(K15);
        inOp.add(ConstantOperator.createVarchar("123"));
        inOp.add(ConstantOperator.createVarchar("456"));
        inOp.add(ConstantOperator.createVarchar("789"));
        inOp.add(ConstantOperator.createVarchar("jqk"));
        List<String> inList = inOp.stream()
                .filter(x -> !(x instanceof ColumnRefOperator || x instanceof SubfieldOperator))
                .map(x -> (ConstantOperator) x)
                .map(ConstantOperator::getVarchar)
                .collect(Collectors.toList());

        InPredicateOperator predicate = new InPredicateOperator(false, inOp);
        convertedExpr = converter.convert(Lists.newArrayList(predicate), context);
        expectedExpr = Expressions.in("k10.k15", inList);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(), "Generated in expression should be correct");

        // notIn
        convertedExpr = converter.convert(Lists.newArrayList(new InPredicateOperator(true, inOp)), context);
        expectedExpr = Expressions.notIn("k10.k15", inList);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated notIn expression should be correct");

        // like
        value = ConstantOperator.createVarchar("a%");
        convertedExpr = converter.convert(Lists.newArrayList(
                new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, K15, value)), context);
        expectedExpr = Expressions.startsWith("k10.k15", "a");
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(), "Generated like expression should be correct");

        // or
        BinaryPredicateOperator op1 = new BinaryPredicateOperator(
                BinaryType.GT, K11, ConstantOperator.createInt(10));
        BinaryPredicateOperator op2 = new BinaryPredicateOperator(
                BinaryType.LT, K11, ConstantOperator.createInt(5));

        Expression expression1 = converter.convert(Lists.newArrayList(op1), context);
        Expression expression2 = converter.convert(Lists.newArrayList(op2), context);
        convertedExpr = converter.convert(Lists.newArrayList(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, op1, op2)), context);
        expectedExpr = Expressions.or(expression1, expression2);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(), "Generated or expression should be correct");
    }

    @Test
    public void testToIcebergCastExpression() {
        ScalarOperatorToIcebergExpr.IcebergContext context = new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());
        ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

        Expression convertedExpr;
        Expression expectedExpr;

        // cast string column to date
        ConstantOperator value = ConstantOperator.createDate(LocalDate.parse("2022-11-11").atTime(0, 0, 0, 0));
        CastOperator cast = new CastOperator(DateType.DATE, K6);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());

        // cast date column to string
        value = ConstantOperator.createVarchar("2022-11-11");
        cast = new CastOperator(VarcharType.VARCHAR, K3);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LT, cast, value)), context);
        long epochDay = LocalDate.parse("2022-11-11").toEpochDay();
        expectedExpr = Expressions.lessThan("k3", epochDay);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "Generated lessThan expression should be correct");

        // cast string column to int
        // don't support cast string to int, different comparator
        value = ConstantOperator.createInt(11);
        cast = new CastOperator(IntegerType.INT, K6);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LT, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());

        // don't support cast float to varchar, different comparator
        value = ConstantOperator.createVarchar("11.11");
        cast = new CastOperator(VarcharType.VARCHAR, K8);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LT, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());

        // don't support cast double to varchar, different comparator
        value = ConstantOperator.createVarchar("11.11");
        cast = new CastOperator(VarcharType.VARCHAR, K9);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LT, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());

        // 11.11 -> LONG throw exception
        value = ConstantOperator.createVarchar("11.11");
        cast = new CastOperator(VarcharType.VARCHAR, K7);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LT, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());

        // time cast throw exception
        value = ConstantOperator.createTime(124578990d);
        cast = new CastOperator(DateType.TIME, K4);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());

        // LONG -> char
        value = ConstantOperator.createBigint(11);
        cast = new CastOperator(IntegerType.BIGINT, K6);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());

        // Double -> char
        value = ConstantOperator.createDouble(11.11);
        cast = new CastOperator(FloatType.DOUBLE, K6);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());

        // Float -> char
        value = ConstantOperator.createFloat(11.11);
        cast = new CastOperator(FloatType.FLOAT, K6);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());

    }

    @Test
    public void testToIcebergExpressionDotColumn() {
        ScalarOperatorToIcebergExpr.IcebergContext context = new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());
        ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

        Expression convertedExpr;

        ConstantOperator value = ConstantOperator.createVarchar("11.11");
        CastOperator cast = new CastOperator(VarcharType.VARCHAR, K17);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LT, cast, value)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());
    }

    @Test
    public void testAndPartialPushdown() {
        ScalarOperatorToIcebergExpr.IcebergContext context = new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());
        ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

        // Build a convertible predicate: k1 > 10
        BinaryPredicateOperator convertible = new BinaryPredicateOperator(
                BinaryType.GT, K1, ConstantOperator.createInt(10));

        // Build an unconvertible predicate: CAST(k6 AS INT) < 5
        // CastOperator(INT, K6) where K6 is a string column causes getLiteralValue to return null
        CastOperator cast = new CastOperator(IntegerType.INT, K6);
        BinaryPredicateOperator unconvertible = new BinaryPredicateOperator(
                BinaryType.LT, cast, ConstantOperator.createInt(5));

        // Verify that the unconvertible predicate alone produces alwaysTrue
        Expression unconvertibleExpr = converter.convert(Lists.newArrayList(unconvertible), context);
        Assertions.assertEquals(Expression.Operation.TRUE, unconvertibleExpr.op(),
                "Unconvertible predicate should produce alwaysTrue");

        Expression convertedExpr;
        Expression expectedExpr;

        // AND(convertible, unconvertible) -> returns the convertible side
        convertedExpr = converter.convert(Lists.newArrayList(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        convertible, unconvertible)), context);
        expectedExpr = Expressions.greaterThan("k1", 10);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "AND(convertible, unconvertible) should push down the convertible side");

        // AND(unconvertible, convertible) -> returns the convertible side
        convertedExpr = converter.convert(Lists.newArrayList(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        unconvertible, convertible)), context);
        expectedExpr = Expressions.greaterThan("k1", 10);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "AND(unconvertible, convertible) should push down the convertible side");

        // OR(convertible, unconvertible) -> returns alwaysTrue (no partial pushdown for OR)
        convertedExpr = converter.convert(Lists.newArrayList(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                        convertible, unconvertible)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "OR(convertible, unconvertible) should NOT do partial pushdown");

        // NOT(AND(convertible, unconvertible)) -> returns alwaysTrue (no partial pushdown inside NOT)
        CompoundPredicateOperator andOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, convertible, unconvertible);
        convertedExpr = converter.convert(Lists.newArrayList(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, andOp)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "NOT(AND(convertible, unconvertible)) should NOT do partial pushdown");

        // AND(convertible1, convertible2) -> returns and(left, right) (regression test)
        BinaryPredicateOperator convertible2 = new BinaryPredicateOperator(
                BinaryType.LT, K2, ConstantOperator.createInt(20));
        convertedExpr = converter.convert(Lists.newArrayList(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        convertible, convertible2)), context);
        expectedExpr = Expressions.and(
                Expressions.greaterThan("k1", 10),
                Expressions.lessThan("k2", 20));
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "AND(convertible1, convertible2) should return and(left, right)");

        // Nested AND: AND(AND(convertible, unconvertible), convertible2) -> returns and(convertible, convertible2)
        CompoundPredicateOperator innerAnd = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, convertible, unconvertible);
        convertedExpr = converter.convert(Lists.newArrayList(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        innerAnd, convertible2)), context);
        expectedExpr = Expressions.and(
                Expressions.greaterThan("k1", 10),
                Expressions.lessThan("k2", 20));
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "AND(AND(convertible, unconvertible), convertible2) should return and(convertible, convertible2)");

        // Strict mode: AND(convertible, unconvertible) -> returns null (no partial pushdown)
        convertedExpr = converter.convertStrict(Lists.newArrayList(
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        convertible, unconvertible)), context);
        Assertions.assertNull(convertedExpr,
                "Strict mode: AND(convertible, unconvertible) should return null");
    }

    @Test
    public void testConvertLastUpdatedSequenceNumberPredicate() {
        ScalarOperatorToIcebergExpr.IcebergContext context = new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());
        ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

        Expression convertedExpr = converter.convert(Lists.newArrayList(
                        new BinaryPredicateOperator(BinaryType.EQ, LAST_UPDATED_SEQUENCE_NUMBER,
                                ConstantOperator.createBigint(1))),
                context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());
    }

    @Test
    public void testSkipSyntheticVariantRewriteColumn() {
        ScalarOperatorToIcebergExpr.IcebergContext context = new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());
        ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

        ColumnRefOperator syntheticVariantColumn = new ColumnRefOperator(19, IntegerType.INT, "v.a.b", true, false);
        syntheticVariantColumn.setHints(List.of(VariantPathRewriteRule.COLUMN_REF_HINT));

        Expression convertedExpr = converter.convert(Lists.newArrayList(
                        new BinaryPredicateOperator(BinaryType.EQ, syntheticVariantColumn, ConstantOperator.createInt(10))),
                context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op());
    }

    /**
     * Verifies the literal-side rejections in {@code ExtractLiteralValue.tryCastToResultType}
     * together with the column-side cast peeling in {@code visitCastOperator}, covering the
     * cases where peeling the column-side cast would otherwise silently push down a wrong
     * predicate (truncated date, lex-vs-numeric ordering, integer wrap, float precision loss,
     * etc.). Same-typed regression cases that do not go through the cast path are also covered.
     */
    @Test
    public void testCastPeelRejectsLossyLiterals() {
        ScalarOperatorToIcebergExpr.IcebergContext context =
                new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());
        ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

        Expression convertedExpr;
        Expression expectedExpr;

        // DATE column = midnight DATETIME literal -> lossless cast, push down.
        ConstantOperator dtMidnight =
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 0, 0, 0));
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K3, dtMidnight)), context);
        expectedExpr = Expressions.equal("k3", LocalDate.of(2024, 1, 1).toEpochDay());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "midnight DATETIME literal should be pushed down to DATE column");

        // DATE column >= DATETIME literal with non-zero time-of-day -> silent truncation, reject.
        ConstantOperator dtNoon =
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 12, 0, 0));
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GE, K3, dtNoon)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "DATETIME literal with non-zero time-of-day must be rejected on DATE column");

        // CAST(date_col AS STRING) = '2024-01-01 12:00:00' -> reject (non-zero time-of-day).
        CastOperator dateColAsString = new CastOperator(VarcharType.VARCHAR, K3);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, dateColAsString,
                        ConstantOperator.createVarchar("2024-01-01 12:00:00"))), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "STRING literal with non-zero time-of-day must be rejected on DATE column");

        // CAST(date_col AS STRING) = '2024-01-01 00:00:00' -> push down (midnight).
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, dateColAsString,
                        ConstantOperator.createVarchar("2024-01-01 00:00:00"))), context);
        expectedExpr = Expressions.equal("k3", LocalDate.of(2024, 1, 1).toEpochDay());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "midnight STRING literal should be pushed down to DATE column");

        // CAST(date_col AS STRING) = '2024/01/01' -> conservatively reject unparseable literal.
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, dateColAsString,
                        ConstantOperator.createVarchar("2024/01/01"))), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "unparseable STRING literal must be conservatively rejected on DATE column");

        // DATETIME column = bare DATE literal -> reject (ambiguous with CAST(dt AS DATE)= pattern).
        ConstantOperator dateLit =
                ConstantOperator.createDate(LocalDate.of(2024, 1, 1).atStartOfDay());
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K4, dateLit)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "bare DATE literal on DATETIME column must be conservatively rejected");

        // CAST(datetime_col AS DATE) = DATE literal -> reject after column-side peel.
        CastOperator datetimeColAsDate = new CastOperator(DateType.DATE, K4);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, datetimeColAsDate, dateLit)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "CAST(datetime_col AS DATE) = DATE literal must be rejected after column-side peel");

        // DATETIME column = same-typed DATETIME literal -> no cast involved, push down.
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K4, dtMidnight)), context);
        long expectedMicros = TimeUnit.MICROSECONDS.convert(
                LocalDateTime.of(2024, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS);
        expectedExpr = Expressions.equal("k4", expectedMicros);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "same-typed DATETIME literal on DATETIME column should be pushed down (no cast involved)");

        // CAST(string_col AS DATETIME) > DATETIME literal -> reject (lex order != date order).
        CastOperator stringColAsDatetime = new CastOperator(DateType.DATETIME, K6);
        ConstantOperator dtMay = ConstantOperator.createDatetime(LocalDateTime.of(2026, 5, 15, 0, 0, 0));
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GT, stringColAsDatetime, dtMay)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "DATETIME literal on STRING column must be rejected (lex order != date order)");

        // CAST(string_col AS INT) > 10 -> reject (lex order != numeric order).
        CastOperator stringColAsInt = new CastOperator(IntegerType.INT, K6);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GT, stringColAsInt,
                        ConstantOperator.createInt(10))), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "numeric literal on STRING column must be rejected (lex order != numeric order)");

        // CAST(string_col AS DATE) = DATE literal -> reject.
        CastOperator stringColAsDate = new CastOperator(DateType.DATE, K6);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, stringColAsDate, dateLit)), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "DATE literal on STRING column must be rejected");

        // CAST(string_col AS BOOLEAN) = false -> reject ('1'/'0' vs 'true'/'false' mismatch).
        CastOperator stringColAsBool = new CastOperator(BooleanType.BOOLEAN, K6);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, stringColAsBool,
                        ConstantOperator.createBoolean(false))), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "BOOLEAN literal on STRING column must be rejected ('1'/'0' vs 'true'/'false' mismatch)");

        // STRING column = same-typed STRING literal -> push down.
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K6,
                        ConstantOperator.createVarchar("2024-01-01"))), context);
        expectedExpr = Expressions.equal("k6", "2024-01-01");
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "same-typed STRING literal on STRING column should be pushed down");

        // INT column = BIGINT literal out of INT range -> reject (silent wrap risk).
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K1,
                        ConstantOperator.createBigint(999999999999L))), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "BIGINT literal out of INT range must be rejected on INT column");

        // INT column = BIGINT literal within INT range -> still rejected (one-shot null policy).
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K1,
                        ConstantOperator.createBigint(100L))), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "BIGINT literal in INT range is currently rejected (acceptable conservatism)");

        // FLOAT column = DOUBLE literal -> reject (precision loss risk).
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K8,
                        ConstantOperator.createDouble(1.0))), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "DOUBLE literal on FLOAT column must be rejected");

        // BIGINT column = INT literal -> reject (numeric one-shot null policy).
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K7,
                        ConstantOperator.createInt(1))), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "INT literal on BIGINT column must be rejected");

        // DOUBLE column = FLOAT literal -> reject (numeric one-shot null policy).
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K9,
                        ConstantOperator.createFloat(1.0))), context);
        Assertions.assertEquals(Expression.Operation.TRUE, convertedExpr.op(),
                "FLOAT literal on DOUBLE column must be rejected");

        // Same-typed regression cases (no cast path involved).

        // DATE column > same-typed DATE literal -> push down.
        ConstantOperator dateMay =
                ConstantOperator.createDate(LocalDate.of(2026, 5, 15).atStartOfDay());
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GT, K3, dateMay)), context);
        expectedExpr = Expressions.greaterThan("k3", LocalDate.of(2026, 5, 15).toEpochDay());
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "same-typed DATE literal on DATE column should be pushed down");

        // INT column = same-typed INT literal -> push down.
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K1,
                        ConstantOperator.createInt(100))), context);
        expectedExpr = Expressions.equal("k1", 100);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "same-typed INT literal on INT column should be pushed down");

        // BOOLEAN column = same-typed BOOLEAN literal -> push down.
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K5,
                        ConstantOperator.createBoolean(true))), context);
        expectedExpr = Expressions.equal("k5", true);
        Assertions.assertEquals(expectedExpr.toString(), convertedExpr.toString(),
                "same-typed BOOLEAN literal on BOOLEAN column should be pushed down");
    }
}
