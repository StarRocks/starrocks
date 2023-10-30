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

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

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
<<<<<<< HEAD
                    Types.NestedField.required(1, "k1", Types.IntegerType.get()),
                    Types.NestedField.required(2, "k2", Types.IntegerType.get()),
                    Types.NestedField.required(3, "k3", Types.DateType.get()),
                    Types.NestedField.required(4, "k4", Types.TimestampType.withoutZone()),
                    Types.NestedField.required(5, "k5", Types.BooleanType.get()),
                    Types.NestedField.required(6, "k6", Types.StringType.get()));
=======
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
                    )));
>>>>>>> 2ff4ffc348 ([BugFix] fix iceberg timestamp/timestamptz issue (#33881))

    private static final ColumnRefOperator K1 = new ColumnRefOperator(3, Type.INT, "k1", true, false);
    private static final ColumnRefOperator K2 = new ColumnRefOperator(4, Type.INT, "k2", true, false);
    private static final ColumnRefOperator K3 = new ColumnRefOperator(5, Type.DATE, "k3", true, false);
    private static final ColumnRefOperator K4 = new ColumnRefOperator(6, Type.DATETIME, "k4", true, false);
    private static final ColumnRefOperator K5 = new ColumnRefOperator(7, Type.BOOLEAN, "k5", true, false);
    private static final ColumnRefOperator K6 = new ColumnRefOperator(8, Type.STRING, "k6", true, false);

    @Test
    public void testToIcebergExpression() {
        ScalarOperatorToIcebergExpr.IcebergContext context = new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());
        ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

        Expression convertedExpr;
        Expression expectedExpr;

        // isNull
        convertedExpr = converter.convert(Lists.newArrayList(new IsNullPredicateOperator(false, K1)), context);
        expectedExpr = Expressions.isNull("k1");
        Assert.assertEquals("Generated isNull expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // notNUll
        convertedExpr = converter.convert(Lists.newArrayList(new IsNullPredicateOperator(true, K1)), context);
        expectedExpr = Expressions.notNull("k1");
        Assert.assertEquals("Generated notNull expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // equal date
        ConstantOperator value = ConstantOperator.createDate(LocalDate.parse("2022-11-11").atTime(0, 0, 0, 0));
        long epochDay = value.getDatetime().toLocalDate().toEpochDay();
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K3, value)), context);
        expectedExpr = Expressions.equal("k3", epochDay);
        Assert.assertEquals("Generated equal expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // equal datetime
        value = ConstantOperator.createDatetime(LocalDateTime.of(2022, 11, 11, 11, 11, 11));
        long epochSec = value.getDatetime().toEpochSecond(ZoneOffset.UTC);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K4, value)), context);
        expectedExpr = Expressions.equal("k4", TimeUnit.MICROSECONDS.convert(epochSec, TimeUnit.SECONDS));
        Assert.assertEquals("Generated equal expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // equal timestamp
        value = ConstantOperator.createDatetime(LocalDateTime.of(2023, 8, 18, 15, 13, 12, 634297000));
        long secs = value.getDatetime().atZone(ZoneOffset.UTC).toEpochSecond() * 1000
                * 1000 * 1000 + value.getDatetime().getNano();
        epochSec = TimeUnit.MICROSECONDS.convert(secs, TimeUnit.NANOSECONDS);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.EQ, K4, value)), context);
        expectedExpr = Expressions.equal("k4", epochSec);
        Assert.assertEquals("Generated equal expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // notEqual
        value = ConstantOperator.createBoolean(true);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.NE, K5, value)), context);
        expectedExpr = Expressions.notEqual("k5", true);
        Assert.assertEquals("Generated notEqual expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // lessThan
        value = ConstantOperator.createInt(5);
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LT, K2, value)), context);
        expectedExpr = Expressions.lessThan("k2", value.getInt());
        Assert.assertEquals("Generated lessThan expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // lessThanOrEqual
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.LE, K2, value)), context);
        expectedExpr = Expressions.lessThanOrEqual("k2", value.getInt());
        Assert.assertEquals("Generated lessThanOrEqual expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // greaterThan
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GT, K2, value)), context);
        expectedExpr = Expressions.greaterThan("k2", value.getInt());
        Assert.assertEquals("Generated greaterThan expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // greaterThanOrEqual
        convertedExpr = converter.convert(Lists.newArrayList(
                new BinaryPredicateOperator(BinaryType.GE, K2, value)), context);
        expectedExpr = Expressions.greaterThanOrEqual("k2", value.getInt());
        Assert.assertEquals("Generated greaterThanOrEqual expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

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
        Assert.assertEquals("Generated in expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // notIn
        convertedExpr = converter.convert(Lists.newArrayList(new InPredicateOperator(true, inOp)), context);
        expectedExpr = Expressions.notIn("k6", inList);
        Assert.assertEquals("Generated notIn expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

        // like
        value = ConstantOperator.createVarchar("a%");
        convertedExpr = converter.convert(Lists.newArrayList(
                new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, K6, value)), context);
        expectedExpr = Expressions.startsWith("k6", "a");
        Assert.assertEquals("Generated like expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());

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
        Assert.assertEquals("Generated or expression should be correct",
                expectedExpr.toString(), convertedExpr.toString());
    }
}
