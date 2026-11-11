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

package com.starrocks.connector.lance;

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static com.starrocks.type.BooleanType.BOOLEAN;
import static com.starrocks.type.DateType.DATE;
import static com.starrocks.type.DateType.DATETIME;
import static com.starrocks.type.FloatType.DOUBLE;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarcharType.VARCHAR;

public class ScalarOperatorToLanceSqlTest {

    private final ScalarOperatorToLanceSql translator = new ScalarOperatorToLanceSql();

    private static ColumnRefOperator col(String name, Type type) {
        return new ColumnRefOperator(1, type, name, true);
    }

    private String sql(ScalarOperator predicate) {
        return translator.translate(List.of(predicate)).orElse(null);
    }

    private static BinaryPredicateOperator binary(BinaryType type, ScalarOperator l, ScalarOperator r) {
        return new BinaryPredicateOperator(type, l, r);
    }

    @Test
    public void testComparisons() {
        Assertions.assertEquals("id = 5", sql(binary(BinaryType.EQ, col("id", INT), ConstantOperator.createInt(5))));
        Assertions.assertEquals("id <> 5", sql(binary(BinaryType.NE, col("id", INT), ConstantOperator.createInt(5))));
        Assertions.assertEquals("id < 5", sql(binary(BinaryType.LT, col("id", INT), ConstantOperator.createInt(5))));
        Assertions.assertEquals("id <= 5", sql(binary(BinaryType.LE, col("id", INT), ConstantOperator.createInt(5))));
        Assertions.assertEquals("id > 5", sql(binary(BinaryType.GT, col("id", INT), ConstantOperator.createInt(5))));
        Assertions.assertEquals("id >= 5", sql(binary(BinaryType.GE, col("id", INT), ConstantOperator.createInt(5))));
    }

    @Test
    public void testEqForNullNotPushed() {
        // <=> has no safe DataFusion equivalent -> residual.
        Assertions.assertNull(sql(binary(BinaryType.EQ_FOR_NULL, col("id", INT), ConstantOperator.createInt(5))));
    }

    @Test
    public void testLiteralFormatting() throws Exception {
        Assertions.assertEquals("flag = TRUE",
                sql(binary(BinaryType.EQ, col("flag", BOOLEAN), ConstantOperator.createBoolean(true))));
        Assertions.assertEquals("n = 42",
                sql(binary(BinaryType.EQ, col("n", BIGINT), ConstantOperator.createBigint(42L))));
        Assertions.assertEquals("d = 1.5",
                sql(binary(BinaryType.EQ, col("d", DOUBLE), ConstantOperator.createDouble(1.5))));
        Assertions.assertEquals("name = 'alice'",
                sql(binary(BinaryType.EQ, col("name", VARCHAR), ConstantOperator.createVarchar("alice"))));
    }

    @Test
    public void testStringEscaping() {
        Assertions.assertEquals("name = 'O''Brien'",
                sql(binary(BinaryType.EQ, col("name", VARCHAR), ConstantOperator.createVarchar("O'Brien"))));
    }

    @Test
    public void testDateAndTimestampLiterals() throws Exception {
        Assertions.assertEquals("d = DATE '2021-03-04'",
                sql(binary(BinaryType.EQ, col("d", DATE),
                        ConstantOperator.createDate(LocalDateTime.of(2021, 3, 4, 0, 0, 0)))));
        Assertions.assertEquals("ts = TIMESTAMP '2021-03-04 05:06:07'",
                sql(binary(BinaryType.EQ, col("ts", DATETIME),
                        ConstantOperator.createDatetime(LocalDateTime.of(2021, 3, 4, 5, 6, 7)))));
    }

    @Test
    public void testIsNull() {
        Assertions.assertEquals("id IS NULL", sql(new IsNullPredicateOperator(false, col("id", INT))));
        Assertions.assertEquals("id IS NOT NULL", sql(new IsNullPredicateOperator(true, col("id", INT))));
    }

    @Test
    public void testInPredicate() {
        InPredicateOperator in = new InPredicateOperator(false, col("id", INT),
                ConstantOperator.createInt(1), ConstantOperator.createInt(2), ConstantOperator.createInt(3));
        Assertions.assertEquals("id IN (1, 2, 3)", sql(in));

        InPredicateOperator notIn = new InPredicateOperator(true, col("id", INT),
                ConstantOperator.createInt(1), ConstantOperator.createInt(2));
        Assertions.assertEquals("id NOT IN (1, 2)", sql(notIn));
    }

    @Test
    public void testInWithNonConstantNotPushed() {
        // A non-constant element makes the whole IN unpushable.
        InPredicateOperator in = new InPredicateOperator(false, col("id", INT),
                ConstantOperator.createInt(1), col("other", INT));
        Assertions.assertNull(sql(in));
    }

    @Test
    public void testAndOrNot() {
        BinaryPredicateOperator a = binary(BinaryType.EQ, col("a", INT), ConstantOperator.createInt(1));
        BinaryPredicateOperator b = binary(BinaryType.GT, col("b", INT), ConstantOperator.createInt(2));

        Assertions.assertEquals("(a = 1 AND b > 2)",
                sql(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, a, b)));
        Assertions.assertEquals("(a = 1 OR b > 2)",
                sql(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, a, b)));
        Assertions.assertEquals("NOT (a = 1)",
                sql(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, a)));
    }

    @Test
    public void testPartialAndPushdownOutsideNot() {
        BinaryPredicateOperator pushable = binary(BinaryType.EQ, col("a", INT), ConstantOperator.createInt(1));
        BinaryPredicateOperator residual = binary(BinaryType.EQ_FOR_NULL, col("b", INT), ConstantOperator.createInt(2));
        // AND outside NOT: push the convertible side alone.
        Assertions.assertEquals("a = 1",
                sql(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, pushable, residual)));
    }

    @Test
    public void testOrWithResidualIsNotPushed() {
        BinaryPredicateOperator pushable = binary(BinaryType.EQ, col("a", INT), ConstantOperator.createInt(1));
        BinaryPredicateOperator residual = binary(BinaryType.EQ_FOR_NULL, col("b", INT), ConstantOperator.createInt(2));
        // OR requires both sides; a dropped disjunct would wrongly narrow results.
        Assertions.assertNull(
                sql(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, pushable, residual)));
    }

    @Test
    public void testNoPartialAndInsideNot() {
        BinaryPredicateOperator pushable = binary(BinaryType.EQ, col("a", INT), ConstantOperator.createInt(1));
        BinaryPredicateOperator residual = binary(BinaryType.EQ_FOR_NULL, col("b", INT), ConstantOperator.createInt(2));
        CompoundPredicateOperator and =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, pushable, residual);
        // NOT(AND(a, b)) = OR(NOT a, NOT b); pushing NOT(a) alone would over-filter -> residual.
        Assertions.assertNull(
                sql(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, and)));
    }

    @Test
    public void testIdentityCastStripped() {
        CastOperator identity = new CastOperator(INT, col("a", INT));
        Assertions.assertEquals("a = 1", sql(binary(BinaryType.EQ, identity, ConstantOperator.createInt(1))));
    }

    @Test
    public void testNonIdentityCastNotPushed() {
        CastOperator widening = new CastOperator(BIGINT, col("a", INT));
        Assertions.assertNull(sql(binary(BinaryType.EQ, widening, ConstantOperator.createBigint(1L))));
    }

    @Test
    public void testQuotedIdentifier() {
        Assertions.assertEquals("\"odd name\" = 1",
                sql(binary(BinaryType.EQ, col("odd name", INT), ConstantOperator.createInt(1))));
    }

    @Test
    public void testTranslateJoinsConjunctsAndSkipsResidual() {
        BinaryPredicateOperator a = binary(BinaryType.EQ, col("a", INT), ConstantOperator.createInt(1));
        BinaryPredicateOperator residual = binary(BinaryType.EQ_FOR_NULL, col("b", INT), ConstantOperator.createInt(2));
        BinaryPredicateOperator c = binary(BinaryType.GT, col("c", INT), ConstantOperator.createInt(3));
        Optional<String> result = translator.translate(List.of(a, residual, c));
        Assertions.assertEquals("a = 1 AND c > 3", result.orElse(null));
    }

    @Test
    public void testEmptyWhenNothingPushable() {
        BinaryPredicateOperator residual = binary(BinaryType.EQ_FOR_NULL, col("b", INT), ConstantOperator.createInt(2));
        Assertions.assertTrue(translator.translate(List.of(residual)).isEmpty());
    }
}
