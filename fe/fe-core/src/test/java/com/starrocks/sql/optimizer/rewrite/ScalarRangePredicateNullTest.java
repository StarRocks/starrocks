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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.property.RangeExtractor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScalarRangePredicateNullTest {
    private ConnectContext previousContext;
    private final ColumnRefOperator x = new ColumnRefOperator(1, IntegerType.INT, "x", true);
    private final ColumnRefOperator y = new ColumnRefOperator(2, IntegerType.INT, "y", true);

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setCboDerivePredicateNullAlternative(true);
        context.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    private ScalarOperator compare(BinaryType type, int value) {
        return new BinaryPredicateOperator(type, x, ConstantOperator.createInt(value));
    }

    private ScalarOperator orNull(ScalarOperator predicate) {
        return Utils.compoundOr(predicate, new IsNullPredicateOperator(x));
    }

    private List<ScalarOperator> extractX(ScalarOperator predicate) {
        return new RangeExtractor().apply(predicate, null).get(x).toScalarOperator();
    }

    @Test
    void testDisjointNullableRangesKeepNull() {
        ScalarOperator predicate = Utils.compoundAnd(
                orNull(compare(BinaryType.LT, 1)), orNull(compare(BinaryType.GT, 2)));
        assertEquals(List.of(new IsNullPredicateOperator(x)), extractX(predicate));
    }

    @Test
    void testTouchingNullableRangesKeepNull() {
        ScalarOperator predicate = Utils.compoundAnd(
                orNull(compare(BinaryType.LT, 1)), orNull(compare(BinaryType.GE, 1)));
        assertEquals(List.of(new IsNullPredicateOperator(x)), extractX(predicate));
    }

    @Test
    void testNullableValuesIntersectRange() {
        assertEquals(List.of(new IsNullPredicateOperator(x)), extractX(Utils.compoundAnd(
                orNull(compare(BinaryType.EQ, 1)), orNull(compare(BinaryType.LT, 0)))));
        // Removing the NULL alternative from either side makes the intersection empty.
        List<ScalarOperator> empty = extractX(Utils.compoundAnd(
                orNull(compare(BinaryType.EQ, 1)), compare(BinaryType.LT, 0)));
        assertEquals(1, empty.size());
        assertTrue(empty.get(0) instanceof ConstantOperator);
        assertTrue(((ConstantOperator) empty.get(0)).isNull());
    }

    @Test
    void testDecimalValuesAndRangeWithNull() {
        Type type = TypeFactory.createDecimalV3NarrowestType(18, 2);
        ColumnRefOperator decimal = new ColumnRefOperator(3, type, "d", true);
        ConstantOperator a = ConstantOperator.createDecimal(new BigDecimal("1.25"), type);
        ConstantOperator b = ConstantOperator.createDecimal(new BigDecimal("2.50"), type);
        ScalarOperator predicate = Utils.compoundOr(
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, decimal, a),
                new BinaryPredicateOperator(BinaryType.EQ, decimal, b), new IsNullPredicateOperator(decimal));
        ScalarOperator expected = Utils.compoundOr(
                new InPredicateOperator(false, decimal, a, b), new IsNullPredicateOperator(decimal));
        assertEquals(expected, new ScalarRangePredicateExtractor().rewriteOnlyColumn(predicate));

        ScalarOperator range = new BinaryPredicateOperator(BinaryType.GT, decimal, a);
        ScalarOperator nullableRange = Utils.compoundOr(range, new IsNullPredicateOperator(decimal));
        // Exercise decimal type validation on a compound range, not just discrete IN values.
        assertEquals(nullableRange, new ScalarRangePredicateExtractor().rewriteOnlyColumn(nullableRange));
    }

    @Test
    void testRelationUsesNonNullBounds() {
        ScalarOperator nullableRange = orNull(compare(BinaryType.GT, 2));
        ScalarOperator relation = new BinaryPredicateOperator(BinaryType.GT, y, x);
        Map<ScalarOperator, RangeExtractor.ValueDescriptor> values =
                new RangeExtractor().apply(Utils.compoundAnd(nullableRange, relation), null);
        assertEquals(List.of(new BinaryPredicateOperator(BinaryType.GT, y, ConstantOperator.createInt(2))),
                values.get(y).toScalarOperator());
        assertEquals(List.of(nullableRange), values.get(x).toScalarOperator());
    }

    @Test
    void testNullOnlySourceHasNoRelationBound() {
        Map<ScalarOperator, RangeExtractor.ValueDescriptor> values = new RangeExtractor().apply(
                Utils.compoundAnd(new IsNullPredicateOperator(x),
                        new BinaryPredicateOperator(BinaryType.GT, y, x)), null);
        assertFalse(values.containsKey(y));
        assertEquals(List.of(new IsNullPredicateOperator(x)), values.get(x).toScalarOperator());
    }
}
