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

package com.starrocks.connector.odps;

import com.aliyun.odps.table.optimizer.predicate.Attribute;
import com.aliyun.odps.table.optimizer.predicate.BinaryPredicate;
import com.aliyun.odps.table.optimizer.predicate.CompoundPredicate;
import com.aliyun.odps.table.optimizer.predicate.Constant;
import com.aliyun.odps.table.optimizer.predicate.InPredicate;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.optimizer.predicate.UnaryPredicate;
import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for EntityConvertUtils.convertPredicate
 */
public class PredicatePushdownTest {

    private static final String COL1_NAME = "column1";
    private static final String COL2_NAME = "column2";
    private static final String COL3_NAME = "column3";
    private static final String PARTITION_COL_NAME = "partitionCol";
    private static final String CHINESE_COL_NAME = "你好";

    // Helper method to create a common ColumnRefOperator for tests
    private ColumnRefOperator createColumnRef(String name, Type type) {
        // Assuming ID 0 is okay for tests, adjust if needed
        return new ColumnRefOperator(0, type, name, true);
    }

    // Helper method to check if a predicate is NO_PREDICATE
    private void assertNoPredicate(Predicate result) {
        assertSame(Predicate.NO_PREDICATE, result, "Expected NO_PREDICATE but got: " + result);
    }

    // Helper method to create a simple binary predicate
    private BinaryPredicateOperator createBinaryOp(BinaryType type, String colName, Object value, Type valueType) {
        return new BinaryPredicateOperator(type, createColumnRef(colName, valueType),
                new ConstantOperator(value, valueType));
    }

    // Helper method to create a simple unary predicate
    private IsNullPredicateOperator createIsNullOp(boolean isNotNull, String colName, Type colType) {
        return new IsNullPredicateOperator(!isNotNull,
                createColumnRef(colName, colType)); // Note: IsNullPredicateOperator constructor might take isNull flag
    }

    @Test
    public void testConvertPredicate_emptyOrNoOp() {
        // Spark tests for empty Seq, AlwaysTrue, AlwaysFalse. For StarRocks, we test with null or empty logic if applicable.
        // If there's no direct equivalent, testing null input might be relevant.
        // Assuming convertPredicate handles null gracefully or is called differently.
        // Let's test a scenario that should ideally result in no predicate, e.g., a predicate on a partition column.
        HashSet<String> partitionColumns = new HashSet<>(Collections.singletonList(COL1_NAME));
        ScalarOperator predOnPartitionCol = createColumnRef(COL1_NAME, Type.BIGINT);
        Predicate result = EntityConvertUtils.convertPredicate(predOnPartitionCol, partitionColumns);
        assertNoPredicate(result);
    }

    @Test
    public void testConvertPredicate_equalTo() {
        BinaryPredicateOperator pred = createBinaryOp(BinaryType.EQ, COL1_NAME, 42, Type.BIGINT);
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = BinaryPredicate.equals(Attribute.of(COL1_NAME), Constant.of(42L));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_greaterThan() {
        BinaryPredicateOperator pred = createBinaryOp(BinaryType.GT, COL1_NAME, 10, Type.BIGINT);
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = BinaryPredicate.greaterThan(Attribute.of(COL1_NAME), Constant.of(10L));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_lessThan() {
        BinaryPredicateOperator pred = createBinaryOp(BinaryType.LT, COL1_NAME, 5.5, Type.DOUBLE);
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = BinaryPredicate.lessThan(Attribute.of(COL1_NAME), Constant.of(5.5));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_greaterThanOrEqual() {
        BinaryPredicateOperator pred = createBinaryOp(BinaryType.GE, COL1_NAME, "value", Type.VARCHAR);
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = BinaryPredicate.greaterThanOrEqual(Attribute.of(COL1_NAME), Constant.of("value"));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_lessThanOrEqual() {
        BinaryPredicateOperator pred = createBinaryOp(BinaryType.LE, COL1_NAME, 100L, Type.BIGINT);
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = BinaryPredicate.lessThanOrEqual(Attribute.of(COL1_NAME), Constant.of(100L));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_isNull() {
        IsNullPredicateOperator pred = createIsNullOp(false, COL1_NAME, Type.BIGINT); // isNull = false means IS NULL
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = UnaryPredicate.isNull(Attribute.of(COL1_NAME));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_isNotNull() {
        IsNullPredicateOperator pred =
                createIsNullOp(true, COL1_NAME, Type.BIGINT); // isNotNull = true means IS NOT NULL
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = UnaryPredicate.notNull(Attribute.of(COL1_NAME));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_in() {
        InPredicateOperator pred = new InPredicateOperator(false, // false means IN
                createColumnRef(COL1_NAME, Type.VARCHAR),
                new ConstantOperator("val1", Type.VARCHAR),
                new ConstantOperator("val2", Type.VARCHAR),
                new ConstantOperator("val3", Type.VARCHAR)
        );
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = InPredicate.in(Attribute.of(COL1_NAME),
                Arrays.asList(Constant.of("val1"), Constant.of("val2"), Constant.of("val3")));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_notIn() {
        InPredicateOperator pred = new InPredicateOperator(true, // true means NOT IN
                createColumnRef(COL1_NAME, Type.VARCHAR),
                new ConstantOperator("valA", Type.VARCHAR),
                new ConstantOperator("valB", Type.VARCHAR)
        );
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = InPredicate.notIn(Attribute.of(COL1_NAME),
                Arrays.asList(Constant.of("valA"), Constant.of("valB")));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_and() {
        CompoundPredicateOperator pred = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                createBinaryOp(BinaryType.EQ, COL1_NAME, "value", Type.VARCHAR),
                createBinaryOp(BinaryType.GT, COL2_NAME, 10, Type.BIGINT)
        );
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = CompoundPredicate.and(
                BinaryPredicate.equals(Attribute.of(COL1_NAME), Constant.of("value")),
                BinaryPredicate.greaterThan(Attribute.of(COL2_NAME), Constant.of(10L))
        );
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_or() {
        CompoundPredicateOperator pred = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                createBinaryOp(BinaryType.LT, COL1_NAME, 5, Type.BIGINT),
                createIsNullOp(true, COL2_NAME, Type.VARCHAR) // IS NOT NULL
        );
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = CompoundPredicate.or(
                BinaryPredicate.lessThan(Attribute.of(COL1_NAME), Constant.of(5L)),
                UnaryPredicate.notNull(Attribute.of(COL2_NAME))
        );
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_not() {
        CompoundPredicateOperator pred = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                createIsNullOp(false, COL1_NAME, Type.BIGINT) // IS NULL
        );
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = CompoundPredicate.not(
                UnaryPredicate.isNull(Attribute.of(COL1_NAME))
        );
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_complexNested() {
        // Equivalent to: ((col1 = 'v1' AND col2 > 10) OR (col1 = 'v2' AND col3 < 5))
        CompoundPredicateOperator innerAnd1 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                createBinaryOp(BinaryType.EQ, COL1_NAME, "v1", Type.VARCHAR),
                createBinaryOp(BinaryType.GT, COL2_NAME, 10, Type.BIGINT)
        );
        CompoundPredicateOperator innerAnd2 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                createBinaryOp(BinaryType.EQ, COL1_NAME, "v2", Type.VARCHAR),
                createBinaryOp(BinaryType.LT, COL3_NAME, 5, Type.BIGINT)
        );
        CompoundPredicateOperator outerOr = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                innerAnd1, innerAnd2
        );

        Predicate result = EntityConvertUtils.convertPredicate(outerOr, new HashSet<>());

        Predicate expected = CompoundPredicate.or(
                CompoundPredicate.and(
                        BinaryPredicate.equals(Attribute.of(COL1_NAME), Constant.of("v1")),
                        BinaryPredicate.greaterThan(Attribute.of(COL2_NAME), Constant.of(10L))
                ),
                CompoundPredicate.and(
                        BinaryPredicate.equals(Attribute.of(COL1_NAME), Constant.of("v2")),
                        BinaryPredicate.lessThan(Attribute.of(COL3_NAME), Constant.of(5L))
                )
        );
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_partitionColumnHandling() {
        // Test that predicates involving partition columns are handled correctly (e.g., filtered out or marked specially)
        // Scenario: (col1 = 'val' OR partitionCol startsWith 'p') AND (col2 > 10 AND partitionCol IS NOT NULL)
        // Assuming StarRocks filters out unsupported or partition col parts:
        // Expected result after filtering: col2 > 10
        // Note: StringStartsWith is not directly supported, so it should be filtered out in OR.
        // IS NOT NULL on partition col should also be filtered out.

        // Let's test a simpler case first: AND with one partition predicate
        CompoundPredicateOperator predWithPartitionInAnd =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        createBinaryOp(BinaryType.GE, COL2_NAME, 10, Type.BIGINT),
                        createIsNullOp(false, PARTITION_COL_NAME, Type.VARCHAR) // IS NULL on partition col
                );
        Predicate result1 =
                EntityConvertUtils.convertPredicate(predWithPartitionInAnd, ImmutableSet.of(PARTITION_COL_NAME));
        // Expected: Only col2 > 10 remains
        assertEquals("col2 > 10", result1.toString());

        // Test with OR where one part is on partition col (unsupported op)
        CompoundPredicateOperator predWithPartitionInOr =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                        createBinaryOp(BinaryType.EQ, COL1_NAME, "val", Type.VARCHAR),
                        createBinaryOp(BinaryType.GE, PARTITION_COL_NAME, "prefix", Type.VARCHAR)
                );
        Predicate result2 =
                EntityConvertUtils.convertPredicate(predWithPartitionInOr, ImmutableSet.of(PARTITION_COL_NAME));
        // Expected: No predicate remains
        assertNoPredicate(result2);

        CompoundPredicateOperator predWithInAndOR =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        predWithPartitionInAnd, predWithPartitionInOr);
        Predicate result3 =
                EntityConvertUtils.convertPredicate(predWithInAndOR, ImmutableSet.of(PARTITION_COL_NAME));
        // Expected: Only col2 > 10 remains
        assertEquals("col2 > 10", result3.toString());
    }

    @Test
    public void testConvertPredicate_stringHandlingAndEscaping() {
        // Test string constants with special characters that might need escaping
        // Note: StarRocks Predicate.Constant.of(String) should handle escaping internally if needed by the target system.
        String stringWithQuote = "I'm fine.";
        BinaryPredicateOperator pred = createBinaryOp(BinaryType.EQ, COL2_NAME, stringWithQuote, Type.VARCHAR);
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = BinaryPredicate.equals(Attribute.of(COL2_NAME), Constant.of(stringWithQuote));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertPredicate_chineseColumnNames() {
        BinaryPredicateOperator pred = createBinaryOp(BinaryType.GT, CHINESE_COL_NAME, 2, Type.BIGINT);
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());

        Predicate expected = BinaryPredicate.greaterThan(Attribute.of(CHINESE_COL_NAME), Constant.of(2L));
        assertEquals(expected, result);
    }
}