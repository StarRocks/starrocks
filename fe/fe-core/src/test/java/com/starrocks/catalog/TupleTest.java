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

package com.starrocks.catalog;

import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TupleTest {

    // ==================== Basic Tuple Tests ====================

    @Test
    public void testTupleCreation() {
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "hello"),
                new BoolVariant(true));

        Tuple tuple = new Tuple(values);
        Assertions.assertNotNull(tuple.getValues());
        Assertions.assertEquals(3, tuple.getValues().size());
        Assertions.assertEquals(1L, tuple.getValues().get(0).getLongValue());
        Assertions.assertEquals("hello", tuple.getValues().get(1).getStringValue());
        Assertions.assertEquals(1L, tuple.getValues().get(2).getLongValue());
    }

    @Test
    public void testEmptyTuple() {
        List<Variant> emptyValues = Collections.emptyList();
        Tuple tuple = new Tuple(emptyValues);

        Assertions.assertNotNull(tuple.getValues());
        Assertions.assertEquals(0, tuple.getValues().size());
    }

    @Test
    public void testSingleElementTuple() {
        List<Variant> singleValue = Collections.singletonList(
                new IntVariant(IntegerType.INT, 42));

        Tuple tuple = new Tuple(singleValue);
        Assertions.assertEquals(1, tuple.getValues().size());
        Assertions.assertEquals(42L, tuple.getValues().get(0).getLongValue());
    }

    // ==================== Tuple Comparison Tests ====================

    @Test
    public void testTupleCompareToEqual() {
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 3));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 3));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertEquals(0, tuple1.compareTo(tuple2));
        Assertions.assertEquals(0, tuple2.compareTo(tuple1));
    }

    @Test
    public void testTupleCompareToLessThan() {
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 3));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.compareTo(tuple2) < 0);
        Assertions.assertTrue(tuple2.compareTo(tuple1) > 0);
    }

    @Test
    public void testTupleCompareToGreaterThan() {
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 1));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 10));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.compareTo(tuple2) > 0);
        Assertions.assertTrue(tuple2.compareTo(tuple1) < 0);
    }

    @Test
    public void testTupleCompareDifferentLengths() {
        // Shorter tuple should be less than longer tuple when all common elements are
        // equal
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 3));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.compareTo(tuple2) < 0);
        Assertions.assertTrue(tuple2.compareTo(tuple1) > 0);
    }

    @Test
    public void testTupleCompareEmptyTuples() {
        Tuple tuple1 = new Tuple(Collections.emptyList());
        Tuple tuple2 = new Tuple(Collections.emptyList());

        Assertions.assertEquals(0, tuple1.compareTo(tuple2));
    }

    @Test
    public void testTupleCompareEmptyWithNonEmpty() {
        Tuple emptyTuple = new Tuple(Collections.emptyList());
        Tuple nonEmptyTuple = new Tuple(Collections.singletonList(
                new IntVariant(IntegerType.INT, 1)));

        Assertions.assertTrue(emptyTuple.compareTo(nonEmptyTuple) < 0);
        Assertions.assertTrue(nonEmptyTuple.compareTo(emptyTuple) > 0);
    }

    // ==================== Mixed Type Tuple Tests ====================

    @Test
    public void testMixedTypeTuple() {
        List<Variant> mixedValues = Arrays.asList(
                new IntVariant(IntegerType.INT, 100),
                new StringVariant(VarcharType.VARCHAR, "test"),
                new BoolVariant(false),
                new LargeIntVariant("12345678901234567890"));

        Tuple tuple = new Tuple(mixedValues);

        Assertions.assertEquals(4, tuple.getValues().size());
        Assertions.assertEquals(100L, tuple.getValues().get(0).getLongValue());
        Assertions.assertEquals("test", tuple.getValues().get(1).getStringValue());
        Assertions.assertEquals(0L, tuple.getValues().get(2).getLongValue());
        Assertions.assertEquals("12345678901234567890", tuple.getValues().get(3).getStringValue());
    }

    @Test
    public void testMixedTypeTupleComparison() {
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "abc"));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "abd"));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.compareTo(tuple2) < 0);
    }

    // ==================== Tuple with Different Integer Types ====================

    @Test
    public void testTupleWithDifferentIntTypes() {
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.TINYINT, 10),
                new IntVariant(IntegerType.SMALLINT, 1000),
                new IntVariant(IntegerType.INT, 100000),
                new IntVariant(IntegerType.BIGINT, 10000000000L));

        Tuple tuple = new Tuple(values);

        Assertions.assertEquals(4, tuple.getValues().size());
        Assertions.assertEquals(10L, tuple.getValues().get(0).getLongValue());
        Assertions.assertEquals(1000L, tuple.getValues().get(1).getLongValue());
        Assertions.assertEquals(100000L, tuple.getValues().get(2).getLongValue());
        Assertions.assertEquals(10000000000L, tuple.getValues().get(3).getLongValue());
    }

    @Test
    public void testTupleComparisonWithCompatibleTypes() {
        // INT and BIGINT with same value should compare equal
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 100),
                new IntVariant(IntegerType.INT, 200));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.BIGINT, 100),
                new IntVariant(IntegerType.BIGINT, 200));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertEquals(0, tuple1.compareTo(tuple2));
    }

    // ==================== Large Tuple Tests ====================

    @Test
    public void testLargeTuple() {
        // Create a tuple with many elements
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 3),
                new IntVariant(IntegerType.INT, 4),
                new IntVariant(IntegerType.INT, 5),
                new IntVariant(IntegerType.INT, 6),
                new IntVariant(IntegerType.INT, 7),
                new IntVariant(IntegerType.INT, 8),
                new IntVariant(IntegerType.INT, 9),
                new IntVariant(IntegerType.INT, 10));

        Tuple tuple = new Tuple(values);
        Assertions.assertEquals(10, tuple.getValues().size());
    }

    @Test
    public void testLargeTupleComparison() {
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 3),
                new IntVariant(IntegerType.INT, 4),
                new IntVariant(IntegerType.INT, 5));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 3),
                new IntVariant(IntegerType.INT, 4),
                new IntVariant(IntegerType.INT, 6) // Different in last element
        );

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.compareTo(tuple2) < 0);
    }

    // ==================== Tuple with DateTime Tests ====================

    @Test
    public void testTupleWithDateTime() {
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00"));

        Tuple tuple = new Tuple(values);

        Assertions.assertEquals(2, tuple.getValues().size());
        Assertions.assertEquals(1L, tuple.getValues().get(0).getLongValue());
        Assertions.assertTrue(tuple.getValues().get(1) instanceof DateVariant);
    }

    @Test
    public void testTupleComparisonWithDateTime() {
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new DateVariant(DateType.DATETIME, "2024-01-15T10:30:00"));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new DateVariant(DateType.DATETIME, "2024-01-15T10:30:01"));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.compareTo(tuple2) < 0);
    }

    // ==================== Tuple Serialization Tests ====================

    @Test
    public void testTupleSerialization() {
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, 100),
                new StringVariant(VarcharType.VARCHAR, "test"));

        Tuple originalTuple = new Tuple(values);

        // Serialize
        String json = GsonUtils.GSON.toJson(originalTuple);
        Assertions.assertNotNull(json);
        Assertions.assertTrue(json.length() > 0);

        // Deserialize
        Tuple deserializedTuple = GsonUtils.GSON.fromJson(json, Tuple.class);
        Assertions.assertNotNull(deserializedTuple);
        Assertions.assertEquals(2, deserializedTuple.getValues().size());

        // Note: After deserialization, we need to verify the values
        Assertions.assertEquals(0, originalTuple.compareTo(deserializedTuple));
    }

    @Test
    public void testTupleSerializationWithMixedTypes() {
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new BoolVariant(true),
                new StringVariant(VarcharType.VARCHAR, "hello"),
                new LargeIntVariant("12345678901234567890"));

        Tuple originalTuple = new Tuple(values);

        // Serialize and deserialize
        String json = GsonUtils.GSON.toJson(originalTuple);
        Tuple deserializedTuple = GsonUtils.GSON.fromJson(json, Tuple.class);

        Assertions.assertNotNull(deserializedTuple);
        Assertions.assertEquals(4, deserializedTuple.getValues().size());
        Assertions.assertEquals(0, originalTuple.compareTo(deserializedTuple));
    }

    @Test
    public void testEmptyTupleSerialization() {
        Tuple emptyTuple = new Tuple(Collections.emptyList());

        String json = GsonUtils.GSON.toJson(emptyTuple);
        Tuple deserializedTuple = GsonUtils.GSON.fromJson(json, Tuple.class);

        Assertions.assertNotNull(deserializedTuple);
        Assertions.assertEquals(0, deserializedTuple.getValues().size());
    }

    // ==================== Edge Cases ====================

    @Test
    public void testTupleWithNegativeValues() {
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, -100),
                new IntVariant(IntegerType.INT, -200),
                new LargeIntVariant("-12345678901234567890"));

        Tuple tuple = new Tuple(values);

        Assertions.assertEquals(3, tuple.getValues().size());
        Assertions.assertEquals(-100L, tuple.getValues().get(0).getLongValue());
        Assertions.assertEquals(-200L, tuple.getValues().get(1).getLongValue());
        Assertions.assertEquals("-12345678901234567890", tuple.getValues().get(2).getStringValue());
    }

    @Test
    public void testTupleComparisonNegativeValues() {
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, -100),
                new IntVariant(IntegerType.INT, 50));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, -50),
                new IntVariant(IntegerType.INT, 100));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        // -100 < -50, so tuple1 < tuple2
        Assertions.assertTrue(tuple1.compareTo(tuple2) < 0);
    }

    @Test
    public void testTupleWithZeroValues() {
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, 0),
                new BoolVariant(false),
                new LargeIntVariant("0"));

        Tuple tuple = new Tuple(values);

        Assertions.assertEquals(3, tuple.getValues().size());
        Assertions.assertEquals(0L, tuple.getValues().get(0).getLongValue());
        Assertions.assertEquals(0L, tuple.getValues().get(1).getLongValue());
        Assertions.assertEquals(0L, tuple.getValues().get(2).getLongValue());
    }

    @Test
    public void testTupleWithMaxMinValues() {
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, Integer.MAX_VALUE),
                new IntVariant(IntegerType.INT, Integer.MIN_VALUE),
                new IntVariant(IntegerType.BIGINT, Long.MAX_VALUE),
                new IntVariant(IntegerType.BIGINT, Long.MIN_VALUE));

        Tuple tuple = new Tuple(values);

        Assertions.assertEquals(4, tuple.getValues().size());
        Assertions.assertEquals((long) Integer.MAX_VALUE, tuple.getValues().get(0).getLongValue());
        Assertions.assertEquals((long) Integer.MIN_VALUE, tuple.getValues().get(1).getLongValue());
        Assertions.assertEquals(Long.MAX_VALUE, tuple.getValues().get(2).getLongValue());
        Assertions.assertEquals(Long.MIN_VALUE, tuple.getValues().get(3).getLongValue());
    }

    @Test
    public void testTupleComparisonWithFirstElementDifferent() {
        // When first elements differ, comparison should stop there
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 999));

        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 1));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.compareTo(tuple2) < 0);
    }

    @Test
    public void testTupleSelfComparison() {
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "test"));

        Tuple tuple = new Tuple(values);

        Assertions.assertEquals(0, tuple.compareTo(tuple));
    }

    // ==================== Tuple equals() Tests ====================

    @Test
    public void testTupleEqualsReflexive() {
        // Test reflexive property: x.equals(x) should return true
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "test"));
        Tuple tuple = new Tuple(values);

        Assertions.assertTrue(tuple.equals(tuple));
    }

    @Test
    public void testTupleEqualsSymmetric() {
        // Test symmetric property: x.equals(y) == y.equals(x)
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 100),
                new BoolVariant(true));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 100),
                new BoolVariant(true));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.equals(tuple2));
        Assertions.assertTrue(tuple2.equals(tuple1));
    }

    @Test
    public void testTupleEqualsTransitive() {
        // Test transitive property: if x.equals(y) and y.equals(z), then x.equals(z)
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));
        List<Variant> values3 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);
        Tuple tuple3 = new Tuple(values3);

        Assertions.assertTrue(tuple1.equals(tuple2));
        Assertions.assertTrue(tuple2.equals(tuple3));
        Assertions.assertTrue(tuple1.equals(tuple3));
    }

    @Test
    public void testTupleEqualsNull() {
        // Test that equals(null) returns false
        List<Variant> values = Arrays.asList(new IntVariant(IntegerType.INT, 1));
        Tuple tuple = new Tuple(values);

        Assertions.assertFalse(tuple.equals(null));
    }

    @Test
    public void testTupleEqualsDifferentClass() {
        // Test that equals returns false for different class
        List<Variant> values = Arrays.asList(new IntVariant(IntegerType.INT, 1));
        Tuple tuple = new Tuple(values);

        Assertions.assertFalse(tuple.equals("not a tuple"));
        Assertions.assertFalse(tuple.equals(Integer.valueOf(1)));
    }

    @Test
    public void testTupleEqualsSameValues() {
        // Tuples with same values should be equal
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 42),
                new StringVariant(VarcharType.VARCHAR, "hello"),
                new BoolVariant(true));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 42),
                new StringVariant(VarcharType.VARCHAR, "hello"),
                new BoolVariant(true));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.equals(tuple2));
        Assertions.assertTrue(tuple2.equals(tuple1));
    }

    @Test
    public void testTupleEqualsDifferentValues() {
        // Tuples with different values should not be equal
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 3));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertFalse(tuple1.equals(tuple2));
        Assertions.assertFalse(tuple2.equals(tuple1));
    }

    @Test
    public void testTupleEqualsDifferentLengths() {
        // Tuples with different lengths should not be equal
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 3));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertFalse(tuple1.equals(tuple2));
        Assertions.assertFalse(tuple2.equals(tuple1));
    }

    @Test
    public void testTupleEqualsEmptyTuples() {
        // Empty tuples should be equal
        Tuple tuple1 = new Tuple(Collections.emptyList());
        Tuple tuple2 = new Tuple(Collections.emptyList());

        Assertions.assertTrue(tuple1.equals(tuple2));
    }

    @Test
    public void testTupleEqualsMixedTypes() {
        // Tuples with mixed types and same values should be equal
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 100),
                new StringVariant(VarcharType.VARCHAR, "test"),
                new BoolVariant(false),
                new LargeIntVariant("999"));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 100),
                new StringVariant(VarcharType.VARCHAR, "test"),
                new BoolVariant(false),
                new LargeIntVariant("999"));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.equals(tuple2));
    }

    // ==================== Tuple hashCode() Tests ====================

    @Test
    public void testTupleHashCodeConsistency() {
        // hashCode should return the same value when called multiple times
        List<Variant> values = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "test"));
        Tuple tuple = new Tuple(values);

        int hash1 = tuple.hashCode();
        int hash2 = tuple.hashCode();
        int hash3 = tuple.hashCode();

        Assertions.assertEquals(hash1, hash2);
        Assertions.assertEquals(hash2, hash3);
    }

    @Test
    public void testTupleHashCodeEqualObjects() {
        // Equal objects must have equal hash codes
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 42),
                new StringVariant(VarcharType.VARCHAR, "hello"));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 42),
                new StringVariant(VarcharType.VARCHAR, "hello"));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.equals(tuple2));
        Assertions.assertEquals(tuple1.hashCode(), tuple2.hashCode());
    }

    @Test
    public void testTupleHashCodeDifferentObjects() {
        // Different objects may have different hash codes (not required, but desirable)
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 3),
                new IntVariant(IntegerType.INT, 4));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertFalse(tuple1.equals(tuple2));
        // Note: Different objects CAN have the same hash code (collision),
        // but we expect them to be different in most cases
    }

    @Test
    public void testTupleHashCodeEmptyTuples() {
        // Empty tuples should have the same hash code
        Tuple tuple1 = new Tuple(Collections.emptyList());
        Tuple tuple2 = new Tuple(Collections.emptyList());

        Assertions.assertEquals(tuple1.hashCode(), tuple2.hashCode());
    }

    @Test
    public void testTupleHashCodeWithMixedTypes() {
        // Tuples with mixed types should have consistent hash codes
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 100),
                new BoolVariant(true),
                new StringVariant(VarcharType.VARCHAR, "data"),
                new LargeIntVariant("12345"));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 100),
                new BoolVariant(true),
                new StringVariant(VarcharType.VARCHAR, "data"),
                new LargeIntVariant("12345"));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertTrue(tuple1.equals(tuple2));
        Assertions.assertEquals(tuple1.hashCode(), tuple2.hashCode());
    }

    @Test
    public void testTupleHashCodeDifferentOrder() {
        // Tuples with same values in different order should have different hash codes
        List<Variant> values1 = Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new IntVariant(IntegerType.INT, 2));
        List<Variant> values2 = Arrays.asList(
                new IntVariant(IntegerType.INT, 2),
                new IntVariant(IntegerType.INT, 1));

        Tuple tuple1 = new Tuple(values1);
        Tuple tuple2 = new Tuple(values2);

        Assertions.assertFalse(tuple1.equals(tuple2));
        // Hash codes will likely be different (but not guaranteed)
    }
}
