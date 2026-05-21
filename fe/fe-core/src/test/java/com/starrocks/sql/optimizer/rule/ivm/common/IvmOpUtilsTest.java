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

package com.starrocks.sql.optimizer.rule.ivm.common;

import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link IvmOpUtils#typesCompatibleForStateUnion(Type, Type)}.
 *
 * Documents the cases the helper accepts (equal types and string types
 * with the same primitive kind, e.g. VARCHAR with different lengths) and
 * the cases it rejects (different primitive kinds, mismatched decimals).
 */
public class IvmOpUtilsTest {

    @Test
    public void equalTypesAreCompatible() {
        ScalarType a = TypeFactory.createVarcharType(100);
        ScalarType b = TypeFactory.createVarcharType(100);
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(a, b));
    }

    @Test
    public void varcharDifferentLengthsAreCompatible() {
        // Motivating case: delta-side VARCHAR(N) vs MV-side VARCHAR(65533)
        // after AnalyzerUtils.transformTableColumnType widens the MV column.
        ScalarType narrow = TypeFactory.createVarcharType(50);
        ScalarType wide = TypeFactory.createVarcharType(65533);
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(narrow, wide));
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(wide, narrow));
    }

    @Test
    public void charAndVarcharAreNotCompatible() {
        // CHAR and VARCHAR are both string types but have different
        // primitive kinds; the helper must not widen across them.
        ScalarType charType = TypeFactory.createCharType(10);
        ScalarType varcharType = TypeFactory.createVarcharType(10);
        Assertions.assertFalse(IvmOpUtils.typesCompatibleForStateUnion(charType, varcharType));
        Assertions.assertFalse(IvmOpUtils.typesCompatibleForStateUnion(varcharType, charType));
    }

    @Test
    public void varcharAndIntAreNotCompatible() {
        ScalarType varcharType = TypeFactory.createVarcharType(10);
        Type intType = IntegerType.INT;
        Assertions.assertFalse(IvmOpUtils.typesCompatibleForStateUnion(varcharType, intType));
        Assertions.assertFalse(IvmOpUtils.typesCompatibleForStateUnion(intType, varcharType));
    }

    @Test
    public void decimalSamePrecisionScaleEqual() {
        ScalarType a = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        ScalarType b = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(a, b));
    }

    @Test
    public void decimalDifferentScaleNotCompatible() {
        // The helper only widens for string types; decimals must match
        // exactly via equals (precision + scale).
        ScalarType a = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        ScalarType b = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 3);
        Assertions.assertFalse(IvmOpUtils.typesCompatibleForStateUnion(a, b));
        Assertions.assertFalse(IvmOpUtils.typesCompatibleForStateUnion(b, a));
    }

    // Nested types — ARRAY_AGG's state is struct<array<varchar>> with the delta
    // side at VARCHAR(1073741824) and MV side at VARCHAR(OlapMaxVarcharLength).

    @Test
    public void arrayOfVarcharDifferentLengthsAreCompatible() {
        ArrayType narrow = new ArrayType(TypeFactory.createVarcharType(50));
        ArrayType wide = new ArrayType(TypeFactory.createVarcharType(1073741824));
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(narrow, wide));
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(wide, narrow));
    }

    @Test
    public void arrayElementMismatchedPrimitiveNotCompatible() {
        ArrayType varcharArr = new ArrayType(TypeFactory.createVarcharType(100));
        ArrayType intArr = new ArrayType(IntegerType.INT);
        Assertions.assertFalse(IvmOpUtils.typesCompatibleForStateUnion(varcharArr, intArr));
    }

    @Test
    public void structFieldsRecursiveCompatible() {
        StructType intermediate = new StructType(List.<StructField>of(
                new StructField("col1", new ArrayType(TypeFactory.createVarcharType(1073741824)))), true);
        StructType mvColumn = new StructType(List.<StructField>of(
                new StructField("col1", new ArrayType(TypeFactory.createVarcharType(1048576)))), true);
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(intermediate, mvColumn));
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(mvColumn, intermediate));
    }

    @Test
    public void structDifferentFieldCountNotCompatible() {
        StructType a = new StructType(List.<StructField>of(
                new StructField("col1", TypeFactory.createVarcharType(100))), true);
        StructType b = new StructType(List.<StructField>of(
                new StructField("col1", TypeFactory.createVarcharType(100)),
                new StructField("col2", IntegerType.INT)), true);
        Assertions.assertFalse(IvmOpUtils.typesCompatibleForStateUnion(a, b));
        Assertions.assertFalse(IvmOpUtils.typesCompatibleForStateUnion(b, a));
    }

    @Test
    public void mapOfVarcharKeysAndValuesDifferentLengths() {
        MapType narrow = new MapType(
                TypeFactory.createVarcharType(50), TypeFactory.createVarcharType(50));
        MapType wide = new MapType(
                TypeFactory.createVarcharType(1073741824), TypeFactory.createVarcharType(1073741824));
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(narrow, wide));
        Assertions.assertTrue(IvmOpUtils.typesCompatibleForStateUnion(wide, narrow));
    }
}
