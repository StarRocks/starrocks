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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.Column;
import com.starrocks.type.ArrayType;
import com.starrocks.type.CharType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StringType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Covers {@link MVRefreshSchemaChecker#isColumnCompatible} for cases the SR SQL test framework
 * cannot reach: iceberg STRUCT/ARRAY/MAP field-level ALTER from Spark/Trino. SR SQL itself
 * rejects {@code ALTER ... ADD FIELD / DROP FIELD} clauses on non-OLAP tables, but the iceberg
 * table can still be modified out of band.
 */
public class MVRefreshSchemaCheckerTest {

    private static boolean isCompatible(Column existed, Type derivedType) {
        return MVRefreshSchemaChecker.isColumnCompatible(existed, derivedType);
    }

    private static StructType struct(StructField... fields) {
        return new StructType(new ArrayList<>(Arrays.asList(fields)));
    }

    @Test
    public void testScalarSameTypeCompatible() {
        Assertions.assertTrue(isCompatible(new Column("k", IntegerType.INT), IntegerType.INT));
        Assertions.assertTrue(isCompatible(new Column("v", IntegerType.BIGINT), IntegerType.BIGINT));
    }

    @Test
    public void testScalarWideningDriftDetected() {
        Assertions.assertFalse(isCompatible(new Column("v", IntegerType.INT), IntegerType.BIGINT));
        Assertions.assertFalse(isCompatible(new Column("v", IntegerType.BIGINT), IntegerType.INT));
    }

    @Test
    public void testStringWidthInterchangeable() {
        // matchesType treats CHAR/VARCHAR of any width as compatible — required so the iceberg
        // `string` → varchar(1073741824) shape doesn't false-positive against a stored MV
        // column that may be varchar(65533).
        Column existed = new Column("s", new VarcharType(50));
        Assertions.assertTrue(isCompatible(existed, new VarcharType(1073741824)));
        Assertions.assertTrue(isCompatible(existed, StringType.STRING));
        Assertions.assertTrue(isCompatible(existed, new CharType(20)));
    }

    @Test
    public void testArrayInnerWideningDriftDetected() {
        // A PrimitiveType.equals shortcut would miss this: both sides return INVALID_TYPE for
        // non-scalar Type, leaving silent NULL / refresh failure with MV still active.
        Column existed = new Column("arr", new ArrayType(IntegerType.INT));
        Assertions.assertTrue(isCompatible(existed, new ArrayType(IntegerType.INT)));
        Assertions.assertFalse(isCompatible(existed, new ArrayType(IntegerType.BIGINT)));
    }

    @Test
    public void testMapKeyWideningDriftDetected() {
        Column existed = new Column("m", new MapType(IntegerType.INT, StringType.STRING));
        Assertions.assertFalse(isCompatible(existed, new MapType(IntegerType.BIGINT, StringType.STRING)));
    }

    @Test
    public void testMapValueWideningDriftDetected() {
        Column existed = new Column("m", new MapType(IntegerType.INT, IntegerType.INT));
        Assertions.assertFalse(isCompatible(existed, new MapType(IntegerType.INT, IntegerType.BIGINT)));
    }

    @Test
    public void testStructFieldTypeDriftDetected() {
        // External engine (Spark) widening a STRUCT field — SR SQL itself rejects this clause
        // on non-OLAP tables, but the iceberg table can still be ALTER'd out of band.
        StructType original = struct(
                new StructField("a", IntegerType.INT),
                new StructField("b", StringType.STRING));
        StructType drifted = struct(
                new StructField("a", IntegerType.BIGINT),
                new StructField("b", StringType.STRING));
        Assertions.assertTrue(isCompatible(new Column("s", original), original));
        Assertions.assertFalse(isCompatible(new Column("s", original), drifted));
    }

    @Test
    public void testStructFieldAddedDriftDetected() {
        StructType original = struct(
                new StructField("a", IntegerType.INT),
                new StructField("b", StringType.STRING));
        StructType wider = struct(
                new StructField("a", IntegerType.INT),
                new StructField("b", StringType.STRING),
                new StructField("c", IntegerType.BIGINT));
        Assertions.assertFalse(isCompatible(new Column("s", original), wider));
    }

    @Test
    public void testStructFieldRenamedDriftDetected() {
        StructType original = struct(
                new StructField("a", IntegerType.INT),
                new StructField("b", StringType.STRING));
        StructType renamed = struct(
                new StructField("a", IntegerType.INT),
                new StructField("b2", StringType.STRING));
        Assertions.assertFalse(isCompatible(new Column("s", original), renamed));
    }

    @Test
    public void testDecimalPrecisionWideningDriftDetected() {
        // DECIMAL(10,2) and DECIMAL(18,2) both bucket into PrimitiveType.DECIMAL64, so
        // matchesType alone returns true — but storing DECIMAL(18,2) values into a
        // DECIMAL(10,2) MV column overflows. Explicit precision check guards against this.
        Column existed = new Column("d", new DecimalType(PrimitiveType.DECIMAL64, 10, 2));
        Assertions.assertTrue(isCompatible(existed, new DecimalType(PrimitiveType.DECIMAL64, 10, 2)));
        Assertions.assertFalse(isCompatible(existed, new DecimalType(PrimitiveType.DECIMAL64, 18, 2)));
    }

    @Test
    public void testDecimalScaleChangeDriftDetected() {
        // matchesType DECIMAL V3 branch already enforces scale equality; assertion guards
        // against future relaxation of that branch.
        Column existed = new Column("d", new DecimalType(PrimitiveType.DECIMAL64, 10, 2));
        Assertions.assertFalse(isCompatible(existed, new DecimalType(PrimitiveType.DECIMAL64, 10, 4)));
    }

    @Test
    public void testDecimalCrossBucketWideningDriftDetected() {
        // Baseline: cross-PrimitiveType widening (DECIMAL64 → DECIMAL128) is already caught
        // by matchesType. Kept for regression coverage.
        Column existed = new Column("d", new DecimalType(PrimitiveType.DECIMAL64, 10, 2));
        Assertions.assertFalse(isCompatible(existed, new DecimalType(PrimitiveType.DECIMAL128, 20, 2)));
    }

    @Test
    public void testNestedArrayOfStructDriftDetected() {
        StructType originalElem = struct(new StructField("a", IntegerType.INT));
        StructType driftedElem = struct(new StructField("a", IntegerType.BIGINT));
        Column existed = new Column("arr", new ArrayType(originalElem));
        Assertions.assertFalse(isCompatible(existed, new ArrayType(driftedElem)));
    }
}
