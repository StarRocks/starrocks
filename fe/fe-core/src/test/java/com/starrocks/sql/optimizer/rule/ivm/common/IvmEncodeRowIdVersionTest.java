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

import com.google.common.collect.Lists;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link IvmOpUtils#deduceEncodeRowIdVersion(List)}.
 *
 * <p>Which keys may use the order-preserving {@code encode_sort_key} is not just a size question: the
 * backend encoder rejects some fixed-length types outright and mis-encodes constants, and a row id that
 * cannot be encoded turns a slow refresh into a failing one.
 */
public class IvmEncodeRowIdVersionTest {

    private static Expr key(Type type) {
        SlotRef slotRef = new SlotRef(new TableName("db", "t"), "c");
        slotRef.setType(type);
        return slotRef;
    }

    private static List<Expr> keys(Type... types) {
        List<Expr> keys = Lists.newArrayList();
        for (Type type : types) {
            keys.add(key(type));
        }
        return keys;
    }

    private static void assertSortKey(List<Expr> keys) {
        Assertions.assertEquals(IvmOpUtils.ENCODE_ROW_ID_VERSION_SORT_KEY,
                IvmOpUtils.deduceEncodeRowIdVersion(keys));
    }

    private static void assertFingerprint(List<Expr> keys) {
        Assertions.assertEquals(IvmOpUtils.ENCODE_ROW_ID_VERSION_FINGERPRINT,
                IvmOpUtils.deduceEncodeRowIdVersion(keys));
    }

    @Test
    public void narrowFixedLengthKeysUseSortKey() {
        assertSortKey(keys(IntegerType.INT));
        assertSortKey(keys(IntegerType.BIGINT));
        assertSortKey(keys(IntegerType.TINYINT));
        assertSortKey(keys(BooleanType.BOOLEAN));
        assertSortKey(keys(DateType.DATE, IntegerType.BIGINT));
        // DATE encodes as 4 bytes and DATETIME as 8 -- not the 16-byte tuple slot size -- so three
        // temporal/int keys are 20 bytes and still fit the budget.
        assertSortKey(keys(DateType.DATETIME, DateType.DATETIME, IntegerType.INT));
    }

    /**
     * encode_float32 emits 4 bytes for a FLOAT where {@code PrimitiveType.getTypeSize()} reports 8, so
     * budgeting by slot size would push five FLOAT keys (29 bytes encoded) onto the fingerprint. Six of
     * them are 35 and do belong there.
     */
    @Test
    public void floatKeysAreBudgetedAtTheirEncodedWidth() {
        assertSortKey(keys(FloatType.FLOAT, FloatType.FLOAT, FloatType.FLOAT, FloatType.FLOAT,
                FloatType.FLOAT));
        assertFingerprint(keys(FloatType.FLOAT, FloatType.FLOAT, FloatType.FLOAT, FloatType.FLOAT,
                FloatType.FLOAT, FloatType.FLOAT));
        // DOUBLE really is 8 bytes, so four of them are already 39.
        assertFingerprint(keys(FloatType.DOUBLE, FloatType.DOUBLE, FloatType.DOUBLE, FloatType.DOUBLE));
    }

    /**
     * The budget is the width the keys actually occupy: a null marker per key and a separator between
     * keys, so three BIGINTs are 29 bytes and four are 39, not 24 and 32.
     */
    @Test
    public void keysOverTheByteBudgetUseFingerprint() {
        assertSortKey(keys(IntegerType.BIGINT, IntegerType.BIGINT, IntegerType.BIGINT));
        assertFingerprint(keys(IntegerType.BIGINT, IntegerType.BIGINT, IntegerType.BIGINT,
                IntegerType.BIGINT));
    }

    @Test
    public void variableLengthAndComplexKeysUseFingerprint() {
        assertFingerprint(keys(TypeFactory.createVarcharType(32)));
        assertFingerprint(keys(TypeFactory.createCharType(8)));
        assertFingerprint(keys(IntegerType.INT, TypeFactory.createVarcharType(8)));
        assertFingerprint(keys(new ArrayType(IntegerType.INT)));
    }

    /**
     * UtilityFunctions::encode_sort_key encodes integrals of at most 8 bytes, so an int128 or int256 payload
     * fails with "unsupported argument type" -- even though LARGEINT (16), DECIMAL128 (16) and DECIMAL256 (32)
     * all fit the byte budget. LARGEINT is a legal PRIMARY KEY type, so this is reachable.
     */
    @Test
    public void keysTheBackendCannotOrderEncodeUseFingerprint() {
        assertFingerprint(keys(IntegerType.LARGEINT));
        assertFingerprint(keys(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2)));
        assertFingerprint(keys(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 2)));
    }

    /**
     * A constant reaches the backend as a ConstColumn, which encode_sort_key appends to the first row's
     * buffer once per row in the chunk and to no other row: that key overflows primary_key_limit_size and
     * every other row loses the constant, so two keys differing only in it would encode identically.
     */
    @Test
    public void constantKeysUseFingerprint() {
        assertFingerprint(Lists.newArrayList(new IntLiteral(0), key(IntegerType.BIGINT)));
        assertFingerprint(Lists.newArrayList(new IntLiteral(7)));
    }

    @Test
    public void unknownOrEmptyKeysUseFingerprint() {
        assertFingerprint(keys(InvalidType.INVALID));
        assertFingerprint(Lists.newArrayList());
    }
}
