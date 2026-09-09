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

import com.starrocks.thrift.TAccessPathType;
import com.starrocks.thrift.TColumnAccessPath;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.type.CharType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.JsonType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import com.starrocks.type.VariantType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The value type of an access path is written by the optimizer from an <em>expression</em> type --
 * {@code PruneSubfieldRule} copies the target type of a cast such as {@code CAST(j->'$.s' AS char)}
 * onto the path -- but BE consumes it as a <em>storage</em> column type. CHAR has no storage
 * counterpart for a flat-JSON / VARIANT subfield (the sub-column on disk is VARCHAR) and a
 * length-less CHAR/VARCHAR carries the wildcard sentinel -1, which BE reads back as an unsigned
 * width of 4294967295. Both must be normalized away as the value type is recorded.
 */
public class ColumnAccessPathValueTypeTest {
    private static final int MAX_LEN = TypeFactory.getOlapMaxVarcharLength();

    private static void assertStorageVarchar(Type actual) {
        Assertions.assertTrue(actual instanceof ScalarType, "expected a scalar type, got " + actual);
        ScalarType scalarType = (ScalarType) actual;
        Assertions.assertEquals(PrimitiveType.VARCHAR, scalarType.getPrimitiveType());
        Assertions.assertEquals(MAX_LEN, scalarType.getLength());
        Assertions.assertFalse(scalarType.isWildcardChar());
        Assertions.assertFalse(scalarType.isWildcardVarchar());
    }

    @Test
    public void constructorNormalizesCharAndUnsizedVarchar() {
        List<Type> unrepresentable = List.of(
                CharType.CHAR,                        // CAST(x AS char)      -> wildcard CHAR(-1)
                TypeFactory.createCharType(3),        // CAST(x AS char(3))
                TypeFactory.createCharType(MAX_LEN),  // a sized CHAR is still not a storage type here
                VarcharType.VARCHAR,                  // CAST(x AS varchar)   -> wildcard VARCHAR(-1)
                TypeFactory.createVarcharType(0));

        for (Type sent : unrepresentable) {
            ColumnAccessPath path = new ColumnAccessPath(TAccessPathType.FIELD, "s", sent);
            assertStorageVarchar(path.getValueType());
        }
    }

    @Test
    public void setValueTypeNormalizesToo() {
        // SubfieldAccessPathNormalizer reaches the leaf through the setter, not the constructor
        ColumnAccessPath path = new ColumnAccessPath(TAccessPathType.FIELD, "s", InvalidType.INVALID);
        path.setValueType(CharType.CHAR);
        assertStorageVarchar(path.getValueType());

        path.setValueType(TypeFactory.createCharType(3));
        assertStorageVarchar(path.getValueType());

        path.setValueType(VarcharType.VARCHAR);
        assertStorageVarchar(path.getValueType());
    }

    @Test
    public void linearPathFactoriesNormalizeEveryNode() {
        ColumnAccessPath root = ColumnAccessPath.createLinearPath(List.of("j", "a", "s"), CharType.CHAR);
        assertStorageVarchar(root.getValueType());
        assertStorageVarchar(root.getChildPath("a").getValueType());
        assertStorageVarchar(root.getChildPath("a").getChildPath("s").getValueType());

        ColumnAccessPath fromLinear = ColumnAccessPath.createFromLinearPath("j.s", CharType.CHAR);
        assertStorageVarchar(fromLinear.getChildPath("s").getValueType());
    }

    /**
     * The declared width is dropped rather than clamped. It has never been enforced on this path --
     * BE reads the subfield through the string reader and ignores it, so
     * {@code CAST(j->'$.s' AS char(3))} does not truncate today -- so keeping the 3 could only
     * introduce a truncation that does not exist. Widening to the max preserves behaviour exactly.
     * This is deliberately unlike {@code AnalyzerUtils#transformTableColumnType}, which keeps
     * {@code min(len, max)} because there the width really does bound a materialized column.
     */
    @Test
    public void aDeclaredWidthIsWidenedNotClamped() {
        ColumnAccessPath sized = new ColumnAccessPath(TAccessPathType.FIELD, "s", TypeFactory.createVarcharType(37));
        assertStorageVarchar(sized.getValueType());
    }

    @Test
    public void nonStringTypesAreUntouched() {
        Assertions.assertSame(JsonType.JSON,
                new ColumnAccessPath(TAccessPathType.FIELD, "s", JsonType.JSON).getValueType());
        Assertions.assertSame(VariantType.VARIANT,
                new ColumnAccessPath(TAccessPathType.FIELD, "s", VariantType.VARIANT).getValueType());
        Assertions.assertSame(IntegerType.BIGINT,
                new ColumnAccessPath(TAccessPathType.FIELD, "s", IntegerType.BIGINT).getValueType());
        Assertions.assertSame(InvalidType.INVALID,
                new ColumnAccessPath(TAccessPathType.FIELD, "s", InvalidType.INVALID).getValueType());
        Assertions.assertNull(new ColumnAccessPath(TAccessPathType.FIELD, "s", null).getValueType());
    }

    /**
     * {@code CharType.CHAR} and {@code VarcharType.VARCHAR} are process-wide singletons and
     * {@code ScalarType#setLength} is a public, unvalidated, in-place setter -- normalizing by
     * mutating the type that came in would rewrite the wildcard types for the whole FE.
     */
    @Test
    public void normalizationDoesNotMutateTheWildcardSingletons() {
        Assertions.assertEquals(-1, CharType.CHAR.getLength());
        Assertions.assertEquals(-1, VarcharType.VARCHAR.getLength());

        ColumnAccessPath fromCtor = new ColumnAccessPath(TAccessPathType.FIELD, "s", CharType.CHAR);
        ColumnAccessPath fromSetter = new ColumnAccessPath(TAccessPathType.FIELD, "s", InvalidType.INVALID);
        fromSetter.setValueType(VarcharType.VARCHAR);

        Assertions.assertEquals(-1, CharType.CHAR.getLength());
        Assertions.assertTrue(CharType.CHAR.isWildcardChar());
        Assertions.assertEquals(-1, VarcharType.VARCHAR.getLength());
        Assertions.assertTrue(VarcharType.VARCHAR.isWildcardVarchar());
        Assertions.assertNotSame(CharType.CHAR, fromCtor.getValueType());
        Assertions.assertNotSame(VarcharType.VARCHAR, fromSetter.getValueType());
    }

    /**
     * The only user-visible effect: a CHAR subfield path renders in {@code EXPLAIN VERBOSE} as the
     * VARCHAR it has always been read as on disk. The value the query returns does not change --
     * BE's cast factory already rewrites CHAR to VARCHAR before evaluating.
     */
    @Test
    public void explainRendersTheNormalizedType() {
        ColumnAccessPath root = new ColumnAccessPath(TAccessPathType.ROOT, "j", InvalidType.INVALID);
        root.addChildPath(new ColumnAccessPath(TAccessPathType.FIELD, "s", CharType.CHAR));

        Assertions.assertEquals("/j/s(varchar(" + MAX_LEN + "))", root.explain());
    }

    @Test
    public void thriftCarriesTheNormalizedType() {
        ColumnAccessPath path = new ColumnAccessPath(TAccessPathType.FIELD, "s", CharType.CHAR);

        TColumnAccessPath thrift = path.toThrift();

        Assertions.assertTrue(thrift.isSetType_desc());
        Assertions.assertEquals(TPrimitiveType.VARCHAR, thrift.type_desc.types.get(0).scalar_type.type);
        Assertions.assertEquals(MAX_LEN, thrift.type_desc.types.get(0).scalar_type.len);
    }
}
