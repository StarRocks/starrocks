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

package com.starrocks.connector.adbc;

import com.starrocks.catalog.Column;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class FlightSQLSchemaResolverTest {

    private static FlightSQLSchemaResolver resolver;

    @BeforeAll
    public static void setUp() {
        resolver = new FlightSQLSchemaResolver();
    }

    private static Field makeField(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), Collections.emptyList());
    }

    // --- Signed integer types ---

    @Test
    public void signedInt8_mapsTinyint() {
        Field field = makeField("col", new ArrowType.Int(8, true));
        assertEquals(IntegerType.TINYINT, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void signedInt16_mapsSmallint() {
        Field field = makeField("col", new ArrowType.Int(16, true));
        assertEquals(IntegerType.SMALLINT, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void signedInt32_mapsInt() {
        Field field = makeField("col", new ArrowType.Int(32, true));
        assertEquals(IntegerType.INT, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void signedInt64_mapsBigint() {
        Field field = makeField("col", new ArrowType.Int(64, true));
        assertEquals(IntegerType.BIGINT, resolver.convertArrowFieldToSRType(field));
    }

    // --- Unsigned integer types (overflow promotion) ---

    @Test
    public void unsignedInt8_mapsSmallint() {
        Field field = makeField("col", new ArrowType.Int(8, false));
        assertEquals(IntegerType.SMALLINT, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void unsignedInt16_mapsInt() {
        Field field = makeField("col", new ArrowType.Int(16, false));
        assertEquals(IntegerType.INT, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void unsignedInt32_mapsBigint() {
        Field field = makeField("col", new ArrowType.Int(32, false));
        assertEquals(IntegerType.BIGINT, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void unsignedInt64_mapsLargeint() {
        Field field = makeField("col", new ArrowType.Int(64, false));
        assertEquals(IntegerType.LARGEINT, resolver.convertArrowFieldToSRType(field));
    }

    // --- Floating point types ---

    @Test
    public void floatSingle_mapsFloat() {
        Field field = makeField("col", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        assertEquals(FloatType.FLOAT, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void floatDouble_mapsDouble() {
        Field field = makeField("col", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        assertEquals(FloatType.DOUBLE, resolver.convertArrowFieldToSRType(field));
    }

    // --- Decimal type ---

    @Test
    public void decimal128_mapsDecimal() {
        Field field = makeField("col", new ArrowType.Decimal(10, 2, 128));
        Type expected = new DecimalType(PrimitiveType.DECIMAL128, 10, 2);
        assertEquals(expected, resolver.convertArrowFieldToSRType(field));
    }

    // --- String types ---

    @Test
    public void utf8_mapsVarchar() {
        Field field = makeField("col", ArrowType.Utf8.INSTANCE);
        Type expected = new VarcharType(StringType.DEFAULT_STRING_LENGTH);
        assertEquals(expected, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void largeUtf8_mapsVarchar() {
        Field field = makeField("col", ArrowType.LargeUtf8.INSTANCE);
        Type expected = new VarcharType(StringType.DEFAULT_STRING_LENGTH);
        assertEquals(expected, resolver.convertArrowFieldToSRType(field));
    }

    // --- Binary types ---

    @Test
    public void binary_mapsVarbinary() {
        Field field = makeField("col", ArrowType.Binary.INSTANCE);
        assertEquals(VarbinaryType.VARBINARY, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void largeBinary_mapsVarbinary() {
        Field field = makeField("col", ArrowType.LargeBinary.INSTANCE);
        assertEquals(VarbinaryType.VARBINARY, resolver.convertArrowFieldToSRType(field));
    }

    // --- Boolean type ---

    @Test
    public void bool_mapsBoolean() {
        Field field = makeField("col", ArrowType.Bool.INSTANCE);
        assertEquals(BooleanType.BOOLEAN, resolver.convertArrowFieldToSRType(field));
    }

    // --- Date type ---

    @Test
    public void date_mapsDate() {
        Field field = makeField("col", new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY));
        assertEquals(DateType.DATE, resolver.convertArrowFieldToSRType(field));
    }

    // --- Timestamp types (all variants map to DATETIME) ---

    @Test
    public void timestampMicrosecondNoTz_mapsDatetime() {
        Field field = makeField("col", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));
        assertEquals(DateType.DATETIME, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void timestampMillisecondUtc_mapsDatetime() {
        Field field = makeField("col", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
        assertEquals(DateType.DATETIME, resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void timestampSecond_mapsDatetime() {
        Field field = makeField("col", new ArrowType.Timestamp(TimeUnit.SECOND, null));
        assertEquals(DateType.DATETIME, resolver.convertArrowFieldToSRType(field));
    }

    // --- Null type ---

    @Test
    public void null_mapsNullType() {
        Field field = makeField("col", ArrowType.Null.INSTANCE);
        assertEquals(NullType.NULL, resolver.convertArrowFieldToSRType(field));
    }

    // --- Unsupported types (return null) ---

    @Test
    public void list_returnsNull() {
        Field field = new Field("col", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(makeField("item", new ArrowType.Int(32, true))));
        assertNull(resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void struct_returnsNull() {
        Field field = new Field("col", FieldType.nullable(ArrowType.Struct.INSTANCE),
                Collections.singletonList(makeField("sub", new ArrowType.Int(32, true))));
        assertNull(resolver.convertArrowFieldToSRType(field));
    }

    @Test
    public void map_returnsNull() {
        // Arrow Map type requires a struct child with "entries" containing key/value
        Field keyField = makeField("key", ArrowType.Utf8.INSTANCE);
        Field valueField = makeField("value", new ArrowType.Int(32, true));
        Field entriesField = new Field("entries", FieldType.nullable(ArrowType.Struct.INSTANCE),
                Arrays.asList(keyField, valueField));
        Field field = new Field("col", FieldType.nullable(new ArrowType.Map(false)),
                Collections.singletonList(entriesField));
        assertNull(resolver.convertArrowFieldToSRType(field));
    }

    // --- Integration: convertToSRTable excludes unsupported columns ---

    @Test
    public void convertToSRTable_excludesUnsupportedColumns() {
        Field intField = makeField("int_col", new ArrowType.Int(32, true));
        Field listField = new Field("list_col", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(makeField("item", new ArrowType.Int(32, true))));
        Field utf8Field = makeField("str_col", ArrowType.Utf8.INSTANCE);

        Schema schema = new Schema(Arrays.asList(intField, listField, utf8Field));
        List<Column> columns = resolver.convertToSRTable(schema);

        assertEquals(2, columns.size());
        assertEquals("int_col", columns.get(0).getName());
        assertEquals("str_col", columns.get(1).getName());
    }
}
