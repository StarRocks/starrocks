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

import com.google.common.collect.Lists;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

/**
 * Comprehensive test suite for TypeSerializer.
 * Tests serialization of all Type variants to Thrift format.
 */
public class TypeSerializerTest {

    // ==================== Helper Methods ====================

    private void assertScalarTypeNode(TTypeNode node, TPrimitiveType expectedType) {
        Assertions.assertEquals(TTypeNodeType.SCALAR, node.getType());
        Assertions.assertNotNull(node.getScalar_type());
        Assertions.assertEquals(expectedType, node.getScalar_type().getType());
    }

    private void assertScalarTypeNodeWithLength(TTypeNode node, TPrimitiveType expectedType, int expectedLength) {
        assertScalarTypeNode(node, expectedType);
        Assertions.assertEquals(expectedLength, node.getScalar_type().getLen());
    }

    private void assertDecimalTypeNode(TTypeNode node, TPrimitiveType expectedType,
                                       int expectedPrecision, int expectedScale) {
        assertScalarTypeNode(node, expectedType);
        Assertions.assertEquals(expectedPrecision, node.getScalar_type().getPrecision());
        Assertions.assertEquals(expectedScale, node.getScalar_type().getScale());
    }

    // ==================== Basic Scalar Type Tests ====================

    @Test
    public void testSerializeBasicScalarTypes() {
        // Test all basic primitive types without special length/precision handling
        // Note: BINARY type is not supported by ScalarType.createType()
        PrimitiveType[] primitiveTypes = {
                PrimitiveType.BOOLEAN,
                PrimitiveType.TINYINT,
                PrimitiveType.SMALLINT,
                PrimitiveType.INT,
                PrimitiveType.BIGINT,
                PrimitiveType.LARGEINT,
                PrimitiveType.FLOAT,
                PrimitiveType.DOUBLE,
                PrimitiveType.DATE,
                PrimitiveType.DATETIME,
                PrimitiveType.TIME,
                PrimitiveType.JSON,
                PrimitiveType.BITMAP,
                PrimitiveType.PERCENTILE
        };

        for (PrimitiveType primitiveType : primitiveTypes) {
            TTypeDesc container = new TTypeDesc();
            container.types = new ArrayList<>();

            Type type = ScalarType.createType(primitiveType);
            TypeSerializer.toThrift(type, container);

            Assertions.assertEquals(1, container.types.size());
            assertScalarTypeNode(container.types.get(0), primitiveType.toThrift());
        }
    }

    @Test
    public void testSerializeNullType() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        Type nullType = ScalarType.createType(PrimitiveType.NULL_TYPE);
        TypeSerializer.toThrift(nullType, container);

        Assertions.assertEquals(1, container.types.size());
        assertScalarTypeNode(container.types.get(0), TPrimitiveType.NULL_TYPE);
    }

    // ==================== String and Character Type Tests ====================

    @Test
    public void testSerializeCharType() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType charType = ScalarType.createCharType(10);
        TypeSerializer.toThrift(charType, container);

        Assertions.assertEquals(1, container.types.size());
        assertScalarTypeNodeWithLength(container.types.get(0), TPrimitiveType.CHAR, 10);
    }

    @Test
    public void testSerializeCharTypeMaxLength() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType charType = ScalarType.createCharType(ScalarType.MAX_CHAR_LENGTH);
        TypeSerializer.toThrift(charType, container);

        Assertions.assertEquals(1, container.types.size());
        assertScalarTypeNodeWithLength(container.types.get(0), TPrimitiveType.CHAR,
                ScalarType.MAX_CHAR_LENGTH);
    }

    @Test
    public void testSerializeVarcharType() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType varcharType = ScalarType.createVarcharType(255);
        TypeSerializer.toThrift(varcharType, container);

        Assertions.assertEquals(1, container.types.size());
        assertScalarTypeNodeWithLength(container.types.get(0), TPrimitiveType.VARCHAR, 255);
    }

    @Test
    public void testSerializeVarcharTypeDefaultLength() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType varcharType = ScalarType.createVarcharType(ScalarType.DEFAULT_STRING_LENGTH);
        TypeSerializer.toThrift(varcharType, container);

        Assertions.assertEquals(1, container.types.size());
        assertScalarTypeNodeWithLength(container.types.get(0), TPrimitiveType.VARCHAR,
                ScalarType.DEFAULT_STRING_LENGTH);
    }

    @Test
    public void testSerializeVarcharTypeLargeLength() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // Use a large length value
        ScalarType varcharType = ScalarType.createVarcharType(1048576);
        TypeSerializer.toThrift(varcharType, container);

        Assertions.assertEquals(1, container.types.size());
        assertScalarTypeNodeWithLength(container.types.get(0), TPrimitiveType.VARCHAR, 1048576);
    }

    @Test
    public void testSerializeVarbinaryType() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType varbinaryType = ScalarType.createVarbinary(1024);
        TypeSerializer.toThrift(varbinaryType, container);

        Assertions.assertEquals(1, container.types.size());
        assertScalarTypeNodeWithLength(container.types.get(0), TPrimitiveType.VARBINARY, 1024);
    }

    @Test
    public void testSerializeVarbinaryTypeLargeLength() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // Use a large length value
        ScalarType varbinaryType = ScalarType.createVarbinary(1048576);
        TypeSerializer.toThrift(varbinaryType, container);

        Assertions.assertEquals(1, container.types.size());
        assertScalarTypeNodeWithLength(container.types.get(0), TPrimitiveType.VARBINARY, 1048576);
    }

    // ==================== HLL Type Tests ====================

    @Test
    public void testSerializeHllType() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType hllType = ScalarType.createHllType();
        TypeSerializer.toThrift(hllType, container);

        Assertions.assertEquals(1, container.types.size());
        assertScalarTypeNodeWithLength(container.types.get(0), TPrimitiveType.HLL,
                ScalarType.MAX_HLL_LENGTH);
    }

    // ==================== Decimal Type Tests ====================

    @Test
    public void testSerializeDecimalV2Type() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType decimalType = ScalarType.createDecimalV2Type(10, 4);
        TypeSerializer.toThrift(decimalType, container);

        Assertions.assertEquals(1, container.types.size());
        assertDecimalTypeNode(container.types.get(0), TPrimitiveType.DECIMALV2, 10, 4);
    }

    @Test
    public void testSerializeDecimalV2MaxPrecision() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // DecimalV2 max precision is 27, max scale is 9
        ScalarType decimalType = ScalarType.createDecimalV2Type(27, 9);
        TypeSerializer.toThrift(decimalType, container);

        Assertions.assertEquals(1, container.types.size());
        assertDecimalTypeNode(container.types.get(0), TPrimitiveType.DECIMALV2, 27, 9);
    }

    @Test
    public void testSerializeDecimal32Type() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType decimal32Type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2);
        TypeSerializer.toThrift(decimal32Type, container);

        Assertions.assertEquals(1, container.types.size());
        assertDecimalTypeNode(container.types.get(0), TPrimitiveType.DECIMAL32, 9, 2);
    }

    @Test
    public void testSerializeDecimal64Type() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType decimal64Type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6);
        TypeSerializer.toThrift(decimal64Type, container);

        Assertions.assertEquals(1, container.types.size());
        assertDecimalTypeNode(container.types.get(0), TPrimitiveType.DECIMAL64, 18, 6);
    }

    @Test
    public void testSerializeDecimal128Type() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType decimal128Type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 10);
        TypeSerializer.toThrift(decimal128Type, container);

        Assertions.assertEquals(1, container.types.size());
        assertDecimalTypeNode(container.types.get(0), TPrimitiveType.DECIMAL128, 38, 10);
    }

    @Test
    public void testSerializeDecimal256Type() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType decimal256Type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL256, 76, 20);
        TypeSerializer.toThrift(decimal256Type, container);

        Assertions.assertEquals(1, container.types.size());
        assertDecimalTypeNode(container.types.get(0), TPrimitiveType.DECIMAL256, 76, 20);
    }

    @Test
    public void testSerializeDecimalTypesWithZeroScale() {
        // Test all decimal types with zero scale
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ScalarType decimalType = ScalarType.createDecimalV2Type(10, 0);
        TypeSerializer.toThrift(decimalType, container);

        Assertions.assertEquals(1, container.types.size());
        assertDecimalTypeNode(container.types.get(0), TPrimitiveType.DECIMALV2, 10, 0);
    }

    // ==================== Array Type Tests ====================

    @Test
    public void testSerializeArrayOfInt() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ArrayType arrayType = new ArrayType(Type.INT);
        TypeSerializer.toThrift(arrayType, container);

        Assertions.assertEquals(2, container.types.size());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(0).getType());
        assertScalarTypeNode(container.types.get(1), TPrimitiveType.INT);
    }

    @Test
    public void testSerializeArrayOfVarchar() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ArrayType arrayType = new ArrayType(ScalarType.createVarcharType(100));
        TypeSerializer.toThrift(arrayType, container);

        Assertions.assertEquals(2, container.types.size());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(0).getType());
        assertScalarTypeNodeWithLength(container.types.get(1), TPrimitiveType.VARCHAR, 100);
    }

    @Test
    public void testSerializeArrayOfDecimal() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        ArrayType arrayType = new ArrayType(ScalarType.createDecimalV2Type(10, 2));
        TypeSerializer.toThrift(arrayType, container);

        Assertions.assertEquals(2, container.types.size());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(0).getType());
        assertDecimalTypeNode(container.types.get(1), TPrimitiveType.DECIMALV2, 10, 2);
    }

    @Test
    public void testSerializeNestedArray() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // ARRAY<ARRAY<INT>>
        ArrayType innerArray = new ArrayType(Type.INT);
        ArrayType outerArray = new ArrayType(innerArray);
        TypeSerializer.toThrift(outerArray, container);

        Assertions.assertEquals(3, container.types.size());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(0).getType());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(1).getType());
        assertScalarTypeNode(container.types.get(2), TPrimitiveType.INT);
    }

    // ==================== Map Type Tests ====================

    @Test
    public void testSerializeMapIntToVarchar() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        MapType mapType = new MapType(Type.INT, ScalarType.createVarcharType(255));
        TypeSerializer.toThrift(mapType, container);

        Assertions.assertEquals(3, container.types.size());
        Assertions.assertEquals(TTypeNodeType.MAP, container.types.get(0).getType());
        assertScalarTypeNode(container.types.get(1), TPrimitiveType.INT);
        assertScalarTypeNodeWithLength(container.types.get(2), TPrimitiveType.VARCHAR, 255);
    }

    @Test
    public void testSerializeMapVarcharToDouble() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        MapType mapType = new MapType(ScalarType.createVarcharType(50), Type.DOUBLE);
        TypeSerializer.toThrift(mapType, container);

        Assertions.assertEquals(3, container.types.size());
        Assertions.assertEquals(TTypeNodeType.MAP, container.types.get(0).getType());
        assertScalarTypeNodeWithLength(container.types.get(1), TPrimitiveType.VARCHAR, 50);
        assertScalarTypeNode(container.types.get(2), TPrimitiveType.DOUBLE);
    }

    @Test
    public void testSerializeMapDecimalToDecimal() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        MapType mapType = new MapType(
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4),
                ScalarType.createDecimalV2Type(10, 2)
        );
        TypeSerializer.toThrift(mapType, container);

        Assertions.assertEquals(3, container.types.size());
        Assertions.assertEquals(TTypeNodeType.MAP, container.types.get(0).getType());
        assertDecimalTypeNode(container.types.get(1), TPrimitiveType.DECIMAL64, 18, 4);
        assertDecimalTypeNode(container.types.get(2), TPrimitiveType.DECIMALV2, 10, 2);
    }

    @Test
    public void testSerializeNestedMapInArray() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // ARRAY<MAP<INT, VARCHAR>>
        MapType mapType = new MapType(Type.INT, ScalarType.createVarcharType(100));
        ArrayType arrayType = new ArrayType(mapType);
        TypeSerializer.toThrift(arrayType, container);

        Assertions.assertEquals(4, container.types.size());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(0).getType());
        Assertions.assertEquals(TTypeNodeType.MAP, container.types.get(1).getType());
        assertScalarTypeNode(container.types.get(2), TPrimitiveType.INT);
        assertScalarTypeNodeWithLength(container.types.get(3), TPrimitiveType.VARCHAR, 100);
    }

    // ==================== Struct Type Tests ====================

    @Test
    public void testSerializeSimpleStruct() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        StructType structType = new StructType(Lists.newArrayList(
                new StructField("field1", Type.INT),
                new StructField("field2", ScalarType.createVarcharType(50))
        ));
        TypeSerializer.toThrift(structType, container);

        Assertions.assertEquals(3, container.types.size());

        // Verify struct node
        TTypeNode structNode = container.types.get(0);
        Assertions.assertEquals(TTypeNodeType.STRUCT, structNode.getType());
        Assertions.assertNotNull(structNode.getStruct_fields());
        Assertions.assertEquals(2, structNode.getStruct_fields().size());
        Assertions.assertEquals("field1", structNode.getStruct_fields().get(0).getName());
        Assertions.assertEquals("field2", structNode.getStruct_fields().get(1).getName());

        // Verify field types
        assertScalarTypeNode(container.types.get(1), TPrimitiveType.INT);
        assertScalarTypeNodeWithLength(container.types.get(2), TPrimitiveType.VARCHAR, 50);
    }

    @Test
    public void testSerializeStructWithComments() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        StructType structType = new StructType(Lists.newArrayList(
                new StructField("id", Type.BIGINT, "User ID"),
                new StructField("name", ScalarType.createVarcharType(100), "User name"),
                new StructField("score", Type.DOUBLE)
        ));
        TypeSerializer.toThrift(structType, container);

        Assertions.assertEquals(4, container.types.size());

        TTypeNode structNode = container.types.get(0);
        Assertions.assertEquals(TTypeNodeType.STRUCT, structNode.getType());
        Assertions.assertEquals(3, structNode.getStruct_fields().size());

        // Verify field names and comments
        Assertions.assertEquals("id", structNode.getStruct_fields().get(0).getName());
        Assertions.assertEquals("User ID", structNode.getStruct_fields().get(0).getComment());

        Assertions.assertEquals("name", structNode.getStruct_fields().get(1).getName());
        Assertions.assertEquals("User name", structNode.getStruct_fields().get(1).getComment());

        Assertions.assertEquals("score", structNode.getStruct_fields().get(2).getName());
        Assertions.assertNull(structNode.getStruct_fields().get(2).getComment());
    }

    @Test
    public void testSerializeStructWithDecimalFields() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        StructType structType = new StructType(Lists.newArrayList(
                new StructField("price", ScalarType.createDecimalV2Type(10, 2)),
                new StructField("quantity", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4))
        ));
        TypeSerializer.toThrift(structType, container);

        Assertions.assertEquals(3, container.types.size());

        TTypeNode structNode = container.types.get(0);
        Assertions.assertEquals(TTypeNodeType.STRUCT, structNode.getType());
        Assertions.assertEquals(2, structNode.getStruct_fields().size());

        assertDecimalTypeNode(container.types.get(1), TPrimitiveType.DECIMALV2, 10, 2);
        assertDecimalTypeNode(container.types.get(2), TPrimitiveType.DECIMAL64, 18, 4);
    }

    @Test
    public void testSerializeNamedStruct() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        StructType structType = new StructType(Lists.newArrayList(
                new StructField("x", Type.INT),
                new StructField("y", Type.INT)
        ), true); // Named struct
        TypeSerializer.toThrift(structType, container);

        Assertions.assertEquals(3, container.types.size());

        TTypeNode structNode = container.types.get(0);
        Assertions.assertEquals(TTypeNodeType.STRUCT, structNode.getType());
        Assertions.assertTrue(structNode.isIs_named());
    }

    @Test
    public void testSerializeUnnamedStruct() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        StructType structType = new StructType(Lists.newArrayList(
                new StructField("x", Type.INT),
                new StructField("y", Type.INT)
        ), false); // Unnamed struct
        TypeSerializer.toThrift(structType, container);

        Assertions.assertEquals(3, container.types.size());

        TTypeNode structNode = container.types.get(0);
        Assertions.assertEquals(TTypeNodeType.STRUCT, structNode.getType());
        Assertions.assertFalse(structNode.isIs_named());
    }

    @Test
    public void testSerializeStructFieldWithFieldId() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // Use constructor with fieldId parameter
        StructField field1 = new StructField("id", 1, Type.INT, null);
        StructField field2 = new StructField("name", 2, ScalarType.createVarcharType(100), null);

        StructType structType = new StructType(Lists.newArrayList(field1, field2));
        TypeSerializer.toThrift(structType, container);

        Assertions.assertEquals(3, container.types.size());

        TTypeNode structNode = container.types.get(0);
        Assertions.assertEquals(1, structNode.getStruct_fields().get(0).getId());
        Assertions.assertEquals(2, structNode.getStruct_fields().get(1).getId());
    }

    @Test
    public void testSerializeStructFieldWithPhysicalName() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // Use constructor with fieldPhysicalName parameter
        StructField field = new StructField("logical_name", -1, "physical_name", Type.INT, null);

        StructType structType = new StructType(Lists.newArrayList(field));
        TypeSerializer.toThrift(structType, container);

        Assertions.assertEquals(2, container.types.size());

        TTypeNode structNode = container.types.get(0);
        Assertions.assertEquals("logical_name", structNode.getStruct_fields().get(0).getName());
        Assertions.assertEquals("physical_name", structNode.getStruct_fields().get(0).getPhysical_name());
    }

    @Test
    public void testSerializeNestedStruct() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // Inner struct: STRUCT<x:INT, y:INT>
        StructType innerStruct = new StructType(Lists.newArrayList(
                new StructField("x", Type.INT),
                new StructField("y", Type.INT)
        ));

        // Outer struct: STRUCT<id:BIGINT, point:STRUCT<x:INT, y:INT>>
        StructType outerStruct = new StructType(Lists.newArrayList(
                new StructField("id", Type.BIGINT),
                new StructField("point", innerStruct)
        ));

        TypeSerializer.toThrift(outerStruct, container);

        Assertions.assertEquals(5, container.types.size());

        // Verify outer struct
        Assertions.assertEquals(TTypeNodeType.STRUCT, container.types.get(0).getType());
        Assertions.assertEquals(2, container.types.get(0).getStruct_fields().size());

        // Verify first field (BIGINT)
        assertScalarTypeNode(container.types.get(1), TPrimitiveType.BIGINT);

        // Verify second field (inner struct)
        Assertions.assertEquals(TTypeNodeType.STRUCT, container.types.get(2).getType());
        Assertions.assertEquals(2, container.types.get(2).getStruct_fields().size());

        // Verify inner struct fields
        assertScalarTypeNode(container.types.get(3), TPrimitiveType.INT);
        assertScalarTypeNode(container.types.get(4), TPrimitiveType.INT);
    }

    // ==================== Complex Nested Type Tests ====================

    @Test
    public void testSerializeComplexNestedType1() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // ARRAY<MAP<VARCHAR, STRUCT<id:INT, value:DOUBLE>>>
        StructType structType = new StructType(Lists.newArrayList(
                new StructField("id", Type.INT),
                new StructField("value", Type.DOUBLE)
        ));

        MapType mapType = new MapType(ScalarType.createVarcharType(50), structType);
        ArrayType arrayType = new ArrayType(mapType);

        TypeSerializer.toThrift(arrayType, container);

        // Verify structure: ARRAY -> MAP -> VARCHAR, STRUCT -> INT, DOUBLE
        Assertions.assertEquals(6, container.types.size());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(0).getType());
        Assertions.assertEquals(TTypeNodeType.MAP, container.types.get(1).getType());
        assertScalarTypeNodeWithLength(container.types.get(2), TPrimitiveType.VARCHAR, 50);
        Assertions.assertEquals(TTypeNodeType.STRUCT, container.types.get(3).getType());
        assertScalarTypeNode(container.types.get(4), TPrimitiveType.INT);
        assertScalarTypeNode(container.types.get(5), TPrimitiveType.DOUBLE);
    }

    @Test
    public void testSerializeComplexNestedType2() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // STRUCT<users:ARRAY<STRUCT<name:VARCHAR, age:INT>>, count:BIGINT>
        StructType userStruct = new StructType(Lists.newArrayList(
                new StructField("name", ScalarType.createVarcharType(100)),
                new StructField("age", Type.INT)
        ));

        ArrayType usersArray = new ArrayType(userStruct);

        StructType outerStruct = new StructType(Lists.newArrayList(
                new StructField("users", usersArray),
                new StructField("count", Type.BIGINT)
        ));

        TypeSerializer.toThrift(outerStruct, container);

        // Verify structure
        Assertions.assertEquals(6, container.types.size());
        Assertions.assertEquals(TTypeNodeType.STRUCT, container.types.get(0).getType());
        Assertions.assertEquals(2, container.types.get(0).getStruct_fields().size());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(1).getType());
        Assertions.assertEquals(TTypeNodeType.STRUCT, container.types.get(2).getType());
        assertScalarTypeNodeWithLength(container.types.get(3), TPrimitiveType.VARCHAR, 100);
        assertScalarTypeNode(container.types.get(4), TPrimitiveType.INT);
        assertScalarTypeNode(container.types.get(5), TPrimitiveType.BIGINT);
    }

    @Test
    public void testSerializeMapOfArrays() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // MAP<INT, ARRAY<VARCHAR>>
        ArrayType arrayType = new ArrayType(ScalarType.createVarcharType(200));
        MapType mapType = new MapType(Type.INT, arrayType);

        TypeSerializer.toThrift(mapType, container);

        Assertions.assertEquals(4, container.types.size());
        Assertions.assertEquals(TTypeNodeType.MAP, container.types.get(0).getType());
        assertScalarTypeNode(container.types.get(1), TPrimitiveType.INT);
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(2).getType());
        assertScalarTypeNodeWithLength(container.types.get(3), TPrimitiveType.VARCHAR, 200);
    }

    @Test
    public void testSerializeArrayOfMaps() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // ARRAY<MAP<VARCHAR, BIGINT>>
        MapType mapType = new MapType(ScalarType.createVarcharType(50), Type.BIGINT);
        ArrayType arrayType = new ArrayType(mapType);

        TypeSerializer.toThrift(arrayType, container);

        Assertions.assertEquals(4, container.types.size());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(0).getType());
        Assertions.assertEquals(TTypeNodeType.MAP, container.types.get(1).getType());
        assertScalarTypeNodeWithLength(container.types.get(2), TPrimitiveType.VARCHAR, 50);
        assertScalarTypeNode(container.types.get(3), TPrimitiveType.BIGINT);
    }

    @Test
    public void testSerializeDeeplyNestedArrays() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // ARRAY<ARRAY<ARRAY<INT>>>
        ArrayType level1 = new ArrayType(Type.INT);
        ArrayType level2 = new ArrayType(level1);
        ArrayType level3 = new ArrayType(level2);

        TypeSerializer.toThrift(level3, container);

        Assertions.assertEquals(4, container.types.size());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(0).getType());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(1).getType());
        Assertions.assertEquals(TTypeNodeType.ARRAY, container.types.get(2).getType());
        assertScalarTypeNode(container.types.get(3), TPrimitiveType.INT);
    }

    // ==================== Error Condition Tests ====================

    @Test
    public void testSerializePseudoType() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        PseudoType pseudoType = Type.ANY_ELEMENT;

        // Should throw IllegalArgumentException
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            TypeSerializer.toThrift(pseudoType, container);
        });
    }

    @Test
    public void testSerializeNullTypeArgument() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // Should throw NullPointerException or similar
        Assertions.assertThrows(Exception.class, () -> {
            TypeSerializer.toThrift(null, container);
        });
    }

    // ==================== Round-trip Tests ====================

    @Test
    public void testSerializeDeserializeRoundTrip() {
        Type[] testTypes = {
                Type.BOOLEAN,
                Type.TINYINT,
                Type.SMALLINT,
                Type.INT,
                Type.BIGINT,
                Type.LARGEINT,
                Type.FLOAT,
                Type.DOUBLE,
                Type.DATE,
                Type.DATETIME,
                ScalarType.createCharType(10),
                ScalarType.createVarcharType(255),
                ScalarType.createVarbinary(1024),
                ScalarType.createHllType(),
                ScalarType.createDecimalV2Type(10, 4),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 10),
                new ArrayType(Type.INT),
                new ArrayType(ScalarType.createVarcharType(100)),
                new MapType(Type.INT, ScalarType.createVarcharType(200)),
                new StructType(Lists.newArrayList(
                        new StructField("f1", Type.INT),
                        new StructField("f2", ScalarType.createVarcharType(50), "comment")
                ))
        };

        for (Type originalType : testTypes) {
            // Serialize
            TTypeDesc container = new TTypeDesc();
            container.types = new ArrayList<>();
            TypeSerializer.toThrift(originalType, container);

            // Deserialize
            Type deserializedType = TypeDeserializer.fromThrift(container);

            // Verify
            Assertions.assertEquals(originalType.toSql(), deserializedType.toSql(),
                    "Round-trip failed for type: " + originalType.toSql());
        }
    }

    @Test
    public void testSerializeDeserializeComplexNestedTypes() {
        // Test complex nested type round-trip
        StructType innerStruct = new StructType(Lists.newArrayList(
                new StructField("x", Type.INT),
                new StructField("y", Type.DOUBLE)
        ));

        MapType mapType = new MapType(ScalarType.createVarcharType(50), innerStruct);
        ArrayType arrayType = new ArrayType(mapType);

        StructType outerStruct = new StructType(Lists.newArrayList(
                new StructField("data", arrayType),
                new StructField("count", Type.BIGINT)
        ));

        // Serialize
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();
        TypeSerializer.toThrift(outerStruct, container);

        // Deserialize
        Type deserializedType = TypeDeserializer.fromThrift(container);

        // Verify structure is preserved
        Assertions.assertTrue(deserializedType.isStructType());
        StructType deserializedStruct = (StructType) deserializedType;
        Assertions.assertEquals(2, deserializedStruct.getFields().size());
    }

    // ==================== Container State Tests ====================

    @Test
    public void testSerializeToNonEmptyContainer() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // Add some existing nodes
        TTypeNode existingNode = new TTypeNode();
        existingNode.setType(TTypeNodeType.SCALAR);
        container.types.add(existingNode);

        // Serialize a new type - should append
        TypeSerializer.toThrift(Type.INT, container);

        Assertions.assertEquals(2, container.types.size());
        Assertions.assertEquals(TTypeNodeType.SCALAR, container.types.get(0).getType());
        assertScalarTypeNode(container.types.get(1), TPrimitiveType.INT);
    }

    @Test
    public void testSerializeMultipleTypes() {
        TTypeDesc container = new TTypeDesc();
        container.types = new ArrayList<>();

        // Serialize multiple types to the same container
        TypeSerializer.toThrift(Type.INT, container);
        TypeSerializer.toThrift(Type.VARCHAR, container);
        TypeSerializer.toThrift(Type.DOUBLE, container);

        Assertions.assertEquals(3, container.types.size());
        assertScalarTypeNode(container.types.get(0), TPrimitiveType.INT);
        assertScalarTypeNode(container.types.get(1), TPrimitiveType.VARCHAR);
        assertScalarTypeNode(container.types.get(2), TPrimitiveType.DOUBLE);
    }
}
