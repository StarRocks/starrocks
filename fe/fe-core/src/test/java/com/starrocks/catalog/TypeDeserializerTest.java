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
import com.starrocks.proto.PScalarType;
import com.starrocks.proto.PStructField;
import com.starrocks.proto.PTypeDesc;
import com.starrocks.proto.PTypeNode;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScalarType;
import com.starrocks.thrift.TStructField;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TypeDeserializerTest {

    // Helper methods to build test data structures

    private TTypeDesc buildThriftScalarType(TPrimitiveType primitiveType) {
        TTypeDesc typeDesc = new TTypeDesc();
        typeDesc.types = new ArrayList<>();

        TTypeNode node = new TTypeNode();
        node.setType(TTypeNodeType.SCALAR);

        TScalarType scalarType = new TScalarType();
        scalarType.setType(primitiveType);

        // Set default lengths for types that require them
        if (primitiveType == TPrimitiveType.VARCHAR) {
            scalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
        } else if (primitiveType == TPrimitiveType.CHAR) {
            scalarType.setLen(1);
        } else if (primitiveType == TPrimitiveType.VARBINARY) {
            scalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
        }

        node.setScalar_type(scalarType);

        typeDesc.types.add(node);
        return typeDesc;
    }

    private TTypeDesc buildThriftScalarTypeWithLength(TPrimitiveType primitiveType, int length) {
        TTypeDesc typeDesc = buildThriftScalarType(primitiveType);
        typeDesc.types.get(0).getScalar_type().setLen(length);
        return typeDesc;
    }

    private TTypeDesc buildThriftDecimalType(TPrimitiveType primitiveType, int precision, int scale) {
        TTypeDesc typeDesc = buildThriftScalarType(primitiveType);
        TScalarType scalarType = typeDesc.types.get(0).getScalar_type();
        scalarType.setPrecision(precision);
        scalarType.setScale(scale);
        return typeDesc;
    }

    private TTypeDesc buildThriftArrayType(TPrimitiveType elementType) {
        TTypeDesc typeDesc = new TTypeDesc();
        typeDesc.types = new ArrayList<>();

        // Array node
        TTypeNode arrayNode = new TTypeNode();
        arrayNode.setType(TTypeNodeType.ARRAY);
        typeDesc.types.add(arrayNode);

        // Element type node
        TTypeNode elementNode = new TTypeNode();
        elementNode.setType(TTypeNodeType.SCALAR);
        TScalarType scalarType = new TScalarType();
        scalarType.setType(elementType);

        // Set default lengths for types that require them
        if (elementType == TPrimitiveType.VARCHAR) {
            scalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
        } else if (elementType == TPrimitiveType.CHAR) {
            scalarType.setLen(1);
        } else if (elementType == TPrimitiveType.VARBINARY) {
            scalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
        }

        elementNode.setScalar_type(scalarType);
        typeDesc.types.add(elementNode);

        return typeDesc;
    }

    private TTypeDesc buildThriftMapType(TPrimitiveType keyType, TPrimitiveType valueType) {
        TTypeDesc typeDesc = new TTypeDesc();
        typeDesc.types = new ArrayList<>();

        // Map node
        TTypeNode mapNode = new TTypeNode();
        mapNode.setType(TTypeNodeType.MAP);
        typeDesc.types.add(mapNode);

        // Key type node
        TTypeNode keyNode = new TTypeNode();
        keyNode.setType(TTypeNodeType.SCALAR);
        TScalarType keyScalarType = new TScalarType();
        keyScalarType.setType(keyType);

        // Set default lengths for key type
        if (keyType == TPrimitiveType.VARCHAR) {
            keyScalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
        } else if (keyType == TPrimitiveType.CHAR) {
            keyScalarType.setLen(1);
        } else if (keyType == TPrimitiveType.VARBINARY) {
            keyScalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
        }

        keyNode.setScalar_type(keyScalarType);
        typeDesc.types.add(keyNode);

        // Value type node
        TTypeNode valueNode = new TTypeNode();
        valueNode.setType(TTypeNodeType.SCALAR);
        TScalarType valueScalarType = new TScalarType();
        valueScalarType.setType(valueType);

        // Set default lengths for value type
        if (valueType == TPrimitiveType.VARCHAR) {
            valueScalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
        } else if (valueType == TPrimitiveType.CHAR) {
            valueScalarType.setLen(1);
        } else if (valueType == TPrimitiveType.VARBINARY) {
            valueScalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
        }

        valueNode.setScalar_type(valueScalarType);
        typeDesc.types.add(valueNode);

        return typeDesc;
    }

    private TTypeDesc buildThriftStructType(List<String> fieldNames, List<TPrimitiveType> fieldTypes) {
        TTypeDesc typeDesc = new TTypeDesc();
        typeDesc.types = new ArrayList<>();

        // Struct node
        TTypeNode structNode = new TTypeNode();
        structNode.setType(TTypeNodeType.STRUCT);
        structNode.setStruct_fields(new ArrayList<>());

        for (int i = 0; i < fieldNames.size(); i++) {
            TStructField field = new TStructField();
            field.setName(fieldNames.get(i));
            if (i % 2 == 0) { // Add comment to some fields for testing
                field.setComment("Test comment for " + fieldNames.get(i));
            }
            structNode.getStruct_fields().add(field);
        }
        typeDesc.types.add(structNode);

        // Field type nodes
        for (TPrimitiveType fieldType : fieldTypes) {
            TTypeNode fieldNode = new TTypeNode();
            fieldNode.setType(TTypeNodeType.SCALAR);
            TScalarType scalarType = new TScalarType();
            scalarType.setType(fieldType);

            // Set default lengths for types that require them
            if (fieldType == TPrimitiveType.VARCHAR) {
                scalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
            } else if (fieldType == TPrimitiveType.CHAR) {
                scalarType.setLen(1);
            } else if (fieldType == TPrimitiveType.VARBINARY) {
                scalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
            }

            fieldNode.setScalar_type(scalarType);
            typeDesc.types.add(fieldNode);
        }

        return typeDesc;
    }

    private PTypeDesc buildProtobufScalarType(TPrimitiveType primitiveType) {
        PTypeDesc typeDesc = new PTypeDesc();
        typeDesc.types = new ArrayList<>();

        PTypeNode node = new PTypeNode();
        node.type = TTypeNodeType.SCALAR.getValue();
        node.scalarType = new PScalarType();
        node.scalarType.type = primitiveType.getValue();

        // Set default values for types that require them
        if (primitiveType == TPrimitiveType.VARCHAR ||
                primitiveType == TPrimitiveType.CHAR ||
                primitiveType == TPrimitiveType.VARBINARY) {
            node.scalarType.len = ScalarType.DEFAULT_STRING_LENGTH;
        }

        typeDesc.types.add(node);
        return typeDesc;
    }

    private PTypeDesc buildProtobufArrayType(TPrimitiveType elementType) {
        PTypeDesc typeDesc = new PTypeDesc();
        typeDesc.types = new ArrayList<>();

        // Array node
        PTypeNode arrayNode = new PTypeNode();
        arrayNode.type = TTypeNodeType.ARRAY.getValue();
        typeDesc.types.add(arrayNode);

        // Element type node
        PTypeNode elementNode = new PTypeNode();
        elementNode.type = TTypeNodeType.SCALAR.getValue();
        elementNode.scalarType = new PScalarType();
        elementNode.scalarType.type = elementType.getValue();

        // Set default values for types that require them
        if (elementType == TPrimitiveType.VARCHAR ||
                elementType == TPrimitiveType.CHAR ||
                elementType == TPrimitiveType.VARBINARY) {
            elementNode.scalarType.len = ScalarType.DEFAULT_STRING_LENGTH;
        }

        typeDesc.types.add(elementNode);

        return typeDesc;
    }

    private PTypeDesc buildProtobufMapType(TPrimitiveType keyType, TPrimitiveType valueType) {
        PTypeDesc typeDesc = new PTypeDesc();
        typeDesc.types = new ArrayList<>();

        // Map node
        PTypeNode mapNode = new PTypeNode();
        mapNode.type = TTypeNodeType.MAP.getValue();
        typeDesc.types.add(mapNode);

        // Key type node
        PTypeNode keyNode = new PTypeNode();
        keyNode.type = TTypeNodeType.SCALAR.getValue();
        keyNode.scalarType = new PScalarType();
        keyNode.scalarType.type = keyType.getValue();

        // Set default values for key type
        if (keyType == TPrimitiveType.VARCHAR ||
                keyType == TPrimitiveType.CHAR ||
                keyType == TPrimitiveType.VARBINARY) {
            keyNode.scalarType.len = ScalarType.DEFAULT_STRING_LENGTH;
        }

        typeDesc.types.add(keyNode);

        // Value type node
        PTypeNode valueNode = new PTypeNode();
        valueNode.type = TTypeNodeType.SCALAR.getValue();
        valueNode.scalarType = new PScalarType();
        valueNode.scalarType.type = valueType.getValue();

        // Set default values for value type
        if (valueType == TPrimitiveType.VARCHAR ||
                valueType == TPrimitiveType.CHAR ||
                valueType == TPrimitiveType.VARBINARY) {
            valueNode.scalarType.len = ScalarType.DEFAULT_STRING_LENGTH;
        }

        typeDesc.types.add(valueNode);

        return typeDesc;
    }

    private PTypeDesc buildProtobufStructType(List<String> fieldNames, List<TPrimitiveType> fieldTypes) {
        PTypeDesc typeDesc = new PTypeDesc();
        typeDesc.types = new ArrayList<>();

        // Struct node
        PTypeNode structNode = new PTypeNode();
        structNode.type = TTypeNodeType.STRUCT.getValue();
        structNode.structFields = new ArrayList<>();

        for (String fieldName : fieldNames) {
            PStructField field = new PStructField();
            field.name = fieldName;
            structNode.structFields.add(field);
        }
        typeDesc.types.add(structNode);

        // Field type nodes
        for (TPrimitiveType fieldType : fieldTypes) {
            PTypeNode fieldNode = new PTypeNode();
            fieldNode.type = TTypeNodeType.SCALAR.getValue();
            fieldNode.scalarType = new PScalarType();
            fieldNode.scalarType.type = fieldType.getValue();

            // Set default values for types that require them
            if (fieldType == TPrimitiveType.VARCHAR ||
                    fieldType == TPrimitiveType.CHAR ||
                    fieldType == TPrimitiveType.VARBINARY) {
                fieldNode.scalarType.len = ScalarType.DEFAULT_STRING_LENGTH;
            }

            typeDesc.types.add(fieldNode);
        }

        return typeDesc;
    }

    // Test cases for Thrift deserialization

    @Test
    public void testFromThriftBasicScalarTypes() {
        // Test basic primitive types
        TPrimitiveType[] primitiveTypes = {
                TPrimitiveType.BOOLEAN, TPrimitiveType.TINYINT, TPrimitiveType.SMALLINT,
                TPrimitiveType.INT, TPrimitiveType.BIGINT, TPrimitiveType.LARGEINT,
                TPrimitiveType.FLOAT, TPrimitiveType.DOUBLE, TPrimitiveType.DATE,
                TPrimitiveType.DATETIME, TPrimitiveType.TIME, TPrimitiveType.NULL_TYPE,
                TPrimitiveType.BINARY, TPrimitiveType.JSON
        };

        for (TPrimitiveType primitiveType : primitiveTypes) {
            TTypeDesc typeDesc = buildThriftScalarType(primitiveType);
            Type type = TypeDeserializer.fromThrift(typeDesc);

            Assertions.assertTrue(type.isScalarType());
            Assertions.assertEquals(TypeDeserializer.fromThrift(primitiveType),
                    ((ScalarType) type).getPrimitiveType());
        }
    }

    @Test
    public void testFromThriftCharAndVarcharTypes() {
        // Test CHAR type
        TTypeDesc charTypeDesc = buildThriftScalarTypeWithLength(TPrimitiveType.CHAR, 10);
        Type charType = TypeDeserializer.fromThrift(charTypeDesc);

        Assertions.assertTrue(charType.isScalarType());
        Assertions.assertTrue(charType.isChar());
        Assertions.assertEquals(PrimitiveType.CHAR, ((ScalarType) charType).getPrimitiveType());
        Assertions.assertEquals(10, ((ScalarType) charType).getLength());

        // Test VARCHAR type
        TTypeDesc varcharTypeDesc = buildThriftScalarTypeWithLength(TPrimitiveType.VARCHAR, 255);
        Type varcharType = TypeDeserializer.fromThrift(varcharTypeDesc);

        Assertions.assertTrue(varcharType.isScalarType());
        Assertions.assertTrue(varcharType.isStringType());
        Assertions.assertEquals(PrimitiveType.VARCHAR, ((ScalarType) varcharType).getPrimitiveType());
        Assertions.assertEquals(255, ((ScalarType) varcharType).getLength());

        // Test VARBINARY type
        TTypeDesc varbinaryTypeDesc = buildThriftScalarTypeWithLength(TPrimitiveType.VARBINARY, 1024);
        Type varbinaryType = TypeDeserializer.fromThrift(varbinaryTypeDesc);

        Assertions.assertTrue(varbinaryType.isScalarType());
        Assertions.assertEquals(PrimitiveType.VARBINARY, ((ScalarType) varbinaryType).getPrimitiveType());
        Assertions.assertEquals(1024, ((ScalarType) varbinaryType).getLength());
    }

    @Test
    public void testFromThriftHllType() {
        TTypeDesc hllTypeDesc = buildThriftScalarTypeWithLength(TPrimitiveType.HLL, ScalarType.MAX_HLL_LENGTH);
        Type hllType = TypeDeserializer.fromThrift(hllTypeDesc);

        Assertions.assertTrue(hllType.isScalarType());
        Assertions.assertEquals(PrimitiveType.HLL, ((ScalarType) hllType).getPrimitiveType());
        Assertions.assertEquals(ScalarType.MAX_HLL_LENGTH, ((ScalarType) hllType).getLength());
    }

    @Test
    public void testFromThriftDecimalTypes() {
        // Test DECIMAL (legacy)
        TTypeDesc decimalTypeDesc = buildThriftDecimalType(TPrimitiveType.DECIMAL, 10, 4);
        Type decimalType = TypeDeserializer.fromThrift(decimalTypeDesc);

        Assertions.assertTrue(decimalType.isScalarType());
        Assertions.assertTrue(decimalType.isDecimalV2());
        Assertions.assertEquals(10, ((ScalarType) decimalType).getScalarPrecision());
        Assertions.assertEquals(4, ((ScalarType) decimalType).getScalarScale());

        // Test DECIMALV2
        TTypeDesc decimalV2TypeDesc = buildThriftDecimalType(TPrimitiveType.DECIMALV2, 27, 9);
        Type decimalV2Type = TypeDeserializer.fromThrift(decimalV2TypeDesc);

        Assertions.assertTrue(decimalV2Type.isScalarType());
        Assertions.assertTrue(decimalV2Type.isDecimalV2());
        Assertions.assertEquals(27, ((ScalarType) decimalV2Type).getScalarPrecision());
        Assertions.assertEquals(9, ((ScalarType) decimalV2Type).getScalarScale());

        // Test DECIMAL32
        TTypeDesc decimal32TypeDesc = buildThriftDecimalType(TPrimitiveType.DECIMAL32, 9, 2);
        Type decimal32Type = TypeDeserializer.fromThrift(decimal32TypeDesc);

        Assertions.assertTrue(decimal32Type.isScalarType());
        Assertions.assertTrue(decimal32Type.isDecimalV3());
        Assertions.assertEquals(PrimitiveType.DECIMAL32, ((ScalarType) decimal32Type).getPrimitiveType());
        Assertions.assertEquals(9, ((ScalarType) decimal32Type).getScalarPrecision());
        Assertions.assertEquals(2, ((ScalarType) decimal32Type).getScalarScale());

        // Test DECIMAL64
        TTypeDesc decimal64TypeDesc = buildThriftDecimalType(TPrimitiveType.DECIMAL64, 18, 6);
        Type decimal64Type = TypeDeserializer.fromThrift(decimal64TypeDesc);

        Assertions.assertTrue(decimal64Type.isScalarType());
        Assertions.assertTrue(decimal64Type.isDecimalV3());
        Assertions.assertEquals(PrimitiveType.DECIMAL64, ((ScalarType) decimal64Type).getPrimitiveType());
        Assertions.assertEquals(18, ((ScalarType) decimal64Type).getScalarPrecision());
        Assertions.assertEquals(6, ((ScalarType) decimal64Type).getScalarScale());

        // Test DECIMAL128
        TTypeDesc decimal128TypeDesc = buildThriftDecimalType(TPrimitiveType.DECIMAL128, 38, 10);
        Type decimal128Type = TypeDeserializer.fromThrift(decimal128TypeDesc);

        Assertions.assertTrue(decimal128Type.isScalarType());
        Assertions.assertTrue(decimal128Type.isDecimalV3());
        Assertions.assertEquals(PrimitiveType.DECIMAL128, ((ScalarType) decimal128Type).getPrimitiveType());
        Assertions.assertEquals(38, ((ScalarType) decimal128Type).getScalarPrecision());
        Assertions.assertEquals(10, ((ScalarType) decimal128Type).getScalarScale());

        // Test DECIMAL256
        TTypeDesc decimal256TypeDesc = buildThriftDecimalType(TPrimitiveType.DECIMAL256, 76, 20);
        Type decimal256Type = TypeDeserializer.fromThrift(decimal256TypeDesc);

        Assertions.assertTrue(decimal256Type.isScalarType());
        Assertions.assertTrue(decimal256Type.isDecimalV3());
        Assertions.assertEquals(PrimitiveType.DECIMAL256, ((ScalarType) decimal256Type).getPrimitiveType());
        Assertions.assertEquals(76, ((ScalarType) decimal256Type).getScalarPrecision());
        Assertions.assertEquals(20, ((ScalarType) decimal256Type).getScalarScale());
    }

    @Test
    public void testFromThriftArrayType() {
        TTypeDesc arrayTypeDesc = buildThriftArrayType(TPrimitiveType.INT);
        Type arrayType = TypeDeserializer.fromThrift(arrayTypeDesc);

        Assertions.assertTrue(arrayType.isArrayType());
        ArrayType array = (ArrayType) arrayType;
        Assertions.assertTrue(array.getItemType().isScalarType());
        Assertions.assertEquals(PrimitiveType.INT, ((ScalarType) array.getItemType()).getPrimitiveType());
    }

    @Test
    public void testFromThriftMapType() {
        TTypeDesc mapTypeDesc = buildThriftMapType(TPrimitiveType.VARCHAR, TPrimitiveType.BIGINT);
        Type mapType = TypeDeserializer.fromThrift(mapTypeDesc);

        Assertions.assertTrue(mapType.isMapType());
        MapType map = (MapType) mapType;
        Assertions.assertTrue(map.getKeyType().isScalarType());
        Assertions.assertTrue(map.getValueType().isScalarType());
        Assertions.assertEquals(PrimitiveType.VARCHAR, ((ScalarType) map.getKeyType()).getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.BIGINT, ((ScalarType) map.getValueType()).getPrimitiveType());
    }

    @Test
    public void testFromThriftStructType() {
        List<String> fieldNames = Lists.newArrayList("field1", "field2", "field3");
        List<TPrimitiveType> fieldTypes = Lists.newArrayList(
                TPrimitiveType.INT, TPrimitiveType.VARCHAR, TPrimitiveType.DOUBLE);

        TTypeDesc structTypeDesc = buildThriftStructType(fieldNames, fieldTypes);
        Type structType = TypeDeserializer.fromThrift(structTypeDesc);

        Assertions.assertTrue(structType.isStructType());
        StructType struct = (StructType) structType;
        Assertions.assertEquals(3, struct.getFields().size());

        // Check field names and types
        Assertions.assertEquals("field1", struct.getFields().get(0).getName());
        Assertions.assertEquals("field2", struct.getFields().get(1).getName());
        Assertions.assertEquals("field3", struct.getFields().get(2).getName());

        Assertions.assertEquals(PrimitiveType.INT,
                ((ScalarType) struct.getFields().get(0).getType()).getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.VARCHAR,
                ((ScalarType) struct.getFields().get(1).getType()).getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.DOUBLE,
                ((ScalarType) struct.getFields().get(2).getType()).getPrimitiveType());

        // Check comments
        Assertions.assertEquals("Test comment for field1", struct.getFields().get(0).getComment());
        Assertions.assertNull(struct.getFields().get(1).getComment());
        Assertions.assertEquals("Test comment for field3", struct.getFields().get(2).getComment());
    }

    @Test
    public void testFromThriftNestedTypes() {
        // Create a complex nested type: ARRAY<MAP<VARCHAR, STRUCT<field1:INT, field2:DOUBLE>>>
        TTypeDesc nestedTypeDesc = new TTypeDesc();
        nestedTypeDesc.types = new ArrayList<>();

        // 1. ARRAY node
        TTypeNode arrayNode = new TTypeNode();
        arrayNode.setType(TTypeNodeType.ARRAY);
        nestedTypeDesc.types.add(arrayNode);

        // 2. MAP node
        TTypeNode mapNode = new TTypeNode();
        mapNode.setType(TTypeNodeType.MAP);
        nestedTypeDesc.types.add(mapNode);

        // 3. VARCHAR key node
        TTypeNode keyNode = new TTypeNode();
        keyNode.setType(TTypeNodeType.SCALAR);
        TScalarType keyScalarType = new TScalarType();
        keyScalarType.setType(TPrimitiveType.VARCHAR);
        keyScalarType.setLen(ScalarType.DEFAULT_STRING_LENGTH);
        keyNode.setScalar_type(keyScalarType);
        nestedTypeDesc.types.add(keyNode);

        // 4. STRUCT value node
        TTypeNode structNode = new TTypeNode();
        structNode.setType(TTypeNodeType.STRUCT);
        structNode.setStruct_fields(new ArrayList<>());

        TStructField field1 = new TStructField();
        field1.setName("field1");
        structNode.getStruct_fields().add(field1);

        TStructField field2 = new TStructField();
        field2.setName("field2");
        structNode.getStruct_fields().add(field2);

        nestedTypeDesc.types.add(structNode);

        // 5. INT field type
        TTypeNode intNode = new TTypeNode();
        intNode.setType(TTypeNodeType.SCALAR);
        TScalarType intScalarType = new TScalarType();
        intScalarType.setType(TPrimitiveType.INT);
        intNode.setScalar_type(intScalarType);
        nestedTypeDesc.types.add(intNode);

        // 6. DOUBLE field type
        TTypeNode doubleNode = new TTypeNode();
        doubleNode.setType(TTypeNodeType.SCALAR);
        TScalarType doubleScalarType = new TScalarType();
        doubleScalarType.setType(TPrimitiveType.DOUBLE);
        doubleNode.setScalar_type(doubleScalarType);
        nestedTypeDesc.types.add(doubleNode);

        Type nestedType = TypeDeserializer.fromThrift(nestedTypeDesc);

        // Verify the nested structure
        Assertions.assertTrue(nestedType.isArrayType());
        ArrayType array = (ArrayType) nestedType;

        Assertions.assertTrue(array.getItemType().isMapType());
        MapType map = (MapType) array.getItemType();

        Assertions.assertTrue(map.getKeyType().isStringType());
        Assertions.assertTrue(map.getValueType().isStructType());

        StructType struct = (StructType) map.getValueType();
        Assertions.assertEquals(2, struct.getFields().size());
        Assertions.assertEquals("field1", struct.getFields().get(0).getName());
        Assertions.assertEquals("field2", struct.getFields().get(1).getName());
        Assertions.assertEquals(PrimitiveType.INT,
                ((ScalarType) struct.getFields().get(0).getType()).getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.DOUBLE,
                ((ScalarType) struct.getFields().get(1).getType()).getPrimitiveType());
    }

    // Test cases for Protobuf deserialization

    @Test
    public void testFromProtobufBasicScalarTypes() {
        TPrimitiveType[] primitiveTypes = {
                TPrimitiveType.BOOLEAN, TPrimitiveType.TINYINT, TPrimitiveType.SMALLINT,
                TPrimitiveType.INT, TPrimitiveType.BIGINT, TPrimitiveType.LARGEINT,
                TPrimitiveType.FLOAT, TPrimitiveType.DOUBLE, TPrimitiveType.DATE,
                TPrimitiveType.DATETIME, TPrimitiveType.TIME, TPrimitiveType.NULL_TYPE,
                TPrimitiveType.BINARY, TPrimitiveType.JSON
        };

        for (TPrimitiveType primitiveType : primitiveTypes) {
            PTypeDesc typeDesc = buildProtobufScalarType(primitiveType);
            Type type = TypeDeserializer.fromProtobuf(typeDesc);

            Assertions.assertTrue(type.isScalarType());
            // Note: ScalarType.createType(PScalarType) method handles the conversion
        }
    }

    @Test
    public void testFromProtobufArrayType() {
        PTypeDesc arrayTypeDesc = buildProtobufArrayType(TPrimitiveType.BIGINT);
        Type arrayType = TypeDeserializer.fromProtobuf(arrayTypeDesc);

        Assertions.assertTrue(arrayType.isArrayType());
        ArrayType array = (ArrayType) arrayType;
        Assertions.assertTrue(array.getItemType().isScalarType());
    }

    @Test
    public void testFromProtobufMapType() {
        PTypeDesc mapTypeDesc = buildProtobufMapType(TPrimitiveType.INT, TPrimitiveType.VARCHAR);
        Type mapType = TypeDeserializer.fromProtobuf(mapTypeDesc);

        Assertions.assertTrue(mapType.isMapType());
        MapType map = (MapType) mapType;
        Assertions.assertTrue(map.getKeyType().isScalarType());
        Assertions.assertTrue(map.getValueType().isScalarType());
    }

    @Test
    public void testFromProtobufStructType() {
        List<String> fieldNames = Lists.newArrayList("id", "name", "score");
        List<TPrimitiveType> fieldTypes = Lists.newArrayList(
                TPrimitiveType.BIGINT, TPrimitiveType.VARCHAR, TPrimitiveType.DOUBLE);

        PTypeDesc structTypeDesc = buildProtobufStructType(fieldNames, fieldTypes);
        Type structType = TypeDeserializer.fromProtobuf(structTypeDesc);

        Assertions.assertTrue(structType.isStructType());
        StructType struct = (StructType) structType;
        Assertions.assertEquals(3, struct.getFields().size());

        Assertions.assertEquals("id", struct.getFields().get(0).getName());
        Assertions.assertEquals("name", struct.getFields().get(1).getName());
        Assertions.assertEquals("score", struct.getFields().get(2).getName());
    }

    @Test
    public void testFromProtobufNestedTypes() {
        // Create ARRAY<MAP<INT, VARCHAR>>
        PTypeDesc nestedTypeDesc = new PTypeDesc();
        nestedTypeDesc.types = new ArrayList<>();

        // 1. ARRAY node
        PTypeNode arrayNode = new PTypeNode();
        arrayNode.type = TTypeNodeType.ARRAY.getValue();
        nestedTypeDesc.types.add(arrayNode);

        // 2. MAP node
        PTypeNode mapNode = new PTypeNode();
        mapNode.type = TTypeNodeType.MAP.getValue();
        nestedTypeDesc.types.add(mapNode);

        // 3. INT key node
        PTypeNode keyNode = new PTypeNode();
        keyNode.type = TTypeNodeType.SCALAR.getValue();
        keyNode.scalarType = new PScalarType();
        keyNode.scalarType.type = TPrimitiveType.INT.getValue();
        nestedTypeDesc.types.add(keyNode);

        // 4. VARCHAR value node
        PTypeNode valueNode = new PTypeNode();
        valueNode.type = TTypeNodeType.SCALAR.getValue();
        valueNode.scalarType = new PScalarType();
        valueNode.scalarType.type = TPrimitiveType.VARCHAR.getValue();
        valueNode.scalarType.len = ScalarType.DEFAULT_STRING_LENGTH;
        nestedTypeDesc.types.add(valueNode);

        Type nestedType = TypeDeserializer.fromProtobuf(nestedTypeDesc);

        Assertions.assertTrue(nestedType.isArrayType());
        ArrayType array = (ArrayType) nestedType;
        Assertions.assertTrue(array.getItemType().isMapType());

        MapType map = (MapType) array.getItemType();
        Assertions.assertTrue(map.getKeyType().isScalarType());
        Assertions.assertTrue(map.getValueType().isScalarType());
    }

    // Test edge cases and error conditions

    @Test
    public void testFromThriftEmptyTypeDesc() {
        TTypeDesc emptyTypeDesc = new TTypeDesc();
        emptyTypeDesc.types = new ArrayList<>();

        Assertions.assertThrows(IllegalStateException.class, () -> {
            TypeDeserializer.fromThrift(emptyTypeDesc);
        });
    }

    @Test
    public void testFromProtobufEmptyTypeDesc() {
        PTypeDesc emptyTypeDesc = new PTypeDesc();
        emptyTypeDesc.types = new ArrayList<>();

        Assertions.assertThrows(IllegalStateException.class, () -> {
            TypeDeserializer.fromProtobuf(emptyTypeDesc);
        });
    }

    @Test
    public void testFromThriftStructWithNoFields() {
        TTypeDesc structTypeDesc = new TTypeDesc();
        structTypeDesc.types = new ArrayList<>();

        TTypeNode structNode = new TTypeNode();
        structNode.setType(TTypeNodeType.STRUCT);
        structNode.setStruct_fields(new ArrayList<>()); // Empty fields
        structTypeDesc.types.add(structNode);

        // Should throw IllegalArgumentException for empty struct fields
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            TypeDeserializer.fromThrift(structTypeDesc);
        });
    }

    @Test
    public void testFromProtobufStructWithNoFields() {
        PTypeDesc structTypeDesc = new PTypeDesc();
        structTypeDesc.types = new ArrayList<>();

        PTypeNode structNode = new PTypeNode();
        structNode.type = TTypeNodeType.STRUCT.getValue();
        structNode.structFields = new ArrayList<>(); // Empty fields
        structTypeDesc.types.add(structNode);

        // Should throw IllegalArgumentException for empty struct fields
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            TypeDeserializer.fromProtobuf(structTypeDesc);
        });
    }

    // Test serialization-deserialization round trip

    @Test
    public void testThriftSerializationRoundTrip() {
        // Test round trip for various types
        Type[] testTypes = {
                Type.BOOLEAN,
                Type.INT,
                Type.BIGINT,
                Type.DOUBLE,
                ScalarType.createVarcharType(255), // Use explicit length for VARCHAR
                ScalarType.createCharType(10),
                ScalarType.createVarcharType(255),
                ScalarType.createDecimalV2Type(10, 4),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6),
                new ArrayType(Type.INT),
                new MapType(ScalarType.createVarcharType(100), Type.BIGINT), // Use explicit length for VARCHAR key
                new StructType(Lists.newArrayList(
                        new StructField("f1", Type.INT),
                        new StructField("f2", ScalarType.createVarcharType(200), "test comment") // Use explicit length
                ))
        };

        for (Type originalType : testTypes) {
            // Serialize to Thrift
            TTypeDesc thriftTypeDesc = new TTypeDesc();
            thriftTypeDesc.types = new ArrayList<>();
            TypeSerializer.toThrift(originalType, thriftTypeDesc);

            // Deserialize from Thrift
            Type deserializedType = TypeDeserializer.fromThrift(thriftTypeDesc);

            // Verify types are equivalent
            Assertions.assertEquals(originalType.toString(), deserializedType.toString());

            if (originalType.isScalarType()) {
                ScalarType originalScalar = (ScalarType) originalType;
                ScalarType deserializedScalar = (ScalarType) deserializedType;
                Assertions.assertEquals(originalScalar.getPrimitiveType(),
                        deserializedScalar.getPrimitiveType());
                if (originalScalar.isStringType() || originalScalar.isChar()) {
                    Assertions.assertEquals(originalScalar.getLength(),
                            deserializedScalar.getLength());
                }
                if (originalScalar.isDecimalV2() || originalScalar.isDecimalV3()) {
                    Assertions.assertEquals(originalScalar.getScalarPrecision(),
                            deserializedScalar.getScalarPrecision());
                    Assertions.assertEquals(originalScalar.getScalarScale(),
                            deserializedScalar.getScalarScale());
                }
            }
        }
    }

    @Test
    public void testThriftDeserializationConsistency() {
        // Test that the same Thrift data always produces the same result
        TTypeDesc typeDesc = buildThriftMapType(TPrimitiveType.VARCHAR, TPrimitiveType.BIGINT);

        Type type1 = TypeDeserializer.fromThrift(typeDesc);
        Type type2 = TypeDeserializer.fromThrift(typeDesc);

        Assertions.assertEquals(type1.toString(), type2.toString());
        Assertions.assertEquals(type1.getClass(), type2.getClass());
    }

    @Test
    public void testProtobufDeserializationConsistency() {
        // Test that the same Protobuf data always produces the same result
        PTypeDesc typeDesc = buildProtobufArrayType(TPrimitiveType.DOUBLE);

        Type type1 = TypeDeserializer.fromProtobuf(typeDesc);
        Type type2 = TypeDeserializer.fromProtobuf(typeDesc);

        Assertions.assertEquals(type1.toString(), type2.toString());
        Assertions.assertEquals(type1.getClass(), type2.getClass());
    }
}
