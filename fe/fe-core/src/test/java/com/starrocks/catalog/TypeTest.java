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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/TypeTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.PScalarType;
import com.starrocks.proto.PStructField;
import com.starrocks.proto.PTypeDesc;
import com.starrocks.proto.PTypeNode;
import com.starrocks.sql.analyzer.ColumnDefAnalyzer;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TTypeNodeType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StandardTypes;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeDeserializer;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TypeTest {
    @Test
    public void testGetMysqlResultSetMetaData() {
        // tinyint
        ScalarType type = StandardTypes.TINYINT;
        Assertions.assertEquals(4, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // smallint
        type = StandardTypes.SMALLINT;
        Assertions.assertEquals(6, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // int
        type = StandardTypes.INT;
        Assertions.assertEquals(11, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // bigint
        type = StandardTypes.BIGINT;
        Assertions.assertEquals(20, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // largeint
        type = StandardTypes.LARGEINT;
        Assertions.assertEquals(40, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // date
        type = StandardTypes.DATE;
        Assertions.assertEquals(10, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // datetime
        type = StandardTypes.DATETIME;
        Assertions.assertEquals(19, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // float
        type = StandardTypes.FLOAT;
        Assertions.assertEquals(12, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(31, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // int
        type = StandardTypes.DOUBLE;
        Assertions.assertEquals(22, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(31, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // decimal
        type = TypeFactory.createDecimalV2Type(15, 5);
        Assertions.assertEquals(19, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(5, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // decimalv2
        type = TypeFactory.createDecimalV2Type(15, 0);
        Assertions.assertEquals(18, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // char
        type = TypeFactory.createCharType(10);
        Assertions.assertEquals(30, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // varchar
        type = TypeFactory.createVarcharType(11);
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // wildcard varchar
        type = TypeFactory.createVarcharType(-1);
        Assertions.assertEquals(192, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // bitmap
        type = StandardTypes.BITMAP;
        // 20 * 3
        Assertions.assertEquals(192, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // hll
        type = StandardTypes.HLL;
        // MAX_HLL_LENGTH(16385) * 3
        Assertions.assertEquals(49155, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // hll
        type = StandardTypes.JSON;
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // array
        ArrayType arrayType = new ArrayType(StandardTypes.INT);
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(arrayType));
        Assertions.assertEquals(0, arrayType.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(arrayType));

        // function (an invisible type for users, just used to express the lambda Functions in high-order functions)
        type = StandardTypes.FUNCTION;
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(type));

        MapType mapType =
                new MapType(TypeFactory.createType(PrimitiveType.INT), TypeFactory.createType(PrimitiveType.INT));
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(mapType));
        Assertions.assertEquals(0, mapType.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(mapType));

        StructField structField = new StructField("a", TypeFactory.createType(PrimitiveType.INT));
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(structField);
        StructType structType = new StructType(structFields);
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(structType));
        Assertions.assertEquals(0, structType.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(structType));
    }

    @Test
    public void testCanonicalName() {
        Object[][] testCases = new Object[][] {
                {TypeFactory.createDecimalV3NarrowestType(9, 2), "DECIMAL(9,2)"},
                {TypeFactory.createDecimalV3NarrowestType(18, 4), "DECIMAL(18,4)"},
                {TypeFactory.createDecimalV3NarrowestType(38, 6), "DECIMAL(38,6)"},
                {TypeFactory.createVarcharType(16), "VARCHAR(16)"},
                {TypeFactory.createCharType(16), "CHAR(16)"},
                {TypeFactory.createType(PrimitiveType.INT), "INT"},
                {TypeFactory.createType(PrimitiveType.FLOAT), "FLOAT"},
        };

        for (Object[] tc : testCases) {
            ScalarType type = (ScalarType) tc[0];
            String name = (String) tc[1];
            Assertions.assertEquals(name, type.canonicalName());
        }
    }

    @Test
    public void testMysqlDataType() {
        Object[][] testCases = new Object[][] {
                {TypeFactory.createType(PrimitiveType.BOOLEAN), "tinyint"},
                {TypeFactory.createType(PrimitiveType.LARGEINT), "bigint unsigned"},
                {TypeFactory.createDecimalV3NarrowestType(18, 4), "decimal"},
                {new ArrayType(StandardTypes.INT), "array"},
                {new MapType(StandardTypes.INT, StandardTypes.INT), "map"},
                {new StructType(Lists.newArrayList(StandardTypes.INT)), "struct"},
        };

        for (Object[] tc : testCases) {
            Type type = (Type) tc[0];
            String name = (String) tc[1];
            Assertions.assertEquals(name, type.toMysqlDataTypeString());
        }
    }

    @Test
    public void testMysqlColumnType() {
        Object[][] testCases = new Object[][] {
                {TypeFactory.createType(PrimitiveType.BOOLEAN), "tinyint(1)"},
                {TypeFactory.createType(PrimitiveType.LARGEINT), "bigint(20) unsigned"},
                {TypeFactory.createDecimalV3NarrowestType(18, 4), "decimal(18, 4)"},
                {new ArrayType(StandardTypes.INT), "array<int(11)>"},
                {new MapType(StandardTypes.INT, StandardTypes.INT), "map<int(11),int(11)>"},
                {new StructType(Lists.newArrayList(StandardTypes.INT)), "struct<col1 int(11)>"},
        };

        for (Object[] tc : testCases) {
            Type type = (Type) tc[0];
            String name = (String) tc[1];
            Assertions.assertEquals(name, type.toMysqlColumnTypeString());
        }
    }

    @Test
    public void testMapSerialAndDeser() {
        // map<int,struct<c1:int,cc1:string>>
        StructType c1 = new StructType(Lists.newArrayList(
                new StructField("c1", TypeFactory.createType(PrimitiveType.INT)),
                new StructField("cc1", TypeFactory.createDefaultCatalogString())
        ));
        MapType mapType =
                new MapType(TypeFactory.createType(PrimitiveType.INT), c1);
        String json = GsonUtils.GSON.toJson(mapType);
        Type deType = GsonUtils.GSON.fromJson(json, Type.class);
        Assertions.assertTrue(deType.isMapType());
        Assertions.assertEquals("MAP<INT,struct<c1 int(11), cc1 varchar(1073741824)>>", deType.toString());
        // Make sure select fields are false when initialized
        Assertions.assertFalse(deType.getSelectedFields()[0]);
        Assertions.assertFalse(deType.getSelectedFields()[1]);
    }

    @Test
    public void testStructSerialAndDeser() {
        // "struct<struct_test:int,c1:struct<c1:int,cc1:string>>"
        StructType c1 = new StructType(Lists.newArrayList(
                new StructField("c1", TypeFactory.createType(PrimitiveType.INT)),
                new StructField("cc1", TypeFactory.createDefaultCatalogString())
        ));
        StructType root = new StructType(Lists.newArrayList(
                new StructField("struct_test", TypeFactory.createType(PrimitiveType.INT), "comment test"),
                new StructField("c1", c1)
        ));
        String json = GsonUtils.GSON.toJson(root);
        Type deType = GsonUtils.GSON.fromJson(json, Type.class);
        Assertions.assertTrue(deType.isStructType());
        Assertions.assertEquals(
                "struct<struct_test int(11) COMMENT 'comment test', c1 struct<c1 int(11), cc1 varchar(1073741824)>>",
                deType.toString());
        // test initialed fieldMap by ctor in deserializer.
        Assertions.assertEquals(1, ((StructType) deType).getFieldPos("c1"));
    }

    private PTypeDesc buildScalarType(TPrimitiveType tPrimitiveType) {
        PTypeNode tn = new PTypeNode();
        tn.type = TTypeNodeType.SCALAR.getValue();
        tn.scalarType = new PScalarType();
        tn.scalarType.type = tPrimitiveType.getValue();

        PTypeDesc td = new PTypeDesc();
        td.types = new ArrayList<>();
        td.types.add(tn);
        return td;
    }

    private PTypeDesc buildArrayType(TPrimitiveType tPrimitiveType) {
        // BIGINT
        PTypeDesc td = new PTypeDesc();
        td.types = new ArrayList<>();

        // 1st: ARRAY
        PTypeNode tn = new PTypeNode();
        tn.type = TTypeNodeType.ARRAY.getValue();
        td.types.add(tn);

        // 2nd: BIGINT
        tn = new PTypeNode();
        tn.type = TTypeNodeType.SCALAR.getValue();
        tn.scalarType = new PScalarType();
        tn.scalarType.type = tPrimitiveType.getValue();
        td.types.add(tn);
        return td;
    }

    private PTypeDesc buildMapType(TPrimitiveType keyType, TPrimitiveType valueType) {
        PTypeDesc td = new PTypeDesc();
        td.types = new ArrayList<>();

        // 1st: ARRAY
        PTypeNode tn = new PTypeNode();
        tn.type = TTypeNodeType.MAP.getValue();
        td.types.add(tn);

        // 2nd: key
        tn = new PTypeNode();
        tn.type = TTypeNodeType.SCALAR.getValue();
        tn.scalarType = new PScalarType();
        tn.scalarType.type = keyType.getValue();
        td.types.add(tn);

        // 3nd: value
        tn = new PTypeNode();
        tn.type = TTypeNodeType.SCALAR.getValue();
        tn.scalarType = new PScalarType();
        tn.scalarType.type = valueType.getValue();
        td.types.add(tn);
        return td;
    }

    private PTypeDesc buildStructType(List<String> fieldNames, List<TPrimitiveType> fieldTypes) {
        PTypeDesc td = new PTypeDesc();
        td.types = new ArrayList<>();

        // STRUCT node
        PTypeNode tn = new PTypeNode();
        tn.type = TTypeNodeType.STRUCT.getValue();
        tn.structFields = new ArrayList<>();

        for (String fieldName : fieldNames) {
            PStructField field = new PStructField();
            field.name = fieldName;
            tn.structFields.add(field);
        }
        td.types.add(tn);

        // field node
        for (TPrimitiveType field : fieldTypes) {
            tn = new PTypeNode();
            tn.type = TTypeNodeType.SCALAR.getValue();
            tn.scalarType = new PScalarType();
            tn.scalarType.type = field.getValue();
            td.types.add(tn);
        }
        return td;
    }

    @Test
    public void testPTypeDescFromProtobuf() {
        PTypeDesc pTypeDesc = buildScalarType(TPrimitiveType.BIGINT);
        Type tp = TypeDeserializer.fromProtobuf(pTypeDesc);
        Assertions.assertTrue(tp.isBigint());

        pTypeDesc = buildArrayType(TPrimitiveType.BIGINT);
        tp = TypeDeserializer.fromProtobuf(pTypeDesc);
        Assertions.assertTrue(tp.isArrayType());

        pTypeDesc = buildMapType(TPrimitiveType.BIGINT, TPrimitiveType.BOOLEAN);
        tp = TypeDeserializer.fromProtobuf(pTypeDesc);
        Assertions.assertTrue(tp.isMapType());

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<TPrimitiveType> fieldTypes = new ArrayList<>();

        fieldNames.add("field_bigint");
        fieldTypes.add(TPrimitiveType.BIGINT);

        fieldNames.add("field_double");
        fieldTypes.add(TPrimitiveType.DOUBLE);

        pTypeDesc = buildStructType(fieldNames, fieldTypes);
        tp = TypeDeserializer.fromProtobuf(pTypeDesc);
        Assertions.assertTrue(tp.isStructType());
    }

    @Test
    public void testExtendedPrecision() {
        ScalarType type = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 10, 4);
        Assertions.assertSame(type, ColumnDefAnalyzer.extendedPrecision(type, true));
        Assertions.assertNotSame(type, ColumnDefAnalyzer.extendedPrecision(type, false));
    }

    @Test
    public void testCastJsonToMap() {
        Type jsonType = StandardTypes.JSON;
        List<Type> mapTypes = Lists.newArrayList(
                new MapType(StandardTypes.VARCHAR, StandardTypes.JSON),
                new MapType(StandardTypes.INT, StandardTypes.VARCHAR),
                new MapType(StandardTypes.VARCHAR, new ArrayType(StandardTypes.INT)),
                new MapType(StandardTypes.VARCHAR, new ArrayType(StandardTypes.JSON)),
                new MapType(StandardTypes.VARCHAR, new ArrayType(StandardTypes.JSON)),
                new MapType(StandardTypes.VARCHAR, new MapType(StandardTypes.VARCHAR, StandardTypes.BOOLEAN)),
                new MapType(StandardTypes.VARCHAR, new MapType(StandardTypes.INT, StandardTypes.JSON)),
                new MapType(StandardTypes.VARCHAR, new MapType(StandardTypes.INT, new ArrayType(StandardTypes.VARCHAR))),
                new MapType(StandardTypes.VARCHAR, new StructType(
                        Arrays.asList(StandardTypes.INT, new ArrayType(StandardTypes.VARCHAR),
                                new MapType(StandardTypes.INT, StandardTypes.JSON))))
        );
        for (Type mapType : mapTypes) {
            Assertions.assertTrue(Type.canCastTo(jsonType, mapType));
        }
    }

    @Test
    public void testSupportZonemap() {
        // Positive cases: Scalar types that are numeric, date, or string
        Assertions.assertTrue(StandardTypes.TINYINT.supportZoneMap());
        Assertions.assertTrue(StandardTypes.SMALLINT.supportZoneMap());
        Assertions.assertTrue(StandardTypes.INT.supportZoneMap());
        Assertions.assertTrue(StandardTypes.BIGINT.supportZoneMap());
        Assertions.assertTrue(StandardTypes.LARGEINT.supportZoneMap());
        Assertions.assertTrue(StandardTypes.FLOAT.supportZoneMap());
        Assertions.assertTrue(StandardTypes.DOUBLE.supportZoneMap());
        Assertions.assertTrue(StandardTypes.DATE.supportZoneMap());
        Assertions.assertTrue(StandardTypes.DATETIME.supportZoneMap());
        Assertions.assertTrue(StandardTypes.VARCHAR.supportZoneMap());
        Assertions.assertTrue(StandardTypes.CHAR.supportZoneMap());
        Assertions.assertTrue(StandardTypes.DEFAULT_DECIMALV2.supportZoneMap());
        Assertions.assertTrue(StandardTypes.DECIMAL32.supportZoneMap());
        Assertions.assertTrue(StandardTypes.DECIMAL64.supportZoneMap());
        Assertions.assertTrue(StandardTypes.DECIMAL128.supportZoneMap());
        Assertions.assertTrue(StandardTypes.DECIMAL256.supportZoneMap());
        Assertions.assertTrue(TypeFactory.createVarcharType(10).supportZoneMap());
        Assertions.assertTrue(TypeFactory.createCharType(5).supportZoneMap());

        // Negative cases: Non-scalar types or scalar types that are not numeric, date, or string
        Assertions.assertFalse(StandardTypes.NULL.supportZoneMap());
        Assertions.assertFalse(StandardTypes.BOOLEAN.supportZoneMap()); // Boolean is not numeric, date or string
        Assertions.assertFalse(StandardTypes.HLL.supportZoneMap());
        Assertions.assertFalse(StandardTypes.BITMAP.supportZoneMap());
        Assertions.assertFalse(StandardTypes.PERCENTILE.supportZoneMap());
        Assertions.assertFalse(StandardTypes.JSON.supportZoneMap());
        Assertions.assertFalse(StandardTypes.FUNCTION.supportZoneMap());
        Assertions.assertFalse(StandardTypes.VARBINARY.supportZoneMap());
        Assertions.assertFalse(StandardTypes.ARRAY_INT.supportZoneMap());
        Assertions.assertFalse(StandardTypes.MAP_VARCHAR_VARCHAR.supportZoneMap());
        Assertions.assertFalse(new StructType(Lists.newArrayList(StandardTypes.INT)).supportZoneMap());
    }
}
