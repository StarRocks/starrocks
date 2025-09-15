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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TypeTest {
    @Test
    public void testGetMysqlResultSetMetaData() {
        // tinyint
        ScalarType type = Type.TINYINT;
        Assertions.assertEquals(4, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // smallint
        type = Type.SMALLINT;
        Assertions.assertEquals(6, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // int
        type = Type.INT;
        Assertions.assertEquals(11, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // bigint
        type = Type.BIGINT;
        Assertions.assertEquals(20, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // largeint
        type = Type.LARGEINT;
        Assertions.assertEquals(40, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // date
        type = Type.DATE;
        Assertions.assertEquals(10, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // datetime
        type = Type.DATETIME;
        Assertions.assertEquals(19, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // float
        type = Type.FLOAT;
        Assertions.assertEquals(12, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(31, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // int
        type = Type.DOUBLE;
        Assertions.assertEquals(22, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(31, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // decimal
        type = ScalarType.createDecimalV2Type(15, 5);
        Assertions.assertEquals(19, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(5, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // decimalv2
        type = ScalarType.createDecimalV2Type(15, 0);
        Assertions.assertEquals(18, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // char
        type = ScalarType.createCharType(10);
        Assertions.assertEquals(30, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // varchar
        type = ScalarType.createVarcharType(11);
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // wildcard varchar
        type = ScalarType.createVarcharType(-1);
        Assertions.assertEquals(192, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // bitmap
        type = Type.BITMAP;
        // 20 * 3
        Assertions.assertEquals(192, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // hll
        type = Type.HLL;
        // MAX_HLL_LENGTH(16385) * 3
        Assertions.assertEquals(49155, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // hll
        type = Type.JSON;
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // array
        ArrayType arrayType = new ArrayType(Type.INT);
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(arrayType));
        Assertions.assertEquals(0, arrayType.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(arrayType));

        // function (an invisible type for users, just used to express the lambda Functions in high-order functions)
        type = Type.FUNCTION;
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(type));

        MapType mapType =
                new MapType(ScalarType.createType(PrimitiveType.INT), ScalarType.createType(PrimitiveType.INT));
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(mapType));
        Assertions.assertEquals(0, mapType.getMysqlResultSetFieldDecimals());
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(mapType));

        StructField structField = new StructField("a", ScalarType.createType(PrimitiveType.INT));
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
                {ScalarType.createDecimalV3NarrowestType(9, 2), "DECIMAL(9,2)"},
                {ScalarType.createDecimalV3NarrowestType(18, 4), "DECIMAL(18,4)"},
                {ScalarType.createDecimalV3NarrowestType(38, 6), "DECIMAL(38,6)"},
                {ScalarType.createVarchar(16), "VARCHAR(16)"},
                {ScalarType.createCharType(16), "CHAR(16)"},
                {ScalarType.createType(PrimitiveType.INT), "INT"},
                {ScalarType.createType(PrimitiveType.FLOAT), "FLOAT"},
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
                {ScalarType.createType(PrimitiveType.BOOLEAN), "tinyint"},
                {ScalarType.createType(PrimitiveType.LARGEINT), "bigint unsigned"},
                {ScalarType.createDecimalV3NarrowestType(18, 4), "decimal"},
                {new ArrayType(Type.INT), "array"},
                {new MapType(Type.INT, Type.INT), "map"},
                {new StructType(Lists.newArrayList(Type.INT)), "struct"},
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
                {ScalarType.createType(PrimitiveType.BOOLEAN), "tinyint(1)"},
                {ScalarType.createType(PrimitiveType.LARGEINT), "bigint(20) unsigned"},
                {ScalarType.createDecimalV3NarrowestType(18, 4), "decimal(18, 4)"},
                {new ArrayType(Type.INT), "array<int(11)>"},
                {new MapType(Type.INT, Type.INT), "map<int(11),int(11)>"},
                {new StructType(Lists.newArrayList(Type.INT)), "struct<col1 int(11)>"},
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
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultCatalogString())
        ));
        MapType mapType =
                new MapType(ScalarType.createType(PrimitiveType.INT), c1);
        String json = GsonUtils.GSON.toJson(mapType);
        Type deType = GsonUtils.GSON.fromJson(json, Type.class);
        Assertions.assertTrue(deType.isMapType());
        Assertions.assertEquals("MAP<INT,struct<c1 int(11), cc1 varchar(1073741824)>>", deType.toString());
        // Make sure select fields are false when initialized
        Assertions.assertFalse(deType.selectedFields[0]);
        Assertions.assertFalse(deType.selectedFields[1]);
    }

    @Test
    public void testStructSerialAndDeser() {
        // "struct<struct_test:int,c1:struct<c1:int,cc1:string>>"
        StructType c1 = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultCatalogString())
        ));
        StructType root = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT), "comment test"),
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
        Type tp = Type.fromProtobuf(pTypeDesc);
        Assertions.assertTrue(tp.isBigint());

        pTypeDesc = buildArrayType(TPrimitiveType.BIGINT);
        tp = Type.fromProtobuf(pTypeDesc);
        Assertions.assertTrue(tp.isArrayType());

        pTypeDesc = buildMapType(TPrimitiveType.BIGINT, TPrimitiveType.BOOLEAN);
        tp = Type.fromProtobuf(pTypeDesc);
        Assertions.assertTrue(tp.isMapType());

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<TPrimitiveType> fieldTypes = new ArrayList<>();

        fieldNames.add("field_bigint");
        fieldTypes.add(TPrimitiveType.BIGINT);

        fieldNames.add("field_double");
        fieldTypes.add(TPrimitiveType.DOUBLE);

        pTypeDesc = buildStructType(fieldNames, fieldTypes);
        tp = Type.fromProtobuf(pTypeDesc);
        Assertions.assertTrue(tp.isStructType());
    }

    @Test
    public void testExtendedPrecision() {
        ScalarType type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 10, 4);
        Assertions.assertSame(type, ColumnDefAnalyzer.extendedPrecision(type, true));
        Assertions.assertNotSame(type, ColumnDefAnalyzer.extendedPrecision(type, false));
    }

    @Test
    public void testCastJsonToMap() {
        Type jsonType = Type.JSON;
        List<Type> mapTypes = Lists.newArrayList(
                new MapType(Type.VARCHAR, Type.JSON),
                new MapType(Type.INT, Type.VARCHAR),
                new MapType(Type.VARCHAR, new ArrayType(Type.INT)),
                new MapType(Type.VARCHAR, new ArrayType(Type.JSON)),
                new MapType(Type.VARCHAR, new ArrayType(Type.JSON)),
                new MapType(Type.VARCHAR, new MapType(Type.VARCHAR, Type.BOOLEAN)),
                new MapType(Type.VARCHAR, new MapType(Type.INT, Type.JSON)),
                new MapType(Type.VARCHAR, new MapType(Type.INT, new ArrayType(Type.VARCHAR))),
                new MapType(Type.VARCHAR, new StructType(
                        Arrays.asList(Type.INT, new ArrayType(Type.VARCHAR), new MapType(Type.INT, Type.JSON))))
        );
        for (Type mapType : mapTypes) {
            Assertions.assertTrue(Type.canCastTo(jsonType, mapType));
        }
    }

    @Test
    public void testSupportZonemap() {
        // Positive cases: Scalar types that are numeric, date, or string
        Assertions.assertTrue(Type.TINYINT.supportZonemap());
        Assertions.assertTrue(Type.SMALLINT.supportZonemap());
        Assertions.assertTrue(Type.INT.supportZonemap());
        Assertions.assertTrue(Type.BIGINT.supportZonemap());
        Assertions.assertTrue(Type.LARGEINT.supportZonemap());
        Assertions.assertTrue(Type.FLOAT.supportZonemap());
        Assertions.assertTrue(Type.DOUBLE.supportZonemap());
        Assertions.assertTrue(Type.DATE.supportZonemap());
        Assertions.assertTrue(Type.DATETIME.supportZonemap());
        Assertions.assertTrue(Type.VARCHAR.supportZonemap());
        Assertions.assertTrue(Type.CHAR.supportZonemap());
        Assertions.assertTrue(Type.DEFAULT_DECIMALV2.supportZonemap());
        Assertions.assertTrue(Type.DECIMAL32.supportZonemap());
        Assertions.assertTrue(Type.DECIMAL64.supportZonemap());
        Assertions.assertTrue(Type.DECIMAL128.supportZonemap());
        Assertions.assertTrue(Type.DECIMAL256.supportZonemap());
        Assertions.assertTrue(ScalarType.createVarcharType(10).supportZonemap());
        Assertions.assertTrue(ScalarType.createCharType(5).supportZonemap());

        // Negative cases: Non-scalar types or scalar types that are not numeric, date, or string
        Assertions.assertFalse(Type.NULL.supportZonemap());
        Assertions.assertFalse(Type.BOOLEAN.supportZonemap()); // Boolean is not numeric, date or string
        Assertions.assertFalse(Type.HLL.supportZonemap());
        Assertions.assertFalse(Type.BITMAP.supportZonemap());
        Assertions.assertFalse(Type.PERCENTILE.supportZonemap());
        Assertions.assertFalse(Type.JSON.supportZonemap());
        Assertions.assertFalse(Type.FUNCTION.supportZonemap());
        Assertions.assertFalse(Type.VARBINARY.supportZonemap());
        Assertions.assertFalse(Type.ARRAY_INT.supportZonemap());
        Assertions.assertFalse(Type.MAP_VARCHAR_VARCHAR.supportZonemap());
        Assertions.assertFalse(new StructType(Lists.newArrayList(Type.INT)).supportZonemap());
    }
}
