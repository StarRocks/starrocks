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
import com.starrocks.sql.common.TypeManager;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TTypeNodeType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BitmapType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.FunctionType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.MapType;
import com.starrocks.type.NullType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeDeserializer;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TypeTest {
    @Test
    public void testGetMysqlResultSetMetaData() {
        // tinyint
        ScalarType type = IntegerType.TINYINT;
        Assertions.assertEquals(4, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // smallint
        type = IntegerType.SMALLINT;
        Assertions.assertEquals(6, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // int
        type = IntegerType.INT;
        Assertions.assertEquals(11, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // bigint
        type = IntegerType.BIGINT;
        Assertions.assertEquals(20, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // largeint
        type = IntegerType.LARGEINT;
        Assertions.assertEquals(40, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // date
        type = DateType.DATE;
        Assertions.assertEquals(10, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // datetime
        type = DateType.DATETIME;
        Assertions.assertEquals(19, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // float
        type = FloatType.FLOAT;
        Assertions.assertEquals(12, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(31, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // int
        type = FloatType.DOUBLE;
        Assertions.assertEquals(22, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(31, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // decimal
        type = TypeFactory.createDecimalV2Type(15, 5);
        Assertions.assertEquals(19, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(5, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // decimalv2
        type = TypeFactory.createDecimalV2Type(15, 0);
        Assertions.assertEquals(18, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(63, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // char
        type = TypeFactory.createCharType(10);
        Assertions.assertEquals(30, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // varchar
        type = TypeFactory.createVarcharType(11);
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // wildcard varchar
        type = TypeFactory.createVarcharType(-1);
        Assertions.assertEquals(192, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // bitmap
        type = BitmapType.BITMAP;
        // 20 * 3
        Assertions.assertEquals(192, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // hll
        type = HLLType.HLL;
        // MAX_HLL_LENGTH(16385) * 3
        Assertions.assertEquals(49155, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // hll
        type = JsonType.JSON;
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(type));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(type));
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(type));

        // array
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(arrayType));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(arrayType));
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(arrayType));

        // function (an invisible type for users, just used to express the lambda Functions in high-order functions)
        type = FunctionType.FUNCTION;
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(type));

        MapType mapType =
                new MapType(IntegerType.INT, IntegerType.INT);
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(mapType));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(mapType));
        Assertions.assertEquals(33, MysqlCodec.getMysqlResultSetFieldCharsetIndex(mapType));

        StructField structField = new StructField("a", IntegerType.INT);
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(structField);
        StructType structType = new StructType(structFields);
        // default 20 * 3
        Assertions.assertEquals(60, MysqlCodec.getMysqlResultSetFieldLength(structType));
        Assertions.assertEquals(0, MysqlCodec.getMysqlResultSetFieldDecimals(structType));
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
                {IntegerType.INT, "INT"},
                {FloatType.FLOAT, "FLOAT"},
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
                {BooleanType.BOOLEAN, "tinyint"},
                {IntegerType.LARGEINT, "bigint unsigned"},
                {TypeFactory.createDecimalV3NarrowestType(18, 4), "decimal"},
                {new ArrayType(IntegerType.INT), "array"},
                {new MapType(IntegerType.INT, IntegerType.INT), "map"},
                {new StructType(Lists.newArrayList(IntegerType.INT)), "struct"},
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
                {BooleanType.BOOLEAN, "tinyint(1)"},
                {IntegerType.LARGEINT, "bigint(20) unsigned"},
                {TypeFactory.createDecimalV3NarrowestType(18, 4), "decimal(18, 4)"},
                {new ArrayType(IntegerType.INT), "array<int(11)>"},
                {new MapType(IntegerType.INT, IntegerType.INT), "map<int(11),int(11)>"},
                {new StructType(Lists.newArrayList(IntegerType.INT)), "struct<col1 int(11)>"},
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
                new StructField("c1", IntegerType.INT),
                new StructField("cc1", TypeFactory.createDefaultCatalogString())
        ));
        MapType mapType =
                new MapType(IntegerType.INT, c1);
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
                new StructField("c1", IntegerType.INT),
                new StructField("cc1", TypeFactory.createDefaultCatalogString())
        ));
        StructType root = new StructType(Lists.newArrayList(
                new StructField("struct_test", IntegerType.INT, "comment test"),
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
        Type jsonType = JsonType.JSON;
        List<Type> mapTypes = Lists.newArrayList(
                new MapType(VarcharType.VARCHAR, JsonType.JSON),
                new MapType(IntegerType.INT, VarcharType.VARCHAR),
                new MapType(VarcharType.VARCHAR, new ArrayType(IntegerType.INT)),
                new MapType(VarcharType.VARCHAR, new ArrayType(JsonType.JSON)),
                new MapType(VarcharType.VARCHAR, new ArrayType(JsonType.JSON)),
                new MapType(VarcharType.VARCHAR, new MapType(VarcharType.VARCHAR, BooleanType.BOOLEAN)),
                new MapType(VarcharType.VARCHAR, new MapType(IntegerType.INT, JsonType.JSON)),
                new MapType(VarcharType.VARCHAR, new MapType(IntegerType.INT, new ArrayType(VarcharType.VARCHAR))),
                new MapType(VarcharType.VARCHAR, new StructType(
                        Arrays.asList(IntegerType.INT, new ArrayType(VarcharType.VARCHAR),
                                new MapType(IntegerType.INT, JsonType.JSON))))
        );
        for (Type mapType : mapTypes) {
            Assertions.assertTrue(TypeManager.canCastTo(jsonType, mapType));
        }
    }

    @Test
    public void testSupportZonemap() {
        // Positive cases: Scalar types that are numeric, date, or string
        Assertions.assertTrue(IntegerType.TINYINT.supportZoneMap());
        Assertions.assertTrue(IntegerType.SMALLINT.supportZoneMap());
        Assertions.assertTrue(IntegerType.INT.supportZoneMap());
        Assertions.assertTrue(IntegerType.BIGINT.supportZoneMap());
        Assertions.assertTrue(IntegerType.LARGEINT.supportZoneMap());
        Assertions.assertTrue(FloatType.FLOAT.supportZoneMap());
        Assertions.assertTrue(FloatType.DOUBLE.supportZoneMap());
        Assertions.assertTrue(DateType.DATE.supportZoneMap());
        Assertions.assertTrue(DateType.DATETIME.supportZoneMap());
        Assertions.assertTrue(VarcharType.VARCHAR.supportZoneMap());
        Assertions.assertTrue(CharType.CHAR.supportZoneMap());
        Assertions.assertTrue(DecimalType.DEFAULT_DECIMALV2.supportZoneMap());
        Assertions.assertTrue(DecimalType.DECIMAL32.supportZoneMap());
        Assertions.assertTrue(DecimalType.DECIMAL64.supportZoneMap());
        Assertions.assertTrue(DecimalType.DECIMAL128.supportZoneMap());
        Assertions.assertTrue(DecimalType.DECIMAL256.supportZoneMap());
        Assertions.assertTrue(TypeFactory.createVarcharType(10).supportZoneMap());
        Assertions.assertTrue(TypeFactory.createCharType(5).supportZoneMap());

        // Negative cases: Non-scalar types or scalar types that are not numeric, date, or string
        Assertions.assertFalse(NullType.NULL.supportZoneMap());
        Assertions.assertFalse(BooleanType.BOOLEAN.supportZoneMap()); // Boolean is not numeric, date or string
        Assertions.assertFalse(HLLType.HLL.supportZoneMap());
        Assertions.assertFalse(BitmapType.BITMAP.supportZoneMap());
        Assertions.assertFalse(PercentileType.PERCENTILE.supportZoneMap());
        Assertions.assertFalse(JsonType.JSON.supportZoneMap());
        Assertions.assertFalse(FunctionType.FUNCTION.supportZoneMap());
        Assertions.assertFalse(VarbinaryType.VARBINARY.supportZoneMap());
        Assertions.assertFalse(ArrayType.ARRAY_INT.supportZoneMap());
        Assertions.assertFalse(MapType.MAP_VARCHAR_VARCHAR.supportZoneMap());
        Assertions.assertFalse(new StructType(Lists.newArrayList(IntegerType.INT)).supportZoneMap());
    }
}
