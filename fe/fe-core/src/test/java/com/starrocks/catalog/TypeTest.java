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
import com.starrocks.persist.gson.GsonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class TypeTest {
    @Test
    public void testGetMysqlResultSetMetaData() {
        // tinyint
        ScalarType type = Type.TINYINT;
        Assert.assertEquals(4, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // smallint
        type = Type.SMALLINT;
        Assert.assertEquals(6, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // int
        type = Type.INT;
        Assert.assertEquals(11, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // bigint
        type = Type.BIGINT;
        Assert.assertEquals(20, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // largeint
        type = Type.LARGEINT;
        Assert.assertEquals(40, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, type.getMysqlResultSetFieldCharsetIndex());

        // date
        type = Type.DATE;
        Assert.assertEquals(10, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // datetime
        type = Type.DATETIME;
        Assert.assertEquals(19, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // float
        type = Type.FLOAT;
        Assert.assertEquals(12, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(31, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // int
        type = Type.DOUBLE;
        Assert.assertEquals(22, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(31, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // decimal
        type = ScalarType.createDecimalV2Type(15, 5);
        Assert.assertEquals(19, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(5, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // decimalv2
        type = ScalarType.createDecimalV2Type(15, 0);
        Assert.assertEquals(18, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(63, type.getMysqlResultSetFieldCharsetIndex());

        // char
        type = ScalarType.createCharType(10);
        Assert.assertEquals(30, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, type.getMysqlResultSetFieldCharsetIndex());

        // varchar
        type = ScalarType.createVarcharType(11);
        Assert.assertEquals(33, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, type.getMysqlResultSetFieldCharsetIndex());

        // wildcard varchar
        type = ScalarType.createVarcharType(-1);
        Assert.assertEquals(192, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, type.getMysqlResultSetFieldCharsetIndex());

        // bitmap
        type = Type.BITMAP;
        // 20 * 3
        Assert.assertEquals(192, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, type.getMysqlResultSetFieldCharsetIndex());

        // hll
        type = Type.HLL;
        // MAX_HLL_LENGTH(16385) * 3
        Assert.assertEquals(49155, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, type.getMysqlResultSetFieldCharsetIndex());

        // hll
        type = Type.JSON;
        // default 20 * 3
        Assert.assertEquals(60, type.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, type.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, type.getMysqlResultSetFieldCharsetIndex());

        // array
        ArrayType arrayType = new ArrayType(Type.INT);
        // default 20 * 3
        Assert.assertEquals(60, arrayType.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, arrayType.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, arrayType.getMysqlResultSetFieldCharsetIndex());

        // function (an invisible type for users, just used to express the lambda Functions in high-order functions)
        type = Type.FUNCTION;
        Assert.assertEquals(60, type.getMysqlResultSetFieldLength());

        MapType mapType =
                new MapType(ScalarType.createType(PrimitiveType.INT), ScalarType.createType(PrimitiveType.INT));
        // default 20 * 3
        Assert.assertEquals(60, mapType.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, mapType.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, mapType.getMysqlResultSetFieldCharsetIndex());

        StructField structField = new StructField("a", ScalarType.createType(PrimitiveType.INT));
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(structField);
        StructType structType = new StructType(structFields);
        // default 20 * 3
        Assert.assertEquals(60, structType.getMysqlResultSetFieldLength());
        Assert.assertEquals(0, structType.getMysqlResultSetFieldDecimals());
        Assert.assertEquals(33, structType.getMysqlResultSetFieldCharsetIndex());
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
            Assert.assertEquals(name, type.canonicalName());
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
            Assert.assertEquals(name, type.toMysqlDataTypeString());
        }
    }

    @Test
    public void testMysqlColumnType() {
        Object[][] testCases = new Object[][] {
                {ScalarType.createType(PrimitiveType.BOOLEAN), "tinyint(1)"},
                {ScalarType.createType(PrimitiveType.LARGEINT), "bigint(20) unsigned"},
                {ScalarType.createDecimalV3NarrowestType(18, 4), "decimal64(18, 4)"},
                {new ArrayType(Type.INT), "array<int(11)>"},
                {new MapType(Type.INT, Type.INT), "map<int(11),int(11)>"},
                {new StructType(Lists.newArrayList(Type.INT)), "struct<int(11)>"},
        };

        for (Object[] tc : testCases) {
            Type type = (Type) tc[0];
            String name = (String) tc[1];
            Assert.assertEquals(name, type.toMysqlColumnTypeString());
        }
    }

    @Test
    public void testMapSerialAndDeser() {
        // map<int,struct<c1:int,cc1:string>>
        StructType c1 = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultExternalTableString())
        ));
        MapType mapType =
                new MapType(ScalarType.createType(PrimitiveType.INT), c1);
        String json = GsonUtils.GSON.toJson(mapType);
        Type deType = GsonUtils.GSON.fromJson(json, Type.class);
        Assert.assertTrue(deType.isMapType());
        Assert.assertEquals("MAP<INT,STRUCT<c1 int(11), cc1 varchar(1048576)>>", deType.toString());
        // Make sure select fields are false when initialized
        Assert.assertFalse(deType.selectedFields[0]);
        Assert.assertFalse(deType.selectedFields[1]);
    }

    @Test
    public void testStructSerialAndDeser() {
        // "struct<struct_test:int,c1:struct<c1:int,cc1:string>>"
        StructType c1 = new StructType(Lists.newArrayList(
                new StructField("c1", ScalarType.createType(PrimitiveType.INT)),
                new StructField("cc1", ScalarType.createDefaultExternalTableString())
        ));
        StructType root = new StructType(Lists.newArrayList(
                new StructField("struct_test", ScalarType.createType(PrimitiveType.INT), "comment test"),
                new StructField("c1", c1)
        ));
        String json = GsonUtils.GSON.toJson(root);
        Type deType = GsonUtils.GSON.fromJson(json, Type.class);
        Assert.assertTrue(deType.isStructType());
        Assert.assertEquals("STRUCT<struct_test int(11) COMMENT 'comment test', c1 STRUCT<c1 int(11), cc1 varchar(1048576)>>",
                deType.toString());
        // test initialed fieldMap by ctor in deserializer.
        Assert.assertEquals(1, ((StructType) deType).getFieldPos("c1"));
    }
}
