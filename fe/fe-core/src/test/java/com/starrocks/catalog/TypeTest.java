// This file is made available under Elastic License 2.0.
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

import org.junit.Assert;
import org.junit.Test;

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
    }

    @Test
    public void testCanonicalName() {
        Object[][] testCases = new Object[][]{
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
}
