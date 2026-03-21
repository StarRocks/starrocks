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

package com.starrocks.connector.opensearch;

import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.NullType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class OpenSearchUtilTest {

    @Test
    public void testConvertNullType() {
        Type result = OpenSearchUtil.convertType("null");
        assertEquals(NullType.NULL, result);
    }

    @Test
    public void testConvertBooleanType() {
        Type result = OpenSearchUtil.convertType("boolean");
        assertEquals(BooleanType.BOOLEAN, result);
    }

    @Test
    public void testConvertByteType() {
        Type result = OpenSearchUtil.convertType("byte");
        assertEquals(IntegerType.TINYINT, result);
    }

    @Test
    public void testConvertShortType() {
        Type result = OpenSearchUtil.convertType("short");
        assertEquals(IntegerType.SMALLINT, result);
    }

    @Test
    public void testConvertIntegerType() {
        Type result = OpenSearchUtil.convertType("integer");
        assertEquals(IntegerType.INT, result);
    }

    @Test
    public void testConvertLongType() {
        Type result = OpenSearchUtil.convertType("long");
        assertEquals(IntegerType.BIGINT, result);
    }

    @Test
    public void testConvertUnsignedLongType() {
        Type result = OpenSearchUtil.convertType("unsigned_long");
        assertEquals(IntegerType.LARGEINT, result);
    }

    @Test
    public void testConvertFloatType() {
        Type result = OpenSearchUtil.convertType("float");
        assertEquals(FloatType.FLOAT, result);
    }

    @Test
    public void testConvertHalfFloatType() {
        Type result = OpenSearchUtil.convertType("half_float");
        assertEquals(FloatType.FLOAT, result);
    }

    @Test
    public void testConvertDoubleType() {
        Type result = OpenSearchUtil.convertType("double");
        assertEquals(FloatType.DOUBLE, result);
    }

    @Test
    public void testConvertScaledFloatType() {
        Type result = OpenSearchUtil.convertType("scaled_float");
        assertEquals(FloatType.DOUBLE, result);
    }

    @Test
    public void testConvertDateType() {
        Type result = OpenSearchUtil.convertType("date");
        assertEquals(DateType.DATETIME, result);
    }

    @Test
    public void testConvertNestedType() {
        Type result = OpenSearchUtil.convertType("nested");
        assertEquals(JsonType.JSON, result);
    }

    @Test
    public void testConvertObjectType() {
        Type result = OpenSearchUtil.convertType("object");
        assertEquals(JsonType.JSON, result);
    }

    @Test
    public void testConvertKeywordType() {
        Type result = OpenSearchUtil.convertType("keyword");
        assertNotNull(result);
        // Keyword maps to default catalog string
    }

    @Test
    public void testConvertTextType() {
        Type result = OpenSearchUtil.convertType("text");
        assertNotNull(result);
        // Text maps to default catalog string
    }

    @Test
    public void testConvertIpType() {
        Type result = OpenSearchUtil.convertType("ip");
        assertNotNull(result);
        // IP maps to default catalog string
    }

    @Test
    public void testConvertUnknownType() {
        Type result = OpenSearchUtil.convertType("unknown_type");
        assertNotNull(result);
        // Unknown types map to default catalog string
    }
}
