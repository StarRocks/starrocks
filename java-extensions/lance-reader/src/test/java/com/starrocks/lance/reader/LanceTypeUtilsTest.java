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

package com.starrocks.lance.reader;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceTypeUtilsTest {

    @Test
    public void testFromArrowTypeBool() {
        assertEquals("boolean", LanceTypeUtils.fromArrowType(ArrowType.Bool.INSTANCE));
    }

    @Test
    public void testFromArrowTypeIntVariants() {
        assertEquals("tinyint", LanceTypeUtils.fromArrowType(new ArrowType.Int(8, true)));
        assertEquals("short", LanceTypeUtils.fromArrowType(new ArrowType.Int(16, true)));
        assertEquals("int", LanceTypeUtils.fromArrowType(new ArrowType.Int(32, true)));
        assertEquals("bigint", LanceTypeUtils.fromArrowType(new ArrowType.Int(64, true)));
    }

    @Test
    public void testFromArrowTypeFloat() {
        assertEquals("float",
                LanceTypeUtils.fromArrowType(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
        assertEquals("double",
                LanceTypeUtils.fromArrowType(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    }

    @Test
    public void testFromArrowTypeUtf8() {
        assertEquals("string", LanceTypeUtils.fromArrowType(ArrowType.Utf8.INSTANCE));
    }

    @Test
    public void testFromArrowTypeBinary() {
        assertEquals("binary", LanceTypeUtils.fromArrowType(new ArrowType.Binary()));
    }

    @Test
    public void testFromArrowTypeDate() {
        assertEquals("date",
                LanceTypeUtils.fromArrowType(
                        new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)));
    }

    @Test
    public void testFromArrowTypeTimestamp() {
        assertEquals("timestamp-micros",
                LanceTypeUtils.fromArrowType(
                        new ArrowType.Timestamp(
                                org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC")));
        assertEquals("timestamp-millis",
                LanceTypeUtils.fromArrowType(
                        new ArrowType.Timestamp(
                                org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)));
        assertEquals("timestamp-micros",
                LanceTypeUtils.fromArrowType(
                        new ArrowType.Timestamp(
                                org.apache.arrow.vector.types.TimeUnit.NANOSECOND, "UTC")));
        assertEquals("timestamp-micros",
                LanceTypeUtils.fromArrowType(
                        new ArrowType.Timestamp(
                                org.apache.arrow.vector.types.TimeUnit.SECOND, null)));
    }

    @Test
    public void testFromArrowTypeDecimal() {
        assertEquals("decimal(10,2)",
                LanceTypeUtils.fromArrowType(new ArrowType.Decimal(10, 2, 128)));
    }

    @Test
    public void testFromArrowTypeList() {
        assertThrows(IllegalArgumentException.class,
                () -> LanceTypeUtils.fromArrowType(ArrowType.List.INSTANCE));
    }

    @Test
    public void testBuildTypeMapping() {
        Schema schema = new Schema(Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("name", ArrowType.Utf8.INSTANCE),
                Field.nullable("score", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                Field.nullable("active", ArrowType.Bool.INSTANCE),
                new Field("tags", FieldType.nullable(ArrowType.List.INSTANCE),
                        Collections.singletonList(Field.nullable("item", ArrowType.Utf8.INSTANCE))),
                new Field("vector", FieldType.nullable(new ArrowType.FixedSizeList(3)),
                        Collections.singletonList(Field.nullable(
                                "item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))))
        ));

        Map<String, String> typeMap = LanceTypeUtils.buildTypeMapping(schema);

        assertNotNull(typeMap);
        assertEquals(6, typeMap.size());
        assertEquals("int", typeMap.get("id"));
        assertEquals("string", typeMap.get("name"));
        assertEquals("double", typeMap.get("score"));
        assertEquals("boolean", typeMap.get("active"));
        assertEquals("array<string>", typeMap.get("tags"));
        assertEquals("array<float>", typeMap.get("vector"));
    }

    @Test
    public void testFromArrowTypeUnknownFallsBackToString() {
        // Duration is an Arrow type not explicitly handled, should fall back to "string"
        assertEquals("string",
                LanceTypeUtils.fromArrowType(
                        new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.SECOND)));
    }
}
