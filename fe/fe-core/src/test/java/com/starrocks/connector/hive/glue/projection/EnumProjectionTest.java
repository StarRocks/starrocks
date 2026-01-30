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

package com.starrocks.connector.hive.glue.projection;

import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EnumProjectionTest {

    @Test
    public void testGetProjectedValuesNoFilter() {
        EnumProjection projection = new EnumProjection("region", "us-east-1,us-west-2,eu-west-1");

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(3, values.size());
        assertTrue(values.contains("us-east-1"));
        assertTrue(values.contains("us-west-2"));
        assertTrue(values.contains("eu-west-1"));
    }

    @Test
    public void testGetProjectedValuesWithMatchingFilter() {
        EnumProjection projection = new EnumProjection("region", "us-east-1,us-west-2,eu-west-1");

        List<String> values = projection.getProjectedValues(Optional.of("us-east-1"));

        assertEquals(1, values.size());
        assertEquals("us-east-1", values.get(0));
    }

    @Test
    public void testGetProjectedValuesWithNonMatchingFilter() {
        EnumProjection projection = new EnumProjection("region", "us-east-1,us-west-2,eu-west-1");

        List<String> values = projection.getProjectedValues(Optional.of("ap-northeast-1"));

        assertTrue(values.isEmpty());
    }

    @Test
    public void testGetProjectedValuesWithSpaces() {
        EnumProjection projection = new EnumProjection("region", " us-east-1 , us-west-2 , eu-west-1 ");

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(3, values.size());
        assertEquals("us-east-1", values.get(0));
        assertEquals("us-west-2", values.get(1));
        assertEquals("eu-west-1", values.get(2));
    }

    @Test
    public void testFormatValue() {
        EnumProjection projection = new EnumProjection("region", "us,eu");

        assertEquals("us", projection.formatValue("us"));
        assertEquals("123", projection.formatValue(123));
    }

    @Test
    public void testGetColumnType() {
        EnumProjection projection = new EnumProjection("region", "us,eu");

        assertEquals(VarcharType.VARCHAR, projection.getColumnType());
    }

    @Test
    public void testGetColumnName() {
        EnumProjection projection = new EnumProjection("region", "us,eu");

        assertEquals("region", projection.getColumnName());
    }

    @Test
    public void testGetValues() {
        EnumProjection projection = new EnumProjection("region", "us,eu,ap");

        List<String> values = projection.getValues();

        assertEquals(3, values.size());
        assertEquals("us", values.get(0));
        assertEquals("eu", values.get(1));
        assertEquals("ap", values.get(2));
    }

    @Test
    public void testConstructorWithNullValues() {
        assertThrows(IllegalArgumentException.class, () ->
                new EnumProjection("region", null));
    }

    @Test
    public void testConstructorWithEmptyValues() {
        assertThrows(IllegalArgumentException.class, () ->
                new EnumProjection("region", ""));

        assertThrows(IllegalArgumentException.class, () ->
                new EnumProjection("region", "   "));
    }

    @Test
    public void testConstructorWithOnlyCommas() {
        assertThrows(IllegalArgumentException.class, () ->
                new EnumProjection("region", ",,,"));
    }

    @Test
    public void testRequiresFilter() {
        EnumProjection projection = new EnumProjection("region", "us,eu");

        assertFalse(projection.requiresFilter());
    }
}
