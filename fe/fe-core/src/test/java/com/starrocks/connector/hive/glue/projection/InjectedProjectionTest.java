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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InjectedProjectionTest {

    @Test
    public void testGetProjectedValuesWithFilter() {
        InjectedProjection projection = new InjectedProjection("user_id");

        List<String> values = projection.getProjectedValues(Optional.of("12345"));

        assertEquals(1, values.size());
        assertEquals("12345", values.get(0));
    }

    @Test
    public void testGetProjectedValuesWithIntegerFilter() {
        InjectedProjection projection = new InjectedProjection("user_id");

        List<String> values = projection.getProjectedValues(Optional.of(12345));

        assertEquals(1, values.size());
        assertEquals("12345", values.get(0));
    }

    @Test
    public void testGetProjectedValuesWithoutFilter() {
        InjectedProjection projection = new InjectedProjection("user_id");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                projection.getProjectedValues(Optional.empty()));

        assertTrue(exception.getMessage().contains("user_id"));
        assertTrue(exception.getMessage().contains("requires a filter"));
    }

    @Test
    public void testFormatValue() {
        InjectedProjection projection = new InjectedProjection("user_id");

        assertEquals("test", projection.formatValue("test"));
        assertEquals("123", projection.formatValue(123));
    }

    @Test
    public void testGetColumnType() {
        InjectedProjection projection = new InjectedProjection("user_id");

        assertEquals(VarcharType.VARCHAR, projection.getColumnType());
    }

    @Test
    public void testGetColumnName() {
        InjectedProjection projection = new InjectedProjection("user_id");

        assertEquals("user_id", projection.getColumnName());
    }

    @Test
    public void testRequiresFilter() {
        InjectedProjection projection = new InjectedProjection("user_id");

        assertTrue(projection.requiresFilter());
    }
}
