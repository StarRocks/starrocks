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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionProjectionPropertiesTest {

    @Test
    public void testParseDisabled() {
        Map<String, String> properties = new HashMap<>();
        properties.put("some.other.property", "value");

        PartitionProjectionProperties props = PartitionProjectionProperties.parse(properties);

        assertFalse(props.isEnabled());
        assertTrue(props.getColumnConfigs().isEmpty());
    }

    @Test
    public void testParseEnabledWithProjectionEnabled() {
        Map<String, String> properties = new HashMap<>();
        properties.put("projection.enabled", "true");
        properties.put("projection.region.type", "enum");
        properties.put("projection.region.values", "us-east-1,us-west-2,eu-west-1");

        PartitionProjectionProperties props = PartitionProjectionProperties.parse(properties);

        assertTrue(props.isEnabled());
        assertEquals(1, props.getColumnConfigs().size());
        assertNotNull(props.getColumnConfig("region"));
        assertEquals(PartitionProjectionProperties.ProjectionType.ENUM,
                props.getColumnConfig("region").getType());
    }

    @Test
    public void testParseEnabledWithProjectionEnable() {
        // Test legacy property key
        Map<String, String> properties = new HashMap<>();
        properties.put("projection.enable", "true");
        properties.put("projection.year.type", "integer");
        properties.put("projection.year.range", "2020,2025");

        PartitionProjectionProperties props = PartitionProjectionProperties.parse(properties);

        assertTrue(props.isEnabled());
        assertEquals(1, props.getColumnConfigs().size());
    }

    @Test
    public void testParseMultipleColumns() {
        Map<String, String> properties = new HashMap<>();
        properties.put("projection.enabled", "true");
        properties.put("projection.region.type", "enum");
        properties.put("projection.region.values", "us,eu");
        properties.put("projection.year.type", "integer");
        properties.put("projection.year.range", "2020,2025");
        properties.put("projection.dt.type", "date");
        properties.put("projection.dt.range", "2024-01-01,NOW");
        properties.put("projection.dt.format", "yyyy-MM-dd");

        PartitionProjectionProperties props = PartitionProjectionProperties.parse(properties);

        assertTrue(props.isEnabled());
        assertEquals(3, props.getColumnConfigs().size());
        assertEquals(PartitionProjectionProperties.ProjectionType.ENUM,
                props.getColumnConfig("region").getType());
        assertEquals(PartitionProjectionProperties.ProjectionType.INTEGER,
                props.getColumnConfig("year").getType());
        assertEquals(PartitionProjectionProperties.ProjectionType.DATE,
                props.getColumnConfig("dt").getType());
    }

    @Test
    public void testParseStorageLocationTemplate() {
        Map<String, String> properties = new HashMap<>();
        properties.put("projection.enabled", "true");
        properties.put("projection.region.type", "enum");
        properties.put("projection.region.values", "us,eu");
        properties.put("storage.location.template", "s3://bucket/data/${region}/");

        PartitionProjectionProperties props = PartitionProjectionProperties.parse(properties);

        assertTrue(props.getStorageLocationTemplate().isPresent());
        assertEquals("s3://bucket/data/${region}/",
                props.getStorageLocationTemplate().get());
    }

    @Test
    public void testParseNullProperties() {
        PartitionProjectionProperties props = PartitionProjectionProperties.parse(null);
        assertFalse(props.isEnabled());
    }

    @Test
    public void testIsProjectionEnabled() {
        Map<String, String> properties = new HashMap<>();

        assertFalse(PartitionProjectionProperties.isProjectionEnabled(null));
        assertFalse(PartitionProjectionProperties.isProjectionEnabled(properties));

        properties.put("projection.enabled", "false");
        assertFalse(PartitionProjectionProperties.isProjectionEnabled(properties));

        properties.put("projection.enabled", "true");
        assertTrue(PartitionProjectionProperties.isProjectionEnabled(properties));

        properties.clear();
        properties.put("projection.enable", "true");
        assertTrue(PartitionProjectionProperties.isProjectionEnabled(properties));
    }

    @Test
    public void testProjectionTypeFromString() {
        assertEquals(PartitionProjectionProperties.ProjectionType.ENUM,
                PartitionProjectionProperties.ProjectionType.fromString("enum"));
        assertEquals(PartitionProjectionProperties.ProjectionType.ENUM,
                PartitionProjectionProperties.ProjectionType.fromString("ENUM"));

        assertEquals(PartitionProjectionProperties.ProjectionType.INTEGER,
                PartitionProjectionProperties.ProjectionType.fromString("integer"));

        assertEquals(PartitionProjectionProperties.ProjectionType.DATE,
                PartitionProjectionProperties.ProjectionType.fromString("date"));

        assertEquals(PartitionProjectionProperties.ProjectionType.INJECTED,
                PartitionProjectionProperties.ProjectionType.fromString("injected"));

        assertThrows(IllegalArgumentException.class, () ->
                PartitionProjectionProperties.ProjectionType.fromString("unknown"));

        assertThrows(IllegalArgumentException.class, () ->
                PartitionProjectionProperties.ProjectionType.fromString(null));
    }
}
