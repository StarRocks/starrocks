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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.connector.hive.Partition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionProjectionServiceTest {

    private PartitionProjectionService service;

    @BeforeEach
    public void setUp() {
        service = new PartitionProjectionService();
    }

    @Test
    public void testIsEnabledWithProjectionEnabled() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.region.type", "enum",
                        "projection.region.values", "us,eu"
                ),
                ImmutableList.of("region")
        );

        assertTrue(service.isEnabled(table));
    }

    @Test
    public void testIsEnabledWithProjectionDisabled() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "false"
                ),
                ImmutableList.of("region")
        );

        assertFalse(service.isEnabled(table));
    }

    @Test
    public void testIsEnabledWithNoProjectionProperty() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(),
                ImmutableList.of("region")
        );

        assertFalse(service.isEnabled(table));
    }

    @Test
    public void testIsEnabledWithNonHiveTable() {
        Table table = mock(Table.class);
        assertFalse(service.isEnabled(table));
    }

    @Test
    public void testGetProjectedPartitionNamesWithEnumProjection() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.region.type", "enum",
                        "projection.region.values", "us,eu,asia"
                ),
                ImmutableList.of("region")
        );

        List<String> partitionNames = service.getProjectedPartitionNames(table);

        assertEquals(3, partitionNames.size());
        assertTrue(partitionNames.contains("region=us"));
        assertTrue(partitionNames.contains("region=eu"));
        assertTrue(partitionNames.contains("region=asia"));
    }

    @Test
    public void testGetProjectedPartitionNamesByValueWithFilter() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.region.type", "enum",
                        "projection.region.values", "us,eu,asia"
                ),
                ImmutableList.of("region")
        );

        List<Optional<String>> partitionValues = ImmutableList.of(Optional.of("us"));
        List<String> partitionNames = service.getProjectedPartitionNamesByValue(table, partitionValues);

        assertEquals(1, partitionNames.size());
        assertEquals("region=us", partitionNames.get(0));
    }

    @Test
    public void testGetProjectedPartitionNamesByValueWithNoFilter() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.region.type", "enum",
                        "projection.region.values", "us,eu"
                ),
                ImmutableList.of("region")
        );

        List<Optional<String>> partitionValues = ImmutableList.of(Optional.empty());
        List<String> partitionNames = service.getProjectedPartitionNamesByValue(table, partitionValues);

        assertEquals(2, partitionNames.size());
    }

    @Test
    public void testGetProjectedPartitionsFromNames() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.region.type", "enum",
                        "projection.region.values", "us,eu,asia"
                ),
                ImmutableList.of("region")
        );

        List<String> partitionNames = Arrays.asList("region=us", "region=eu");
        Map<String, Partition> partitions = service.getProjectedPartitionsFromNames(table, partitionNames);

        assertEquals(2, partitions.size());
        assertNotNull(partitions.get("region=us"));
        assertNotNull(partitions.get("region=eu"));

        // Verify partition locations
        assertTrue(partitions.get("region=us").getFullPath().endsWith("region=us"));
        assertTrue(partitions.get("region=eu").getFullPath().endsWith("region=eu"));
    }

    @Test
    public void testGetProjectedPartitionsFromNamesWithMultipleColumns() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.year.type", "integer",
                        "projection.year.range", "2023,2024",
                        "projection.region.type", "enum",
                        "projection.region.values", "us,eu"
                ),
                ImmutableList.of("year", "region")
        );

        List<String> partitionNames = Arrays.asList("year=2023/region=us", "year=2024/region=eu");
        Map<String, Partition> partitions = service.getProjectedPartitionsFromNames(table, partitionNames);

        assertEquals(2, partitions.size());
        assertNotNull(partitions.get("year=2023/region=us"));
        assertNotNull(partitions.get("year=2024/region=eu"));
    }

    @Test
    public void testGetProjectedPartitionsFromNamesWithEmptyList() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.region.type", "enum",
                        "projection.region.values", "us,eu"
                ),
                ImmutableList.of("region")
        );

        List<String> partitionNames = ImmutableList.of();
        Map<String, Partition> partitions = service.getProjectedPartitionsFromNames(table, partitionNames);

        assertTrue(partitions.isEmpty());
    }

    @Test
    public void testCreateProjectionWithIntegerType() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.year.type", "integer",
                        "projection.year.range", "2020,2024"
                ),
                ImmutableList.of("year")
        );

        List<String> partitionNames = service.getProjectedPartitionNames(table);

        assertEquals(5, partitionNames.size());
        assertTrue(partitionNames.contains("year=2020"));
        assertTrue(partitionNames.contains("year=2024"));
    }

    @Test
    public void testCreateProjectionWithDateType() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.dt.type", "date",
                        "projection.dt.range", "2024-01-01,2024-01-03",
                        "projection.dt.format", "yyyy-MM-dd"
                ),
                ImmutableList.of("dt")
        );

        List<String> partitionNames = service.getProjectedPartitionNames(table);

        assertEquals(3, partitionNames.size());
        assertTrue(partitionNames.contains("dt=2024-01-01"));
        assertTrue(partitionNames.contains("dt=2024-01-02"));
        assertTrue(partitionNames.contains("dt=2024-01-03"));
    }

    private HiveTable createMockHiveTable(Map<String, String> properties, List<String> partitionColumns) {
        HiveTable table = mock(HiveTable.class);
        when(table.getProperties()).thenReturn(new HashMap<>(properties));
        when(table.getPartitionColumnNames()).thenReturn(partitionColumns);
        when(table.getTableLocation()).thenReturn("s3://bucket/table");
        when(table.getStorageFormat()).thenReturn(HiveStorageFormat.PARQUET);
        when(table.getSerdeProperties()).thenReturn(new HashMap<>());
        return table;
    }
}
