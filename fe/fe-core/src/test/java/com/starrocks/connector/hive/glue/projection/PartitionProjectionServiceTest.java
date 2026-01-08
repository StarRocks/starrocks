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
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.connector.hive.Partition;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.PrimitiveType;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    // ==================== Bug Fix Tests ====================

    /**
     * Test for Bug 1 fix: Multiple partition keys should all be processed
     * Previously only the first partition key was used, ignoring others.
     * e.g., WHERE year IN (2023, 2024) should return partitions for both years.
     */
    @Test
    public void testGetProjectedPartitionsWithMultiplePartitionKeys() throws Exception {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.year.type", "integer",
                        "projection.year.range", "2020,2025"
                ),
                ImmutableList.of("year")
        );

        // Create multiple partition keys simulating WHERE year IN (2023, 2024)
        PartitionKey key2023 = new PartitionKey();
        key2023.pushColumn(new StringLiteral("2023"), PrimitiveType.VARCHAR);

        PartitionKey key2024 = new PartitionKey();
        key2024.pushColumn(new StringLiteral("2024"), PrimitiveType.VARCHAR);

        List<PartitionKey> partitionKeys = Arrays.asList(key2023, key2024);

        Map<String, Partition> partitions = service.getProjectedPartitions(table, partitionKeys);

        // Should return partitions for BOTH years, not just the first one
        assertEquals(2, partitions.size());
        assertNotNull(partitions.get("year=2023"));
        assertNotNull(partitions.get("year=2024"));
    }

    /**
     * Test for Bug 2 fix: Storage location template should be used in getProjectedPartitionsFromNames
     */
    @Test
    public void testGetProjectedPartitionsFromNamesWithStorageLocationTemplate() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.region.type", "enum",
                        "projection.region.values", "us,eu",
                        "projection.year.type", "integer",
                        "projection.year.range", "2023,2024",
                        "storage.location.template", "s3://bucket/data/${region}/${year}/"
                ),
                ImmutableList.of("region", "year")
        );

        List<String> partitionNames = Arrays.asList("region=us/year=2024");
        Map<String, Partition> partitions = service.getProjectedPartitionsFromNames(table, partitionNames);

        assertEquals(1, partitions.size());
        Partition partition = partitions.get("region=us/year=2024");
        assertNotNull(partition);

        // Verify that storage location template is used
        // Should be "s3://bucket/data/us/2024/" instead of "s3://bucket/table/region=us/year=2024"
        String location = partition.getFullPath();
        assertEquals("s3://bucket/data/us/2024/", location);
    }

    /**
     * Test for Bug 2 fix: Default location when no storage template is defined
     */
    @Test
    public void testGetProjectedPartitionsFromNamesWithoutStorageLocationTemplate() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.region.type", "enum",
                        "projection.region.values", "us,eu"
                ),
                ImmutableList.of("region")
        );

        List<String> partitionNames = Arrays.asList("region=us");
        Map<String, Partition> partitions = service.getProjectedPartitionsFromNames(table, partitionNames);

        assertEquals(1, partitions.size());
        Partition partition = partitions.get("region=us");
        assertNotNull(partition);

        // Without template, should use default format: baseLocation/partitionName
        String location = partition.getFullPath();
        assertEquals("s3://bucket/table/region=us", location);
    }

    /**
     * Test for Issue 3 improvement: INJECTED columns should throw clear error in getProjectedPartitionNames
     */
    @Test
    public void testGetProjectedPartitionNamesWithInjectedColumnThrowsError() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.user_id.type", "injected"
                ),
                ImmutableList.of("user_id")
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.getProjectedPartitionNames(table)
        );

        // Verify the error message is clear and helpful
        assertTrue(exception.getMessage().contains("INJECTED"));
        assertTrue(exception.getMessage().contains("user_id"));
        assertTrue(exception.getMessage().contains("WHERE clause"));
    }

    /**
     * Test for Issue 3: Mixed columns with INJECTED should still throw error
     */
    @Test
    public void testGetProjectedPartitionNamesWithMixedColumnsIncludingInjected() {
        HiveTable table = createMockHiveTable(
                ImmutableMap.of(
                        "projection.enabled", "true",
                        "projection.year.type", "integer",
                        "projection.year.range", "2020,2024",
                        "projection.user_id.type", "injected"
                ),
                ImmutableList.of("year", "user_id")
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.getProjectedPartitionNames(table)
        );

        assertTrue(exception.getMessage().contains("INJECTED"));
        assertTrue(exception.getMessage().contains("user_id"));
    }
}
