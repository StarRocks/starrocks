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

import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionProjectionTest {

    @Test
    public void testGetProjectedPartitionsSingleColumn() {
        Map<String, ColumnProjection> projections = new HashMap<>();
        projections.put("region", new EnumProjection("region", "us,eu"));

        PartitionProjection projection = new PartitionProjection(
                "s3://bucket/data",
                Optional.empty(),
                projections,
                Arrays.asList("region"),
                RemoteFileInputFormat.PARQUET,
                null
        );

        Map<String, Partition> partitions = projection.getProjectedPartitions(new HashMap<>());

        assertEquals(2, partitions.size());
        assertTrue(partitions.containsKey("region=us"));
        assertTrue(partitions.containsKey("region=eu"));

        // Check locations
        assertEquals("s3://bucket/data/region=us", partitions.get("region=us").getFullPath());
        assertEquals("s3://bucket/data/region=eu", partitions.get("region=eu").getFullPath());
    }

    @Test
    public void testGetProjectedPartitionsMultipleColumns() {
        Map<String, ColumnProjection> projections = new HashMap<>();
        projections.put("region", new EnumProjection("region", "us,eu"));
        projections.put("year", new IntegerProjection("year", "2020,2021", Optional.empty(), Optional.empty()));

        PartitionProjection projection = new PartitionProjection(
                "s3://bucket/data",
                Optional.empty(),
                projections,
                Arrays.asList("region", "year"),
                RemoteFileInputFormat.PARQUET,
                null
        );

        Map<String, Partition> partitions = projection.getProjectedPartitions(new HashMap<>());

        // 2 regions * 2 years = 4 partitions
        assertEquals(4, partitions.size());
        assertTrue(partitions.containsKey("region=us/year=2020"));
        assertTrue(partitions.containsKey("region=us/year=2021"));
        assertTrue(partitions.containsKey("region=eu/year=2020"));
        assertTrue(partitions.containsKey("region=eu/year=2021"));
    }

    @Test
    public void testGetProjectedPartitionsWithFilter() {
        Map<String, ColumnProjection> projections = new HashMap<>();
        projections.put("region", new EnumProjection("region", "us,eu,ap"));
        projections.put("year", new IntegerProjection("year", "2020,2025", Optional.empty(), Optional.empty()));

        PartitionProjection projection = new PartitionProjection(
                "s3://bucket/data",
                Optional.empty(),
                projections,
                Arrays.asList("region", "year"),
                RemoteFileInputFormat.PARQUET,
                null
        );

        Map<String, Optional<Object>> filters = new HashMap<>();
        filters.put("region", Optional.of("us"));
        filters.put("year", Optional.of(2023));

        Map<String, Partition> partitions = projection.getProjectedPartitions(filters);

        assertEquals(1, partitions.size());
        assertTrue(partitions.containsKey("region=us/year=2023"));
    }

    @Test
    public void testGetProjectedPartitionsWithStorageTemplate() {
        Map<String, ColumnProjection> projections = new HashMap<>();
        projections.put("region", new EnumProjection("region", "us"));
        projections.put("year", new IntegerProjection("year", "2024,2024", Optional.empty(), Optional.empty()));

        PartitionProjection projection = new PartitionProjection(
                "s3://bucket/data",
                Optional.of("s3://bucket/data/${region}/${year}/"),
                projections,
                Arrays.asList("region", "year"),
                RemoteFileInputFormat.PARQUET,
                null
        );

        Map<String, Partition> partitions = projection.getProjectedPartitions(new HashMap<>());

        assertEquals(1, partitions.size());
        Partition partition = partitions.get("region=us/year=2024");
        assertNotNull(partition);
        assertEquals("s3://bucket/data/us/2024/", partition.getFullPath());
    }

    @Test
    public void testGetProjectedPartitionsEmptyResult() {
        Map<String, ColumnProjection> projections = new HashMap<>();
        projections.put("region", new EnumProjection("region", "us,eu"));

        PartitionProjection projection = new PartitionProjection(
                "s3://bucket/data",
                Optional.empty(),
                projections,
                Arrays.asList("region"),
                RemoteFileInputFormat.PARQUET,
                null
        );

        Map<String, Optional<Object>> filters = new HashMap<>();
        filters.put("region", Optional.of("ap")); // Not in enum list

        Map<String, Partition> partitions = projection.getProjectedPartitions(filters);

        assertTrue(partitions.isEmpty());
    }

    @Test
    public void testGetProjectedPartitionsMissingProjection() {
        Map<String, ColumnProjection> projections = new HashMap<>();
        projections.put("region", new EnumProjection("region", "us,eu"));
        // Missing projection for "year"

        PartitionProjection projection = new PartitionProjection(
                "s3://bucket/data",
                Optional.empty(),
                projections,
                Arrays.asList("region", "year"),
                RemoteFileInputFormat.PARQUET,
                null
        );

        assertThrows(IllegalArgumentException.class, () ->
                projection.getProjectedPartitions(new HashMap<>()));
    }

    @Test
    public void testPartitionInputFormat() {
        Map<String, ColumnProjection> projections = new HashMap<>();
        projections.put("region", new EnumProjection("region", "us"));

        PartitionProjection projection = new PartitionProjection(
                "s3://bucket/data",
                Optional.empty(),
                projections,
                Arrays.asList("region"),
                RemoteFileInputFormat.ORC,
                null
        );

        Map<String, Partition> partitions = projection.getProjectedPartitions(new HashMap<>());

        Partition partition = partitions.get("region=us");
        assertEquals(RemoteFileInputFormat.ORC, partition.getFileFormat());
    }

    @Test
    public void testBaseLocationWithTrailingSlash() {
        Map<String, ColumnProjection> projections = new HashMap<>();
        projections.put("region", new EnumProjection("region", "us"));

        PartitionProjection projection = new PartitionProjection(
                "s3://bucket/data/",
                Optional.empty(),
                projections,
                Arrays.asList("region"),
                RemoteFileInputFormat.PARQUET,
                null
        );

        Map<String, Partition> partitions = projection.getProjectedPartitions(new HashMap<>());

        assertEquals("s3://bucket/data/region=us", partitions.get("region=us").getFullPath());
    }
}
