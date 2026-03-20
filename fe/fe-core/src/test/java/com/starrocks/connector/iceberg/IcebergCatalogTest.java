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

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.StructProjection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class IcebergCatalogTest {
    @Test
    public void testGetPartitionsSkipsMissingSpecId() {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class, Mockito.CALLS_REAL_METHODS);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec currentSpec = Mockito.mock(PartitionSpec.class);
        Mockito.when(currentSpec.isUnpartitioned()).thenReturn(false);
        Mockito.when(nativeTable.spec()).thenReturn(currentSpec);
        // Use specId 1 which exists in specs map
        Mockito.when(nativeTable.specs()).thenReturn(ImmutableMap.of(1, currentSpec));
        Mockito.when(nativeTable.name()).thenReturn("db.tbl");

        IcebergTable icebergTable = new IcebergTable(1, "srTable", "iceberg_catalog",
                "resource", "db", "tbl", "", Lists.newArrayList(), nativeTable, Maps.newHashMap());

        PartitionsTable partitionsTable = Mockito.mock(PartitionsTable.class);
        TableScan scan = Mockito.mock(TableScan.class);
        Mockito.when(partitionsTable.newScan()).thenReturn(scan);

        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataTask dataTask = Mockito.mock(DataTask.class);
        StructLike row = Mockito.mock(StructLike.class);
        StructProjection partitionData = Mockito.mock(StructProjection.class);
        Mockito.when(task.asDataTask()).thenReturn(dataTask);
        Mockito.when(row.get(0, StructProjection.class)).thenReturn(partitionData);
        // Use valid specId 1 instead of 99
        Mockito.when(row.get(1, Integer.class)).thenReturn(1);
        CloseableIterable<StructLike> rowIterable = CloseableIterable.withNoopClose(Lists.newArrayList(row));
        Mockito.when(dataTask.rows()).thenReturn(rowIterable);

        CloseableIterable<FileScanTask> taskIterable = CloseableIterable.withNoopClose(Lists.newArrayList(task));
        Mockito.when(scan.planFiles()).thenReturn(taskIterable);

        // Use Mockito.mockStatic instead of JMockit's MockUp
        try (MockedStatic<MetadataTableUtils> metadataTableUtils = Mockito.mockStatic(MetadataTableUtils.class)) {
            metadataTableUtils.when(() -> MetadataTableUtils.createMetadataTableInstance(
                    nativeTable, MetadataTableType.PARTITIONS)).thenReturn(partitionsTable);

            // Mock PartitionUtil.convertIcebergPartitionToPartitionName
            try (MockedStatic<com.starrocks.connector.PartitionUtil> partitionUtil = 
                    Mockito.mockStatic(com.starrocks.connector.PartitionUtil.class)) {
                partitionUtil.when(() -> com.starrocks.connector.PartitionUtil.convertIcebergPartitionToPartitionName(
                        Mockito.any(PartitionSpec.class), Mockito.any(StructProjection.class)))
                        .thenReturn("dt=2023-12-01");

                Map<String, Partition> partitions = catalog.getPartitions(icebergTable, -1, null);
                // Since we use valid specId, partition should be returned
                Assertions.assertFalse(partitions.isEmpty());
                Assertions.assertNotNull(partitions.get("dt=2023-12-01"));
            }
        }
    }

    @Test
    public void testGetPartitionsAcceptsIntegerStatsColumns() {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class, Mockito.CALLS_REAL_METHODS);
        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec currentSpec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(currentSpec.isUnpartitioned()).thenReturn(false);
        Mockito.when(nativeTable.spec()).thenReturn(currentSpec);
        Mockito.when(nativeTable.specs()).thenReturn(ImmutableMap.of(1, currentSpec));
        Mockito.when(nativeTable.name()).thenReturn("db.tbl");
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(10L);

        IcebergTable icebergTable = new IcebergTable(1, "srTable", "iceberg_catalog",
                "resource", "db", "tbl", "", Lists.newArrayList(), nativeTable, Maps.newHashMap());

        PartitionsTable partitionsTable = Mockito.mock(PartitionsTable.class);
        TableScan scan = Mockito.mock(TableScan.class);
        Mockito.when(partitionsTable.newScan()).thenReturn(scan);

        FileScanTask task = Mockito.mock(FileScanTask.class);
        DataTask dataTask = Mockito.mock(DataTask.class);
        StructLike row = Mockito.mock(StructLike.class);
        StructProjection partitionData = Mockito.mock(StructProjection.class);
        Mockito.when(task.asDataTask()).thenReturn(dataTask);
        Mockito.when(row.get(IcebergCatalog.PARTITION_DATA_COLUMN_INDEX, StructProjection.class)).thenReturn(partitionData);
        Mockito.when(row.get(IcebergCatalog.SPEC_ID_COLUMN_INDEX, Integer.class)).thenReturn(1);
        Mockito.when(row.get(IcebergCatalog.PARTITION_LAST_UPDATED_AT_COLUMN_INDEX, Long.class)).thenReturn(123L);
        Mockito.when(row.get(IcebergCatalog.PARTITION_RECORD_COUNT_COLUMN_INDEX, Number.class)).thenReturn(7);
        Mockito.when(row.get(IcebergCatalog.PARTITION_FILE_COUNT_COLUMN_INDEX, Number.class)).thenReturn(2);
        Mockito.when(row.get(IcebergCatalog.PARTITION_TOTAL_DATA_FILE_SIZE_COLUMN_INDEX, Number.class)).thenReturn(99L);
        Mockito.when(row.get(IcebergCatalog.PARTITION_POSITION_DELETE_RECORD_COUNT_COLUMN_INDEX, Number.class)).thenReturn(0);
        Mockito.when(row.get(IcebergCatalog.PARTITION_POSITION_DELETE_FILE_COUNT_COLUMN_INDEX, Number.class)).thenReturn(0);
        Mockito.when(row.get(IcebergCatalog.PARTITION_EQUALITY_DELETE_RECORD_COUNT_COLUMN_INDEX, Number.class)).thenReturn(0);
        Mockito.when(row.get(IcebergCatalog.PARTITION_EQUALITY_DELETE_FILE_COUNT_COLUMN_INDEX, Number.class)).thenReturn(0);

        CloseableIterable<StructLike> rowIterable = CloseableIterable.withNoopClose(Lists.newArrayList(row));
        Mockito.when(dataTask.rows()).thenReturn(rowIterable);

        CloseableIterable<FileScanTask> taskIterable = CloseableIterable.withNoopClose(Lists.newArrayList(task));
        Mockito.when(scan.planFiles()).thenReturn(taskIterable);

        // Use Mockito.mockStatic instead of JMockit's MockUp
        try (MockedStatic<MetadataTableUtils> metadataTableUtils = Mockito.mockStatic(MetadataTableUtils.class)) {
            metadataTableUtils.when(() -> MetadataTableUtils.createMetadataTableInstance(
                    nativeTable, MetadataTableType.PARTITIONS)).thenReturn(partitionsTable);

            // Mock PartitionUtil.convertIcebergPartitionToPartitionName
            try (MockedStatic<com.starrocks.connector.PartitionUtil> partitionUtil = 
                    Mockito.mockStatic(com.starrocks.connector.PartitionUtil.class)) {
                partitionUtil.when(() -> com.starrocks.connector.PartitionUtil.convertIcebergPartitionToPartitionName(
                        Mockito.any(PartitionSpec.class), Mockito.any(StructProjection.class)))
                        .thenReturn("dt=2023-12-04");

                Map<String, Partition> partitions = catalog.getPartitions(icebergTable, -1, null);
                Partition partition = partitions.get("dt=2023-12-04");
                Assertions.assertNotNull(partition);
                Assertions.assertTrue(partition.getVersion() >= 0);
            }
        }
    }
}
