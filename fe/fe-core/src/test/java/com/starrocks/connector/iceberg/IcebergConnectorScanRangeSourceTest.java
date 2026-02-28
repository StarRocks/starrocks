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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.BucketProperty;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.thrift.THdfsScanRange;
import org.apache.iceberg.FileScanTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarcharType.VARCHAR;

/**
 * Test cases for IcebergConnectorScanRangeSource focusing on initBucketInfo and extractBucketId methods
 */
public class IcebergConnectorScanRangeSourceTest extends TableTestBase {
    private TupleDescriptor tupleDescriptor;

    @BeforeEach
    public void setUp() {
        // Setup tuple descriptor
        tupleDescriptor = new TupleDescriptor(new TupleId(1));
        
        // Setup slot descriptors
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), tupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        
        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), tupleDescriptor);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));
        
        tupleDescriptor.addSlot(idSlot);
        tupleDescriptor.addSlot(dataSlot);
    }

    @Test
    public void testExtractBucketIdFromTask() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("k1", INT));
        schema.add(new Column("k2", VARCHAR));
        mockedNativeTable2Bucket.newFastAppend().appendFile(FILE_J_1).appendFile(FILE_J_2).commit();
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTable2Bucket, Maps.newHashMap());
        Assertions.assertTrue(icebergTable.hasBucketProperties());
        List<BucketProperty> bucketProperties = icebergTable.getBucketProperties();

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, tupleDescriptor, Optional.of(bucketProperties),
                PartitionIdGenerator.of(), false, false);

        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTable2Bucket.newScan().planFiles());
        Assertions.assertEquals(2, fileScanTasks.size());
        for (FileScanTask fileScanTask : fileScanTasks) {
            int mappingId = scanRangeSource.extractBucketId(fileScanTask);
            if (fileScanTask.file().location().endsWith("data-j1.parquet")) {
                // 1 * (64 + 1) + 1 data-j1
                Assertions.assertEquals((BUCKETS_NUMBER2 + 1) + 1, mappingId);
            } else {
                // 2 * (64 + 1) + 1 data-j2
                Assertions.assertEquals(2 * (BUCKETS_NUMBER2 + 1) + 1, mappingId);
            }
        }
    }

    @Test
    public void testExtractBucketIdFromTaskOnlyOneBucketProperty() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("k1", INT));
        schema.add(new Column("k2", VARCHAR));
        mockedNativeTable2Bucket.newFastAppend().appendFile(FILE_J_1).appendFile(FILE_J_2).commit();
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTable2Bucket, Maps.newHashMap());
        Assertions.assertTrue(icebergTable.hasBucketProperties());
        List<BucketProperty> bucketProperties = icebergTable.getBucketProperties();
        List<BucketProperty> oneBucketProperties = List.of(bucketProperties.get(0));

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, tupleDescriptor, Optional.of(oneBucketProperties),
                PartitionIdGenerator.of(), false, false);

        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTable2Bucket.newScan().planFiles());
        Assertions.assertEquals(2, fileScanTasks.size());
        for (FileScanTask fileScanTask : fileScanTasks) {
            int mappingId = scanRangeSource.extractBucketId(fileScanTask);
            if (fileScanTask.file().location().endsWith("data-j1.parquet")) {
                System.out.println("J1 mapping id: " + mappingId);
                // 1 data-j1
                Assertions.assertEquals(1, mappingId);
            } else {
                // 2 data-j2
                Assertions.assertEquals(2, mappingId);
            }
        }
    }

    @Test
    public void testSamePartitionIdForSamePartitionKeysAcrossDifferentSources() throws Exception {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("k1", INT));
        schema.add(new Column("k2", VARCHAR));
        mockedNativeTable2Bucket.newFastAppend().appendFile(FILE_J_1).appendFile(FILE_J_2).commit();
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema,
                mockedNativeTable2Bucket, Maps.newHashMap());
        Assertions.assertTrue(icebergTable.hasBucketProperties());
        List<BucketProperty> bucketProperties = icebergTable.getBucketProperties();
        List<BucketProperty> oneBucketProperties = List.of(bucketProperties.get(0));
        // Use the same partition key values for both
        PartitionIdGenerator partitionIdGenerator = PartitionIdGenerator.of();
        IcebergConnectorScanRangeSource scanRangeSource1 = new IcebergConnectorScanRangeSource(
                icebergTable, RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, tupleDescriptor,
                Optional.of(oneBucketProperties), partitionIdGenerator, false, false);
        IcebergConnectorScanRangeSource scanRangeSource2 = new IcebergConnectorScanRangeSource(
                icebergTable, RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, tupleDescriptor,
                Optional.of(oneBucketProperties), partitionIdGenerator, false, false);
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTable2Bucket.newScan().planFiles());
        Assertions.assertFalse(fileScanTasks.isEmpty());
        for (FileScanTask fileScanTask : fileScanTasks) {
            // Simulate partition id generation for the same partition key values
            long partitionId1 = scanRangeSource1.addPartition(fileScanTask);
            long partitionId2 = scanRangeSource2.addPartition(fileScanTask);
            Assertions.assertEquals(partitionId1, partitionId2, "Partition IDs should " +
                    "be the same for the same partition keys and values");
        }
    }

    @Test
    public void testToFullSchemasContainsV3RowLineageColumns() {
        TestTables.TestTable mockedNativeTableV3 = create(SCHEMA_A, SPEC_A, "tv3", 3);
        List<Column> fullSchema = IcebergApiConverter.toFullSchemas(
                mockedNativeTableV3.schema(), mockedNativeTableV3);

        Assertions.assertTrue(fullSchema.stream().anyMatch(
                column -> column.getName().equals(IcebergTable.ROW_ID) && column.isHidden()));
        Assertions.assertTrue(fullSchema.stream().anyMatch(
                column -> column.getName().equals(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER) && column.isHidden()));
    }

    @Test
    public void testToFullSchemasV2NotContainsLastUpdatedSequenceNumber() {
        List<Column> fullSchema = IcebergApiConverter.toFullSchemas(
                mockedNativeTableA.schema(), mockedNativeTableA);

        Assertions.assertFalse(fullSchema.stream().anyMatch(
                column -> column.getName().equals(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER)));
    }

    @Test
    public void testBuildScanRangeContainsLastUpdatedSequenceNumberExtendedColumn() throws Exception {
        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(2));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));
        localTupleDescriptor.addSlot(dataSlot);

        SlotDescriptor lastUpdatedSequenceNumberSlot = new SlotDescriptor(new SlotId(3), localTupleDescriptor);
        lastUpdatedSequenceNumberSlot.setType(BIGINT);
        lastUpdatedSequenceNumberSlot.setColumn(new Column(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER, BIGINT));
        localTupleDescriptor.addSlot(lastUpdatedSequenceNumberSlot);

        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableA, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableA.newScan().planFiles()).get(0);
        long partitionId = scanRangeSource.addPartition(fileScanTask);
        THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(fileScanTask, fileScanTask.file(), partitionId);

        Assertions.assertTrue(
                hdfsScanRange.getExtended_columns().containsKey(lastUpdatedSequenceNumberSlot.getId().asInt()));
        Assertions.assertTrue(
                scanRangeSource.getExtendedColumnSlotIds().contains(lastUpdatedSequenceNumberSlot.getId().asInt()));
    }

    @Test
    public void testBuildScanRangeFailFastWhenRowIdWithoutFirstRowId() throws Exception {
        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(3));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));
        localTupleDescriptor.addSlot(dataSlot);

        SlotDescriptor rowIdSlot = new SlotDescriptor(new SlotId(3), localTupleDescriptor);
        rowIdSlot.setType(BIGINT);
        rowIdSlot.setColumn(new Column(IcebergTable.ROW_ID, BIGINT));
        localTupleDescriptor.addSlot(rowIdSlot);

        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableA, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableA.newScan().planFiles()).get(0);
        long partitionId = scanRangeSource.addPartition(fileScanTask);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> scanRangeSource.buildScanRange(fileScanTask, fileScanTask.file(), partitionId));
    }

    @Test
    public void testBuildScanRangeWithRowIdWhenFirstRowIdPresent() throws Exception {
        TestTables.TestTable mockedNativeTableV3 = create(SCHEMA_A, SPEC_A, "tv3_rowid", 3);

        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(4));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));
        localTupleDescriptor.addSlot(dataSlot);

        SlotDescriptor rowIdSlot = new SlotDescriptor(new SlotId(3), localTupleDescriptor);
        rowIdSlot.setType(BIGINT);
        rowIdSlot.setColumn(new Column(IcebergTable.ROW_ID, BIGINT));
        localTupleDescriptor.addSlot(rowIdSlot);

        mockedNativeTableV3.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_v3", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableV3, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableV3.newScan().planFiles()).get(0);
        long partitionId = scanRangeSource.addPartition(fileScanTask);

        if (fileScanTask.file().firstRowId() != null) {
            THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(
                    fileScanTask, fileScanTask.file(), partitionId);
            Assertions.assertTrue(hdfsScanRange.isSetFirst_row_id());
            Assertions.assertEquals(fileScanTask.file().firstRowId().longValue(),
                    hdfsScanRange.getFirst_row_id());
        }
    }
}
