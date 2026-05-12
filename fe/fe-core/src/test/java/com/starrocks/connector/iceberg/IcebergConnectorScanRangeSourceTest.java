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
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.thrift.TExprNodeType;
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

        // _last_updated_sequence_number is passed as an extended column (with dataSequenceNumber value)
        // but NOT registered as an extended slot (so BE treats it as a reserved field).
        Assertions.assertTrue(
                hdfsScanRange.getExtended_columns().containsKey(lastUpdatedSequenceNumberSlot.getId().asInt()));
        // It should NOT be in the extendedColumnSlotIds list
        Assertions.assertFalse(scanRangeSource.getExtendedColumnSlotIds().contains(
                lastUpdatedSequenceNumberSlot.getId().asInt()));
    }

    @Test
    public void testBuildScanRangeReturnsNullRowIdWhenFirstRowIdMissing() throws Exception {
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
        THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(fileScanTask, fileScanTask.file(), partitionId);
        Assertions.assertFalse(hdfsScanRange.isSetFirst_row_id());
        Assertions.assertFalse(hdfsScanRange.getExtended_columns().containsKey(rowIdSlot.getId().asInt()));
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

    @Test
    public void testBuildScanRangeWithRowIdWhenFirstRowIdMissingReturnsNullAtReadTime() throws Exception {
        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(13));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        SlotDescriptor rowIdSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        rowIdSlot.setType(BIGINT);
        rowIdSlot.setColumn(new Column(IcebergTable.ROW_ID, BIGINT));
        localTupleDescriptor.addSlot(rowIdSlot);

        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_rowid_null", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableA, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableA.newScan().planFiles()).get(0);
        long partitionId = scanRangeSource.addPartition(fileScanTask);

        THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(fileScanTask, fileScanTask.file(), partitionId);
        Assertions.assertFalse(hdfsScanRange.isSetFirst_row_id());
        Assertions.assertFalse(hdfsScanRange.getExtended_columns().containsKey(rowIdSlot.getId().asInt()));
    }

    /**
     * Test that when late materialization adds _row_id along with _row_source_id and _scan_range_id,
     * and the file doesn't have firstRowId, FE still builds the scan range without first_row_id.
     * BE preserves the legacy lookup behavior by falling back to row_position for the internal fetch key.
     */
    @Test
    public void testBuildScanRangeWithLateMaterializationColumnsWithoutFirstRowIdUsesLegacyLookupKey() throws Exception {
        // Use v2 table which doesn't have firstRowId in files
        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(5));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));
        localTupleDescriptor.addSlot(dataSlot);

        // _row_id added by late materialization
        SlotDescriptor rowIdSlot = new SlotDescriptor(new SlotId(3), localTupleDescriptor);
        rowIdSlot.setType(BIGINT);
        rowIdSlot.setColumn(new Column(IcebergTable.ROW_ID, BIGINT));
        localTupleDescriptor.addSlot(rowIdSlot);

        // _row_source_id - indicates late materialization is active
        SlotDescriptor rowSourceIdSlot = new SlotDescriptor(new SlotId(4), localTupleDescriptor);
        rowSourceIdSlot.setType(INT);
        rowSourceIdSlot.setColumn(new Column("_row_source_id", INT));
        localTupleDescriptor.addSlot(rowSourceIdSlot);

        // _scan_range_id - indicates late materialization is active
        SlotDescriptor scanRangeIdSlot = new SlotDescriptor(new SlotId(5), localTupleDescriptor);
        scanRangeIdSlot.setType(INT);
        scanRangeIdSlot.setColumn(new Column("_scan_range_id", INT));
        localTupleDescriptor.addSlot(scanRangeIdSlot);

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
        Assertions.assertFalse(hdfsScanRange.isSetFirst_row_id());
    }

    /**
     * Test that when late materialization adds _row_id along with _row_source_id and _scan_range_id,
     * and the file has firstRowId (v3 table), the scan range should include first_row_id.
     */
    @Test
    public void testBuildScanRangeWithLateMaterializationColumnsSetsFirstRowIdWhenPresent() throws Exception {
        TestTables.TestTable mockedNativeTableV3 = create(SCHEMA_A, SPEC_A, "tv3_late", 3);

        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(6));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));
        localTupleDescriptor.addSlot(dataSlot);

        // _row_id added by late materialization
        SlotDescriptor rowIdSlot = new SlotDescriptor(new SlotId(3), localTupleDescriptor);
        rowIdSlot.setType(BIGINT);
        rowIdSlot.setColumn(new Column(IcebergTable.ROW_ID, BIGINT));
        localTupleDescriptor.addSlot(rowIdSlot);

        // _row_source_id - indicates late materialization is active
        SlotDescriptor rowSourceIdSlot = new SlotDescriptor(new SlotId(4), localTupleDescriptor);
        rowSourceIdSlot.setType(INT);
        rowSourceIdSlot.setColumn(new Column("_row_source_id", INT));
        localTupleDescriptor.addSlot(rowSourceIdSlot);

        // _scan_range_id - indicates late materialization is active
        SlotDescriptor scanRangeIdSlot = new SlotDescriptor(new SlotId(5), localTupleDescriptor);
        scanRangeIdSlot.setType(INT);
        scanRangeIdSlot.setColumn(new Column("_scan_range_id", INT));
        localTupleDescriptor.addSlot(scanRangeIdSlot);

        mockedNativeTableV3.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_v3_late", "iceberg_catalog",
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

    /**
     * Test that when both _row_id and _last_updated_sequence_number are requested (full row lineage query),
     * the scan range contains both: first_row_id is set, and _last_updated_sequence_number is in extended_columns
     * but not in extendedColumnSlotIds (so BE handles it as a reserved field that checks for physical column first).
     */
    @Test
    public void testBuildScanRangeWithFullRowLineageColumns() throws Exception {
        TestTables.TestTable mockedNativeTableV3 = create(SCHEMA_A, SPEC_A, "tv3_full_lineage", 3);

        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(10));
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

        SlotDescriptor seqSlot = new SlotDescriptor(new SlotId(4), localTupleDescriptor);
        seqSlot.setType(BIGINT);
        seqSlot.setColumn(new Column(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER, BIGINT));
        localTupleDescriptor.addSlot(seqSlot);

        mockedNativeTableV3.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_v3_full", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableV3, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableV3.newScan().planFiles()).get(0);
        long partitionId = scanRangeSource.addPartition(fileScanTask);

        if (fileScanTask.file().firstRowId() != null) {
            THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(
                    fileScanTask, fileScanTask.file(), partitionId);

            // _row_id: first_row_id should be set in scan range
            Assertions.assertTrue(hdfsScanRange.isSetFirst_row_id());
            Assertions.assertEquals(fileScanTask.file().firstRowId().longValue(),
                    hdfsScanRange.getFirst_row_id());

            // _last_updated_sequence_number: should be in extended_columns (fallback for BE)
            Assertions.assertTrue(
                    hdfsScanRange.getExtended_columns().containsKey(seqSlot.getId().asInt()));
            // But NOT registered as an extended slot (so BE treats it as reserved field)
            Assertions.assertFalse(
                    scanRangeSource.getExtendedColumnSlotIds().contains(seqSlot.getId().asInt()));

            // _row_id should NOT be in extended columns (it's a reserved field, not extended)
            Assertions.assertFalse(
                    hdfsScanRange.getExtended_columns().containsKey(rowIdSlot.getId().asInt()));
        }
    }

    @Test
    public void testBuildScanRangeWithFullRowLineageColumnsWhenFirstRowIdMissingKeepsSequenceNumber() throws Exception {
        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(14));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        SlotDescriptor rowIdSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        rowIdSlot.setType(BIGINT);
        rowIdSlot.setColumn(new Column(IcebergTable.ROW_ID, BIGINT));
        localTupleDescriptor.addSlot(rowIdSlot);

        SlotDescriptor seqSlot = new SlotDescriptor(new SlotId(3), localTupleDescriptor);
        seqSlot.setType(BIGINT);
        seqSlot.setColumn(new Column(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER, BIGINT));
        localTupleDescriptor.addSlot(seqSlot);

        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_full_null", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableA, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableA.newScan().planFiles()).get(0);
        long partitionId = scanRangeSource.addPartition(fileScanTask);
        THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(fileScanTask, fileScanTask.file(), partitionId);

        Assertions.assertFalse(hdfsScanRange.isSetFirst_row_id());
        Assertions.assertFalse(hdfsScanRange.getExtended_columns().containsKey(rowIdSlot.getId().asInt()));
        Assertions.assertTrue(hdfsScanRange.getExtended_columns().containsKey(seqSlot.getId().asInt()));
        var texpr = hdfsScanRange.getExtended_columns().get(seqSlot.getId().asInt());
        Assertions.assertEquals(TExprNodeType.INT_LITERAL, texpr.getNodes().get(0).getNode_type());
        Assertions.assertEquals(fileScanTask.file().dataSequenceNumber().longValue(),
                texpr.getNodes().get(0).getInt_literal().getValue());
    }

    /**
     * Test that _last_updated_sequence_number extended column value matches the file's dataSequenceNumber.
     * This is the fallback value used by BE when the physical column is not present (non-compacted files).
     */
    @Test
    public void testLastUpdatedSequenceNumberValueMatchesDataSequenceNumber() throws Exception {
        TestTables.TestTable mockedNativeTableV3 = create(SCHEMA_A, SPEC_A, "tv3_seq_val", 3);

        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(11));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        SlotDescriptor seqSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        seqSlot.setType(BIGINT);
        seqSlot.setColumn(new Column(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER, BIGINT));
        localTupleDescriptor.addSlot(seqSlot);

        mockedNativeTableV3.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_v3_seqval", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableV3, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableV3.newScan().planFiles()).get(0);
        long partitionId = scanRangeSource.addPartition(fileScanTask);
        THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(
                fileScanTask, fileScanTask.file(), partitionId);

        // The extended column value should contain the file's dataSequenceNumber
        Assertions.assertTrue(
                hdfsScanRange.getExtended_columns().containsKey(seqSlot.getId().asInt()));

        // Verify the actual value: the TExpr should have an INT_LITERAL node
        // with the file's dataSequenceNumber
        var texpr = hdfsScanRange.getExtended_columns().get(seqSlot.getId().asInt());
        Assertions.assertFalse(texpr.getNodes().isEmpty());
        long expectedSeqNum = fileScanTask.file().dataSequenceNumber();
        Assertions.assertEquals(expectedSeqNum, texpr.getNodes().get(0).getInt_literal().getValue());
    }

    /**
     * Test that _row_id and _last_updated_sequence_number with late materialization columns
     * on a V3 table correctly sets first_row_id and passes sequence number as extended column.
     * This simulates what happens when StarRocks queries a V3 table that has been compacted
     * (the BE will check for physical columns first, then fall back to these values).
     */
    @Test
    public void testBuildScanRangeV3WithLateMaterializationAndRowLineage() throws Exception {
        TestTables.TestTable mockedNativeTableV3 = create(SCHEMA_A, SPEC_A, "tv3_late_lineage", 3);

        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(12));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        // _row_id
        SlotDescriptor rowIdSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        rowIdSlot.setType(BIGINT);
        rowIdSlot.setColumn(new Column(IcebergTable.ROW_ID, BIGINT));
        localTupleDescriptor.addSlot(rowIdSlot);

        // _last_updated_sequence_number
        SlotDescriptor seqSlot = new SlotDescriptor(new SlotId(3), localTupleDescriptor);
        seqSlot.setType(BIGINT);
        seqSlot.setColumn(new Column(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER, BIGINT));
        localTupleDescriptor.addSlot(seqSlot);

        // Late materialization columns
        SlotDescriptor rowSourceIdSlot = new SlotDescriptor(new SlotId(4), localTupleDescriptor);
        rowSourceIdSlot.setType(INT);
        rowSourceIdSlot.setColumn(new Column("_row_source_id", INT));
        localTupleDescriptor.addSlot(rowSourceIdSlot);

        SlotDescriptor scanRangeIdSlot = new SlotDescriptor(new SlotId(5), localTupleDescriptor);
        scanRangeIdSlot.setType(INT);
        scanRangeIdSlot.setColumn(new Column("_scan_range_id", INT));
        localTupleDescriptor.addSlot(scanRangeIdSlot);

        mockedNativeTableV3.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_v3_late_lin", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableV3, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableV3.newScan().planFiles()).get(0);
        long partitionId = scanRangeSource.addPartition(fileScanTask);

        // Should not throw even with late materialization columns
        THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(
                fileScanTask, fileScanTask.file(), partitionId);

        if (fileScanTask.file().firstRowId() != null) {
            Assertions.assertTrue(hdfsScanRange.isSetFirst_row_id());
        }

        // _last_updated_sequence_number should be in extended_columns but NOT in extendedColumnSlotIds
        Assertions.assertTrue(
                hdfsScanRange.getExtended_columns().containsKey(seqSlot.getId().asInt()));
        Assertions.assertFalse(
                scanRangeSource.getExtendedColumnSlotIds().contains(seqSlot.getId().asInt()));
    }

    /**
     * Test that when _row_id is present with only _row_source_id (partial late materialization),
     * and the file has no firstRowId, FE still leaves first_row_id unset so BE can fall back to row_position.
     */
    @Test
    public void testBuildScanRangeWithOnlyRowSourceIdWithoutFirstRowIdUsesLegacyLookupKey() throws Exception {
        // Use v2 table which doesn't have firstRowId in files
        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(7));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), localTupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        localTupleDescriptor.addSlot(idSlot);

        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), localTupleDescriptor);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));
        localTupleDescriptor.addSlot(dataSlot);

        // _row_id added by late materialization
        SlotDescriptor rowIdSlot = new SlotDescriptor(new SlotId(3), localTupleDescriptor);
        rowIdSlot.setType(BIGINT);
        rowIdSlot.setColumn(new Column(IcebergTable.ROW_ID, BIGINT));
        localTupleDescriptor.addSlot(rowIdSlot);

        // Only _row_source_id
        SlotDescriptor rowSourceIdSlot = new SlotDescriptor(new SlotId(4), localTupleDescriptor);
        rowSourceIdSlot.setType(INT);
        rowSourceIdSlot.setColumn(new Column("_row_source_id", INT));
        localTupleDescriptor.addSlot(rowSourceIdSlot);

        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_partial", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableA, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableA.newScan().planFiles()).get(0);
        long partitionId = scanRangeSource.addPartition(fileScanTask);

        THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(fileScanTask, fileScanTask.file(), partitionId);
        Assertions.assertFalse(hdfsScanRange.isSetFirst_row_id());
    }
}
