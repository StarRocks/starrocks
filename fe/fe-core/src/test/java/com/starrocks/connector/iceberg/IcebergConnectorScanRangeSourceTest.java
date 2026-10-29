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
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.BucketProperty;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TIcebergDeleteFile;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.DateType;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    // A position delete file is sent with its own format, which may differ from its data file's.
    @Test
    public void testPositionDeleteFileFormat() {
        DataFile orcFile = DataFiles.builder(SPEC_A)
                .withPath("/path/to/data-a-orc.orc")
                .withFormat(FileFormat.ORC)
                .withFileSizeInBytes(10)
                .withPartitionPath("data_bucket=0")
                .withRecordCount(2)
                .build();
        DeleteFile parquetDeletes = FileMetadata.deleteFileBuilder(SPEC_A).ofPositionDeletes()
                .withPath("/path/to/data-a-orc-deletes.parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10).withRecordCount(1).withPartitionPath("data_bucket=0").build();
        TestTables.TestTable v2Table = create(SCHEMA_A, SPEC_A, "ta_v2_orc_pos_delete", 2);
        v2Table.newFastAppend().appendFile(orcFile).commit();
        v2Table.newRowDelta().addDeletes(parquetDeletes).commit();
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog", "resource", "db",
                "table", "", Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR)), v2Table,
                Maps.newHashMap());
        List<RemoteFileInfo> infos = new ArrayList<>();
        v2Table.newScan().planFiles().forEach(task -> infos.add(new IcebergRemoteFileInfo(task)));
        IcebergConnectorScanRangeSource source = new IcebergConnectorScanRangeSource(icebergTable,
                new RemoteFileInfoDefaultSource(infos), IcebergMORParams.EMPTY, tupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false, false);

        THdfsScanRange range = source.getSourceOutputs(100).get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(THdfsFileFormat.ORC, range.getFile_format());
        Assertions.assertEquals(THdfsFileFormat.PARQUET, range.getDelete_files().get(0).getFile_format());
    }

    private static DeleteFile positionDeletes(String path, String referencedDataFile, String lowerPath,
                                              String upperPath) {
        FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(SPEC_A).ofPositionDeletes()
                .withPath(path).withFormat(FileFormat.PARQUET).withFileSizeInBytes(10).withRecordCount(1)
                .withPartitionPath("data_bucket=0");
        if (referencedDataFile != null) {
            builder.withReferencedDataFile(referencedDataFile);
        }
        if (lowerPath != null) {
            int filePathId = MetadataColumns.DELETE_FILE_PATH.fieldId();
            builder.withMetrics(new Metrics(1L, null, null, null, null,
                    Map.of(filePathId, Conversions.toByteBuffer(Types.StringType.get(), lowerPath)),
                    Map.of(filePathId, Conversions.toByteBuffer(Types.StringType.get(), upperPath))));
        }
        return builder.build();
    }

    @Test
    public void testReferencesOnlyDataFile() {
        String a = FILE_A.location();
        String b = FILE_A_2.location();
        // referenced_data_file decides when it is set
        Assertions.assertTrue(IcebergConnectorScanRangeSource.referencesOnlyDataFile(
                positionDeletes("/d1.parquet", a, null, null), FILE_A));
        Assertions.assertFalse(IcebergConnectorScanRangeSource.referencesOnlyDataFile(
                positionDeletes("/d2.parquet", b, null, null), FILE_A));
        // else the file_path bounds must both be this data file's path
        Assertions.assertTrue(IcebergConnectorScanRangeSource.referencesOnlyDataFile(
                positionDeletes("/d3.parquet", null, a, a), FILE_A));
        Assertions.assertFalse(IcebergConnectorScanRangeSource.referencesOnlyDataFile(
                positionDeletes("/d4.parquet", null, a, b), FILE_A));
        Assertions.assertFalse(IcebergConnectorScanRangeSource.referencesOnlyDataFile(
                positionDeletes("/d5.parquet", null, b, b), FILE_A));
        // a truncated bound is a prefix of the path
        String prefix = a.substring(0, a.length() - 1);
        Assertions.assertFalse(IcebergConnectorScanRangeSource.referencesOnlyDataFile(
                positionDeletes("/d6.parquet", null, prefix, prefix), FILE_A));
        // no bounds: unknown
        Assertions.assertFalse(IcebergConnectorScanRangeSource.referencesOnlyDataFile(
                positionDeletes("/d7.parquet", null, null, null), FILE_A));
    }

    @Test
    public void testPositionDeleteFileReferencesOnlyDataFileIsSent() {
        TestTables.TestTable v2Table = create(SCHEMA_A, SPEC_A, "ta_v2_pos_delete_single_file", 2);
        v2Table.newFastAppend().appendFile(FILE_A).commit();
        v2Table.newRowDelta()
                .addDeletes(positionDeletes("/path/to/a-file-scoped-deletes.parquet", FILE_A.location(), null, null))
                .addDeletes(positionDeletes("/path/to/a-partition-deletes.parquet", null, null, null))
                .commit();
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog", "resource", "db",
                "table", "", Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR)), v2Table,
                Maps.newHashMap());
        List<RemoteFileInfo> infos = new ArrayList<>();
        v2Table.newScan().planFiles().forEach(task -> infos.add(new IcebergRemoteFileInfo(task)));
        IcebergConnectorScanRangeSource source = new IcebergConnectorScanRangeSource(icebergTable,
                new RemoteFileInfoDefaultSource(infos), IcebergMORParams.EMPTY, tupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false, false);

        THdfsScanRange range = source.getSourceOutputs(100).get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(2, range.getDelete_files().size());
        for (TIcebergDeleteFile deleteFile : range.getDelete_files()) {
            Assertions.assertEquals(deleteFile.getFull_path().endsWith("a-file-scoped-deletes.parquet"),
                    deleteFile.isReferences_only_data_file(), deleteFile.getFull_path());
        }
    }

    @Test
    public void testBucketsOfOneIdentityPartitionShareAPartitionId() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_B).identity("k2").bucket("k1", 4).build();
        TestTables.TestTable nativeTable = create(SCHEMA_B, spec, "t_identity_bucket", 2);
        DataFile k2Is1Bucket0 = DataFiles.builder(spec).withPath("/path/to/k2=1/k1_bucket=0/a.parquet")
                .withFileSizeInBytes(10).withPartitionPath("k2=1/k1_bucket=0").withRecordCount(1).build();
        DataFile k2Is1Bucket1 = DataFiles.builder(spec).withPath("/path/to/k2=1/k1_bucket=1/b.parquet")
                .withFileSizeInBytes(10).withPartitionPath("k2=1/k1_bucket=1").withRecordCount(1).build();
        DataFile k2Is2Bucket0 = DataFiles.builder(spec).withPath("/path/to/k2=2/k1_bucket=0/c.parquet")
                .withFileSizeInBytes(10).withPartitionPath("k2=2/k1_bucket=0").withRecordCount(1).build();
        nativeTable.newFastAppend().appendFile(k2Is1Bucket0).appendFile(k2Is1Bucket1).appendFile(k2Is2Bucket0).commit();

        TupleDescriptor desc = new TupleDescriptor(new TupleId(5));
        SlotDescriptor k1Slot = new SlotDescriptor(new SlotId(1), desc);
        k1Slot.setType(INT);
        k1Slot.setColumn(new Column("k1", INT));
        desc.addSlot(k1Slot);
        SlotDescriptor k2Slot = new SlotDescriptor(new SlotId(2), desc);
        k2Slot.setType(INT);
        k2Slot.setColumn(new Column("k2", INT));
        desc.addSlot(k2Slot);

        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog", "resource", "db",
                "table", "", Lists.newArrayList(new Column("k1", INT), new Column("k2", INT)), nativeTable,
                Maps.newHashMap());
        PartitionIdGenerator partitionIdGenerator = Mockito.spy(PartitionIdGenerator.of());
        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, desc, Optional.empty(),
                partitionIdGenerator, false, false);

        Map<String, Long> idByFile = Maps.newHashMap();
        Map<String, Long> k2ByFile = Maps.newHashMap();
        Map<String, THdfsPartition> valueByFile = Maps.newHashMap();
        for (FileScanTask task : nativeTable.newScan().planFiles()) {
            long partitionId = scanRangeSource.addPartition(task);
            // a second call hits the cache and returns the same id
            Assertions.assertEquals(partitionId, scanRangeSource.addPartition(task));
            THdfsScanRange range = scanRangeSource.buildScanRange(task, task.file(), partitionId);
            idByFile.put(task.file().location(), partitionId);
            valueByFile.put(task.file().location(), range.getPartition_value());
            k2ByFile.put(task.file().location(),
                    range.getPartition_value().getPartition_key_exprs().get(0).getNodes().get(0).getInt_literal().getValue());
        }

        Assertions.assertEquals(idByFile.get(k2Is1Bucket0.location()), idByFile.get(k2Is1Bucket1.location()));
        Assertions.assertNotEquals(idByFile.get(k2Is1Bucket0.location()), idByFile.get(k2Is2Bucket0.location()));
        Assertions.assertEquals(1L, k2ByFile.get(k2Is1Bucket0.location()));
        Assertions.assertEquals(1L, k2ByFile.get(k2Is1Bucket1.location()));
        Assertions.assertEquals(2L, k2ByFile.get(k2Is2Bucket0.location()));
        // files of one partition id share one thrift partition value
        Assertions.assertSame(valueByFile.get(k2Is1Bucket0.location()), valueByFile.get(k2Is1Bucket1.location()));
        Assertions.assertNotSame(valueByFile.get(k2Is1Bucket0.location()), valueByFile.get(k2Is2Bucket0.location()));
        // the partition count still counts every (k2, bucket) combination
        Assertions.assertEquals(3, scanRangeSource.selectedPartitionCount());
        // the PartitionKey (and its id) is built once per identity partition, not once per bucket
        Mockito.verify(partitionIdGenerator, Mockito.times(2)).getOrGenerate(Mockito.any());
    }

    private static DataFile dataFile(PartitionSpec spec, String path, Object... partitionValues) {
        PartitionData partition = new PartitionData(spec.partitionType());
        for (int i = 0; i < partitionValues.length; i++) {
            partition.put(i, partitionValues[i]);
        }
        return DataFiles.builder(spec).withPath(path).withFileSizeInBytes(10).withPartition(partition)
                .withRecordCount(1).build();
    }

    @Test
    public void testIdentityPartitionCacheMatchesUncachedPartitionKeys() throws Exception {
        // string and date identity fields with NULLs beside a bucket, across two specs
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "s", Types.StringType.get()),
                Types.NestedField.optional(3, "d", Types.DateType.get()));
        PartitionSpec bucketSpec = PartitionSpec.builderFor(schema).identity("s").identity("d").bucket("id", 4).build();
        TestTables.TestTable nativeTable = create(schema, bucketSpec, "t_identity_cache", 2);
        // (s, d as days since epoch, id_bucket); 19723 = 2024-01-01
        Object[][] bucketPartitions = {
                {"a", 19723, 0}, {"a", 19723, 1}, {"a", 19724, 0}, {"b", 19723, 2},
                {null, 19723, 0}, {null, 19723, 3}, {"a", null, 1}, {null, null, 1}};
        AppendFiles append = nativeTable.newFastAppend();
        for (int i = 0; i < bucketPartitions.length; i++) {
            append.appendFile(dataFile(bucketSpec, "/path/to/bucket-spec-" + i + ".parquet", bucketPartitions[i]));
        }
        append.commit();

        nativeTable.updateSpec().removeField("id_bucket").commit();
        PartitionSpec identitySpec = nativeTable.spec();
        Assertions.assertNotEquals(bucketSpec.specId(), identitySpec.specId());
        nativeTable.newFastAppend()
                .appendFile(dataFile(identitySpec, "/path/to/identity-spec-0.parquet", "a", 19723))
                .appendFile(dataFile(identitySpec, "/path/to/identity-spec-1.parquet", null, 19725))
                .commit();

        TupleDescriptor desc = new TupleDescriptor(new TupleId(5));
        SlotDescriptor sSlot = new SlotDescriptor(new SlotId(2), desc);
        sSlot.setType(VARCHAR);
        sSlot.setColumn(new Column("s", VARCHAR));
        desc.addSlot(sSlot);
        SlotDescriptor dSlot = new SlotDescriptor(new SlotId(3), desc);
        dSlot.setType(DateType.DATE);
        dSlot.setColumn(new Column("d", DateType.DATE));
        desc.addSlot(dSlot);
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog", "resource", "db",
                "table", "", Lists.newArrayList(new Column("id", INT), new Column("s", VARCHAR),
                        new Column("d", DateType.DATE)), nativeTable, Maps.newHashMap());
        PartitionIdGenerator partitionIdGenerator = PartitionIdGenerator.of();
        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, desc, Optional.empty(),
                partitionIdGenerator, false, false);

        List<FileScanTask> tasks = Lists.newArrayList(nativeTable.newScan().planFiles());
        Assertions.assertEquals(bucketPartitions.length + 2, tasks.size());
        Set<Long> partitionIds = new HashSet<>();
        for (FileScanTask task : tasks) {
            long partitionId = scanRangeSource.addPartition(task);
            THdfsPartition value = scanRangeSource.buildScanRange(task, task.file(), partitionId).getPartition_value();

            // A source that sees only this file builds its PartitionKey without any cached identity partition.
            IcebergConnectorScanRangeSource uncached = new IcebergConnectorScanRangeSource(icebergTable,
                    RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, desc, Optional.empty(),
                    partitionIdGenerator, false, false);
            long expectedId = uncached.addPartition(task);
            Assertions.assertEquals(expectedId, partitionId, task.file().location());
            Assertions.assertEquals(uncached.buildScanRange(task, task.file(), expectedId).getPartition_value(), value,
                    task.file().location());
            partitionIds.add(partitionId);
        }
        // (a, 2024-01-01), (a, 2024-01-02), (b, 2024-01-01), (null, 2024-01-01), (a, null), (null, null),
        // (null, 2024-01-03); the identity spec's (a, 2024-01-01) file shares the bucket spec's id
        Assertions.assertEquals(7, partitionIds.size());
        // the partition count counts every (spec, full partition tuple)
        Assertions.assertEquals(bucketPartitions.length + 2, scanRangeSource.selectedPartitionCount());
    }

    @Test
    public void testTaskSpecIsParsedOncePerSpecId() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_B).identity("k2").build();
        TestTables.TestTable nativeTable = create(SCHEMA_B, spec, "t_spec_parse", 2);
        nativeTable.newFastAppend().appendFile(DataFiles.builder(spec).withPath("/path/to/k2=1/a.parquet")
                .withFileSizeInBytes(10).withPartitionPath("k2=1").withRecordCount(1).build())
                .appendFile(DataFiles.builder(spec).withPath("/path/to/k2=2/b.parquet")
                .withFileSizeInBytes(10).withPartitionPath("k2=2").withRecordCount(1).build()).commit();

        TupleDescriptor desc = new TupleDescriptor(new TupleId(5));
        SlotDescriptor k2Slot = new SlotDescriptor(new SlotId(2), desc);
        k2Slot.setType(INT);
        k2Slot.setColumn(new Column("k2", INT));
        desc.addSlot(k2Slot);
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog", "resource", "db",
                "table", "", Lists.newArrayList(new Column("k1", INT), new Column("k2", INT)), nativeTable,
                Maps.newHashMap());
        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, desc, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        List<FileScanTask> tasks = Lists.newArrayList(nativeTable.newScan().planFiles());
        Assertions.assertEquals(2, tasks.size());
        long firstId = scanRangeSource.addPartition(tasks.get(0));
        // task.spec() parses the spec per task, hashing the whole schema; a later file of the spec, in another
        // partition or the same one, must not need it
        FileScanTask otherPartition = Mockito.spy(tasks.get(1));
        Assertions.assertNotEquals(firstId, scanRangeSource.addPartition(otherPartition));
        FileScanTask samePartition = Mockito.spy(tasks.get(0));
        Assertions.assertEquals(firstId, scanRangeSource.addPartition(samePartition));
        Mockito.verify(otherPartition, Mockito.never()).spec();
        Mockito.verify(samePartition, Mockito.never()).spec();
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

    // A runtime-filter pruning column of an equality delete scan holds the delete file's identity partition value,
    // also without a session (incremental scan ranges), and a delete file that cannot provide it fails the query.
    @Test
    public void testEqualityDeleteRangeTakesPruningColumnFromPartition() throws Exception {
        ConnectContext.remove();
        DeleteFile eqDelete = FileMetadata.deleteFileBuilder(SPEC_B)
                .ofEqualityDeletes(1)
                .withPath("/path/to/data-b1-eq-deletes.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10)
                .withPartitionPath("k2=2")
                .withRecordCount(1)
                .build();
        mockedNativeTableC.newFastAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableC.newRowDelta().addDeletes(eqDelete).commit();
        List<Column> schema = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableC, Maps.newHashMap());

        TupleDescriptor localTupleDescriptor = new TupleDescriptor(new TupleId(3));
        for (Column column : schema) {
            SlotDescriptor slot = new SlotDescriptor(new SlotId(localTupleDescriptor.getSlots().size() + 1),
                    localTupleDescriptor);
            slot.setType(INT);
            slot.setColumn(column);
            localTupleDescriptor.addSlot(slot);
        }
        IcebergMORParams eqDeleteParams = IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, List.of(1));
        FileScanTask task = Lists.newArrayList(mockedNativeTableC.newScan().planFiles()).get(0);

        IcebergConnectorScanRangeSource source = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, eqDeleteParams, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);
        source.setRuntimeFilterPruningColumns(Set.of("k2"));
        List<TScanRangeLocations> ranges = source.toScanRanges(task);
        Assertions.assertEquals(1, ranges.size());
        THdfsScanRange range = ranges.get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertTrue(range.getPartition_id() >= 0);
        Assertions.assertEquals(List.of(localTupleDescriptor.getColumnSlot("k2").getId().asInt()),
                range.getIdentity_partition_slot_ids());

        IcebergConnectorScanRangeSource notPartitioned = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, eqDeleteParams, localTupleDescriptor, Optional.empty(),
                PartitionIdGenerator.of(), false, false);
        notPartitioned.setRuntimeFilterPruningColumns(Set.of("k1"));
        Assertions.assertThrows(StarRocksConnectorException.class, () -> notPartitioned.toScanRanges(task));
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

    private IcebergConnectorScanRangeSource newScanRangeSourceForTableC() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("k1", INT));
        schema.add(new Column("k2", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableC, Maps.newHashMap());
        return new IcebergConnectorScanRangeSource(icebergTable, RemoteFileInfoDefaultSource.EMPTY,
                IcebergMORParams.EMPTY, tupleDescriptor, Optional.empty(), PartitionIdGenerator.of(), false, false);
    }

    @Test
    public void testPartitionNumLimitThrowsWhenExceeded() throws Exception {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setThreadLocalInfo();
        connectContext.getSessionVariable().setScanLakePartitionNumLimit(1);

        // FILE_B_1 is in partition k2=2, FILE_B_2 is in a different partition k2=3 - two distinct partitions.
        mockedNativeTableC.newFastAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        IcebergConnectorScanRangeSource scanRangeSource = newScanRangeSourceForTableC();
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableC.newScan().planFiles());
        Assertions.assertEquals(2, fileScanTasks.size());

        Assertions.assertThrows(StarRocksConnectorException.class, () -> {
            for (FileScanTask task : fileScanTasks) {
                scanRangeSource.addPartition(task);
            }
        });
    }

    @Test
    public void testPartitionNumLimitNotExceededWithinLimit() throws Exception {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setThreadLocalInfo();
        connectContext.getSessionVariable().setScanLakePartitionNumLimit(2);

        mockedNativeTableC.newFastAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        IcebergConnectorScanRangeSource scanRangeSource = newScanRangeSourceForTableC();
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableC.newScan().planFiles());
        Assertions.assertEquals(2, fileScanTasks.size());

        for (FileScanTask task : fileScanTasks) {
            scanRangeSource.addPartition(task);
        }
        Assertions.assertEquals(2, scanRangeSource.selectedPartitionCount());
    }

    @Test
    public void testPartitionNumLimitDisabledByDefault() throws Exception {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setThreadLocalInfo();
        // 0 (the default) means unlimited.
        connectContext.getSessionVariable().setScanLakePartitionNumLimit(0);

        mockedNativeTableC.newFastAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        IcebergConnectorScanRangeSource scanRangeSource = newScanRangeSourceForTableC();
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableC.newScan().planFiles());

        for (FileScanTask task : fileScanTasks) {
            scanRangeSource.addPartition(task);
        }
        Assertions.assertEquals(2, scanRangeSource.selectedPartitionCount());
    }

    /**
     * Test that for an Iceberg V1 table, dropping an identity partition field does not cause NPE
     * when scanning files.
     * Background: For V1 tables, Iceberg's `removeField` does NOT remove the field from the spec.
     * Instead, the field is kept with its transform rewritten to `VoidTransform` (always-null),
     * and the field name stays the same as the source column. This is why we need to check for
     * `isIdentity` when adding partitions.
     */
    @Test
    public void testAddPartitionWithV1DroppedIdentityPartitionFieldDoesNotThrow() throws Exception {
        PartitionSpec specIdentityK1 = PartitionSpec.builderFor(SCHEMA_F).identity("k1").build();
        TestTables.TestTable v1Table = create(SCHEMA_F, specIdentityK1, "ti_k1", 1);
        Assertions.assertEquals(1, v1Table.operations().current().formatVersion());

        DataFile fileBeforeDrop = DataFiles.builder(v1Table.spec())
                .withPath("/path/to/i-before-drop.parquet")
                .withFileSizeInBytes(10)
                .withPartitionPath("k1=1")
                .withRecordCount(2)
                .build();
        v1Table.newFastAppend().appendFile(fileBeforeDrop).commit();
        // Drop the identity partition field "k1".
        // For V1, this keeps the field in the spec with VoidTransform.
        v1Table.updateSpec().removeField("k1").commit();
        // Sanity check: the new spec still contains a field whose name is "k1"
        // (V1 semantics), but its transform is no longer identity.
        PartitionSpec newSpec = v1Table.spec();
        Assertions.assertTrue(newSpec.fields().stream()
                        .anyMatch(f -> f.name().equals("k1") && !f.transform().isIdentity()),
                "V1 drop identity partition field should keep a non-identity (void) field with the same name");
        DataFile fileAfterDrop = DataFiles.builder(newSpec)
                .withPath("/path/to/i-after-drop.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build();
        v1Table.newFastAppend().appendFile(fileAfterDrop).commit();

        TupleDescriptor localTuple = new TupleDescriptor(new TupleId(101));
        SlotDescriptor k1Slot = new SlotDescriptor(new SlotId(1), localTuple);
        k1Slot.setType(INT);
        k1Slot.setColumn(new Column("k1", INT));
        localTuple.addSlot(k1Slot);
        SlotDescriptor dtSlot = new SlotDescriptor(new SlotId(2), localTuple);
        dtSlot.setType(DateType.DATE);
        dtSlot.setColumn(new Column("dt", DateType.DATE));
        localTuple.addSlot(dtSlot);

        List<Column> schema = Lists.newArrayList(new Column("k1", INT), new Column("dt", DateType.DATE));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table_v1_drop_id", "iceberg_catalog",
                "resource", "db", "table", "", schema, v1Table, Maps.newHashMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTuple, Optional.empty(),
                PartitionIdGenerator.of(), false, false);

        List<FileScanTask> tasks = Lists.newArrayList(v1Table.newScan().planFiles());
        Assertions.assertFalse(tasks.isEmpty(), "should scan at least one file");
        for (FileScanTask task : tasks) {
            long pid = Assertions.assertDoesNotThrow(() -> scanRangeSource.addPartition(task),
                    "addPartition should not throw NPE for V1 table with dropped identity partition field");
            Assertions.assertTrue(pid >= 0);
        }
    }

    /**
     * V1 iceberg table with the only identity partition field dropped. The spec
     * is rewritten to {@code [VoidTransform(k1)]}. Both lists must end up empty so that
     * BE's {@code _init_partition_values} does NOT read past the end of {@code _partition_values}.
     */
    @Test
    public void testBuildPartitionSlotIdsExcludesVoidFieldAfterDrop() throws Exception {
        ConnectContext prevCtx = ConnectContext.get();
        new ConnectContext().setThreadLocalInfo();
        try {
            PartitionSpec specIdentityK1 = PartitionSpec.builderFor(SCHEMA_F).identity("k1").build();
            TestTables.TestTable v1Table = create(SCHEMA_F, specIdentityK1, "ti_void_only", 1);

            // Drop the only identity partition field; for V1 this rewrites it to VoidTransform.
            v1Table.updateSpec().removeField("k1").commit();
            PartitionSpec newSpec = v1Table.spec();
            Assertions.assertTrue(newSpec.fields().stream().noneMatch(f -> f.transform().isIdentity()),
                    "after drop, spec should contain no identity field");

            DataFile fileAfterDrop = DataFiles.builder(newSpec)
                    .withPath("/path/to/void-only.parquet")
                    .withFileSizeInBytes(10)
                    .withRecordCount(2)
                    .build();
            v1Table.newFastAppend().appendFile(fileAfterDrop).commit();

            TupleDescriptor localTuple = new TupleDescriptor(new TupleId(102));
            SlotDescriptor k1Slot = new SlotDescriptor(new SlotId(1), localTuple);
            k1Slot.setType(INT);
            k1Slot.setColumn(new Column("k1", INT));
            localTuple.addSlot(k1Slot);
            SlotDescriptor dtSlot = new SlotDescriptor(new SlotId(2), localTuple);
            dtSlot.setType(DateType.DATE);
            dtSlot.setColumn(new Column("dt", DateType.DATE));
            localTuple.addSlot(dtSlot);

            List<Column> schema = Lists.newArrayList(new Column("k1", INT), new Column("dt", DateType.DATE));
            IcebergTable icebergTable = new IcebergTable(1, "iceberg_void_only", "iceberg_catalog",
                    "resource", "db", "table", "", schema, v1Table, Maps.newHashMap());

            IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                    RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTuple, Optional.empty(),
                    PartitionIdGenerator.of(), false, false);

            List<FileScanTask> tasks = Lists.newArrayList(v1Table.newScan().planFiles());
            Assertions.assertFalse(tasks.isEmpty());
            for (FileScanTask task : tasks) {
                long pid = scanRangeSource.addPartition(task);
                THdfsScanRange hdfsScanRange = scanRangeSource.buildScanRange(task, task.file(), pid);

                List<Integer> identitySlotIds = hdfsScanRange.getIdentity_partition_slot_ids();
                int identitySlotIdsSize = identitySlotIds == null ? 0 : identitySlotIds.size();

                THdfsPartition partitionValue = hdfsScanRange.getPartition_value();
                int keyExprsSize = partitionValue == null || partitionValue.getPartition_key_exprs() == null
                        ? 0 : partitionValue.getPartition_key_exprs().size();

                Assertions.assertEquals(0, identitySlotIdsSize,
                        "VoidTransform field must NOT contribute to identity_partition_slot_ids");
                Assertions.assertEquals(0, keyExprsSize,
                        "VoidTransform field must NOT contribute to partition_key_exprs");
            }
        } finally {
            if (prevCtx == null) {
                ConnectContext.remove();
            } else {
                prevCtx.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testCacheKeyShouldUseEffectiveIdentityKey() throws Exception {
        ConnectContext prevCtx = ConnectContext.get();
        new ConnectContext().setThreadLocalInfo();
        try {
            PartitionSpec specBothIdentity =
                    PartitionSpec.builderFor(SCHEMA_F).identity("k1").identity("dt").build();
            TestTables.TestTable v1Table = create(SCHEMA_F, specBothIdentity, "ti_reuse", 1);
            Assertions.assertEquals(1, v1Table.operations().current().formatVersion());

            PartitionKey oldKey = new PartitionKey(specBothIdentity, SCHEMA_F);
            oldKey.set(0, null); // k1 = NULL
            oldKey.set(1, null); // dt = NULL
            DataFile oldFile = DataFiles.builder(specBothIdentity)
                    .withPath("/path/to/old-before-drop.parquet")
                    .withFileSizeInBytes(10)
                    .withPartition(oldKey)
                    .withRecordCount(2)
                    .build();
            v1Table.newFastAppend().appendFile(oldFile).commit();

            // Drop the identity partition field "k1". For V1 this rewrites it to VoidTransform,
            // i.e. new spec = [void(k1), identity(dt)]. VoidTransform always writes NULL for k1.
            v1Table.updateSpec().removeField("k1").commit();
            PartitionSpec newSpec = v1Table.spec();
            Assertions.assertNotEquals(specBothIdentity.specId(), newSpec.specId(),
                    "drop should produce a new spec id");
            Assertions.assertTrue(newSpec.fields().stream()
                            .anyMatch(f -> f.name().equals("k1") && !f.transform().isIdentity()),
                    "after V1 drop, k1 should still be present but with VoidTransform");

            // New file under the new spec, partition struct = (void=NULL, dt=NULL).
            PartitionKey newKey = new PartitionKey(newSpec, SCHEMA_F);
            newKey.set(0, null); // void(k1) = NULL (mandatory)
            newKey.set(1, null); // dt = NULL (same value as old file)
            DataFile newFile = DataFiles.builder(newSpec)
                    .withPath("/path/to/new-after-drop.parquet")
                    .withFileSizeInBytes(10)
                    .withPartition(newKey)
                    .withRecordCount(2)
                    .build();
            v1Table.newFastAppend().appendFile(newFile).commit();

            // Build the tuple descriptor with both partition columns materialised.
            TupleDescriptor localTuple = new TupleDescriptor(new TupleId(201));
            SlotDescriptor k1Slot = new SlotDescriptor(new SlotId(1), localTuple);
            k1Slot.setType(INT);
            k1Slot.setColumn(new Column("k1", INT));
            localTuple.addSlot(k1Slot);
            SlotDescriptor dtSlot = new SlotDescriptor(new SlotId(2), localTuple);
            dtSlot.setType(DateType.DATE);
            dtSlot.setColumn(new Column("dt", DateType.DATE));
            localTuple.addSlot(dtSlot);

            List<Column> schema = Lists.newArrayList(new Column("k1", INT), new Column("dt", DateType.DATE));
            IcebergTable icebergTable = new IcebergTable(1, "iceberg_reuse", "iceberg_catalog",
                    "resource", "db", "table", "", schema, v1Table, Maps.newHashMap());

            IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                    RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, localTuple, Optional.empty(),
                    PartitionIdGenerator.of(), false, false);

            List<FileScanTask> allTasks = Lists.newArrayList(v1Table.newScan().planFiles());
            FileScanTask oldTask = allTasks.stream()
                    .filter(t -> t.file().path().toString().endsWith("old-before-drop.parquet"))
                    .findFirst().orElseThrow();
            FileScanTask newTask = allTasks.stream()
                    .filter(t -> t.file().path().toString().endsWith("new-after-drop.parquet"))
                    .findFirst().orElseThrow();
            // Sanity check: the two files come from different specs.
            Assertions.assertNotEquals(oldTask.spec().specId(), newTask.spec().specId(),
                    "old and new file must use different specs to reproduce the bug");

            long oldPid = scanRangeSource.addPartition(oldTask);
            long newPid = scanRangeSource.addPartition(newTask);

            Assertions.assertNotEquals(oldPid, newPid,
                    "Files from different specs must NOT share a partition id, even when their "
                            + "raw Iceberg partition structs happen to be equal.");

            THdfsScanRange oldRange = scanRangeSource.buildScanRange(oldTask, oldTask.file(), oldPid);
            THdfsScanRange newRange = scanRangeSource.buildScanRange(newTask, newTask.file(), newPid);

            int oldKeyExprs = oldRange.getPartition_value().getPartition_key_exprs().size();
            int oldIdentitySlotIds = oldRange.getIdentity_partition_slot_ids() == null
                    ? 0 : oldRange.getIdentity_partition_slot_ids().size();
            int newKeyExprs = newRange.getPartition_value().getPartition_key_exprs().size();
            int newIdentitySlotIds = newRange.getIdentity_partition_slot_ids() == null
                    ? 0 : newRange.getIdentity_partition_slot_ids().size();

            Assertions.assertEquals(2, oldKeyExprs);
            Assertions.assertEquals(2, oldIdentitySlotIds);
            Assertions.assertEquals(1, newKeyExprs);
            Assertions.assertEquals(1, newIdentitySlotIds);
            Assertions.assertEquals(newKeyExprs, newIdentitySlotIds,
                    "partition_key_exprs.size() and identity_partition_slot_ids.size() must match — "
                            + "otherwise BE's _init_partition_values goes out of sync.");
        } finally {
            if (prevCtx == null) {
                ConnectContext.remove();
            } else {
                prevCtx.setThreadLocalInfo();
            }
        }
    }

    // Data files and equality delete files get the same key exactly when they share spec and partition.
    @Test
    public void testBuildPartitionKey() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p", Types.StringType.get()));
        PartitionSpec unpartitioned = PartitionSpec.builderFor(schema).withSpecId(0).build();
        PartitionSpec byP = PartitionSpec.builderFor(schema).withSpecId(1).identity("p").build();
        PartitionSpec byPAgain = PartitionSpec.builderFor(schema).withSpecId(2).identity("p").build();

        Assertions.assertNull(IcebergConnectorScanRangeSource.buildPartitionKey(unpartitioned, null));
        String a = IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition("a"));
        Assertions.assertEquals(a, IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition("a")));
        Assertions.assertNotEquals(a, IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition("b")));
        // the same partition value under another spec is another partition
        Assertions.assertNotEquals(a, IcebergConnectorScanRangeSource.buildPartitionKey(byPAgain, partition("a")));
        // a null partition value differs from every string value
        String nullKey = IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition(null));
        for (String value : List.of("", "n", "null")) {
            Assertions.assertNotEquals(nullKey, IcebergConnectorScanRangeSource.buildPartitionKey(byP, partition(value)));
        }
    }

    // int -> long and float -> double promotions: a side that sees the field under either type, holding a value
    // of either class, must produce the same key.
    @Test
    public void testBuildPartitionKeyAcrossTypePromotion() {
        PartitionSpec byIntP = PartitionSpec.builderFor(new Schema(
                Types.NestedField.optional(1, "p", Types.IntegerType.get()))).withSpecId(0).identity("p").build();
        PartitionSpec byLongP = PartitionSpec.builderFor(new Schema(
                Types.NestedField.optional(1, "p", Types.LongType.get()))).withSpecId(0).identity("p").build();
        String key = IcebergConnectorScanRangeSource.buildPartitionKey(byIntP, partition(5));
        Assertions.assertEquals(key, IcebergConnectorScanRangeSource.buildPartitionKey(byLongP, partition(5L)));
        Assertions.assertEquals(key, IcebergConnectorScanRangeSource.buildPartitionKey(byLongP, partition(5)));
        Assertions.assertEquals(key, IcebergConnectorScanRangeSource.buildPartitionKey(byIntP, partition(5L)));

        PartitionSpec byFloatQ = PartitionSpec.builderFor(new Schema(
                Types.NestedField.optional(1, "q", Types.FloatType.get()))).withSpecId(0).identity("q").build();
        PartitionSpec byDoubleQ = PartitionSpec.builderFor(new Schema(
                Types.NestedField.optional(1, "q", Types.DoubleType.get()))).withSpecId(0).identity("q").build();
        Assertions.assertEquals(IcebergConnectorScanRangeSource.buildPartitionKey(byFloatQ, partition(1.5f)),
                IcebergConnectorScanRangeSource.buildPartitionKey(byDoubleQ, partition(1.5d)));
    }

    // Only the column the equality delete rewrite adds is the partition key; a real column of that name is not.
    @Test
    public void testIsPartitionKeyColumn() {
        Schema plain = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        Schema withRealColumn = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, IcebergTable.PARTITION_KEY, Types.StringType.get()));
        Assertions.assertTrue(IcebergConnectorScanRangeSource.isPartitionKeyColumn(IcebergTable.PARTITION_KEY, plain));
        Assertions.assertFalse(IcebergConnectorScanRangeSource.isPartitionKeyColumn("id", plain));
        Assertions.assertFalse(
                IcebergConnectorScanRangeSource.isPartitionKeyColumn(IcebergTable.PARTITION_KEY, withRealColumn));
    }

    // Each equality-id group has its own delete scan, which must only read the delete files of that group.
    @Test
    public void testDeleteFileScanRangesKeepOnlyTheirEqualityGroup() {
        DeleteFile deletesByData = FileMetadata.deleteFileBuilder(SPEC_A)
                .ofEqualityDeletes(2)
                .withPath("/path/to/data-a-deletes-by-data.orc")
                .withFormat(FileFormat.ORC)
                .withFileSizeInBytes(10)
                .withPartitionPath("data_bucket=0")
                .withRecordCount(1)
                .build();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A2_DELETES).addDeletes(deletesByData).commit();
        List<Column> schema = Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableA, Maps.newHashMap());
        FileScanTask fileScanTask = Lists.newArrayList(mockedNativeTableA.newScan().planFiles()).get(0);
        Assertions.assertEquals(2, fileScanTask.deletes().size());

        Assertions.assertEquals(List.of(FILE_A2_DELETES.location()), deleteScanPaths(icebergTable, fileScanTask, 1));
        Assertions.assertEquals(List.of(deletesByData.location()), deleteScanPaths(icebergTable, fileScanTask, 2));
    }

    private List<String> deleteScanPaths(IcebergTable icebergTable, FileScanTask fileScanTask, int equalityId) {
        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable,
                RemoteFileInfoDefaultSource.EMPTY,
                IcebergMORParams.of(IcebergMORParams.ScanTaskType.EQ_DELETE, List.of(equalityId)),
                tupleDescriptor, Optional.empty(), PartitionIdGenerator.of(), false, false);
        List<String> paths = new ArrayList<>();
        for (TScanRangeLocations locations : scanRangeSource.toScanRanges(fileScanTask)) {
            paths.add(locations.getScan_range().getHdfs_scan_range().getFull_path());
        }
        return paths;
    }

    private static StructLike partition(Object value) {
        return new StructLike() {
            @Override
            public int size() {
                return 1;
            }

            @Override
            public <T> T get(int pos, Class<T> javaClass) {
                return javaClass.cast(value);
            }

            @Override
            public <T> void set(int pos, T newValue) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
