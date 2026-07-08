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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TIcebergDeletionVectorDescriptor;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static com.starrocks.connector.iceberg.IcebergApiConverter.toPartitionField;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarcharType.VARCHAR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for handling Iceberg V3 features:
 * 1. Deletion Vectors (planned into scan range descriptor)
 * 2. Extended Types (graceful degradation to UNKNOWN_TYPE)
 * 3. Multi-argument transforms (fail-fast)
 * 4. Table encryption (fail-fast)
 */
public class IcebergV3UnsupportedFeaturesTest extends TableTestBase {

    private TupleDescriptor tupleDescriptor;

    @BeforeEach
    public void setUp() {
        tupleDescriptor = new TupleDescriptor(new TupleId(1));

        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), tupleDescriptor);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));

        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), tupleDescriptor);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));

        tupleDescriptor.addSlot(idSlot);
        tupleDescriptor.addSlot(dataSlot);
    }

    // ========== 1. Deletion Vectors ==========

    @Test
    public void testDeletionVectorIsDetectedByContentFileUtil() {
        // DVs are POSITION_DELETES in Puffin format with referencedDataFile set.
        DeleteFile dvDeleteFile = mock(DeleteFile.class);
        when(dvDeleteFile.content()).thenReturn(FileContent.POSITION_DELETES);
        when(dvDeleteFile.format()).thenReturn(FileFormat.PUFFIN);
        when(dvDeleteFile.referencedDataFile()).thenReturn("/path/to/data-a.parquet");
        when(dvDeleteFile.path()).thenReturn("/path/to/dv-file.puffin");

        Assertions.assertTrue(ContentFileUtil.isDV(dvDeleteFile),
                "Puffin position delete with referencedDataFile should be detected as DV");
    }

    /**
     * Builds a single scan range from a mock data file and list of delete files.
     * Uses SPEC_A (bucket partition) and FILE_A's partition data for infrastructure plumbing.
     * The provided dataFile must have location() and format() stubbed; additional stubs for
     * path(), content(), fileSizeInBytes(), partition(), recordCount() are added here.
     */
    public static THdfsScanRange buildSingleScanRange(DataFile dataFile, List<DeleteFile> deletes) throws Exception {
        // Stub path() so buildScanRange's file.path().toString() call does not NPE.
        // location() is a default method that calls path().toString(); since the mock stubs
        // location() independently, we also stub path() to return the same value.
        when(dataFile.path()).thenAnswer(inv -> dataFile.location());
        when(dataFile.content()).thenReturn(FileContent.DATA);
        when(dataFile.fileSizeInBytes()).thenReturn(10L);
        when(dataFile.recordCount()).thenReturn(1L);
        when(dataFile.partition()).thenReturn(FILE_A.partition());
        when(dataFile.specId()).thenReturn(0);
        when(dataFile.splitOffsets()).thenReturn(null);
        when(dataFile.dataSequenceNumber()).thenReturn(null);
        when(dataFile.firstRowId()).thenReturn(null);

        org.apache.iceberg.Table mockNativeTable = mock(org.apache.iceberg.Table.class);
        when(mockNativeTable.location()).thenReturn("/test-table");
        when(mockNativeTable.spec()).thenReturn(SPEC_A);
        when(mockNativeTable.properties()).thenReturn(new HashMap<>());

        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", INT));
        columns.add(new Column("data", VARCHAR));
        IcebergTable icebergTable = IcebergTable.builder()
                .setNativeTable(mockNativeTable)
                .setFullSchema(columns)
                .setCatalogName("test_catalog")
                .setCatalogDBName("test_db")
                .setCatalogTableName("test_table")
                .build();

        FileScanTask task = mock(FileScanTask.class);
        when(task.file()).thenReturn(dataFile);
        when(task.deletes()).thenReturn(deletes);
        when(task.spec()).thenReturn(SPEC_A);
        when(task.partition()).thenReturn(FILE_A.partition());
        when(task.start()).thenReturn(0L);
        when(task.length()).thenReturn(10L);

        TupleDescriptor td = new TupleDescriptor(new TupleId(0));
        IcebergConnectorScanRangeSource source = new IcebergConnectorScanRangeSource(
                icebergTable,
                RemoteFileInfoDefaultSource.EMPTY,
                IcebergMORParams.EMPTY,
                td,
                Optional.empty(),
                PartitionIdGenerator.of(),
                false,
                false);

        List<TScanRangeLocations> ranges = source.toScanRanges(task);
        return ranges.get(0).getScan_range().getHdfs_scan_range();
    }

    @Test
    public void testDeletionVectorBuildsDescriptor() throws Exception {
        // data file is parquet, DV references it.
        DataFile dataFile = mock(DataFile.class);
        when(dataFile.location()).thenReturn("/path/to/data-a.parquet");
        when(dataFile.format()).thenReturn(FileFormat.PARQUET);

        DeleteFile dv = mock(DeleteFile.class);
        when(dv.content()).thenReturn(FileContent.POSITION_DELETES);
        when(dv.format()).thenReturn(FileFormat.PUFFIN);
        when(dv.referencedDataFile()).thenReturn("/path/to/data-a.parquet");
        when(dv.path()).thenReturn("/path/to/dv.puffin");
        when(dv.contentOffset()).thenReturn(4L);
        when(dv.contentSizeInBytes()).thenReturn(38L);
        when(dv.recordCount()).thenReturn(6L);
        when(dv.fileSizeInBytes()).thenReturn(64L);

        THdfsScanRange range = IcebergV3UnsupportedFeaturesTest.buildSingleScanRange(dataFile, List.of(dv));

        Assertions.assertTrue(range.isSetIceberg_deletion_vector_descriptor());
        TIcebergDeletionVectorDescriptor d = range.getIceberg_deletion_vector_descriptor();
        Assertions.assertEquals("/path/to/dv.puffin", d.getPuffin_file_path());
        Assertions.assertEquals(4L, d.getContent_offset());
        Assertions.assertEquals(38L, d.getContent_size_in_bytes());
        Assertions.assertEquals(6L, d.getRecord_count());
        Assertions.assertEquals("/path/to/data-a.parquet", d.getReferenced_data_file());
    }

    @Test
    public void testMultipleDeletionVectorsFailFast() {
        DataFile dataFile = mock(DataFile.class);
        when(dataFile.location()).thenReturn("/d.parquet");
        when(dataFile.format()).thenReturn(FileFormat.PARQUET);
        DeleteFile dv1 = newDv("/d.parquet", "/x.puffin", 4L, 38L, 6L, 64L);
        DeleteFile dv2 = newDv("/d.parquet", "/y.puffin", 4L, 38L, 6L, 64L);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergV3UnsupportedFeaturesTest.buildSingleScanRange(dataFile, List.of(dv1, dv2)));
    }

    @Test
    public void testDeletionVectorMissingFieldFailFast() {
        DataFile dataFile = mock(DataFile.class);
        when(dataFile.location()).thenReturn("/d.parquet");
        when(dataFile.format()).thenReturn(FileFormat.PARQUET);
        DeleteFile dv = newDv("/d.parquet", "/x.puffin", null, 38L, 6L, 64L); // null offset
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergV3UnsupportedFeaturesTest.buildSingleScanRange(dataFile, List.of(dv)));
    }

    @Test
    public void testDeletionVectorOnOrcFailFast() {
        DataFile dataFile = mock(DataFile.class);
        when(dataFile.location()).thenReturn("/d.orc");
        when(dataFile.format()).thenReturn(FileFormat.ORC);
        DeleteFile dv = newDv("/d.orc", "/x.puffin", 4L, 38L, 6L, 64L);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergV3UnsupportedFeaturesTest.buildSingleScanRange(dataFile, List.of(dv)));
    }

    // helper: construct a DV mock; offset accepts null (to test missing-field path)
    private static DeleteFile newDv(String ref, String path, Long offset, Long size, long rc, long fsize) {
        DeleteFile dv = mock(DeleteFile.class);
        when(dv.content()).thenReturn(FileContent.POSITION_DELETES);
        when(dv.format()).thenReturn(FileFormat.PUFFIN);
        when(dv.referencedDataFile()).thenReturn(ref);
        when(dv.path()).thenReturn(path);
        when(dv.contentOffset()).thenReturn(offset);
        when(dv.contentSizeInBytes()).thenReturn(size);
        when(dv.recordCount()).thenReturn(rc);
        when(dv.fileSizeInBytes()).thenReturn(fsize);
        return dv;
    }

    @Test
    public void testPositionDeleteWithoutDVIsAllowed() {
        // Regular position deletes (not DVs) should still work fine
        DeleteFile regularPosDelete = FileMetadata.deleteFileBuilder(SPEC_A)
                .ofPositionDeletes()
                .withPath("/path/to/pos-deletes.orc")
                .withFormat(FileFormat.ORC)
                .withFileSizeInBytes(10)
                .withPartitionPath("data_bucket=0")
                .withRecordCount(1)
                .build();

        // Verify this is NOT a DV
        Assertions.assertFalse(
                org.apache.iceberg.util.ContentFileUtil.isDV(regularPosDelete),
                "Regular position delete file should not be detected as DV");
    }

    @Test
    public void testDeleteFileWrapperProxiesDVFields() {
        DeleteFile dvFile = mock(DeleteFile.class);
        when(dvFile.path()).thenReturn("/path/to/dv.puffin");
        when(dvFile.referencedDataFile()).thenReturn("/path/to/data.parquet");
        when(dvFile.contentOffset()).thenReturn(1024L);
        when(dvFile.contentSizeInBytes()).thenReturn(2048L);

        DeleteFileWrapper wrapper = DeleteFileWrapper.wrap(dvFile);
        Assertions.assertEquals("/path/to/data.parquet", wrapper.referencedDataFile());
        Assertions.assertEquals(1024L, wrapper.contentOffset());
        Assertions.assertEquals(2048L, wrapper.contentSizeInBytes());
    }

    // ========== 2. Extended Types ==========

    @Test
    public void testUnsupportedIcebergTypeReturnsUnknown() {
        // Unsupported V3 types should degrade to UNKNOWN_TYPE, not throw.
        // The query analyzer will reject queries on these columns at analysis time.
        Assertions.assertTrue(fromIcebergType(Types.GeometryType.crs84()).isUnknown());
        Assertions.assertTrue(fromIcebergType(Types.GeographyType.crs84()).isUnknown());
        Assertions.assertTrue(fromIcebergType(Types.FixedType.ofLength(16)).isUnknown());
    }

    @Test
    public void testSupportedTypesStillWork() {
        // Verify supported types are not affected
        Assertions.assertFalse(fromIcebergType(Types.BooleanType.get()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.IntegerType.get()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.LongType.get()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.FloatType.get()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.DoubleType.get()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.DateType.get()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.TimestampType.withZone()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.StringType.get()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.BinaryType.get()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.UUIDType.get()).isUnknown());
        Assertions.assertFalse(fromIcebergType(Types.TimeType.get()).isUnknown());
        Assertions.assertTrue(fromIcebergType(Types.VariantType.get()).isVariantType());
    }

    @Test
    public void testToFullSchemasDegradesToUnknownForUnsupportedType() {
        // Unsupported V3 types should degrade to UNKNOWN_TYPE, not throw,
        // so that queries not involving the column still work.
        Types.NestedField field = Types.NestedField.optional(1, "geo_col", Types.GeographyType.crs84());
        Schema schema = new Schema(field);

        List<Column> columns = IcebergApiConverter.toFullSchemas(schema);
        Assertions.assertEquals(1, columns.size());
        Assertions.assertTrue(columns.get(0).getType().isUnknown(),
                "Unsupported type should degrade to UNKNOWN, got: " + columns.get(0).getType());
    }

    // ========== 3. Multi-argument transforms ==========

    @Test
    public void testUnknownTransformFromStringParsesAsUnknown() {
        // A V3 multi-argument transform like "range" should parse as UNKNOWN
        IcebergPartitionTransform transform =
                IcebergPartitionTransform.fromString("range[10, 20]");
        Assertions.assertEquals(IcebergPartitionTransform.UNKNOWN, transform);

        // Another example: a hypothetical "sorted" transform
        transform = IcebergPartitionTransform.fromString("sorted");
        Assertions.assertEquals(IcebergPartitionTransform.UNKNOWN, transform);
    }

    @Test
    public void testToPartitionFieldFailsOnUnsupportedTransform() {
        // Create a partition spec with a known transform, then test with unknown transform string
        // We use a mock PartitionField to simulate an unsupported V3 multi-arg transform
        org.apache.iceberg.PartitionField mockField = mock(org.apache.iceberg.PartitionField.class);
        org.apache.iceberg.transforms.Transform mockTransform =
                mock(org.apache.iceberg.transforms.Transform.class);
        when(mockTransform.toString()).thenReturn("range[10, 20]");
        when(mockField.transform()).thenReturn(mockTransform);
        when(mockField.sourceId()).thenReturn(1);

        PartitionSpec mockSpec = mock(PartitionSpec.class);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        when(mockSpec.schema()).thenReturn(schema);

        StarRocksConnectorException ex = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> toPartitionField(mockSpec, mockField, false));
        Assertions.assertTrue(ex.getMessage().contains("Unsupported partition transform"),
                "Expected unsupported transform error, got: " + ex.getMessage());
    }

    @Test
    public void testIsSupportedConvertPartitionTransformRejectsUnknown() {
        Assertions.assertFalse(
                IcebergPartitionUtils.isSupportedConvertPartitionTransform(
                        IcebergPartitionTransform.UNKNOWN),
                "UNKNOWN transform should not be considered supported");
    }

    // ========== 4. Table encryption ==========

    @Test
    public void testEncryptionTableKeyPropertyFailsFast() {
        org.apache.iceberg.Table encryptedTable = mock(org.apache.iceberg.Table.class);
        Map<String, String> encryptionProps = new HashMap<>();
        encryptionProps.put("encryption.key-id", "my-key");
        when(encryptedTable.properties()).thenReturn(encryptionProps);
        when(encryptedTable.name()).thenReturn("encrypted_table");

        StarRocksConnectorException ex = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> IcebergMetadata.checkUnsupportedEncryption(encryptedTable));
        Assertions.assertTrue(ex.getMessage().contains("encryption is not supported"),
                "Expected encryption error, got: " + ex.getMessage());
    }

    @Test
    public void testEncryptionKeysInMetadataFailsFast() {
        // Test the TableMetadata.encryptionKeys() check path (lines 692-694)
        org.apache.iceberg.BaseTable baseTable = mock(org.apache.iceberg.BaseTable.class);
        Map<String, String> emptyProps = new HashMap<>();
        when(baseTable.properties()).thenReturn(emptyProps);
        when(baseTable.name()).thenReturn("encrypted_via_metadata");

        org.apache.iceberg.TableOperations ops = mock(org.apache.iceberg.TableOperations.class);
        org.apache.iceberg.TableMetadata metadata = mock(org.apache.iceberg.TableMetadata.class);
        when(baseTable.operations()).thenReturn(ops);
        when(ops.current()).thenReturn(metadata);
        when(metadata.encryptionKeys()).thenReturn(
                Lists.newArrayList(mock(org.apache.iceberg.encryption.EncryptedKey.class)));

        StarRocksConnectorException ex = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> IcebergMetadata.checkUnsupportedEncryption(baseTable));
        Assertions.assertTrue(ex.getMessage().contains("encryption is not supported"),
                "Expected encryption error, got: " + ex.getMessage());
    }

    @Test
    public void testNonEncryptedTablePasses() {
        org.apache.iceberg.Table normalTable = mock(org.apache.iceberg.Table.class);
        Map<String, String> normalProps = new HashMap<>();
        normalProps.put("write.format.default", "parquet");
        normalProps.put("commit.retry.num-retries", "4");
        when(normalTable.properties()).thenReturn(normalProps);

        // Should not throw
        IcebergMetadata.checkUnsupportedEncryption(normalTable);
    }
}
