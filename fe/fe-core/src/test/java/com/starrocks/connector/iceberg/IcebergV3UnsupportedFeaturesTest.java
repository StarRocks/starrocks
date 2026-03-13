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
import com.starrocks.type.Type;
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
 * Tests for fail-fast behavior on unsupported Iceberg V3 features:
 * 1. Deletion Vectors
 * 2. Extended Types (unknown type IDs)
 * 3. Multi-argument transforms
 * 4. Table encryption
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

    @Test
    public void testDeletionVectorFailsFastInScanRanges() {
        // Mock a DV delete file
        DeleteFile dvDeleteFile = mock(DeleteFile.class);
        when(dvDeleteFile.content()).thenReturn(FileContent.POSITION_DELETES);
        when(dvDeleteFile.format()).thenReturn(FileFormat.PUFFIN);
        when(dvDeleteFile.referencedDataFile()).thenReturn("/path/to/data-a.parquet");
        when(dvDeleteFile.path()).thenReturn("/path/to/dv-file.puffin");
        when(dvDeleteFile.fileSizeInBytes()).thenReturn(100L);
        when(dvDeleteFile.specId()).thenReturn(0);
        when(dvDeleteFile.partition()).thenReturn(FILE_A.partition());
        when(dvDeleteFile.pos()).thenReturn(null);

        // Mock a FileScanTask that includes the DV delete file
        FileScanTask taskWithDV = mock(FileScanTask.class);
        when(taskWithDV.file()).thenReturn(FILE_A);
        when(taskWithDV.deletes()).thenReturn(Lists.newArrayList(dvDeleteFile));
        when(taskWithDV.spec()).thenReturn(SPEC_A);
        when(taskWithDV.partition()).thenReturn(FILE_A.partition());

        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", INT));
        columns.add(new Column("data", VARCHAR));
        IcebergTable icebergTable = IcebergTable.builder()
                .setNativeTable(mockedNativeTableA)
                .setFullSchema(columns)
                .setCatalogName("test_catalog")
                .setCatalogDBName("test_db")
                .setCatalogTableName("test_table")
                .build();

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(
                icebergTable,
                RemoteFileInfoDefaultSource.EMPTY,
                IcebergMORParams.EMPTY,
                tupleDescriptor,
                Optional.empty(),
                PartitionIdGenerator.of(),
                false,
                false);

        // toScanRanges wraps exceptions: "build scan range failed" with cause containing DV message
        StarRocksConnectorException ex = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> scanRangeSource.toScanRanges(taskWithDV));
        // The DV error is wrapped by toScanRanges, check either the message or the cause
        boolean hasDvMessage = ex.getMessage().contains("Deletion Vectors are not supported") ||
                (ex.getCause() != null && ex.getCause().getMessage().contains("Deletion Vectors are not supported"));
        Assertions.assertTrue(hasDvMessage,
                "Expected DV error message, got: " + ex.getMessage());
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

    // ========== 2. Extended Types ==========

    @Test
    public void testUnsupportedIcebergTypeThrowsException() {
        // GEOMETRY is a real Iceberg V3 type not handled by StarRocks
        org.apache.iceberg.types.Type geometryType = Types.GeometryType.crs84();

        StarRocksConnectorException ex = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> fromIcebergType(geometryType));
        Assertions.assertTrue(ex.getMessage().contains("Unsupported Iceberg type"),
                "Expected unsupported type error, got: " + ex.getMessage());
    }

    @Test
    public void testFixedTypeStillReturnsUnknown() {
        // FIXED type should still return UNKNOWN_TYPE (not throw)
        org.apache.iceberg.types.Type fixedType = Types.FixedType.ofLength(16);
        Type result = fromIcebergType(fixedType);
        Assertions.assertTrue(result.isUnknown(),
                "FIXED type should return UNKNOWN_TYPE, got: " + result);
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
    public void testToFullSchemasFailsOnUnsupportedType() {
        // Use a real GEOGRAPHY type which is not supported by StarRocks
        Types.NestedField field = Types.NestedField.optional(1, "geo_col", Types.GeographyType.crs84());
        Schema schema = new Schema(field);

        StarRocksConnectorException ex = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> IcebergApiConverter.toFullSchemas(schema));
        Assertions.assertTrue(ex.getMessage().contains("geo_col"),
                "Error should mention the column name, got: " + ex.getMessage());
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
    public void testEncryptedTableFailsFast() {
        org.apache.iceberg.Table encryptedTable = mock(org.apache.iceberg.Table.class);
        Map<String, String> encryptionProps = new HashMap<>();
        encryptionProps.put("encryption.key-metadata", "some-key-metadata");
        when(encryptedTable.properties()).thenReturn(encryptionProps);
        when(encryptedTable.name()).thenReturn("encrypted_table");

        StarRocksConnectorException ex = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> IcebergMetadata.checkUnsupportedV3Features(encryptedTable));
        Assertions.assertTrue(ex.getMessage().contains("encryption is not supported"),
                "Expected encryption error, got: " + ex.getMessage());
    }

    @Test
    public void testEncryptedTableWithDefaultAlgorithmFailsFast() {
        org.apache.iceberg.Table encryptedTable = mock(org.apache.iceberg.Table.class);
        Map<String, String> encryptionProps = new HashMap<>();
        encryptionProps.put("encryption.default-algorithm", "AES_GCM_V1");
        when(encryptedTable.properties()).thenReturn(encryptionProps);
        when(encryptedTable.name()).thenReturn("encrypted_table_2");

        StarRocksConnectorException ex = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> IcebergMetadata.checkUnsupportedV3Features(encryptedTable));
        Assertions.assertTrue(ex.getMessage().contains("encryption is not supported"),
                "Expected encryption error, got: " + ex.getMessage());
    }

    @Test
    public void testEncryptedTableWithColumnKeysFailsFast() {
        org.apache.iceberg.Table encryptedTable = mock(org.apache.iceberg.Table.class);
        Map<String, String> encryptionProps = new HashMap<>();
        encryptionProps.put("encryption.column-keys", "col1:key1,col2:key2");
        when(encryptedTable.properties()).thenReturn(encryptionProps);
        when(encryptedTable.name()).thenReturn("encrypted_table_3");

        StarRocksConnectorException ex = Assertions.assertThrows(
                StarRocksConnectorException.class,
                () -> IcebergMetadata.checkUnsupportedV3Features(encryptedTable));
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
        IcebergMetadata.checkUnsupportedV3Features(normalTable);
    }
}
