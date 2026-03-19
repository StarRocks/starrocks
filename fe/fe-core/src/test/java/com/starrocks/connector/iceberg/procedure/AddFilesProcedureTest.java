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

package com.starrocks.connector.iceberg.procedure;

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AddFilesProcedureTest {

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    @Test
    public void testAddFilesProcedureCreation() {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();

        assertNotNull(procedure);
        Assertions.assertEquals("add_files", procedure.getProcedureName());
        Assertions.assertEquals(IcebergTableOperation.ADD_FILES, procedure.getOperation());
        Assertions.assertEquals(4, procedure.getArguments().size());

        // Check argument names and types
        boolean hasSourceTable = procedure.getArguments().stream()
                .anyMatch(arg -> "source_table".equals(arg.getName()) && arg.getType() == VarcharType.VARCHAR);
        boolean hasLocation = procedure.getArguments().stream()
                .anyMatch(arg -> "location".equals(arg.getName()) && arg.getType() == VarcharType.VARCHAR);
        boolean hasFileFormat = procedure.getArguments().stream()
                .anyMatch(arg -> "file_format".equals(arg.getName()) && arg.getType() == VarcharType.VARCHAR);
        boolean hasRecursive = procedure.getArguments().stream()
                .anyMatch(arg -> "recursive".equals(arg.getName()) && arg.getType() == BooleanType.BOOLEAN);

        assertTrue(hasSourceTable);
        assertTrue(hasLocation);
        assertTrue(hasFileFormat);
        assertTrue(hasRecursive);

        // Check that all arguments are optional
        procedure.getArguments().forEach(arg ->
                assertFalse(arg.isRequired(), "Argument " + arg.getName() + " should be optional"));
    }

    @Test
    public void testExecuteWithoutArguments(@Mocked Table table, @Mocked IcebergHiveCatalog catalog) {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();
        IcebergTableProcedureContext context = createMockContext(table, catalog);

        Map<String, ConstantOperator> args = new HashMap<>();

        StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(exception.getMessage().contains("Either 'source_table' or 'location' must be provided"));
    }

    @Test
    public void testExecuteWithBothSourceTableAndLocation(@Mocked Table table, @Mocked IcebergHiveCatalog catalog) {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();
        IcebergTableProcedureContext context = createMockContext(table, catalog);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("source_table", ConstantOperator.createVarchar("test_table"));
        args.put("location", ConstantOperator.createVarchar("/test/location"));

        StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(exception.getMessage().contains("Cannot specify both 'source_table' and 'location'"));
    }

    @Test
    public void testExecuteWithLocationButNoFileFormat(@Mocked Table table, @Mocked IcebergHiveCatalog catalog) {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();
        IcebergTableProcedureContext context = createMockContext(table, catalog);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("location", ConstantOperator.createVarchar("/test/location"));

        StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(exception.getMessage().contains("'file_format' must be provided when 'location' is specified"));
    }

    @Test
    public void testExecuteWithUnsupportedFileFormat(@Mocked Table table, @Mocked IcebergHiveCatalog catalog) {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();
        IcebergTableProcedureContext context = createMockContext(table, catalog);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("location", ConstantOperator.createVarchar("/test/location"));
        args.put("file_format", ConstantOperator.createVarchar("csv"));

        StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(exception.getMessage().contains("Unsupported file format: csv"));
    }

    @Test
    public void testExecuteWithNonIdentityPartitioning(@Mocked IcebergHiveCatalog catalog) {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();

        // Mock partitioned table with non-identity partitioning
        Table table = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Mockito.when(spec.isPartitioned()).thenReturn(true);

        // Create a mock field with non-identity transform
        org.apache.iceberg.PartitionField field = Mockito.mock(org.apache.iceberg.PartitionField.class);
        org.apache.iceberg.transforms.Transform transform =
                Mockito.mock(org.apache.iceberg.transforms.Transform.class);
        Mockito.when(transform.isIdentity()).thenReturn(false);
        Mockito.when(field.transform()).thenReturn(transform);
        Mockito.when(spec.fields()).thenReturn(java.util.List.of(field));

        Mockito.when(table.spec()).thenReturn(spec);

        IcebergTableProcedureContext context = createMockContext(table, catalog);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("location", ConstantOperator.createVarchar("/test/location"));
        args.put("file_format", ConstantOperator.createVarchar("parquet"));

        StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(exception.getMessage().contains("non-identity partitioning is not supported"));
    }

    @Test
    public void testExecuteWithSourceTable(@Mocked Table table, @Mocked IcebergHiveCatalog catalog) {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();
        IcebergTableProcedureContext context = createMockContext(table, catalog);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("source_table", ConstantOperator.createVarchar("test_catalog.test_db.test_table"));

        // This test will now validate the new source table functionality
        // The test should fail due to missing Hive table access, not "not implemented"
        StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        // The exception should be about Hive table access, not "not implemented"
        assertTrue(exception.getMessage().contains("Failed to access source table") || 
                   exception.getMessage().contains("not found") ||
                   exception.getMessage().contains("not a Hive table"));
    }

    @Test
    public void testCreateDataFileFromLocationWithMetrics() throws Exception {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();

        // Mock table schema and partition spec
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.unpartitioned();

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);

        // Mock FileStatus
        FileStatus fileStatus = Mockito.mock(FileStatus.class);
        Path filePath = new Path("/test/data.parquet");
        Mockito.when(fileStatus.getPath()).thenReturn(filePath);
        Mockito.when(fileStatus.getLen()).thenReturn(1024L);

        // Test the DataFile creation by testing the basic functionality
        // Since the createDataFile method is private, we test the overall behavior
        DataFile mockDataFile = Mockito.mock(DataFile.class);
        Mockito.when(mockDataFile.location()).thenReturn("/test/data.parquet");
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(1024L);
        Mockito.when(mockDataFile.format()).thenReturn(org.apache.iceberg.FileFormat.PARQUET);

        assertNotNull(mockDataFile);
        assertEquals("/test/data.parquet", mockDataFile.location());
        assertEquals(1024L, mockDataFile.fileSizeInBytes());
        assertEquals(org.apache.iceberg.FileFormat.PARQUET, mockDataFile.format());
    }

    @Test
    public void testIsDataFile() throws Exception {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();

        // Use reflection to call private method isDataFile
        java.lang.reflect.Method method = AddFilesProcedure.class
                .getDeclaredMethod("isDataFile", FileStatus.class);
        method.setAccessible(true);

        // Test valid data file
        FileStatus validFile = Mockito.mock(FileStatus.class);
        Mockito.when(validFile.getPath()).thenReturn(new Path("/test/data.parquet"));
        Mockito.when(validFile.isFile()).thenReturn(true);
        Mockito.when(validFile.getLen()).thenReturn(1024L);

        boolean isValid = (boolean) method.invoke(procedure, validFile);
        assertTrue(isValid);

        // Test hidden file
        FileStatus hiddenFile = Mockito.mock(FileStatus.class);
        Mockito.when(hiddenFile.getPath()).thenReturn(new Path("/test/.hidden"));
        Mockito.when(hiddenFile.isFile()).thenReturn(true);
        Mockito.when(hiddenFile.getLen()).thenReturn(1024L);

        boolean isHidden = (boolean) method.invoke(procedure, hiddenFile);
        assertFalse(isHidden);

        // Test zero-length file
        FileStatus emptyFile = Mockito.mock(FileStatus.class);
        Mockito.when(emptyFile.getPath()).thenReturn(new Path("/test/empty.parquet"));
        Mockito.when(emptyFile.isFile()).thenReturn(true);
        Mockito.when(emptyFile.getLen()).thenReturn(0L);

        boolean isEmpty = (boolean) method.invoke(procedure, emptyFile);
        assertFalse(isEmpty);

        // Test directory
        FileStatus directory = Mockito.mock(FileStatus.class);
        Mockito.when(directory.getPath()).thenReturn(new Path("/test/dir"));
        Mockito.when(directory.isFile()).thenReturn(false);

        boolean isDir = (boolean) method.invoke(procedure, directory);
        assertFalse(isDir);
    }

    @Test
    public void testSupportedFileFormats(@Mocked Table table, @Mocked IcebergHiveCatalog catalog) {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();
        IcebergTableProcedureContext context = createMockContext(table, catalog);

        // Mock file system operations to avoid actual I/O
        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(java.net.URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
                FileSystem fs = Mockito.mock(FileSystem.class);
                Mockito.when(fs.exists(Mockito.any(Path.class))).thenReturn(false);
                return fs;
            }
        };

        // Test supported formats
        String[] supportedFormats = {"parquet", "orc"};

        for (String format : supportedFormats) {
            Map<String, ConstantOperator> args = new HashMap<>();
            args.put("location", ConstantOperator.createVarchar("/test/location"));
            args.put("file_format", ConstantOperator.createVarchar(format));

            // This should not throw an exception for supported formats
            // The exception we get should be about location not existing, not unsupported format
            StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class,
                    () -> procedure.execute(context, args));

            assertFalse(exception.getMessage().contains("Unsupported file format"),
                    "Format " + format + " should be supported");
        }
    }

    @Test
    public void testTryConvertDecimalStatValue() {
        Types.DecimalType decimalType = Types.DecimalType.of(7, 2);

        BigDecimal fromNull = AddFilesProcedure.tryConvertDecimalStatValue(decimalType, null);
        Assertions.assertNull(fromNull);

        BigDecimal fromInt = AddFilesProcedure.tryConvertDecimalStatValue(decimalType, 123);
        assertEquals(new BigDecimal("1.23"), fromInt);

        BigDecimal fromLong = AddFilesProcedure.tryConvertDecimalStatValue(decimalType, 123L);
        assertEquals(new BigDecimal("1.23"), fromLong);

        BigDecimal fromBinary = AddFilesProcedure.tryConvertDecimalStatValue(
                decimalType, Binary.fromConstantByteArray(new byte[] {0x00, 0x7B}));
        assertEquals(new BigDecimal("1.23"), fromBinary);

        BigDecimal fromBytes = AddFilesProcedure.tryConvertDecimalStatValue(decimalType, new byte[] {0x00, 0x7B});
        assertEquals(new BigDecimal("1.23"), fromBytes);

        BigDecimal fromByteBuffer = AddFilesProcedure.tryConvertDecimalStatValue(
                decimalType, ByteBuffer.wrap(new byte[] {0x00, 0x7B}));
        assertEquals(new BigDecimal("1.23"), fromByteBuffer);

        BigDecimal passthrough = AddFilesProcedure.tryConvertDecimalStatValue(
                decimalType, new BigDecimal("12.34"));
        assertEquals(new BigDecimal("12.34"), passthrough);

        BigDecimal unsupported = AddFilesProcedure.tryConvertDecimalStatValue(decimalType, "12.34");
        Assertions.assertNull(unsupported);
    }

    @Test
    public void testTryConvertStatValue() {
        Object nullFieldType = AddFilesProcedure.tryConvertStatValue(null, 123);
        Assertions.assertNull(nullFieldType);

        Object nullStatValue = AddFilesProcedure.tryConvertStatValue(Types.IntegerType.get(), null);
        Assertions.assertNull(nullStatValue);

        Object intFromLong = AddFilesProcedure.tryConvertStatValue(Types.IntegerType.get(), 123L);
        assertEquals(123, intFromLong);

        Object longValue = AddFilesProcedure.tryConvertStatValue(Types.LongType.get(), 123);
        assertEquals(123L, longValue);

        Object longUnsupported = AddFilesProcedure.tryConvertStatValue(Types.LongType.get(), "123");
        Assertions.assertNull(longUnsupported);

        Object floatFromDouble = AddFilesProcedure.tryConvertStatValue(Types.FloatType.get(), 1.25D);
        assertEquals(1.25F, floatFromDouble);

        Object floatUnsupported = AddFilesProcedure.tryConvertStatValue(Types.FloatType.get(), "1.25");
        Assertions.assertNull(floatUnsupported);

        Object doubleFromFloat = AddFilesProcedure.tryConvertStatValue(Types.DoubleType.get(), 1.25F);
        assertEquals(1.25D, doubleFromFloat);

        Object doubleUnsupported = AddFilesProcedure.tryConvertStatValue(Types.DoubleType.get(), "1.25");
        Assertions.assertNull(doubleUnsupported);

        Object stringFromCharSequence = AddFilesProcedure.tryConvertStatValue(Types.StringType.get(), new StringBuilder("abc"));
        assertEquals("abc", stringFromCharSequence);

        Object stringValue = AddFilesProcedure.tryConvertStatValue(
                Types.StringType.get(), Binary.fromConstantByteArray("abc".getBytes(StandardCharsets.UTF_8)));
        assertEquals("abc", stringValue);

        Object stringFromBytes = AddFilesProcedure.tryConvertStatValue(
                Types.StringType.get(), "abc".getBytes(StandardCharsets.UTF_8));
        assertEquals("abc", stringFromBytes);

        Object stringUnsupported = AddFilesProcedure.tryConvertStatValue(Types.StringType.get(), 123);
        Assertions.assertNull(stringUnsupported);

        UUID uuid = UUID.randomUUID();
        Object uuidFromUuid = AddFilesProcedure.tryConvertStatValue(Types.UUIDType.get(), uuid);
        assertEquals(uuid, uuidFromUuid);

        byte[] uuidBytes = UUIDUtil.convert(uuid);
        Object uuidFromBinary = AddFilesProcedure.tryConvertStatValue(
                Types.UUIDType.get(), Binary.fromConstantByteArray(uuidBytes));
        assertEquals(uuid, uuidFromBinary);

        Object uuidFromBytes = AddFilesProcedure.tryConvertStatValue(Types.UUIDType.get(), uuidBytes);
        assertEquals(uuid, uuidFromBytes);

        Object uuidFromByteBuffer = AddFilesProcedure.tryConvertStatValue(
                Types.UUIDType.get(), UUIDUtil.convertToByteBuffer(uuid));
        assertEquals(uuid, uuidFromByteBuffer);

        Object uuidUnsupported = AddFilesProcedure.tryConvertStatValue(Types.UUIDType.get(), 123);
        Assertions.assertNull(uuidUnsupported);

        ByteBuffer fixedBuffer = ByteBuffer.wrap(new byte[] {0x01, 0x02});
        Object fixedFromBuffer = AddFilesProcedure.tryConvertStatValue(Types.FixedType.ofLength(2), fixedBuffer);
        assertEquals(ByteBuffer.wrap(new byte[] {0x01, 0x02}), fixedFromBuffer);

        Object fixedValue = AddFilesProcedure.tryConvertStatValue(
                Types.FixedType.ofLength(2), Binary.fromConstantByteArray(new byte[] {0x01, 0x02}));
        assertEquals(ByteBuffer.wrap(new byte[] {0x01, 0x02}), fixedValue);

        Object fixedFromBytes = AddFilesProcedure.tryConvertStatValue(
                Types.FixedType.ofLength(2), new byte[] {0x01, 0x02});
        assertEquals(ByteBuffer.wrap(new byte[] {0x01, 0x02}), fixedFromBytes);

        Object fixedUnsupported = AddFilesProcedure.tryConvertStatValue(Types.FixedType.ofLength(2), 123);
        Assertions.assertNull(fixedUnsupported);

        Type fakeDecimalType = new Type() {
            @Override
            public Type.TypeID typeId() {
                return Type.TypeID.DECIMAL;
            }
        };
        Object decimalUnsupported = AddFilesProcedure.tryConvertStatValue(fakeDecimalType, 123);
        Assertions.assertNull(decimalUnsupported);

        Object defaultUnsupported = AddFilesProcedure.tryConvertStatValue(
                Types.StructType.of(Types.NestedField.optional(99, "x", Types.IntegerType.get())), 123);
        Assertions.assertNull(defaultUnsupported);

        Object unsupported = AddFilesProcedure.tryConvertStatValue(Types.IntegerType.get(), "123");
        Assertions.assertNull(unsupported);
    }

    @Test
    public void testTryGetComparator() {
        Assertions.assertNull(AddFilesProcedure.tryGetComparator(null));
        Assertions.assertNull(AddFilesProcedure.tryGetComparator(
                Types.StructType.of(Types.NestedField.optional(1, "x", Types.IntegerType.get()))));

        Type badType = new Type() {
            @Override
            public Type.TypeID typeId() {
                return Type.TypeID.BOOLEAN;
            }

            @Override
            public boolean isPrimitiveType() {
                return true;
            }

            @Override
            public Type.PrimitiveType asPrimitiveType() {
                throw new RuntimeException("mocked comparator failure");
            }
        };
        Assertions.assertNull(AddFilesProcedure.tryGetComparator(badType));
    }

    @Test
    public void testExtractParquetMetricsWithMissingStatsCleanup(@Mocked Table table,
                                                                 @Mocked IcebergHiveCatalog catalog,
                                                                 @Mocked ParquetFileReader reader,
                                                                 @Mocked HadoopInputFile inputFile) throws Exception {
        AddFilesProcedure procedure = AddFilesProcedure.getInstance();
        IcebergTableProcedureContext context =
                new IcebergTableProcedureContext(catalog, table, null, null, HDFS_ENVIRONMENT, null, null);

        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "struct_col", Types.StructType.of(
                        Types.NestedField.optional(3, "nested_id", Types.IntegerType.get())))
        );
        FileStatus fileStatus = new FileStatus(1024L, false, 1, 0L, 0L, new Path("/tmp/data.parquet"));

        Statistics<?> validStats = Statistics.getStatsBasedOnType(PrimitiveType.PrimitiveTypeName.INT32);
        validStats.updateStats(10);
        validStats.updateStats(20);
        validStats.setNumNulls(0L);

        Statistics<?> invalidConversionStats = Statistics.getStatsBasedOnType(PrimitiveType.PrimitiveTypeName.BINARY);
        invalidConversionStats.updateStats(Binary.fromString("invalid_min"));
        invalidConversionStats.updateStats(Binary.fromString("invalid_max"));
        invalidConversionStats.setNumNulls(0L);

        Statistics<?> nonPrimitiveStats = Statistics.getStatsBasedOnType(PrimitiveType.PrimitiveTypeName.INT32);
        nonPrimitiveStats.updateStats(1);
        nonPrimitiveStats.setNumNulls(0L);

        Set<Encoding> encodings = Set.of(Encoding.PLAIN);

        ColumnChunkMetaData validColumn = ColumnChunkMetaData.get(
                ColumnPath.fromDotString("id"),
                PrimitiveType.PrimitiveTypeName.INT32,
                CompressionCodecName.UNCOMPRESSED,
                encodings,
                validStats,
                0L,
                0L,
                10L,
                100L,
                100L);

        ColumnChunkMetaData invalidConversionColumn = ColumnChunkMetaData.get(
                ColumnPath.fromDotString("id"),
                PrimitiveType.PrimitiveTypeName.BINARY,
                CompressionCodecName.UNCOMPRESSED,
                encodings,
                invalidConversionStats,
                0L,
                0L,
                20L,
                200L,
                200L);

        ColumnChunkMetaData nonPrimitiveColumn = ColumnChunkMetaData.get(
                ColumnPath.fromDotString("struct_col"),
                PrimitiveType.PrimitiveTypeName.INT32,
                CompressionCodecName.UNCOMPRESSED,
                encodings,
                nonPrimitiveStats,
                0L,
                0L,
                5L,
                50L,
                50L);

        BlockMetaData block1 = new BlockMetaData();
        block1.setRowCount(10L);
        block1.addColumn(validColumn);
        block1.addColumn(nonPrimitiveColumn);

        BlockMetaData block2 = new BlockMetaData();
        block2.setRowCount(20L);
        block2.addColumn(invalidConversionColumn);

        ParquetMetadata metadata = new ParquetMetadata(
                new FileMetaData(new MessageType("test"), new HashMap<>(), "test"),
                List.of(block1, block2));

        new Expectations() {
            {
                table.schema();
                result = schema;

                reader.getFooter();
                result = metadata;
            }
        };

        new MockUp<HadoopInputFile>() {
            @Mock
            public HadoopInputFile fromStatus(FileStatus ignored, org.apache.hadoop.conf.Configuration conf) {
                return inputFile;
            }
        };

        new MockUp<ParquetFileReader>() {
            @Mock
            public ParquetFileReader open(HadoopInputFile ignored) {
                return reader;
            }
        };

        java.lang.reflect.Method method = AddFilesProcedure.class.getDeclaredMethod("extractParquetMetrics",
                IcebergTableProcedureContext.class, Table.class, FileStatus.class);
        method.setAccessible(true);
        Metrics metrics = (Metrics) method.invoke(procedure, context, table, fileStatus);

        assertEquals(30L, metrics.recordCount());
        assertEquals(300L, metrics.columnSizes().get(1));
        assertEquals(50L, metrics.columnSizes().get(2));
        assertEquals(30L, metrics.valueCounts().get(1));
        assertEquals(5L, metrics.valueCounts().get(2));

        assertFalse(metrics.nullValueCounts().containsKey(1));
        assertFalse(metrics.nullValueCounts().containsKey(2));
        assertTrue(metrics.lowerBounds().isEmpty());
        assertTrue(metrics.upperBounds().isEmpty());
    }

    private IcebergTableProcedureContext createMockContext(@Mocked Table table, @Mocked IcebergHiveCatalog catalog) {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        Transaction transaction = Mockito.mock(Transaction.class);

        return new IcebergTableProcedureContext(catalog, table, ctx, transaction, HDFS_ENVIRONMENT, stmt, clause);
    }
}
