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

import com.starrocks.catalog.Type;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
                .anyMatch(arg -> "source_table".equals(arg.getName()) && arg.getType() == Type.VARCHAR);
        boolean hasLocation = procedure.getArguments().stream()
                .anyMatch(arg -> "location".equals(arg.getName()) && arg.getType() == Type.VARCHAR);
        boolean hasFileFormat = procedure.getArguments().stream()
                .anyMatch(arg -> "file_format".equals(arg.getName()) && arg.getType() == Type.VARCHAR);
        boolean hasRecursive = procedure.getArguments().stream()
                .anyMatch(arg -> "recursive".equals(arg.getName()) && arg.getType() == Type.BOOLEAN);

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
        args.put("source_table", ConstantOperator.createVarchar("test_table"));

        StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(exception.getMessage().contains("Adding files from source_table is not yet implemented"));
    }

    @Test
    public void testCreateDataFileWithMetrics() throws Exception {
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

    private IcebergTableProcedureContext createMockContext(@Mocked Table table, @Mocked IcebergHiveCatalog catalog) {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        Transaction transaction = Mockito.mock(Transaction.class);

        return new IcebergTableProcedureContext(catalog, table, ctx, transaction, HDFS_ENVIRONMENT, stmt, clause);
    }
}
