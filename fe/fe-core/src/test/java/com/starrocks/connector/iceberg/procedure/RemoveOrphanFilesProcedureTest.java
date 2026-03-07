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
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoveOrphanFilesProcedureTest {

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    private static final String TABLE_LOCATION = "s3://bucket/table";
    private static final String MANIFEST_LIST_LOCATION = "s3://bucket/table/metadata/snap-123-manifest-list.avro";

    @Test
    void testTableLocationEmptyThrows() {
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        when(table.location()).thenReturn("");
        when(table.currentSnapshot()).thenReturn(snapshot);

        IcebergTableProcedureContext context = createContext(table);

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, Collections.emptyMap()));

        assertTrue(ex.getMessage().contains("table location is empty"));
    }

    @Test
    void testLocationEmptyThrows() {
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        when(table.location()).thenReturn("s3://bucket/table");
        when(table.currentSnapshot()).thenReturn(snapshot);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(RemoveOrphanFilesProcedure.LOCATION, ConstantOperator.createVarchar(""));

        IcebergTableProcedureContext context = createContext(table);

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(ex.getMessage().contains("invalid argument value"));
        assertTrue(ex.getMessage().contains(RemoveOrphanFilesProcedure.LOCATION));
        assertTrue(ex.getMessage().contains("expected non-empty string"));
    }

    @Test
    void testLocationNotSubdirOfTableThrows() {
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        when(table.location()).thenReturn("s3://bucket/table");
        when(table.currentSnapshot()).thenReturn(snapshot);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(RemoveOrphanFilesProcedure.LOCATION, ConstantOperator.createVarchar("s3://other/path"));

        IcebergTableProcedureContext context = createContext(table);

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(ex.getMessage().contains("invalid argument value"));
        assertTrue(ex.getMessage().contains(RemoveOrphanFilesProcedure.LOCATION));
        assertTrue(ex.getMessage().contains("must be a subdirectory of"));
        assertTrue(ex.getMessage().contains("table location"));
        assertTrue(ex.getMessage().contains("s3://bucket/table"));
        assertTrue(ex.getMessage().contains("s3://other/path"));
    }

    @Test
    void testLocationSiblingPathRejected() {
        // location that shares prefix but is not a subdirectory (e.g. table2 vs table) must be rejected
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        when(table.location()).thenReturn("s3://bucket/table");
        when(table.currentSnapshot()).thenReturn(snapshot);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(RemoveOrphanFilesProcedure.LOCATION, ConstantOperator.createVarchar("s3://bucket/table2"));

        IcebergTableProcedureContext context = createContext(table);

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(ex.getMessage().contains("invalid argument value"));
        assertTrue(ex.getMessage().contains(RemoveOrphanFilesProcedure.LOCATION));
        assertTrue(ex.getMessage().contains("must be a subdirectory of"));
        assertTrue(ex.getMessage().contains("s3://bucket/table"));
        assertTrue(ex.getMessage().contains("s3://bucket/table2"));
    }

    @Test
    void testLocationValidSubdirPassesValidation() {
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        when(table.location()).thenReturn("s3://bucket/table");
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.snapshots()).thenReturn(Collections.emptyList());

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(RemoveOrphanFilesProcedure.LOCATION, ConstantOperator.createVarchar("s3://bucket/table/data"));

        IcebergTableProcedureContext context = createContext(table);

        // Location validation passes; any exception comes from later logic (metadata/filesystem),
        // not from "expected non-empty string" or "must be a subdirectory".
        Throwable t = assertThrows(Throwable.class, () -> procedure.execute(context, args));
        String msg = t.getMessage();
        assertFalse(msg != null && (msg.contains("expected non-empty string") || msg.contains("must be a subdirectory")),
                "location validation should pass; got: " + msg);
    }

    @Test
    void testLocationEqualsTableLocationPassesValidation() {
        // When location exactly equals table location, validateAndResolveScanLocation returns early
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        when(table.location()).thenReturn("oss://bucket/table");
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.snapshots()).thenReturn(Collections.emptyList());

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(RemoveOrphanFilesProcedure.LOCATION, ConstantOperator.createVarchar("oss://bucket/table"));

        IcebergTableProcedureContext context = createContext(table);

        Throwable t = assertThrows(Throwable.class, () -> procedure.execute(context, args));
        String msg = t.getMessage();
        assertFalse(msg != null && (msg.contains("expected non-empty string") || msg.contains("must be a subdirectory")),
                "location equals table location should pass validation; got: " + msg);
    }

    @Test
    void testLocationWithTrailingSlashPassesValidation() {
        // Trailing slash in location is normalized (stripTrailingSlash); validation still passes
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        when(table.location()).thenReturn("s3://bucket/table");
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.snapshots()).thenReturn(Collections.emptyList());

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(RemoveOrphanFilesProcedure.LOCATION, ConstantOperator.createVarchar("s3://bucket/table/data/"));

        IcebergTableProcedureContext context = createContext(table);

        Throwable t = assertThrows(Throwable.class, () -> procedure.execute(context, args));
        String msg = t.getMessage();
        assertFalse(msg != null && (msg.contains("expected non-empty string") || msg.contains("must be a subdirectory")),
                "location with trailing slash should pass validation; got: " + msg);
    }

    @Test
    void testLocationDifferentSchemeThrows() {
        // Scheme must match (e.g. s3 vs oss)
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        when(table.location()).thenReturn("s3://bucket/table");
        when(table.currentSnapshot()).thenReturn(snapshot);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(RemoveOrphanFilesProcedure.LOCATION, ConstantOperator.createVarchar("oss://bucket/table/data"));

        IcebergTableProcedureContext context = createContext(table);

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(ex.getMessage().contains("invalid argument value"));
        assertTrue(ex.getMessage().contains(RemoveOrphanFilesProcedure.LOCATION));
        assertTrue(ex.getMessage().contains("must be a subdirectory of"));
    }

    @Test
    void testLocationDifferentAuthorityThrows() {
        // Authority (e.g. bucket) must match
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        when(table.location()).thenReturn("s3://bucket-a/table");
        when(table.currentSnapshot()).thenReturn(snapshot);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(RemoveOrphanFilesProcedure.LOCATION, ConstantOperator.createVarchar("s3://bucket-b/table/data"));

        IcebergTableProcedureContext context = createContext(table);

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(ex.getMessage().contains("invalid argument value"));
        assertTrue(ex.getMessage().contains(RemoveOrphanFilesProcedure.LOCATION));
        assertTrue(ex.getMessage().contains("must be a subdirectory of"));
    }

    /**
     * When snapshot has no manifest list (manifestListLocation is null), the procedure uses
     * the fallback path: readManifests returns CloseableIterable.withNoopClose(snapshot.allManifests(io)).
     * This test verifies that snapshot.allManifests(any()) is invoked, i.e. the fallback path is used.
     */
    @Test
    void testExecuteWithSnapshotsWithoutManifestListUsesAllManifestsFallback() throws Exception {
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();

        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.manifestListLocation()).thenReturn(null);
        when(snapshot.allManifests(any(FileIO.class))).thenReturn(Collections.emptyList());

        FileIO fileIO = Mockito.mock(FileIO.class);
        Table table = Mockito.mock(Table.class);
        when(table.location()).thenReturn(TABLE_LOCATION);
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.snapshots()).thenReturn(Collections.singletonList(snapshot));
        when(table.io()).thenReturn(fileIO);

        try (MockedStatic<org.apache.iceberg.ReachableFileUtil> reachableUtil =
                        mockStatic(org.apache.iceberg.ReachableFileUtil.class);
                MockedStatic<FileSystem> fsStatic = mockStatic(FileSystem.class)) {

            reachableUtil.when(() -> org.apache.iceberg.ReachableFileUtil.metadataFileLocations(any(Table.class), eq(false)))
                    .thenReturn(Collections.emptySet());
            reachableUtil.when(() -> org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations(any(Table.class)))
                    .thenReturn(Collections.emptyList());

            FileSystem mockFs = Mockito.mock(FileSystem.class);
            RemoteIterator<LocatedFileStatus> emptyIterator = new RemoteIterator<LocatedFileStatus>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public LocatedFileStatus next() {
                    throw new java.util.NoSuchElementException();
                }
            };
            when(mockFs.listFiles(any(Path.class), eq(true))).thenReturn(emptyIterator);
            fsStatic.when(() -> FileSystem.get(any(), any())).thenReturn(mockFs);

            IcebergTableProcedureContext context = createContext(table);

            assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

            verify(snapshot).allManifests(any(FileIO.class));
        }
    }

    /**
     * When snapshot has a manifest list location, the procedure reads manifests from the AVRO file
     * via InternalData.read(...).build() instead of snapshot.allManifests(). This test mocks
     * InternalData.read() to return an empty iterable and verifies the AVRO path is taken
     * (allManifests is not called, and newInputFile(manifestListLocation) is called).
     */
    @Test
    @SuppressWarnings("unchecked")
    void testExecuteWithManifestListLocationReadsFromAvroPath() throws Exception {
        RemoveOrphanFilesProcedure procedure = RemoveOrphanFilesProcedure.getInstance();

        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.manifestListLocation()).thenReturn(MANIFEST_LIST_LOCATION);

        InputFile mockInputFile = mock(InputFile.class);
        FileIO fileIO = mock(FileIO.class);
        when(fileIO.newInputFile(MANIFEST_LIST_LOCATION)).thenReturn(mockInputFile);

        InternalData.ReadBuilder mockBuilder = mock(InternalData.ReadBuilder.class);
        when(mockBuilder.setRootType(any(Class.class))).thenReturn(mockBuilder);
        when(mockBuilder.project(any(org.apache.iceberg.Schema.class))).thenReturn(mockBuilder);
        when(mockBuilder.reuseContainers()).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(CloseableIterable.withNoopClose(Collections.emptyList()));

        Table table = mock(Table.class);
        when(table.location()).thenReturn(TABLE_LOCATION);
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.snapshots()).thenReturn(Collections.singletonList(snapshot));
        when(table.io()).thenReturn(fileIO);

        try (MockedStatic<InternalData> internalDataMock = mockStatic(InternalData.class);
                MockedStatic<org.apache.iceberg.ReachableFileUtil> reachableUtil =
                        mockStatic(org.apache.iceberg.ReachableFileUtil.class);
                MockedStatic<FileSystem> fsStatic = mockStatic(FileSystem.class)) {

            internalDataMock.when(() -> InternalData.read(any(org.apache.iceberg.FileFormat.class), any(InputFile.class)))
                    .thenReturn(mockBuilder);

            reachableUtil.when(() -> org.apache.iceberg.ReachableFileUtil.metadataFileLocations(any(Table.class), eq(false)))
                    .thenReturn(Collections.emptySet());
            reachableUtil.when(() -> org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations(any(Table.class)))
                    .thenReturn(Collections.emptyList());

            FileSystem mockFs = mock(FileSystem.class);
            RemoteIterator<LocatedFileStatus> emptyIterator = new RemoteIterator<LocatedFileStatus>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public LocatedFileStatus next() {
                    throw new java.util.NoSuchElementException();
                }
            };
            when(mockFs.listFiles(any(Path.class), eq(true))).thenReturn(emptyIterator);
            fsStatic.when(() -> FileSystem.get(any(), any())).thenReturn(mockFs);

            IcebergTableProcedureContext context = createContext(table);

            assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

            verify(fileIO).newInputFile(MANIFEST_LIST_LOCATION);
            verify(snapshot, never()).allManifests(any(FileIO.class));
        }
    }

    private IcebergTableProcedureContext createContext(Table table) {
        IcebergHiveCatalog catalog = Mockito.mock(IcebergHiveCatalog.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        return new IcebergTableProcedureContext(catalog, table, ctx, null, HDFS_ENVIRONMENT, stmt, clause);
    }
}