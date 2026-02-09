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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class RemoveOrphanFilesProcedureTest {

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

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

    private IcebergTableProcedureContext createContext(Table table) {
        IcebergHiveCatalog catalog = Mockito.mock(IcebergHiveCatalog.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        return new IcebergTableProcedureContext(catalog, table, ctx, null, HDFS_ENVIRONMENT, stmt, clause);
    }
}