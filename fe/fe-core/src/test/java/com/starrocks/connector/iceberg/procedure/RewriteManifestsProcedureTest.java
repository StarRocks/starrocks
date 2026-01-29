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
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RewriteManifestsProcedureTest {

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    @Test
    void testProcedureCreation() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();

        assertNotNull(procedure);
        assertEquals("rewrite_manifests", procedure.getProcedureName());
        assertEquals(Collections.emptyList(), procedure.getArguments());
        assertEquals(IcebergTableOperation.REWRITE_MANIFESTS, procedure.getOperation());
    }

    @Test
    void testExecuteWithInvalidArgs() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        IcebergTableProcedureContext context = createContext(table, Mockito.mock(Transaction.class));

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("some_arg", ConstantOperator.createVarchar("value"));

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(ex.getMessage().contains("invalid args"));
        assertTrue(ex.getMessage().contains("rewrite_manifests"));
        assertTrue(ex.getMessage().contains("does not support any arguments"));
    }

    @Test
    void testExecuteWithNoSnapshot() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(null);
        IcebergTableProcedureContext context = createContext(table, Mockito.mock(Transaction.class));

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));
        verify(table, never()).io();
    }

    @Test
    void testExecuteWithEmptyManifests() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.allManifests(any())).thenReturn(Collections.emptyList());

        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(snapshot);
        IcebergTableProcedureContext context = createContext(table, Mockito.mock(Transaction.class));

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

        Transaction txn = context.transaction();
        verify(txn, never()).rewriteManifests();
    }

    @Test
    void testExecuteWithSingleSmallManifest() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        ManifestFile smallManifest = Mockito.mock(ManifestFile.class);
        when(smallManifest.length()).thenReturn(100L);

        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.allManifests(any())).thenReturn(List.of(smallManifest));

        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.properties()).thenReturn(Collections.emptyMap());
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());

        Transaction txn = Mockito.mock(Transaction.class);
        IcebergTableProcedureContext context = createContext(table, txn);

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));
        verify(txn, never()).rewriteManifests();
    }

    @Test
    void testExecuteRewriteSuccess() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();

        ManifestFile m1 = Mockito.mock(ManifestFile.class);
        ManifestFile m2 = Mockito.mock(ManifestFile.class);
        when(m1.length()).thenReturn(10 * 1024 * 1024L);
        when(m2.length()).thenReturn(10 * 1024 * 1024L);

        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.allManifests(any())).thenReturn(List.of(m1, m2));

        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.properties()).thenReturn(Collections.emptyMap());
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());

        RewriteManifests rewriteManifests = Mockito.mock(RewriteManifests.class);
        when(rewriteManifests.clusterBy(any())).thenReturn(rewriteManifests);
        doNothing().when(rewriteManifests).commit();

        Transaction txn = Mockito.mock(Transaction.class);
        when(txn.rewriteManifests()).thenReturn(rewriteManifests);

        IcebergTableProcedureContext context = createContext(table, txn);

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

        verify(txn).rewriteManifests();
        verify(rewriteManifests).clusterBy(any());
        verify(rewriteManifests).commit();
    }

    @Test
    void testExecuteWithSingleLargeManifestTriggersRewrite() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        // One manifest larger than default target (8MB) still triggers rewrite
        ManifestFile largeManifest = Mockito.mock(ManifestFile.class);
        when(largeManifest.length()).thenReturn(10 * 1024 * 1024L);

        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.allManifests(any())).thenReturn(List.of(largeManifest));

        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.properties()).thenReturn(Collections.emptyMap());
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());

        RewriteManifests rewriteManifests = Mockito.mock(RewriteManifests.class);
        when(rewriteManifests.clusterBy(any())).thenReturn(rewriteManifests);
        doNothing().when(rewriteManifests).commit();

        Transaction txn = Mockito.mock(Transaction.class);
        when(txn.rewriteManifests()).thenReturn(rewriteManifests);

        IcebergTableProcedureContext context = createContext(table, txn);

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

        verify(txn).rewriteManifests();
        verify(rewriteManifests).clusterBy(any());
        verify(rewriteManifests).commit();
    }

    private IcebergTableProcedureContext createContext(Table table, Transaction transaction) {
        IcebergHiveCatalog catalog = Mockito.mock(IcebergHiveCatalog.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        return new IcebergTableProcedureContext(catalog, table, ctx, transaction, HDFS_ENVIRONMENT, stmt, clause);
    }
}
