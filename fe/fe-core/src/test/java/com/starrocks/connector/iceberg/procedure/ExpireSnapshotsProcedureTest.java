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
import com.starrocks.type.DateType;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExpireSnapshotsProcedureTest {

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    @Test
    void testRetainLastInvalidTypeThrows() {
        ExpireSnapshotsProcedure procedure = ExpireSnapshotsProcedure.getInstance();
        IcebergTableProcedureContext context = createContextWithTransaction();

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(ExpireSnapshotsProcedure.RETAIN_LAST, ConstantOperator.createVarchar("not_an_int"));

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(ex.getMessage().contains("invalid argument type"));
        assertTrue(ex.getMessage().contains(ExpireSnapshotsProcedure.RETAIN_LAST));
        assertTrue(ex.getMessage().contains("expected INT"));
    }

    @Test
    void testRetainLastZeroThrows() {
        ExpireSnapshotsProcedure procedure = ExpireSnapshotsProcedure.getInstance();
        IcebergTableProcedureContext context = createContextWithTransaction();

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(ExpireSnapshotsProcedure.RETAIN_LAST, ConstantOperator.createInt(0));

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(ex.getMessage().contains("invalid argument value"));
        assertTrue(ex.getMessage().contains(ExpireSnapshotsProcedure.RETAIN_LAST));
        assertTrue(ex.getMessage().contains("must be >= 1"));
        assertTrue(ex.getMessage().contains("0"));
    }

    @Test
    void testRetainLastValidCallsRetainLastAndCommit() {
        ExpireSnapshotsProcedure procedure = ExpireSnapshotsProcedure.getInstance();
        ExpireSnapshots expireSnapshots = Mockito.mock(ExpireSnapshots.class);
        when(expireSnapshots.retainLast(anyInt())).thenReturn(expireSnapshots);
        doNothing().when(expireSnapshots).commit();

        Transaction txn = Mockito.mock(Transaction.class);
        when(txn.expireSnapshots()).thenReturn(expireSnapshots);

        IcebergTableProcedureContext context = createContextWithTransaction(txn);

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(ExpireSnapshotsProcedure.RETAIN_LAST, ConstantOperator.createInt(5));

        assertDoesNotThrow(() -> procedure.execute(context, args));

        verify(txn).expireSnapshots();
        verify(expireSnapshots).retainLast(5);
        verify(expireSnapshots, never()).expireOlderThan(anyLong());
        verify(expireSnapshots).commit();
    }

    @Test
    void testRetainLastAbsentNoRetainLastCall() {
        ExpireSnapshotsProcedure procedure = ExpireSnapshotsProcedure.getInstance();
        ExpireSnapshots expireSnapshots = Mockito.mock(ExpireSnapshots.class);
        doNothing().when(expireSnapshots).commit();

        Transaction txn = Mockito.mock(Transaction.class);
        when(txn.expireSnapshots()).thenReturn(expireSnapshots);

        IcebergTableProcedureContext context = createContextWithTransaction(txn);

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

        verify(txn).expireSnapshots();
        verify(expireSnapshots, never()).retainLast(anyInt());
        verify(expireSnapshots, never()).expireOlderThan(anyLong());
        verify(expireSnapshots).commit();
    }

    @Test
    void testRetainLastWithOlderThanBothApplied() {
        ExpireSnapshotsProcedure procedure = ExpireSnapshotsProcedure.getInstance();
        ExpireSnapshots expireSnapshots = Mockito.mock(ExpireSnapshots.class);
        when(expireSnapshots.expireOlderThan(anyLong())).thenReturn(expireSnapshots);
        when(expireSnapshots.retainLast(anyInt())).thenReturn(expireSnapshots);
        doNothing().when(expireSnapshots).commit();

        Transaction txn = Mockito.mock(Transaction.class);
        when(txn.expireSnapshots()).thenReturn(expireSnapshots);

        IcebergTableProcedureContext context = createContextWithTransaction(txn);

        LocalDateTime olderThan = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put(ExpireSnapshotsProcedure.OLDER_THAN, ConstantOperator.createDatetime(olderThan, DateType.DATETIME));
        args.put(ExpireSnapshotsProcedure.RETAIN_LAST, ConstantOperator.createInt(3));

        assertDoesNotThrow(() -> procedure.execute(context, args));

        verify(expireSnapshots).expireOlderThan(Mockito.anyLong());
        verify(expireSnapshots).retainLast(3);
        verify(expireSnapshots).commit();
    }

    private IcebergTableProcedureContext createContextWithTransaction() {
        return createContextWithTransaction(Mockito.mock(Transaction.class));
    }

    private IcebergTableProcedureContext createContextWithTransaction(Transaction transaction) {
        IcebergHiveCatalog catalog = Mockito.mock(IcebergHiveCatalog.class);
        Table table = Mockito.mock(Table.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        return new IcebergTableProcedureContext(catalog, table, ctx, transaction, HDFS_ENVIRONMENT, stmt, clause);
    }
}