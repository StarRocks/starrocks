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

import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.procedure.RemoveOrphanFilesProcedure;
import com.starrocks.connector.iceberg.procedure.RollbackToSnapshotProcedure;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ProcedureArgument;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.Snapshot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.connector.iceberg.IcebergTableOperation.REMOVE_ORPHAN_FILES;
import static com.starrocks.connector.iceberg.IcebergTableOperation.ROLLBACK_TO_SNAPSHOT;

public class IcebergTableOperationTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();
    public static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testRollbackToSnapshot(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                       @Mocked org.apache.iceberg.BaseTable table,
                                       @Mocked Snapshot snapshot) throws Exception {
        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }
        };

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return table;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName) {
                return true;
            }
        };

        new Expectations() {
            {
                snapshot.snapshotId();
                result = 1L;
                minTimes = 0;
                snapshot.parentId();
                result = null;
                minTimes = 0;
                snapshot.timestampMillis();
                result = System.currentTimeMillis();
                minTimes = 0;

                table.currentSnapshot();
                result = snapshot;
                minTimes = 0;
                table.snapshot(1L);
                result = snapshot;
                minTimes = 0;
                table.snapshots();
                result = List.of(snapshot);
                minTimes = 0;
            }
        };

        TableName tableName = new TableName(CATALOG_NAME, "db", "table");
        AlterTableOperationClause clause = new AlterTableOperationClause(
                NodePosition.ZERO, ROLLBACK_TO_SNAPSHOT.toString(),
                List.of(new ProcedureArgument(RollbackToSnapshotProcedure.SNAPSHOT_ID,
                        new IntLiteral(1, NodePosition.ZERO))), null);
        clause.setTableProcedure(RollbackToSnapshotProcedure.getInstance());
        clause.setAnalyzedArgs(Map.of(RollbackToSnapshotProcedure.SNAPSHOT_ID,
                ConstantOperator.createBigint(1)));
        IcebergAlterTableExecutor executor = new IcebergAlterTableExecutor(
                new AlterTableStmt(tableName, List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()),
                icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();
    }

    @Test
    public void testAlterTableExecuteRemoveOrphanFiles(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                                       @Mocked Snapshot snapshot) throws Exception {
        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }
        };

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableA;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName) {
                return true;
            }
        };


        // Normalize Date
        TableName tableName = new TableName(CATALOG_NAME, "db", "table");
        AlterTableOperationClause clause = new AlterTableOperationClause(NodePosition.ZERO,
                REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN,
                ConstantOperator.createChar("2024-01-01 00:00:00")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        IcebergAlterTableExecutor executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                tableName,
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();

        // Illegal date
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN,
                ConstantOperator.createChar("illegal date")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                tableName,
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        IcebergAlterTableExecutor finalExecutor = executor;
        Assertions.assertThrows(DdlException.class, finalExecutor::execute);

        // Default retention interval
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN, ConstantOperator.createChar("")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                tableName,
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        finalExecutor = executor;
        Assertions.assertThrows(DdlException.class, finalExecutor::execute);

        // Mock snapshot behavior
        new Expectations() {
            {
                snapshot.snapshotId();
                result = 1L;
                minTimes = 0;

                snapshot.timestampMillis();
                result = System.currentTimeMillis();
                minTimes = 0;

                snapshot.parentId();
                result = 0L;
                minTimes = 0;
            }
        };

        new MockUp<org.apache.iceberg.BaseTable>() {
            @Mock
            public org.apache.iceberg.Snapshot currentSnapshot() {
                return snapshot;
            }

            @Mock
            public Iterable<org.apache.iceberg.Snapshot> snapshots() {
                return List.of(snapshot, snapshot, snapshot);
            }
        };

        // inject snapshot
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN,
                ConstantOperator.createChar("2024-01-01 00:00:00")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                tableName,
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();
    }
}
