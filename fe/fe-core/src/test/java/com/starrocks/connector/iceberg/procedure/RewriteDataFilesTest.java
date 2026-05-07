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
import com.starrocks.connector.iceberg.IcebergRewriteDataJob;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class RewriteDataFilesTest {
    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    @Test
    void rewriteDataFilesShouldBuildSQLAndRunJob(@Mocked org.apache.iceberg.Table table,
                                                 @Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        // --- arrange
        String catalog = "c1";
        String db = "db";
        String tbl = "table";

        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        Mockito.when(stmt.getCatalogName()).thenReturn(catalog);
        Mockito.when(stmt.getDbName()).thenReturn(db);
        Mockito.when(stmt.getTableName()).thenReturn(tbl);

        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);

        Expr where = Mockito.mock(Expr.class);
        SlotRef slot = Mockito.mock(SlotRef.class);
        Mockito.doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            List<SlotRef> list = (List<SlotRef>) inv.getArgument(1);
            list.add(slot);
            return null;
        }).when(where).collect(Mockito.eq(SlotRef.class), Mockito.anyList());
        Mockito.when(ExprToSql.toSql(where)).thenReturn("k1 = 1");
        Mockito.when(clause.getWhere()).thenReturn(where);

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        QueryState queryState = new QueryState();
        Mockito.when(ctx.getState()).thenReturn(queryState);
        new MockUp<IcebergRewriteDataJob>() {
            @Mock
            public void prepare() {
            }

            @Mock
            public IcebergRewriteDataJob.RewriteMetrics execute() {
                return new IcebergRewriteDataJob.RewriteMetrics(1, 10, 2);
            }
        };

        IcebergTableProcedure procedure = RewriteDataFilesProcedure.getInstance();
        IcebergTableProcedureContext procedureContext =
                new IcebergTableProcedureContext(icebergHiveCatalog, table, ctx, null, HDFS_ENVIRONMENT, stmt, clause);
        procedure.execute(procedureContext, Map.of("rewrite_all", ConstantOperator.createBoolean(true),
                "min_file_size_bytes", ConstantOperator.createBigint(128L),
                "batch_size", ConstantOperator.createBigint(10L)));

        Assertions.assertNotNull(queryState.getInfoMessage());
        Assertions.assertTrue(queryState.getInfoMessage().contains("input_data_files=1"));
    }

    @Test
    void rewriteDataFilesShouldWrapAndThrowWhenJobExecuteFails(@Mocked org.apache.iceberg.Table table,
                                                               @Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        // --- arrange
        String catalog = "c1";
        String db = "db";
        String tbl = "table";

        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        Mockito.when(stmt.getCatalogName()).thenReturn(catalog);
        Mockito.when(stmt.getDbName()).thenReturn(db);
        Mockito.when(stmt.getTableName()).thenReturn(tbl);

        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);

        Expr where = Mockito.mock(Expr.class);
        SlotRef slot = Mockito.mock(SlotRef.class);
        Mockito.doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            List<SlotRef> list = (List<SlotRef>) inv.getArgument(1);
            list.add(slot);
            return null;
        }).when(where).collect(Mockito.eq(SlotRef.class), Mockito.anyList());
        Mockito.when(ExprToSql.toSql(where)).thenReturn("k1 = 1");
        Mockito.when(clause.getWhere()).thenReturn(where);

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        QueryState queryState = new QueryState();
        Mockito.when(ctx.getState()).thenReturn(queryState);
        new MockUp<IcebergRewriteDataJob>() {
            @Mock
            public void prepare() {
            }

            @Mock
            public IcebergRewriteDataJob.RewriteMetrics execute() {
                throw new RuntimeException("boom");
            }
        };


        IcebergTableProcedure procedure = RewriteDataFilesProcedure.getInstance();
        IcebergTableProcedureContext procedureContext =
                new IcebergTableProcedureContext(icebergHiveCatalog, table, ctx, null, HDFS_ENVIRONMENT, stmt, clause);

        StarRocksConnectorException ex = Assertions.assertThrows(StarRocksConnectorException.class, () ->
                procedure.execute(procedureContext, Map.of("rewrite_all", ConstantOperator.createBoolean(true),
                        "min_file_size_bytes", ConstantOperator.createBigint(128L),
                        "batch_size", ConstantOperator.createBigint(10L))));
        Assertions.assertTrue(ex.getMessage().contains("db.table"));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
    }

    @Test
    void rewriteDataFilesV3ShouldSkipRowLineageWhenHiddenSessionVariableIsDisabled(
            @Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        // --- arrange: create a v3 BaseTable mock
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.formatVersion()).thenReturn(3);
        Mockito.when(ops.current()).thenReturn(metadata);
        BaseTable baseTable = Mockito.mock(BaseTable.class);
        Mockito.when(baseTable.operations()).thenReturn(ops);

        String catalog = "c1";
        String db = "db";
        String tbl = "table";

        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        Mockito.when(stmt.getCatalogName()).thenReturn(catalog);
        Mockito.when(stmt.getDbName()).thenReturn(db);
        Mockito.when(stmt.getTableName()).thenReturn(tbl);

        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        Mockito.when(clause.getWhere()).thenReturn(null);

        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableIcebergCompactionWithRowLineage(false);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        QueryState queryState = new QueryState();
        Mockito.when(ctx.getState()).thenReturn(queryState);
        Mockito.when(ctx.getSessionVariable()).thenReturn(sessionVariable);

        final String[] capturedSql = new String[1];
        try (MockedConstruction<IcebergRewriteDataJob> mocked = Mockito.mockConstruction(IcebergRewriteDataJob.class,
                (mock, context) -> {
                    capturedSql[0] = (String) context.arguments().get(0);
                    Mockito.doNothing().when(mock).prepare();
                    Mockito.when(mock.execute()).thenReturn(new IcebergRewriteDataJob.RewriteMetrics(1, 10, 2));
                })) {

            IcebergTableProcedure procedure = RewriteDataFilesProcedure.getInstance();
            IcebergTableProcedureContext procedureContext =
                    new IcebergTableProcedureContext(icebergHiveCatalog, baseTable, ctx, null,
                            HDFS_ENVIRONMENT, stmt, clause);
            procedure.execute(procedureContext, Map.of());
        }

        Assertions.assertNotNull(capturedSql[0]);
        Assertions.assertFalse(capturedSql[0].contains("_row_id"),
                "disabled hidden switch should fall back to SELECT *: " + capturedSql[0]);
    }

    @Test
    void rewriteDataFilesV3ShouldIncludeRowLineageColumnsByDefault(
            @Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.formatVersion()).thenReturn(3);
        Mockito.when(ops.current()).thenReturn(metadata);
        BaseTable baseTable = Mockito.mock(BaseTable.class);
        Mockito.when(baseTable.operations()).thenReturn(ops);

        String catalog = "c1";
        String db = "db";
        String tbl = "table";

        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        Mockito.when(stmt.getCatalogName()).thenReturn(catalog);
        Mockito.when(stmt.getDbName()).thenReturn(db);
        Mockito.when(stmt.getTableName()).thenReturn(tbl);

        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        Mockito.when(clause.getWhere()).thenReturn(null);

        SessionVariable sessionVariable = new SessionVariable();
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        QueryState queryState = new QueryState();
        Mockito.when(ctx.getState()).thenReturn(queryState);
        Mockito.when(ctx.getSessionVariable()).thenReturn(sessionVariable);

        final String[] capturedSql = new String[1];
        try (MockedConstruction<IcebergRewriteDataJob> mocked = Mockito.mockConstruction(IcebergRewriteDataJob.class,
                (mock, context) -> {
                    capturedSql[0] = (String) context.arguments().get(0);
                    Mockito.doNothing().when(mock).prepare();
                    Mockito.when(mock.execute()).thenReturn(new IcebergRewriteDataJob.RewriteMetrics(1, 10, 2));
                })) {

            IcebergTableProcedure procedure = RewriteDataFilesProcedure.getInstance();
            IcebergTableProcedureContext procedureContext =
                    new IcebergTableProcedureContext(icebergHiveCatalog, baseTable, ctx, null,
                            HDFS_ENVIRONMENT, stmt, clause);
            procedure.execute(procedureContext, Map.of());
        }

        Assertions.assertNotNull(capturedSql[0]);
        Assertions.assertTrue(capturedSql[0].contains("_row_id"),
                "default v3 rewrite should request _row_id: " + capturedSql[0]);
        Assertions.assertTrue(capturedSql[0].contains("_last_updated_sequence_number"),
                "default v3 rewrite should include sequence number: " + capturedSql[0]);
    }

    @Test
    void rewriteDataFilesV3ShouldIncludeRowLineageColumnsWhenHiddenSwitchIsEnabled(
            @Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.formatVersion()).thenReturn(3);
        Mockito.when(ops.current()).thenReturn(metadata);
        BaseTable baseTable = Mockito.mock(BaseTable.class);
        Mockito.when(baseTable.operations()).thenReturn(ops);

        String catalog = "c1";
        String db = "db";
        String tbl = "table";

        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        Mockito.when(stmt.getCatalogName()).thenReturn(catalog);
        Mockito.when(stmt.getDbName()).thenReturn(db);
        Mockito.when(stmt.getTableName()).thenReturn(tbl);

        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        Mockito.when(clause.getWhere()).thenReturn(null);

        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableIcebergCompactionWithRowLineage(true);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        QueryState queryState = new QueryState();
        Mockito.when(ctx.getState()).thenReturn(queryState);
        Mockito.when(ctx.getSessionVariable()).thenReturn(sessionVariable);

        final String[] capturedSql = new String[1];
        try (MockedConstruction<IcebergRewriteDataJob> mocked = Mockito.mockConstruction(IcebergRewriteDataJob.class,
                (mock, context) -> {
                    capturedSql[0] = (String) context.arguments().get(0);
                    Mockito.doNothing().when(mock).prepare();
                    Mockito.when(mock.execute()).thenReturn(new IcebergRewriteDataJob.RewriteMetrics(1, 10, 2));
                })) {

            IcebergTableProcedure procedure = RewriteDataFilesProcedure.getInstance();
            IcebergTableProcedureContext procedureContext =
                    new IcebergTableProcedureContext(icebergHiveCatalog, baseTable, ctx, null,
                            HDFS_ENVIRONMENT, stmt, clause);
            procedure.execute(procedureContext, Map.of());
        }

        Assertions.assertNotNull(capturedSql[0]);
        Assertions.assertTrue(capturedSql[0].contains("_row_id"),
                "enabled hidden switch should request _row_id: " + capturedSql[0]);
    }

    @Test
    void rewriteDataFilesV3ShouldNotIncludeRowLineageColumnsWhenHiddenSwitchIsDisabled(
            @Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.formatVersion()).thenReturn(3);
        Mockito.when(ops.current()).thenReturn(metadata);
        BaseTable baseTable = Mockito.mock(BaseTable.class);
        Mockito.when(baseTable.operations()).thenReturn(ops);

        String catalog = "c1";
        String db = "db";
        String tbl = "table";

        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        Mockito.when(stmt.getCatalogName()).thenReturn(catalog);
        Mockito.when(stmt.getDbName()).thenReturn(db);
        Mockito.when(stmt.getTableName()).thenReturn(tbl);

        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        Mockito.when(clause.getWhere()).thenReturn(null);

        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableIcebergCompactionWithRowLineage(false);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        QueryState queryState = new QueryState();
        Mockito.when(ctx.getState()).thenReturn(queryState);
        Mockito.when(ctx.getSessionVariable()).thenReturn(sessionVariable);

        final String[] capturedSql = new String[1];
        try (MockedConstruction<IcebergRewriteDataJob> mocked = Mockito.mockConstruction(IcebergRewriteDataJob.class,
                (mock, context) -> {
                    capturedSql[0] = (String) context.arguments().get(0);
                    Mockito.doNothing().when(mock).prepare();
                    Mockito.when(mock.execute()).thenReturn(new IcebergRewriteDataJob.RewriteMetrics(1, 10, 2));
                })) {

            IcebergTableProcedure procedure = RewriteDataFilesProcedure.getInstance();
            IcebergTableProcedureContext procedureContext =
                    new IcebergTableProcedureContext(icebergHiveCatalog, baseTable, ctx, null,
                            HDFS_ENVIRONMENT, stmt, clause);
            procedure.execute(procedureContext, Map.of());
        }

        Assertions.assertNotNull(capturedSql[0]);
        Assertions.assertFalse(capturedSql[0].contains("_row_id"),
                "disabled hidden switch should not request _row_id: " + capturedSql[0]);
    }

    @Test
    void rewriteDataFilesV2ShouldNotIncludeRowLineageColumns(
            @Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        // --- arrange: v2 table (row lineage is v3-only feature)
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.formatVersion()).thenReturn(2);
        Mockito.when(ops.current()).thenReturn(metadata);
        BaseTable baseTable = Mockito.mock(BaseTable.class);
        Mockito.when(baseTable.operations()).thenReturn(ops);

        String catalog = "c1";
        String db = "db";
        String tbl = "table";

        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        Mockito.when(stmt.getCatalogName()).thenReturn(catalog);
        Mockito.when(stmt.getDbName()).thenReturn(db);
        Mockito.when(stmt.getTableName()).thenReturn(tbl);

        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        Mockito.when(clause.getWhere()).thenReturn(null);

        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableIcebergCompactionWithRowLineage(true);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        QueryState queryState = new QueryState();
        Mockito.when(ctx.getState()).thenReturn(queryState);
        Mockito.when(ctx.getSessionVariable()).thenReturn(sessionVariable);

        final String[] capturedSql = new String[1];
        try (MockedConstruction<IcebergRewriteDataJob> mocked = Mockito.mockConstruction(IcebergRewriteDataJob.class,
                (mock, context) -> {
                    capturedSql[0] = (String) context.arguments().get(0);
                    Mockito.doNothing().when(mock).prepare();
                    Mockito.when(mock.execute()).thenReturn(new IcebergRewriteDataJob.RewriteMetrics(1, 10, 2));
                })) {

            IcebergTableProcedure procedure = RewriteDataFilesProcedure.getInstance();
            IcebergTableProcedureContext procedureContext =
                    new IcebergTableProcedureContext(icebergHiveCatalog, baseTable, ctx, null,
                            HDFS_ENVIRONMENT, stmt, clause);
            procedure.execute(procedureContext, Map.of());
        }

        // v2 table should NOT include row lineage columns regardless of session variable
        Assertions.assertNotNull(capturedSql[0]);
        Assertions.assertFalse(capturedSql[0].contains("_row_id"),
                "SQL should NOT include _row_id for v2 table: " + capturedSql[0]);
    }
}
