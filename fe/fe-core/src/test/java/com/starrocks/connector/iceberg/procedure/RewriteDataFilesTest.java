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
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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
        Mockito.when(where.toSql()).thenReturn("k1 = 1");
        Mockito.when(clause.getWhere()).thenReturn(where);

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        new MockUp<IcebergRewriteDataJob>() {
            @Mock
            public void prepare() {
            }

            @Mock
            public void execute() {
            }
        };

        IcebergTableProcedure procedure = RewriteDataFilesProcedure.getInstance();
        IcebergTableProcedureContext procedureContext =
                new IcebergTableProcedureContext(icebergHiveCatalog, table, ctx, null, HDFS_ENVIRONMENT, stmt, clause);
        procedure.execute(procedureContext, Map.of("rewrite_all", ConstantOperator.createBoolean(true),
                "min_file_size_bytes", ConstantOperator.createBigint(128L),
                "batch_size", ConstantOperator.createBigint(10L)));
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
        Mockito.when(where.toSql()).thenReturn("k1 = 1");
        Mockito.when(clause.getWhere()).thenReturn(where);

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        new MockUp<IcebergRewriteDataJob>() {
            @Mock
            public void prepare() {
            }

            @Mock
            public void execute() {
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


}
