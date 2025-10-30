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

package com.starrocks.analysis;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.iceberg.procedure.CherryPickSnapshotProcedure;
import com.starrocks.connector.iceberg.procedure.ExpireSnapshotsProcedure;
import com.starrocks.connector.iceberg.procedure.FastForwardProcedure;
import com.starrocks.connector.iceberg.procedure.RemoveOrphanFilesProcedure;
import com.starrocks.connector.iceberg.procedure.RewriteDataFilesProcedure;
import com.starrocks.connector.iceberg.procedure.RollbackToSnapshotProcedure;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class AlterTableOperationStmtTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp(@Mocked MetadataMgr metadataMgr,
                      @Mocked CatalogMgr catalogMgr) throws Exception {
        Table icebergTable = new IcebergTable(1, "test_table", "iceberg_catalog", "iceberg_catalog",
                "iceberg_db", "test_table", "",
                List.of(new Column("k1", Type.INT, true), new Column("partition_date", Type.DATE, true)),
                null, Maps.newHashMap());

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                GlobalStateMgr.getCurrentState().getCatalogMgr();
                result = catalogMgr;
                minTimes = 0;

                metadataMgr.getTemporaryTable((UUID)any, anyString, anyLong, anyString);
                result = null;
                minTimes = 0;

                metadataMgr.getTable((ConnectContext) any, anyString, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testCherryPickSnapshotProcedure() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE cherrypick_snapshot(snapshot_id = 12345)";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
        Assertions.assertEquals("iceberg_db", alterStmt.getDbName());
        Assertions.assertEquals("iceberg_catalog", alterStmt.getCatalogName());
    }

    @Test
    public void testCherryPickSnapshotProcedureWithPositionalArgs() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE cherrypick_snapshot(12345)";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testExpireSnapshotsProcedure() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE expire_snapshots(older_than = '2024-01-01 00:00:00')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testExpireSnapshotsProcedureNoArgs() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE expire_snapshots()";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testFastForwardProcedure() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE fast_forward(from_branch = 'source', to_branch = 'target')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testFastForwardProcedureWithPositionalArgs() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE fast_forward('source', 'target')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testRemoveOrphanFilesProcedure() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE remove_orphan_files(older_than = '2024-01-01 00:00:00')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testRemoveOrphanFilesProcedureNoArgs() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE remove_orphan_files()";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testRewriteDataFilesProcedure() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE rewrite_data_files(rewrite_all = true, " +
                "min_file_size_bytes = 268435456, batch_size = 5368709120)";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testRewriteDataFilesProcedurePartialArgs() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE rewrite_data_files(rewrite_all = false)";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testRewriteDataFilesProcedureNoArgs() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE rewrite_data_files()";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testRewriteDataFilesProcedureWithWhereClause(@Mocked IcebergTable icebergTable) {
        new MockUp<ExpressionAnalyzer>() {
            @Mock
            public void analyzeExpression(Expr expr, AnalyzeState analyzeState, Scope scope, ConnectContext context) {
                // do nothing
            }
        };
        new MockUp<SqlToScalarOperatorTranslator>() {
            @Mock
            public ScalarOperator translate(Expr expr, ExpressionMapping expressionMapping,
                                            ColumnRefFactory columnRefFactory) {
                return new BinaryPredicateOperator(BinaryType.GE, new ColumnRefOperator(1, Type.DATE, "partition_date",
                        true),
                        ConstantOperator.createVarchar("2024-01-01"));
            }
        };

        new Expectations() {
            {
                icebergTable.getTableProcedure(anyString);
                result = RewriteDataFilesProcedure.getInstance();
                minTimes = 0;

                icebergTable.getPartitionColumnsIncludeTransformed();
                result = List.of(new Column("k1", Type.INT, true),
                        new Column("partition_date", Type.DATE, true));
                minTimes = 0;
            }
        };

        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE rewrite_data_files() " +
                "WHERE partition_date > '2024-01-01'";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
        AlterTableOperationClause clause = (AlterTableOperationClause) alterStmt.getAlterClauseList().get(0);
        Assertions.assertNotNull(clause.getPartitionFilter());
        Assertions.assertEquals("col >= 2024-01-01", clause.getPartitionFilter().debugString());
    }

    @Test
    public void testRollbackToSnapshotProcedure() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE rollback_to_snapshot(snapshot_id = 98765)";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testRollbackToSnapshotProcedureWithPositionalArgs() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE rollback_to_snapshot(98765)";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alterStmt = (AlterTableStmt) stmt;
        Assertions.assertEquals("test_table", alterStmt.getTableName());
    }

    @Test
    public void testCaseInsensitiveProcedureNames() {
        String[] procedures = {
            "EXPIRE_SNAPSHOTS",
            "expire_snapshots",
            "REMOVE_ORPHAN_FILES",
            "remove_orphan_files",
            "REWRITE_DATA_FILES",
            "rewrite_data_files",
        };

        for (String procedure : procedures) {
            String sql = String.format("ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE %s()", procedure);
            StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
            Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        }

        procedures = new String[] {
            "CHERRYPICK_SNAPSHOT",
            "cherrypick_snapshot",
            "Cherrypick_Snapshot",
            "ROLLBACK_TO_SNAPSHOT",
            "rollback_to_snapshot"
        };

        for (String procedure : procedures) {
            String sql = String.format("ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE %s(snapshot_id = 1234)",
                    procedure);
            StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
            Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        }

        procedures = new String[] {
                "FAST_FORWARD",
                "fast_forward"
        };

        for (String procedure : procedures) {
            String sql = String.format("ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE %s(from_branch = 'source', " +
                    "to_branch = 'target')", procedure);
            StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
            Assertions.assertInstanceOf(AlterTableStmt.class, stmt);
        }
    }

    @Test
    public void testInvalidProcedureName() {
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE invalid_procedure()";
        AnalyzeTestUtil.analyzeFail(sql);
    }

    @Test
    public void testMixedArgumentTypes() {
        // Test mixing positional and named arguments should fail
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE fast_forward('source', to_branch = 'target')";
        AnalyzeTestUtil.analyzeFail(sql);
    }

    @Test
    public void testInvalidArgumentValues() {
        // Test invalid argument types
        String sql = "ALTER TABLE iceberg_catalog.iceberg_db.test_table EXECUTE cherrypick_snapshot(snapshot_id = 'invalid')";
        // This should be handled by the analyzer or at runtime
        AnalyzeTestUtil.analyzeFail(sql);
    }

    @Test
    public void testProcedureArgumentValidation() {
        // Test procedure instances and their argument validation
        CherryPickSnapshotProcedure cherryPickProc = CherryPickSnapshotProcedure.getInstance();
        Assertions.assertEquals("cherrypick_snapshot", cherryPickProc.getProcedureName());
        Assertions.assertEquals(1, cherryPickProc.getArguments().size());
        Assertions.assertEquals("snapshot_id", cherryPickProc.getArguments().get(0).getName());
        Assertions.assertTrue(cherryPickProc.getArguments().get(0).isRequired());

        ExpireSnapshotsProcedure expireProc = ExpireSnapshotsProcedure.getInstance();
        Assertions.assertEquals("expire_snapshots", expireProc.getProcedureName());
        Assertions.assertEquals(1, expireProc.getArguments().size());
        Assertions.assertEquals("older_than", expireProc.getArguments().get(0).getName());
        Assertions.assertFalse(expireProc.getArguments().get(0).isRequired());

        FastForwardProcedure fastForwardProc = FastForwardProcedure.getInstance();
        Assertions.assertEquals("fast_forward", fastForwardProc.getProcedureName());
        Assertions.assertEquals(2, fastForwardProc.getArguments().size());

        RemoveOrphanFilesProcedure removeOrphanProc = RemoveOrphanFilesProcedure.getInstance();
        Assertions.assertEquals("remove_orphan_files", removeOrphanProc.getProcedureName());
        Assertions.assertEquals(1, removeOrphanProc.getArguments().size());

        RewriteDataFilesProcedure rewriteProc = RewriteDataFilesProcedure.getInstance();
        Assertions.assertEquals("rewrite_data_files", rewriteProc.getProcedureName());
        Assertions.assertEquals(4, rewriteProc.getArguments().size());

        RollbackToSnapshotProcedure rollbackProc = RollbackToSnapshotProcedure.getInstance();
        Assertions.assertEquals("rollback_to_snapshot", rollbackProc.getProcedureName());
        Assertions.assertEquals(1, rollbackProc.getArguments().size());
        Assertions.assertEquals("snapshot_id", rollbackProc.getArguments().get(0).getName());
        Assertions.assertTrue(rollbackProc.getArguments().get(0).isRequired());
    }

    @Test
    public void testDifferentTableReferences() {
        // Test without catalog prefix
        String sql1 = "ALTER TABLE iceberg_db.test_table EXECUTE expire_snapshots()";
        StatementBase stmt1 = AnalyzeTestUtil.analyzeSuccess(sql1);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt1);

        // Test without database prefix
        String sql2 = "ALTER TABLE test_table EXECUTE expire_snapshots()";
        StatementBase stmt2 = AnalyzeTestUtil.analyzeSuccess(sql2);
        Assertions.assertInstanceOf(AlterTableStmt.class, stmt2);
    }
}
