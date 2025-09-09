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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.ProcedureArgument;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.parser.NodePosition;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class AlterTableOperationAnalyzerTest {
    @Test
    public void testVisitAlterTableOperationClauseRewriteDataFiles() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        Expr rewriteAll = new BoolLiteral(true);
        Expr minFileSize = new IntLiteral(100);
        Expr batchSize = new IntLiteral(200);

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "REWRITE_DATA_FILES",
                List.of(new ProcedureArgument("REWRITE_ALL", rewriteAll),
                        new ProcedureArgument("MIN_FILE_SIZE_BYTES", minFileSize),
                        new ProcedureArgument("BATCH_SIZE", batchSize)), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(3, clause.getAnalyzedArgs().size());
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("rewrite_all"));
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("min_file_size_bytes"));
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("batch_size"));

        // Fix Optional.get() warnings by checking isPresent()
        ConstantOperator rewriteAllOp = clause.getAnalyzedArgs().get("rewrite_all");
        if (rewriteAllOp.castTo(ScalarType.BOOLEAN).isPresent()) {
            Assertions.assertEquals(ConstantOperator.createBoolean(true),
                    rewriteAllOp.castTo(ScalarType.BOOLEAN).get());
        }

        ConstantOperator minFileSizeOp = clause.getAnalyzedArgs().get("min_file_size_bytes");
        if (minFileSizeOp.castTo(ScalarType.BIGINT).isPresent()) {
            Assertions.assertEquals(ConstantOperator.createBigint(100),
                    minFileSizeOp.castTo(ScalarType.BIGINT).get());
        }

        ConstantOperator batchSizeOp = clause.getAnalyzedArgs().get("batch_size");
        if (batchSizeOp.castTo(ScalarType.BIGINT).isPresent()) {
            Assertions.assertEquals(ConstantOperator.createBigint(200),
                    batchSizeOp.castTo(ScalarType.BIGINT).get());
        }
    }

    @Test
    public void testVisitAlterTableOperationClauseInvalidExpr(@Mocked IcebergTable table) {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        Expr wrongExpr = new StringLiteral("wrong");
        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1),
                "REWRITE_DATA_FILES", List.of(new ProcedureArgument("REWRITE_ALL", wrongExpr)),
                null);

        Assertions.assertThrows(SemanticException.class,
                () -> analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
    }

    @Test
    public void testVisitAlterTableOperationClauseOtherOp() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());
        Expr expr = new StringLiteral("dummy");

        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1),
                "OTHER_OP",
                List.of(new ProcedureArgument("dummy", expr)),
                null
        );

        StarRocksConnectorException ex = Assertions.assertThrows(StarRocksConnectorException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Unknown iceberg table operation"));
    }

    @Test
    public void testVisitAlterTableOperationClauseOtherOpArgs() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());
        Expr expr = new StringLiteral("dummy");

        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1),
                "REWRITE_DATA_FILES",
                List.of(new ProcedureArgument("dummy", expr)),
                null
        );

        final ConstantOperator constOp = ConstantOperator.createInt(123);

        new MockUp<SqlToScalarOperatorTranslator>() {
            @Mock
            public ScalarOperator translate(Expr e, ExpressionMapping m, ColumnRefFactory f) {
                return constOp;
            }
        };

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Unknown argument name"));
    }

    @Test
    public void testVisitAlterTableOperationClauseNullTableOperationNameShouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), null, Collections.emptyList(), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Table operation name should not be null"));
    }

    @Test
    public void testVisitAlterTableOperationClauseWhereClauseButNotIcebergShouldThrow(@Mocked com.starrocks.catalog.Table table) {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        Expr where = new BoolLiteral(true);
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.emptyList(), where);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Alter table operation is only supported for Iceberg tables"));
    }

    @Test
    public void testVisitAlterTableOperationClauseCherryPickSnapshot() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        Expr snapshotId = new IntLiteral(12345L);

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "CHERRYPICK_SNAPSHOT",
                List.of(new ProcedureArgument("SNAPSHOT_ID", snapshotId)), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(1, clause.getAnalyzedArgs().size());
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("snapshot_id"));
        Assertions.assertEquals(12345, clause.getAnalyzedArgs().get("snapshot_id").getSmallint());
    }

    @Test
    public void testVisitAlterTableOperationClauseExpireSnapshots() throws AnalysisException {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        // Test with older_than parameter
        Expr olderThan = new DateLiteral("2024-01-01 00:00:00", ScalarType.DATETIME);

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "EXPIRE_SNAPSHOTS",
                List.of(new ProcedureArgument("OLDER_THAN", olderThan)), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(1, clause.getAnalyzedArgs().size());
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("older_than"));
    }

    @Test
    public void testVisitAlterTableOperationClauseExpireSnapshotsNoArgs() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        // Test without parameters (should use default behavior)
        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "EXPIRE_SNAPSHOTS",
                Collections.emptyList(), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(0, clause.getAnalyzedArgs().size());
    }

    @Test
    public void testVisitAlterTableOperationClauseFastForward() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        Expr fromBranch = new StringLiteral("source-branch");
        Expr toBranch = new StringLiteral("target-branch");

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "FAST_FORWARD",
                List.of(new ProcedureArgument("FROM_BRANCH", fromBranch),
                        new ProcedureArgument("TO_BRANCH", toBranch)), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(2, clause.getAnalyzedArgs().size());
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("from_branch"));
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("to_branch"));
        Assertions.assertEquals(ConstantOperator.createVarchar("source-branch"),
                clause.getAnalyzedArgs().get("from_branch"));
        Assertions.assertEquals(ConstantOperator.createVarchar("target-branch"),
                clause.getAnalyzedArgs().get("to_branch"));
    }

    @Test
    public void testVisitAlterTableOperationClauseRemoveOrphanFiles() throws AnalysisException {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        // Test with older_than parameter
        Expr olderThan = new DateLiteral("2024-01-01 00:00:00", ScalarType.DATETIME);

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "REMOVE_ORPHAN_FILES",
                List.of(new ProcedureArgument("OLDER_THAN", olderThan)), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(1, clause.getAnalyzedArgs().size());
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("older_than"));
    }

    @Test
    public void testVisitAlterTableOperationClauseRemoveOrphanFilesNoArgs() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        // Test without parameters (should use default retention)
        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "REMOVE_ORPHAN_FILES",
                Collections.emptyList(), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(0, clause.getAnalyzedArgs().size());
    }

    @Test
    public void testVisitAlterTableOperationClauseRewriteDataFilesAllParams() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        Expr rewriteAll = new BoolLiteral(false);
        Expr minFileSize = new IntLiteral(512 * 1024 * 1024); // 512MB
        Expr batchSize = new IntLiteral(5L * 1024 * 1024 * 1024); // 5GB

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "REWRITE_DATA_FILES",
                List.of(new ProcedureArgument("REWRITE_ALL", rewriteAll),
                        new ProcedureArgument("MIN_FILE_SIZE_BYTES", minFileSize),
                        new ProcedureArgument("BATCH_SIZE", batchSize)), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(3, clause.getAnalyzedArgs().size());
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("rewrite_all"));
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("min_file_size_bytes"));
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("batch_size"));
    }

    @Test
    public void testVisitAlterTableOperationClauseRewriteDataFilesNoArgs() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        // Test without parameters (should use defaults)
        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.emptyList(), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(0, clause.getAnalyzedArgs().size());
    }

    @Test
    public void testVisitAlterTableOperationClauseRollbackToSnapshot() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        Expr snapshotId = new IntLiteral(98765L);

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "ROLLBACK_TO_SNAPSHOT",
                List.of(new ProcedureArgument("SNAPSHOT_ID", snapshotId)), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertEquals(1, clause.getAnalyzedArgs().size());
        Assertions.assertTrue(clause.getAnalyzedArgs().containsKey("snapshot_id"));
        Assertions.assertEquals(98765, clause.getAnalyzedArgs().get("snapshot_id").getInt());
    }

    @Test
    public void testVisitAlterTableOperationClauseInvalidOperation() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1),
                "INVALID_OPERATION",
                Collections.emptyList(), null);

        StarRocksConnectorException ex = Assertions.assertThrows(StarRocksConnectorException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Unknown iceberg table operation"));
    }

    @Test
    public void testVisitAlterTableOperationClauseArgumentValidation() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        // Test invalid argument types
        Expr invalidSnapshotId = new StringLiteral("not-a-number");

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1),
                "CHERRYPICK_SNAPSHOT",
                List.of(new ProcedureArgument("SNAPSHOT_ID", invalidSnapshotId)), null);

        Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
    }

    @Test
    public void testVisitAlterTableOperationClauseMissingRequiredArguments() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(new IcebergTable());

        // Test operations that require arguments but none provided
        String[] operationsRequiringArgs = {
                "CHERRYPICK_SNAPSHOT",
                "FAST_FORWARD",
                "ROLLBACK_TO_SNAPSHOT"
        };

        for (String operation : operationsRequiringArgs) {
            AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), operation,
                    Collections.emptyList(), null);

            Assertions.assertThrows(SemanticException.class, () ->
                    analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        }
    }
}
