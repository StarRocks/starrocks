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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterTableOperationClause;
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

public class AlterTableOperationTest {

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
        Assertions.assertEquals(clause.getAnalyzedArgs().get("rewrite_all").castTo(ScalarType.BOOLEAN).get(),
                ConstantOperator.createBoolean(true));
        Assertions.assertEquals(clause.getAnalyzedArgs().get("min_file_size_bytes").castTo(ScalarType.BIGINT).get(),
                ConstantOperator.createBigint(100));
        Assertions.assertEquals(clause.getAnalyzedArgs().get("batch_size").castTo(ScalarType.BIGINT).get(),
                ConstantOperator.createBigint(200));
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

        StarRocksConnectorException ex = Assertions.assertThrows(StarRocksConnectorException.class, ()->
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

        SemanticException ex =  Assertions.assertThrows(SemanticException.class, () ->
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
}
