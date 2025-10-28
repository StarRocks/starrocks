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

package com.starrocks.sql;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatementPlannerTest extends PlanTestBase {

    @Test
    public void testDeferLock() throws Exception {
        {
            FeConstants.runningUnitTest = true;
            String sql = "insert into t0 select * from t0";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
            assertTrue(StatementPlanner.analyzeStatement(stmt, connectContext, locker));
        }

        {
            FeConstants.runningUnitTest = false;
            String sql = "insert into t0 select * from t0";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
            assertFalse(StatementPlanner.analyzeStatement(stmt, connectContext, locker));
        }

        {
            FeConstants.runningUnitTest = true;
            String sql = "submit task as insert into t0 select * from t0";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
            assertTrue(StatementPlanner.analyzeStatement(stmt, connectContext, locker));
        }
    }

    @Test
    public void testFilesTableMixedWithNormalTableForcesLockAndCachesSchema() throws Exception {
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = false;
            String sql = "insert into t0 select t.v1, t.v2, t.v3 "
                    + "from files(\"path\"=\"fake://test\", \"format\"=\"csv\") as f "
                    + "join t0 as t on true";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);

            List<FileTableFunctionRelation> preRelations =
                    AnalyzerUtils.collectFileTableFunctionRelation(((InsertStmt) stmt).getQueryStatement());
            assertEquals(1, preRelations.size());
            FileTableFunctionRelation relation = preRelations.get(0);
            Consumer<TableFunctionTable> original = relation.getPushDownSchemaFunc();
            AtomicInteger analyzeCount = new AtomicInteger();
            relation.setPushDownSchemaFunc(table -> {
                analyzeCount.incrementAndGet();
                if (original != null) {
                    original.accept(table);
                }
            });

            boolean deferred = StatementPlanner.analyzeStatement(stmt, connectContext, locker);
            assertFalse(deferred);

            List<FileTableFunctionRelation> relations =
                    AnalyzerUtils.collectFileTableFunctionRelation(((InsertStmt) stmt).getQueryStatement());
            assertEquals(1, relations.size());
            assertNotNull(relations.get(0).getTable());
            assertEquals(1, analyzeCount.get());
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }

    @Test
    public void testPreAnalyzedFilesTableStillAppliesPushDown() throws Exception {
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = false;
            String sql = "insert into t0 select 1, 2, 3 "
                    + "from files(\"path\"=\"fake://test\", \"format\"=\"csv\") as f";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            QueryStatement queryStatement = ((InsertStmt) stmt).getQueryStatement();

            List<FileTableFunctionRelation> relations =
                    AnalyzerUtils.collectFileTableFunctionRelation(queryStatement);
            assertEquals(1, relations.size());
            FileTableFunctionRelation relation = relations.get(0);

            new QueryAnalyzer(connectContext).analyzeFilesOnly(queryStatement);
            TableFunctionTable tableBefore = (TableFunctionTable) relation.getTable();
            assertNotNull(tableBefore);
            assertEquals(2, tableBefore.getFullSchema().size());
            assertEquals(Type.INT, tableBefore.getFullSchema().get(0).getType());

            AtomicBoolean pushDownApplied = new AtomicBoolean(false);
            relation.setPushDownSchemaFunc(table -> {
                pushDownApplied.set(true);
                List<Column> newSchema = table.getFullSchema().stream()
                        .map(Column::new)
                        .collect(Collectors.toList());
                newSchema.forEach(column -> column.setType(Type.BIGINT));
                table.setNewFullSchema(newSchema);
            });

            new QueryAnalyzer(connectContext).analyze(queryStatement);

            TableFunctionTable tableAfter = (TableFunctionTable) relation.getTable();
            assertTrue(pushDownApplied.get());
            assertEquals(Type.BIGINT, tableAfter.getFullSchema().get(0).getType());
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }

    @Test
    public void testFilesOnlyVisitorDoesNotForceLateralForListing() throws Exception {
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = false;
            String sql = "select * from (select 1) t cross join "
                    + "files(\"path\"=\"fake://test\", \"list_files_only\"=\"true\") as f";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            QueryStatement queryStatement = (QueryStatement) stmt;
            SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
            JoinRelation joinRelation = (JoinRelation) selectRelation.getRelation();

            assertFalse(joinRelation.isLateral());

            new QueryAnalyzer(connectContext).analyzeFilesOnly(queryStatement);

            assertFalse(joinRelation.isLateral());
            Relation rightAfterFilesOnly = joinRelation.getRight();
            if (rightAfterFilesOnly instanceof FileTableFunctionRelation) {
                TableFunctionTable table = (TableFunctionTable) ((FileTableFunctionRelation) rightAfterFilesOnly).getTable();
                assertNotNull(table);
                assertTrue(table.isListFilesOnly());
            } else {
                assertTrue(rightAfterFilesOnly instanceof ValuesRelation,
                        "Unexpected right relation after files-only analysis: " + rightAfterFilesOnly.getClass());
            }

            new QueryAnalyzer(connectContext).analyze(queryStatement);

            assertFalse(joinRelation.isLateral());
            assertTrue(joinRelation.getRight() instanceof ValuesRelation);
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }

    @Test
    public void testFilesOnlyVisitorDoesNotForceLateralForLoad() throws Exception {
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = false;
            String sql = "select * from (select 1) t join "
                    + "files(\"path\"=\"fake://test\", \"format\"=\"csv\") as f on true";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            QueryStatement queryStatement = (QueryStatement) stmt;
            SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
            JoinRelation joinRelation = (JoinRelation) selectRelation.getRelation();

            assertFalse(joinRelation.isLateral());

            new QueryAnalyzer(connectContext).analyzeFilesOnly(queryStatement);

            assertFalse(joinRelation.isLateral());
            assertTrue(joinRelation.getRight() instanceof FileTableFunctionRelation);
            TableFunctionTable table =
                    (TableFunctionTable) ((FileTableFunctionRelation) joinRelation.getRight()).getTable();
            assertNotNull(table);
            assertFalse(table.isListFilesOnly());

            new QueryAnalyzer(connectContext).analyze(queryStatement);

            assertFalse(joinRelation.isLateral());
            assertTrue(joinRelation.getRight() instanceof FileTableFunctionRelation);
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }

    @Test
    public void testFilesOnlyVisitorHandlesNestedFilesJoins() throws Exception {
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = false;
            String sql = "select * from files(\"path\"=\"fake://left\", \"format\"=\"csv\") as f "
                    + "join files(\"path\"=\"fake://mid\", \"format\"=\"csv\") as m on true "
                    + "join files(\"path\"=\"fake://right\", \"format\"=\"csv\") as r on true";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            QueryStatement queryStatement = (QueryStatement) stmt;

            new QueryAnalyzer(connectContext).analyzeFilesOnly(queryStatement);

            SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
            JoinRelation outerJoin = (JoinRelation) selectRelation.getRelation();

            assertTrue(outerJoin.getRight() instanceof FileTableFunctionRelation);
            assertNotNull(((FileTableFunctionRelation) outerJoin.getRight()).getTable());

            assertTrue(outerJoin.getLeft() instanceof JoinRelation);
            JoinRelation innerJoin = (JoinRelation) outerJoin.getLeft();

            assertTrue(innerJoin.getLeft() instanceof FileTableFunctionRelation);
            assertNotNull(((FileTableFunctionRelation) innerJoin.getLeft()).getTable());

            assertTrue(innerJoin.getRight() instanceof FileTableFunctionRelation);
            assertNotNull(((FileTableFunctionRelation) innerJoin.getRight()).getTable());
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }

    @Test
    public void testFilesOnlyVisitorTraversesSubqueryAndSetOp() throws Exception {
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = false;
            String sql = "select * from (select * from files(\"path\"=\"fake://a\", \"format\"=\"csv\") "
                    + "union all select * from files(\"path\"=\"fake://b\", \"format\"=\"csv\")) as u";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            QueryStatement queryStatement = (QueryStatement) stmt;

            new QueryAnalyzer(connectContext).analyzeFilesOnly(queryStatement);

            SelectRelation outerSelect = (SelectRelation) queryStatement.getQueryRelation();
            assertTrue(outerSelect.getRelation() instanceof SubqueryRelation);
            SubqueryRelation subquery = (SubqueryRelation) outerSelect.getRelation();

            QueryRelation innerRelation = subquery.getQueryStatement().getQueryRelation();
            assertTrue(innerRelation instanceof SetOperationRelation);
            SetOperationRelation setOperationRelation = (SetOperationRelation) innerRelation;

            for (QueryRelation relation : setOperationRelation.getRelations()) {
                assertTrue(relation instanceof SelectRelation);
                Relation child = ((SelectRelation) relation).getRelation();
                assertTrue(child instanceof FileTableFunctionRelation);
                assertNotNull(((FileTableFunctionRelation) child).getTable());
            }
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }
}
