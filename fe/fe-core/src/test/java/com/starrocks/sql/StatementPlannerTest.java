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

import com.starrocks.common.FeConstants;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.sql.analyzer.Analyzer;
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
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

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

    @Ignore
    public void testInsertPartialUpdateMode() throws Exception {
        {
            FeConstants.runningUnitTest = true;
            connectContext.getSessionVariable().setPartialUpdateMode("column");
            String sql = "insert into tprimary_multi_cols (pk, v1) values (1, '1')";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            InsertPlanner planner = new InsertPlanner();
            ExecPlan plan = planner.plan((InsertStmt) stmt, connectContext);
            assertEquals(TPartialUpdateMode.COLUMN_UPSERT_MODE, getPartialUpdateMode(plan));
        }

        {
            FeConstants.runningUnitTest = true;
            connectContext.getSessionVariable().setPartialUpdateMode("auto");
            String sql = "insert into tprimary_multi_cols (pk, v1) values (1, '1')";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            InsertPlanner planner = new InsertPlanner();
            ExecPlan plan = planner.plan((InsertStmt) stmt, connectContext);
            assertEquals(TPartialUpdateMode.COLUMN_UPSERT_MODE, getPartialUpdateMode(plan));
        }

        {
            FeConstants.runningUnitTest = true;
            connectContext.getSessionVariable().setPartialUpdateMode("auto");
            String sql = "insert into tprimary_multi_cols (pk, v1, v2) values (1, '1', 1)";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            InsertPlanner planner = new InsertPlanner();
            ExecPlan plan = planner.plan((InsertStmt) stmt, connectContext);
            assertEquals(TPartialUpdateMode.AUTO_MODE, getPartialUpdateMode(plan));
        }
    }

    private TPartialUpdateMode getPartialUpdateMode(ExecPlan plan) {
        PlanFragment fragment = plan.getFragments().get(0);
        OlapTableSink sink = (OlapTableSink) fragment.getSink();
        return sink.getPartialUpdateMode();
    }

    @Test
    public void testPlanExceptionWithoutSchemaChange() throws Exception {
        // Exception occurs in plan builder, schema is valid, exception should be re-thrown
        FeConstants.runningUnitTest = false;
        String sql = "select * from t1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        QueryStatement queryStmt = (QueryStatement) stmt;
        Analyzer.analyze(queryStmt, connectContext);
        PlannerMetaLocker plannerMetaLocker = new PlannerMetaLocker(connectContext, queryStmt);
        try (MockedStatic<PlanFragmentBuilder> mockedPlanFragmentBuilder = mockStatic(PlanFragmentBuilder.class);
                MockedStatic<OptimisticVersion> mockedOptimisticVersion =
                        mockStatic(OptimisticVersion.class)) {
            // Mock PlanFragmentBuilder.createPhysicalPlan to throw exception
            RuntimeException testException = new RuntimeException("Test exception during ExecPlanBuild");
            mockedPlanFragmentBuilder.when(() -> PlanFragmentBuilder.createPhysicalPlan(
                    any(), any(), anyList(), any(), anyList(), any(), anyBoolean()))
                    .thenThrow(testException);

            // Mock OptimisticVersion.validateTableUpdate to return true (schema is valid)
            mockedOptimisticVersion.when(() -> com.starrocks.sql.OptimisticVersion.validateTableUpdate(
                    any(OlapTable.class), anyLong())).thenReturn(true);

            // Call createQueryPlanWithReTry and verify exception is re-thrown
            long planStartTime = OptimisticVersion.generate();
            RuntimeException thrownException = assertThrows(RuntimeException.class, () ->
                    StatementPlanner.createQueryPlanWithReTry(
                            queryStmt, connectContext, TResultSinkType.MYSQL_PROTOCAL,
                            plannerMetaLocker, planStartTime));

            assertEquals("Test exception during ExecPlanBuild", thrownException.getMessage());

            // Verify that OptimisticVersion.validateTableUpdate was called in the catch block
            Set<OlapTable> olapTables = StatementPlanner.collectOriginalOlapTables(connectContext, queryStmt);
            if (!olapTables.isEmpty()) {
                mockedOptimisticVersion.verify(() -> OptimisticVersion.validateTableUpdate(
                        any(OlapTable.class), eq(planStartTime)), times(olapTables.size()));
            }
        }
    }

    @Test
    public void testPlanExceptionWithSchemaChange() throws Exception {
        // Exception occurs, schema is invalid, should retry instead of re-throwing
        FeConstants.runningUnitTest = false;
        String sql = "select * from t1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        QueryStatement queryStmt = (QueryStatement) stmt;
        Analyzer.analyze(queryStmt, connectContext);
        PlannerMetaLocker plannerMetaLocker = new PlannerMetaLocker(connectContext, queryStmt);

        // Collect olap tables before mocking
        Set<OlapTable> olapTables = StatementPlanner.collectOriginalOlapTables(connectContext, queryStmt);
        int tableCount = olapTables.size();
        try (MockedStatic<PlanFragmentBuilder> mockedPlanFragmentBuilder = mockStatic(PlanFragmentBuilder.class);
                MockedStatic<com.starrocks.sql.OptimisticVersion> mockedOptimisticVersion =
                        mockStatic(com.starrocks.sql.OptimisticVersion.class)) {

            // Mock PlanFragmentBuilder.createPhysicalPlan
            // First call throws exception, second call returns a plan
            RuntimeException testException = new RuntimeException("Test exception during ExecPlanBuild");
            ExecPlan mockPlan = Mockito.mock(ExecPlan.class);
            AtomicInteger callCount = new AtomicInteger(0);
            mockedPlanFragmentBuilder.when(() -> PlanFragmentBuilder.createPhysicalPlan(
                    any(), any(), anyList(), any(), anyList(), any(), anyBoolean()))
                    .thenAnswer(invocation -> {
                        int count = callCount.incrementAndGet();
                        if (count == 1) {
                            throw testException;
                        } else {
                            return mockPlan;
                        }
                    });

            // Mock OptimisticVersion.validateTableUpdate
            // First call (in catch block) returns false (schema invalid), triggering retry
            // Second call (in normal flow) returns true (schema valid after retry)
            AtomicInteger validateCallCount = new AtomicInteger(0);
            mockedOptimisticVersion.when(() -> OptimisticVersion.validateTableUpdate(
                    any(OlapTable.class), anyLong())).thenAnswer(invocation -> {
                        int count = validateCallCount.incrementAndGet();
                        // First validation (in catch block) returns false, subsequent returns true
                        // For each table, we expect: first call (in catch) returns false, second call (in normal flow) returns true
                        return count > tableCount;
                    });

            // Call createQueryPlanWithReTry
            // Since schema is invalid on first exception, it should retry and succeed
            long planStartTime = OptimisticVersion.generate();
            ExecPlan result = StatementPlanner.createQueryPlanWithReTry(
                    queryStmt, connectContext, TResultSinkType.MYSQL_PROTOCAL,
                    plannerMetaLocker, planStartTime);

            // Verify that a plan was returned (retry succeeded)
            assertNotNull(result);

            // Verify that PlanFragmentBuilder.createPhysicalPlan was called twice
            // (once throwing exception, once succeeding)
            mockedPlanFragmentBuilder.verify(() -> PlanFragmentBuilder.createPhysicalPlan(
                    any(), any(), anyList(), any(), anyList(), any(), anyBoolean()),
                    times(2));

            // Verify that OptimisticVersion.validateTableUpdate was called
            // (at least once in catch block, and once in normal flow)
            if (!olapTables.isEmpty()) {
                mockedOptimisticVersion.verify(() -> OptimisticVersion.validateTableUpdate(
                        any(OlapTable.class), anyLong()), Mockito.atLeast(tableCount * 2));
            }
        }
    }
<<<<<<< HEAD
>>>>>>> 14827a423f ([BugFix] Fix NPE in query planning during schema change (backport #66811) (#66828))
}
=======
}
>>>>>>> 6d4986a14f9 (fix bugs)
