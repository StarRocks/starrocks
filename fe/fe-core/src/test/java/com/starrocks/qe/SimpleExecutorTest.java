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

package com.starrocks.qe;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.scheduler.history.TaskRunHistoryTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleExecutorTest {

    private static final String INSERT_SQL = "INSERT INTO db1.t1 VALUES(1)";

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
        FeConstants.runningUnitTest = false;
    }

    /**
     * StmtExecutor.execute() does not rethrow a StarRocksException, it only records the failure in
     * the ConnectContext state and returns normally. The internal executor must not report that as
     * a success, otherwise its callers silently lose the data they believe they have persisted.
     */
    @Test
    public void testExecuteDMLFailsWhenStatementSetsErrorState() {
        mockStatementFailure("Tablet lost replicas. Check if any backend is down or not. tablet_id: 10093");

        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL);
        SemanticException e =
                assertThrows(SemanticException.class, () -> executor.executeDML(INSERT_SQL));
        assertTrue(e.getMessage().contains("Tablet lost replicas"), e.getMessage());
    }

    @Test
    public void testExecuteControlFailsWhenStatementSetsErrorState() {
        mockStatementFailure("Unknown thread id: 1024");

        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.MYSQL_PROTOCAL);
        SemanticException e =
                assertThrows(SemanticException.class, () -> executor.executeControl("KILL 1024"));
        assertTrue(e.getMessage().contains("Unknown thread id"), e.getMessage());
    }

    @Test
    public void testExecuteDMLSucceedsWhenStatementIsOk() {
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() {
            }
        };

        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL);
        executor.executeDML(INSERT_SQL);
    }

    /**
     * Serving-path internal queries (e.g. filling information_schema.materialized_views, lookup_string)
     * must be bounded by the outer user query's remaining query_timeout, not the 1h
     * statistic_collect_query_timeout. Background callers (no outer query) keep the old fallback.
     */
    @Test
    public void testOuterRemainingQueryTimeoutS() throws Exception {
        // No outer user-query context: fall back to statistic_collect_query_timeout (old behavior).
        ConnectContext.remove();
        assertEquals((int) Config.statistic_collect_query_timeout, SimpleExecutor.outerRemainingQueryTimeoutS());

        // Outer query with query_timeout=300s started ~100s ago -> remaining ~200s.
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().setQueryTimeoutS(300);
        ctx.setStartTime(Instant.now().minusSeconds(100));
        int remaining = SimpleExecutor.outerRemainingQueryTimeoutS();
        assertTrue(remaining > 190 && remaining <= 200, "remaining=" + remaining);

        // Budget already exhausted (started ~400s ago, budget 300s) -> <= 0, so the caller can time out.
        ctx.setStartTime(Instant.now().minusSeconds(400));
        assertTrue(SimpleExecutor.outerRemainingQueryTimeoutS() <= 0);

        ConnectContext.remove();
    }

    /**
     * The internal task_run_history read (fired while filling information_schema.materialized_views /
     * SHOW MATERIALIZED VIEWS) must inherit the OUTER user query's remaining query_timeout, not the
     * default statistic_collect_query_timeout (1h).
     */
    @Test
    public void testInternalTaskRunHistoryReadUsesOuterQueryTimeout() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().setQueryTimeoutS(300);
        ctx.setStartTime(Instant.now());

        int[] capturedTimeout = {-1};
        new MockUp<SimpleExecutor>() {
            @Mock
            public List<TResultBatch> executeDQL(String sql, int queryTimeoutSeconds) {
                capturedTimeout[0] = queryTimeoutSeconds;
                return Collections.emptyList();
            }
        };

        new TaskRunHistoryTable().lookupLastJobOfTasks("db", Collections.singleton("mvTask"));
        ConnectContext.remove();

        // Inherited the outer query_timeout (~300s), not statistic_collect_query_timeout (3600s).
        assertTrue(capturedTimeout[0] > 290 && capturedTimeout[0] <= 300, "timeout=" + capturedTimeout[0]);
    }

    @Test
    public void testParentQueryPolicyIsInheritedWithoutMutatingCaller() throws Exception {
        ConnectContext parent = UtFrameUtils.createDefaultCtx();
        parent.setQualifiedUser("index_reader");
        parent.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("index_reader", "%"));
        parent.setCurrentRoleIds(Set.of(101L));
        parent.setGroups(Set.of("analysts"));
        parent.setRemoteIP("192.0.2.1");
        parent.setBypassAuthorizerCheck(true);
        parent.getSessionVariable().setResourceGroup("user_group");
        GlobalStateMgr.getCurrentState().getVariableMgr().setSystemVariable(parent.getSessionVariable(),
                new SystemVariable(SessionVariable.QUERY_MEM_LIMIT, new IntLiteral(123456789L)), false);
        parent.getSessionVariable().setSqlSelectLimit(7);
        parent.getSessionVariable().setQueryTimeoutS(120);
        parent.setThreadLocalInfo();
        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL) {
            @Override
            public List<TResultBatch> executeDQL(String sql, ConnectContext context) {
                assertSame(parent, context.getParent());
                assertEquals(parent.getCurrentUserIdentity(), context.getCurrentUserIdentity());
                assertEquals(parent.getCurrentRoleIds(), context.getCurrentRoleIds());
                assertEquals(parent.getGroups(), context.getGroups());
                assertEquals(parent.getRemoteIP(), context.getRemoteIP());
                assertFalse(context.isBypassAuthorizerCheck());
                assertNotSame(parent.getSessionVariable(), context.getSessionVariable());
                assertEquals("user_group", context.getSessionVariable().getResourceGroup());
                assertEquals(123456789L, context.getSessionVariable().toThrift().getQuery_mem_limit());
                assertEquals(parent.getCurrentWarehouseName(), context.getCurrentWarehouseName());
                assertEquals(7, context.getSessionVariable().getSqlSelectLimit());
                assertEquals(30, context.getSessionVariable().getQueryTimeoutS());
                return List.of();
            }
        };
        try {
            executor.executeDQLAsCaller("select 1", 30, parent);
            assertSame(parent, ConnectContext.get());
            assertEquals(7, parent.getSessionVariable().getSqlSelectLimit());
            assertEquals(120, parent.getSessionVariable().getQueryTimeoutS());
            assertEquals("user_group", parent.getSessionVariable().getResourceGroup());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testResolvedResourceGroupAndContextRestoredOnFailure() {
        ConnectContext parent = UtFrameUtils.createDefaultCtx();
        parent.setResourceGroup(new TWorkGroup().setName("classified_group"));
        parent.setThreadLocalInfo();
        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL) {
            @Override
            public List<TResultBatch> executeDQL(String sql, ConnectContext context) {
                assertEquals("classified_group", context.getSessionVariable().getResourceGroup());
                throw new SemanticException("test failure");
            }
        };
        try {
            assertThrows(SemanticException.class, () -> executor.executeDQLAsCaller("select 1", 30, parent));
            assertSame(parent, ConnectContext.get());
            assertEquals("", parent.getSessionVariable().getResourceGroup());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testBackgroundQueryRetainsGlobalSessionDefaults() {
        SessionVariable defaults = GlobalStateMgr.getCurrentState().getVariableMgr().getDefaultSessionVariable();
        long originalLimit = defaults.getSqlSelectLimit();
        defaults.setSqlSelectLimit(3);
        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL) {
            @Override
            public List<TResultBatch> executeDQL(String sql, ConnectContext context) {
                assertEquals(UserIdentity.ROOT, context.getCurrentUserIdentity());
                assertEquals(3, context.getSessionVariable().getSqlSelectLimit());
                return List.of();
            }
        };
        try {
            executor.executeDQL("select 1", 30);
        } finally {
            defaults.setSqlSelectLimit(originalLimit);
            ConnectContext.remove();
        }
    }

    @Test
    public void testRestoresPreviousContextDifferentFromCaller() {
        ConnectContext caller = UtFrameUtils.createDefaultCtx();
        caller.getSessionVariable().setSqlSelectLimit(0);
        // The explicitly supplied caller need not be the thread-local context being restored.
        ConnectContext previous = UtFrameUtils.createDefaultCtx();
        previous.setThreadLocalInfo();
        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL) {
            @Override
            public List<TResultBatch> executeDQL(String sql, ConnectContext context) {
                assertSame(caller, context.getParent());
                assertSame(context, ConnectContext.get());
                assertEquals(0, context.getSessionVariable().getSqlSelectLimit());
                throw new SemanticException("test failure");
            }
        };
        try {
            assertThrows(SemanticException.class, () -> executor.executeDQLAsCaller(
                    "select 1", 30, caller));
            assertSame(previous, ConnectContext.get());
            assertEquals(0, caller.getSessionVariable().getSqlSelectLimit());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testCallerRemainingBudgetCapsExplicitTimeout() {
        ConnectContext caller = UtFrameUtils.createDefaultCtx();
        caller.getSessionVariable().setQueryTimeoutS(300);
        caller.setStartTime(Instant.now().minusSeconds(100));
        ConnectContext.remove();
        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL) {
            @Override
            public List<TResultBatch> executeDQL(String sql, ConnectContext context) {
                long timeout = context.getSessionVariable().getQueryTimeoutS();
                assertTrue(timeout > 190 && timeout <= 200, "timeout=" + timeout);
                return List.of();
            }
        };
        executor.executeDQLAsCaller("select 1", 600, caller);
        assertNull(ConnectContext.get());
        assertEquals(300, caller.getSessionVariable().getQueryTimeoutS());
    }

    @Test
    public void testCancelledOrExpiredCallerDoesNotStartInternalQuery() throws Exception {
        ConnectContext caller = UtFrameUtils.createDefaultCtx();
        StmtExecutor statement = new StmtExecutor(caller,
                SqlParser.parseOneWithStarRocksDialect("select 1", caller.getSessionVariable()));
        caller.setExecutor(statement);
        caller.setThreadLocalInfo();
        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL) {
            @Override
            public ConnectContext createConnectContext() {
                throw new AssertionError("cancelled/expired query must not create a child context");
            }
        };
        try {
            caller.getSessionVariable().setQueryTimeoutS(1);
            caller.setStartTime(Instant.now().minusSeconds(2));
            assertTrue(assertThrows(SemanticException.class,
                    () -> executor.executeDQLAsCaller("select 1", 30, caller)).getMessage().contains("timed out"));
            caller.setStartTime();
            caller.kill(false, "test cancellation");
            assertTrue(assertThrows(SemanticException.class,
                    () -> executor.executeDQLAsCaller("select 1", 30, caller)).getMessage().contains("cancelled"));
            assertSame(caller, ConnectContext.get());
            assertTrue(statement.getSubStmtExecutors().isEmpty());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testLateChildRegistrationInheritsCancellation() throws Exception {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        StmtExecutor parent = new StmtExecutor(context,
                SqlParser.parseOneWithStarRocksDialect("select 1", context.getSessionVariable()));
        StmtExecutor child = new StmtExecutor(context,
                SqlParser.parseOneWithStarRocksDialect("select 2", context.getSessionVariable()));
        parent.cancel("cancel before registration");
        parent.registerSubStmtExecutor(child);
        assertTrue(child.isCancelled());
        assertEquals(List.of(child), parent.getSubStmtExecutors());
    }

    @Test
    public void testChildRegistrationDuringCancellation() throws Exception {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        StmtExecutor parent = new StmtExecutor(context,
                SqlParser.parseOneWithStarRocksDialect("select 1", context.getSessionVariable()));
        StmtExecutor lateChild = new StmtExecutor(context,
                SqlParser.parseOneWithStarRocksDialect("select 2", context.getSessionVariable()));
        StmtExecutor child = new StmtExecutor(context,
                SqlParser.parseOneWithStarRocksDialect("select 3", context.getSessionVariable())) {
            @Override
            public void cancel(String message) {
                // Add a child while cancel() is traversing its original child snapshot.
                parent.registerSubStmtExecutor(lateChild);
                super.cancel(message);
            }
        };
        parent.registerSubStmtExecutor(child);
        parent.cancel("cancel during registration");
        assertTrue(child.isCancelled());
        assertTrue(lateChild.isCancelled());
        assertEquals(List.of(child, lateChild), parent.getSubStmtExecutors());
    }

    @Test
    public void testCallerQueryRegistersChildExecutor() throws Exception {
        ConnectContext caller = UtFrameUtils.createDefaultCtx();
        StmtExecutor parent = new StmtExecutor(caller,
                SqlParser.parseOneWithStarRocksDialect("select 1", caller.getSessionVariable()));
        caller.setExecutor(parent);
        caller.setThreadLocalInfo();
        new MockUp<StmtExecutor>() {
            @Mock
            public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan) {
                assertSame(caller, context.getParent());
                assertEquals(List.of(context.getExecutor()), parent.getSubStmtExecutors());
                return Pair.create(List.of(), Status.OK);
            }
        };
        try {
            new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL)
                    .executeDQLAsCaller("select 1", 30, caller);
            assertEquals(1, parent.getSubStmtExecutors().size());
            assertSame(caller, ConnectContext.get());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testCancellationBeforeCoordinatorIsPublished(@Mocked DefaultCoordinator coordinator) throws Exception {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        context.setQueryId(UUID.randomUUID());
        context.setThreadLocalInfo();
        new MockUp<DefaultCoordinator.Factory>() {
            @Mock
            public DefaultCoordinator createQueryScheduler(ConnectContext child, List<PlanFragment> fragments,
                                                           List<ScanNode> scans, TDescriptorTable descriptors,
                                                           ExecPlan plan) {
                // Simulate cancellation before StmtExecutor can publish its coordinator.
                child.setKilled();
                return coordinator;
            }
        };
        try {
            var statement = SqlParser.parseOneWithStarRocksDialect("select 1", context.getSessionVariable());
            ExecPlan plan = StatementPlanner.plan(statement, context, TResultSinkType.HTTP_PROTOCAL);
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, statement);
            context.setExecutor(executor);
            assertSame(Status.CANCELLED, executor.executeStmtWithExecPlan(context, plan).second);
            new Verifications() {
                {
                    coordinator.exec();
                    times = 0;
                    coordinator.cancel("User cancelled");
                    times = 1;
                }
            };
        } finally {
            ConnectContext.remove();
        }
    }

    private static void mockStatementFailure(String errorMessage) {
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() {
                ConnectContext.get().getState().setError(errorMessage);
            }
        };
    }
}
