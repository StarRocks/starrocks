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

import com.google.common.collect.Lists;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.thrift.TUniqueId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class QeProcessorImplTest {

    private ConnectContext mockContext;
    private TUniqueId queryId;

    @BeforeEach
    public void setUp() {
        mockContext = Mockito.mock(ConnectContext.class);
        queryId = new TUniqueId(1, 2);

        Mockito.when(mockContext.getExecutionId()).thenReturn(queryId);
        Mockito.when(mockContext.getState()).thenReturn(new QueryState());
        Mockito.when(mockContext.getCustomQueryId()).thenReturn("");
        Mockito.when(mockContext.getQualifiedUser()).thenReturn("test_user");
        Mockito.when(mockContext.getConnectionId()).thenReturn(100);
        Mockito.when(mockContext.getDatabase()).thenReturn("test_db");
        Mockito.when(mockContext.getCurrentWarehouseName()).thenReturn("default_warehouse");
    }

    @AfterEach
    public void tearDown() {
        // Clean up any registered queries to avoid interference between tests
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testRegisterPlanningQuery() throws StarRocksException {
        // Planning queries have null coordinator
        QeProcessorImpl.QueryInfo info =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT 1");

        Assertions.assertNull(info.getCoord());
        Assertions.assertEquals("SELECT 1", info.getSql());
        Assertions.assertEquals(mockContext, info.getConnectContext());

        QeProcessorImpl.INSTANCE.registerQuery(queryId, info);

        // Verify it's counted
        Assertions.assertTrue(QeProcessorImpl.INSTANCE.getCoordinatorCount() > 0);

        // Verify getCoordinator returns null for planning query
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(queryId));

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testPlanningQueryVisibleInStatistics() throws StarRocksException {
        Mockito.when(mockContext.isPlanning()).thenReturn(true);

        QeProcessorImpl.QueryInfo info =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT * FROM t");

        QeProcessorImpl.INSTANCE.registerQuery(queryId, info);

        Map<String, QueryStatisticsItem> stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        String queryIdStr = DebugUtil.printId(queryId);

        Assertions.assertTrue(stats.containsKey(queryIdStr));
        QueryStatisticsItem item = stats.get(queryIdStr);
        Assertions.assertEquals("CREATED", item.getExecState());
        Assertions.assertEquals("SELECT * FROM t", item.getSql());
        Assertions.assertEquals("test_user", item.getUser());
        Assertions.assertEquals("test_db", item.getDb());
        Assertions.assertEquals("default_warehouse", item.getWarehouseName());
        Assertions.assertEquals("", item.getResourceGroupName());
        Assertions.assertTrue(item.getFragmentInstanceInfos().isEmpty());

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testPlanningQueryUnregisteredAfterPlanning() throws StarRocksException {
        Mockito.when(mockContext.isPlanning()).thenReturn(true);

        QeProcessorImpl.QueryInfo info =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT 1");

        QeProcessorImpl.INSTANCE.registerQuery(queryId, info);

        // Verify it's registered
        Map<String, QueryStatisticsItem> stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        Assertions.assertTrue(stats.containsKey(DebugUtil.printId(queryId)));

        // Unregister (simulating planning complete)
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);

        // Verify it's gone
        stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        Assertions.assertFalse(stats.containsKey(DebugUtil.printId(queryId)));
    }

    @Test
    public void testReRegisterWithCoordinatorAfterPlanning() throws StarRocksException {
        // Phase 1: Register as planning query
        Mockito.when(mockContext.isPlanning()).thenReturn(true);

        QeProcessorImpl.QueryInfo planningInfo =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT * FROM t");
        QeProcessorImpl.INSTANCE.registerQuery(queryId, planningInfo);

        // Phase 2: Unregister planning entry
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);

        // Phase 3: Re-register with real coordinator (simulating handleQueryStmt)
        Mockito.when(mockContext.isPlanning()).thenReturn(false);
        Mockito.when(mockContext.isPending()).thenReturn(false);

        Coordinator mockCoord = Mockito.mock(Coordinator.class);
        Mockito.when(mockCoord.getFragmentInstanceInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(mockCoord.getQueryProfile()).thenReturn(new RuntimeProfile("test"));
        Mockito.when(mockCoord.getWarehouseName()).thenReturn("default_warehouse");
        Mockito.when(mockCoord.getResourceGroupName()).thenReturn("rg1");
        Mockito.when(mockCoord.isDone()).thenReturn(false);

        QeProcessorImpl.QueryInfo execInfo =
                new QeProcessorImpl.QueryInfo(mockContext, "SELECT * FROM t", mockCoord);
        QeProcessorImpl.INSTANCE.registerQuery(queryId, execInfo);

        // Verify it's now shown as RUNNING
        Map<String, QueryStatisticsItem> stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        String queryIdStr = DebugUtil.printId(queryId);
        Assertions.assertTrue(stats.containsKey(queryIdStr));
        Assertions.assertEquals("RUNNING", stats.get(queryIdStr).getExecState());
        Assertions.assertEquals("rg1", stats.get(queryIdStr).getResourceGroupName());

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testGetCoordinatorsFiltersNullForPlanningQueries() throws StarRocksException {
        // Register a planning query (null coordinator)
        QeProcessorImpl.QueryInfo planningInfo =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT 1");
        QeProcessorImpl.INSTANCE.registerQuery(queryId, planningInfo);

        // getCoordinators should not return null entries
        List<Coordinator> coordinators = QeProcessorImpl.INSTANCE.getCoordinators();
        for (Coordinator c : coordinators) {
            Assertions.assertNotNull(c);
        }

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testPlanningQueryWithNullSqlSkipped() throws StarRocksException {
        // If sql is null, getQueryStatistics should skip it
        QeProcessorImpl.QueryInfo info =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, null);
        QeProcessorImpl.INSTANCE.registerQuery(queryId, info);

        Map<String, QueryStatisticsItem> stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        Assertions.assertFalse(stats.containsKey(DebugUtil.printId(queryId)));

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testUnregisterPlanningQueryDoesNotCallOnFinished() throws StarRocksException {
        // Planning queries have null coordinator, so unregister should not NPE
        QeProcessorImpl.QueryInfo info =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT 1");
        QeProcessorImpl.INSTANCE.registerQuery(queryId, info);

        // This should not throw NPE
        Assertions.assertDoesNotThrow(() ->
                QeProcessorImpl.INSTANCE.unregisterQuery(queryId));
    }

    @Test
    public void testPendingStateNotOverriddenByPlanning() throws StarRocksException {
        // Verify that a non-planning, pending query still shows as PENDING
        Mockito.when(mockContext.isPlanning()).thenReturn(false);
        Mockito.when(mockContext.isPending()).thenReturn(true);

        Coordinator mockCoord = Mockito.mock(Coordinator.class);
        Mockito.when(mockCoord.getFragmentInstanceInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(mockCoord.getQueryProfile()).thenReturn(new RuntimeProfile("test"));
        Mockito.when(mockCoord.getWarehouseName()).thenReturn("wh");
        Mockito.when(mockCoord.getResourceGroupName()).thenReturn("rg");
        Mockito.when(mockCoord.isDone()).thenReturn(false);

        QeProcessorImpl.QueryInfo info =
                new QeProcessorImpl.QueryInfo(mockContext, "SELECT 1", mockCoord);
        QeProcessorImpl.INSTANCE.registerQuery(queryId, info);

        Map<String, QueryStatisticsItem> stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        String queryIdStr = DebugUtil.printId(queryId);
        Assertions.assertEquals("PENDING", stats.get(queryIdStr).getExecState());

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testDoubleRegisterThrowsException() throws StarRocksException {
        QeProcessorImpl.QueryInfo info =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT 1");
        QeProcessorImpl.INSTANCE.registerQuery(queryId, info);

        // Attempting to register the same query ID should throw
        Assertions.assertThrows(StarRocksException.class, () ->
                QeProcessorImpl.INSTANCE.registerQuery(queryId, info));

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testPlanningQueryStateTransitions() throws StarRocksException {
        // Simulate the full lifecycle: CREATED -> unregister -> RUNNING -> FINISHED
        Mockito.when(mockContext.isPlanning()).thenReturn(true);

        // Step 1: Register as planning
        QeProcessorImpl.QueryInfo planningInfo =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT 1");
        QeProcessorImpl.INSTANCE.registerQuery(queryId, planningInfo);

        Map<String, QueryStatisticsItem> stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        Assertions.assertEquals("CREATED", stats.get(DebugUtil.printId(queryId)).getExecState());

        // Step 2: Planning complete, unregister
        Mockito.when(mockContext.isPlanning()).thenReturn(false);
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);

        // Step 3: Register with coordinator (RUNNING)
        Coordinator mockCoord = Mockito.mock(Coordinator.class);
        Mockito.when(mockCoord.getFragmentInstanceInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(mockCoord.getQueryProfile()).thenReturn(new RuntimeProfile("test"));
        Mockito.when(mockCoord.getWarehouseName()).thenReturn("wh");
        Mockito.when(mockCoord.getResourceGroupName()).thenReturn("rg");
        Mockito.when(mockCoord.isDone()).thenReturn(false);

        QeProcessorImpl.QueryInfo execInfo =
                new QeProcessorImpl.QueryInfo(mockContext, "SELECT 1", mockCoord);
        QeProcessorImpl.INSTANCE.registerQuery(queryId, execInfo);

        stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        Assertions.assertEquals("RUNNING", stats.get(DebugUtil.printId(queryId)).getExecState());

        // Step 4: Unregister (FINISHED)
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        Assertions.assertFalse(stats.containsKey(DebugUtil.printId(queryId)));
    }

    @Test
    public void testCoordinatorOnlyQueryInfoSkipped() throws StarRocksException {
        // Queries registered with the coordinator-only constructor (sql == null, context == null),
        // e.g. the historical registration in executeStmtWithExecPlan, must be filtered out by
        // getQueryStatistics() and therefore stay invisible in current_queries.
        Coordinator mockCoord = Mockito.mock(Coordinator.class);
        Mockito.when(mockCoord.isDone()).thenReturn(false);

        QeProcessorImpl.QueryInfo info = new QeProcessorImpl.QueryInfo(mockCoord);
        Assertions.assertNull(info.getSql());
        Assertions.assertNull(info.getConnectContext());

        QeProcessorImpl.INSTANCE.registerQuery(queryId, info);

        Map<String, QueryStatisticsItem> stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        Assertions.assertFalse(stats.containsKey(DebugUtil.printId(queryId)));

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testInternalQueryVisibleWhenRegisteredWithContextAndSql() throws StarRocksException {
        // executeStmtWithExecPlan / executeStmtWithResultQueue now register coordinator-backed internal
        // queries (statistics collection sub-queries, metadata collection, etc.) with the ConnectContext
        // and SQL, so they become visible in current_queries / global_current_queries.
        Mockito.when(mockContext.isPlanning()).thenReturn(false);
        Mockito.when(mockContext.isPending()).thenReturn(false);

        Coordinator mockCoord = Mockito.mock(Coordinator.class);
        Mockito.when(mockCoord.getFragmentInstanceInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(mockCoord.getQueryProfile()).thenReturn(new RuntimeProfile("test"));
        Mockito.when(mockCoord.getWarehouseName()).thenReturn("default_warehouse");
        Mockito.when(mockCoord.getResourceGroupName()).thenReturn("");
        Mockito.when(mockCoord.isDone()).thenReturn(false);

        QeProcessorImpl.QueryInfo info = new QeProcessorImpl.QueryInfo(
                mockContext, "INSERT INTO _statistics_.column_statistics SELECT ...", mockCoord);
        QeProcessorImpl.INSTANCE.registerQuery(queryId, info);

        Map<String, QueryStatisticsItem> stats = QeProcessorImpl.INSTANCE.getQueryStatistics();
        String queryIdStr = DebugUtil.printId(queryId);
        Assertions.assertTrue(stats.containsKey(queryIdStr));
        Assertions.assertEquals("RUNNING", stats.get(queryIdStr).getExecState());
        Assertions.assertEquals("INSERT INTO _statistics_.column_statistics SELECT ...",
                stats.get(queryIdStr).getSql());

        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testQueryTypeClassification() throws StarRocksException {
        Coordinator mockCoord = Mockito.mock(Coordinator.class);
        Mockito.when(mockCoord.getFragmentInstanceInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(mockCoord.getQueryProfile()).thenReturn(new RuntimeProfile("test"));
        Mockito.when(mockCoord.getWarehouseName()).thenReturn("wh");
        Mockito.when(mockCoord.getResourceGroupName()).thenReturn("");
        Mockito.when(mockCoord.isDone()).thenReturn(false);

        // Statistics connection wins over query source -> "Statistics" (covers ANALYZE collection).
        Mockito.when(mockContext.isStatisticsConnection()).thenReturn(true);
        Mockito.when(mockContext.getQuerySource()).thenReturn(QueryDetail.QuerySource.INTERNAL);
        QeProcessorImpl.INSTANCE.registerQuery(queryId, new QeProcessorImpl.QueryInfo(mockContext, "sql", mockCoord));
        Assertions.assertEquals("Statistics",
                QeProcessorImpl.INSTANCE.getQueryStatistics().get(DebugUtil.printId(queryId)).getQueryType());
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);

        // Task-submitted query -> "Task".
        Mockito.when(mockContext.isStatisticsConnection()).thenReturn(false);
        Mockito.when(mockContext.getQuerySource()).thenReturn(QueryDetail.QuerySource.TASK);
        QeProcessorImpl.INSTANCE.registerQuery(queryId, new QeProcessorImpl.QueryInfo(mockContext, "sql", mockCoord));
        Assertions.assertEquals("Task",
                QeProcessorImpl.INSTANCE.getQueryStatistics().get(DebugUtil.printId(queryId)).getQueryType());
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);

        // Regular user query -> "Query".
        Mockito.when(mockContext.getQuerySource()).thenReturn(QueryDetail.QuerySource.EXTERNAL);
        QeProcessorImpl.INSTANCE.registerQuery(queryId, new QeProcessorImpl.QueryInfo(mockContext, "sql", mockCoord));
        Assertions.assertEquals("Query",
                QeProcessorImpl.INSTANCE.getQueryStatistics().get(DebugUtil.printId(queryId)).getQueryType());
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testCoordinatorBackedQueryVisibleWhenContextStateFinished() throws StarRocksException {
        // Internal queries (e.g. statistics collection) reuse a ConnectContext whose last-command state may
        // already be EOF/OK while a new coordinator-backed statement is actively running. Liveness must be
        // taken from the coordinator (isDone), not from context.getState().isRunning(), so such queries stay
        // visible in current_queries.
        QueryState finishedState = new QueryState();
        finishedState.setEof(); // isRunning() == false
        Mockito.when(mockContext.getState()).thenReturn(finishedState);
        Mockito.when(mockContext.isPlanning()).thenReturn(false);
        Mockito.when(mockContext.isPending()).thenReturn(false);

        Coordinator mockCoord = Mockito.mock(Coordinator.class);
        Mockito.when(mockCoord.getFragmentInstanceInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(mockCoord.getQueryProfile()).thenReturn(new RuntimeProfile("test"));
        Mockito.when(mockCoord.getWarehouseName()).thenReturn("wh");
        Mockito.when(mockCoord.getResourceGroupName()).thenReturn("");

        // Coordinator still running -> visible despite the finished context state.
        Mockito.when(mockCoord.isDone()).thenReturn(false);
        QeProcessorImpl.INSTANCE.registerQuery(queryId,
                new QeProcessorImpl.QueryInfo(mockContext, "SELECT ...", mockCoord));
        Assertions.assertTrue(
                QeProcessorImpl.INSTANCE.getQueryStatistics().containsKey(DebugUtil.printId(queryId)));
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);

        // Coordinator done -> filtered out.
        Mockito.when(mockCoord.isDone()).thenReturn(true);
        QeProcessorImpl.INSTANCE.registerQuery(queryId,
                new QeProcessorImpl.QueryInfo(mockContext, "SELECT ...", mockCoord));
        Assertions.assertFalse(
                QeProcessorImpl.INSTANCE.getQueryStatistics().containsKey(DebugUtil.printId(queryId)));
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testGetConnectContextByQueryId() throws StarRocksException {
        Coordinator mockCoord = Mockito.mock(Coordinator.class);
        Mockito.when(mockCoord.isDone()).thenReturn(false);

        String queryIdStr = DebugUtil.printId(queryId);

        // A coordinator-backed query registered with a ConnectContext is resolvable by its query id,
        // so KILL QUERY can target it (e.g. statistics collection / task runs).
        QeProcessorImpl.INSTANCE.registerQuery(queryId,
                new QeProcessorImpl.QueryInfo(mockContext, "SELECT 1", mockCoord));
        Assertions.assertSame(mockContext, QeProcessorImpl.INSTANCE.getConnectContextByQueryId(queryIdStr));
        // Unknown query id resolves to null.
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getConnectContextByQueryId("does-not-exist"));
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);

        // A coordinator-only registration (no ConnectContext) is not resolvable, matching its invisibility
        // in current_queries.
        QeProcessorImpl.INSTANCE.registerQuery(queryId, new QeProcessorImpl.QueryInfo(mockCoord));
        Assertions.assertNull(QeProcessorImpl.INSTANCE.getConnectContextByQueryId(queryIdStr));
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
    }

    @Test
    public void testFromPlanningQueryStartExecTime() throws StarRocksException {
        long beforeCreate = System.currentTimeMillis();
        QeProcessorImpl.QueryInfo info =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT 1");
        long afterCreate = System.currentTimeMillis();

        Assertions.assertTrue(info.getStartExecTime() >= beforeCreate);
        Assertions.assertTrue(info.getStartExecTime() <= afterCreate);
    }
}
