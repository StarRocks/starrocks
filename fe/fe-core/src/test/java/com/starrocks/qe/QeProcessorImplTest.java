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
    public void testFromPlanningQueryStartExecTime() throws StarRocksException {
        long beforeCreate = System.currentTimeMillis();
        QeProcessorImpl.QueryInfo info =
                QeProcessorImpl.QueryInfo.fromPlanningQuery(mockContext, "SELECT 1");
        long afterCreate = System.currentTimeMillis();

        Assertions.assertTrue(info.getStartExecTime() >= beforeCreate);
        Assertions.assertTrue(info.getStartExecTime() <= afterCreate);
    }
}
