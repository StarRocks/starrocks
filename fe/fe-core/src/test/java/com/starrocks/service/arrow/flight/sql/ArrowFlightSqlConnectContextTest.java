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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.StatementBase;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ArrowFlightSqlConnectContextTest {

    private ArrowFlightSqlConnectContext context;
    private final String token = "test-token";

    @BeforeEach
    public void setUp() {
        context = new ArrowFlightSqlConnectContext(token);
    }

    @Test
    public void testConstructor() {
        assertEquals(token, context.getArrowFlightSqlToken());
        assertEquals("", context.getQuery());
        assertTrue(context.returnFromFE());
    }

    @Test
    public void testSetAndGetStatement() {
        StatementBase mockStatement = mock(StatementBase.class);
        context.setStatement(mockStatement);
        assertEquals(mockStatement, context.getStatement());
    }

    @Test
    public void testReset() {
        context.reset("SELECT 1");
        assertEquals("SELECT 1", context.getQuery());
        assertNotNull(context.getQueryId());
    }

    @Test
    public void testSetReturnResultFromFE() {
        context.setReturnResultFromFE(false);
        assertFalse(context.returnFromFE());
    }

    @Test
    public void testSetEmptyResultIfNotExist() {
        String queryId = "query-1";
        assertNull(context.getResult(queryId));

        try (var mocked = mockStatic(ArrowUtil.class)) {
            VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
            mocked.when(() -> ArrowUtil.createSingleSchemaRoot(anyString(), anyString()))
                    .thenReturn(mockRoot);

            context.setEmptyResultIfNotExist(queryId);
            assertNotNull(context.getResult(queryId));
        }
    }

    @Test
    public void testAddShowResultAndGetResult() {
        String queryId = "query-2";

        Column column = new Column("col1", ScalarType.createVarchar(20));
        ShowResultSetMetaData metaData = new ShowResultSetMetaData(Collections.singletonList(column));
        ShowResultSet showResultSet = new ShowResultSet(metaData, List.of(Collections.singletonList("value1")));

        try (MockedStatic<ArrowUtil> mocked = mockStatic(ArrowUtil.class)) {
            // mock VarCharVector
            VarCharVector mockVector = mock(VarCharVector.class);
            doNothing().when(mockVector).setSafe(anyInt(), any(byte[].class));
            doNothing().when(mockVector).setNull(anyInt());

            // mock ArrowUtil.createVarCharVector
            mocked.when(() -> ArrowUtil.createVarCharVector(anyString(), any(), anyInt()))
                    .thenReturn(mockVector);

            context.addShowResult(queryId, showResultSet);
            VectorSchemaRoot result = context.getResult(queryId);

            assertNotNull(result);
            assertEquals(1, result.getRowCount());
        }
    }

    @Test
    public void testRemoveResult() {
        String queryId = "query-3";
        try (var mocked = mockStatic(ArrowUtil.class)) {
            VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
            mocked.when(() -> ArrowUtil.createSingleSchemaRoot(anyString(), anyString()))
                    .thenReturn(mockRoot);

            context.setEmptyResultIfNotExist(queryId);
            assertNotNull(context.getResult(queryId));
            context.removeResult(queryId);
            assertNull(context.getResult(queryId));
        }
    }


    @Test
    public void testRemoveAllResults() {
        try (var mocked = mockStatic(ArrowUtil.class)) {
            VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
            mocked.when(() -> ArrowUtil.createSingleSchemaRoot(anyString(), anyString()))
                    .thenReturn(mockRoot);

            context.setEmptyResultIfNotExist("query-1");
            context.setEmptyResultIfNotExist("query-2");
            context.removeAllResults();
            assertNull(context.getResult("query-1"));
            assertNull(context.getResult("query-2"));
        }
    }

    @Test
    public void testIsArrowFlightSQL() {
        assertTrue(context instanceof ArrowFlightSqlConnectContext);
    }

    @Test
    public void testIsFromFECoordinator() {
        assertTrue(context.isFromFECoordinator());
    }

    @Test
    public void testCancelQuery() {
        var mockExecutor = mock(com.starrocks.qe.StmtExecutor.class);
        context.setStmtExecutor(mockExecutor);
        context.cancelQuery();
        verify(mockExecutor, times(1)).cancel("Arrow Flight SQL client disconnected");
    }

    @Test
    public void testSetDeploymentFinishedAndWaitSuccess() throws Exception {
        Coordinator coordinator = mock(Coordinator.class);
        context.setDeploymentFinished(coordinator);
        Coordinator result = context.waitForDeploymentFinished(1000);
        assertEquals(coordinator, result);
    }

    @Test
    public void testWaitForDeploymentFinishedTimeout() {
        assertThrows(TimeoutException.class, () -> {
            context.waitForDeploymentFinished(1);
        });
    }

    @Test
    public void testKill() throws Exception {
        ArrowFlightSqlConnectContext context = new ArrowFlightSqlConnectContext("token");

        // mock executor
        StmtExecutor executor = mock(StmtExecutor.class);
        Field executorField = ConnectContext.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        executorField.set(context, executor);

        // mock coordinator
        Coordinator coordinator = mock(Coordinator.class);
        CompletableFuture<Coordinator> future = new CompletableFuture<>();
        future.complete(coordinator);
        Field futureField = ArrowFlightSqlConnectContext.class.getDeclaredField("coordinatorFuture");
        futureField.setAccessible(true);
        futureField.set(context, future);

        // mock allocator
        BufferAllocator allocator = mock(BufferAllocator.class);
        Field allocatorField = ArrowFlightSqlConnectContext.class.getDeclaredField("allocator");
        allocatorField.setAccessible(true);
        allocatorField.set(context, allocator);

        MysqlChannel mysqlChannel = mock(MysqlChannel.class);
        Field mysqlChannelField = ConnectContext.class.getDeclaredField("mysqlChannel");
        mysqlChannelField.setAccessible(true);
        mysqlChannelField.set(context, mysqlChannel);

        // mock ExecuteEnv.getInstance().getScheduler().unregisterConnection(this);
        ConnectScheduler scheduler = mock(ConnectScheduler.class);
        ExecuteEnv mockEnv = mock(ExecuteEnv.class);
        when(mockEnv.getScheduler()).thenReturn(scheduler);

        try (MockedStatic<ExecuteEnv> mocked = mockStatic(ExecuteEnv.class)) {
            mocked.when(ExecuteEnv::getInstance).thenReturn(mockEnv);

            context.kill(true, "cancelled");

            verify(mysqlChannel).close();

            verify(executor).cancel("cancelled");

            verify(coordinator).cancel("cancelled");

            verify(scheduler).unregisterConnection(context);
        }
    }
}
