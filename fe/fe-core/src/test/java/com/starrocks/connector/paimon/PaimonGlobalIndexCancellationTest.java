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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.Status;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class PaimonGlobalIndexCancellationTest {
    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @AfterEach
    public void removeContext() {
        ConnectContext.remove();
    }

    @Test
    public void testKillQueryCancelsRunningIndexCoordinator() throws Exception {
        ConnectContext outer = createOuterContext();
        DefaultCoordinator coordinator = mock(DefaultCoordinator.class);
        Status status = new Status();
        when(coordinator.getExecStatus()).thenReturn(status);
        CountDownLatch fetching = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        doAnswer(invocation -> {
            status.setStatus(Status.CANCELLED);
            cancelled.countDown();
            return null;
        }).when(coordinator).cancel(anyString());
        when(coordinator.getNext()).thenAnswer(invocation -> {
            fetching.countDown();
            assertTrue(cancelled.await(10, TimeUnit.SECONDS), "inner coordinator did not receive KILL QUERY");
            return new RowBatch();
        });
        mockPlanAndCoordinator(coordinator, () -> { });
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<?> query = worker.submit(() -> {
            outer.setThreadLocalInfo();
            try {
                createService().evaluate();
            } finally {
                ConnectContext.remove();
            }
        });
        try {
            assertTrue(fetching.await(10, TimeUnit.SECONDS));
            outer.kill(false, "test kill query");
            ExecutionException failure = assertThrows(ExecutionException.class, () -> query.get(10, TimeUnit.SECONDS));
            assertInstanceOf(SemanticException.class, failure.getCause());
            assertFalse(outer.isKilled(), "KILL QUERY must not require killing the connection");
            assertTrue(outer.getExecutor().getSubStmtExecutors().get(0).isCancelled());
            verify(coordinator).cancel("test kill query");
        } finally {
            query.cancel(true);
            worker.shutdownNow();
        }
    }

    @Test
    public void testCancellationBeforeCoordinatorCreationDoesNotDeploy() throws Exception {
        ConnectContext outer = createOuterContext();
        DefaultCoordinator coordinator = mock(DefaultCoordinator.class);
        when(coordinator.getExecStatus()).thenReturn(new Status());
        mockPlanAndCoordinator(coordinator, () -> outer.kill(false, "cancel during coordinator creation"));

        assertThrows(SemanticException.class, () -> createService().evaluate());

        verify(coordinator).cancel(anyString());
        verify(coordinator, never()).exec();
        verify(coordinator, never()).getNext();
    }

    @Test
    public void testAlreadyCancelledQueryDoesNotPlanIndex() throws Exception {
        ConnectContext outer = createOuterContext();
        DefaultCoordinator coordinator = mock(DefaultCoordinator.class);
        mockPlanAndCoordinator(coordinator, () -> { });
        outer.kill(false, "already cancelled");

        assertThrows(SemanticException.class, () -> createService().evaluate());

        verifyNoInteractions(coordinator);
        assertTrue(outer.getExecutor().getSubStmtExecutors().isEmpty());
    }

    private static ConnectContext createOuterContext() throws Exception {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        context.setThreadLocalInfo();
        context.setExecutor(new StmtExecutor(context,
                SqlParser.parseOneWithStarRocksDialect("select 1", context.getSessionVariable())));
        return context;
    }

    private static PaimonGlobalIndexService createService() {
        PaimonTable table = mock(PaimonTable.class);
        when(table.getCatalogName()).thenReturn("paimon");
        when(table.getCatalogDBName()).thenReturn("db");
        when(table.getCatalogTableName()).thenReturn("orders");
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        IndexCondition condition = new IndexCondition(new BinaryPredicateOperator(
                BinaryType.EQ, column, ConstantOperator.createInt(1)), Map.of(1, 0));
        return new PaimonGlobalIndexService(table,
                PaimonGlobalIndexRequest.create("file:/tmp/orders", "main", 1L, 0L, 1000L, condition));
    }

    private static void mockPlanAndCoordinator(DefaultCoordinator coordinator, Runnable beforeCoordinatorCreated) {
        ExecPlan plan = mock(ExecPlan.class);
        when(plan.getDescTbl()).thenReturn(new DescriptorTable());
        new MockUp<StatementPlanner>() {
            @Mock
            public ExecPlan plan(StatementBase statement, ConnectContext context, TResultSinkType resultSinkType) {
                return plan;
            }
        };
        new MockUp<DefaultCoordinator.Factory>() {
            @Mock
            public DefaultCoordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                            List<ScanNode> scanNodes, TDescriptorTable descriptor,
                                                            ExecPlan execPlan) {
                beforeCoordinatorCreated.run();
                return coordinator;
            }
        };
    }
}
