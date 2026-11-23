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

package com.starrocks.connector.metadata;

import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Status;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.connector.metadata.iceberg.IcebergMetadataCollectJob;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import mockit.VerificationsInOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataExecutorTest {
    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @AfterEach
    public void afterEach() {
        ConnectContext.remove();
    }

    private MetadataCollectJob newJob(ConnectContext parent) {
        MetadataCollectJob job = new MetadataCollectJob("default_catalog", "test", "t", TResultSinkType.CUSTOMIZED) {
            @Override
            protected String buildCollectMetadataSQL() {
                return "select 1";
            }
        };
        job.init(parent);
        job.getContext().getSessionVariable().setEnableProfile(false);
        job.getContext().getSessionVariable().setQueryTimeoutS(60);
        parent.setThreadLocalInfo();
        return job;
    }


    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testIcebergJobInheritsCallerAdmissionPolicy(boolean queueEnabled) throws Exception {
        ConnectContext parent = newParent();
        parent.getSessionVariable().setEnableQueryQueue(queueEnabled);
        GlobalStateMgr.getCurrentState().getVariableMgr().setSystemVariable(parent.getSessionVariable(),
                new SystemVariable(SessionVariable.QUERY_MEM_LIMIT, new IntLiteral(1234567)), false);
        new MockUp<CoordinatorPreprocessor>() {
            @Mock
            public TWorkGroup prepareResourceGroup(ConnectContext context, ResourceGroupClassifier.QueryType type) {
                assertSame(parent, context);
                return new TWorkGroup().setId(123).setName("caller_group");
            }
        };
        MetadataCollectJob job = new IcebergMetadataCollectJob("iceberg", "db", "table",
                TResultSinkType.METADATA_ICEBERG, 1, "");
        job.init(parent);
        ConnectContext child = job.getContext();
        assertSame(parent, child.getParent());
        assertEquals(UserIdentity.ROOT, child.getCurrentUserIdentity());
        assertTrue(child.isNeedQueued());
        assertEquals(queueEnabled, child.getSessionVariable().isEnableQueryQueue());
        assertEquals("caller_group", child.getSessionVariable().getResourceGroup());
        assertEquals(1234567, child.getSessionVariable().getQueryMemLimit());
        assertEquals(parent.getCurrentWarehouseId(), child.getCurrentWarehouseId());
    }


    @Test
    public void testIcebergJobStartsAfterCallerQueueWait() {
        ConnectContext parent = newParent();
        parent.getSessionVariable().setQueryTimeoutS(60);
        parent.setStartTime(Instant.now().minusSeconds(125));
        parent.setPendingTimeSecond(120);
        MetadataCollectJob job = new IcebergMetadataCollectJob("iceberg", "db", "table",
                TResultSinkType.METADATA_ICEBERG, 1, "");

        job.init(parent);

        assertSame(parent, job.getContext().getParent());
        int remaining = SimpleExecutor.checkInternalQueryState(job.getContext());
        assertTrue(remaining > 50 && remaining <= 55, "remaining=" + remaining);
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "dispatch failure", "result failure"})
    public void testReleaseSlotBeforeAsyncProfile(String outcome, @Mocked DefaultCoordinator coordinator) throws Exception {
        MetadataCollectJob job = newJob(newParent());
        job.getContext().getSessionVariable().setEnableProfile(true);
        new MockUp<StatementPlanner>() {
            @Mock
            public ExecPlan plan(StatementBase statement, ConnectContext context, TResultSinkType sink) {
                return new ExecPlan();
            }
        };
        new MockUp<DefaultCoordinator.Factory>() {
            @Mock
            public DefaultCoordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                    List<ScanNode> scans, TDescriptorTable descriptor, ExecPlan plan) {
                return coordinator;
            }
        };
        new Expectations() {
            {
                coordinator.getExecStatus();
                result = new Status();
                minTimes = 0;
                coordinator.getQueryProfile();
                result = new RuntimeProfile("Execution");
                coordinator.tryProcessProfileAsync((Consumer<Boolean>) any);
                result = true;
                if (outcome.equals("dispatch failure")) {
                    coordinator.exec();
                    result = new IllegalStateException(outcome);
                } else {
                    coordinator.getNext();
                    result = outcome.equals("result failure") ? new IllegalStateException(outcome) : new RowBatch();
                }
            }
        };
        try {
            job.asyncCollectMetadata();
            assertEquals(outcome.equals("success") ? QueryState.MysqlStateType.EOF : QueryState.MysqlStateType.ERR,
                    job.getContext().getState().getStateType());
            // The profile is still collecting, but its admission must already have been returned.
            assertSame(coordinator, QeProcessorImpl.INSTANCE.getCoordinator(job.getContext().getExecutionId()));
            new VerificationsInOrder() {
                {
                    coordinator.onReleaseSlots();
                    times = 1;
                    coordinator.tryProcessProfileAsync((Consumer<Boolean>) any);
                    times = 1;
                }
            };
        } finally {
            QeProcessorImpl.INSTANCE.unMonitorQuery(job.getContext().getExecutionId());
            QeProcessorImpl.INSTANCE.unregisterQuery(job.getContext().getExecutionId());
        }
    }

    private void stopQuery(ConnectContext parent, ConnectContext child, String reason) {
        switch (reason) {
            case "parent timeout" -> parent.setStartTime(Instant.now().minusSeconds(1000));
            case "child timeout" -> child.setStartTime(Instant.now().minusSeconds(1000));
            case "cancelled" -> parent.getExecutor().cancel("test cancellation");
            default -> throw new AssertionError(reason);
        }
    }

    private ConnectContext newParent() {
        ConnectContext parent = UtFrameUtils.createDefaultCtx();
        parent.getSessionVariable().setQueryTimeoutS(300);
        parent.setStartTime();
        parent.setExecutor(new StmtExecutor(parent,
                SqlParser.parseOneWithStarRocksDialect("select 1", parent.getSessionVariable())));
        return parent;
    }

    @ParameterizedTest
    @ValueSource(strings = {"parent timeout", "child timeout", "cancelled"})
    public void testStoppedQueryDoesNotPlan(String reason) {
        ConnectContext parent = newParent();
        MetadataCollectJob job = newJob(parent);
        stopQuery(parent, job.getContext(), reason);
        new MockUp<StatementPlanner>() {
            @Mock
            public ExecPlan plan(StatementBase statement, ConnectContext context, TResultSinkType sink) {
                throw new AssertionError("Stopped metadata queries must not be planned");
            }
        };
        job.asyncCollectMetadata();
        assertEquals(QueryState.MysqlStateType.ERR, job.getContext().getState().getStateType());
        assertTrue(job.getContext().getState().getErrorMessage()
                .contains(reason.equals("cancelled") ? "cancelled" : "timed out"));
        assertTrue(parent.getExecutor().getSubStmtExecutors().isEmpty());
        assertSame(parent, ConnectContext.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"parent timeout", "child timeout", "cancelled"})
    public void testStoppedDuringPlanningDoesNotCreateCoordinator(String reason, @Mocked ScanNode scan) {
        ConnectContext parent = newParent();
        MetadataCollectJob job = newJob(parent);
        new MockUp<StatementPlanner>() {
            @Mock
            public ExecPlan plan(StatementBase statement, ConnectContext context, TResultSinkType sink) {
                stopQuery(parent, context, reason);
                ExecPlan plan = new ExecPlan();
                plan.getScanNodes().add(scan);
                return plan;
            }
        };
        new MockUp<DefaultCoordinator.Factory>() {
            @Mock
            public DefaultCoordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                    List<ScanNode> scans, TDescriptorTable descriptor, ExecPlan plan) {
                throw new AssertionError("Stopped metadata queries must not create a coordinator");
            }
        };
        job.asyncCollectMetadata();
        assertEquals(QueryState.MysqlStateType.ERR, job.getContext().getState().getStateType());
        assertTrue(job.getContext().getState().getErrorMessage()
                .contains(reason.equals("cancelled") ? "cancelled" : "timed out"));
        assertSame(parent, ConnectContext.get());
        new Verifications() {
            {
                scan.clear();
                times = 1;
            }
        };
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    public void testExecutionUsesSmallerRemainingBudget(boolean parentIsShorter, boolean queued,
                                                       @Mocked DefaultCoordinator coordinator) throws Exception {
        ConnectContext parent = newParent();
        parent.getSessionVariable().setQueryTimeoutS(parentIsShorter ? 30 : 300);
        parent.getSessionVariable().setEnableSPMRewrite(true);
        MetadataCollectJob job = newJob(parent);
        new MockUp<StatementPlanner>() {
            @Mock
            public ExecPlan plan(StatementBase statement, ConnectContext context, TResultSinkType sink) {
                assertSame(job.getContext(), ConnectContext.get());
                assertFalse(context.getSessionVariable().isEnableSPMRewrite());
                assertFalse(context.getSessionVariable().isEnableMaterializedViewRewrite());
                // Planning consumes execution time; each context's own queue wait does not.
                int parentPendingSeconds = queued ? 120 : 0;
                int childPendingSeconds = queued ? 90 : 0;
                parent.setStartTime(Instant.now().minusSeconds(parentPendingSeconds + 10));
                parent.setPendingTimeSecond(parentPendingSeconds);
                context.setStartTime(Instant.now().minusSeconds(childPendingSeconds + 20));
                context.setPendingTimeSecond(childPendingSeconds);
                return new ExecPlan();
            }
        };
        new MockUp<DefaultCoordinator.Factory>() {
            @Mock
            public DefaultCoordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                    List<ScanNode> scans, TDescriptorTable descriptor, ExecPlan plan) {
                return coordinator;
            }
        };
        new Expectations() {
            {
                coordinator.getNext();
                result = new RowBatch();
            }
        };
        job.asyncCollectMetadata();
        assertEquals(QueryState.MysqlStateType.EOF, job.getContext().getState().getStateType());
        assertSame(parent, ConnectContext.get());
        assertEquals(60, job.getContext().getSessionVariable().getQueryTimeoutS());
        assertEquals(parentIsShorter ? 30 : 300, parent.getSessionVariable().getQueryTimeoutS());
        assertTrue(parent.getSessionVariable().isEnableSPMRewrite());
        assertEquals(List.of(job.getContext().getExecutor()), parent.getExecutor().getSubStmtExecutors());
        List<Integer> timeouts = new ArrayList<>();
        new Verifications() {
            {
                coordinator.setTimeoutSecond(withCapture(timeouts));
                times = 1;
                coordinator.exec();
                times = 1;
                coordinator.clearExternalResources();
                times = 1;
            }
        };
        int expected = parentIsShorter ? 20 : 40;
        assertTrue(timeouts.get(0) > expected - 5 && timeouts.get(0) <= expected, timeouts.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"parent timeout", "cancelled"})
    public void testStoppedDuringCoordinatorCreationDoesNotDispatch(String reason, @Mocked DefaultCoordinator coordinator)
            throws Exception {
        ConnectContext parent = newParent();
        MetadataCollectJob job = newJob(parent);
        new MockUp<StatementPlanner>() {
            @Mock
            public ExecPlan plan(StatementBase statement, ConnectContext context, TResultSinkType sink) {
                return new ExecPlan();
            }
        };
        new MockUp<DefaultCoordinator.Factory>() {
            @Mock
            public DefaultCoordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                    List<ScanNode> scans, TDescriptorTable descriptor, ExecPlan plan) {
                stopQuery(parent, context, reason);
                return coordinator;
            }
        };
        new Expectations() {
            {
                coordinator.getExecStatus();
                result = new Status();
            }
        };
        job.asyncCollectMetadata();
        assertEquals(QueryState.MysqlStateType.ERR, job.getContext().getState().getStateType());
        assertSame(parent, ConnectContext.get());
        new Verifications() {
            {
                coordinator.exec();
                times = 0;
                coordinator.clearExternalResources();
                times = 1;
            }
        };
    }
}
