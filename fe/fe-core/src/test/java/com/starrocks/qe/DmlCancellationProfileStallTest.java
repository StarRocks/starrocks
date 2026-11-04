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

import com.starrocks.analysis.IntLiteral;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.qe.scheduler.QueryRuntimeProfile;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.system.Backend;
import com.starrocks.system.BackendHbResponse;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Verifies that missing fragment reports cannot indefinitely delay an already failed DML query.
 */
public class DmlCancellationProfileStallTest extends PlanTestNoneDBBase {
    private static final String WRITE_ERROR = "injected write failure";
    private static final TUniqueId FAILED_INSTANCE = new TUniqueId(1, 1);
    private static final TUniqueId MISSING_INSTANCE = new TUniqueId(1, 2);

    private ConnectContext ctx;
    private DefaultCoordinator coordinator;
    private QueryRuntimeProfile profile;
    private ExecutionDAG dag;
    private Backend worker;

    @BeforeEach
    public void setUp() {
        super.setUp();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setExecutionId(new TUniqueId(1, 0));
        ctx.getSessionVariable().setEnableProfile(false);
        ctx.getSessionVariable().setBigQueryProfileThreshold("30s");
        // Enable automatic profiling for a long-running insert without sleeping.
        ctx.setStartTime(Instant.now().minusSeconds(60));
        coordinator = new DefaultCoordinator.Factory().createInsertScheduler(
                ctx, Collections.emptyList(), Collections.emptyList(), new TDescriptorTable(), null);
        profile = Deencapsulation.getField(coordinator, "queryProfile");
        profile.initFragmentProfiles(1);
        profile.attachInstances(Arrays.asList(FAILED_INSTANCE, MISSING_INSTANCE));
        dag = coordinator.getPrepareInfo().getExecutionDAG();
        worker = new Backend(99999, "127.0.0.1", 9050);
        worker.setAlive(true);
        worker.setLastStartTime(100);
        worker.setLastMissingHeartbeatTime(200);
        addExecutingInstance(FAILED_INSTANCE, 0);
        addExecutingInstance(MISSING_INSTANCE, 1);

        // Exercise production execution state, cancellation, report processing and profile completion.
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(
                    TNetworkAddress address, TUniqueId queryId, TUniqueId instanceId,
                    PPlanFragmentCancelReason reason, boolean pipeline, String errorMessage) {
                return CompletableFuture.completedFuture(new PCancelPlanFragmentResult());
            }
        };
    }

    @Test
    public void testFailedInsertFinishesAfterPositiveProfileGracePeriod() throws Exception {
        setProfileTimeout(1);
        Assertions.assertFalse(ctx.getSessionVariable().isEnableProfile());
        Assertions.assertTrue(ctx.isProfileEnabled(), "automatic profiling is active for this long insert");

        reportWriteFailure();
        assertWaitingForMissingProfile();
        Assertions.assertTrue(coordinator.checkBackendState());

        // A one-second grace period must end the wait before either the five-second polling interval
        // or the much longer statement timeout, even when the remaining worker stays healthy.
        assertJoinFinishesPromptly();
        Assertions.assertTrue(coordinator.isDone());
        assertOriginalError();
        Assertions.assertEquals(Collections.singletonList(DebugUtil.printId(MISSING_INSTANCE)),
                profile.getUnfinishedInstanceIds(), "retain the missing instance for profile diagnostics");
    }

    @Test
    public void testFinalReportWithinGracePeriodCompletesFailedInsert() throws Exception {
        setProfileTimeout(60);
        reportWriteFailure();
        assertWaitingForMissingProfile();

        coordinator.updateFragmentExecStatus(report(MISSING_INSTANCE, 1, TStatusCode.CANCELLED, "cancelled"));

        Assertions.assertTrue(coordinator.isDone());
        Assertions.assertTrue(profile.getUnfinishedInstanceIds().isEmpty());
        assertJoinFinishesPromptly();
        assertOriginalError();
    }

    @Test
    public void testFailedInsertFinishesImmediatelyWhenProfilingIsDisabled() throws Exception {
        setProfileTimeout(60);
        ctx.getSessionVariable().setBigQueryProfileThreshold("0s");
        Assertions.assertFalse(ctx.isProfileEnabled());

        reportWriteFailure();

        Assertions.assertEquals(FragmentInstanceExecState.State.CANCELLING, dag.getExecution(1).getState());
        Assertions.assertTrue(coordinator.isDone(), "cancellation releases completion without final reports");
        assertJoinFinishesPromptly();
        assertOriginalError();
    }

    @Test
    public void testShutdownWorkerCannotHoldFailedInsertPastGracePeriod() throws Exception {
        setProfileTimeout(60);
        long lastStartTime = worker.getLastStartTime();
        long lastMissingHeartbeatTime = worker.getLastMissingHeartbeatTime();
        worker.handleHbResponse(new BackendHbResponse(worker.getId(), TStatusCode.SHUTDOWN, "shutting down"), false);

        Assertions.assertFalse(worker.isAlive());
        Assertions.assertEquals(ComputeNode.Status.SHUTDOWN, worker.getStatus());
        Assertions.assertEquals(lastStartTime, worker.getLastStartTime());
        Assertions.assertEquals(lastMissingHeartbeatTime, worker.getLastMissingHeartbeatTime());
        Assertions.assertTrue(dag.getExecution(1).isBackendStateHealthy());
        Assertions.assertTrue(coordinator.checkBackendState(), "shutdown alone does not change health timestamps");

        reportWriteFailure();
        assertWaitingForMissingProfile();
        // Move the cancellation timestamp beyond the configured grace period without a minute-long sleep.
        Deencapsulation.setField(coordinator, "cancelledAtMs", System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(61));

        assertJoinFinishesPromptly();
        Assertions.assertTrue(coordinator.isDone());
        assertOriginalError();
    }

    @Test
    public void testUncancelledInsertStillWaitsForFinalReports() throws Exception {
        setProfileTimeout(0);

        Assertions.assertFalse(coordinator.join(1), "the cancellation grace period does not limit a healthy query");
        Assertions.assertFalse(coordinator.isDone());
        Assertions.assertTrue(coordinator.getExecStatus().ok());

        coordinator.updateFragmentExecStatus(report(FAILED_INSTANCE, 0, TStatusCode.OK, ""));
        Assertions.assertFalse(coordinator.isDone());
        coordinator.updateFragmentExecStatus(report(MISSING_INSTANCE, 1, TStatusCode.OK, ""));

        Assertions.assertTrue(coordinator.isDone());
        assertJoinFinishesPromptly();
        Assertions.assertTrue(coordinator.getExecStatus().ok());
    }

    private void reportWriteFailure() {
        coordinator.updateFragmentExecStatus(report(FAILED_INSTANCE, 0, TStatusCode.INTERNAL_ERROR, WRITE_ERROR));
        assertOriginalError();
    }

    private void assertOriginalError() {
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, coordinator.getExecStatus().getErrorCode());
        Assertions.assertEquals(WRITE_ERROR, coordinator.getExecStatus().getErrorMsg());
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
    }

    private void assertWaitingForMissingProfile() {
        Assertions.assertEquals(FragmentInstanceExecState.State.CANCELLING, dag.getExecution(1).getState());
        Assertions.assertEquals(Collections.singletonList(DebugUtil.printId(MISSING_INSTANCE)),
                profile.getUnfinishedInstanceIds());
        Assertions.assertFalse(coordinator.isDone());
    }

    private void assertJoinFinishesPromptly() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(3), () -> Assertions.assertTrue(coordinator.join(30)));
    }

    private void setProfileTimeout(int timeoutSecond) throws Exception {
        GlobalStateMgr.getCurrentState().getVariableMgr().setSystemVariable(
                ctx.getSessionVariable(),
                new SystemVariable(SessionVariable.PROFILE_TIMEOUT, new IntLiteral(timeoutSecond)), true);
    }

    private void addExecutingInstance(TUniqueId instanceId, int index) {
        JobSpec jobSpec = Deencapsulation.getField(coordinator, "jobSpec");
        TExecPlanFragmentParams request = new TExecPlanFragmentParams();
        request.setBackend_num(index);
        request.setParams(new TPlanFragmentExecParams().setFragment_instance_id(instanceId));
        FragmentInstanceExecState execution = FragmentInstanceExecState.createExecution(
                jobSpec, new PlanFragmentId(0), 0, request, worker);
        Deencapsulation.setField(execution, "state", FragmentInstanceExecState.State.EXECUTING);
        dag.addExecution(execution);
        dag.addNeedCheckExecution(execution);
    }

    private TReportExecStatusParams report(TUniqueId instanceId, int index, TStatusCode code, String message) {
        TReportExecStatusParams report = new TReportExecStatusParams();
        report.setQuery_id(ctx.getExecutionId());
        report.setFragment_instance_id(instanceId);
        report.setBackend_num(index);
        report.setBackend_id(worker.getId());
        report.setDone(true);
        report.setStatus(new TStatus(code).setError_msgs(Collections.singletonList(message)));
        return report;
    }
}
