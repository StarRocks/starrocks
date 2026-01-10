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

package com.starrocks.load.batchwrite;

import com.starrocks.common.Config;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.streamload.StreamLoadHttpHeader;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnStateCallbackFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class MergeCommitTaskTest extends BatchWriteTestBase {

    private String label;
    private TUniqueId loadId;
    private StreamLoadKvParams kvParams;
    private StreamLoadInfo streamLoadInfo;
    private String warehouseName;
    private TestMergeCommitTaskCallback loadExecuteCallback;
    private Coordinator.Factory coordinatorFactory;
    private Coordinator coordinator;

    @BeforeEach
    public void setup() throws Exception {
        loadId = UUIDUtil.genTUniqueId();
        label = "merge_commit_" + DebugUtil.printId(loadId);

        Map<String, String> map = new HashMap<>();
        map.put(StreamLoadHttpHeader.HTTP_FORMAT, "json");
        map.put(StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE, "true");
        map.put(StreamLoadHttpHeader.HTTP_BATCH_WRITE_ASYNC, "true");
        kvParams = new StreamLoadKvParams(map);
        streamLoadInfo = StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), kvParams);
        warehouseName = "default_warehouse";
        loadExecuteCallback = new TestMergeCommitTaskCallback();
        coordinatorFactory = Mockito.mock(Coordinator.Factory.class);
        coordinator = Mockito.mock(Coordinator.class);
    }

    @Test
    public void testBasicAccessors() {
        long taskId = GlobalStateMgr.getCurrentState().getNextId();
        Set<Long> backendIds = new HashSet<>(Arrays.asList(10002L, 10003L));
        MergeCommitTask task = new MergeCommitTask(
                taskId,
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                backendIds,
                coordinatorFactory,
                loadExecuteCallback);

        assertEquals(taskId, task.getId());
        assertEquals(label, task.getLabel());
        assertTrue(task.containsBackend(10002L));
        assertFalse(task.containsBackend(99999L));
        assertTrue(task.getTxnId() < 0);

        Set<Long> returnedBackendIds = task.getBackendIds();
        assertEquals(backendIds, returnedBackendIds);
        try {
            returnedBackendIds.add(10004L);
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException ignored) {
            // ok
        }
    }

    @Test
    public void testLoadSuccess() {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        stubCoordinatorSuccess();

        task.run();
        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        assertEquals(TransactionStatus.VISIBLE, getTxnStatus(label));
        assertTrue(task.getTxnId() > 0);
        assertNull(ProfileManager.getInstance().getProfile(DebugUtil.printId(loadId)));
        // The task must always remove its txn callback after completion to avoid leaking callbacks.
        TxnStateCallbackFactory callbackFactory =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory();
        assertNull(callbackFactory.getCallback(task.getId()));
    }

    @Test
    public void testExecuteFailWhenStatusNotOk() {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        stubCoordinatorSuccess();
        when(coordinator.getExecStatus()).thenReturn(new Status(TStatusCode.INTERNAL_ERROR, "artificial failure"));

        task.run();
        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        assertEquals(TransactionStatus.ABORTED, getTxnStatus(label));
        assertTrue(task.getTxnId() > 0);
    }

    @Test
    public void testExecuteTimeout() {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        stubCoordinatorCommon();
        when(coordinator.join(anyInt())).thenReturn(false);

        task.run();
        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        assertEquals(TransactionStatus.ABORTED, getTxnStatus(label));
        assertTrue(task.getTxnId() > 0);
        // Callback must be removed even on failure/timeout.
        TxnStateCallbackFactory callbackFactory =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory();
        assertNull(callbackFactory.getCallback(task.getId()));
    }

    @Test
    public void testTableDoesNotExist() {
        String fakeTableName = TABLE_NAME_1_1 + "_fake";
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, fakeTableName),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        task.run();
        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        assertEquals(TransactionStatus.UNKNOWN, getTxnStatus(label));
        assertTrue(task.getTxnId() < 0);
    }

    @Test
    public void testDatabaseDoesNotExist() {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                -1,
                new TableId("merge_commit_db_not_exist", TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        task.run();
        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        assertEquals(TransactionStatus.UNKNOWN, getTxnStatus(label));
        assertTrue(task.getTxnId() < 0);
    }

    @Test
    public void testMaxFilterRatioThrowsDataQualityException() {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        stubCoordinatorSuccess();
        Map<String, String> counters = new HashMap<>();
        counters.put(LoadEtlTask.DPP_NORMAL_ALL, "100");
        counters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "100");
        when(coordinator.getLoadCounters()).thenReturn(counters);
        when(coordinator.getTrackingUrl()).thenReturn("test_tracking_url");

        task.run();
        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        assertEquals(TransactionStatus.ABORTED, getTxnStatus(label));
        assertTrue(task.getTxnId() > 0);
    }

    @Test
    public void testIsActiveWindowDuringLoading() throws Exception {
        int mergeCommitIntervalMs = 200;
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                mergeCommitIntervalMs,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        stubCoordinatorCommon();
        CountDownLatch joinEntered = new CountDownLatch(1);
        CountDownLatch joinRelease = new CountDownLatch(1);
        doAnswer(invocation -> {
            joinEntered.countDown();
            joinRelease.await(3, TimeUnit.SECONDS);
            return true;
        }).when(coordinator).join(anyInt());
        when(coordinator.getExecStatus()).thenReturn(new Status());
        when(coordinator.getCommitInfos()).thenReturn(buildCommitInfos());

        Thread t = new Thread(task::run);
        t.start();

        assertTrue(joinEntered.await(30, TimeUnit.SECONDS));
        // Callback should be registered once the task begins transaction and enters execution.
        TxnStateCallbackFactory callbackFactory =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory();
        assertNotNull(callbackFactory.getCallback(task.getId()));
        assertTrue(task.isActive());

        Thread.sleep(mergeCommitIntervalMs + 100L);
        assertFalse(task.isActive());

        joinRelease.countDown();
        t.join(30_000);
        // Callback should be removed after task completes.
        assertNull(callbackFactory.getCallback(task.getId()));
    }

    @Test
    public void testCancelBeforeRunSkipsCoordinatorCreation() {
        Coordinator.Factory factory = Mockito.mock(Coordinator.Factory.class);
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                factory,
                loadExecuteCallback);

        task.cancel("unit-test cancel");
        task.run();

        verifyNoInteractions(factory);
        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        assertTrue(task.getTxnId() < 0);
    }

    @Test
    public void testCancelAfterFinish() {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        stubCoordinatorSuccess();

        task.run();
        assertEquals(MergeCommitTask.TaskState.FINISHED, getTaskState(task));
        String stateMsgBeforeCancel = getTaskStateMessage(task);

        // Cancel after task finished should not change task state or state message.
        task.cancel("cancel after finish");
        assertEquals(MergeCommitTask.TaskState.FINISHED, getTaskState(task));
        assertEquals(stateMsgBeforeCancel, getTaskStateMessage(task));
    }

    @Test
    public void testAbortTransactionTriggersCancel() throws Exception {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        List<StateTransition> transitions = Collections.synchronizedList(new ArrayList<>());
        task.setStateTransitionObserver((from, to, msg, hasHandler) ->
                transitions.add(new StateTransition(from, to, msg, hasHandler)));

        stubCoordinatorCommon();
        CountDownLatch joinEntered = new CountDownLatch(1);
        CountDownLatch joinRelease = new CountDownLatch(1);
        doAnswer(invocation -> {
            joinEntered.countDown();
            joinRelease.await(3, TimeUnit.SECONDS);
            return true;
        }).when(coordinator).join(anyInt());
        // After cancellation we want the execution to fail fast instead of attempting commit.
        when(coordinator.getExecStatus()).thenReturn(new Status(TStatusCode.INTERNAL_ERROR, "aborted"));
        when(coordinator.getCommitInfos()).thenReturn(buildCommitInfos());

        Thread t = new Thread(task::run);
        t.start();

        // Ensure we are in LOADING state (cancel handler is installed) before aborting the transaction.
        assertTrue(joinEntered.await(30, TimeUnit.SECONDS), "join not entered in time");
        long deadlineMs = System.currentTimeMillis() + 30_000;
        while (task.getTxnId() < 0 && System.currentTimeMillis() < deadlineMs) {
            Thread.sleep(10);
        }
        assertTrue(task.getTxnId() > 0, "txnId is not assigned in time");

        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .abortTransaction(DATABASE_1.getId(), task.getTxnId(), "external abort reason");

        verify(coordinator, Mockito.timeout(30_000))
                .cancel(eq(com.starrocks.proto.PPlanFragmentCancelReason.USER_CANCEL), eq("external abort reason"));

        joinRelease.countDown();
        t.join(30_000);

        boolean hasCancelled = transitions.stream().anyMatch(
                tr -> tr.toState == MergeCommitTask.TaskState.CANCELLED &&
                        "external abort reason".equals(tr.toStateMsg));
        assertTrue(hasCancelled, "expected CANCELLED transition, got: " + transitions);
    }

    @Test
    public void testProfile() throws Exception {
        starRocksAssert.alterTableProperties(
                String.format("alter table %s.%s set('enable_load_profile'='true');", DB_NAME_1, TABLE_NAME_1_1));
        long oldIntervalSecond = Config.load_profile_collect_interval_second;
        Config.load_profile_collect_interval_second = 1;
        try {
            MergeCommitTask task = new MergeCommitTask(
                    GlobalStateMgr.getCurrentState().getNextId(),
                    DATABASE_1.getId(),
                    new TableId(DB_NAME_1, TABLE_NAME_1_1),
                    label,
                    loadId,
                    streamLoadInfo,
                    1000,
                    kvParams,
                    warehouseName,
                    new HashSet<>(Arrays.asList(10002L, 10003L)),
                    coordinatorFactory,
                    loadExecuteCallback
            );

            stubCoordinatorSuccess();
            doNothing().when(coordinator).collectProfileSync();
            when(coordinator.buildQueryProfile(true)).thenReturn(new RuntimeProfile("Execution"));

            task.run();
            assertNotNull(ProfileManager.getInstance().getProfile(DebugUtil.printId(loadId)));
        } finally {
            Config.load_profile_collect_interval_second = oldIntervalSecond;
            starRocksAssert.alterTableProperties(
                    String.format("alter table %s.%s set('enable_load_profile'='false');", DB_NAME_1, TABLE_NAME_1_1));
        }
    }

    @Test
    public void testCancelDuringLoadingTriggersCoordinatorCancel() throws Exception {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        stubCoordinatorCommon();
        CountDownLatch joinEntered = new CountDownLatch(1);
        CountDownLatch joinRelease = new CountDownLatch(1);
        doAnswer(invocation -> {
            joinEntered.countDown();
            joinRelease.await(3, TimeUnit.SECONDS);
            return true;
        }).when(coordinator).join(anyInt());
        when(coordinator.getExecStatus()).thenReturn(new Status());
        when(coordinator.getCommitInfos()).thenReturn(buildCommitInfos());
        when(coordinator.getLoadCounters()).thenReturn(Collections.emptyMap());
        when(coordinator.getRejectedRecordPaths()).thenReturn(Collections.emptyList());
        when(coordinator.getFailInfos()).thenReturn(Collections.emptyList());

        Thread t = new Thread(task::run);
        t.start();

        assertTrue(joinEntered.await(30, TimeUnit.SECONDS));
        task.cancel("unit-test cancel");
        verify(coordinator).cancel(eq(com.starrocks.proto.PPlanFragmentCancelReason.USER_CANCEL), eq("unit-test cancel"));

        joinRelease.countDown();
        t.join(30_000);

        assertEquals(1, loadExecuteCallback.getFinishedLoads().size());
        assertEquals(label, loadExecuteCallback.getFinishedLoads().get(0));
        assertEquals(TransactionStatus.ABORTED, getTxnStatus(label));
    }

    @Test
    public void testStateMachineTransitionsOnSuccess() {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        List<StateTransition> transitions = Collections.synchronizedList(new ArrayList<>());
        task.setStateTransitionObserver((from, to, msg, hasHandler) ->
                transitions.add(new StateTransition(from, to, msg, hasHandler)));

        stubCoordinatorSuccess();

        task.run();

        // Success path expected transitions:
        // PENDING -> BEGIN_TXN -> PLANNING -> LOADING -> COMMITTING -> COMMITTED -> FINISHED
        assertEquals(6, transitions.size(), "unexpected transition count: " + transitions);
        assertTransition(transitions.get(0),
                MergeCommitTask.TaskState.PENDING, MergeCommitTask.TaskState.BEGINNING_TXN, "", false);
        assertTransition(transitions.get(1),
                MergeCommitTask.TaskState.BEGINNING_TXN, MergeCommitTask.TaskState.PLANNING, "", false);
        assertTransition(transitions.get(2),
                MergeCommitTask.TaskState.PLANNING, MergeCommitTask.TaskState.EXECUTING, "", true);
        assertTransition(transitions.get(3),
                MergeCommitTask.TaskState.EXECUTING, MergeCommitTask.TaskState.COMMITTING, "", false);
        assertTransition(transitions.get(4),
                MergeCommitTask.TaskState.COMMITTING, MergeCommitTask.TaskState.COMMITTED, "", false);
        assertTransition(transitions.get(5),
                MergeCommitTask.TaskState.COMMITTED, MergeCommitTask.TaskState.FINISHED, "", false);
    }

    @Test
    public void testStateMachineTransitionsOnExecuteFail() {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        List<StateTransition> transitions = Collections.synchronizedList(new ArrayList<>());
        task.setStateTransitionObserver((from, to, msg, hasHandler) ->
                transitions.add(new StateTransition(from, to, msg, hasHandler)));

        stubCoordinatorSuccess();
        when(coordinator.getExecStatus()).thenReturn(new Status(TStatusCode.INTERNAL_ERROR, "artificial failure"));

        task.run();

        assertTrue(transitions.size() >= 4, "unexpected transition count: " + transitions);
        StateTransition last = transitions.get(transitions.size() - 1);
        assertEquals(MergeCommitTask.TaskState.ABORTED, last.toState);
        assertTrue(last.toStateMsg.contains("Load execution failed"), "msg=" + last.toStateMsg);
    }

    @Test
    public void testStateMachineTransitionsOnCancelDuringLoading() throws Exception {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        List<StateTransition> transitions = Collections.synchronizedList(new ArrayList<>());
        task.setStateTransitionObserver((from, to, msg, hasHandler) ->
                transitions.add(new StateTransition(from, to, msg, hasHandler)));

        stubCoordinatorCommon();
        CountDownLatch joinEntered = new CountDownLatch(1);
        CountDownLatch joinRelease = new CountDownLatch(1);
        doAnswer(invocation -> {
            joinEntered.countDown();
            joinRelease.await(3, TimeUnit.SECONDS);
            return true;
        }).when(coordinator).join(anyInt());
        when(coordinator.getExecStatus()).thenReturn(new Status());
        when(coordinator.getCommitInfos()).thenReturn(buildCommitInfos());

        Thread t = new Thread(task::run);
        t.start();

        assertTrue(joinEntered.await(30, TimeUnit.SECONDS));
        task.cancel("unit-test cancel");
        joinRelease.countDown();
        t.join(30_000);

        assertTrue(transitions.size() >= 3, "unexpected transition count: " + transitions);
        boolean hasCancelled = transitions.stream().anyMatch(
                tr -> tr.toState == MergeCommitTask.TaskState.CANCELLED && "unit-test cancel".equals(tr.toStateMsg));
        assertTrue(hasCancelled, "expected CANCELLED transition, got: " + transitions);
    }

    @Test
    public void testStateMachineTransitionsOnPublishTimeout() throws Exception {
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                label,
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        MergeCommitTask taskSpy = Mockito.spy(task);
        doReturn(false).when(taskSpy).awaitPublish(any(), anyLong());

        List<StateTransition> transitions = Collections.synchronizedList(new ArrayList<>());
        taskSpy.setStateTransitionObserver((from, to, msg, hasHandler) ->
                transitions.add(new StateTransition(from, to, msg, hasHandler)));

        stubCoordinatorSuccess();

        taskSpy.run();

        StateTransition last = transitions.get(transitions.size() - 1);
        assertEquals(MergeCommitTask.TaskState.FINISHED, last.toState);
        assertTrue(last.toStateMsg.startsWith("publish failed:"), "msg=" + last.toStateMsg);
        assertTrue(last.toStateMsg.contains("publish timed out"), "msg=" + last.toStateMsg);
    }

    private static void assertTransition(StateTransition tr, MergeCommitTask.TaskState from,
            MergeCommitTask.TaskState to, String msg, boolean hasCancelHandler) {
        assertEquals(from, tr.fromState);
        assertEquals(to, tr.toState);
        assertEquals(msg, tr.toStateMsg);
        assertEquals(hasCancelHandler, tr.hasCancelHandler);
    }

    private static final class StateTransition {
        private final MergeCommitTask.TaskState fromState;
        private final MergeCommitTask.TaskState toState;
        private final String toStateMsg;
        private final boolean hasCancelHandler;

        private StateTransition(MergeCommitTask.TaskState fromState, MergeCommitTask.TaskState toState, String toStateMsg,
                boolean hasCancelHandler) {
            this.fromState = fromState;
            this.toState = toState;
            this.toStateMsg = toStateMsg;
            this.hasCancelHandler = hasCancelHandler;
        }

        @Override
        public String toString() {
            return "StateTransition{" +
                    "fromState=" + fromState +
                    ", toState=" + toState +
                    ", toStateMsg='" + toStateMsg + '\'' +
                    ", hasCancelHandler=" + hasCancelHandler +
                    '}';
        }
    }

    private void stubCoordinatorCommon() {
        when(coordinatorFactory.createStreamLoadScheduler(any(LoadPlanner.class))).thenReturn(coordinator);
        when(coordinator.getFailInfos()).thenReturn(Collections.emptyList());
        when(coordinator.getRejectedRecordPaths()).thenReturn(Collections.emptyList());
        when(coordinator.getLoadCounters()).thenReturn(Collections.emptyMap());
        when(coordinator.getTrackingUrl()).thenReturn(null);
    }

    private void stubCoordinatorSuccess() {
        stubCoordinatorCommon();
        when(coordinator.join(anyInt())).thenReturn(true);
        when(coordinator.getExecStatus()).thenReturn(new Status());
        when(coordinator.getCommitInfos()).thenReturn(buildCommitInfos());
    }

    private static class TestMergeCommitTaskCallback implements MergeCommitTaskCallback {

        private final List<String> finishedLoads = new ArrayList<>();

        public List<String> getFinishedLoads() {
            return finishedLoads;
        }

        @Override
        public void finish(MergeCommitTask mergeCommitTask) {
            finishedLoads.add(mergeCommitTask.getLabel());
        }
    }

    private static MergeCommitTask.TaskState getTaskState(MergeCommitTask task) {
        return (MergeCommitTask.TaskState) getPrivateField(task, "taskState");
    }

    private static String getTaskStateMessage(MergeCommitTask task) {
        return (String) getPrivateField(task, "taskStateMessage");
    }

    private static Object getPrivateField(Object obj, String fieldName) {
        try {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(obj);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read field '" + fieldName + "' from " + obj.getClass().getName(), e);
        }
    }
}
