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
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.streamload.StreamLoadHttpHeader;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnStateCallbackFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
                "root",
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
                "root",
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
                "root",
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
                "root",
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
                "root",
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
                "root",
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
                "root",
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
                "root",
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
                "root",
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
                "root",
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);

        stubCoordinatorSuccess();

        task.run();
        assertEquals(MergeCommitTask.TaskState.FINISHED, task.getTaskState().first);
        String stateMsgBeforeCancel = task.getTaskState().second;

        // Cancel after task finished should not change task state or state message.
        task.cancel("cancel after finish");
        assertEquals(MergeCommitTask.TaskState.FINISHED, task.getTaskState().first);
        assertEquals(stateMsgBeforeCancel, task.getTaskState().second);
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
                "root",
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
                    "root",
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
                "root",
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
                "root",
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
                "root",
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
                "root",
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
                "root",
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

    @Test
    public void testCheckNeedRemove() {
        // Save original config value
        int originalKeepMaxSecond = Config.stream_load_task_keep_max_second;
        long currentTimeMs = System.currentTimeMillis();

        try {
            // Non-final state task should return false
            MergeCommitTask pendingTask = createTaskWithState(MergeCommitTask.TaskState.PENDING);
            assertFalse(pendingTask.checkNeedRemove(currentTimeMs, false));
            assertFalse(pendingTask.checkNeedRemove(currentTimeMs, true));

            // Test final state
            for (MergeCommitTask.TaskState state : Arrays.asList(
                    MergeCommitTask.TaskState.FINISHED,
                    MergeCommitTask.TaskState.ABORTED,
                    MergeCommitTask.TaskState.CANCELLED)) {
                // Final state but time within threshold should return false
                // Set a large threshold (e.g., 1 day) so current time is within threshold
                Config.stream_load_task_keep_max_second = 24 * 3600; // 1 day
                MergeCommitTask task = createTaskWithState(state);
                long endTimeMs = task.endTimeMs();
                assertTrue(endTimeMs > 0, "Final state task should have endTime set");
                assertFalse(task.checkNeedRemove(endTimeMs, false));
                assertFalse(task.checkNeedRemove(endTimeMs + 1000, false));

                // Final state and time exceeds threshold should return true
                // Set a very small threshold (1 second) so current time exceeds threshold
                Config.stream_load_task_keep_max_second = 1; // 1 second
                // Use a time that is 2 seconds after endTime to exceed the 1-second threshold
                long testTimeMs = endTimeMs + 2000L;
                assertTrue(task.checkNeedRemove(testTimeMs, false));

                // Force remove (isForce=true) should return true even if time within threshold
                Config.stream_load_task_keep_max_second = 24 * 3600; // Reset to large threshold
                assertTrue(task.checkNeedRemove(endTimeMs, true));
                assertTrue(task.checkNeedRemove(endTimeMs + 1000, true));
            }
        } finally {
            // Restore original config value
            Config.stream_load_task_keep_max_second = originalKeepMaxSecond;
        }
    }

    @Test
    public void testObservability() {
        for (MergeCommitTask.TaskState state : Arrays.asList(
                MergeCommitTask.TaskState.FINISHED,
                MergeCommitTask.TaskState.ABORTED,
                MergeCommitTask.TaskState.CANCELLED)) {
            MergeCommitTask task = createTaskWithState(state);
            verifyGetter(task);
            verifyToThrift(task);
            verifyGetShowInfo(task);
            verifyGetShowBriefInfo(task);
            verifyToStreamLoadThrift(task);
        }
    }

    private MergeCommitTask createTaskWithState(MergeCommitTask.TaskState targetState) {
        String taskLabel = "test_pending_" + DebugUtil.printId(UUIDUtil.genTUniqueId());
        TUniqueId taskLoadId = UUIDUtil.genTUniqueId();
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                taskLabel,
                taskLoadId,
                streamLoadInfo,
                1000,
                kvParams,
                "test_user",
                warehouseName,
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                loadExecuteCallback);
        if (targetState == MergeCommitTask.TaskState.FINISHED) {
            stubCoordinatorSuccess();
            Map<String, String> counters = new HashMap<>();
            counters.put(LoadEtlTask.DPP_NORMAL_ALL, "100");
            counters.put(LoadJob.LOADED_BYTES, "1000");
            counters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "0");
            when(coordinator.getLoadCounters()).thenReturn(counters);
            task.run();
            assertEquals(MergeCommitTask.TaskState.FINISHED, task.getTaskState().first);
            return task;
        } else if (targetState == MergeCommitTask.TaskState.ABORTED) {
            // Simulate data quality issue causing ABORTED
            stubCoordinatorSuccess();
            Map<String, String> counters = new HashMap<>();
            counters.put(LoadEtlTask.DPP_NORMAL_ALL, "100");
            counters.put(LoadJob.LOADED_BYTES, "1000");
            counters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "100");
            when(coordinator.getLoadCounters()).thenReturn(counters);
            when(coordinator.getTrackingUrl()).thenReturn("http://test_tracking_url");
            when(coordinator.getRejectedRecordPaths()).thenReturn(Arrays.asList("/path/to/rejected_records.csv"));
            task.run();
            assertEquals(MergeCommitTask.TaskState.ABORTED, task.getTaskState().first);
        } else if (targetState == MergeCommitTask.TaskState.CANCELLED) {
            task.cancel("test cancellation");
            assertEquals(MergeCommitTask.TaskState.CANCELLED, task.getTaskState().first);
        }
        assertEquals(taskLabel, task.getLabel());
        assertEquals(taskLoadId, task.getLoadId());
        return task;
    }

    private void verifyGetter(MergeCommitTask task) {
        assertEquals(DATABASE_1.getId(), task.getDBId());
        assertEquals(DB_NAME_1, task.getDBName());
        assertEquals(TABLE_NAME_1_1, task.getTableName());
        MergeCommitTask.TaskState taskState = task.getTaskState().first;
        assertEquals(convertTaskStateToLoadState(taskState), task.getStateName());
        assertEquals(taskState.isFinalState(), task.isFinalState());
        assertTrue(task.createTimeMs() > 0);
        assertEquals(taskState.isFinalState(), task.endTimeMs() > 0);
        assertEquals("MERGE_COMMIT", task.getStringByType());
        if (taskState == MergeCommitTask.TaskState.ABORTED || taskState == MergeCommitTask.TaskState.FINISHED) {
            assertTrue(task.getTxnId() > 0);
        } else {
            assertEquals(-1, task.getTxnId());
        }
    }

    private void verifyToThrift(MergeCommitTask task) {
        List<TLoadInfo> loadInfos = task.toThrift();
        assertNotNull(loadInfos);
        assertEquals(1, loadInfos.size());
        TLoadInfo info = loadInfos.get(0);
        
        MergeCommitTask.TaskState taskState = task.getTaskState().first;
        
        // Verify basic fields (all states)
        assertEquals(task.getId(), info.getJob_id());
        assertEquals("MERGE_COMMIT", info.getType());
        assertEquals(DB_NAME_1, info.getDb());
        assertEquals(TABLE_NAME_1_1, info.getTable());
        assertEquals("test_user", info.getUser());
        assertEquals(task.getLabel(), info.getLabel());
        assertEquals(DebugUtil.printId(task.getLoadId()), info.getLoad_id());
        assertNotNull(info.getProperties());
        assertEquals("NORMAL", info.getPriority());
        assertNotNull(info.getCreate_time());
        assertNotNull(info.getRuntime_details());
        assertNotNull(info.getProgress());
        assertTrue(info.getProgress().startsWith("Merge Window"));
        String expectedState = convertTaskStateToLoadState(taskState);
        assertEquals(expectedState, info.getState());
        assertNotNull(info.getWarehouse());
        
        // Verify conditional fields based on state
        if (taskState == MergeCommitTask.TaskState.PENDING) {
            assertEquals(-1, info.getTxn_id());
            assertNull(info.getError_msg());
            assertEquals(0, info.getNum_scan_rows());
            assertEquals(0, info.getNum_scan_bytes());
            assertEquals(0, info.getNum_sink_rows());
            assertEquals(0, info.getNum_filtered_rows());
            assertEquals(0, info.getNum_unselected_rows());
            assertNull(info.getUrl());
            assertNull(info.getTracking_sql());
            assertNull(info.getRejected_record_path());
            assertNotNull(info.getCreate_time());
        } else if (taskState == MergeCommitTask.TaskState.FINISHED) {
            assertTrue(info.getTxn_id() > 0);
            assertTrue(task.endTimeMs() > 0);
            assertNotNull(info.getLoad_start_time());
            assertFalse(info.getLoad_start_time().isEmpty());
            assertNotNull(info.getLoad_commit_time());
            assertFalse(info.getLoad_commit_time().isEmpty());
            assertNotNull(info.getLoad_finish_time());
            assertFalse(info.getLoad_finish_time().isEmpty());
            assertTrue(info.getNum_scan_rows() > 0);
            assertTrue(info.getNum_scan_bytes() > 0);
            assertTrue(info.getNum_sink_rows() > 0);
            assertEquals(0, info.getNum_filtered_rows());
            assertEquals(0, info.getNum_unselected_rows());

            assertNull(info.getUrl(), "FINISHED state should not have url when trackingUrl is null");
            assertNull(info.getTracking_sql(), "FINISHED state should not have tracking_sql when url is null");
            assertNull(info.getRejected_record_path(),
                    "FINISHED state should not have rejected_record_path when rejectedRecordPaths is empty");
        } else if (taskState == MergeCommitTask.TaskState.ABORTED) {
            // ABORTED state: has txn, has loadStats, has end time, has error msg
            assertTrue(info.getTxn_id() > 0);
            assertTrue(task.endTimeMs() > 0);
            assertNotNull(info.getLoad_finish_time());
            assertNotNull(info.getError_msg());
            assertFalse(info.getError_msg().isEmpty());
            
            assertNotNull(info.getUrl(), "ABORTED state should have url");
            assertFalse(info.getUrl().isEmpty(), "ABORTED state url should not be empty");
            assertNotNull(info.getTracking_sql(), "ABORTED state should have tracking_sql");
            assertFalse(info.getTracking_sql().isEmpty(), "ABORTED state tracking_sql should not be empty");
            assertNotNull(info.getRejected_record_path(), "ABORTED state should have rejected_record_path");
            assertFalse(info.getRejected_record_path().isEmpty(), 
                    "ABORTED state rejected_record_path should not be empty");
            
            assertTrue(info.getNum_scan_rows() > 0);
            assertTrue(info.getNum_scan_bytes() > 0);
            assertTrue(info.getNum_sink_rows() > 0);
            assertTrue(info.getNum_filtered_rows() > 0);
            assertEquals(0, info.getNum_unselected_rows());
        } else if (taskState == MergeCommitTask.TaskState.CANCELLED) {
            assertEquals(-1, info.getTxn_id());
            assertTrue(task.endTimeMs() > 0);
            assertNotNull(info.getLoad_finish_time());
            assertNotNull(info.getError_msg());
            assertEquals("test cancellation", info.getError_msg());
            assertEquals(0, info.getNum_scan_rows());
            assertEquals(0, info.getNum_scan_bytes());
            assertEquals(0, info.getNum_sink_rows());
            assertEquals(0, info.getNum_filtered_rows());
            assertEquals(0, info.getNum_unselected_rows());
            assertNull(info.getUrl());
            assertNull(info.getTracking_sql());
            assertNull(info.getRejected_record_path());
        }
    }

    private void verifyGetShowInfo(MergeCommitTask task) {
        List<List<String>> loadInfos = task.getShowInfo();
        assertNotNull(loadInfos);
        assertEquals(1, loadInfos.size());
        List<String> info = loadInfos.get(0);
        assertEquals(25, info.size());
        
        MergeCommitTask.TaskState taskState = task.getTaskState().first;
        
        // Verify common fields (all states)
        assertEquals(task.getLabel(), info.get(0));
        assertEquals(String.valueOf(task.getId()), info.get(1));
        assertEquals(DebugUtil.printId(task.getLoadId()), info.get(2));
        assertEquals(DB_NAME_1, info.get(4));
        assertEquals(TABLE_NAME_1_1, info.get(5));
        assertEquals(String.valueOf(task.getBackendIds().size()), info.get(9));
        assertEquals(String.valueOf(streamLoadInfo.getTimeout()), info.get(15));
        assertEquals("", info.get(22));
        assertEquals("MERGE_COMMIT", info.get(23));
        
        // Verify state-specific fields
        if (taskState == MergeCommitTask.TaskState.PENDING) {
            // PENDING state: no txn, no stats, no endTime
            assertEquals("-1", info.get(3));
            assertEquals("PENDING", info.get(6));
            assertNull(info.get(7));
            assertEquals("", info.get(8));
            assertEquals("0", info.get(10));
            assertEquals("0", info.get(11));
            assertEquals("0", info.get(12));
            assertEquals("0", info.get(13));
            assertEquals("0", info.get(14));
            assertNotNull(info.get(16));
            // startTime may be -1 for PENDING, so it will be "\\N"
            String startTime = info.get(17);
            assertNotNull(startTime);
            // startPreparingTime (index 19) and finishPreparingTime (index 20) should be "\\N" for PENDING
            assertEquals("\\N", info.get(19));
            assertEquals("\\N", info.get(20));
            String endTime = info.get(21);
            assertEquals("\\N", endTime);
            assertEquals("", info.get(24));
        } else if (taskState == MergeCommitTask.TaskState.FINISHED) {
            // FINISHED state: has txn, has stats, has endTime
            assertTrue(Long.parseLong(info.get(3)) > 0);
            assertEquals("FINISHED", info.get(6));
            // taskStateMessage may be null or contain publish failure message
            String stateMsg = info.get(7);
            assertTrue(stateMsg == null || stateMsg.isEmpty() || stateMsg.startsWith("publish failed:"));
            assertEquals("", info.get(8));
            assertEquals("0", info.get(10));
            assertTrue(Long.parseLong(info.get(11)) > 0);
            assertEquals("0", info.get(12));
            assertEquals("0", info.get(13));
            assertTrue(Long.parseLong(info.get(14)) > 0);
            assertNotNull(info.get(16));
            assertNotNull(info.get(17));
            assertFalse(info.get(17).equals("\\N"));
            assertEquals(info.get(17), info.get(18));
            // startPreparingTime (index 19) and finishPreparingTime (index 20) should be valid for FINISHED
            assertNotNull(info.get(19));
            assertNotNull(info.get(20));
            assertFalse(info.get(20).equals("\\N"));
            assertNotNull(info.get(21));
            assertFalse(info.get(21).equals("\\N"));
            assertFalse(info.get(21).isEmpty());
            assertEquals("", info.get(24));
        } else if (taskState == MergeCommitTask.TaskState.ABORTED) {
            // ABORTED state: has txn, has stats, has trackingUrl, has endTime
            assertTrue(Long.parseLong(info.get(3)) > 0);
            assertEquals("CANCELLED", info.get(6));
            assertNotNull(info.get(7));
            assertFalse(info.get(7).isEmpty());
            assertNotNull(info.get(8));
            assertFalse(info.get(8).isEmpty());
            assertEquals("0", info.get(10));
            assertTrue(Long.parseLong(info.get(11)) > 0);
            assertTrue(Long.parseLong(info.get(12)) > 0);
            assertEquals("0", info.get(13));
            assertTrue(Long.parseLong(info.get(14)) > 0);
            assertNotNull(info.get(16));
            assertNotNull(info.get(17));
            assertNotEquals("\\N", info.get(17));
            // startPreparingTime (index 19) and finishPreparingTime (index 20) should be valid for ABORTED
            assertNotNull(info.get(19));
            assertNotNull(info.get(20));
            assertEquals("\\N", info.get(20));
            assertNotNull(info.get(21));
            assertNotEquals("\\N", info.get(21));
            assertFalse(info.get(21).isEmpty());
            assertNotNull(info.get(24));
            assertFalse(info.get(24).isEmpty());
            assertTrue(info.get(24).contains("SELECT tracking_log"));
        } else if (taskState == MergeCommitTask.TaskState.CANCELLED) {
            // CANCELLED state: no txn, no stats, has endTime, has error message
            assertEquals("-1", info.get(3));
            assertEquals("CANCELLED", info.get(6));
            assertNotNull(info.get(7));
            assertEquals("test cancellation", info.get(7));
            assertEquals("", info.get(8));
            assertEquals("0", info.get(10));
            assertEquals("0", info.get(11));
            assertEquals("0", info.get(12));
            assertEquals("0", info.get(13));
            assertEquals("0", info.get(14));
            assertNotNull(info.get(16));
            // startPreparingTime (index 19) and finishPreparingTime (index 20) should be "\\N" for CANCELLED (no commit)
            assertEquals("\\N", info.get(19));
            assertEquals("\\N", info.get(20));
            assertNotNull(info.get(21));
            assertFalse(info.get(21).equals("\\N"));
            assertFalse(info.get(21).isEmpty());
            assertEquals("", info.get(24));
        }
        
        // Verify time fields format (all states that have valid times)
        String createTime = info.get(16);
        assertNotNull(createTime);
        assertFalse(createTime.equals("\\N"));
        assertFalse(createTime.isEmpty());
    }

    private void verifyGetShowBriefInfo(MergeCommitTask task) {
        List<List<String>> loadInfos = task.getShowBriefInfo();
        assertNotNull(loadInfos);
        assertEquals(1, loadInfos.size());
        List<String> info = loadInfos.get(0);
        assertEquals(5, info.size());
        assertEquals(task.getLabel(), info.get(0));
        assertEquals(String.valueOf(task.getId()), info.get(1));
        assertEquals(DB_NAME_1, info.get(2));
        assertEquals(TABLE_NAME_1_1, info.get(3));
        assertEquals(convertTaskStateToLoadState(task.getTaskState().first), info.get(4));
    }

    private void verifyToStreamLoadThrift(MergeCommitTask task) {
        List<TStreamLoadInfo> loadInfos = task.toStreamLoadThrift();
        assertNotNull(loadInfos);
        assertEquals(1, loadInfos.size());
        TStreamLoadInfo info = loadInfos.get(0);
        
        MergeCommitTask.TaskState taskState = task.getTaskState().first;
        
        // Verify basic fields (all states)
        assertEquals(task.getLabel(), info.getLabel());
        assertEquals(task.getId(), info.getId());
        assertEquals(DebugUtil.printId(task.getLoadId()), info.getLoad_id());
        assertEquals(DB_NAME_1, info.getDb_name());
        assertEquals(TABLE_NAME_1_1, info.getTable_name());
        String expectedState = convertTaskStateToLoadState(taskState);
        assertEquals(expectedState, info.getState());
        assertEquals("MERGE_COMMIT", info.getType());
        assertEquals(2, info.getChannel_num()); // backendIds.size() = 2
        assertEquals(streamLoadInfo.getTimeout(), info.getTimeout_second());
        assertNotNull(info.getCreate_time_ms());
        assertNotNull(info.getBefore_load_time_ms());
        assertNotNull(info.getStart_loading_time_ms());
        assertEquals("", info.getChannel_state());
        
        // Verify state-dependent fields
        if (taskState == MergeCommitTask.TaskState.PENDING) {
            assertEquals(-1, info.getTxn_id());
            assertNull(info.getError_msg());
            assertEquals(0, info.getPrepared_channel_num());
            assertEquals(0, info.getNum_rows_normal());
            assertEquals(0, info.getNum_rows_ab_normal());
            assertEquals(0, info.getNum_rows_unselected());
            assertEquals(0, info.getNum_load_bytes());
            assertNull(info.getTracking_url());
            assertNull(info.getTracking_sql());
            // end_time_ms should be NULL_STRING ("\\N") for non-final state
            assertEquals("\\N", info.getEnd_time_ms());
        } else if (taskState == MergeCommitTask.TaskState.FINISHED) {
            assertTrue(info.getTxn_id() > 0);
            assertTrue(task.endTimeMs() > 0);
            assertNotNull(info.getEnd_time_ms());
            assertFalse(info.getEnd_time_ms().isEmpty());
            assertEquals(0, info.getPrepared_channel_num()); // Not COMMITTED state
            // FINISHED state has loadStats
            assertTrue(info.getNum_rows_normal() > 0);
            assertTrue(info.getNum_load_bytes() > 0);
            assertEquals(0, info.getNum_rows_ab_normal()); // No abnormal rows in success case
            assertEquals(0, info.getNum_rows_unselected());
            // No trackingUrl set in createTaskWithState for FINISHED
            assertNull(info.getTracking_url(), "FINISHED state should not have tracking_url when trackingUrl is null");
            assertNull(info.getTracking_sql(), "FINISHED state should not have tracking_sql when trackingUrl is null");
        } else if (taskState == MergeCommitTask.TaskState.ABORTED) {
            assertTrue(info.getTxn_id() > 0);
            assertTrue(task.endTimeMs() > 0);
            assertNotNull(info.getEnd_time_ms());
            assertFalse(info.getEnd_time_ms().isEmpty());
            assertNotNull(info.getError_msg());
            assertFalse(info.getError_msg().isEmpty());
            assertEquals(0, info.getPrepared_channel_num());
            // ABORTED state has loadStats
            assertTrue(info.getNum_rows_normal() > 0);
            assertTrue(info.getNum_rows_ab_normal() > 0);
            assertTrue(info.getNum_load_bytes() > 0);
            assertEquals(0, info.getNum_rows_unselected());
            // ABORTED state has trackingUrl set in createTaskWithState
            assertNotNull(info.getTracking_url(), "ABORTED state should have tracking_url");
            assertFalse(info.getTracking_url().isEmpty(), "ABORTED state tracking_url should not be empty");
            assertNotNull(info.getTracking_sql(), "ABORTED state should have tracking_sql");
            assertFalse(info.getTracking_sql().isEmpty(), "ABORTED state tracking_sql should not be empty");
        } else if (taskState == MergeCommitTask.TaskState.CANCELLED) {
            assertEquals(-1, info.getTxn_id());
            assertTrue(task.endTimeMs() > 0);
            assertNotNull(info.getEnd_time_ms());
            assertFalse(info.getEnd_time_ms().isEmpty());
            assertNotNull(info.getError_msg());
            assertEquals("test cancellation", info.getError_msg());
            assertEquals(0, info.getPrepared_channel_num());
            // CANCELLED state has no loadStats
            assertEquals(0, info.getNum_rows_normal());
            assertEquals(0, info.getNum_rows_ab_normal());
            assertEquals(0, info.getNum_rows_unselected());
            assertEquals(0, info.getNum_load_bytes());
            assertNull(info.getTracking_url());
            assertNull(info.getTracking_sql());
        }
        
        // Verify time fields
        assertNotNull(info.getStart_preparing_time_ms());
        assertNotNull(info.getFinish_preparing_time_ms());
        if (taskState == MergeCommitTask.TaskState.PENDING) {
            // For PENDING state, these should be NULL_STRING since commit hasn't happened
            assertEquals("\\N", info.getStart_preparing_time_ms(), 
                    "PENDING state should have NULL_STRING for start_preparing_time_ms");
            assertEquals("\\N", info.getFinish_preparing_time_ms(), 
                    "PENDING state should have NULL_STRING for finish_preparing_time_ms");
        } else {
            // For final states, these should have valid time strings
            assertFalse(info.getStart_preparing_time_ms().isEmpty());
            assertFalse(info.getFinish_preparing_time_ms().isEmpty());
        }
    }
    
    private String convertTaskStateToLoadState(MergeCommitTask.TaskState taskState) {
        return switch (taskState) {
            case PENDING, COMMITTED, FINISHED -> taskState.name();
            case CANCELLED, ABORTED -> "CANCELLED";
            default -> "LOADING";
        };
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
}
