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

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Pair;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadHttpHeader;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnStateCallbackFactory;
import mockit.Expectations;
import mockit.Mocked;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeCommitJobTest extends BatchWriteTestBase {

    @Mocked
    private CoordinatorBackendAssigner assigner;
    private TestThreadPoolExecutor executor;
    private TxnStateDispatcher txnStateDispatcher;
    private int parallel;

    private MergeCommitJob load;

    @BeforeEach
    public void setup() throws Exception {
        executor = new TestThreadPoolExecutor();
        txnStateDispatcher = new TxnStateDispatcher(executor);
        parallel = 4;
        assertTrue(parallel < allNodes.size(), "Number nodes " + allNodes.size());
        Map<String, String> map = new HashMap<>();
        map.put(StreamLoadHttpHeader.HTTP_FORMAT, "json");
        map.put(StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE, "true");
        map.put(StreamLoadHttpHeader.HTTP_BATCH_WRITE_ASYNC, "false");
        StreamLoadKvParams params = new StreamLoadKvParams(map);
        StreamLoadInfo streamLoadInfo =
                StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), params);
        load = new MergeCommitJob(
                1,
                new TableId(DB_NAME_1, TABLE_NAME_1_1),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME,
                streamLoadInfo,
                1000,
                parallel,
                params,
                assigner,
                executor,
                txnStateDispatcher);
    }

    @Test
    public void testRequestBackendsSuccess() {
        List<ComputeNode> nodes = allNodes.subList(0, parallel);
        new Expectations() {
            {
                assigner.getBackends(1);
                result = Optional.of(nodes);
            }
        };
        RequestCoordinatorBackendResult requestResult = load.requestCoordinatorBackends();
        assertTrue(requestResult.isOk());
        assertSame(nodes, requestResult.getValue());
    }

    @Test
    public void testRequestBackendsWithEmptyResult() {
        new Expectations() {
            {
                assigner.getBackends(1);
                result = Optional.of(Collections.emptyList());
            }
        };
        RequestCoordinatorBackendResult requestResult = load.requestCoordinatorBackends();
        assertFalse(requestResult.isOk());
        assertEquals(TStatusCode.SERVICE_UNAVAILABLE, requestResult.getStatus().getStatus_code());
    }

    @Test
    public void testRequestBackendsWithException() {
        new Expectations() {
            {
                assigner.getBackends(1);
                result = new Exception("artificial failure");
            }
        };
        RequestCoordinatorBackendResult requestResult = load.requestCoordinatorBackends();
        assertFalse(requestResult.isOk());
        assertEquals(TStatusCode.INTERNAL_ERROR, requestResult.getStatus().getStatus_code());
    }

    @Test
    public void testRequestLoadFromCoordinatorBackend() throws Exception {
        List<ComputeNode> nodes = allNodes.subList(0, parallel);
        new Expectations() {
            {
                assigner.getBackends(1);
                result = Optional.of(nodes);
            }
        };
        RequestLoadResult result1 = load.requestLoad("root", nodes.get(0).getId(), nodes.get(0).getHost());
        assertTrue(result1.isOk());
        String label = result1.getValue();
        assertNotNull(label);
        assertEquals(1, load.numRunningLoads());
        MergeCommitTask mergeCommitTask = load.getTask(label);
        assertNotNull(mergeCommitTask);
        assertEquals(nodes.stream().map(ComputeNode::getId).collect(Collectors.toSet()),
                mergeCommitTask.getBackendIds());

        StreamLoadMgr streamLoadMgr = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
        AbstractStreamLoadTask streamLoadTask = streamLoadMgr.getTaskByLabel(label);
        assertSame(mergeCommitTask, streamLoadTask);

        List<AbstractStreamLoadTask> tasksByName = streamLoadMgr.getTaskByName(label);
        assertNotNull(tasksByName, "getTaskByName should return non-null list");
        assertEquals(1, tasksByName.size(), "Task list should not be empty");
        assertSame(mergeCommitTask, tasksByName.get(0));

        TxnStateCallbackFactory txnStateCallbackFactory =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory();
        assertNull(txnStateCallbackFactory.getCallback(mergeCommitTask.getId()));

        RequestLoadResult result2 = load.requestLoad("root", nodes.get(1).getId(), nodes.get(1).getHost());
        assertTrue(result2.isOk());
        assertEquals(label, result2.getValue());

        executor.manualRun(mergeCommitTask);

        assertEquals(TransactionStatus.VISIBLE, getTxnStatus(label));
        assertNull(load.getTask(label));
        assertEquals(0, load.numRunningLoads());
        assertEquals(mergeCommitTask.getBackendIds().size(), txnStateDispatcher.getNumSubmittedTasks());
    }

    @Test
    public void testRequestLoadFromNoneCoordinatorBackend() throws Exception {
        List<ComputeNode> nodes = allNodes.subList(0, parallel);
        new Expectations() {
            {
                assigner.getBackends(1);
                result = Optional.of(nodes);
            }
        };

        // Request from coordinator backend
        RequestLoadResult result1 = load.requestLoad("root", nodes.get(0).getId(), nodes.get(0).getHost());
        assertTrue(result1.isOk());
        String label1 = result1.getValue();
        assertNotNull(label1);
        assertEquals(1, load.numRunningLoads());
        MergeCommitTask mergeCommitTask1 = load.getTask(label1);
        assertNotNull(mergeCommitTask1);
        assertEquals(nodes.stream().map(ComputeNode::getId).collect(Collectors.toSet()),
                mergeCommitTask1.getBackendIds());

        RequestLoadResult result2 = load.requestLoad("root", allNodes.get(parallel).getId(), allNodes.get(parallel).getHost());
        assertTrue(result2.isOk());
        String label2 = result2.getValue();
        assertNotNull(label2);
        assertEquals(2, load.numRunningLoads());
        assertNotEquals(label1, label2);
        MergeCommitTask mergeCommitTask2 = load.getTask(label2);
        assertNotNull(mergeCommitTask2);
        assertNotSame(mergeCommitTask1, mergeCommitTask2);
        Set<Long> expectNodeIds = nodes.stream().map(ComputeNode::getId).collect(Collectors.toSet());
        expectNodeIds.add(allNodes.get(parallel).getId());
        assertEquals(expectNodeIds, mergeCommitTask2.getBackendIds());

        executor.manualRun(mergeCommitTask1);
        assertEquals(TransactionStatus.VISIBLE, getTxnStatus(label1));
        assertEquals(mergeCommitTask1.getBackendIds().size(), txnStateDispatcher.getNumSubmittedTasks());

        executor.manualRun(mergeCommitTask2);
        assertEquals(TransactionStatus.VISIBLE, getTxnStatus(label2));
        assertEquals(mergeCommitTask1.getBackendIds().size() + mergeCommitTask2.getBackendIds().size(),
                txnStateDispatcher.getNumSubmittedTasks());

        assertEquals(0, load.numRunningLoads());
    }

    @Test
    public void testRequestLoadFromUnavailableBackend() {
        List<ComputeNode> nodes = allNodes.subList(0, parallel);
        new Expectations() {
            {
                assigner.getBackends(1);
                result = Optional.of(nodes);
            }
        };

        RequestLoadResult result = load.requestLoad("root", Integer.MAX_VALUE, "127.0.0.1");
        assertFalse(result.isOk());
        assertEquals(TStatusCode.SERVICE_UNAVAILABLE, result.getStatus().getStatus_code());
    }

    @Test
    public void testExecuteLoadFail() {
        List<ComputeNode> nodes = allNodes.subList(0, parallel);
        new Expectations() {
            {
                assigner.getBackends(1);
                result = Optional.of(nodes);
            }
        };

        executor.setThrowException(true);

        Map<String, MergeCommitTask> allTasksBefore = getTasksFromStreamLoadMgr(DB_NAME_1, TABLE_NAME_1_1);
        RequestLoadResult result = load.requestLoad("test_user", nodes.get(0).getId(), nodes.get(0).getHost());
        assertFalse(result.isOk(), "requestLoad should fail");
        assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatus_code(),
                "Status code should be INTERNAL_ERROR");
        assertNotNull(result.getStatus().getError_msgs(), "Error messages should not be null");
        assertFalse(result.getStatus().getError_msgs().isEmpty(), "Error messages should not be empty");
        String errorMsg = result.getStatus().getError_msgs().get(0);
        assertNotNull(errorMsg, "Error message should not be null");
        assertTrue(errorMsg.contains("artificial failure"), "Error message should contain exception info: " + errorMsg);
        assertEquals(0, load.numRunningLoads(), "Task should be removed from mergeCommitTasks");

        Map<String, MergeCommitTask> allTasksAfter = getTasksFromStreamLoadMgr(DB_NAME_1, TABLE_NAME_1_1);
        for (MergeCommitTask task : allTasksBefore.values()) {
            assertSame(task, allTasksAfter.remove(task.getLabel()));
        }
        assertEquals(1, allTasksAfter.size());
        MergeCommitTask failedTask = allTasksAfter.values().iterator().next();
        assertNotNull(failedTask);
        Pair<MergeCommitTask.TaskState, String> taskState = failedTask.getTaskState();
        assertEquals(MergeCommitTask.TaskState.CANCELLED, taskState.first);
        assertTrue(taskState.second.contains("artificial failure"));
    }

    @Test
    public void testDatabaseDoesNotExist() throws Exception {
        List<ComputeNode> nodes = allNodes.subList(0, parallel);
        new Expectations() {
            {
                assigner.getBackends(1);
                result = Optional.of(nodes);
            }
        };

        // Create a MergeCommitJob with a non-existent database name
        Map<String, String> map = new HashMap<>();
        map.put(StreamLoadHttpHeader.HTTP_FORMAT, "json");
        map.put(StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE, "true");
        map.put(StreamLoadHttpHeader.HTTP_BATCH_WRITE_ASYNC, "false");
        StreamLoadKvParams params = new StreamLoadKvParams(map);
        StreamLoadInfo streamLoadInfo =
                StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), params);

        MergeCommitJob mergeCommitJob = new MergeCommitJob(
                1,
                new TableId("non_existent_db", TABLE_NAME_1_1),
                WarehouseManager.DEFAULT_WAREHOUSE_NAME,
                streamLoadInfo,
                1000,
                parallel,
                params,
                assigner,
                executor,
                txnStateDispatcher);
        RequestLoadResult result = mergeCommitJob.requestLoad("root", nodes.get(0).getId(), nodes.get(0).getHost());
        assertFalse(result.isOk());
        assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatus_code());
        assertTrue(result.getStatus().getError_msgs().get(0).contains("Database 'non_existent_db' does not exist"));
    }

    @Test
    public void testStreamLoadMgrCleanTask() throws Exception {
        List<ComputeNode> nodes = allNodes.subList(0, parallel);
        new Expectations() {
            {
                assigner.getBackends(1);
                result = Optional.of(nodes);
            }
        };

        // Step 1: Create and complete a MergeCommitTask
        RequestLoadResult result = load.requestLoad("root", nodes.get(0).getId(), nodes.get(0).getHost());
        assertTrue(result.isOk());
        String label = result.getValue();
        assertNotNull(label);
        assertEquals(1, load.numRunningLoads());

        MergeCommitTask mergeCommitTask = load.getTask(label);
        assertNotNull(mergeCommitTask);

        // Execute the task to completion
        executor.manualRun(mergeCommitTask);

        // Verify task is in final state and has endTime set
        assertTrue(mergeCommitTask.isFinalState(), "Task should be in final state after completion");
        assertTrue(mergeCommitTask.endTimeMs() > 0, "Task should have endTime set after completion");
        assertEquals(TransactionStatus.VISIBLE, getTxnStatus(label));

        // Step 2: Verify task exists in StreamLoadMgr
        StreamLoadMgr streamLoadMgr = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
        AbstractStreamLoadTask taskByLabel = streamLoadMgr.getTaskByLabel(label);
        assertNotNull(taskByLabel, "Task should exist in StreamLoadMgr before cleanup");
        assertSame(mergeCommitTask, taskByLabel);

        List<AbstractStreamLoadTask> allTasksBefore = streamLoadMgr.getAllTasks();
        assertTrue(allTasksBefore.contains(mergeCommitTask),
                "Task should be in getAllTasks() before cleanup");

        // Step 3: Call cleanOldStreamLoadTasks with force=true
        streamLoadMgr.cleanOldStreamLoadTasks(true);

        // Step 4: Verify task has been cleaned
        AbstractStreamLoadTask taskByLabelAfter = streamLoadMgr.getTaskByLabel(label);
        assertNull(taskByLabelAfter, "Task should be removed from StreamLoadMgr after cleanup");
    }

    private Map<String, MergeCommitTask> getTasksFromStreamLoadMgr(String db, String table) {
        return GlobalStateMgr.getCurrentState().getStreamLoadMgr().getAllTasks().stream()
                .filter(task -> task.getDBName().equals(db) && task.getTableName().equals(table))
                .map(task -> (MergeCommitTask) task)
                .collect(Collectors.toMap(AbstractStreamLoadTask::getLabel, Function.identity()));
    }

    private class TestThreadPoolExecutor implements Executor {

        private final Set<Runnable> pendingRunnable;
        private boolean throwException;

        public TestThreadPoolExecutor() {
            this.pendingRunnable = new HashSet<>();
            this.throwException = false;
        }

        public boolean isThrowException() {
            return throwException;
        }

        public void setThrowException(boolean throwException) {
            this.throwException = throwException;
        }

        @Override
        public void execute(@NotNull Runnable command) {
            if (throwException) {
                throw new RejectedExecutionException("artificial failure");
            }

            pendingRunnable.add(command);
        }

        public void manualRun(Runnable runnable) throws Exception {
            boolean exist = pendingRunnable.remove(runnable);
            if (!exist) {
                return;
            }

            if (!(runnable instanceof MergeCommitTask mergeCommitTask)) {
                runnable.run();
                return;
            }

            Thread thread = new Thread(mergeCommitTask);
            thread.start();
            
            long endTime = System.currentTimeMillis() + 120000;
            do {
                if (System.currentTimeMillis() > endTime) {
                    throw new Exception("Load executor execute plan timeout");
                }
                Thread.sleep(10);
            } while (!mergeCommitTask.isPlanDeployed());

            DefaultCoordinator coordinator =
                    (DefaultCoordinator) QeProcessorImpl.INSTANCE.getCoordinator(mergeCommitTask.getLoadId());
            assertNotNull(coordinator, "Coordinator should be registered");
            coordinator.getExecutionDAG().getExecutions().forEach(execution -> {
                int indexInJob = execution.getIndexInJob();
                TReportExecStatusParams request = new TReportExecStatusParams(FrontendServiceVersion.V1);
                request.setQuery_id(mergeCommitTask.getLoadId());
                request.setBackend_num(indexInJob)
                        .setDone(true)
                        .setStatus(new TStatus(TStatusCode.OK))
                        .setFragment_instance_id(execution.getInstanceId());
                request.setCommitInfos(buildCommitInfos());
                TTabletFailInfo failInfo = new TTabletFailInfo();
                request.setFailInfos(Collections.singletonList(failInfo));
                Map<String, String> currLoadCounters = ImmutableMap.of(
                        LoadEtlTask.DPP_NORMAL_ALL, String.valueOf(10),
                        LoadEtlTask.DPP_ABNORMAL_ALL, String.valueOf(0),
                        LoadJob.UNSELECTED_ROWS, String.valueOf(0),
                        LoadJob.LOADED_BYTES, String.valueOf(40)
                );
                request.setLoad_counters(currLoadCounters);
                TNetworkAddress beAddr = execution.getAddress();
                QeProcessorImpl.INSTANCE.reportExecStatus(request, beAddr);
            });
            thread.join();
        }
    }
}
