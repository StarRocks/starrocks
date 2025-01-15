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
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.streamload.StreamLoadHttpHeader;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.transaction.TransactionStatus;
import mockit.Expectations;
import mockit.Mocked;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class IsomorphicBatchWriteTest extends BatchWriteTestBase {

    @Mocked
    private CoordinatorBackendAssigner assigner;
    private TestThreadPoolExecutor executor;
    private TxnStateDispatcher txnStateDispatcher;
    private int parallel;

    private IsomorphicBatchWrite load;

    @Before
    public void setup() throws Exception {
        executor = new TestThreadPoolExecutor();
        txnStateDispatcher = new TxnStateDispatcher(executor);
        parallel = 4;
        assertTrue("Number nodes " + allNodes.size(), parallel < allNodes.size());
        Map<String, String> map = new HashMap<>();
        map.put(StreamLoadHttpHeader.HTTP_FORMAT, "json");
        map.put(StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE, "true");
        map.put(StreamLoadHttpHeader.HTTP_BATCH_WRITE_ASYNC, "false");
        StreamLoadKvParams params = new StreamLoadKvParams(map);
        StreamLoadInfo streamLoadInfo =
                StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), params);
        load = new IsomorphicBatchWrite(
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
        RequestLoadResult result1 = load.requestLoad(nodes.get(0).getId(), nodes.get(0).getHost());
        assertTrue(result1.isOk());
        String label = result1.getValue();
        assertNotNull(label);
        assertEquals(1, load.numRunningLoads());
        LoadExecutor loadExecutor = load.getLoadExecutor(label);
        assertNotNull(loadExecutor);
        assertEquals(nodes.stream().map(ComputeNode::getId).collect(Collectors.toSet()),
                loadExecutor.getCoordinatorBackendIds());

        RequestLoadResult result2 = load.requestLoad(nodes.get(1).getId(), nodes.get(1).getHost());
        assertTrue(result2.isOk());
        assertEquals(label, result2.getValue());

        executor.manualRun(loadExecutor);

        assertEquals(TransactionStatus.VISIBLE, getTxnStatus(label));
        assertNull(load.getLoadExecutor(label));
        assertEquals(0, load.numRunningLoads());
        assertEquals(loadExecutor.getBackendIds().size(), txnStateDispatcher.getNumSubmittedTasks());
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
        RequestLoadResult result1 = load.requestLoad(nodes.get(0).getId(), nodes.get(0).getHost());
        assertTrue(result1.isOk());
        String label1 = result1.getValue();
        assertNotNull(label1);
        assertEquals(1, load.numRunningLoads());
        LoadExecutor loadExecutor1 = load.getLoadExecutor(label1);
        assertNotNull(loadExecutor1);
        assertEquals(nodes.stream().map(ComputeNode::getId).collect(Collectors.toSet()),
                loadExecutor1.getCoordinatorBackendIds());

        RequestLoadResult result2 = load.requestLoad(allNodes.get(parallel).getId(), allNodes.get(parallel).getHost());
        assertTrue(result2.isOk());
        String label2 = result2.getValue();
        assertNotNull(label2);
        assertEquals(2, load.numRunningLoads());
        assertNotEquals(label1, label2);
        LoadExecutor loadExecutor2 = load.getLoadExecutor(label2);
        assertNotNull(loadExecutor2);
        assertNotSame(loadExecutor1, loadExecutor2);
        Set<Long> expectNodeIds = nodes.stream().map(ComputeNode::getId).collect(Collectors.toSet());
        expectNodeIds.add(allNodes.get(parallel).getId());
        assertEquals(expectNodeIds, loadExecutor2.getCoordinatorBackendIds());

        executor.manualRun(loadExecutor1);
        assertEquals(TransactionStatus.VISIBLE, getTxnStatus(label1));
        assertEquals(loadExecutor1.getCoordinatorBackendIds().size(), txnStateDispatcher.getNumSubmittedTasks());

        executor.manualRun(loadExecutor2);
        assertEquals(TransactionStatus.VISIBLE, getTxnStatus(label2));
        assertEquals(loadExecutor1.getBackendIds().size() + loadExecutor2.getBackendIds().size(),
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

        RequestLoadResult result = load.requestLoad(Integer.MAX_VALUE, "127.0.0.1");
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
        RequestLoadResult result = load.requestLoad(nodes.get(0).getId(), nodes.get(0).getHost());
        assertFalse(result.isOk());
        assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatus_code());
        assertEquals(0, load.numRunningLoads());
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

            if (!(runnable instanceof LoadExecutor)) {
                runnable.run();
                return;
            }

            LoadExecutor loadExecutor = (LoadExecutor) runnable;
            Thread thread = new Thread(loadExecutor);
            thread.start();
            long endTime = System.currentTimeMillis() + 120000;
            while (loadExecutor.getTimeTrace().joinPlanTimeMs.get() <= 0) {
                if (System.currentTimeMillis() > endTime) {
                    throw new Exception("Load executor execute plan timeout");
                }
                Thread.sleep(10);
            }
            DefaultCoordinator coordinator = (DefaultCoordinator) loadExecutor.getCoordinator();
            assertNotNull(coordinator);
            coordinator.getExecutionDAG().getExecutions().forEach(execution -> {
                int indexInJob = execution.getIndexInJob();
                TReportExecStatusParams request = new TReportExecStatusParams(FrontendServiceVersion.V1);
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
                coordinator.updateFragmentExecStatus(request);
            });
            thread.join();
        }
    }
}
