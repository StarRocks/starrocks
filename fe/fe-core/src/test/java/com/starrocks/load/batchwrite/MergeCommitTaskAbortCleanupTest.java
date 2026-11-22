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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.lake.LakeTable;
import com.starrocks.load.streamload.StreamLoadHttpHeader;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.proto.AbortTxnRequest;
import com.starrocks.proto.AbortTxnResponse;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.MockedBackend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

// A merge-commit load that fails after it has written data aborts its transaction. On a shared-data cluster the
// abort has to name the tablets that were written: without them the compute nodes are told to skip the cleanup, and
// the segments of the transaction stay on the object storage. Autovacuum deletes only the txn logs, and the full
// vacuum, which would delete the segments, is off by default.
public class MergeCommitTaskAbortCleanupTest {

    private static final String DB_NAME = "merge_commit_abort_cleanup_db";
    private static final String TABLE_NAME = "merge_commit_abort_cleanup_tbl";

    // The abort requests the compute nodes received, by transaction id.
    private static final Map<Long, List<AbortTxnRequest>> ABORT_REQUESTS = new ConcurrentHashMap<>();

    private static Database database;
    private static LakeTable table;
    // The only node of the cluster, which writes every tablet.
    private static long nodeId;

    private StreamLoadKvParams kvParams;
    private StreamLoadInfo streamLoadInfo;
    private Coordinator.Factory coordinatorFactory;
    private Coordinator coordinator;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        new StarRocksAssert(connectContext).withDatabase(DB_NAME).useDatabase(DB_NAME)
                .withTable("CREATE TABLE " + TABLE_NAME + " (k BIGINT NOT NULL, v STRING) PRIMARY KEY (k) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 3");
        database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        table = (LakeTable) database.getTable(TABLE_NAME);
        nodeId = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds().get(0);

        new MockUp<LoadPlanner>() {
            @Mock
            public void plan() {
                // The coordinator is mocked, so there is no plan to run.
            }
        };
        new MockUp<MockedBackend.MockLakeService>() {
            @Mock
            public Future<AbortTxnResponse> abortTxn(Invocation invocation, AbortTxnRequest request) {
                if (request.txnInfos != null) {
                    for (TxnInfoPB txnInfo : request.txnInfos) {
                        ABORT_REQUESTS.computeIfAbsent(txnInfo.txnId, id -> new CopyOnWriteArrayList<>()).add(request);
                    }
                }
                return invocation.proceed();
            }
        };
    }

    @BeforeEach
    public void setup() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(StreamLoadHttpHeader.HTTP_FORMAT, "json");
        map.put(StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE, "true");
        map.put(StreamLoadHttpHeader.HTTP_BATCH_WRITE_ASYNC, "true");
        kvParams = new StreamLoadKvParams(map);
        streamLoadInfo = StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), kvParams,
                CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_NAME));
        coordinatorFactory = Mockito.mock(Coordinator.Factory.class);
        coordinator = Mockito.mock(Coordinator.class);
    }

    @Test
    public void testDataQualityFailureCleansUpWrittenTablets() throws Exception {
        List<Long> writtenTablets = allTabletIds();
        stubCoordinator(new Status(), writtenTablets);
        when(coordinator.getLoadCounters()).thenReturn(
                Map.of(LoadEtlTask.DPP_NORMAL_ALL, "100", LoadEtlTask.DPP_ABNORMAL_ALL, "100"));

        MergeCommitTask task = runTask();
        assertAbortCleansUp(task, "data quality", writtenTablets);
    }

    @Test
    public void testExecutionFailureCleansUpReportedTablets() throws Exception {
        // The tablets written by the instances that finished before another one failed.
        List<Long> writtenTablets = allTabletIds().subList(0, 1);
        stubCoordinator(new Status(TStatusCode.INTERNAL_ERROR, "artificial failure"), writtenTablets);

        MergeCommitTask task = runTask();
        assertAbortCleansUp(task, "Load execution failed", writtenTablets);
    }

    @Test
    public void testCommitFailureCleansUpWrittenTablets() throws Exception {
        // Only one tablet is reported, so the commit fails on the tablets that have no report.
        List<Long> writtenTablets = allTabletIds().subList(0, 1);
        stubCoordinator(new Status(), writtenTablets);

        MergeCommitTask task = runTask();
        assertAbortCleansUp(task, "fail to execute commit task", writtenTablets);
    }

    private static List<Long> allTabletIds() {
        List<Long> tabletIds = new ArrayList<>();
        for (Partition partition : table.getPartitions()) {
            tabletIds.addAll(partition.getDefaultPhysicalPartition().getLatestBaseIndex().getTabletIdsInOrder());
        }
        assertEquals(3, tabletIds.size());
        return tabletIds;
    }

    private void stubCoordinator(Status execStatus, List<Long> writtenTablets) throws Exception {
        List<TTabletCommitInfo> commitInfos = new ArrayList<>();
        for (long tabletId : writtenTablets) {
            commitInfos.add(new TTabletCommitInfo(tabletId, nodeId));
        }
        when(coordinatorFactory.createStreamLoadScheduler(any(LoadPlanner.class))).thenReturn(coordinator);
        when(coordinator.join(anyInt())).thenReturn(true);
        when(coordinator.getExecStatus()).thenReturn(execStatus);
        when(coordinator.getCommitInfos()).thenReturn(commitInfos);
        when(coordinator.getFailInfos()).thenReturn(List.of());
        when(coordinator.getLoadCounters()).thenReturn(Map.of());
        when(coordinator.getRejectedRecordPaths()).thenReturn(List.of());
    }

    private MergeCommitTask runTask() {
        TUniqueId loadId = UUIDUtil.genTUniqueId();
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                database.getId(),
                new TableId(DB_NAME, TABLE_NAME),
                "merge_commit_" + DebugUtil.printId(loadId),
                loadId,
                streamLoadInfo,
                1000,
                kvParams,
                "root",
                WarehouseManager.DEFAULT_WAREHOUSE_NAME,
                Set.of(nodeId),
                coordinatorFactory,
                mergeCommitTask -> {});
        task.run();
        return task;
    }

    private static void assertAbortCleansUp(MergeCommitTask task, String abortReason, List<Long> writtenTablets) {
        assertEquals(MergeCommitTask.TaskState.ABORTED, task.getTaskState().first);
        assertTrue(task.getTaskState().second.contains(abortReason), task.getTaskState().second);
        assertEquals(TransactionStatus.ABORTED, GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getLabelStatus(database.getId(), task.getLabel()).getStatus());

        List<AbortTxnRequest> requests = ABORT_REQUESTS.getOrDefault(task.getTxnId(), List.of());
        String received = requests.stream()
                .map(request -> "{skipCleanup=" + request.skipCleanup + ", tabletIds=" + request.tabletIds + "}")
                .collect(Collectors.joining(", "));
        List<AbortTxnRequest> cleanups = requests.stream()
                .filter(request -> Boolean.FALSE.equals(request.skipCleanup))
                .collect(Collectors.toList());
        assertEquals(1, cleanups.size(), "abort requests: " + received);
        assertEquals(new HashSet<>(writtenTablets), new HashSet<>(cleanups.get(0).tabletIds), received);
    }
}
