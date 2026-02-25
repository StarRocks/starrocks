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

package com.starrocks.load.streamload;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStmtExecutor;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class StreamLoadMultiStmtTaskTest {
    private Database db;
    private StreamLoadMultiStmtTask multiTask;

    @BeforeEach
    public void setUp() {
        // Initialize basic fixtures
        db = new Database(1L, "test_db");
        multiTask = new StreamLoadMultiStmtTask(1L, db, "label_multi", "u", "127.0.0.1",
                1000L, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
    }

    @Test
    public void testExecuteTaskNoSubTask() {
        TransactionResult resp = new TransactionResult();
        Assertions.assertNull(multiTask.executeTask(0, "unknown", null, resp));
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testPrepareChannelNoSubTask() {
        TransactionResult resp = new TransactionResult();
        multiTask.prepareChannel(0, "unknown", null, resp);
        Assertions.assertFalse(resp.stateOK());
    }

    @Test
    public void testCommitTxnEmpty() throws StarRocksException {
        TransactionResult resp = new TransactionResult();
        multiTask.commitTxn(null, resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertEquals("COMMITED", multiTask.getStateName());
    }

    @Test
    public void testManualCancelTask() throws StarRocksException {
        TransactionResult resp = new TransactionResult();
        multiTask.manualCancelTask(resp);
        Assertions.assertEquals("CANCELLED", multiTask.getStateName());
        Assertions.assertTrue(multiTask.endTimeMs() > 0);
    }

    @Test
    public void testBeginTxnSetsExecutionIdAndResource() throws Exception {
        // Capture loadId from task for later comparison
        TUniqueId expectedLoadId = (TUniqueId) Deencapsulation.getField(multiTask, "loadId");

        // Mock static beginStmt to assert context is pre-populated and set a fake txnId
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void beginStmt(com.starrocks.qe.ConnectContext ctx,
                                  com.starrocks.sql.ast.txn.BeginStmt stmt,
                                  TransactionState.LoadJobSourceType sourceType,
                                  String labelOverride) {
                // executionId must be set and convertible to non-empty label
                Assertions.assertNotNull(ctx.getExecutionId());
                String label = DebugUtil.printId(ctx.getExecutionId());
                Assertions.assertNotNull(label);
                Assertions.assertFalse(label.isEmpty());

                // executionId should equal task.loadId
                Assertions.assertEquals(expectedLoadId.getHi(), ctx.getExecutionId().getHi());
                Assertions.assertEquals(expectedLoadId.getLo(), ctx.getExecutionId().getLo());

                // compute resource should be propagated
                Assertions.assertEquals(WarehouseManager.DEFAULT_RESOURCE, ctx.getCurrentComputeResource());

                // labelOverride should equal the multi-statement task's label
                Assertions.assertEquals("label_multi", labelOverride);

                // simulate txn id assignment inside begin
                ctx.setTxnId(987654321L);
            }
        };

        TransactionResult resp = new TransactionResult();
        multiTask.beginTxn(resp);
        Assertions.assertTrue(resp.stateOK());
        Assertions.assertEquals(987654321L, multiTask.getTxnId());
    }

    @Test
    public void testCheckNeedRemoveAndDurable() throws Exception {
        Assertions.assertFalse(multiTask.checkNeedRemove(System.currentTimeMillis(), false));
        StreamLoadTask sub = new StreamLoadTask(2L, db, new OlapTable(), "label_sub", "u", "127.0.0.1", 1000, 1, 0,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        Deencapsulation.setField(sub, "state", StreamLoadTask.State.FINISHED);
        @SuppressWarnings("unchecked") java.util.Map<String, StreamLoadTask> map =
                (java.util.Map<String, StreamLoadTask>) Deencapsulation.getField(multiTask, "taskMaps");
        map.put("tbl", sub);
        Deencapsulation.setField(multiTask, "endTimeMs",
                System.currentTimeMillis() - (Config.stream_load_task_keep_max_second * 1000L + 10));
        Assertions.assertTrue(multiTask.isFinalState());
        Assertions.assertTrue(multiTask.checkNeedRemove(System.currentTimeMillis(), false));
    }

    @Test
    public void testToThriftAndStreamLoadThriftEmpty() {
        Assertions.assertTrue(multiTask.toThrift().isEmpty());
        Assertions.assertTrue(multiTask.toStreamLoadThrift().isEmpty());
    }

    @Test
    public void testCallbackDelegations() throws Exception {
        StreamLoadTask sub1 = new StreamLoadTask(3L, db, new OlapTable(), "l1", "u", "127.0.0.1", 1000, 1, 0,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        StreamLoadTask sub2 = new StreamLoadTask(4L, db, new OlapTable(), "l2", "u", "127.0.0.1", 1000, 1, 0,
                System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
        java.util.Map<String, StreamLoadTask> map =
                (java.util.Map<String, StreamLoadTask>) Deencapsulation.getField(multiTask, "taskMaps");
        map.put("t1", sub1);
        map.put("t2", sub2);
        TransactionState txnState = new TransactionState();
        multiTask.beforePrepared(txnState);
        multiTask.afterPrepared(txnState, true);
        multiTask.replayOnPrepared(txnState);
        multiTask.beforeCommitted(txnState);
        multiTask.afterCommitted(txnState, true);
        multiTask.replayOnCommitted(txnState);
        multiTask.afterAborted(txnState, true, "reason");
        multiTask.replayOnAborted(txnState);
        multiTask.afterVisible(txnState, true);
        multiTask.replayOnVisible(txnState);
        List<List<String>> show = multiTask.getShowInfo();
        Assertions.assertEquals(2, show.size());
    }
}
