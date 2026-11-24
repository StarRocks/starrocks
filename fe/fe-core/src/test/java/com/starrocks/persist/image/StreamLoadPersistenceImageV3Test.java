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

package com.starrocks.persist.image;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.StreamLoadMultiStmtTaskPB;
import com.starrocks.persist.proto.StreamLoadTaskPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionStmtExecutor;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamLoadPersistenceImageV3Test {
    @BeforeEach
    public void setUp() {
        TaskImageTestSupport.global();
    }

    static StreamLoadTask task(long id, StreamLoadTask.State state) {
        OlapTable table = mock(OlapTable.class);
        when(table.getId()).thenReturn(20L);
        when(table.getName()).thenReturn("table");
        StreamLoadTask task = new StreamLoadTask(id, new Database(10, "db"), table, "stream-" + id,
                "user", "127.0.0.1", 120000, 3, 0, System.currentTimeMillis(), WarehouseComputeResource.of(77));
        setField(task, "state", state);
        setField(task, "loadId", new TUniqueId(id + 100, id + 200));
        setField(task, "errorMsg", "last-error");
        setField(task, "trackingUrl", "http://127.0.0.1/track");
        setField(task, "beforeLoadTimeMs", 101L);
        setField(task, "startLoadingTimeMs", 102L);
        setField(task, "startPreparingTimeMs", 103L);
        setField(task, "finishPreparingTimeMs", 104L);
        setField(task, "commitTimeMs", 105L);
        setField(task, "endTimeMs", System.currentTimeMillis());
        setField(task, "txnId", id + 10000);
        setField(task, "preparedChannelNum", 2);
        setField(task, "numRowsNormal", 1001L);
        setField(task, "numRowsAbnormal", 23L);
        setField(task, "numRowsUnselected", 45L);
        setField(task, "numLoadBytesTotal", 987654L);
        setField(task, "beginTxnTimeMs", 106L);
        setField(task, "planTimeMs", 107L);
        setField(task, "receiveDataTimeMs", 108L);
        return task;
    }

    static StreamLoadMultiStmtTask multi(long id, StreamLoadMultiStmtTask.State state) {
        StreamLoadMultiStmtTask task = new StreamLoadMultiStmtTask(id, new Database(10, "db"), "multi-" + id,
                "multi-user", "127.0.0.2", 120000, System.currentTimeMillis(), WarehouseComputeResource.of(88));
        setField(task, "state", state);
        setField(task, "txnId", id + 20000);
        setField(task, "loadId", new TUniqueId(id + 300, id + 400));
        setField(task, "endTimeMs", System.currentTimeMillis());
        setField(task, "abortRetryCount", 7);
        setField(task, "nextAbortRetryTimeMs", 123456789L);
        setField(task, "abortReason", "restart-abort");
        Map<String, StreamLoadTask> children = getField(task, "taskMaps");
        children.put("original-table-key", task(id + 1000, StreamLoadTask.State.FINISHED));
        return task;
    }

    @Test
    public void testAllOrdinaryTaskFieldsAndStates() throws Exception {
        for (StreamLoadTask.State state : StreamLoadTask.State.values()) {
            StreamLoadTask source = task(1, state);
            StreamLoadTask restored = StreamLoadTask.fromProto(StreamLoadTaskPB.parseFrom(source.toProto().toByteArray()));
            GsonEquivalence.assertEquivalent(source, restored);
            assertEquals(source.getTUniqueId(), restored.getTUniqueId());
            assertEquals(state.name(), restored.getStateName());
            assertEquals(77L, restored.getCurrentWarehouseId());
            assertEquals(10001L, restored.getTxnId());
            TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
        }
    }

    @Test
    public void testAllMultiStatementTaskFieldsAndStates() throws Exception {
        for (StreamLoadMultiStmtTask.State state : StreamLoadMultiStmtTask.State.values()) {
            StreamLoadMultiStmtTask source = multi(1, state);
            StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(
                    StreamLoadMultiStmtTaskPB.parseFrom(source.toProto().toByteArray()));
            GsonEquivalence.assertEquivalent(source, restored);
            assertEquals(state.name(), restored.getStateName());
            assertEquals(88L, restored.getCurrentWarehouseId());
            assertEquals(20001L, restored.getTxnId());
            TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
        }
    }

    @Test
    public void testUuidComesFromCurrentRuntimeId() {
        StreamLoadTask source = task(1, StreamLoadTask.State.LOADING);
        setField(source, "loadIdHi", -123L);
        setField(source, "loadIdLo", -456L);
        StreamLoadTaskPB pb = source.toProto();
        assertEquals(101L, pb.getLoadIdHi());
        assertEquals(201L, pb.getLoadIdLo());
        assertEquals(new TUniqueId(101, 201), StreamLoadTask.fromProto(pb).getTUniqueId());
        StreamLoadMultiStmtTask multi = multi(2, StreamLoadMultiStmtTask.State.ABORTING);
        setField(multi, "loadIdHi", -123L);
        setField(multi, "loadIdLo", -456L);
        StreamLoadMultiStmtTaskPB multiPb = multi.toProto();
        assertEquals(302L, multiPb.getLoadIdHi());
        assertEquals(402L, multiPb.getLoadIdLo());
        TUniqueId restoredId = getField(StreamLoadMultiStmtTask.fromProto(multiPb), "loadId");
        assertEquals(new TUniqueId(302, 402), restoredId);
    }

    @Test
    public void testOrdinaryRuntimeInitializationMatchesV2() {
        StreamLoadTask source = task(1, StreamLoadTask.State.PREPARING);
        setField(source, "isCommitting", true);
        StreamLoadTask expected = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), StreamLoadTask.class);
        StreamLoadTask restored = StreamLoadTask.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(expected, restored);
        for (String field : List.of("channels", "taskDeadlineMs", "isCommitting", "isSyncStreamLoad", "type")) {
            assertEquals((Object) getField(expected, field), (Object) getField(restored, field), field);
        }
        List<StreamLoadTask.State> channels = getField(restored, "channels");
        assertEquals(List.of(StreamLoadTask.State.PREPARING, StreamLoadTask.State.PREPARING,
                StreamLoadTask.State.PREPARING), channels);
        ConnectContext context = getField(restored, "context");
        assertEquals("user", context.getQualifiedUser());
        ReentrantReadWriteLock lock = getField(restored, "lock");
        assertTrue(lock.isFair());
        for (String field : List.of("coord", "streamLoadParams", "streamLoadInfo", "channelIdToBEHTTPAddress",
                "channelIdToBEHTTPPort", "table")) {
            assertNull(getField(restored, field), field);
        }
        assertNotSame(getField(source, "lock"), lock);
    }

    @Test
    public void testMultiStatementRestoresLockAndAbortRetryState() {
        StreamLoadMultiStmtTask source = multi(1, StreamLoadMultiStmtTask.State.ABORTING);
        StreamLoadMultiStmtTask expected = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(source), StreamLoadMultiStmtTask.class);
        StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(expected, restored);
        ReentrantReadWriteLock lock = getField(restored, "lock");
        assertTrue(lock.isFair());
        assertNotSame(getField(source, "lock"), lock);
        assertEquals(7, (int) getField(restored, "abortRetryCount"));
        assertEquals(123456789L, (long) getField(restored, "nextAbortRetryTimeMs"));
        assertEquals("restart-abort", getField(restored, "abortReason"));
    }

    private void rejectSessionRollback() {
        new MockUp<TransactionStmtExecutor>() {
            @Mock
            public void rollbackStmt(ConnectContext context, RollbackStmt statement) {
                fail("An image-restored task must not roll back through a new session");
            }
        };
    }

    @Test
    public void testMultiStatementRestartCancellationMatchesV2() {
        StreamLoadMultiStmtTask source = multi(1, StreamLoadMultiStmtTask.State.BEGIN);
        StreamLoadMultiStmtTask expected = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(source), StreamLoadMultiStmtTask.class);
        StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(source.toProto());
        rejectSessionRollback();
        for (StreamLoadMultiStmtTask task : List.of(expected, restored)) {
            long beforeCancel = System.currentTimeMillis();
            task.cancelAfterRestart();
            assertEquals(StreamLoadMultiStmtTask.State.CANCELLED.name(), task.getStateName());
            assertEquals(source.getTxnId(), task.getTxnId());
            assertTrue(task.endTimeMs() >= beforeCancel);
            assertTrue(task.endTimeMs() <= System.currentTimeMillis());
            assertTrue(task.getTasks().stream().allMatch(t -> t.getStateName().equals("CANCELLED")));
        }
    }

    @Test
    public void testMultiStatementAbortRetryMatchesV2() {
        StreamLoadMultiStmtTask source = multi(1, StreamLoadMultiStmtTask.State.ABORTING);
        StreamLoadMultiStmtTask expected = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(source), StreamLoadMultiStmtTask.class);
        StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(source.toProto());
        rejectSessionRollback();
        for (StreamLoadMultiStmtTask task : List.of(expected, restored)) {
            assertFalse(task.retryAbortIfNeeded(123456788L));
            assertEquals(StreamLoadMultiStmtTask.State.ABORTING.name(), task.getStateName());
            assertTrue(task.retryAbortIfNeeded(123456789L));
            assertEquals(StreamLoadMultiStmtTask.State.CANCELLED.name(), task.getStateName());
            assertTrue(task.retryAbortIfNeeded(123456790L));
            assertTrue(task.getTasks().stream().allMatch(t -> t.getStateName().equals("FINISHED")));
        }
    }

    @Test
    public void testCancelledMultiStatementLoadResponseMatchesV2() throws Exception {
        StreamLoadMultiStmtTask source = multi(1, StreamLoadMultiStmtTask.State.CANCELLED);
        setField(source, "errorMsg", "runtime-only error");
        StreamLoadMultiStmtTask expected = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(source), StreamLoadMultiStmtTask.class);
        StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(source.toProto());
        TransactionResult expectedResponse = new TransactionResult();
        TransactionResult actualResponse = new TransactionResult();
        assertNull(expected.tryLoad(0, "original-table-key", expectedResponse));
        assertNull(restored.tryLoad(0, "original-table-key", actualResponse));
        assertFalse(actualResponse.stateOK());
        assertEquals(expectedResponse.toJson(), actualResponse.toJson());
    }

    @Test
    public void testNestedTaskMapRetainsOriginalKeyAndIsMutable() {
        StreamLoadMultiStmtTask source = multi(1, StreamLoadMultiStmtTask.State.PREPARED);
        StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(source.toProto());
        Map<String, StreamLoadTask> children = getField(restored, "taskMaps");
        assertEquals(List.of("original-table-key"), List.copyOf(children.keySet()));
        StreamLoadTask child = children.get("original-table-key");
        assertEquals("table", child.getTableName());
        assertNotNull(getField(child, "lock"));
        assertEquals(new TUniqueId(1101, 1201), child.getTUniqueId());
        children.put("second", task(5000, StreamLoadTask.State.BEGIN));
        assertEquals(2, restored.getTasks().size());
        assertEquals(1, source.getTasks().size());
    }

    @Test
    public void testEmptyNestedTasksAndNullableStrings() {
        StreamLoadMultiStmtTask source = multi(1, StreamLoadMultiStmtTask.State.BEGIN);
        source.getTasks().clear();
        setField(source, "abortReason", null);
        setField(source, "user", null);
        StreamLoadMultiStmtTask expected = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(source), StreamLoadMultiStmtTask.class);
        StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(expected, restored);
        assertTrue(restored.getTasks().isEmpty());
        assertNull(restored.getUser());
    }

    @Test
    public void testOrdinaryRuntimeTypeAndSyncFlagAreNotPersisted() {
        for (StreamLoadTask.Type type : StreamLoadTask.Type.values()) {
            StreamLoadTask source = task(1, StreamLoadTask.State.BEGIN);
            setField(source, "type", type);
            source.setIsSyncStreamLoad(true);
            StreamLoadTask expected = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), StreamLoadTask.class);
            StreamLoadTask restored = StreamLoadTask.fromProto(source.toProto());
            assertFalse(restored.isSyncStreamLoad());
            assertEquals(expected.getStringByType(), restored.getStringByType());
            assertEquals(StreamLoadTask.Type.PARALLEL_STREAM_LOAD, getField(restored, "type"));
        }
    }

    @Test
    public void testMultiStatementTypeIsPersisted() {
        for (StreamLoadTask.Type type : StreamLoadTask.Type.values()) {
            StreamLoadMultiStmtTask source = multi(1, StreamLoadMultiStmtTask.State.BEGIN);
            setField(source, "type", type);
            StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(source.toProto());
            assertEquals(type, getField(restored, "type"));
            GsonEquivalence.assertEquivalent(source, restored);
        }
    }

    @Test
    public void testExpiryBoundaryForBothTypes() {
        long end = 100000L;
        long cutoff = end + Config.stream_load_task_keep_max_second * 1000L;
        StreamLoadTask task = task(1, StreamLoadTask.State.FINISHED);
        setField(task, "endTimeMs", end);
        StreamLoadTask restored = StreamLoadTask.fromProto(task.toProto());
        assertFalse(restored.checkNeedRemove(cutoff, false));
        assertTrue(restored.checkNeedRemove(cutoff + 1, false));
        StreamLoadMultiStmtTask multi = multi(2, StreamLoadMultiStmtTask.State.FINISHED);
        setField(multi, "endTimeMs", end);
        StreamLoadMultiStmtTask restoredMulti = StreamLoadMultiStmtTask.fromProto(multi.toProto());
        assertFalse(restoredMulti.checkNeedRemove(cutoff, false));
        assertTrue(restoredMulti.checkNeedRemove(cutoff + 1, false));
    }

    @Test
    public void testAbortingTaskSurvivesExpiryAndForceCleanup() {
        StreamLoadMultiStmtTask source = multi(1, StreamLoadMultiStmtTask.State.ABORTING);
        setField(source, "endTimeMs", 1L);
        StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(source.toProto());
        assertFalse(restored.checkNeedRemove(Long.MAX_VALUE, false));
        assertFalse(restored.checkNeedRemove(Long.MAX_VALUE, true));
        assertEquals(StreamLoadMultiStmtTask.State.ABORTING.name(), restored.getStateName());
        assertEquals(7, (int) getField(restored, "abortRetryCount"));
    }

    @Test
    public void testMissingFieldsMatchLegacyGsonAllocation() {
        StreamLoadMultiStmtTask legacy = GsonUtils.GSON.fromJson("{}", StreamLoadMultiStmtTask.class);
        StreamLoadMultiStmtTask restored = StreamLoadMultiStmtTask.fromProto(StreamLoadMultiStmtTaskPB.getDefaultInstance());
        GsonEquivalence.assertEquivalent(legacy, restored);
        StreamLoadTask legacyOrdinary = GsonUtils.GSON.fromJson("{}", StreamLoadTask.class);
        StreamLoadTask restoredOrdinary = StreamLoadTask.fromProto(StreamLoadTaskPB.getDefaultInstance());
        GsonEquivalence.assertEquivalent(legacyOrdinary, restoredOrdinary);
    }
}
