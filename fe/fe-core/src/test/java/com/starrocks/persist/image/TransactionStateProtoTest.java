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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.starrocks.catalog.ColumnId;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.lake.compaction.Quantiles;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.FailMsg;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.loadv2.MiniLoadTxnCommitAttachment;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.PulsarProgress;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.load.routineload.RoutineLoadProgress;
import com.starrocks.load.streamload.StreamLoadTxnCommitAttachment;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.EtlStatusPB;
import com.starrocks.persist.proto.FailMsgPB;
import com.starrocks.persist.proto.InsertTxnCommitAttachmentPB;
import com.starrocks.persist.proto.LoadJobSourceTypePB;
import com.starrocks.persist.proto.RoutineLoadProgressPB;
import com.starrocks.persist.proto.TransactionIdGeneratorPB;
import com.starrocks.persist.proto.TransactionStatePB;
import com.starrocks.persist.proto.TxnCommitAttachmentPB;
import com.starrocks.replication.ReplicationTxnCommitAttachment;
import com.starrocks.thrift.TEtlState;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionIdGenerator;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.transaction.TxnFinishState;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionStateProtoTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        MetricRepo.init();
    }

    static TransactionState transaction(long dbId, long id, TransactionStatus status, long commit, long finish) {
        TransactionState state = new TransactionState();
        setField(state, "dbId", dbId);
        setField(state, "transactionId", id);
        setField(state, "tableIdList", new ArrayList<>(List.of(101L, 102L)));
        setField(state, "label", "label-" + id);
        setField(state, "transactionStatus", status);
        state.setCommitTime(commit);
        state.setFinishTime(finish);
        return state;
    }

    static TransactionState richTransaction() {
        TransactionState state = transaction(7, 19, TransactionStatus.COMMITTED, 700, 900);
        setField(state, "txnCoordinator", TransactionState.TxnCoordinator.fromBackend("10.0.0.8", 800));
        setField(state, "sourceType", TransactionState.LoadJobSourceType.BACKEND_STREAMING);
        setField(state, "prepareTime", 101L);
        setField(state, "preparedTime", 201L);
        setField(state, "reason", "persisted reason");
        setField(state, "globalTransactionId", 701L);
        setField(state, "newFinish", true);
        TxnFinishState finish = new TxnFinishState();
        finish.normalReplicas.addAll(List.of(11L, 12L));
        finish.abnormalReplicasWithVersion.put(13L, 99L);
        setField(state, "finishState", finish);
        setField(state, "errorReplicas", new HashSet<>(List.of(41L, 42L)));
        setField(state, "unknownReplicas", new HashSet<>(List.of(51L, 52L)));
        setField(state, "useCombinedTxnLog", true);
        setField(state, "isNoOpPublish", true);
        setField(state, "noOpPublishReason", "empty input");
        state.addLoadId(new TUniqueId(123, 456));
        state.addLoadId(new TUniqueId(789, 901));
        setField(state, "callbackId", 80L);
        setField(state, "callbackIdList", new ArrayList<>(List.of(80L, 81L)));
        setField(state, "timeoutMs", 9001L);
        setField(state, "preparedTimeoutMs", 9002L);
        state.setComputeResource(new WarehouseComputeResource(29));
        setField(state, "txnCommitAttachment", new InsertTxnCommitAttachment(700, 8));
        Map<Long, Map<Long, List<Long>>> indexes = getField(state, "loadedTblPartitionIndexes");
        indexes.put(101L, Map.of(201L, List.of(91L, 90L), 202L, List.of()));
        TableCommitInfo table = new TableCommitInfo(101);
        PartitionCommitInfo partition = new PartitionCommitInfo(201, 100, 700,
                List.of(ColumnId.create("invalid")), List.of(ColumnId.create("valid")), List.of(13L, 17L));
        setField(partition, "dataVersion", 501L);
        setField(partition, "versionEpoch", 502L);
        setField(partition, "compactionScore", new Quantiles(1.25, 2.5, 7.75));
        Map<Long, PartitionCommitInfo> partitions = getField(table, "idToPartitionCommitInfo");
        partitions.put(201L, partition);
        Map<Long, TableCommitInfo> tables = getField(state, "idToTableCommitInfos");
        tables.put(101L, table);
        return state;
    }

    @Test
    public void testAllTransactionFieldsAndRuntimeDefaults() throws Exception {
        TransactionState source = richTransaction();
        TransactionState loaded = TransactionState.fromProto(TransactionStatePB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(source, loaded);
        List<TUniqueId> ids = loaded.getLoadIds();
        assertEquals(List.of(new TUniqueId(123, 456), new TUniqueId(789, 901)), ids);
        // Backend redirection and publish tasks are runtime state, as with v2.
        assertEquals(-1, loaded.getCoordinator().getBackendId());
        assertNotNull(getField(loaded, "txnLock"));
        assertNotNull(getField(loaded, "latch"));
        assertNotNull(getField(loaded, "txnSpan"));
        Map<?, ?> tasks = getField(loaded, "publishVersionTasks");
        assertTrue(tasks.isEmpty());
        TransactionState v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), TransactionState.class);
        GsonEquivalence.assertEquivalent(source, v2);
        List<TUniqueId> v2Ids = v2.getLoadIds();
        assertEquals(ids, v2Ids);
        assertEquals(source.getCommitTime(), loaded.getCommitTime());
        assertEquals(source.getFinishTime(), loaded.getFinishTime());
    }

    @Test
    public void testCallbackFallbackDoesNotMutateSource() {
        TransactionState source = new TransactionState();
        setField(source, "callbackId", -1L);
        setField(source, "callbackIdList", new ArrayList<>(List.of(17L, 18L)));
        TransactionStatePB pb = source.toProto();
        assertEquals(-1L, (long) getField(source, "callbackId"));
        assertEquals(17, pb.getCallbackId());
        assertEquals(List.of(17L, 18L), pb.getCallbackIdList().getValuesList());
        TransactionState restored = TransactionState.fromProto(pb);
        assertEquals(17L, (long) getField(restored, "callbackId"));
        // GSON has the same compatibility fallback, but its pre-hook modifies the source.
        GsonEquivalence.assertEquivalent(source, restored);
    }

    @Test
    public void testNullEmptyAndLoadIdPresence() {
        TransactionState source = new TransactionState();
        for (String field : List.of("loadIds", "errorReplicas", "unknownReplicas", "callbackIdList", "txnCommitAttachment")) {
            setField(source, field, null);
        }
        TransactionState loaded = TransactionState.fromProto(source.toProto());
        for (String field : List.of("loadIds", "errorReplicas", "unknownReplicas", "callbackIdList", "txnCommitAttachment")) {
            assertNull(getField(loaded, field), field);
        }
        setField(source, "loadIds", new ArrayList<>());
        setField(source, "callbackIdList", new ArrayList<>());
        assertNull(TransactionState.fromProto(source.toProto()).getLoadIds());
        assertNull(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), TransactionState.class).getLoadIds());
        source.addLoadId(new TUniqueId());
        source.addLoadId(new TUniqueId().setHi(-7));
        List<TUniqueId> restored = TransactionState.fromProto(source.toProto()).getLoadIds();
        assertEquals(List.of(new TUniqueId(0, 0), new TUniqueId(-7, 0)), restored);
        assertEquals(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), TransactionState.class).getLoadIds(), restored);
    }

    @Test
    public void testLegacyLoadIdsMatchV2Recovery() {
        for (String json : List.of("{\"loadIds\":[{},null]}", "{\"loadIds\":[null]}",
                "{\"lids\":[{\"hi\":123,\"lo\":456}]}")) {
            TransactionState v2 = GsonUtils.GSON.fromJson(json, TransactionState.class);
            setField(v2, "transactionStatus", TransactionStatus.COMMITTED);
            TransactionState v3 = TransactionState.fromProto(v2.toProto());
            assertEquals(v2.getLoadIds(), v3.getLoadIds());
            assertEquals(v2.hasLostLoadIds(), v3.hasLostLoadIds());
            GsonEquivalence.assertEquivalent(v2, v3);
        }
    }

    @Test
    public void testDefaultScalarsAndIdHighWatermark() {
        TransactionState defaults = new TransactionState();
        TransactionState loaded = TransactionState.fromProto(TransactionStatePB.getDefaultInstance());
        for (String name : List.of("dbId", "transactionId", "prepareTime", "commitTime", "finishTime",
                "callbackId", "timeoutMs", "preparedTimeoutMs")) {
            assertEquals((Object) getField(defaults, name), (Object) getField(loaded, name), name);
        }
        assertEquals(defaults.getTransactionStatus(), loaded.getTransactionStatus());
        assertEquals(defaults.getSourceType(), loaded.getSourceType());
        assertEquals(defaults.getCoordinator().toString(), loaded.getCoordinator().toString());
        assertEquals(defaults.getComputeResource(), loaded.getComputeResource());
        for (long batchEnd : List.of(0L, 2000L)) {
            TransactionIdGenerator source = new TransactionIdGenerator();
            source.initTransactionId(batchEnd);
            TransactionIdGenerator restored = TransactionIdGenerator.fromProto(source.toProto());
            assertEquals(batchEnd + 1, restored.peekNextTransactionId());
        }
        assertEquals(1, TransactionIdGenerator.fromProto(TransactionIdGeneratorPB.getDefaultInstance())
                .peekNextTransactionId());
    }

    @Test
    public void testStatesAndSourceTypesDoNotCountCompletions() {
        long succeeded = MetricRepo.COUNTER_TXN_SUCCESS.getValue();
        long failed = MetricRepo.COUNTER_TXN_FAILED.getValue();
        for (TransactionStatus status : TransactionStatus.values()) {
            for (TransactionState.LoadJobSourceType sourceType : TransactionState.LoadJobSourceType.values()) {
                TransactionState source = transaction(1, 2, status, 3, 4);
                setField(source, "sourceType", sourceType);
                TransactionState loaded = TransactionState.fromProto(source.toProto());
                assertEquals(status, loaded.getTransactionStatus());
                assertEquals(sourceType, loaded.getSourceType());
            }
        }
        assertEquals(succeeded, MetricRepo.COUNTER_TXN_SUCCESS.getValue());
        assertEquals(failed, MetricRepo.COUNTER_TXN_FAILED.getValue());
    }

    static List<TxnCommitAttachment> attachments() {
        InsertTxnCommitAttachment insert = new InsertTxnCommitAttachment(99, 100);
        insert.setRequireEmptyPartitions(true);
        setField(insert, "shadowRewriteWatershedTxnId", 120L);
        setField(insert, "shadowRewriteAlterVersion", 130L);
        EtlStatus etl = new EtlStatus();
        setField(etl, "state", TEtlState.FINISHED);
        setField(etl, "trackingUrl", "http://host/etl");
        setField(etl, "stats", Map.of("raw", "{\"kept\":true}"));
        setField(etl, "counters", Map.of("rows", "123"));
        EtlStatus.LoadStatistic statistics = new EtlStatus.LoadStatistic();
        long i = 1;
        for (String field : List.of("counterTbl", "sourceScanBytesCounterTbl", "sinkBytesCounterTbl",
                "sourceRowsCounterTbl", "sourceBytesCounterTbl", "filteredRowsCounterTbl", "unselectedRowsCounterTbl")) {
            Table<String, String, Long> counters = HashBasedTable.create();
            counters.put("load-1", "fragment-1", i++);
            counters.put("load-2", "fragment-2", i++);
            setField(statistics, field, counters);
        }
        setField(statistics, "unfinishedBackendIds", Map.of("load-1", List.of(8L, 7L)));
        setField(statistics, "allBackendIds", Map.of("load-1", List.of(8L, 7L, 6L), "load-2", List.of()));
        statistics.fileNum = 3;
        statistics.totalFileSizeB = 12345;
        statistics.numScanRange = 67;
        setField(statistics, "loadFinish", true);
        setField(etl, "loadStatistic", statistics);
        LoadJobFinalOperation load = new LoadJobFinalOperation(11, etl, 97, 101, 102,
                JobState.FINISHED, new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, "failure detail"));
        ManualLoadTxnCommitAttachment manual = new ManualLoadTxnCommitAttachment();
        MiniLoadTxnCommitAttachment mini = new MiniLoadTxnCommitAttachment();
        for (TxnCommitAttachment attachment : List.of(manual, mini)) {
            setField(attachment, "loadedRows", 101L);
            setField(attachment, "filteredRows", 7L);
            setField(attachment, "errorLogUrl", "http://host/errors");
        }
        RLTaskTxnCommitAttachment routine = new RLTaskTxnCommitAttachment();
        i = 201;
        for (String name : List.of("filteredRows", "loadedRows", "unselectedRows", "receivedBytes", "taskExecutionTimeMs")) {
            setField(routine, name, i++);
        }
        setField(routine, "progress", new KafkaProgress(Map.of(3, 77L, 4, 88L)));
        setField(routine, "timestampProgress", new KafkaProgress(Map.of(3, 10007L, 4, 10008L)));
        StreamLoadTxnCommitAttachment stream = new StreamLoadTxnCommitAttachment(1, 2, 3, 4, 5, 6, 7, 8, 9, "url");
        ReplicationTxnCommitAttachment replication = new ReplicationTxnCommitAttachment(Map.of(6L, 600L), Map.of(6L, 700L));
        CompactionTxnCommitAttachment compaction = new CompactionTxnCommitAttachment();
        setField(compaction, "forceCommit", true);
        setField(compaction, "unshare", true);
        return List.of(insert, load, manual, mini, routine, stream, replication, compaction);
    }

    @Test
    public void testAllAttachmentVariantsThroughTransaction() throws Exception {
        for (TxnCommitAttachment attachment : attachments()) {
            // sourceType is a persisted business field, independent of the concrete class.
            setField(attachment, "sourceType", TransactionState.LoadJobSourceType.SHADOW_REWRITE);
            TransactionState state = richTransaction();
            setField(state, "txnCommitAttachment", attachment);
            TransactionState loaded = TransactionState.fromProto(TransactionStatePB.parseFrom(
                    assertInstances(state.toProto()).toByteArray()));
            TxnCommitAttachment restored = getField(loaded, "txnCommitAttachment");
            assertEquals(attachment.getClass(), restored.getClass());
            GsonEquivalence.assertEquivalent(state, loaded);
            TxnCommitAttachment v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(attachment, TxnCommitAttachment.class),
                    TxnCommitAttachment.class);
            GsonEquivalence.assertEquivalent(attachment, v2);
        }
    }

    @Test
    public void testInsertWithoutEmptyPartitionFlagKeepsDefault() {
        TxnCommitAttachmentPB pb = new InsertTxnCommitAttachment(10).toProto().toBuilder()
                .setInsert(InsertTxnCommitAttachmentPB.newBuilder().setLoadedRows(10)).build();
        assertFalse(pb.getInsert().hasRequireEmptyPartitions());
        InsertTxnCommitAttachment restored = (InsertTxnCommitAttachment) TxnCommitAttachment.fromProto(pb);
        assertFalse(restored.getRequireEmptyPartitions());
        assertEquals(10, restored.getLoadedRows());
    }

    @Test
    public void testPulsarProgressAndNullableAttachments() {
        PulsarProgress progress = new PulsarProgress();
        setField(progress, "partitionToBacklogNum", Map.of("topic-partition-0", 10L, "topic-partition-1", 20L));
        setField(progress, "partitionToInitialPosition", Map.of("runtime", 42L));
        RoutineLoadProgress loaded = RoutineLoadProgress.fromProto(assertInstances(progress.toProto()));
        assertEquals(PulsarProgress.class, loaded.getClass());
        GsonEquivalence.assertEquivalent(progress, loaded);
        assertTrue(((Map<?, ?>) getField(loaded, "partitionToInitialPosition")).isEmpty());
        for (TxnCommitAttachment attachment : List.of(new ManualLoadTxnCommitAttachment(),
                new MiniLoadTxnCommitAttachment(), new ReplicationTxnCommitAttachment(), new RLTaskTxnCommitAttachment())) {
            GsonEquivalence.assertEquivalent(attachment, TxnCommitAttachment.fromProto(attachment.toProto()));
        }
        PartitionCommitInfo info = new PartitionCommitInfo();
        setField(info, "invalidDictCacheColumns", null);
        setField(info, "validDictCacheColumns", new ArrayList<>());
        setField(info, "dictCollectedVersions", null);
        PartitionCommitInfo restored = PartitionCommitInfo.fromProto(info.toProto());
        assertEquals(List.of(), getField(restored, "invalidDictCacheColumns"));
        assertEquals(List.of(), getField(restored, "validDictCacheColumns"));
        assertEquals(List.of(), getField(restored, "dictCollectedVersions"));
        PartitionCommitInfo v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(info), PartitionCommitInfo.class);
        GsonEquivalence.assertEquivalent(v2, restored);
    }

    @Test
    public void testUnknownEnumsAndMissingVariantsFail() {
        assertThrows(IllegalArgumentException.class,
                () -> TxnCommitAttachment.fromProto(TxnCommitAttachmentPB.getDefaultInstance()));
        assertThrows(SubtypeNotFoundException.class, () -> TxnCommitAttachment.fromProto(
                TxnCommitAttachmentPB.newBuilder().setKindValue(999).build()));
        for (TxnCommitAttachmentPB.Kind kind : TxnCommitAttachmentPB.Kind.values()) {
            if (kind == TxnCommitAttachmentPB.Kind.UNRECOGNIZED || kind == TxnCommitAttachmentPB.Kind.UNSPECIFIED) {
                continue;
            }
            assertThrows(IllegalArgumentException.class, () -> TxnCommitAttachment.fromProto(
                    TxnCommitAttachmentPB.newBuilder().setKind(kind).build()));
        }
        assertThrows(IllegalArgumentException.class, () -> TransactionState.fromProto(
                TransactionStatePB.newBuilder().setTransactionStatusValue(999).build()));
        assertThrows(IllegalArgumentException.class, () -> TransactionState.fromProto(
                TransactionStatePB.newBuilder().setSourceType(LoadJobSourceTypePB.LOAD_JOB_SOURCE_TYPE_UNSPECIFIED).build()));
        assertThrows(IllegalArgumentException.class,
                () -> EtlStatus.fromProto(EtlStatusPB.newBuilder().setStateValue(999).build()));
        assertThrows(IllegalArgumentException.class,
                () -> FailMsg.fromProto(FailMsgPB.newBuilder().setCancelTypeValue(999).build()));
        assertThrows(IllegalArgumentException.class, () -> RoutineLoadProgress.fromProto(
                RoutineLoadProgressPB.newBuilder().setKind(RoutineLoadProgressPB.Kind.KAFKA_PROGRESS).build()));
    }

    @Test
    public void testLoadStatisticsNullEmptyAndAllEnums() {
        EtlStatus.LoadStatistic statistics = new EtlStatus.LoadStatistic();
        List<String> tables = List.of("counterTbl", "sourceScanBytesCounterTbl", "sinkBytesCounterTbl",
                "sourceRowsCounterTbl", "sourceBytesCounterTbl", "filteredRowsCounterTbl", "unselectedRowsCounterTbl");
        for (String field : tables) {
            setField(statistics, field, null);
        }
        setField(statistics, "unfinishedBackendIds", null);
        setField(statistics, "allBackendIds", Map.of());
        EtlStatus.LoadStatistic restored = EtlStatus.LoadStatistic.fromProto(statistics.toProto());
        for (String field : tables) {
            assertNull(getField(restored, field), field);
            setField(statistics, field, HashBasedTable.create());
        }
        assertNull(getField(restored, "unfinishedBackendIds"));
        assertEquals(Map.of(), getField(restored, "allBackendIds"));
        restored = EtlStatus.LoadStatistic.fromProto(statistics.toProto());
        for (String field : tables) {
            Table<?, ?, ?> table = getField(restored, field);
            assertTrue(table.isEmpty(), field);
        }
        for (TEtlState state : TEtlState.values()) {
            EtlStatus etl = new EtlStatus();
            setField(etl, "state", state);
            assertEquals(state, EtlStatus.fromProto(etl.toProto()).getState());
        }
        for (FailMsg.CancelType type : FailMsg.CancelType.values()) {
            FailMsg failure = new FailMsg(type, "reason");
            assertEquals(type, FailMsg.fromProto(failure.toProto()).getCancelType());
        }
        for (JobState state : JobState.values()) {
            LoadJobFinalOperation operation = new LoadJobFinalOperation(1, new EtlStatus(), 17, 100, 200, state, null);
            GsonEquivalence.assertEquivalent(operation, TxnCommitAttachment.fromProto(operation.toProto()));
        }
    }
}
