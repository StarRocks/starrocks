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

import com.starrocks.common.Config;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.PulsarProgress;
import com.starrocks.load.routineload.PulsarRoutineLoadJob;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.RoutineLoadJobPB;
import com.starrocks.persist.proto.RoutineLoadMgrPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnStateCallbackFactory;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.RoutineLoadJobProtoTest.kafka;
import static com.starrocks.persist.image.RoutineLoadJobProtoTest.pulsar;
import static com.starrocks.persist.image.TransactionStateProtoTest.transaction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RoutineLoadMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
        Mockito.when(global.getStreamLoadMgr()).thenReturn(Mockito.mock(StreamLoadMgr.class));
    }

    static RoutineLoadMgr manager(List<? extends RoutineLoadJob> jobs) {
        RoutineLoadMgr manager = new RoutineLoadMgr();
        Map<Long, RoutineLoadJob> byId = getField(manager, "idToRoutineLoadJob");
        for (RoutineLoadJob job : jobs) {
            byId.put(job.getId(), job);
        }
        return manager;
    }

    private RoutineLoadMgr v2(RoutineLoadMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.saveRoutineLoadJobsV2(image.getImageWriter());
        RoutineLoadMgr result = new RoutineLoadMgr();
        LoadImageTestSupport.transactions(global);
        {
            SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
            result.loadRoutineLoadJobsV2(reader);
            reader.close();
        }
        return result;
    }

    private RoutineLoadMgr v3(RoutineLoadMgr source) throws Exception {
        Path file = LoadImageTestSupport.save(tempDir, source::save);
        RoutineLoadMgr result = new RoutineLoadMgr();
        LoadImageTestSupport.transactions(global);
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, result.getImageV3Loaders());
        assertEquals(110, sections.get(0).getType());
        Map<Long, RoutineLoadJob> allSaved = getField(source, "idToRoutineLoadJob");
        assertEquals(allSaved.size(), sections.get(0).getNumEntries());
        return result;
    }

    private static Map<Long, Map<String, List<Long>>> names(RoutineLoadMgr manager) {
        Map<Long, Map<String, List<RoutineLoadJob>>> source = getField(manager, "dbToNameToRoutineLoadJob");
        Map<Long, Map<String, List<Long>>> result = new HashMap<>();
        source.forEach((db, jobs) -> {
            Map<String, List<Long>> names = new HashMap<>();
            jobs.forEach((name, list) -> names.put(name, list.stream().map(RoutineLoadJob::getId).toList()));
            result.put(db, names);
        });
        return result;
    }

    @Test
    public void testJobStatesExpiryHistoryAndCallbacks() throws Exception {
        long now = System.currentTimeMillis();
        int keep = Config.label_keep_max_second;
        Config.label_keep_max_second = 3600;
        try {
            List<RoutineLoadJob> jobs = new ArrayList<>();
            long id = 1;
            for (RoutineLoadJob.JobState state : RoutineLoadJob.JobState.values()) {
                RoutineLoadJob job = id % 2 == 0 ? pulsar(id) : kafka(id);
                setField(job, "state", state);
                setField(job, "endTimestamp", now);
                jobs.add(job);
                id++;
            }
            RoutineLoadJob expired = kafka(99);
            setField(expired, "state", RoutineLoadJob.JobState.STOPPED);
            setField(expired, "endTimestamp", now - 7_200_000);
            jobs.add(expired);
            RoutineLoadJob otherDb = pulsar(100);
            setField(otherDb, "dbId", 11L);
            jobs.add(otherDb);
            RoutineLoadMgr source = manager(jobs);
            RoutineLoadMgr baseline = v2(source);
            RoutineLoadMgr loaded = v3(source);
            assertEquals(names(baseline), names(loaded));
            assertNull(loaded.getJob(99));
            TxnStateCallbackFactory callbacks = global.getGlobalTransactionMgr().getCallbackFactory();
            assertEquals(4, callbacks.getCallBackCnt());
            for (RoutineLoadJob sourceJob : jobs) {
                if (sourceJob.getId() == 99) {
                    continue;
                }
                RoutineLoadJob restored = loaded.getJob(sourceJob.getId());
                GsonEquivalence.assertEquivalent(baseline.getJob(sourceJob.getId()), restored);
                if (restored.isFinal()) {
                    assertNull(callbacks.getCallback(restored.getId()));
                } else {
                    assertSame(restored, callbacks.getCallback(restored.getId()));
                }
                assertEquals(0, restored.getSizeOfRoutineLoadTaskInfoList());
            }

            Mockito.verifyNoInteractions(global.getEditLog(), global.getStreamLoadMgr());
        } finally {
            Config.label_keep_max_second = keep;
        }
    }

    @Test
    public void testExpiredAndRetainedFinalJobsMatchV2() throws Exception {
        long now = System.currentTimeMillis();
        int keep = Config.label_keep_max_second;
        Config.label_keep_max_second = 10;
        try {
            List<RoutineLoadJob> jobs = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                RoutineLoadJob job = kafka(i + 1);
                setField(job, "state", RoutineLoadJob.JobState.CANCELLED);
                setField(job, "endTimestamp", i == 2 ? 0L : now + 60_000L);
                jobs.add(job);
            }
            RoutineLoadMgr source = manager(jobs);
            RoutineLoadMgr baseline = v2(source);
            RoutineLoadMgr loaded = v3(source);
            assertEquals(names(baseline), names(loaded));
            assertNotNull(loaded.getJob(1));
            assertNotNull(loaded.getJob(2));
            assertNull(loaded.getJob(3));
            assertEquals(0, global.getGlobalTransactionMgr().getCallbackFactory().getCallBackCnt());
        } finally {
            Config.label_keep_max_second = keep;
        }
    }

    static RLTaskTxnCommitAttachment attachment(boolean kafka) {
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        setField(attachment, "loadedRows", 11L);
        setField(attachment, "filteredRows", 2L);
        setField(attachment, "unselectedRows", 3L);
        setField(attachment, "receivedBytes", 400L);
        setField(attachment, "taskExecutionTimeMs", 5L);
        if (kafka) {
            setField(attachment, "progress", new KafkaProgress(new ConcurrentHashMap<>(Map.of(1, 19L))));
            setField(attachment, "timestampProgress", new KafkaProgress(new ConcurrentHashMap<>(Map.of(1, 1000L))));
        } else {
            PulsarProgress progress = new PulsarProgress();
            setField(progress, "partitionToBacklogNum", new ConcurrentHashMap<>(Map.of("partition-1", 19L)));
            setField(attachment, "progress", progress);
            setField(attachment, "timestampProgress", new PulsarProgress());
        }
        return attachment;
    }

    static TransactionState callbackTransaction(long jobId, long txnId, TransactionStatus status) {
        TransactionState transaction = transaction(10, txnId, status, 100, 200);
        setField(transaction, "callbackId", jobId);
        setField(transaction, "callbackIdList", new ArrayList<>(List.of(jobId)));
        return transaction;
    }

    @Test
    public void testRecoveredJobsReceiveCommitAndAbortReplay() throws Exception {
        RoutineLoadMgr loaded = v3(manager(List.of(kafka(1), pulsar(2))));
        for (long id : List.of(1L, 2L)) {
            RoutineLoadJob job = loaded.getJob(id);
            assertEquals(0L, (long) getField(job, "committedTaskNum"));
            TransactionState committed = callbackTransaction(id, 100 + id, TransactionStatus.COMMITTED);
            committed.setTxnCommitAttachment(attachment(id == 1));
            committed.replaySetTransactionStatus();
            assertEquals(1L, (long) getField(job, "committedTaskNum"));
            assertEquals(16L, (long) getField(job, "totalRows"));
            assertEquals(2L, (long) getField(job, "errorRows"));
            assertEquals(3L, (long) getField(job, "unselectedRows"));
            assertEquals(400L, (long) getField(job, "receivedBytes"));
            assertEquals(6L, (long) getField(job, "totalTaskExcutionTimeMs"));
            if (id == 1) {
                // Kafka stores the next offset after the committed offset.
                assertEquals(Map.of(1, 20L), ((KafkaProgress) job.getProgress()).getPartitionIdToOffset(List.of(1)));
            } else {
                assertEquals(Map.of("partition-1", 19L), getField(job.getProgress(), "partitionToBacklogNum"));
            }
            TransactionState aborted = callbackTransaction(id, 200 + id, TransactionStatus.ABORTED);
            aborted.replaySetTransactionStatus();
            assertEquals(1L, (long) getField(job, "abortedTaskNum"));
            assertEquals(16L, (long) getField(job, "totalRows"));
            assertSame(job, global.getGlobalTransactionMgr().getCallbackFactory().getCallback(id));
        }
        Mockito.verifyNoInteractions(global.getEditLog());

    }

    @Test
    public void testFilePropertiesAreResolvedOnlyAtPrepare() throws Exception {
        SmallFileMgr files = Mockito.mock(SmallFileMgr.class);
        Mockito.when(global.getSmallFileMgr()).thenReturn(files);
        WarehouseManager warehouses = Mockito.mock(WarehouseManager.class);
        Mockito.when(global.getWarehouseMgr()).thenReturn(warehouses);
        Mockito.when(warehouses.acquireComputeResource(Mockito.any())).thenReturn(WarehouseManager.DEFAULT_RESOURCE);
        Mockito.when(files.getSmallFile(10, KafkaRoutineLoadJob.KAFKA_FILE_CATALOG, "ca", true))
                .thenReturn(new SmallFileMgr.SmallFile(10L, "kafka", "ca", 91L, "data", 4, "abcd", true));
        Mockito.when(files.getSmallFile(10, PulsarRoutineLoadJob.PULSAR_FILE_CATALOG, "ca", true))
                .thenReturn(new SmallFileMgr.SmallFile(10L, "pulsar", "ca", 92L, "data", 4, "ef01", true));
        KafkaRoutineLoadJob kafka = kafka(1);
        PulsarRoutineLoadJob pulsar = pulsar(2);
        for (RoutineLoadJob job : List.of(kafka, pulsar)) {
            setField(job, "customProperties", new HashMap<>(Map.of("ssl.ca.location", "FILE:ca")));
        }
        RoutineLoadMgr loaded = v3(manager(List.of(kafka, pulsar)));
        Mockito.verifyNoInteractions(files, warehouses);
        KafkaRoutineLoadJob restoredKafka = (KafkaRoutineLoadJob) loaded.getJob(1);
        PulsarRoutineLoadJob restoredPulsar = (PulsarRoutineLoadJob) loaded.getJob(2);
        assertTrue(restoredKafka.getConvertedCustomProperties().isEmpty());
        assertTrue(restoredPulsar.getConvertedCustomProperties().isEmpty());
        restoredKafka.prepare();
        restoredPulsar.prepare();
        assertEquals("FILE:91:abcd", restoredKafka.getConvertedCustomProperties().get("ssl.ca.location"));
        assertEquals("FILE:92:ef01", restoredPulsar.getConvertedCustomProperties().get("ssl.ca.location"));
    }

    @Test
    public void testEmptyManager() throws Exception {
        RoutineLoadMgr loaded = v3(new RoutineLoadMgr());
        assertTrue(names(loaded).isEmpty());
        assertEquals(0, global.getGlobalTransactionMgr().getCallbackFactory().getCallBackCnt());

    }

    @Test
    public void testInvalidEntryIsReportedAsImageErrorBeforeRegistration() throws Exception {
        RoutineLoadJobPB valid = kafka(1).toProto();
        List<RoutineLoadJobPB> invalid = List.of(valid.toBuilder().setKindValue(999).build(),
                valid.toBuilder().clearKafka().build(),
                valid.toBuilder().setCore(valid.getCore().toBuilder().setProgress(new PulsarProgress().toProto())).build(),
                valid.toBuilder().setCore(valid.getCore().toBuilder()
                        .setState(com.starrocks.persist.proto.RoutineLoadJobCorePB.State.STOPPED).setEndTimestamp(-1)).build());
        for (RoutineLoadJobPB entry : invalid) {
            RoutineLoadMgr loaded = new RoutineLoadMgr();
            Path file = LoadImageTestSupport.entries(tempDir, SectionId.ROUTINE_LOAD_MGR,
                    RoutineLoadMgrPB.getDefaultInstance(), List.of(entry));
            ImageFormatException exception = assertThrows(ImageFormatException.class,
                    () -> LoadImageTestSupport.load(file, loaded.getImageV3Loaders()));
            assertTrue(exception.getMessage().contains("ROUTINE_LOAD_MGR"));
            assertNull(loaded.getJob(1));
            assertTrue(names(loaded).isEmpty());
            assertEquals(0, global.getGlobalTransactionMgr().getCallbackFactory().getCallBackCnt());
        }
    }
}
