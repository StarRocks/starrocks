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

import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnStateCallbackFactory;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.ColocateTableIndexImageV3Test.group;
import static com.starrocks.persist.image.LoadJobProtoTest.broker;
import static com.starrocks.persist.image.RoutineLoadJobProtoTest.kafka;
import static com.starrocks.persist.image.RoutineLoadMgrImageV3Test.attachment;
import static com.starrocks.persist.image.RoutineLoadMgrImageV3Test.callbackTransaction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Reads the consecutive v2 save-order sections and then replays transactions newer than the image. */
public class LoadManagersRecoveryImageV3Test {
    @TempDir
    Path tempDir;

    @Test
    public void testImageStateIsNotReappliedThroughNewlyRegisteredCallbacks() throws Exception {
        GlobalStateMgr global = LoadImageTestSupport.global();
        Mockito.when(global.getStreamLoadMgr()).thenReturn(Mockito.mock(StreamLoadMgr.class));
        LocalMetastore metastore = Mockito.mock(LocalMetastore.class);
        Mockito.when(global.getLocalMetastore()).thenReturn(metastore);
        Database db = new Database(10, "db");
        Mockito.when(metastore.getDbIncludeRecycleBin(10)).thenReturn(db);
        Mockito.when(metastore.getTableIncludeRecycleBin(db, 20)).thenReturn(new OlapTable());
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_NOTHING;
            }
        };
        GlobalTransactionMgr liveTransactions = global.getGlobalTransactionMgr();
        liveTransactions.addDatabaseTransactionMgr(10L);
        RoutineLoadJob routineJob = kafka(201);
        setField(routineJob, "totalRows", 100L);
        setField(routineJob, "committedTaskNum", 10L);
        setField(routineJob, "progress", new KafkaProgress(new java.util.concurrent.ConcurrentHashMap<>(Map.of(1, 10L))));
        RoutineLoadMgr liveRoutine = RoutineLoadMgrImageV3Test.manager(List.of(routineJob));
        LoadJob loadJob = broker(301, JobState.COMMITTED);
        setField(loadJob, "progress", 99);
        LoadMgr liveLoad = LoadMgrImageV3Test.manager(List.of(loadJob));
        ColocateTableIndex liveColocate = new ColocateTableIndex();
        GroupId groupId = new GroupId(10, 100);
        group(liveColocate, groupId, "10_group", DistributionInfoType.HASH, 20);
        TransactionState routineTxn = callbackTransaction(201, 401, TransactionStatus.COMMITTED);
        routineTxn.setTxnCommitAttachment(attachment(true));
        TransactionState loadTxn = callbackTransaction(301, 501, TransactionStatus.COMMITTED);
        for (TransactionState txn : List.of(routineTxn, loadTxn)) {
            invoke(liveTransactions.getDatabaseTransactionMgr(10), "unprotectUpsertTransactionState", txn);
        }
        Path file = LoadImageTestSupport.save(tempDir, writer -> {
            liveTransactions.save(writer);
            liveColocate.save(writer);
            liveRoutine.save(writer);
            liveLoad.save(writer);
        });
        GlobalTransactionMgr restoredTransactions = LoadImageTestSupport.transactions(global);
        restoredTransactions.addDatabaseTransactionMgr(10L);
        ColocateTableIndex restoredColocate = new ColocateTableIndex();
        RoutineLoadMgr restoredRoutine = new RoutineLoadMgr();
        LoadMgr restoredLoad = LoadMgrImageV3Test.manager(List.of());
        List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>(restoredTransactions.getImageV3Loaders());
        loaders.addAll(restoredColocate.getImageV3Loaders());
        loaders.addAll(restoredRoutine.getImageV3Loaders());
        loaders.addAll(restoredLoad.getImageV3Loaders());
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaders);
        assertEquals(List.of(90, 100, 110, 120),
                sections.stream().map(IndexEntryPB::getType).toList());
        assertEquals(groupId, restoredColocate.getGroup(20));
        assertNotNull(restoredTransactions.getTransactionState(10, 401));
        assertNotNull(restoredTransactions.getTransactionState(10, 501));
        RoutineLoadJob restoredRoutineJob = restoredRoutine.getJob(201);
        LoadJob restoredLoadJob = restoredLoad.getLoadJob(301);
        TxnStateCallbackFactory callbacks = restoredTransactions.getCallbackFactory();
        assertEquals(2, callbacks.getCallBackCnt());
        assertSame(restoredRoutineJob, callbacks.getCallback(201));
        assertSame(restoredLoadJob, callbacks.getCallback(301));
        // The image already includes job statistics for transactions in the image.
        assertEquals(100L, (long) getField(restoredRoutineJob, "totalRows"));
        assertEquals(10L, (long) getField(restoredRoutineJob, "committedTaskNum"));
        assertEquals(Map.of(1, 10L), ((KafkaProgress) restoredRoutineJob.getProgress()).getPartitionIdToOffset(List.of(1)));
        assertEquals(JobState.COMMITTED, restoredLoadJob.getState());
        assertEquals(99, restoredLoadJob.getProgress());
        Mockito.verifyNoInteractions(global.getStreamLoadMgr());
        TransactionState newRoutineTxn = callbackTransaction(201, 402, TransactionStatus.COMMITTED);
        newRoutineTxn.setTxnCommitAttachment(attachment(true));
        newRoutineTxn.replaySetTransactionStatus();
        assertEquals(116L, (long) getField(restoredRoutineJob, "totalRows"));
        assertEquals(11L, (long) getField(restoredRoutineJob, "committedTaskNum"));
        assertEquals(Map.of(1, 20L), ((KafkaProgress) restoredRoutineJob.getProgress()).getPartitionIdToOffset(List.of(1)));
        TransactionState visible = callbackTransaction(301, 501, TransactionStatus.VISIBLE);
        visible.replaySetTransactionStatus();
        assertEquals(JobState.FINISHED, restoredLoadJob.getState());
        assertEquals(100, restoredLoadJob.getProgress());
        assertNull(callbacks.getCallback(301));
        assertSame(restoredRoutineJob, callbacks.getCallback(201));
        Mockito.clearInvocations(global.getStreamLoadMgr());

        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testV2AndV3HaveSameCallbackEffectsAfterRecovery() throws Exception {
        GlobalStateMgr global = LoadImageTestSupport.global();
        Mockito.when(global.getStreamLoadMgr()).thenReturn(Mockito.mock(StreamLoadMgr.class));
        RoutineLoadMgr routineSource = RoutineLoadMgrImageV3Test.manager(List.of(kafka(1)));
        LoadMgr loadSource = LoadMgrImageV3Test.manager(List.of(broker(2, JobState.LOADING)));
        RoutineLoadJob v2Routine;
        LoadJob v2Load;
        UtFrameUtils.PseudoImage routineImage = new UtFrameUtils.PseudoImage();
        routineSource.saveRoutineLoadJobsV2(routineImage.getImageWriter());
        UtFrameUtils.PseudoImage loadImage = new UtFrameUtils.PseudoImage();
        loadSource.saveLoadJobsV2JsonFormat(loadImage.getImageWriter());
        RoutineLoadMgr oldRoutine = new RoutineLoadMgr();
        LoadMgr oldLoad = LoadMgrImageV3Test.manager(List.of());
        {
            SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(routineImage.getJsonReader());
            oldRoutine.loadRoutineLoadJobsV2(reader);
            reader.close();
        }
        {
            SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(loadImage.getJsonReader());
            oldLoad.loadLoadJobsV2JsonFormat(reader);
            reader.close();
        }
        TransactionState routineCommit = callbackTransaction(1, 101, TransactionStatus.COMMITTED);
        routineCommit.setTxnCommitAttachment(attachment(true));
        TransactionState loadAbort = callbackTransaction(2, 102, TransactionStatus.ABORTED);
        setField(loadAbort, "reason", "parse error");
        routineCommit.replaySetTransactionStatus();
        loadAbort.replaySetTransactionStatus();
        v2Routine = oldRoutine.getJob(1);
        v2Load = oldLoad.getLoadJob(2);
        LoadImageTestSupport.transactions(global);
        Path file = LoadImageTestSupport.save(tempDir, writer -> {
            routineSource.save(writer);
            loadSource.save(writer);
        });
        RoutineLoadMgr restoredRoutine = new RoutineLoadMgr();
        LoadMgr restoredLoad = LoadMgrImageV3Test.manager(List.of());
        List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>(restoredRoutine.getImageV3Loaders());
        loaders.addAll(restoredLoad.getImageV3Loaders());
        LoadImageTestSupport.load(file, loaders);
        routineCommit.replaySetTransactionStatus();
        loadAbort.replaySetTransactionStatus();
        GsonEquivalence.assertEquivalent(v2Routine, restoredRoutine.getJob(1));
        GsonEquivalence.assertEquivalent(v2Load, restoredLoad.getLoadJob(2));
        assertEquals(1, global.getGlobalTransactionMgr().getCallbackFactory().getCallBackCnt());
        assertNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(2));

    }
    @Test
    public void testImageAndJournalReplayDoNotIncrementLeaderMetrics() throws Exception {
        GlobalStateMgr global = LoadImageTestSupport.global();
        Mockito.when(global.getStreamLoadMgr()).thenReturn(Mockito.mock(StreamLoadMgr.class));
        boolean wasInitialized = MetricRepo.hasInit;
        Map<Field, Object> originalCounters = new LinkedHashMap<>();
        List<LongCounterMetric> counters = new ArrayList<>();
        try {
            for (String name : List.of("COUNTER_LOAD_ADD", "COUNTER_LOAD_FINISHED", "COUNTER_ROUTINE_LOAD_ROWS",
                    "COUNTER_ROUTINE_LOAD_RECEIVED_BYTES", "COUNTER_ROUTINE_LOAD_ERROR_ROWS", "COUNTER_ROUTINE_LOAD_PAUSED")) {
                Field field = MetricRepo.class.getField(name);
                originalCounters.put(field, field.get(null));
                LongCounterMetric counter = new LongCounterMetric(name, Metric.MetricUnit.NOUNIT, "restore test");
                counters.add(counter);
                field.set(null, counter);
            }
            MetricRepo.hasInit = true;
            RoutineLoadJob active = kafka(1);
            RoutineLoadJob paused = RoutineLoadJobProtoTest.pulsar(2);
            setField(paused, "state", RoutineLoadJob.JobState.PAUSED);
            RoutineLoadMgr routine = RoutineLoadMgrImageV3Test.manager(List.of(active, paused));
            LoadMgr load = LoadMgrImageV3Test.manager(List.of(LoadJobProtoTest.broker(3, JobState.COMMITTED)));
            Path file = LoadImageTestSupport.save(tempDir, writer -> {
                routine.save(writer);
                load.save(writer);
            });
            RoutineLoadMgr restoredRoutine = RoutineLoadMgrImageV3Test.manager(List.of());
            LoadMgr restoredLoad = LoadMgrImageV3Test.manager(List.of());
            List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>(restoredRoutine.getImageV3Loaders());
            loaders.addAll(restoredLoad.getImageV3Loaders());
            LoadImageTestSupport.transactions(global);
            LoadImageTestSupport.load(file, loaders);
            for (LongCounterMetric counter : counters) {
                assertEquals(0L, counter.getValue());
            }
            TransactionState commit = callbackTransaction(1, 101, TransactionStatus.COMMITTED);
            commit.setTxnCommitAttachment(attachment(true));
            commit.replaySetTransactionStatus();
            callbackTransaction(3, 103, TransactionStatus.VISIBLE).replaySetTransactionStatus();
            assertEquals(16L, (long) getField(restoredRoutine.getJob(1), "totalRows"));
            assertEquals(JobState.FINISHED, restoredLoad.getLoadJob(3).getState());
            for (LongCounterMetric counter : counters) {
                assertEquals(0L, counter.getValue());
            }
            Mockito.verifyNoInteractions(global.getEditLog());
        } finally {
            MetricRepo.hasInit = wasInitialized;
            for (Map.Entry<Field, Object> entry : originalCounters.entrySet()) {
                entry.getKey().set(null, entry.getValue());
            }
        }
    }

}
