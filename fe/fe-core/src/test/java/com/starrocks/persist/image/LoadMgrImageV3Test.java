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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.load.BrokerFileGroupAggInfo;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.FailMsg;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.loadv2.LoadJobScheduler;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.LoadJobPB;
import com.starrocks.persist.proto.LoadMgrPB;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnStateCallbackFactory;
import com.starrocks.type.IntegerType;
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
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.LoadJobProtoTest.broker;
import static com.starrocks.persist.image.LoadJobProtoTest.insert;
import static com.starrocks.persist.image.LoadJobProtoTest.spark;
import static com.starrocks.persist.image.RoutineLoadMgrImageV3Test.callbackTransaction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoadMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
    }

    static LoadMgr manager(List<? extends LoadJob> jobs) {
        LoadMgr manager = new LoadMgr(new LoadJobScheduler());
        Map<Long, LoadJob> byId = getField(manager, "idToLoadJob");
        for (LoadJob job : jobs) {
            byId.put(job.getId(), job);
        }
        return manager;
    }

    private static Map<Long, Map<String, List<Long>>> labels(LoadMgr manager) {
        Map<Long, Map<String, List<LoadJob>>> source = getField(manager, "dbIdToLabelToLoadJobs");
        Map<Long, Map<String, List<Long>>> result = new HashMap<>();
        source.forEach((db, jobs) -> {
            Map<String, List<Long>> labels = new HashMap<>();
            jobs.forEach((label, list) -> labels.put(label, list.stream().map(LoadJob::getId).toList()));
            result.put(db, labels);
        });
        return result;
    }

    private LoadMgr v2(LoadMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.saveLoadJobsV2JsonFormat(image.getImageWriter());
        LoadMgr loaded = manager(List.of());
        LoadImageTestSupport.transactions(global);
        {
            SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
            loaded.loadLoadJobsV2JsonFormat(reader);
            reader.close();
        }
        return loaded;
    }

    private LoadMgr v3(LoadMgr source) throws Exception {
        Path file = LoadImageTestSupport.save(tempDir, source::save);
        LoadImageTestSupport.transactions(global);
        LoadMgr loaded = manager(List.of());
        List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
        assertEquals(120, sections.get(0).getType());
        return loaded;
    }

    private static List<LoadJob> queued(LoadMgr manager) {
        LoadJobScheduler scheduler = getField(manager, "loadJobScheduler");
        LinkedBlockingQueue<LoadJob> queue = getField(scheduler, "needScheduleJobs");
        return new ArrayList<>(queue);
    }

    @Test
    public void testAllStatesLabelHistoryAndCallbacks() throws Exception {
        List<LoadJob> jobs = new ArrayList<>();
        long id = 1;
        for (JobState state : JobState.values()) {
            LoadJob job = broker(id++, state);
            jobs.add(job);
        }
        LoadJob sameLabelOtherDatabase = insert(100, JobState.FINISHED);
        setField(sameLabelOtherDatabase, "dbId", 11L);
        jobs.add(sameLabelOtherDatabase);
        jobs.add(spark(101, JobState.ETL));
        LoadMgr source = manager(jobs);
        LoadMgr baseline = v2(source);
        LoadMgr loaded = v3(source);
        assertEquals(labels(baseline), labels(loaded));
        TxnStateCallbackFactory callbacks = global.getGlobalTransactionMgr().getCallbackFactory();
        long expectedCallbacks = jobs.stream().filter(job -> !job.isCompleted()).count();
        assertEquals(expectedCallbacks, callbacks.getCallBackCnt());
        for (LoadJob sourceJob : jobs) {
            LoadJob restored = loaded.getLoadJob(sourceJob.getId());
            GsonEquivalence.assertEquivalent(baseline.getLoadJob(sourceJob.getId()), restored);
            if (sourceJob.isCompleted()) {
                assertNull(callbacks.getCallback(sourceJob.getId()));
            } else {
                assertSame(restored, callbacks.getCallback(sourceJob.getId()));
            }
        }
        assertTrue(queued(loaded).isEmpty());

        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testSaveAndLoadRetentionKeepTheirSecondBoundary() throws Exception {
        long now = System.currentTimeMillis();
        int keep = Config.label_keep_max_second;
        Config.label_keep_max_second = 10;
        try {
            List<LoadJob> jobs = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                LoadJob job = insert(i + 1, JobState.FINISHED);
                setField(job, "finishTimestamp", i == 2 ? 0L : now + 60_000L);
                jobs.add(job);
            }
            LoadJob unfinished = broker(4, JobState.COMMITTED);
            setField(unfinished, "finishTimestamp", now - 1_000_000);
            jobs.add(unfinished);
            LoadJob unknown = insert(5, JobState.UNKNOWN);
            setField(unknown, "finishTimestamp", now - 1_000_000);
            jobs.add(unknown);
            LoadMgr source = manager(jobs);
            // Verify the existing integer-second boundary with an explicit clock value.
            LoadJob boundary = insert(99, JobState.FINISHED);
            setField(boundary, "finishTimestamp", now - 10_999);
            assertFalse((boolean) invoke(source, "isJobExpired", LoadJob.fromProto(boundary.toProto()), now));
            setField(boundary, "finishTimestamp", now - 11_000);
            assertTrue((boolean) invoke(source, "isJobExpired", LoadJob.fromProto(boundary.toProto()), now));
            LoadMgr baseline = v2(source);
            Path file = LoadImageTestSupport.save(tempDir, source::save);
            LoadMgr loaded = manager(List.of());
            LoadImageTestSupport.transactions(global);
            List<IndexEntryPB> sections = LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
            assertEquals(3, sections.get(0).getNumEntries());
            assertEquals(labels(baseline), labels(loaded));
            assertNotNull(loaded.getLoadJob(1));
            assertNotNull(loaded.getLoadJob(2));
            assertNull(loaded.getLoadJob(3));
            assertNotNull(loaded.getLoadJob(4));
            assertNull(loaded.getLoadJob(5));

            // An image that aged after save must still discard completed jobs on load.
            Path agedFile = LoadImageTestSupport.entries(tempDir, SectionId.LOAD_MGR, LoadMgrPB.getDefaultInstance(),
                    jobs.stream().map(LoadJob::toProto).toList());
            LoadMgr agedLoaded = manager(List.of());
            LoadImageTestSupport.load(agedFile, agedLoaded.getImageV3Loaders());
            assertNull(agedLoaded.getLoadJob(3));
            assertNull(agedLoaded.getLoadJob(5));
            assertNotNull(agedLoaded.getLoadJob(4));
        } finally {
            Config.label_keep_max_second = keep;
        }
    }

    @Test
    public void testCommittedJobReceivesVisibleReplayAfterLoad() throws Exception {
        LoadJob source = broker(1, JobState.COMMITTED);
        setField(source, "progress", 99);
        LoadMgr loaded = v3(manager(List.of(source)));
        LoadJob restored = loaded.getLoadJob(1);
        TxnStateCallbackFactory callbacks = global.getGlobalTransactionMgr().getCallbackFactory();
        assertSame(restored, callbacks.getCallback(1));
        TransactionState visible = callbackTransaction(1, 101, TransactionStatus.VISIBLE);
        visible.replaySetTransactionStatus();
        assertEquals(JobState.FINISHED, restored.getState());
        assertEquals(100, restored.getProgress());
        assertEquals(visible.getFinishTime(), (long) getField(restored, "finishTimestamp"));
        assertNull(callbacks.getCallback(1));
        assertNull(getField(restored, "failMsg"));

        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testLoadingJobReceivesCommitAttachmentThenVisible() throws Exception {
        for (LoadJob source : List.of(broker(1, JobState.LOADING), spark(2, JobState.LOADING))) {
            LoadMgr loaded = v3(manager(List.of(source)));
            LoadJob restored = loaded.getLoadJob(source.getId());
            EtlStatus status = new EtlStatus();
            status.setTrackingUrl("https://tracking/completed");
            status.getCounters().put("dpp.norm.ALL", "15");
            LoadJobFinalOperation operation = new LoadJobFinalOperation(source.getId(), status, 72, 300, 400,
                    JobState.LOADING, null);
            TransactionState commit = callbackTransaction(source.getId(), 101, TransactionStatus.COMMITTED);
            commit.setTxnCommitAttachment(operation);
            commit.replaySetTransactionStatus();
            assertEquals(JobState.COMMITTED, restored.getState());
            assertEquals(99, restored.getProgress());
            assertEquals(101L, (long) getField(restored, "transactionId"));
            EtlStatus restoredStatus = getField(restored, "loadingStatus");
            assertEquals("15", restoredStatus.getCounters().get("dpp.norm.ALL"));
            assertSame(restored, global.getGlobalTransactionMgr().getCallbackFactory().getCallback(source.getId()));
            TransactionState visible = callbackTransaction(source.getId(), 101, TransactionStatus.VISIBLE);
            visible.setTxnCommitAttachment(operation);
            visible.replaySetTransactionStatus();
            assertEquals(JobState.FINISHED, restored.getState());
            assertEquals(100, restored.getProgress());
            assertNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(source.getId()));
        }
    }

    @Test
    public void testAbortReplayUpdatesFailureAndRemovesCallback() throws Exception {
        for (LoadJob source : List.of(broker(1, JobState.LOADING), spark(2, JobState.ETL))) {
            LoadMgr loaded = v3(manager(List.of(source)));
            LoadJob restored = loaded.getLoadJob(source.getId());
            TransactionState aborted = callbackTransaction(source.getId(), 101, TransactionStatus.ABORTED);
            setField(aborted, "reason", "parse error");
            aborted.replaySetTransactionStatus();
            assertEquals(JobState.CANCELLED, restored.getState());
            FailMsg failure = getField(restored, "failMsg");
            assertEquals("parse error", failure.getMsg());
            assertEquals(FailMsg.CancelType.LOAD_RUN_FAIL, failure.getCancelType());
            assertEquals(aborted.getFinishTime(), (long) getField(restored, "finishTimestamp"));
            assertNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(source.getId()));
        }
    }

    @Test
    public void testRetryableBrokerAbortRetainsCallback() throws Exception {
        LoadMgr loaded = v3(manager(List.of(broker(1, JobState.LOADING))));
        LoadJob restored = loaded.getLoadJob(1);
        int retries = getField(restored, "retryTime");
        TransactionState aborted = callbackTransaction(1, 101, TransactionStatus.ABORTED);
        setField(aborted, "reason", "backend unavailable");
        aborted.replaySetTransactionStatus();
        assertEquals(JobState.CANCELLED, restored.getState());
        assertEquals(retries - 1, (int) getField(restored, "retryTime"));
        assertSame(restored, global.getGlobalTransactionMgr().getCallbackFactory().getCallback(1));
    }

    @Test
    public void testInsertUsesResidualCancellationInsteadOfTransactionReplay() throws Exception {
        LoadJob source = insert(1, JobState.LOADING);
        setField(source, "transactionId", 100L);
        Mockito.when(global.getLocalMetastore()).thenReturn(Mockito.mock(LocalMetastore.class));
        EditLog editLog = global.getEditLog();
        Mockito.doAnswer(call -> {
            WALApplier apply = call.getArgument(1);
            apply.apply(call.getArgument(0));
            return null;
        }).when(editLog).logEndLoadJob(Mockito.any(), Mockito.any());
        LoadMgr loaded = v3(manager(List.of(source)));
        LoadJob restored = loaded.getLoadJob(1);
        for (TransactionStatus status : List.of(TransactionStatus.COMMITTED,
                TransactionStatus.VISIBLE, TransactionStatus.ABORTED)) {
            callbackTransaction(1, 100, status).replaySetTransactionStatus();
            assertEquals(JobState.LOADING, restored.getState());
        }
        global.getGlobalTransactionMgr().addDatabaseTransactionMgr(10L);
        // No transaction with this label exists after restore.
        loaded.endResidualInsertJobs();
        assertEquals(JobState.CANCELLED, restored.getState());
        assertEquals("Cancelled since transaction status unknown", ((FailMsg) getField(restored, "failMsg")).getMsg());
        assertTrue(restored.getFinishTimestamp() > 0);
        assertNull(global.getGlobalTransactionMgr().getCallbackFactory().getCallback(1));

        Mockito.verify(editLog, Mockito.times(1)).logEndLoadJob(Mockito.any(), Mockito.any());
    }

    @Test
    public void testPrepareRebuildsOnlyPendingFileGroupsAndThenQueuesJobs() throws Exception {
        LocalMetastore metastore = Mockito.mock(LocalMetastore.class);
        Mockito.when(global.getLocalMetastore()).thenReturn(metastore);
        Mockito.when(global.getLockManager()).thenReturn(new LockManager());
        Database db = new Database(10, "db");
        List<Column> columns = List.of(new Column("k", IntegerType.INT));
        OlapTable table = new OlapTable(20, "tbl", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new HashDistributionInfo(1, columns));
        Mockito.when(metastore.getDb(10)).thenReturn(db);
        Mockito.when(metastore.getTable("db", "tbl")).thenReturn(table);
        List<LoadJob> jobs = List.of(broker(1, JobState.PENDING), spark(2, JobState.PENDING),
                broker(3, JobState.LOADING), spark(4, JobState.ETL));
        for (LoadJob job : jobs) {
            setField(job, "originStmt", new OriginStatementInfo("LOAD LABEL db.label "
                    + "(DATA INFILE('s3://bucket/file.csv') INTO TABLE tbl FORMAT AS 'csv' (k)) "
                    + "WITH BROKER 'broker'", 0));
            setField(job, "sessionVariables", new HashMap<>(Map.of(SessionVariable.SQL_MODE, "0")));
        }
        LoadMgr loaded = v3(manager(jobs));
        for (LoadJob job : jobs) {
            BrokerFileGroupAggInfo info = getField(loaded.getLoadJob(job.getId()), "fileGroupAggInfo");
            assertTrue(info.getAllTableIds().isEmpty());
        }
        assertTrue(queued(loaded).isEmpty());
        loaded.prepareJobs();
        assertEquals(Set.of(1L, 2L), queued(loaded).stream().map(LoadJob::getId).collect(java.util.stream.Collectors.toSet()));
        for (long id : List.of(1L, 2L)) {
            LoadJob restored = loaded.getLoadJob(id);
            assertEquals(JobState.PENDING, restored.getState());
            BrokerFileGroupAggInfo info = getField(restored, "fileGroupAggInfo");
            assertEquals(Set.of(20L), info.getAllTableIds());
        }
        for (long id : List.of(3L, 4L)) {
            BrokerFileGroupAggInfo info = getField(loaded.getLoadJob(id), "fileGroupAggInfo");
            assertTrue(info.getAllTableIds().isEmpty());
        }
        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testEmptyManagerAndInvalidEntries() throws Exception {
        LoadMgr empty = v3(manager(List.of()));
        assertTrue(labels(empty).isEmpty());
        assertTrue(queued(empty).isEmpty());

        LoadJobPB valid = broker(1, JobState.PENDING).toProto();
        for (LoadJobPB entry : List.of(valid.toBuilder().setKindValue(999).build(),
                valid.toBuilder().clearBroker().build(),
                valid.toBuilder().setCore(valid.getCore().toBuilder().setStateValue(999)).build())) {
            LoadMgr loaded = manager(List.of());
            Path file = LoadImageTestSupport.entries(tempDir, SectionId.LOAD_MGR, LoadMgrPB.getDefaultInstance(), List.of(entry));
            ImageFormatException exception = assertThrows(ImageFormatException.class,
                    () -> LoadImageTestSupport.load(file, loaded.getImageV3Loaders()));
            assertTrue(exception.getMessage().contains("LOAD_MGR"));
            assertNull(loaded.getLoadJob(1));
            assertTrue(labels(loaded).isEmpty());
            assertEquals(0, global.getGlobalTransactionMgr().getCallbackFactory().getCallBackCnt());
        }
    }
}
