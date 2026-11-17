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

package com.starrocks.alter.reshard;

import com.google.common.collect.Iterables;
import com.starrocks.alter.ConcurrentLoad;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.EditLogDeserializer;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Checks, for split and merge alike, what a reshard job journals when it makes its new layout visible.
 *
 * <p>The journal is recorded at the EditLog methods that write reshard-job and transaction records, through the
 * journal's codec, in the order the writes commit. UtFrameUtils.PseudoJournalReplayer cannot stand in for that here:
 * it swaps the EditLog, and a database's transaction manager keeps the EditLog it was created with, so the transaction
 * records of a test that did not create its database under the pseudo journal never reach it.
 *
 * <p>The test runs as the leader in this process, so the follower is emulated for the one table that matters: a copy of
 * the table taken when the follower would have replayed up to that point is swapped into the database while the
 * records written after it are replayed.
 */
final class ReshardJournalReplay {
    private final List<JournalEntity> journal = new CopyOnWriteArrayList<>();

    /**
     * Starts recording; beforeFirstCleaningRecord runs once, on the writing thread, just before the first CLEANING
     * record of a reshard job is journaled.
     */
    private ReshardJournalReplay(Runnable beforeFirstCleaningRecord) {
        AtomicBoolean cleaningSeen = new AtomicBoolean();
        new MockUp<EditLog>() {
            @Mock
            public void logUpdateTabletReshardJob(Invocation invocation, TabletReshardJob job) {
                recordJob(invocation, job);
            }

            @Mock
            public void logUpdateTabletReshardJob(Invocation invocation, TabletReshardJob job, WALApplier applier) {
                recordJob(invocation, job);
            }

            @Mock
            public void logInsertTransactionState(Invocation invocation, TransactionState state) {
                record(invocation, OperationType.OP_UPSERT_TRANSACTION_STATE_V2, state);
            }

            private void recordJob(Invocation invocation, TabletReshardJob job) {
                if (job.getJobState() == TabletReshardJob.JobState.CLEANING && cleaningSeen.compareAndSet(false, true)) {
                    beforeFirstCleaningRecord.run();
                }
                record(invocation, OperationType.OP_UPDATE_TABLET_RESHARD_JOB_LOG, job);
            }

            private void record(Invocation invocation, short opCode, Object payload) {
                byte[] bytes = serialize(payload);
                invocation.proceed();
                journal.add(deserialize(opCode, bytes));
            }
        };
    }

    /**
     * A load committed at the version right above a reshard job's sees the partition reach the job's version as soon as
     * the job installs its new layout, and its publish, already done on the compute nodes, finishes then. The job's
     * CLEANING record is what makes that version visible on replay, so it must reach the journal before the load's
     * VISIBLE record; otherwise every FE replaying the journal fails on the load.
     *
     * <p>Begins a load, runs the job (not yet admitted, on a table with one physical partition) to RUNNING, commits the
     * load above the job's version, and has the load try to become visible, exactly as the publish daemon would, at the
     * moment the job writes its CLEANING record. Then replays, onto a follower's copy of the table taken at RUNNING,
     * every record the job and the load wrote, in journal order, checking the follower after each one.
     */
    static void checkCleaningJournaledBeforeNextLoadIsVisible(Database db, OlapTable table, TabletReshardJob job)
            throws Exception {
        // Replaying puts the job's records into TabletReshardJobMgr, whose daemon would run them as the leader's.
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            protected void runAfterCatalogReady() {
            }
        };
        ConcurrentLoad.holdPublishDaemon();
        long loadId = ConcurrentLoad.begin(db, table);
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        long partitionId = partition.getId();
        long commitVersion = partition.getVisibleVersion() + 1;
        job.init();
        job.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, job.getJobState());
        ReshardingPhysicalPartition reshardingPartition = reshardingPartitions(job).get(partitionId);
        Assertions.assertEquals(commitVersion, reshardingPartition.getCommitVersion());
        long newBaseIndexId = reshardingPartition.getReshardingIndexes().values().iterator().next()
                .getMaterializedIndex().getId();

        OlapTable followerTable = copyTable(table);
        AtomicBoolean visibleBeforeCleaning = new AtomicBoolean();
        ReshardJournalReplay recorder = new ReshardJournalReplay(() -> visibleBeforeCleaning.set(
                ConcurrentLoad.tryMakeVisible(db, table, transactionState(db, loadId), 100)));
        TransactionState load = ConcurrentLoad.commit(db, table, loadId);
        Assertions.assertEquals(commitVersion + 1, ConcurrentLoad.versionIn(load, table, partitionId));

        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (job.getJobState() == TabletReshardJob.JobState.RUNNING) {
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs, "job is still RUNNING");
            job.run();
            Thread.sleep(10);
        }
        if (!visibleBeforeCleaning.get()) {
            ConcurrentLoad.makeVisible(db, load);
        }
        while (job.getJobState() != TabletReshardJob.JobState.FINISHED) {
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }

        List<JournalEntity> records = recorder.recordsOf(job.getJobId(), loadId);
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        try {
            withFollowerTable(db, followerTable, () -> {
                PhysicalPartition followerPartition = followerTable.getPhysicalPartition(partitionId);
                for (JournalEntity record : records) {
                    replay(record);
                    if (isJobRecord(record, TabletReshardJob.JobState.CLEANING)) {
                        Assertions.assertEquals(commitVersion, followerPartition.getVisibleVersion());
                        Assertions.assertEquals(((TabletReshardJob) record.data()).stateStartedTimeMs,
                                followerPartition.getVisibleVersionTime());
                        Assertions.assertEquals(newBaseIndexId, followerPartition.getLatestBaseIndex().getId());
                    } else if (isTxnRecord(record, TransactionStatus.VISIBLE)) {
                        Assertions.assertEquals(commitVersion + 1, followerPartition.getVisibleVersion());
                    }
                }
                Assertions.assertEquals(commitVersion + 1, followerPartition.getVisibleVersion());
                Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, followerTable.getState());
            });
        } finally {
            // The replay registered the job's records in the shared manager; the leader's job never was.
            if (tabletReshardJobMgr.getTabletReshardJob(job.getJobId()) != null) {
                tabletReshardJobMgr.replayRemoveTabletReshardJob(job.getJobId());
            }
        }

        int cleaning = Iterables.indexOf(records, r -> isJobRecord(r, TabletReshardJob.JobState.CLEANING));
        int visible = Iterables.indexOf(records, r -> isTxnRecord(r, TransactionStatus.VISIBLE));
        Assertions.assertTrue(cleaning >= 0 && cleaning < visible,
                "the CLEANING record must precede the load's VISIBLE record: " + records);
    }

    // On this branch each job type, not TabletReshardJob, exposes its resharding partitions.
    private static Map<Long, ReshardingPhysicalPartition> reshardingPartitions(TabletReshardJob job) {
        return job instanceof SplitTabletJob splitJob ? splitJob.getReshardingPhysicalPartitions()
                : ((MergeTabletJob) job).getReshardingPhysicalPartitions();
    }

    /** Counts the CLEANING records of reshard jobs journaled from now on. */
    static AtomicInteger countCleaningRecords() {
        AtomicInteger written = new AtomicInteger();
        new MockUp<EditLog>() {
            @Mock
            public void logUpdateTabletReshardJob(Invocation invocation, TabletReshardJob job, WALApplier applier) {
                invocation.proceed();
                if (job.getJobState() == TabletReshardJob.JobState.CLEANING) {
                    written.incrementAndGet();
                }
            }
        };
        return written;
    }

    // The records of the given reshard job and the given transaction journaled since recording started, in order.
    private List<JournalEntity> recordsOf(long jobId, long txnId) {
        List<JournalEntity> kept = new ArrayList<>();
        for (JournalEntity entity : journal) {
            if (entity.opCode() == OperationType.OP_UPDATE_TABLET_RESHARD_JOB_LOG
                    && ((TabletReshardJob) entity.data()).getJobId() == jobId) {
                kept.add(entity);
            } else if (entity.opCode() == OperationType.OP_UPSERT_TRANSACTION_STATE_V2
                    && ((TransactionState) entity.data()).getTransactionId() == txnId) {
                kept.add(entity);
            }
        }
        return kept;
    }

    // The table as a follower that has replayed everything journaled so far holds it.
    private static OlapTable copyTable(OlapTable table) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), table.getClass());
    }

    // Replays one record through the follower's entry point; a record that cannot be applied throws there.
    private static void replay(JournalEntity entity) throws Exception {
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(), entity);
    }

    // Puts the follower's copy of the table in place of the leader's for the duration of the action.
    private static void withFollowerTable(Database db, OlapTable followerTable, ThrowingRunnable action)
            throws Exception {
        OlapTable leaderTable = (OlapTable) db.getTable(followerTable.getId());
        db.dropTable(leaderTable.getId());
        db.registerTableUnlocked(followerTable);
        try {
            action.run();
        } finally {
            db.dropTable(followerTable.getId());
            db.registerTableUnlocked(leaderTable);
        }
    }

    private static TransactionState transactionState(Database db, long txnId) {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionState(db.getId(), txnId);
    }

    private static boolean isJobRecord(JournalEntity entity, TabletReshardJob.JobState state) {
        return entity.opCode() == OperationType.OP_UPDATE_TABLET_RESHARD_JOB_LOG
                && ((TabletReshardJob) entity.data()).getJobState() == state;
    }

    private static boolean isTxnRecord(JournalEntity entity, TransactionStatus status) {
        return entity.opCode() == OperationType.OP_UPSERT_TRANSACTION_STATE_V2
                && ((TransactionState) entity.data()).getTransactionStatus() == status;
    }

    // What EditLog.logJsonObject writes for the object.
    private static byte[] serialize(Object object) {
        try (DataOutputBuffer buffer = new DataOutputBuffer(1024)) {
            Text.writeString(buffer, GsonUtils.GSON.toJson(object));
            return Arrays.copyOf(buffer.getData(), buffer.getLength());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // What a replaying FE reads back from the record.
    private static JournalEntity deserialize(short opCode, byte[] payload) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            return new JournalEntity(opCode, EditLogDeserializer.deserialize(opCode, in));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
