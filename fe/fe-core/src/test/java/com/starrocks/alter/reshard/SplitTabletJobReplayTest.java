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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.Config;
import com.starrocks.common.lock.LockTestUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.ReshardingTabletInfoPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.DatabaseTransactionMgr;
import com.starrocks.transaction.LakeTableTxnStateListener;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What a follower knows about a split job lives in two independent places: the job object, which
 * every journal entry replaces wholesale, and {@link TabletReshardJobMgr}'s resharding-tablet
 * registry, which only replayPreparingJob ever writes. A decision the leader takes after PREPARING
 * therefore reaches the job but not the registry -- and the identical fallback, taken when the BE
 * cannot split the tablet, is such a decision. Once that follower is promoted it builds every cross
 * publish from the registry, so a stale entry makes it ask the BE forever for a child tablet the
 * leader already dropped and no BE ever wrote metadata for, freezing the partition's version.
 */
public class SplitTabletJobReplayTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("test").useDatabase("test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        // Run the publish the reshard daemon submits synchronously, so a job driven by run() observes
        // its own publish result in the next cycle -- but on a thread of its own, which is what the
        // helper is for. Running it inline would hand the publish the two metadata locks the caller
        // holds, and SplitTabletJob.publishVersion would then contact the BEs under a lock it never
        // has in production, where that publish goes to publishThreadPool.
        LockTestUtils.fakeSynchronousExecutorOffTheCallersThread();

        // Silence the reshard daemon. It is a started leader-only daemon on a 10 ms tick
        // (Config.tablet_reshard_job_scheduler_interval_ms) that runs every non-final job in
        // TabletReshardJobMgr's map -- and these tests are the only ones that put jobs in that map,
        // via replayUpdateTabletReshardJob. Left running, it drives a replayed CLEANING job to
        // FINISHED between two of the replay steps below, unregistering the very registry entry the
        // prune reads, which makes testReplayDropsDiscardedNewTabletFromInvertedIndex fail about one
        // run in ten. The replay path under test is driven directly here; the scheduler has no part
        // in it.
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
    }

    @Test
    public void testReplayRefreshesReshardingTabletRegistryAfterIdenticalFallback() throws Exception {
        FollowerReplay replay = replayIdenticalFallbackAsFollower("replay_registry_table");

        ReshardingTablet registered = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr()
                .getReshardingTablet(replay.oldTabletId(), replay.commitVersion());
        Assertions.assertNotNull(registered);
        Assertions.assertEquals(List.of(replay.keptTabletId()), registered.getNewTabletIds());

        ReshardingTabletInfoPB crossPublishInfo = registered.toProto();
        Assertions.assertNull(crossPublishInfo.splittingTabletInfo);
        Assertions.assertNotNull(crossPublishInfo.identicalTabletInfo);
        Assertions.assertEquals(replay.keptTabletId(), crossPublishInfo.identicalTabletInfo.newTabletId);
    }

    /**
     * replayPreparingJob adds every preallocated new tablet to the inverted index. The ones the
     * identical fallback discards never reach any materialized index, so unless the replay drops
     * them again they stay there resolving to an index that does not hold them.
     */
    @Test
    public void testReplayDropsDiscardedNewTabletFromInvertedIndex() throws Exception {
        FollowerReplay replay = replayIdenticalFallbackAsFollower("replay_inverted_index_table");

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        Assertions.assertNotNull(invertedIndex.getTabletMeta(replay.keptTabletId()));
        Assertions.assertNull(invertedIndex.getTabletMeta(replay.discardedTabletId()));
    }

    /**
     * The refresh belongs to the replay seam, not to one state: it must already have taken effect on
     * the RUNNING entry, otherwise a fix that only worked for CLEANING would look correct.
     */
    @Test
    public void testAnyReplayedRecordRefreshesTheRegistry() throws Exception {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();

        SplitTabletJob origin = createSplitJob(createTable("replay_any_state_table"));
        origin.createShardsOnStarOS();
        tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(origin));

        origin.setJobState(TabletReshardJob.JobState.PREPARING);
        TabletReshardJob preparingRecord = journalCopy(origin);
        tabletReshardJobMgr.replayUpdateTabletReshardJob(preparingRecord);

        long commitVersion = firstReshardingPartition(preparingRecord).getCommitVersion();
        long oldTabletId = firstSplittingTablet(preparingRecord).getOldTabletId();
        long keptTabletId = firstSplittingTablet(preparingRecord).getNewTabletIds().get(0);

        TabletReshardJob leaderView = journalCopy(preparingRecord);
        firstSplittingTablet(leaderView).fallbackToIdenticalTablet();
        leaderView.setJobState(TabletReshardJob.JobState.RUNNING);
        tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(leaderView));

        Assertions.assertEquals(List.of(keptTabletId), tabletReshardJobMgr
                .getReshardingTablet(oldTabletId, commitVersion).getNewTabletIds());
    }

    /**
     * abort() only accepts PENDING, which is before a commit version has been reserved, so an
     * ABORTING record carries commit version 0 -- and getReshardingTablet() answers every version at
     * or above the registered one. Registering that record would redirect every later publish on the
     * old tablets into new tablets the job is in the middle of throwing away.
     */
    @Test
    public void testReplayDoesNotRedirectPublishForAJobAbortedBeforeReservingAVersion() throws Exception {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();

        SplitTabletJob origin = createSplitJob(createTable("replay_aborting_table"));
        long oldTabletId = firstSplittingTablet(origin).getOldTabletId();
        Assertions.assertTrue(origin.abort("aborted before reserving a version"));
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTING, origin.getJobState());
        Assertions.assertEquals(0, firstReshardingPartition(origin).getCommitVersion());

        tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(origin));

        Assertions.assertNull(tabletReshardJobMgr.getReshardingTablet(oldTabletId, 1));
    }

    /**
     * replayAbortedJob() unregisters the family, and the refresh that now follows every replayed
     * entry must not put it back.
     *
     * <p>No guard this code has ever shipped admits a final state, so this fails against neither
     * the old guard nor the new one -- it is an invariant test, not a regression test for a bug that
     * happened. What it pins is that the refresh cannot resurrect an unregistration, which is the
     * failure mode the seam introduces and which only the guard prevents.
     */
    @Test
    public void testReplayOfAnAbortedRecordLeavesNothingRegistered() throws Exception {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();

        SplitTabletJob origin = createSplitJob(createTable("replay_aborted_table"));
        origin.createShardsOnStarOS();
        tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(origin));

        origin.setJobState(TabletReshardJob.JobState.PREPARING);
        TabletReshardJob preparingRecord = journalCopy(origin);
        tabletReshardJobMgr.replayUpdateTabletReshardJob(preparingRecord);

        long commitVersion = firstReshardingPartition(preparingRecord).getCommitVersion();
        long oldTabletId = firstSplittingTablet(preparingRecord).getOldTabletId();
        Assertions.assertNotNull(tabletReshardJobMgr.getReshardingTablet(oldTabletId, commitVersion));

        TabletReshardJob abortedRecord = journalCopy(preparingRecord);
        abortedRecord.setJobState(TabletReshardJob.JobState.ABORTED);
        tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(abortedRecord));

        Assertions.assertNull(tabletReshardJobMgr.getReshardingTablet(oldTabletId, commitVersion));
    }

    /**
     * Pins the two post-conditions the replay tests above reproduce by hand. A single-process test
     * cannot host a leader and a follower over one catalog, so those tests apply the leader's
     * mutations directly; this one drives the real state machine and asserts they are the same two.
     */
    @Test
    public void testLeaderIdenticalFallbackShrinksTheSplitFamily() throws Exception {
        OlapTable table = createTable("leader_fallback_table");
        installIdenticalFallbackLakeServiceMock();

        SplitTabletJob job = createSplitJob(table);
        SplittingTablet family = firstSplittingTablet(job);
        long keptTabletId = family.getNewTabletIds().get(0);
        long discardedTabletId = family.getNewTabletIds().get(1);
        MaterializedIndex newIndex = firstReshardingIndex(job).getMaterializedIndex();

        job.init();
        job.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, job.getJobState());
        job.run();
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, job.getJobState());

        Assertions.assertEquals(List.of(keptTabletId), family.getNewTabletIds());
        Assertions.assertNull(newIndex.getTablet(discardedTabletId));
        // Leader-side only, and MaterializedIndex#removeTablet is what does it -- asserted here so
        // the replay tests' hand-written stand-in for this pair cannot drift from it.
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                .getTabletMeta(discardedTabletId));
    }

    private record FollowerReplay(long oldTabletId, long keptTabletId, long discardedTabletId,
                                  long commitVersion) {
    }

    /**
     * Replay the journal entries of one split job the way a follower does, with the leader taking
     * the identical fallback in between. The leader's own copy of the job is a separate object graph
     * -- that is the whole point -- so its post-PREPARING decisions reach this side only through the
     * record it journals next. The two mutations attributed to the leader are exactly the ones
     * {@link #testLeaderIdenticalFallbackShrinksTheSplitFamily} proves runRunningJob makes.
     */
    private FollowerReplay replayIdenticalFallbackAsFollower(String tableName) throws Exception {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        SplitTabletJob origin = createSplitJob(createTable(tableName));
        // The original leader creates the shards in runPendingJob, before anyone replays the job.
        origin.createShardsOnStarOS();
        tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(origin));

        origin.setJobState(TabletReshardJob.JobState.PREPARING);
        TabletReshardJob preparingRecord = journalCopy(origin);
        tabletReshardJobMgr.replayUpdateTabletReshardJob(preparingRecord);

        SplittingTablet preparedFamily = firstSplittingTablet(preparingRecord);
        FollowerReplay replay = new FollowerReplay(preparedFamily.getOldTabletId(),
                preparedFamily.getNewTabletIds().get(0), preparedFamily.getNewTabletIds().get(1),
                firstReshardingPartition(preparingRecord).getCommitVersion());
        Assertions.assertEquals(2, tabletReshardJobMgr
                .getReshardingTablet(replay.oldTabletId(), replay.commitVersion()).getNewTabletIds().size());
        Assertions.assertNotNull(invertedIndex.getTabletMeta(replay.discardedTabletId()));

        // The leader carries on from its own copy, which holds the commit version reserved at PREPARING.
        TabletReshardJob leaderView = journalCopy(preparingRecord);
        leaderView.setJobState(TabletReshardJob.JobState.RUNNING);
        tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(leaderView));

        TabletMeta discardedMeta = invertedIndex.getTabletMeta(replay.discardedTabletId());
        firstReshardingIndex(leaderView).getMaterializedIndex().removeTablet(replay.discardedTabletId());
        firstSplittingTablet(leaderView).fallbackToIdenticalTablet();
        // removeTablet also drops the id from the process-wide inverted index. In production that
        // happens in the leader's process; the replaying follower still holds its own entry, which is
        // the state under test here, so put it back.
        invertedIndex.addTablet(replay.discardedTabletId(), discardedMeta);

        leaderView.setJobState(TabletReshardJob.JobState.CLEANING);
        tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(leaderView));

        return replay;
    }

    /**
     * A replay must never write to the edit log. {@code setJobState} always journals, and
     * {@code getOlapTable()} calls it when the table is gone -- so a replay whose table has been
     * dropped journals from whatever thread is replaying. In production that thread is a follower's
     * replayer, the leader's activation catch-up, or the checkpoint worker, and the write there
     * either throws at the closed WAL gate or dereferences a null journal queue; both were observed
     * on 2026-09-20 and both were swallowed by {@code replay()}'s catch.
     *
     * <p>Mixed red/green by construction, and deliberately so: of the seven states, PREPARING,
     * CLEANING, FINISHED and ABORTED reach {@code getOlapTable()} with {@code canAbort()} false and
     * journal on the unfixed tree; PENDING does not, because the journaling is already guarded by
     * {@code !canAbort()} and PENDING is abortable; RUNNING and ABORTING touch no table at all. The
     * passing rows are kept as coverage rather than removed, so the matrix states the whole contract.
     */
    @Test
    public void testReplayNeverJournalsWhenTableIsDropped() throws Exception {
        // Collected rather than asserted per iteration, so one run reports the whole matrix instead
        // of stopping at the first offending state.
        List<TabletReshardJob.JobState> journaled = new ArrayList<>();

        // Everything this test sets up needs a leader -- creating the table, creating the shards and
        // dropping the table are all DDL. The replay itself is the one step that never runs on one,
        // so the flag is flipped around that call and only that call: a follower, an activation
        // catch-up and the checkpoint worker are all non-leader by construction. Without it the test
        // would drive the replay path on a process that is itself the leader, which cannot happen in
        // production and where the guard under test is correctly inert.
        AtomicBoolean leader = new AtomicBoolean(true);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return leader.get();
            }
        };

        for (TabletReshardJob.JobState state : TabletReshardJob.JobState.values()) {
            String tableName = "replay_no_journal_" + state.name().toLowerCase();
            SplitTabletJob origin = createSplitJob(createTable(tableName));
            origin.createShardsOnStarOS();

            TabletReshardJob record = journalCopy(origin);
            record.jobState = state;

            starRocksAssert.dropTable(tableName);

            AtomicInteger journalWrites = countJournalWrites();
            leader.set(false);
            try {
                GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().replayUpdateTabletReshardJob(record);
            } finally {
                leader.set(true);
            }

            if (journalWrites.get() != 0) {
                journaled.add(state);
            }
        }

        Assertions.assertEquals(List.of(), journaled,
                "these replayed states wrote to the edit log with the table dropped");
    }

    /**
     * The table-independent work a replay does must still happen when the table is gone.
     *
     * <p>This test passes on the unfixed tree. It exists to rule out a fix that was drafted and
     * rejected: returning from {@code replay()} before dispatch when the table lookup comes back
     * empty. That would have been quieter, but {@code replayCleaningJob} prunes discarded child
     * tablet ids from the inverted index before it ever touches the table, and those ids never enter
     * a materialized index -- so dropping the table does not reclaim them either, because force
     * deletion only walks tablets reachable from the table's materialized indexes. Skipping the
     * prune would leak them and undo the fix in #63300.
     */
    @Test
    public void testReplayStillPrunesDiscardedTabletsWhenTableIsDropped() throws Exception {
        FollowerReplay replay = replayIdenticalFallbackAsFollower("replay_prune_dropped_table");
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        Assertions.assertNull(invertedIndex.getTabletMeta(replay.discardedTabletId()),
                "the discarded child tablet must be pruned from the inverted index");
        Assertions.assertNotNull(invertedIndex.getTabletMeta(replay.keptTabletId()),
                "the surviving child tablet must stay in the inverted index");
    }

    /**
     * Characterization, not a regression test: this passes on the unfixed tree, and its job is to
     * hold the leader's behaviour still while the replay path changes underneath it. A job past the
     * abortable window whose table has been dropped must still reach ABORTING, because
     * {@code abort()} refuses anything but PENDING and nothing else would move it.
     *
     * <p>It does kill the mutant that matters: invert the {@code isLeader()} guard and this fails,
     * because the leader stops recording the transition.
     */
    @Test
    public void testLeaderStillAbortsWhenTableIsDropped() throws Exception {
        String tableName = "leader_abort_dropped_table";
        SplitTabletJob job = createSplitJob(createTable(tableName));
        job.createShardsOnStarOS();
        job.jobState = TabletReshardJob.JobState.PREPARING;

        starRocksAssert.dropTable(tableName);

        Assertions.assertThrows(TabletReshardException.class, job::runPreparingJob);
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTING, job.getJobState());
    }

    /**
     * The inverted index is not in the image: an FE that starts from one rebuilds it from the tablets
     * installed on partitions, and a split job installs its new tablets only at CLEANING. When that FE
     * then replays the CLEANING record, loads start writing into the new tablets, so a commit that
     * cannot resolve them records no partition at all and the transaction turns VISIBLE without its
     * rows.
     */
    @Test
    public void testImageAtRunningThenReplayedCleaningCommitsIntoNewTablets() throws Exception {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        OlapTable table = createTable("image_running_commit_table");
        SplitTabletJob preparingRecord = replayThroughPreparing(table);
        try {
            SplitTabletJob runningRecord = copyInState(preparingRecord, TabletReshardJob.JobState.RUNNING);
            forgetAsImageLoadedFe(runningRecord);

            recoverFromImage(runningRecord);
            SplittingTablet family = firstSplittingTablet(runningRecord);
            long commitVersion = firstReshardingPartition(runningRecord).getCommitVersion();
            Assertions.assertEquals(family.getNewTabletIds(), tabletReshardJobMgr
                    .getReshardingTablet(family.getOldTabletId(), commitVersion).getNewTabletIds());

            tabletReshardJobMgr.replayUpdateTabletReshardJob(
                    copyInState(runningRecord, TabletReshardJob.JobState.CLEANING));

            long physicalPartitionId = firstReshardingPartition(runningRecord).getPhysicalPartitionId();
            long newIndexId = firstReshardingIndex(runningRecord).getMaterializedIndex().getId();
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            Assertions.assertEquals(newIndexId, physicalPartition.getLatestBaseIndex().getId());

            TransactionState txnState = commitEveryTabletOfLatestBaseIndex(table, physicalPartition);
            Assertions.assertTrue(txnState.getTableCommitInfo(table.getId()).getIdToPartitionCommitInfo()
                            .containsKey(physicalPartitionId),
                    "a load into the new tablets must commit their partition, not an empty transaction");

            for (long tabletId : newTabletIds(runningRecord)) {
                assertResolvesTo(tabletId, physicalPartitionId, newIndexId);
            }
        } finally {
            dropFromManager(preparingRecord);
        }
    }

    /**
     * The same image, but this FE is the leader and runs the job on: the identical fallback, the
     * CLEANING install and FINISHED all happen here, so the kept child has to be in the inverted
     * index before the job runs, and MaterializedIndex#removeTablet drops the discarded one.
     */
    @Test
    public void testImageAtRunningThenLeaderFinishesJob() throws Exception {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        OlapTable table = createTable("image_running_leader_table");
        SplitTabletJob preparingRecord = replayThroughPreparing(table);
        try {
            SplitTabletJob runningRecord = copyInState(preparingRecord, TabletReshardJob.JobState.RUNNING);
            long keptTabletId = firstSplittingTablet(runningRecord).getNewTabletIds().get(0);
            long discardedTabletId = firstSplittingTablet(runningRecord).getNewTabletIds().get(1);
            forgetAsImageLoadedFe(runningRecord);
            installIdenticalFallbackLakeServiceMock();

            SplitTabletJob recovered = recoverFromImage(runningRecord);
            for (int i = 0; i < 10 && recovered.getJobState() != TabletReshardJob.JobState.FINISHED; i++) {
                recovered.run();
            }
            Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, recovered.getJobState());

            long physicalPartitionId = firstReshardingPartition(runningRecord).getPhysicalPartitionId();
            long newIndexId = firstReshardingIndex(runningRecord).getMaterializedIndex().getId();
            Assertions.assertEquals(newIndexId,
                    table.getPhysicalPartition(physicalPartitionId).getLatestBaseIndex().getId());
            assertResolvesTo(keptTabletId, physicalPartitionId, newIndexId);
            Assertions.assertNull(invertedIndex.getTabletMeta(discardedTabletId));
        } finally {
            dropFromManager(preparingRecord);
        }
    }

    /**
     * An image at RUNNING carries the full family, because the leader takes the identical fallback in
     * memory and journals it only with the CLEANING record. Recovery must therefore put both children
     * back, and replaying that CLEANING record then prunes the discarded one, as it does on an FE that
     * replayed PREPARING.
     */
    @Test
    public void testImageAtRunningThenReplayedFallbackCleaningPrunesDiscardedTablet() throws Exception {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        OlapTable table = createTable("image_running_fallback_table");
        SplitTabletJob preparingRecord = replayThroughPreparing(table);
        try {
            long keptTabletId = firstSplittingTablet(preparingRecord).getNewTabletIds().get(0);
            long discardedTabletId = firstSplittingTablet(preparingRecord).getNewTabletIds().get(1);
            SplitTabletJob cleaningRecord = leaderFallbackCleaningRecord(preparingRecord, discardedTabletId);
            SplitTabletJob runningRecord = copyInState(preparingRecord, TabletReshardJob.JobState.RUNNING);
            forgetAsImageLoadedFe(runningRecord);

            recoverFromImage(runningRecord);
            Assertions.assertNotNull(invertedIndex.getTabletMeta(keptTabletId));
            Assertions.assertNotNull(invertedIndex.getTabletMeta(discardedTabletId));

            tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(cleaningRecord));

            assertResolvesTo(keptTabletId, firstReshardingPartition(cleaningRecord).getPhysicalPartitionId(),
                    firstReshardingIndex(cleaningRecord).getMaterializedIndex().getId());
            Assertions.assertNull(invertedIndex.getTabletMeta(discardedTabletId));
        } finally {
            dropFromManager(preparingRecord);
        }
    }

    /**
     * Characterization: an image at CLEANING holds the new index already installed, minus the child
     * the identical fallback dropped, so the rebuild from partitions alone gives the right inverted
     * index. Recovering the job must not bring the dropped child back.
     */
    @Test
    public void testImageAtCleaningKeepsOnlyTheInstalledChildren() throws Exception {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        OlapTable table = createTable("image_cleaning_table");
        SplitTabletJob preparingRecord = replayThroughPreparing(table);
        try {
            long keptTabletId = firstSplittingTablet(preparingRecord).getNewTabletIds().get(0);
            long discardedTabletId = firstSplittingTablet(preparingRecord).getNewTabletIds().get(1);
            SplitTabletJob cleaningRecord = leaderFallbackCleaningRecord(preparingRecord, discardedTabletId);
            tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(cleaningRecord));
            forgetAsImageLoadedFe(cleaningRecord);
            // What image load does before the job manager block: rebuild from the installed indexes.
            GlobalStateMgr.getCurrentState().getLocalMetastore().recreateTabletInvertIndex();

            recoverFromImage(cleaningRecord);

            assertResolvesTo(keptTabletId, firstReshardingPartition(cleaningRecord).getPhysicalPartitionId(),
                    firstReshardingIndex(cleaningRecord).getMaterializedIndex().getId());
            Assertions.assertNull(invertedIndex.getTabletMeta(discardedTabletId));
        } finally {
            dropFromManager(preparingRecord);
        }
    }

    /**
     * Image recovery adds the new tablets exactly in the states where an FE that replayed PREPARING
     * holds them in its inverted index while the partition does not: PREPARING and RUNNING. PENDING
     * has not reserved a version, CLEANING and FINISHED are covered by the rebuild from partitions
     * (and must not bring back a child the fallback dropped), and ABORTING/ABORTED are giving them up.
     */
    @Test
    public void testImageRecoveryAddsNewTabletsOnlyBeforeCleaning() throws Exception {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        // Collected rather than asserted per state, so one run reports the whole matrix.
        List<String> mismatches = new ArrayList<>();
        for (TabletReshardJob.JobState state : TabletReshardJob.JobState.values()) {
            SplitTabletJob preparingRecord =
                    replayThroughPreparing(createTable("image_matrix_" + state.name().toLowerCase()));
            try {
                SplitTabletJob record = copyInState(preparingRecord, state);
                forgetAsImageLoadedFe(record);

                recoverFromImage(record);

                List<Long> newTabletIds = newTabletIds(record);
                long resolved = newTabletIds.stream().filter(id -> invertedIndex.getTabletMeta(id) != null).count();
                boolean expectAdded = state == TabletReshardJob.JobState.PREPARING
                        || state == TabletReshardJob.JobState.RUNNING;
                if (resolved != (expectAdded ? newTabletIds.size() : 0)) {
                    mismatches.add(state + ": " + resolved + " of " + newTabletIds.size() + " new tablets resolve");
                }
            } finally {
                dropFromManager(preparingRecord);
            }
        }

        Assertions.assertEquals(List.of(), mismatches);
    }

    /**
     * Bring a split job to where every FE that replayed its PREPARING record stands: the commit version
     * reserved, the new tablets in the inverted index and the family registered. That is the FE whose
     * image the recovery tests load.
     */
    private SplitTabletJob replayThroughPreparing(OlapTable table) throws Exception {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        SplitTabletJob origin = createSplitJob(table);
        origin.createShardsOnStarOS();
        tabletReshardJobMgr.replayUpdateTabletReshardJob(journalCopy(origin));

        origin.setJobState(TabletReshardJob.JobState.PREPARING);
        SplitTabletJob preparingRecord = (SplitTabletJob) journalCopy(origin);
        tabletReshardJobMgr.replayUpdateTabletReshardJob(preparingRecord);
        return preparingRecord;
    }

    /**
     * The leader's CLEANING record after it took the identical fallback, built from its own copy of the
     * job with the two mutations runRunningJob makes (see testLeaderIdenticalFallbackShrinksTheSplitFamily).
     * removeTablet also drops the discarded id from this process's inverted index.
     */
    private static SplitTabletJob leaderFallbackCleaningRecord(SplitTabletJob preparingRecord, long discardedTabletId) {
        SplitTabletJob leaderView = (SplitTabletJob) journalCopy(preparingRecord);
        firstReshardingIndex(leaderView).getMaterializedIndex().removeTablet(discardedTabletId);
        firstSplittingTablet(leaderView).fallbackToIdenticalTablet();
        leaderView.jobState = TabletReshardJob.JobState.CLEANING;
        return leaderView;
    }

    /**
     * Turn this process into an FE that starts from an image written by an FE which replayed up to
     * record. It shares this process's catalog, but its inverted index was rebuilt from partitions and
     * its job manager comes only from the image block, so it knows neither the job, its registry entries
     * nor its new tablets yet.
     */
    private static void forgetAsImageLoadedFe(SplitTabletJob record) {
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        dropFromManager(record);
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : record.getReshardingPhysicalPartitions().values()) {
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition.getReshardingIndexes().values()) {
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    Assertions.assertNull(tabletReshardJobMgr.getReshardingTablet(
                            reshardingTablet.getFirstOldTabletId(), reshardingPhysicalPartition.getCommitVersion()));
                }
            }
        }
        List<Long> newTabletIds = newTabletIds(record);
        invertedIndex.deleteTablets(newTabletIds);
        for (long tabletId : newTabletIds) {
            Assertions.assertNull(invertedIndex.getTabletMeta(tabletId));
        }
    }

    // Write record into a TabletReshardJobMgr image block and load it the way FE startup does:
    // TabletReshardJobMgr.load -> readJson -> gsonPostProcess.
    private static SplitTabletJob recoverFromImage(SplitTabletJob record) throws Exception {
        TabletReshardJobMgr imageSide = new TabletReshardJobMgr();
        imageSide.getTabletReshardJobs().put(record.getJobId(), journalCopy(record));
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        imageSide.save(image.getImageWriter());
        SRMetaBlockReader reader = image.getMetaBlockReader();
        TabletReshardJobMgr tabletReshardJobMgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        tabletReshardJobMgr.load(reader);
        reader.close();
        return (SplitTabletJob) tabletReshardJobMgr.getTabletReshardJob(record.getJobId());
    }

    private static void dropFromManager(SplitTabletJob record) {
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJobs().remove(record.getJobId());
        record.unregisterReshardingTablets();
    }

    private static SplitTabletJob copyInState(SplitTabletJob record, TabletReshardJob.JobState state) {
        SplitTabletJob copy = (SplitTabletJob) journalCopy(record);
        copy.jobState = state;
        return copy;
    }

    private static List<Long> newTabletIds(SplitTabletJob record) {
        List<Long> tabletIds = new ArrayList<>();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : record.getReshardingPhysicalPartitions().values()) {
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition.getReshardingIndexes().values()) {
                for (Tablet tablet : reshardingIndex.getMaterializedIndex().getTablets()) {
                    tabletIds.add(tablet.getId());
                }
            }
        }
        return tabletIds;
    }

    private static void assertResolvesTo(long tabletId, long physicalPartitionId, long indexId) {
        TabletMeta tabletMeta = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(tabletId);
        Assertions.assertNotNull(tabletMeta, "tablet " + tabletId + " is not in the inverted index");
        Assertions.assertEquals(physicalPartitionId, tabletMeta.getPhysicalPartitionId());
        Assertions.assertEquals(indexId, tabletMeta.getIndexId());
    }

    // Commit a load that wrote every tablet of the partition's latest base index. The listener resolves
    // the written tablets through the inverted index, as a shared-data commit does.
    private static TransactionState commitEveryTabletOfLatestBaseIndex(OlapTable table, PhysicalPartition partition)
            throws Exception {
        DatabaseTransactionMgr dbTxnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getDatabaseTransactionMgr(db.getId());
        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator()
                .getNextTransactionId();
        TransactionState txnState = new TransactionState(db.getId(), List.of(table.getId()), txnId,
                "image_recovery_load_" + txnId, null, TransactionState.LoadJobSourceType.BACKEND_STREAMING, null, 0,
                60_000);
        List<TabletCommitInfo> finishedTablets = new ArrayList<>();
        for (Tablet tablet : partition.getLatestBaseIndex().getTablets()) {
            finishedTablets.add(new TabletCommitInfo(tablet.getId(), 1));
        }
        LakeTableTxnStateListener listener = new LakeTableTxnStateListener(dbTxnMgr, table);
        listener.prePrepared(txnState, finishedTablets, Collections.emptyList());
        txnState.setTransactionStatus(TransactionStatus.COMMITTED);
        listener.preWriteCommitLog(txnState);
        return txnState;
    }

    private static AtomicInteger countJournalWrites() {
        AtomicInteger writes = new AtomicInteger();
        new MockUp<EditLog>() {
            @Mock
            public void logUpdateTabletReshardJob(TabletReshardJob job) {
                writes.incrementAndGet();
            }
        };
        return writes;
    }

    private OlapTable createTable(String tableName) throws Exception {
        starRocksAssert.withTable("create table " + tableName + " (key1 int, key2 varchar(10))\n"
                + "order by(key1)\n"
                + "properties('replication_num' = '1'); ");
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
    }

    private SplitTabletJob createSplitJob(OlapTable table) throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        TabletList tabletList = new TabletList(List.of(materializedIndex.getTablets().get(0).getId()));

        Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, "-2");
        SplitTabletClause clause = new SplitTabletClause(null, tabletList, properties);
        clause.setTabletReshardTargetSize(-2);

        return (SplitTabletJob) new SplitTabletJobFactory(db, table, clause).createTabletReshardJob();
    }

    /**
     * A BE that could not derive split boundaries: it publishes only the first new tablet, so FE
     * sees no range for the rest of the family and takes the identical fallback.
     */
    private void installIdenticalFallbackLakeServiceMock() {
        new MockUp<MockLakeService>() {
            @Mock
            public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
                return CompletableFuture.completedFuture(fallbackResponse(List.of(request)));
            }

            @Mock
            public Future<PublishVersionResponse> aggregatePublishVersion(AggregatePublishVersionRequest request) {
                return CompletableFuture.completedFuture(fallbackResponse(request.publishReqs));
            }
        };
    }

    private static PublishVersionResponse fallbackResponse(List<PublishVersionRequest> requests) {
        PublishVersionResponse response = new PublishVersionResponse();
        response.status = new StatusPB();
        response.status.statusCode = TStatusCode.OK.getValue();
        response.tabletRanges = new HashMap<>();
        for (PublishVersionRequest request : requests) {
            if (request == null || request.reshardingTabletInfos == null) {
                continue;
            }
            for (ReshardingTabletInfoPB info : request.reshardingTabletInfos) {
                if (info.splittingTabletInfo == null) {
                    continue;
                }
                // Only the first child gets a range, which is what makes FE take the fallback path.
                // The value is incidental: the consumer keys off presence, and every tablet of a
                // freshly created range-distributed table carries this same unbounded range anyway
                // (LocalMetastore#createLakeTablets).
                response.tabletRanges.put(info.splittingTabletInfo.newTabletIds.get(0),
                        new TabletRange().toProto());
            }
        }
        return response;
    }

    // Serialize and deserialize through the base type, exactly as the journal does, so every replayed
    // record is an independent object graph.
    private static TabletReshardJob journalCopy(TabletReshardJob job) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job, TabletReshardJob.class),
                TabletReshardJob.class);
    }

    private static ReshardingPhysicalPartition firstReshardingPartition(TabletReshardJob job) {
        return ((SplitTabletJob) job).getReshardingPhysicalPartitions().values().iterator().next();
    }

    private static ReshardingMaterializedIndex firstReshardingIndex(TabletReshardJob job) {
        return firstReshardingPartition(job).getReshardingIndexes().values().iterator().next();
    }

    private static SplittingTablet firstSplittingTablet(TabletReshardJob job) {
        for (ReshardingTablet reshardingTablet : firstReshardingIndex(job).getReshardingTablets()) {
            if (reshardingTablet.getSplittingTablet() != null) {
                return reshardingTablet.getSplittingTablet();
            }
        }
        throw new IllegalStateException("no splitting tablet in job");
    }
}
