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

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LeaderLease;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletGroupList;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static com.starrocks.alter.reshard.TabletReshardJob.JobState.ABORTED;
import static com.starrocks.alter.reshard.TabletReshardJob.JobState.ABORTING;
import static com.starrocks.alter.reshard.TabletReshardJob.JobState.FINISHED;
import static com.starrocks.alter.reshard.TabletReshardJob.JobState.PENDING;
import static com.starrocks.alter.reshard.TabletReshardJob.JobState.PREPARING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A reshard job's PENDING step creates the new shards on StarOS. The scheduler runs every reshard job in the
 * cluster on one thread, so the request runs on the lake alter publish pool and later rounds poll it.
 */
public class TabletReshardPendingShardCreationTest {
    private static final Set<TabletReshardJob.JobState> POST_PENDING = EnumSet.range(PREPARING, FINISHED);

    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private static TabletReshardJobMgr mgr;
    private static boolean rangeDistributionEnabled;
    private static boolean tabletMergeEnabled;

    // The leader lease the mocked GlobalStateMgr treats as current, in the tests that call installLeaseMock().
    private final LeaderLease[] current = {new LeaderLease(1, 1)};
    // {generation of current[0] at call time, partition id} of each createShardsForSplit or createShardsForMerge call
    // a test records.
    private final List<long[]> sentBatches = new CopyOnWriteArrayList<>();

    enum Kind {
        SPLIT,
        MERGE
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // Every scheduler round in this class is an explicit runAfterCatalogReadyForTest() call, so stop the
        // leader daemons, the reshard scheduler among them, before any job is admitted.
        List<LeaderDaemon> daemons = LeaderDaemon.getRunningInstances();
        daemons.forEach(LeaderDaemon::setStop);
        LeaderDaemon.awaitQuiesced(daemons, 30_000L);
        mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        assertFalse(mgr.isRunning());

        rangeDistributionEnabled = Config.enable_range_distribution;
        tabletMergeEnabled = Config.tablet_reshard_enable_tablet_merge;
        Config.enable_range_distribution = true;
        Config.tablet_reshard_enable_tablet_merge = true;

        starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.withDatabase("reshard_pending").useDatabase("reshard_pending");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("reshard_pending");
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_range_distribution = rangeDistributionEnabled;
        Config.tablet_reshard_enable_tablet_merge = tabletMergeEnabled;
    }

    @BeforeEach
    public void beforeEach() {
        // Each test works on tables of its own; drop the jobs an earlier test left behind so a scheduler round
        // here runs only this test's jobs.
        mgr.getTabletReshardJobs().clear();
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    public void slowShardCreationDoesNotHoldBackAnotherTablesJob(Kind kind) throws Exception {
        String suffix = kind.name().toLowerCase();
        OlapTable slowTable = createTable("slow_" + suffix, false);
        OlapTable fastTable = createTable("fast_" + suffix, false);
        String slowTableId = Long.toString(slowTable.getId());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exitedSlowCall = new CountDownLatch(1);
        AtomicReference<String> slowCallFailure = new AtomicReference<>();
        // No call reaches StarMgr: this test is about scheduling, not StarOS. Shard creation returns at once, except
        // the slow table's, which returns only after the test releases it.
        new MockUp<StarOSAgent>() {
            @Mock
            public void createShardsForSplit(Map<Long, Long> newToOldShardId,
                                             Map<Long, List<Long>> newShardIdToGroupIds, FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo, Map<String, String> props,
                                             ComputeResource computeResource, boolean unpinPlacement)
                    throws InterruptedException {
                holdIfSlow(props);
            }

            @Mock
            public void createShardsForMerge(Map<Long, List<Long>> newToOldShardIds,
                                             Map<Long, List<Long>> newShardIdToGroupIds, FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo, Map<String, String> props,
                                             ComputeResource computeResource) throws InterruptedException {
                holdIfSlow(props);
            }

            private void holdIfSlow(Map<String, String> props) throws InterruptedException {
                if (!slowTableId.equals(props.get(LakeTablet.PROPERTY_KEY_TABLE_ID))) {
                    return;
                }
                entered.countDown();
                try {
                    if (!release.await(60, TimeUnit.SECONDS)) {
                        slowCallFailure.set("the slow table's shard creation was not released within 60 s");
                        throw new AssertionError(slowCallFailure.get());
                    }
                } finally {
                    exitedSlowCall.countDown();
                }
            }
        };
        TabletReshardJob slow = kind == Kind.SPLIT ? splitJob(slowTable) : mergeJob(slowTable);
        TabletReshardJob fast = splitJob(fastTable);

        AtomicBoolean stopped = new AtomicBoolean();
        AtomicReference<Throwable> tickerFailure = new AtomicReference<>();
        Thread ticker = new Thread(() -> {
            try {
                while (!stopped.get()) {
                    mgr.runAfterCatalogReadyForTest();
                    Thread.sleep(10);
                }
            } catch (Throwable t) {
                tickerFailure.set(t);
            }
        }, "reshard-scheduler-ticker");
        try {
            mgr.addTabletReshardJob(slow);
            ticker.start();
            assertTrue(entered.await(30, TimeUnit.SECONDS));

            mgr.addTabletReshardJob(fast);
            assertTrue(waitUntil(() -> tickerFailure.get() == null && POST_PENDING.contains(fast.getJobState())
                            && commitVersionOf(fast) > 0, 30_000L),
                    () -> "another table's job must not wait for this job's shard creation, ticker failure: "
                            + tickerFailure.get());
            // The slow call is still held, so the fast job got through while it was in flight.
            assertEquals(1L, exitedSlowCall.getCount());
            assertEquals(PENDING, slow.getJobState());
            assertEquals(0L, transactionIdOf(slow));

            release.countDown();
            assertTrue(waitUntil(() -> POST_PENDING.contains(slow.getJobState()) && commitVersionOf(slow) > 0,
                    30_000L));
        } finally {
            release.countDown();
            stopped.set(true);
            ticker.join(60_000L);
        }
        assertFalse(ticker.isAlive());
        assertNull(tickerFailure.get());
        assertNull(slowCallFailure.get());
    }

    @Test
    public void rejectedSubmissionBacksOffAndRetries() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.REJECT, Mode.INLINE);
        executor.install();
        long[] clock = {1_000_000_000_000L};
        new MockUp<TabletReshardJob>() {
            @Mock
            long nowNanos() {
                return clock[0];
            }
        };
        SplitTabletJob job = splitJob(createTable("rejected_submission", false));
        job.init();

        long rejectedAt = clock[0];
        job.run();
        assertEquals(PENDING, job.getJobState());
        assertNull(job.getErrorMessage());
        assertEquals(0L, job.getTransactionId());
        assertEquals(Long.valueOf(rejectedAt), job.createShardsRejectedNanos);

        // Not resubmitted until the delay has passed since the rejection, on the job's monotonic clock.
        long delayNanos = TimeUnit.MILLISECONDS.toNanos(TabletReshardJob.SHARD_CREATION_RESUBMIT_DELAY_MS);
        clock[0] = rejectedAt + delayNanos - 1;
        job.run();
        assertEquals(1, executor.submitCount());

        clock[0] = rejectedAt + delayNanos;
        job.run();
        assertEquals(2, executor.submitCount());
        assertNull(job.createShardsRejectedNanos);
        assertTrue(POST_PENDING.contains(job.getJobState()));
        assertTrue(job.getTransactionId() > 0);
    }

    @Test
    public void shardCreationFailureFoundLaterAbortsJob() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.MANUAL);
        executor.install();
        SplitTabletJob job = splitJob(createTable("failed_shard_creation", false));
        job.init();

        job.run();
        assertEquals(PENDING, job.getJobState());

        executor.submission(0).fail(new TabletReshardException("Failed to create new shards on StarOS: injected"));
        job.run();
        assertEquals(ABORTING, job.getJobState());
        assertEquals("Failed to create new shards on StarOS: injected", job.getErrorMessage());

        job.run();
        assertEquals(ABORTED, job.getJobState());
    }

    @Test
    public void errorFromShardCreationEscapesRun() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.MANUAL);
        executor.install();
        SplitTabletJob job = splitJob(createTable("error_shard_creation", false));
        job.init();

        job.run();
        assertEquals(PENDING, job.getJobState());

        executor.submission(0).fail(new AssertionError("boom"));
        assertThrows(AssertionError.class, job::run);
        assertEquals(PENDING, job.getJobState());
        assertNull(job.getErrorMessage());
    }

    @Test
    public void resultFromEarlierLeaderSessionIsDiscarded() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.MANUAL, Mode.INLINE);
        executor.install();
        installLeaseMock();
        SplitTabletJob job = splitJob(createTable("earlier_leader_session", false));
        job.init();

        job.run();
        current[0] = LeaderLease.INVALID;
        executor.submission(0).fail(new TabletReshardException("Failed to create new shards on StarOS: demoted"));
        // The outcome belongs to the session that ended: dropped unread, and nothing is submitted while this node
        // is not the leader.
        job.run();
        assertEquals(1, executor.submitCount());
        assertEquals(PENDING, job.getJobState());
        assertNull(job.getErrorMessage());

        // Re-elected: the new leader session submits its own request.
        current[0] = new LeaderLease(1, 2);
        job.run();
        assertEquals(2, executor.submitCount());
        assertEquals(1, executor.submission(1).runs());
        assertTrue(POST_PENDING.contains(job.getJobState()) && commitVersionOf(job) > 0);
    }

    @Test
    public void sessionEndedFailureNeverAbortsJob() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.MANUAL, Mode.INLINE);
        executor.install();
        SplitTabletJob job = splitJob(createTable("session_ended_failure", false));
        job.init();

        job.run();
        // The lease stays valid. A worker stops this way only after its lease has ended, which the poll's own
        // check already catches, so this is defensive: the creation is submitted again, never aborted.
        executor.submission(0).fail(new TabletReshardJob.ShardCreationSessionEndedException(
                "Stopped creating new shards on StarOS: the leader session that started it has ended"));
        job.run();
        assertEquals(PENDING, job.getJobState());
        assertEquals(1, executor.submitCount());
        assertNull(job.getErrorMessage());
        assertEquals(0L, commitVersionOf(job));

        job.run();
        assertEquals(2, executor.submitCount());
        assertTrue(POST_PENDING.contains(job.getJobState()) && commitVersionOf(job) > 0);
    }

    @Test
    public void pollChecksDoneBeforeLease() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.MANUAL, Mode.INLINE);
        executor.install();
        LeaderLease[] current = this.current;
        AtomicBoolean injected = new AtomicBoolean();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public LeaderLease captureLeaderLease() {
                return current[0];
            }

            // The first check of the lease after #0 was submitted fails #0, and the lease stays valid.
            @Mock
            public boolean isLeaderLeaseValid(LeaderLease lease) {
                if (executor.submitCount() > 0 && injected.compareAndSet(false, true)) {
                    executor.submission(0).fail(
                            new TabletReshardException("Failed to create new shards on StarOS: injected"));
                }
                return lease != null && lease.isValid() && lease.equals(current[0]);
            }
        };
        SplitTabletJob job = splitJob(createTable("poll_checks_done_before_lease", false));
        job.init();

        job.run();
        assertEquals(1, executor.submitCount());
        assertFalse(injected.get());

        // #0 fails while its lease is being checked. This round saw it unfinished before that check, so it leaves
        // the failure to the next round.
        job.run();
        assertTrue(injected.get());
        assertEquals(PENDING, job.getJobState());
        assertNull(job.getErrorMessage());
        assertEquals(1, executor.submitCount());

        job.run();
        assertEquals(ABORTING, job.getJobState());
        assertEquals("Failed to create new shards on StarOS: injected", job.getErrorMessage());
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    public void oldWorkerStopsBeforeNextBatch(Kind kind) throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.THREAD, Mode.INLINE);
        executor.install();
        installLeaseMock();
        OlapTable table = createTable("old_worker_next_batch_" + kind.name().toLowerCase(), true);
        List<Long> partitionIds = table.getAllPhysicalPartitions().stream().map(PhysicalPartition::getId).toList();
        long firstPartitionId = partitionIds.get(0);
        long secondPartitionId = partitionIds.get(1);
        AtomicReference<Long> firstBatchPartition = new AtomicReference<>();
        CountDownLatch enteredFirstBatch = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        LeaderLease[] current = this.current;
        List<long[]> sentBatches = this.sentBatches;
        new MockUp<StarOSAgent>() {
            @Mock
            public void createShardsForSplit(Map<Long, Long> newToOldShardId,
                                             Map<Long, List<Long>> newShardIdToGroupIds, FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo, Map<String, String> props,
                                             ComputeResource computeResource, boolean unpinPlacement)
                    throws InterruptedException {
                recordBatch(props);
            }

            @Mock
            public void createShardsForMerge(Map<Long, List<Long>> newToOldShardIds,
                                             Map<Long, List<Long>> newShardIdToGroupIds, FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo, Map<String, String> props,
                                             ComputeResource computeResource) throws InterruptedException {
                recordBatch(props);
            }

            // Records the batch and holds the first one until the test releases it. Neither call is forwarded to
            // StarOS: the test is about which batches are sent, and StarMgr placement time must not count against
            // the waits below (the merge fixture's sibling tablets have no StarOS shard anyway).
            private void recordBatch(Map<String, String> props) throws InterruptedException {
                long partitionId = Long.parseLong(props.get(LakeTablet.PROPERTY_KEY_PARTITION_ID));
                sentBatches.add(new long[] {current[0].getGeneration(), partitionId});
                if (firstBatchPartition.compareAndSet(null, partitionId)) {
                    enteredFirstBatch.countDown();
                    if (!release.await(60, TimeUnit.SECONDS)) {
                        throw new AssertionError("the first batch was never released");
                    }
                }
            }
        };
        TabletReshardJob job = kind == Kind.SPLIT ? splitJobAcrossPartitions(table) : mergeJobAcrossPartitions(table);
        job.init();
        try {
            job.run();
            assertTrue(enteredFirstBatch.await(30, TimeUnit.SECONDS));
            // Re-elected while the first batch is in flight: that batch finishes, the next one is never sent.
            current[0] = new LeaderLease(1, 2);
            release.countDown();
            ExecutionException e = assertThrows(ExecutionException.class,
                    () -> executor.submission(0).get(30, TimeUnit.SECONDS));
            assertInstanceOf(TabletReshardJob.ShardCreationSessionEndedException.class, e.getCause());
        } finally {
            release.countDown();
        }
        // The job keeps its partitions in a HashMap, so either partition may go first.
        assertTrue(Set.of(firstPartitionId, secondPartitionId).contains(firstBatchPartition.get()));
        assertEquals(List.of(firstBatchPartition.get()), partitionsSentUnder(1));

        job.run();
        assertEquals(2, partitionsSentUnder(2).size());
        assertEquals(Set.of(firstPartitionId, secondPartitionId), Set.copyOf(partitionsSentUnder(2)));
        assertTrue(commitVersionOf(job) > 0);
    }

    @Test
    public void stopClearsInFlightShardCreation() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.MANUAL, Mode.INLINE);
        executor.install();
        SplitTabletJob job = splitJob(createTable("stop_clears_shard_creation", false));
        mgr.addTabletReshardJob(job);
        job.run();
        assertEquals(1, executor.submitCount());

        // The scheduler stops as on demotion, forgetting the request of the session that ended.
        mgr.start();
        mgr.setStop();
        LeaderDaemon.awaitQuiesced(List.of(mgr), 30_000L);
        assertFalse(mgr.isRunning());
        // Dropped, not cancelled.
        assertFalse(executor.submission(0).isCancelled());

        job.run();
        assertEquals(2, executor.submitCount());
        assertTrue(POST_PENDING.contains(job.getJobState()));
    }

    @Test
    public void replacedJobObjectIgnoresOldWorker() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.MANUAL, Mode.INLINE);
        executor.install();
        OlapTable table = createTable("replaced_job_object", false);
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        SplitTabletJob oldJob = splitJob(table);
        mgr.addTabletReshardJob(oldJob);
        oldJob.run();
        long nextBefore = partition.getNextVersion();

        // Replay installs another object for the same job, which reserves the version once.
        SplitTabletJob replacement = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(oldJob), SplitTabletJob.class);
        mgr.replayUpdateTabletReshardJob(replacement);
        assertSame(replacement, mgr.getTabletReshardJob(replacement.getJobId()));
        replacement.run();
        assertTrue(POST_PENDING.contains(replacement.getJobState()));
        assertEquals(nextBefore, commitVersionOf(replacement));
        assertEquals(nextBefore + 1, partition.getNextVersion());

        // The old object's worker finishes late: it neither journals nor changes either job or the partition.
        int[] writes = countJobJournalWrites();
        executor.submission(0).run();
        // StarMgr takes the repeated request as the same shards.
        assertTrue((Integer) executor.submission(0).get() > 0);
        assertEquals(0, writes[0]);
        assertEquals(PENDING, oldJob.getJobState());
        assertEquals(nextBefore, commitVersionOf(replacement));
        assertEquals(nextBefore + 1, partition.getNextVersion());
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    public void staleWorkerFindingNoTableChangesNothing(Kind kind) throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.MANUAL, Mode.INLINE);
        executor.install();
        installLeaseMock();
        new MockUp<StarOSAgent>() {
            // The merge fixture's sibling tablet has no StarOS shard.
            @Mock
            public void createShardsForMerge(Map<Long, List<Long>> newToOldShardIds,
                                             Map<Long, List<Long>> newShardIdToGroupIds, FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo, Map<String, String> props,
                                             ComputeResource computeResource) {
            }
        };
        OlapTable table = createTable("stale_worker_no_table_" + kind.name().toLowerCase(), false);
        TabletReshardJob job = kind == Kind.SPLIT ? splitJob(table) : mergeJob(table);
        job.init();

        job.run();
        // Re-elected before the first session's worker started: the new session creates the shards and the job
        // leaves PENDING.
        current[0] = new LeaderLease(1, 2);
        job.run();
        TabletReshardJob.JobState before = job.getJobState();
        assertTrue(commitVersionOf(job) > 0);

        // The first session's worker runs late and finds the table gone.
        Long tableId = table.getId();
        AtomicInteger lookups = new AtomicInteger();
        new MockUp<LocalMetastore>() {
            @Mock
            public Table getTable(Invocation inv, Long dbId, Long id) {
                if (tableId.equals(id)) {
                    lookups.incrementAndGet();
                    return null;
                }
                return inv.proceed();
            }
        };
        int[] writes = countJobJournalWrites();
        executor.submission(0).run();
        ExecutionException e = assertThrows(ExecutionException.class, () -> executor.submission(0).get());
        assertTrue(e.getCause().getMessage().startsWith("Table not found"));
        // At least once: the lock may look the table up as well.
        assertTrue(lookups.get() >= 1);
        assertEquals(0, writes[0]);
        assertEquals(before, job.getJobState());
        assertNull(job.getErrorMessage());
    }

    private static OlapTable createTable(String name, boolean twoPartitions) throws Exception {
        String partitionClause = twoPartitions
                ? " partition by range(k) (partition p1 values less than ('10'),"
                        + " partition p2 values less than (MAXVALUE))"
                : "";
        starRocksAssert.withTable("create table " + name + " (k int, v int)" + partitionClause
                + " order by(k) properties('replication_num'='1')");
        return (OlapTable) db.getTable(name);
    }

    private static SplitTabletJob splitJob(OlapTable table) throws Exception {
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        return splitJob(table, List.of(partition.getLatestBaseIndex().getTablets().get(0).getId()));
    }

    // Splits the first tablet of every partition in one job.
    private static SplitTabletJob splitJobAcrossPartitions(OlapTable table) throws Exception {
        return splitJob(table, table.getAllPhysicalPartitions().stream()
                .map(partition -> partition.getLatestBaseIndex().getTablets().get(0).getId())
                .toList());
    }

    private static SplitTabletJob splitJob(OlapTable table, List<Long> tabletIds) throws Exception {
        SplitTabletClause clause = new SplitTabletClause(null, new TabletList(tabletIds),
                Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, "-2"));
        clause.setTabletReshardTargetSize(-2);
        return (SplitTabletJob) new SplitTabletJobFactory(db, table, clause).createTabletReshardJob();
    }

    // Cuts the table's only tablet at 15 and adds a sibling tablet for [15, +inf), then merges the two. The
    // sibling has no StarOS shard, so a test running this job stubs createShardsForMerge to succeed without
    // forwarding the call.
    private static MergeTabletJob mergeJob(OlapTable table) throws Exception {
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        return mergeJob(table, List.of(addSibling(table, partition, "15")));
    }

    // In each partition of the two-partition table (p1 < 10 <= p2), cuts the first tablet at a value inside that
    // partition (5 in p1, 15 in p2) and adds a sibling for the rest, then merges each pair in one job. As in
    // mergeJob, the siblings have no StarOS shard.
    private static MergeTabletJob mergeJobAcrossPartitions(OlapTable twoPartitionTable) throws Exception {
        PhysicalPartition p1 = twoPartitionTable.getPartition("p1").getDefaultPhysicalPartition();
        PhysicalPartition p2 = twoPartitionTable.getPartition("p2").getDefaultPhysicalPartition();
        return mergeJob(twoPartitionTable, List.of(addSibling(twoPartitionTable, p1, "5"),
                addSibling(twoPartitionTable, p2, "15")));
    }

    private static MergeTabletJob mergeJob(OlapTable table, List<List<Long>> tabletGroups) throws Exception {
        MergeTabletClause clause = new MergeTabletClause(null, new TabletGroupList(tabletGroups), null);
        return (MergeTabletJob) new MergeTabletJobFactory(db, table, clause).createTabletReshardJob();
    }

    // Cuts the partition's first tablet at the given value: it keeps (-inf, cut) and a new sibling tablet takes
    // [cut, +inf). Returns {original, sibling}.
    private static List<Long> addSibling(OlapTable table, PhysicalPartition partition, String cutValue) {
        MaterializedIndex index = partition.getLatestBaseIndex();
        LakeTablet original = (LakeTablet) index.getTablets().get(0);
        Tuple cut = new Tuple(List.of(Variant.of(IntegerType.INT, cutValue)));
        LakeTablet sibling = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId(), new TabletRange());
        original.setRange(new TabletRange(Range.of(null, cut, false, false)));
        sibling.setRange(new TabletRange(Range.of(cut, null, true, false)));
        original.setHasSharedFiles(false);
        sibling.setHasSharedFiles(false);
        index.addTablet(sibling, new TabletMeta(db.getId(), table.getId(), partition.getId(),
                index.getId(), TStorageMedium.HDD, true));
        return List.of(original.getId(), sibling.getId());
    }

    private static long commitVersionOf(TabletReshardJob job) {
        return job.getReshardingPhysicalPartitions().values().stream()
                .mapToLong(ReshardingPhysicalPartition::getCommitVersion).min().orElse(0L);
    }

    private static long transactionIdOf(TabletReshardJob job) {
        return job instanceof SplitTabletJob split ? split.getTransactionId()
                : ((MergeTabletJob) job).getTransactionId();
    }

    private static boolean waitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadlineMs = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadlineMs) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return condition.getAsBoolean();
    }

    // Makes current[0] the leader lease GlobalStateMgr reports, and the only one it treats as valid. Only for tests
    // that call job.init() and job.run() themselves: addTabletReshardJob checks the real lease.
    private void installLeaseMock() {
        LeaderLease[] current = this.current;
        new MockUp<GlobalStateMgr>() {
            @Mock
            public LeaderLease captureLeaderLease() {
                return current[0];
            }

            @Mock
            public boolean isLeaderLeaseValid(LeaderLease lease) {
                return lease != null && lease.isValid() && lease.equals(current[0]);
            }
        };
    }

    // The partition ids of the recorded shard creation calls made while current[0] had this generation.
    private List<Long> partitionsSentUnder(long generation) {
        return sentBatches.stream().filter(batch -> batch[0] == generation).map(batch -> batch[1]).toList();
    }

    // Counts the reshard job records written to the journal from now on.
    private static int[] countJobJournalWrites() {
        int[] writes = {0};
        new MockUp<EditLog>() {
            @Mock
            public void logUpdateTabletReshardJob(Invocation inv, TabletReshardJob job) {
                writes[0]++;
                inv.proceed();
            }

            @Mock
            public void logUpdateTabletReshardJob(Invocation inv, TabletReshardJob snapshot, WALApplier applier) {
                writes[0]++;
                inv.proceed();
            }
        };
        return writes;
    }

    enum Mode {
        // submit throws RejectedExecutionException
        REJECT,
        // the task runs before submit returns
        INLINE,
        // the test runs or fails the returned future
        MANUAL,
        // the task runs on a new thread
        THREAD
    }

    /** A future the test can complete by hand, counting how often its task ran. */
    static final class ControlledFuture<T> extends FutureTask<T> {
        private final AtomicInteger runs;

        private ControlledFuture(Callable<T> task, AtomicInteger runs) {
            super(task);
            this.runs = runs;
        }

        static <T> ControlledFuture<T> of(Callable<T> task) {
            AtomicInteger runs = new AtomicInteger();
            return new ControlledFuture<>(() -> {
                runs.incrementAndGet();
                return task.call();
            }, runs);
        }

        void fail(Throwable failure) {
            setException(failure);
        }

        int runs() {
            return runs.get();
        }
    }

    /** Stands in for the lake alter publish pool: each submission follows the next scripted mode. */
    static final class ScriptedExecutor extends ThreadPoolExecutor {
        private final Queue<Mode> script;
        private final List<ControlledFuture<?>> submissions = new CopyOnWriteArrayList<>();
        private final AtomicInteger submitCount = new AtomicInteger();

        ScriptedExecutor(Mode... modes) {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
            script = new ConcurrentLinkedQueue<>(List.of(modes));
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            submitCount.incrementAndGet();
            Mode mode = script.poll();
            if (mode == null) {
                throw new IllegalStateException("unscripted submission " + submitCount.get());
            }
            if (mode == Mode.REJECT) {
                throw new RejectedExecutionException("scripted rejection");
            }
            ControlledFuture<T> future = ControlledFuture.of(task);
            submissions.add(future);
            if (mode == Mode.INLINE) {
                future.run();
            } else if (mode == Mode.THREAD) {
                new Thread(future, "scripted-shard-creation").start();
            }
            return future;
        }

        int submitCount() {
            return submitCount.get();
        }

        ControlledFuture<?> submission(int i) {
            return submissions.get(i);
        }

        void install() {
            ScriptedExecutor executor = this;
            new MockUp<GlobalStateMgr>() {
                @Mock
                public ThreadPoolExecutor getLakeAlterPublishExecutor() {
                    return executor;
                }
            };
        }
    }
}
