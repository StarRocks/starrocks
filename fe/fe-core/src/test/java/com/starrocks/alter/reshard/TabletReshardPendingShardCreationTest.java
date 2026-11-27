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
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
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

    enum Kind {
        SPLIT,
        MERGE
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // Every scheduler round in this class is an explicit runAfterCatalogReadyForTest() call, so stop the reshard
        // scheduler before any job is admitted. It checks the stop flag only between ticks, so wait for the thread
        // to exit rather than race an in-flight round.
        mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        assertTrue(mgr.isRunning());
        mgr.setStop();
        mgr.join(30_000L);
        assertFalse(mgr.isAlive());

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
        OlapTable slowTable = createTable("slow_" + suffix);
        OlapTable fastTable = createTable("fast_" + suffix);
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
                                             ComputeResource computeResource, boolean spreadNewShards)
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
        SplitTabletJob job = splitJob(createTable("rejected_submission"));
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
        SplitTabletJob job = splitJob(createTable("failed_shard_creation"));
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
        SplitTabletJob job = splitJob(createTable("error_shard_creation"));
        job.init();

        job.run();
        assertEquals(PENDING, job.getJobState());

        executor.submission(0).fail(new AssertionError("boom"));
        assertThrows(AssertionError.class, job::run);
        assertEquals(PENDING, job.getJobState());
        assertNull(job.getErrorMessage());
    }

    @Test
    public void replacedJobObjectIgnoresOldWorker() throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(Mode.MANUAL, Mode.INLINE);
        executor.install();
        OlapTable table = createTable("replaced_job_object");
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

    private static OlapTable createTable(String name) throws Exception {
        starRocksAssert.withTable("create table " + name + " (k int, v int)"
                + " order by(k) properties('replication_num'='1')");
        return (OlapTable) db.getTable(name);
    }

    private static SplitTabletJob splitJob(OlapTable table) throws Exception {
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        List<Long> tabletIds = List.of(partition.getLatestBaseIndex().getTablets().get(0).getId());
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
        List<List<Long>> tabletGroups = List.of(addSibling(table, partition, "15"));
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
        index.addTablet(sibling, new TabletMeta(db.getId(), table.getId(), partition.getId(),
                index.getId(), TStorageMedium.HDD, true));
        return List.of(original.getId(), sibling.getId());
    }

    private static long commitVersionOf(TabletReshardJob job) {
        Map<Long, ReshardingPhysicalPartition> partitions = job instanceof SplitTabletJob split
                ? split.getReshardingPhysicalPartitions()
                : ((MergeTabletJob) job).getReshardingPhysicalPartitions();
        return partitions.values().stream()
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
        MANUAL
    }

    /** A future the test can complete by hand. */
    static final class ControlledFuture<T> extends FutureTask<T> {
        ControlledFuture(Callable<T> task) {
            super(task);
        }

        void fail(Throwable failure) {
            setException(failure);
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
            ControlledFuture<T> future = new ControlledFuture<>(task);
            submissions.add(future);
            if (mode == Mode.INLINE) {
                future.run();
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
