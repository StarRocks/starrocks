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
import com.staros.proto.PlacementPolicy;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.LakeRangeRollupJob;
import com.starrocks.alter.LakeTableSchemaChangeJob;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.planner.RangeColocateScanDispatch;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.ReshardingTabletInfoPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The tablets of a new partition, or of a table joining a range colocate group, are cut against the ColocateRanges the
 * group has when they are built, outside the table lock, and attached later without comparing with the current ranges.
 * A bound spliced into the group in between by a split of a member, followed by the colocate checker marking the group
 * stable, leaves a stable group with a tablet crossing the new bound, and every colocate plan reading it fails. The
 * checker therefore checks the stable range colocate groups against their current ColocateRanges once per sweep
 * interval and marks a misaligned one unstable, which hands it to the alignment the checker runs for unstable groups.
 *
 * <p>The races are reproduced deterministically (a hook inside the shard creation of the attaching statement, and one
 * inside the checker's placement-convergence query); each test then asserts what the sweep does about them. The last
 * tests check that the sweep leaves aligned groups, hash colocate groups and legitimate layout changes alone.
 */
public class RangeColocatePartitionAttachTest {
    private static final long SWEEP_INTERVAL_NANOS = ColocateChecker.STABLE_GROUP_SWEEP_INTERVAL.toNanos();

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;

    /**
     * Runs once, inside the next call of the createShards overload that takes a shard group list, whatever the caller
     * (range colocate tablets are created through it).
     */
    private static volatile Runnable onNextRangeColocateCreateShards;

    /** Runs once, inside the checker's next placement-convergence query that covers this PACK shard group. */
    private static volatile long convergenceHookPackGroup = -1;
    private static volatile Runnable onConvergenceQuery;

    @BeforeAll
    public static void beforeClass() throws Exception {
        // Nothing may act behind the test: the reshard daemon also runs the colocate checker, and the rollup handler
        // would run the rollup job a test drives itself.
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
        new MockUp<MaterializedViewHandler>() {
            @Mock
            protected void runAfterLeaseValid() {
            }
        };
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Boolean> queryShardGroupStable(List<Long> shardGroupIds, long workerGroupId) {
                Runnable hook = onConvergenceQuery;
                if (hook != null && shardGroupIds.contains(convergenceHookPackGroup)) {
                    onConvergenceQuery = null;
                    hook.run();
                }
                return Collections.nCopies(shardGroupIds.size(), Boolean.TRUE);
            }

            @Mock
            public List<Long> createShards(Invocation invocation, int numShards, FilePathInfo pathInfo,
                                           FileCacheInfo cacheInfo, List<Long> groupIds, List<Long> matchShardIds,
                                           Map<String, String> properties, ComputeResource computeResource) {
                Runnable hook = onNextRangeColocateCreateShards;
                if (hook != null) {
                    onNextRangeColocateCreateShards = null;
                    hook.run();
                }
                return invocation.proceed();
            }
        };
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // The schema change test drives its job itself.
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;
        starRocksAssert.withDatabase("rc_attach_peer");
        starRocksAssert.withDatabase("rc_attach").useDatabase("rc_attach");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("rc_attach");
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private static ColocateTableIndex index() {
        return GlobalStateMgr.getCurrentState().getColocateTableIndex();
    }

    private static Tuple k1(int v) {
        return new Tuple(List.of(Variant.of(IntegerType.INT, String.valueOf(v))));
    }

    /** The canonical lower bound (v, NULL) a split child at the colocate bound k1 = v carries. */
    private static Tuple canonicalLower(int v) {
        return new Tuple(List.of(Variant.of(IntegerType.INT, String.valueOf(v)), Variant.nullVariant(IntegerType.INT)));
    }

    private static OlapTable table(String name) {
        return table(db.getFullName(), name);
    }

    private static OlapTable table(String dbName, String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbName, name);
    }

    private static final class Fixture {
        long grpId;
        long packLow;
        long packMid;
        long packTop;
        String tableName;
        OlapTable table;
        ColocateTableIndex.GroupId groupId;
        OlapTable peer;
    }

    /**
     * Creates the seed table {@code seed_<name>} in the group {@code g_<name>:k1} and gives the group three
     * ColocateRanges on k1, (-inf, 100), [100, 200) and [200, +inf), each with its own PACK shard group, as earlier
     * splits would have: a table created in the group from now on has one tablet per range. The seed keeps its single
     * tablet over everything; the caller drops it.
     */
    private static Fixture createThreeRangeGroup(String name) throws Exception {
        Fixture f = new Fixture();
        String seed = "seed_" + name;
        starRocksAssert.withTable("create table " + seed + " (k1 int, v int) duplicate key(k1) order by(k1) "
                + "properties('replication_num' = '1', 'colocate_with' = 'g_" + name + ":k1')");
        f.grpId = index().getGroup(table(seed).getId()).grpId;
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        f.packLow = index().getColocateRanges(f.grpId).get(0).getShardGroupId();
        f.packMid = starOSAgent.createShardGroup(db.getId(), table(seed).getId(), 0L, 0L, PlacementPolicy.PACK);
        f.packTop = starOSAgent.createShardGroup(db.getId(), table(seed).getId(), 0L, 0L, PlacementPolicy.PACK);
        index().getColocateRangeMgr().setColocateRanges(f.grpId, List.of(
                new ColocateRange(Range.lt(k1(100)), f.packLow),
                new ColocateRange(Range.gelt(k1(100), k1(200)), f.packMid),
                new ColocateRange(Range.ge(k1(200)), f.packTop)));
        return f;
    }

    /**
     * Table {@code t_<name>}, partitioned by k2 with partition p1, in group {@code g_<name>:k1} whose ColocateRanges are
     * (-inf, 100) and [100, +inf), stable. p1 already has a tablet bound at k1 = 200 (it was created while the group had
     * the three ranges (-inf, 100), [100, 200), [200, +inf), and each tablet is in its PACK group), so once a split of a
     * member splices 200 into the group, p1 is aligned with no alignment split and the checker can mark the group
     * stable in one cycle. With {@code peerDbName}, the group also has the member {@code p_<name>} in that database,
     * created like p1 with a tablet per range of the three.
     */
    private static Fixture createFixture(String name, String peerDbName) throws Exception {
        Fixture f = createThreeRangeGroup(name);
        String group = "g_" + name;
        f.tableName = "t_" + name;
        starRocksAssert.withTable("create table " + f.tableName + " (k1 int, k2 int, v int)\n"
                + "duplicate key(k1, k2)\n"
                + "partition by range(k2) (partition p1 values less than ('100'))\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1', 'colocate_with' = '" + group + ":k1');");
        if (peerDbName != null) {
            starRocksAssert.withTable("create table " + peerDbName + ".p_" + name + " (k1 int, v int) "
                    + "duplicate key(k1) order by(k1) "
                    + "properties('replication_num' = '1', 'colocate_with' = '" + group + ":k1')");
            f.peer = table(peerDbName, "p_" + name);
        }
        // The seed has one tablet over everything; a dropped member is skipped by the checker.
        starRocksAssert.dropTable("seed_" + name);
        // The group as it was before the split that introduces the bound at 200.
        index().getColocateRangeMgr().setColocateRanges(f.grpId, List.of(
                new ColocateRange(Range.lt(k1(100)), f.packLow),
                new ColocateRange(Range.ge(k1(100)), f.packMid)));
        f.table = table(f.tableName);
        f.groupId = index().getGroup(f.table.getId());
        index().markAllGroupsWithSameColocateGroupIdStable(f.grpId, false);
        Assertions.assertFalse(index().isAnyGroupWithSameColocateGroupIdUnstable(f.grpId));
        Assertions.assertEquals(3, baseIndex(f.table, "p1").getTablets().size());
        return f;
    }

    private static MaterializedIndex baseIndex(OlapTable t, String partitionName) {
        return t.getPartition(partitionName).getDefaultPhysicalPartition().getLatestBaseIndex();
    }

    private static MaterializedIndex tempBaseIndex(OlapTable t, String partitionName) {
        return t.getPartition(partitionName, true).getDefaultPhysicalPartition().getLatestBaseIndex();
    }

    /**
     * What a split of a member that publishes children at k1 = 200 does (SplitTabletJob.applyColocateRangeSplitResult
     * -> ColocateTableIndex.applyRangeSplitResult). The new right range [200, +inf) gets packTop, the PACK group p1's
     * [200, +inf) tablet is already in.
     */
    private static void spliceBoundAt200(Fixture f) {
        try {
            index().applyRangeSplitResult(f.grpId, Set.of(canonicalLower(200)), 1, () -> f.packTop);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** {@link #spliceBoundAt200}, then one cycle of {@code checker}. */
    private static void spliceBoundAt200AndRunChecker(Fixture f, ColocateChecker checker) {
        spliceBoundAt200(f);
        Assertions.assertEquals(3, index().getColocateRanges(f.grpId).size());
        Assertions.assertTrue(index().isGroupUnstable(f.groupId), "the splice marks the group unstable");
        checker.runOneCycle();
    }

    private static boolean alignedWithCurrentRanges(OlapTable t, MaterializedIndex index) {
        ColocateTableIndex.GroupId groupId = index().getGroup(t.getId());
        List<Column> sortKey = MetaUtils.getRangeDistributionColumns(t, index.getMetaId());
        return RangeColocateScanDispatch.isTabletRangesAligned(index, sortKey,
                index().getColocateRanges(groupId.grpId), 1);
    }

    private static List<TabletReshardJob> reshardJobsOf(OlapTable t) {
        return GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJobs().values().stream()
                .filter(job -> job.getTableId() == t.getId())
                .collect(Collectors.toList());
    }

    /**
     * Adds the partition p2 (k2 < 200) to the table of {@code f} while a split of a member splices the bound 200 into
     * the group and {@code checker} runs a cycle: ADD PARTITION has read the two ColocateRanges before the splice, and
     * the checker marks the group stable before p2 is attached, as it only sees attached partitions. Returns p2, which
     * has a tablet crossing 200 in a stable group.
     */
    private static PhysicalPartition addPartitionBuiltBeforeSplice(Fixture f, ColocateChecker checker) throws Exception {
        boolean[] stableBeforeAttach = new boolean[1];
        onNextRangeColocateCreateShards = () -> {
            // ADD PARTITION has read the two ColocateRanges and is creating the shard of the first one.
            spliceBoundAt200AndRunChecker(f, checker);
            Assertions.assertNull(f.table.getPartition("p2"), "p2 is not attached yet");
            stableBeforeAttach[0] = !index().isGroupUnstable(f.groupId);
        };
        starRocksAssert.alterTable("alter table " + f.tableName + " add partition p2 values less than ('200')");
        Assertions.assertNull(onNextRangeColocateCreateShards, "the hook ran");
        Assertions.assertTrue(stableBeforeAttach[0],
                "the checker marked the group stable while p2 was being built: it only sees attached partitions");
        MaterializedIndex p2 = baseIndex(f.table, "p2");
        Assertions.assertEquals(2, p2.getTablets().size(), "p2 was cut against the two ranges it read");
        Assertions.assertFalse(alignedWithCurrentRanges(f.table, p2), "p2 has a tablet crossing k1 = 200");
        Assertions.assertFalse(index().isGroupUnstable(f.groupId), "the group is stable nevertheless");
        return f.table.getPartition("p2").getDefaultPhysicalPartition();
    }

    /**
     * Asserts the repair: every GroupId of the group is unstable, and the checker submitted an alignment split of the
     * table {@code t} that covers {@code partition}.
     */
    private static void assertMarkedUnstableAndAligning(long grpId, OlapTable t, PhysicalPartition partition) {
        List<ColocateTableIndex.GroupId> peers = index().getAllGroupIdsWithSameColocateGroupId(grpId);
        Assertions.assertFalse(peers.isEmpty());
        for (ColocateTableIndex.GroupId peer : peers) {
            Assertions.assertTrue(index().isGroupUnstable(peer), "GroupId " + peer + " of the group must be unstable");
        }
        Assertions.assertTrue(reshardJobsOf(t).stream()
                        .anyMatch(job -> job.getReshardingPhysicalPartitions().containsKey(partition.getId())),
                "no alignment split of " + t.getName() + " covers partition " + partition.getId());
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(60, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /** Records the GroupId of every mark-unstable journal entry the test writes from now on. */
    private static List<ColocateTableIndex.GroupId> recordMarkUnstableEntries() {
        List<ColocateTableIndex.GroupId> marked = new CopyOnWriteArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logColocateMarkUnstable(Invocation invocation, ColocatePersistInfo info, WALApplier walApplier) {
                marked.add(info.getGroupId());
                invocation.proceed();
            }
        };
        return marked;
    }

    /**
     * Runs a checker cycle at each of the next three sweep intervals and asserts that none of them writes a
     * mark-unstable journal entry for the groups {@code grpIds} or submits a reshard job of a table of {@code tables},
     * and that the groups stay stable.
     */
    private static void assertSweepsLeaveGroupsStable(ColocateChecker checker, long[] now,
                                                      List<ColocateTableIndex.GroupId> marked, Set<Long> grpIds,
                                                      List<OlapTable> tables) {
        Map<String, Integer> jobsBefore = reshardJobCounts(tables);
        marked.clear();
        for (int i = 0; i < 3; i++) {
            now[0] += SWEEP_INTERVAL_NANOS;
            checker.runOneCycle();
        }
        Assertions.assertEquals(List.of(),
                marked.stream().filter(groupId -> grpIds.contains(groupId.grpId)).collect(Collectors.toList()),
                "a sweep marked an aligned group unstable");
        Assertions.assertEquals(jobsBefore, reshardJobCounts(tables), "a sweep led to a reshard job");
        for (long grpId : grpIds) {
            Assertions.assertFalse(index().isAnyGroupWithSameColocateGroupIdUnstable(grpId));
        }
    }

    private static Map<String, Integer> reshardJobCounts(List<OlapTable> tables) {
        return tables.stream().collect(Collectors.toMap(OlapTable::getName, t -> reshardJobsOf(t).size()));
    }

    // ---------------------------------------------------------------------------------------------
    // A stale attach in a stable group is found by the next sweep
    // ---------------------------------------------------------------------------------------------

    /** ADD PARTITION built against the ColocateRanges before a splice and attached after the group converged. */
    @Test
    public void testSweepMarksGroupUnstableAfterAddPartitionBuiltBeforeSplice() throws Exception {
        Fixture f = createFixture("addp", null);
        PhysicalPartition p2 = addPartitionBuiltBeforeSplice(f, new ColocateChecker());

        new ColocateChecker().runOneCycle();

        assertMarkedUnstableAndAligning(f.grpId, f.table, p2);
    }

    /**
     * CREATE TABLE joins the group before it builds its tablets, and the checker counts the member it cannot look up
     * yet as aligned, so it marks the group stable before the table is registered with tablets crossing the new bound.
     */
    @Test
    public void testSweepMarksGroupUnstableAfterCreateTableBuiltBeforeSplice() throws Exception {
        Fixture f = createFixture("ctas", null);
        Set<Long> membersBefore = new HashSet<>(index().getAllTableIds(f.groupId));
        boolean[] newMemberInvisibleToChecker = new boolean[1];
        boolean[] stableBeforeRegistered = new boolean[1];
        onNextRangeColocateCreateShards = () -> {
            // CREATE TABLE has already joined the group (range groups join before tablet creation) and read the
            // two ColocateRanges; the table is not registered in the database yet.
            Set<Long> members = new HashSet<>(index().getAllTableIds(f.groupId));
            members.removeAll(membersBefore);
            Assertions.assertEquals(1, members.size(), "the new table is already a member of the group");
            long newTableId = members.iterator().next();
            newMemberInvisibleToChecker[0] =
                    GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), newTableId) == null;
            spliceBoundAt200AndRunChecker(f, new ColocateChecker());
            stableBeforeRegistered[0] = !index().isGroupUnstable(f.groupId);
        };
        starRocksAssert.withTable("create table c_ctas (k1 int, k2 int, v int) duplicate key(k1, k2) order by(k1, k2) "
                + "properties('replication_num' = '1', 'colocate_with' = 'g_ctas:k1')");
        Assertions.assertNull(onNextRangeColocateCreateShards, "the hook ran");
        Assertions.assertTrue(newMemberInvisibleToChecker[0], "the checker looks the member up and finds nothing");
        Assertions.assertTrue(stableBeforeRegistered[0],
                "the checker counts the unregistered member as aligned and marks the group stable");
        OlapTable c = table("c_ctas");
        PhysicalPartition partition = c.getPhysicalPartitions().iterator().next();
        Assertions.assertEquals(2, partition.getLatestBaseIndex().getTablets().size());
        Assertions.assertFalse(alignedWithCurrentRanges(c, partition.getLatestBaseIndex()),
                "c_ctas has a tablet crossing k1 = 200");
        Assertions.assertFalse(index().isGroupUnstable(f.groupId), "the group is stable nevertheless");

        new ColocateChecker().runOneCycle();

        assertMarkedUnstableAndAligning(f.grpId, c, partition);
    }

    /**
     * The checker never looks at a temporary partition, so one built before a splice is still cut against the old
     * ranges when REPLACE PARTITION (or INSERT OVERWRITE, or an MV refresh) installs it into a stable group.
     */
    @Test
    public void testSweepMarksGroupUnstableAfterReplacePartitionInstallsStaleTemporaryPartition() throws Exception {
        Fixture f = createFixture("tmp", null);
        starRocksAssert.alterTable("alter table " + f.tableName
                + " add temporary partition tp1 values less than ('100')");
        Assertions.assertEquals(2, tempBaseIndex(f.table, "tp1").getTablets().size());
        // Long after tp1 was attached (as a temporary partition), a split splices 200 and the checker runs.
        spliceBoundAt200AndRunChecker(f, new ColocateChecker());
        Assertions.assertFalse(index().isGroupUnstable(f.groupId),
                "the checker marked the group stable: it skips temporary partitions");

        starRocksAssert.alterTable("alter table " + f.tableName
                + " replace partition (p1) with temporary partition (tp1)");
        MaterializedIndex p1 = baseIndex(f.table, "p1");
        Assertions.assertEquals(2, p1.getTablets().size());
        Assertions.assertFalse(alignedWithCurrentRanges(f.table, p1), "p1 now crosses k1 = 200");
        Assertions.assertFalse(index().isGroupUnstable(f.groupId), "the group is stable nevertheless");

        new ColocateChecker().runOneCycle();

        assertMarkedUnstableAndAligning(f.grpId, f.table, f.table.getPartition("p1").getDefaultPhysicalPartition());
    }

    /**
     * The partition is attached while the group is still unstable, yet the checker cycle that was already running
     * marks the group stable after the attach: it checked the table before p2 appeared. The next sweep of the same
     * checker finds p2.
     */
    @Test
    public void testSweepMarksGroupUnstableAfterAttachLostByCheckerCycleInFlight() throws Exception {
        Fixture f = createFixture("inflight", null);
        long[] now = {0L};
        ColocateChecker checker = new ColocateChecker(() -> now[0]);
        CountDownLatch checkerPastTableChecks = new CountDownLatch(1);
        CountDownLatch attached = new CountDownLatch(1);
        Thread[] checkerThread = new Thread[1];
        convergenceHookPackGroup = f.packLow;
        onConvergenceQuery = () -> {
            // The cycle has checked every member (p2 not attached yet) and asks StarOS whether placement converged.
            checkerPastTableChecks.countDown();
            await(attached);
        };
        onNextRangeColocateCreateShards = () -> {
            spliceBoundAt200(f);
            checkerThread[0] = new Thread(checker::runOneCycle);
            checkerThread[0].start();
            await(checkerPastTableChecks);
        };
        starRocksAssert.alterTable("alter table " + f.tableName + " add partition p2 values less than ('200')");
        boolean unstableWhenAttached = index().isGroupUnstable(f.groupId);
        attached.countDown();
        checkerThread[0].join(60000);
        Assertions.assertFalse(checkerThread[0].isAlive());

        MaterializedIndex p2 = baseIndex(f.table, "p2");
        Assertions.assertTrue(unstableWhenAttached, "p2 was attached while the group was unstable");
        Assertions.assertFalse(alignedWithCurrentRanges(f.table, p2));
        Assertions.assertFalse(index().isGroupUnstable(f.groupId),
                "the cycle in flight marks the group stable after p2 was attached");

        now[0] += SWEEP_INTERVAL_NANOS;
        checker.runOneCycle();

        assertMarkedUnstableAndAligning(f.grpId, f.table, f.table.getPartition("p2").getDefaultPhysicalPartition());
    }

    /**
     * A partition dropped before a splice keeps the old layout in the recycle bin while the group converges without
     * it; RECOVER PARTITION brings it back into the stable group.
     */
    @Test
    public void testSweepMarksGroupUnstableAfterRecoverPartition() throws Exception {
        Fixture f = createFixture("recover", null);
        starRocksAssert.alterTable("alter table " + f.tableName + " add partition p2 values less than ('200')");
        Assertions.assertEquals(2, baseIndex(f.table, "p2").getTablets().size());
        starRocksAssert.alterTable("alter table " + f.tableName + " drop partition p2");
        spliceBoundAt200AndRunChecker(f, new ColocateChecker());
        Assertions.assertFalse(index().isGroupUnstable(f.groupId), "the group converged without p2");

        starRocksAssert.ddl("recover partition p2 from " + db.getFullName() + "." + f.tableName);
        MaterializedIndex p2 = baseIndex(f.table, "p2");
        Assertions.assertEquals(2, p2.getTablets().size());
        Assertions.assertFalse(alignedWithCurrentRanges(f.table, p2), "the recovered p2 crosses k1 = 200");
        Assertions.assertFalse(index().isGroupUnstable(f.groupId), "the group is stable nevertheless");

        new ColocateChecker().runOneCycle();

        assertMarkedUnstableAndAligning(f.grpId, f.table, f.table.getPartition("p2").getDefaultPhysicalPartition());
    }

    /** The stale attach is in one database; the sweep marks the GroupId of every database of the group unstable. */
    @Test
    public void testSweepMarksEveryPeerUnstable() throws Exception {
        Fixture f = createFixture("xdb", "rc_attach_peer");
        ColocateTableIndex.GroupId peerGroupId = index().getGroup(f.peer.getId());
        Assertions.assertNotEquals(f.groupId, peerGroupId);
        Assertions.assertEquals(f.grpId, peerGroupId.grpId, "the members of both databases are one group");
        PhysicalPartition p2 = addPartitionBuiltBeforeSplice(f, new ColocateChecker());
        Assertions.assertFalse(index().isGroupUnstable(peerGroupId));

        new ColocateChecker().runOneCycle();

        Assertions.assertTrue(index().isGroupUnstable(f.groupId));
        Assertions.assertTrue(index().isGroupUnstable(peerGroupId));
        assertMarkedUnstableAndAligning(f.grpId, f.table, p2);
        Assertions.assertEquals(List.of(), reshardJobsOf(f.peer), "the peer is aligned");
    }

    // ---------------------------------------------------------------------------------------------
    // Sweep interval
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testSweepRunsAtMostOncePerInterval() throws Exception {
        Fixture f = createFixture("interval", null);
        long t0 = 0L;
        long[] now = {t0};
        ColocateChecker checker = new ColocateChecker(() -> now[0]);
        // The first cycle sweeps: the group is clean.
        checker.runOneCycle();
        Assertions.assertFalse(index().isGroupUnstable(f.groupId));

        now[0] = t0 + TimeUnit.MILLISECONDS.toNanos(500);
        PhysicalPartition p2 = addPartitionBuiltBeforeSplice(f, checker);

        now[0] = t0 + TimeUnit.SECONDS.toNanos(1);
        checker.runOneCycle();
        Assertions.assertFalse(index().isGroupUnstable(f.groupId), "no sweep is due one second after the last one");
        Assertions.assertEquals(List.of(), reshardJobsOf(f.table));

        now[0] = t0 + SWEEP_INTERVAL_NANOS;
        checker.runOneCycle();
        assertMarkedUnstableAndAligning(f.grpId, f.table, p2);
    }

    // ---------------------------------------------------------------------------------------------
    // No false positives
    // ---------------------------------------------------------------------------------------------

    /**
     * An aligned stable range group, with an empty member and a member in the middle of an alter whose latest indexes
     * are aligned, and a hash colocate group: sweeps across several intervals write no mark-unstable entry and submit
     * no job.
     */
    @Test
    public void testSweepLeavesAlignedAndHashColocateGroupsStable() throws Exception {
        List<ColocateTableIndex.GroupId> marked = recordMarkUnstableEntries();
        Fixture g = createThreeRangeGroup("aligned");
        starRocksAssert.withTable("create table a_aligned (k1 int, k2 int, v int) duplicate key(k1, k2)\n"
                + "partition by range(k2) (partition p1 values less than ('100'), partition p2 values less than ('200'))\n"
                + "order by(k1, k2) properties('replication_num' = '1', 'colocate_with' = 'g_aligned:k1')");
        starRocksAssert.withTable("create table e_aligned (k1 int, k2 int, v int) duplicate key(k1, k2)\n"
                + "partition by range(k2) ()\n"
                + "order by(k1, k2) properties('replication_num' = '1', 'colocate_with' = 'g_aligned:k1')");
        starRocksAssert.withTable("create table s_aligned (k1 int, v int) duplicate key(k1) order by(k1) "
                + "properties('replication_num' = '1', 'colocate_with' = 'g_aligned:k1')");
        starRocksAssert.dropTable("seed_aligned");
        starRocksAssert.withTable("create table h_hash (k1 int, v int) duplicate key(k1) "
                + "distributed by hash(k1) buckets 2 "
                + "properties('replication_num' = '1', 'colocate_with' = 'g_hash')");
        OlapTable aligned = table("a_aligned");
        OlapTable empty = table("e_aligned");
        OlapTable altering = table("s_aligned");
        OlapTable hash = table("h_hash");
        Assertions.assertTrue(empty.getPhysicalPartitions().isEmpty());
        long hashGrpId = index().getGroup(hash.getId()).grpId;
        Assertions.assertFalse(index().isRangeColocateGroup(index().getGroup(hash.getId())));
        Assertions.assertFalse(index().isAnyGroupWithSameColocateGroupIdUnstable(g.grpId));

        altering.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            long[] now = {0L};
            ColocateChecker checker = new ColocateChecker(() -> now[0]);
            checker.runOneCycle();
            assertSweepsLeaveGroupsStable(checker, now, marked, Set.of(g.grpId, hashGrpId),
                    List.of(aligned, empty, altering, hash));
        } finally {
            altering.setState(OlapTable.OlapTableState.NORMAL);
        }
    }

    /**
     * Answers the publish of a tablet split as a compute node does. A split at boundaries the frontend chose, such as
     * an alignment split of the colocate checker, gets those ranges back. A split that leaves the boundaries to the
     * compute node gets the ranges {@code chosenSplits} holds for its tablet.
     */
    private static void answerSplitPublishes(Map<Long, List<TabletRange>> chosenSplits) {
        new MockUp<MockLakeService>() {
            @Mock
            public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
                return CompletableFuture.completedFuture(splitPublishResponse(List.of(request), chosenSplits));
            }

            @Mock
            public Future<PublishVersionResponse> aggregatePublishVersion(AggregatePublishVersionRequest request) {
                return CompletableFuture.completedFuture(splitPublishResponse(request.publishReqs, chosenSplits));
            }
        };
    }

    private static PublishVersionResponse splitPublishResponse(List<PublishVersionRequest> requests,
                                                               Map<Long, List<TabletRange>> chosenSplits) {
        PublishVersionResponse response = new PublishVersionResponse();
        response.status = new StatusPB();
        response.status.statusCode = TStatusCode.OK.getValue();
        response.tabletRanges = new HashMap<>();
        for (PublishVersionRequest request : requests) {
            if (request.reshardingTabletInfos == null) {
                continue;
            }
            for (ReshardingTabletInfoPB info : request.reshardingTabletInfos) {
                if (info.splittingTabletInfo == null) {
                    continue;
                }
                List<Long> newTabletIds = info.splittingTabletInfo.newTabletIds;
                List<TabletRangePB> ranges = info.splittingTabletInfo.newTabletRanges;
                if (ranges == null || ranges.isEmpty()) {
                    ranges = chosenSplits.get(info.splittingTabletInfo.oldTabletId).stream()
                            .map(TabletRange::toProto).collect(Collectors.toList());
                }
                for (int i = 0; i < newTabletIds.size(); i++) {
                    response.tabletRanges.put(newTabletIds.get(i), ranges.get(i));
                }
            }
        }
        return response;
    }

    /** Runs a tablet reshard job to FINISHED. Its publish runs on the publish thread pool, so a round may find it pending. */
    private static void runReshardJobToFinished(TabletReshardJob job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (job.getJobState() != TabletReshardJob.JobState.FINISHED) {
            Assertions.assertFalse(job.isDone(), "job " + job.getJobState() + ": " + job.getErrorMessage());
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }
    }

    /**
     * Splits the tablet [100, 200) of {@code peer} at 150, submitted the way a SPLIT TABLET statement is, and runs the
     * job to FINISHED: the split adds the bound 150 to the ColocateRanges of the group and marks the group unstable.
     */
    private static void splitPeerTabletAt150(OlapTable peer, Map<Long, List<TabletRange>> chosenSplits)
            throws Exception {
        Tablet tablet = peer.getPhysicalPartitions().iterator().next().getLatestBaseIndex().getTablets().stream()
                .filter(candidate -> candidate.getRange().getRange().equals(Range.gelt(k1(100), k1(200))))
                .findFirst().orElseThrow();
        chosenSplits.put(tablet.getId(), List.of(new TabletRange(Range.gelt(k1(100), k1(150))),
                new TabletRange(Range.gelt(k1(150), k1(200)))));
        // A negative target size names the number of new tablets, whatever the size of the tablet.
        SplitTabletClause clause = new SplitTabletClause(null, new TabletList(List.of(tablet.getId())),
                Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, "-2"));
        clause.setTabletReshardTargetSize(-2);
        runReshardJobToFinished(
                GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().createTabletReshardJob(db, peer, clause));
    }

    /**
     * Runs a range rollup job of a table in a range colocate group from PENDING to FINISHED, the way the rollup handler
     * does: the loads before the watershed are reported finished, the rewrite of each partition is reported published
     * as the transaction of its INSERT would be, and the flip's publish to the compute nodes is skipped.
     */
    private static void runRangeRollupJobToFinished(LakeRangeRollupJob job) throws Exception {
        job.setSampler(request -> SampleSet.EMPTY);
        Deencapsulation.invoke(job, "runPendingJob");
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        // The first round allocates the watershed and exposes the shadow index to loads.
        Deencapsulation.invoke(job, "runWaitingTxnJob");
        new MockUp<LakeRangeRollupJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return true;
            }
        };
        Deencapsulation.invoke(job, "runWaitingTxnJob");
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        long watershedTxnId = job.getTransactionId().orElseThrow();
        Map<Long, TransactionState> rewriteTxns = new HashMap<>();
        for (long physicalPartitionId : job.getPartitionIdsForTest()) {
            long rewriteTxnId = GlobalStateMgr.getCurrentState().getNextId();
            job.setRewriteTxnIdForTest(physicalPartitionId, rewriteTxnId);
            TransactionState rewriteTxn = new TransactionState(0L, new ArrayList<>(), rewriteTxnId, "rewrite", null,
                    TransactionState.LoadJobSourceType.SHADOW_REWRITE,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"), 0L, 60_000L);
            rewriteTxn.setTransactionStatus(TransactionStatus.VISIBLE);
            InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(0L);
            attachment.setShadowRewriteWatershedTxnId(watershedTxnId);
            attachment.setShadowRewriteAlterVersion(job.getWatershedVersion(physicalPartitionId));
            rewriteTxn.setTxnCommitAttachment(attachment);
            rewriteTxns.put(rewriteTxnId, rewriteTxn);
        }
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(Invocation invocation, long dbId, long transactionId) {
                TransactionState rewriteTxn = rewriteTxns.get(transactionId);
                return rewriteTxn != null ? rewriteTxn : invocation.proceed();
            }
        };
        // Each rewrite is published already, so the job runs none. The first round places the shadow tablets into the
        // PACK shard groups of their ColocateRanges and waits; the second finds them placed.
        Deencapsulation.invoke(job, "runRunningJob");
        Deencapsulation.invoke(job, "runRunningJob");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        job.setPublishExecutor(publishingJob -> true);
        Deencapsulation.invoke(job, "runFinishedRewritingJob");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
    }

    /** Runs a lake schema change job to FINISHED through run(), the entry point of the schema change handler. */
    private static void runSchemaChangeJobToFinished(LakeTableSchemaChangeJob job) throws Exception {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(task -> task.setFinished(true));
            }
        };
        new MockUp<AlterJobV2>() {
            @Mock
            public boolean publishVersion() {
                return true;
            }
        };
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (!job.isDone()) {
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState(),
                String.valueOf(Deencapsulation.<String>getField(job, "errMsg")));
    }

    /** The only unfinished alter job of {@code t} the handler of {@code jobClass} holds. */
    private static <T extends AlterJobV2> T soleAlterJob(OlapTable t, Class<T> jobClass) {
        List<AlterJobV2> jobs = jobClass == LakeRangeRollupJob.class
                ? GlobalStateMgr.getCurrentState().getRollupHandler().getUnfinishedAlterJobV2ByTableId(t.getId())
                : GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(t.getId());
        Assertions.assertEquals(1, jobs.size());
        return Assertions.assertInstanceOf(jobClass, jobs.get(0));
    }

    /**
     * On a converged group, a user split that splices a new bound (followed by the alignment split of the other member
     * and the stable flip, with the superseded parent indexes still installed), a range rollup, and a heavy schema
     * change each leave a stable, aligned group: sweeps across several intervals after each write no mark-unstable
     * entry and submit no job. The retained parent indexes cross the new bound, so only the latest indexes may count.
     */
    @Test
    public void testSweepAfterLegitimateLayoutChangesKeepsGroupStable() throws Exception {
        List<ColocateTableIndex.GroupId> marked = recordMarkUnstableEntries();
        Fixture g = createThreeRangeGroup("legit");
        starRocksAssert.withTable("create table t_legit (k1 int, k2 int, k3 int, v1 int) duplicate key(k1, k2, k3) "
                + "order by(k1, k2, k3) properties('replication_num' = '1', 'colocate_with' = 'g_legit:k1')");
        starRocksAssert.withTable("create table t_legit_peer (k1 int, v2 int) duplicate key(k1) "
                + "properties('replication_num' = '1', 'colocate_with' = 'g_legit:k1')");
        starRocksAssert.dropTable("seed_legit");
        OlapTable table = table("t_legit");
        OlapTable peer = table("t_legit_peer");
        Assertions.assertFalse(index().isAnyGroupWithSameColocateGroupIdUnstable(g.grpId));
        long[] now = {0L};
        ColocateChecker checker = new ColocateChecker(() -> now[0]);
        checker.runOneCycle();
        Set<Long> grpIds = Set.of(g.grpId);
        List<OlapTable> members = List.of(table, peer);
        try {
            // A user split of the peer splices 150; the checker aligns the table with one split and marks the group
            // stable.
            Map<Long, List<TabletRange>> chosenSplits = new HashMap<>();
            answerSplitPublishes(chosenSplits);
            splitPeerTabletAt150(peer, chosenSplits);
            Assertions.assertEquals(4, index().getColocateRanges(g.grpId).size());
            Assertions.assertTrue(index().isAnyGroupWithSameColocateGroupIdUnstable(g.grpId));
            checker.runOneCycle();
            List<TabletReshardJob> alignmentSplits = reshardJobsOf(table);
            Assertions.assertEquals(1, alignmentSplits.size());
            runReshardJobToFinished(alignmentSplits.get(0));
            checker.runOneCycle();
            Assertions.assertFalse(index().isAnyGroupWithSameColocateGroupIdUnstable(g.grpId));
            for (OlapTable member : members) {
                PhysicalPartition partition = member.getPhysicalPartitions().iterator().next();
                List<MaterializedIndex> latest = partition.getLatestMaterializedIndices(IndexExtState.VISIBLE);
                Assertions.assertTrue(partition.getAllMaterializedIndices(IndexExtState.VISIBLE).stream()
                                .anyMatch(index -> !latest.contains(index) && !alignedWithCurrentRanges(member, index)),
                        "the superseded parent index of " + member.getName() + " stays installed across 150");
            }
            assertSweepsLeaveGroupsStable(checker, now, marked, grpIds, members);

            // A range rollup whose sort key keeps the colocate column first.
            starRocksAssert.alterTable("alter table t_legit add rollup r (k1, k3, k2) order by (k1, k3)");
            runRangeRollupJobToFinished(soleAlterJob(table, LakeRangeRollupJob.class));
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            assertSweepsLeaveGroupsStable(checker, now, marked, grpIds, members);

            // A heavy schema change of a value column: without fast schema evolution, MODIFY COLUMN rewrites the data
            // with a schema change job.
            boolean savedFastSchemaEvolution = Config.enable_fast_schema_evolution_in_share_data_mode;
            Config.enable_fast_schema_evolution_in_share_data_mode = false;
            try {
                starRocksAssert.alterTable("alter table t_legit modify column v1 bigint");
            } finally {
                Config.enable_fast_schema_evolution_in_share_data_mode = savedFastSchemaEvolution;
            }
            runSchemaChangeJobToFinished(soleAlterJob(table, LakeTableSchemaChangeJob.class));
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            assertSweepsLeaveGroupsStable(checker, now, marked, grpIds, members);
        } finally {
            GlobalStateMgr.getCurrentState().getRollupHandler().clearJobs();
            GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
        }
    }
}
