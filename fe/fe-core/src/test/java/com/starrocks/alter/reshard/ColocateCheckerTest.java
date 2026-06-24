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

import com.staros.client.StarClientException;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exercises {@link ColocateChecker}'s core decision paths:
 *   - unstable + already-aligned group is marked stable in one cycle;
 *   - unstable + misaligned partition triggers an alignment {@link SplitTabletJob};
 *   - no unstable groups => no work.
 *
 * <p>Calls {@link ColocateChecker#runOneCycle()} directly so the tests focus on the
 * checking body rather than the host {@link TabletReshardJobMgr}'s scheduler.
 */
public class ColocateCheckerTest {

    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;

    private OlapTable table;
    private ColocateTableIndex.GroupId groupId;
    private static final AtomicInteger NEXT_TABLE_SEQ = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("p4_align_test").useDatabase("p4_align_test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("p4_align_test");
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Each test gets a fresh colocate group so prior tests' ColocateRangeMgr mutations do
        // not leak across tests; the cluster shares one ColocateTableIndex instance.
        int seq = NEXT_TABLE_SEQ.incrementAndGet();
        String tableName = "t_p4_align_" + seq;
        String sql = "create table " + tableName + " (k1 int, k2 int)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'p4_align_grp_" + seq + ":k1');";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        groupId = GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(table.getId());
    }

    @AfterEach
    public void tearDown() {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        if (groupId != null && colocateTableIndex.isGroupUnstable(groupId)) {
            colocateTableIndex.markGroupStable(groupId, /* needEditLog */ false);
        }
    }

    @Test
    public void testAlignedUnstableGroupBecomesStable() throws Exception {
        // The freshly-created table has one tablet covering the single default ColocateRange
        // [MIN, MAX) -> already aligned. Marking the group unstable then running the checker
        // must find every partition aligned and mark every peer GroupId stable. F7 also gates the
        // flip on StarOS placement convergence, so stub it converged.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Boolean> queryShardGroupStable(List<Long> shardGroupIds, long workerGroupId) {
                return Collections.nCopies(shardGroupIds.size(), Boolean.TRUE);
            }
        };
        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId));

        ColocateChecker checker = new ColocateChecker();
        checker.runOneCycle();

        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId),
                "aligned + membership-settled + placement-converged unstable group must be marked stable after one cycle");
    }

    private long firstVisibleTabletId() {
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            for (MaterializedIndex index : physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    return tablet.getId();
                }
            }
        }
        return -1;
    }

    private long firstVisibleIndexSpreadGroup() {
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            for (MaterializedIndex index : physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                return index.getShardGroupId();
            }
        }
        return -1;
    }

    @Test
    public void testReconcileAddsMissingPackGroupAndStaysUnstable() throws Exception {
        // The fresh single-range group is range-aligned. Stub getShardInfo so the lone tablet is
        // missing its expected PACK group: the checker must issue an add-only reassign AND keep the
        // group unstable (placement not yet confirmed) so a later cycle re-reads before stabilizing.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long expectedPackGroup = colocateTableIndex.getColocateRangeMgr()
                .getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        long tabletId = firstVisibleTabletId();
        final long stubSpreadGroup = firstVisibleIndexSpreadGroup();

        Map<Long, List<Long>> reassignedAdd = new HashMap<>();
        Map<Long, List<Long>> reassignedRemove = new HashMap<>();
        boolean[] convergenceQueried = {false};
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> result = new ArrayList<>();
                for (long id : shardIds) {
                    // group_ids deliberately omits the expected PACK group -> tablet looks misplaced.
                    result.add(ShardInfo.newBuilder().setShardId(id).addGroupIds(stubSpreadGroup).build());
                }
                return result;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                reassignedAdd.put(shardId, new ArrayList<>(addGroupIds));
                reassignedRemove.put(shardId, new ArrayList<>(removeGroupIds));
            }

            @Mock
            public List<Boolean> queryShardGroupStable(List<Long> shardGroupIds, long workerGroupId) {
                convergenceQueried[0] = true;
                return Collections.nCopies(shardGroupIds.size(), Boolean.TRUE);
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        new ColocateChecker().runOneCycle();

        Assertions.assertEquals(List.of(expectedPackGroup), reassignedAdd.get(tabletId),
                "missing-PACK tablet must be reassigned to add its expected PACK group");
        Assertions.assertEquals(List.of(), reassignedRemove.get(tabletId),
                "no stale PACK group to remove in the add-only case");
        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                "group must stay unstable until a re-read confirms the repaired placement");
        Assertions.assertFalse(convergenceQueried[0],
                "convergence must not be queried while PACK membership is still being repaired");
    }

    @Test
    public void testReconcileBecomesStableAfterRepair() throws Exception {
        // Simulate the real flow: the reassign applies the membership synchronously on StarMgr, so
        // the next cycle's getShardInfo reflects it. The group must flip stable on that second cycle.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long expectedPackGroup = colocateTableIndex.getColocateRangeMgr()
                .getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        final long stubSpreadGroup = firstVisibleIndexSpreadGroup();
        boolean[] repaired = {false};
        int[] reassignCount = {0};
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> result = new ArrayList<>();
                for (long id : shardIds) {
                    ShardInfo.Builder b = ShardInfo.newBuilder().setShardId(id).addGroupIds(stubSpreadGroup);
                    if (repaired[0]) {
                        b.addGroupIds(expectedPackGroup);
                    }
                    result.add(b.build());
                }
                return result;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                reassignCount[0]++;
                repaired[0] = true;
            }

            @Mock
            public List<Boolean> queryShardGroupStable(List<Long> shardGroupIds, long workerGroupId) {
                return Collections.nCopies(shardGroupIds.size(), Boolean.TRUE);
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        // The first cycle issues the reassign (which flips the stub to "repaired"); a later cycle
        // re-reads the now-correct membership and flips the group stable. Run two cycles to drive
        // convergence and assert the end state — the shared background scheduler may also tick, so
        // the transient unstable state and exact reassign count are not asserted here (the
        // stays-unstable-until-repaired behavior is covered by
        // testReconcileAddsMissingPackGroupAndStaysUnstable).
        new ColocateChecker().runOneCycle();
        new ColocateChecker().runOneCycle();
        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId),
                "group must become stable once the re-read shows the repaired placement and placement converged");
        Assertions.assertTrue(reassignCount[0] >= 1, "a reassign must have been issued to repair placement");
    }

    @Test
    public void testReconcileStaysUnstableOnGetShardInfoFailure() throws Exception {
        // A membership-read failure must NOT mark the group stable (else a mis-placement could leak);
        // it stays unstable and is retried on the next tick.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) throws StarClientException {
                throw new StarClientException(com.staros.proto.StatusCode.INVALID_ARGUMENT, "mocked read failure");
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        new ColocateChecker().runOneCycle();

        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                "a getShardInfo failure must leave the group unstable for retry");
    }

    @Test
    public void testReconcileStaysUnstableOnReassignFailure() throws Exception {
        // A reassign RPC failure must keep the group unstable so the repair is retried.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        final long stubSpreadGroup = firstVisibleIndexSpreadGroup();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> result = new ArrayList<>();
                for (long id : shardIds) {
                    result.add(ShardInfo.newBuilder().setShardId(id).addGroupIds(stubSpreadGroup).build());
                }
                return result;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds)
                    throws DdlException {
                throw new DdlException("mocked reassign failure");
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        new ColocateChecker().runOneCycle();

        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                "a reassign failure must leave the group unstable for retry");
    }

    @Test
    public void testReconcileRemovesStalePackGroup() throws Exception {
        // Build a 2-range aligned layout ([MIN,100)->P0, [100,MAX)->P1) with one tablet per range,
        // then stub the first tablet as sitting in P1 (a sibling PACK group) instead of its expected
        // P0. The reassign must both add P0 and remove the stale P1.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = colocateTableIndex.getColocateRangeMgr();
        long packForLow = rangeMgr.getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        long packForHigh = GlobalStateMgr.getCurrentState().getStarOSAgent()
                .createShardGroup(db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK);
        Tuple boundary = intPrefix(100);
        rangeMgr.setColocateRanges(groupId.grpId, Arrays.asList(
                new ColocateRange(Range.lt(boundary), packForLow),
                new ColocateRange(Range.ge(boundary), packForHigh)));

        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);
        MaterializedIndex index = table.getPhysicalPartitions().iterator().next()
                .getLatestMaterializedIndices(IndexExtState.VISIBLE).iterator().next();
        Tablet lowTablet = index.getTablets().iterator().next();
        lowTablet.setRange(new TabletRange(
                ColocateRangeUtils.expandToFullSortKey(Range.lt(boundary), sortKeyColumns, 1)));
        long highTabletId = 900000001L;
        LakeTablet highTablet = new LakeTablet(highTabletId, new TabletRange(
                ColocateRangeUtils.expandToFullSortKey(Range.ge(boundary), sortKeyColumns, 1)));
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        index.addTablet(highTablet, new TabletMeta(db.getId(), table.getId(), physicalPartitionId,
                index.getId(), TStorageMedium.HDD, true));
        long lowTabletId = lowTablet.getId();

        Map<Long, List<Long>> reassignedAdd = new HashMap<>();
        Map<Long, List<Long>> reassignedRemove = new HashMap<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> result = new ArrayList<>();
                for (long id : shardIds) {
                    // Both tablets report membership in P1; only the low tablet (expected P0) is stale.
                    result.add(ShardInfo.newBuilder().setShardId(id).addGroupIds(packForHigh).build());
                }
                return result;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                reassignedAdd.put(shardId, new ArrayList<>(addGroupIds));
                reassignedRemove.put(shardId, new ArrayList<>(removeGroupIds));
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        new ColocateChecker().runOneCycle();

        Assertions.assertEquals(List.of(packForLow), reassignedAdd.get(lowTabletId),
                "stale tablet must be added to its expected PACK group P0");
        Assertions.assertEquals(List.of(packForHigh), reassignedRemove.get(lowTabletId),
                "stale tablet must be removed from the sibling PACK group P1");
    }

    @Test
    public void testReconcileSendsRemoveOnlyWhenAlreadyInExpectedGroup() throws Exception {
        // Double-membership (e.g. after a partial prior repair): the low tablet is already in its
        // expected PACK group P0 but also still in the sibling P1. The reassign must be remove-only
        // (empty add) so it does not redundantly re-add P0.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = colocateTableIndex.getColocateRangeMgr();
        long packForLow = rangeMgr.getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        long packForHigh = GlobalStateMgr.getCurrentState().getStarOSAgent()
                .createShardGroup(db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK);
        Tuple boundary = intPrefix(100);
        rangeMgr.setColocateRanges(groupId.grpId, Arrays.asList(
                new ColocateRange(Range.lt(boundary), packForLow),
                new ColocateRange(Range.ge(boundary), packForHigh)));

        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);
        MaterializedIndex index = table.getPhysicalPartitions().iterator().next()
                .getLatestMaterializedIndices(IndexExtState.VISIBLE).iterator().next();
        Tablet lowTablet = index.getTablets().iterator().next();
        lowTablet.setRange(new TabletRange(
                ColocateRangeUtils.expandToFullSortKey(Range.lt(boundary), sortKeyColumns, 1)));
        long highTabletId = 900000002L;
        LakeTablet highTablet = new LakeTablet(highTabletId, new TabletRange(
                ColocateRangeUtils.expandToFullSortKey(Range.ge(boundary), sortKeyColumns, 1)));
        long physicalPartitionId = table.getPhysicalPartitions().iterator().next().getId();
        index.addTablet(highTablet, new TabletMeta(db.getId(), table.getId(), physicalPartitionId,
                index.getId(), TStorageMedium.HDD, true));
        long lowTabletId = lowTablet.getId();

        Map<Long, List<Long>> reassignedAdd = new HashMap<>();
        Map<Long, List<Long>> reassignedRemove = new HashMap<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> result = new ArrayList<>();
                for (long id : shardIds) {
                    ShardInfo.Builder builder = ShardInfo.newBuilder().setShardId(id).addGroupIds(packForHigh);
                    if (id == lowTabletId) {
                        // already in its expected P0 AND still in the stale sibling P1
                        builder.addGroupIds(packForLow);
                    }
                    result.add(builder.build());
                }
                return result;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                reassignedAdd.put(shardId, new ArrayList<>(addGroupIds));
                reassignedRemove.put(shardId, new ArrayList<>(removeGroupIds));
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        new ColocateChecker().runOneCycle();

        Assertions.assertEquals(List.of(), reassignedAdd.get(lowTabletId),
                "must not re-add the already-present expected PACK group");
        Assertions.assertEquals(List.of(packForHigh), reassignedRemove.get(lowTabletId),
                "must remove the stale sibling PACK group");
    }

    @Test
    public void testStaysUnstableWhenPlacementNotConverged() throws Exception {
        // Fresh single-range group is range-aligned and membership-settled (embedded StarMgr), but
        // StarOS reports the PACK group has NOT converged onto co-resident workers -> stay unstable.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Boolean> queryShardGroupStable(List<Long> shardGroupIds, long workerGroupId) {
                return Collections.nCopies(shardGroupIds.size(), Boolean.FALSE);
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        new ColocateChecker().runOneCycle();

        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                "group must stay unstable until StarOS reports placement converged");
    }

    @Test
    public void testBecomesStableWhenPlacementConverged() throws Exception {
        // Fresh single-range group is range-aligned + membership-settled, and StarOS now reports the
        // PACK group converged -> the group flips stable. Also assert the OSS worker group (0L) is the
        // one queried (Option B resolves to the default warehouse worker group).
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long[] capturedWorkerGroupId = {-1L};
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Boolean> queryShardGroupStable(List<Long> shardGroupIds, long workerGroupId) {
                capturedWorkerGroupId[0] = workerGroupId;
                return Collections.nCopies(shardGroupIds.size(), Boolean.TRUE);
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        new ColocateChecker().runOneCycle();

        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId),
                "aligned + membership-settled + placement-converged group must be marked stable");
        Assertions.assertEquals(StarOSAgent.DEFAULT_WORKER_GROUP_ID, capturedWorkerGroupId[0],
                "F7 must query the default warehouse worker group in OSS");
    }

    @Test
    public void testStaysUnstableOnConvergenceQueryFailure() throws Exception {
        // A queryShardGroupStable RPC failure must NOT flip the group stable (fail-closed).
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Boolean> queryShardGroupStable(List<Long> shardGroupIds, long workerGroupId)
                    throws StarClientException {
                throw new StarClientException(com.staros.proto.StatusCode.INTERNAL, "mocked query failure");
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        new ColocateChecker().runOneCycle();

        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                "a placement-convergence query failure must leave the group unstable for retry");
    }

    @Test
    public void testStaysUnstableOnShortConvergenceResponse() throws Exception {
        // A short/empty response (size != requested) must fail closed so a subset can never flip the
        // group stable while a group is still migrating.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Boolean> queryShardGroupStable(List<Long> shardGroupIds, long workerGroupId) {
                return new ArrayList<>(); // empty response for a non-empty request
            }
        };

        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
        new ColocateChecker().runOneCycle();

        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                "a short convergence response must leave the group unstable");
    }

    @Test
    public void testMisalignedTabletTriggersAlignmentJob() throws Exception {
        // Splice a synthetic second ColocateRange entry at colocate-prefix 100 to create
        // misalignment: the table's single tablet spans [MIN, MAX) but two ColocateRanges now exist.
        // The checker must (a) detect misalignment, (b) compute the canonical boundary at k1=100,
        // and (c) submit one alignment SplitTabletJob.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        spliceSecondColocateRangeAt100AndMarkUnstable();

        ColocateChecker checker = new ColocateChecker();
        checker.runOneCycle();

        // Filter by this test's table id: the host TabletReshardJobMgr scheduler may have
        // ticked concurrently and submitted its own alignment job for the same unstable group,
        // so the total job count can be >= 1 — assert at least one alignment job exists for
        // *our* table, which is sufficient evidence the checker did its work.
        Assertions.assertTrue(countAlignmentJobsForTable(table.getId()) >= 1,
                "misaligned partition must trigger at least one alignment SplitTabletJob");

        // Group must remain unstable: alignment job is pending; only after every peer is aligned
        // does the checker mark stable. Submitting the job is not enough.
        Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                "group must remain unstable until the alignment job actually publishes");
    }

    @Test
    public void testTableInNonNormalStateIsSkipped() throws Exception {
        // A misaligned partition would normally trigger an alignment job, but if the table
        // is mid-alter (state != NORMAL) the checker must skip it this cycle to avoid the
        // SplitTabletJob.setTableState(NORMAL -> TABLET_RESHARD) precondition failure, and
        // must leave the group unstable so a later cycle retries.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        spliceSecondColocateRangeAt100AndMarkUnstable();

        OlapTable.OlapTableState originalState = table.getState();
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            long ourJobsBefore = countAlignmentJobsForTable(table.getId());

            ColocateChecker checker = new ColocateChecker();
            checker.runOneCycle();

            long ourJobsAfter = countAlignmentJobsForTable(table.getId());
            Assertions.assertEquals(ourJobsBefore, ourJobsAfter,
                    "non-NORMAL table state must skip alignment job submission");
            Assertions.assertTrue(colocateTableIndex.isGroupUnstable(groupId),
                    "group must remain unstable so a later cycle retries once state returns to NORMAL");
        } finally {
            table.setState(originalState);
        }
    }

    private static long countAlignmentJobsForTable(long tableId) {
        return GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJobs().values().stream()
                .filter(j -> j.getJobType() == TabletReshardJob.JobType.SPLIT_TABLET)
                .filter(j -> j instanceof SplitTabletJob)
                .map(j -> (SplitTabletJob) j)
                .filter(j -> j.getTableId() == tableId)
                .count();
    }

    /**
     * Synthetically splices a second {@link ColocateRange} entry at colocate-prefix 100 and
     * marks the group unstable. The table starts with a single tablet spanning {@code [MIN, MAX)}
     * — afterwards there are two ColocateRanges {@code [MIN, 100)} / {@code [100, MAX)} but the
     * tablet still spans the whole space, so the layout is misaligned against the group's
     * expected ranges.
     */
    private void spliceSecondColocateRangeAt100AndMarkUnstable() throws DdlException {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = colocateTableIndex.getColocateRangeMgr();
        long colocateGroupId = groupId.grpId;
        long firstShardGroupId = rangeMgr.getColocateRanges(colocateGroupId).get(0).getShardGroupId();
        long secondShardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().createShardGroup(
                db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK);
        Tuple boundaryPrefix = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "100")));
        rangeMgr.setColocateRanges(colocateGroupId, Arrays.asList(
                new ColocateRange(Range.lt(boundaryPrefix), firstShardGroupId),
                new ColocateRange(Range.ge(boundaryPrefix), secondShardGroupId)));
        colocateTableIndex.markGroupUnstable(groupId, /* needEditLog */ false);
    }

    // ---- Misplaced-PACK-group detection (pure, no cluster) ----
    //
    // These exercise the read-only detection logic that reassignShardGroups consumes. They build
    // ColocateRange lists and per-tablet group-id lists directly, so they do not depend on the
    // cluster spun up in beforeClass.

    private static final long SPREAD = 900L;
    private static final long PACK_G1 = 1001L;
    private static final long PACK_G2 = 1002L;

    private static Tuple intPrefix(int v) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(v))));
    }

    /** Two colocate ranges split at prefix 100: [MIN, 100) -> G1, [100, MAX) -> G2. */
    private static List<ColocateRange> twoRangesAt100() {
        Tuple boundary = intPrefix(100);
        return Arrays.asList(
                new ColocateRange(Range.lt(boundary), PACK_G1),
                new ColocateRange(Range.ge(boundary), PACK_G2));
    }

    @Test
    public void testClassifyCorrectPlacementReturnsNull() {
        Set<Long> packGroupIds = Set.of(PACK_G1, PACK_G2);
        // Tablet sits in its SPREAD group and the expected PACK group G2, nothing stale.
        Assertions.assertNull(ColocateChecker.classifyTabletPlacement(
                77L, List.of(SPREAD, PACK_G2), PACK_G2, packGroupIds));
    }

    @Test
    public void testClassifyStalePackGroupIsMisplaced() {
        Set<Long> packGroupIds = Set.of(PACK_G1, PACK_G2);
        // Originating-partition case: tablet stayed in old PACK group G1 but belongs in G2.
        Assertions.assertEquals(new ColocateChecker.MisplacedTablet(77L, PACK_G1, PACK_G2),
                ColocateChecker.classifyTabletPlacement(77L, List.of(SPREAD, PACK_G1), PACK_G2, packGroupIds));
    }

    @Test
    public void testClassifyMissingPackGroupHasNoCurrent() {
        Set<Long> packGroupIds = Set.of(PACK_G1, PACK_G2);
        // Tablet is in no PACK group at all -> needs an add, no remove.
        Assertions.assertEquals(
                new ColocateChecker.MisplacedTablet(77L, PhysicalPartition.INVALID_SHARD_GROUP_ID, PACK_G2),
                ColocateChecker.classifyTabletPlacement(77L, List.of(SPREAD), PACK_G2, packGroupIds));
    }

    @Test
    public void testClassifyDoubleMembershipIsMisplaced() {
        Set<Long> packGroupIds = Set.of(PACK_G1, PACK_G2);
        // Transient double membership: in expected G2 but also still in stale G1 -> must shed G1.
        Assertions.assertEquals(new ColocateChecker.MisplacedTablet(77L, PACK_G1, PACK_G2),
                ColocateChecker.classifyTabletPlacement(
                        77L, List.of(SPREAD, PACK_G1, PACK_G2), PACK_G2, packGroupIds));
    }

    @Test
    public void testFindMisplacedTabletsComposesRangeAndMembership() {
        List<ColocateRange> ranges = twoRangesAt100();
        Map<Long, Range<Tuple>> tabletIdToRange = new HashMap<>();
        tabletIdToRange.put(10L, Range.ge(intPrefix(50)));    // expected G1
        tabletIdToRange.put(20L, Range.ge(intPrefix(150)));   // expected G2
        Map<Long, List<Long>> tabletIdToGroupIds = new HashMap<>();
        tabletIdToGroupIds.put(10L, List.of(SPREAD, PACK_G1)); // correctly placed
        tabletIdToGroupIds.put(20L, List.of(SPREAD, PACK_G1)); // stuck in G1, belongs in G2

        List<ColocateChecker.MisplacedTablet> result = ColocateChecker.findMisplacedTablets(
                tabletIdToRange, tabletIdToGroupIds, ranges, 1);

        Assertions.assertEquals(
                List.of(new ColocateChecker.MisplacedTablet(20L, PACK_G1, PACK_G2)), result);
    }

    @Test
    public void testNoUnstableGroupNoOp() throws Exception {
        // With no unstable groups, the checker must produce no work even with the table present.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId));

        int jobsBefore = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr()
                .getTabletReshardJobs().size();

        ColocateChecker checker = new ColocateChecker();
        checker.runOneCycle();

        Assertions.assertFalse(colocateTableIndex.isGroupUnstable(groupId));
        Assertions.assertEquals(jobsBefore,
                GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJobs().size(),
                "no unstable groups => no alignment jobs submitted");
    }
}
