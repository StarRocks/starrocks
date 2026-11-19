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

import com.google.common.collect.Multimap;
import com.staros.client.StarClientException;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
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
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.lock.LockTestUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.ReshardingTabletInfoPB;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Exercises the post-split colocate-range classification path on a range-colocate table.
 * Mocks the BE response via MockLakeService.publishVersion to return tablet ranges in the
 * specified canonical (Level 1) or within-prefix (Level 2) shape, then asserts ColocateRangeMgr
 * convergence and unstable-mark state.
 */
public class SplitTabletJobColocateTest {

    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;

    private OlapTable table;
    private ColocateTableIndex.GroupId groupId;
    private static int tableSeq = 0;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("p3_split_test").useDatabase("p3_split_test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("p3_split_test");

        LockTestUtils.fakeSynchronousExecutorOffTheCallersThread();

        // The TabletReshardJobMgr daemon ticks every 10ms and runs ColocateChecker.runOneCycle,
        // which re-marks an unstable colocate group stable as soon as every peer is range-aligned.
        // In these single-DB tests the split itself produces fully-aligned tablets, so the daemon
        // would race the test thread and clear the unstable bit before the assertion runs.
        new MockUp<ColocateChecker>() {
            @Mock
            public void runOneCycle() {
            }
        };
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Each test creates its own table so ColocateRangeMgr state from a prior test does not
        // leak across tests; the cluster shares a single ColocateTableIndex instance.
        String tableName = "t_p3_" + (++tableSeq);
        String sql = "create table " + tableName + " (k1 int, k2 int)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'p3_grp_" + tableSeq + ":k1');";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        groupId = GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(table.getId());
    }

    @AfterEach
    public void tearDown() {
        // The cluster shares one ColocateTableIndex; restore stable so the unstable bit from
        // a Level-1 or unstable-guard test does not leak into the next case.
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        if (groupId != null && idx.isGroupUnstable(groupId)) {
            idx.markGroupStable(groupId, /* needEditLog */ false);
        }
    }

    /**
     * A within-prefix (Level 2) pre-split adds no ColocateRange boundary, but because pre-split creates
     * its new shards in the SPREAD group ONLY (root fix so they spread across CNs at creation), the
     * post-publish path must still get each child into the existing owning PACK group — otherwise a
     * boundary-less split would strand them SPREAD-only and lose colocation permanently. The fix does
     * both: (1) an immediate reconcile places each child into the existing PACK group, and (2) the group
     * is marked unstable to arm the periodic ColocateChecker backstop as a safety net (a boundary-less
     * split would otherwise leave the group stable and never revisited).
     */
    @Test
    public void testLevelTwoPreSplitReconcilesChildIntoExistingPackGroup() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = idx.getColocateRangeMgr();
        long existingPackGroup = rangeMgr.getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        long spreadGroup = table.getAllPhysicalPartitions().iterator().next()
                .getLatestBaseIndex().getShardGroupId();
        Assertions.assertEquals(1, rangeMgr.getColocateRanges(groupId.grpId).size());

        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            // Two new tablets, both with WITHIN-PREFIX (non-canonical) lower bounds —
            // a Level 2 split that does NOT cross any colocate boundary (both stay in the one range).
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0),
                    new TabletRange(Range.lt(makeTwoColTuple(100, 50))).toProto());
            result.put(newTabletIds.get(1),
                    new TabletRange(Range.ge(makeTwoColTuple(100, 50))).toProto());
            return result;
        });
        // Pre-split created the children in the SPREAD group ONLY (no PACK group).
        Map<Long, List<Long>> reassignedAdd = new HashMap<>();
        Map<Long, List<Long>> reassignedRemove = new HashMap<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> infos = new ArrayList<>();
                for (long id : shardIds) {
                    infos.add(ShardInfo.newBuilder().setShardId(id).addGroupIds(spreadGroup).build());
                }
                return infos;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                reassignedAdd.put(shardId, new ArrayList<>(addGroupIds));
                reassignedRemove.put(shardId, new ArrayList<>(removeGroupIds));
            }
        };

        runSplitJob();

        Assertions.assertEquals(1, rangeMgr.getColocateRanges(groupId.grpId).size(),
                "Level 2 split must not add a ColocateRange entry");
        Assertions.assertTrue(idx.isGroupUnstable(groupId),
                "Level 2 pre-split must arm the backstop: mark the group unstable so the periodic checker "
                        + "converges the SPREAD-only children even if the immediate reconcile fails");
        Assertions.assertFalse(reassignedAdd.isEmpty(),
                "SPREAD-only pre-split children must be reconciled into the existing PACK group");
        for (Map.Entry<Long, List<Long>> e : reassignedAdd.entrySet()) {
            Assertions.assertEquals(List.of(existingPackGroup), e.getValue(),
                    "child " + e.getKey() + " must be added to the existing owning PACK group");
            Assertions.assertEquals(List.of(), reassignedRemove.get(e.getKey()),
                    "child " + e.getKey() + " had no PACK group, so nothing is removed");
        }
    }

    @Test
    public void testLevelOneSplitAddsCanonicalBoundary() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = idx.getColocateRangeMgr();
        Assertions.assertEquals(1, rangeMgr.getColocateRanges(groupId.grpId).size());

        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            // Canonical (k, NULL) lower bound on the second child marks the boundary at 100 as
            // a valid Level 1 boundary that splices into ColocateRangeMgr.
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0),
                    new TabletRange(Range.lt(makeCanonicalTuple(100))).toProto());
            result.put(newTabletIds.get(1),
                    new TabletRange(Range.ge(makeCanonicalTuple(100))).toProto());
            return result;
        });

        runSplitJob();

        List<ColocateRange> ranges = rangeMgr.getColocateRanges(groupId.grpId);
        Assertions.assertEquals(2, ranges.size(), "Level 1 split must splice a new ColocateRange");
        Assertions.assertTrue(idx.isGroupUnstable(groupId),
                "Level 1 split must mark the group unstable");
    }

    @Test
    public void testCannotSplitWhileUnstable() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        idx.markGroupUnstable(groupId, /* needEditLog */ false);

        Assertions.assertThrows(StarRocksException.class, this::createTabletReshardJob);
    }

    /**
     * Models a leader crash between {@code OP_COLOCATE_RANGE_UPDATE} and
     * {@code OP_COLOCATE_MARK_UNSTABLE_V2}: ColocateRangeMgr already has the canonical boundary,
     * but the unstable bit was never persisted. Retrying must re-mark unstable without
     * re-allocating PACK shard groups.
     */
    @Test
    public void testApplyRangeSplitResultRemarksUnstableOnRetry() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = groupId.grpId;
        Set<Tuple> canonicalLowers = new LinkedHashSet<>();
        canonicalLowers.add(makeCanonicalTuple(100));

        idx.applyRangeSplitResult(grpId, canonicalLowers, 1,
                () -> GlobalStateMgr.getCurrentState().getStarOSAgent().createShardGroup(
                        db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK));
        Assertions.assertEquals(2, idx.getColocateRangeMgr().getColocateRanges(grpId).size());
        Assertions.assertTrue(idx.isGroupUnstable(groupId));

        // Simulate the lost mark-unstable record.
        idx.markGroupStable(groupId, /* needEditLog */ false);
        Assertions.assertFalse(idx.isGroupUnstable(groupId));

        // Retry: the supplier must NOT be invoked because the boundary already exists.
        idx.applyRangeSplitResult(grpId, canonicalLowers, 1, () -> {
            throw new IllegalStateException("supplier must not run when boundary already exists");
        });
        Assertions.assertEquals(2, idx.getColocateRangeMgr().getColocateRanges(grpId).size());
        Assertions.assertTrue(idx.isGroupUnstable(groupId), "retry must re-emit OP_COLOCATE_MARK_UNSTABLE_V2");
    }

    /**
     * Multi-way Level 1 split: BE produces three children where the boundary-touching middle child
     * has a canonical lower {@code (k, NULL)} but a non-canonical upper {@code (k, x)}. The
     * boundary at {@code k} must still be detected and spliced into ColocateRangeMgr.
     */
    @Test
    public void testMultiWaySplitDetectsCanonicalLowerWithNonCanonicalUpper() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = idx.getColocateRangeMgr();
        long grpId = groupId.grpId;
        Assertions.assertEquals(1, rangeMgr.getColocateRanges(grpId).size());

        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            // 3-way split crossing the boundary at colocate prefix 100. The MIDDLE child has
            // canonical (100, NULL) lower but a within-prefix (100, 50) upper — codex's case.
            //   Child 0: [-inf,        (100, NULL))
            //   Child 1: [(100, NULL), (100, 50))    <- canonical lower, non-canonical upper
            //   Child 2: [(100, 50),   +inf)
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0),
                    new TabletRange(Range.lt(makeCanonicalTuple(100))).toProto());
            result.put(newTabletIds.get(1),
                    new TabletRange(Range.gelt(makeCanonicalTuple(100), makeTwoColTuple(100, 50))).toProto());
            result.put(newTabletIds.get(2),
                    new TabletRange(Range.ge(makeTwoColTuple(100, 50))).toProto());
            return result;
        });

        runSplitJob(-3);

        Assertions.assertEquals(2, rangeMgr.getColocateRanges(grpId).size(),
                "boundary at 100 must be spliced into ColocateRangeMgr");
        Assertions.assertTrue(idx.isGroupUnstable(groupId),
                "multi-way Level 1 split must mark the group unstable");
    }

    /**
     * Cross-DB colocate: tables in different databases sharing the same {@code grpId} must all be
     * marked unstable when one of them triggers a Level 1 split. Otherwise a peer DB would claim
     * "stable" while its tablets are mid-migration.
     */
    @Test
    public void testCrossDbPeerGroupsMarkedUnstable() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = groupId.grpId;
        long peerDbId = db.getId() + 100_000;
        long peerTableId = table.getId() + 100_000;
        ColocateTableIndex.GroupId peerGroupId = new ColocateTableIndex.GroupId(peerDbId, grpId);
        String peerName = peerDbId + "_p3_peer";

        Map<ColocateTableIndex.GroupId, ColocateGroupSchema> group2Schema =
                Deencapsulation.getField(idx, "group2Schema");
        Multimap<ColocateTableIndex.GroupId, Long> group2Tables =
                Deencapsulation.getField(idx, "group2Tables");
        Map<Long, ColocateTableIndex.GroupId> table2Group = Deencapsulation.getField(idx, "table2Group");
        Map<String, ColocateTableIndex.GroupId> groupName2Id = Deencapsulation.getField(idx, "groupName2Id");

        group2Schema.put(peerGroupId, idx.getGroupSchema(groupId));
        group2Tables.put(peerGroupId, peerTableId);
        table2Group.put(peerTableId, peerGroupId);
        groupName2Id.put(peerName, peerGroupId);
        try {
            Assertions.assertFalse(idx.isGroupUnstable(groupId));
            Assertions.assertFalse(idx.isGroupUnstable(peerGroupId));

            Set<Tuple> canonicalLowers = new LinkedHashSet<>();
            canonicalLowers.add(makeCanonicalTuple(100));
            idx.applyRangeSplitResult(grpId, canonicalLowers, 1,
                    () -> GlobalStateMgr.getCurrentState().getStarOSAgent().createShardGroup(
                            db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK));

            Assertions.assertTrue(idx.isGroupUnstable(groupId), "primary GroupId must be unstable");
            Assertions.assertTrue(idx.isGroupUnstable(peerGroupId), "peer DB GroupId must be unstable too");
        } finally {
            // Restore stable state on the peer; @AfterEach handles the primary.
            if (idx.isGroupUnstable(peerGroupId)) {
                idx.markGroupStable(peerGroupId, /* needEditLog */ false);
            }
            group2Schema.remove(peerGroupId);
            group2Tables.removeAll(peerGroupId);
            table2Group.remove(peerTableId);
            groupName2Id.remove(peerName);
        }
    }

    /**
     * BE returns a non-canonical tablet range that straddles an existing ColocateRange boundary —
     * the most likely cause is a colocate-unaware BE. Classification must mark the group unstable
     * (so the scan-time alignment guard fails closed) without adding any new boundary.
     */
    @Test
    public void testNonCanonicalCrossingMarksUnstable() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = groupId.grpId;
        ColocateRangeMgr rangeMgr = idx.getColocateRangeMgr();

        // Seed two ColocateRanges with a boundary at colocate-prefix (100).
        long firstShardGroupId = rangeMgr.getColocateRanges(grpId).get(0).getShardGroupId();
        long secondShardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().createShardGroup(
                db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK);
        Tuple boundaryPrefix = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "100")));
        rangeMgr.setColocateRanges(grpId, Arrays.asList(
                new ColocateRange(Range.lt(boundaryPrefix), firstShardGroupId),
                new ColocateRange(Range.ge(boundaryPrefix), secondShardGroupId)));

        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            // First child range non-canonically straddles the existing boundary at 100.
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0),
                    new TabletRange(Range.lt(makeTwoColTuple(150, 0))).toProto());
            result.put(newTabletIds.get(1),
                    new TabletRange(Range.ge(makeTwoColTuple(150, 0))).toProto());
            return result;
        });

        runSplitJob();

        Assertions.assertTrue(idx.isGroupUnstable(groupId),
                "non-canonical range crossing existing boundary must mark unstable");
        Assertions.assertEquals(2, rangeMgr.getColocateRanges(grpId).size(),
                "non-canonical crossing must not add a ColocateRange entry");
    }

    /**
     * A Level 1 boundary split must immediately reassign the boundary child (whose range now belongs
     * in the newly-spliced PACK group) from the old PACK group to the new one, without waiting for the
     * ColocateChecker backstop. The below-boundary child stays put.
     */
    @Test
    public void testLevelOneSplitImmediatelyReassignsBoundaryChild() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = idx.getColocateRangeMgr();
        long oldPackGroup = rangeMgr.getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        // The table's real SPREAD shard group id: a distinct StarMgr-allocated id that can never
        // collide with a PACK group id, so findMisplacedTablets correctly ignores it.
        long spreadGroup = table.getAllPhysicalPartitions().iterator().next()
                .getLatestBaseIndex().getShardGroupId();

        List<Long> orderedNewTabletIds = new ArrayList<>();
        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            orderedNewTabletIds.clear();
            orderedNewTabletIds.addAll(newTabletIds);
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0), new TabletRange(Range.lt(makeCanonicalTuple(100))).toProto());
            result.put(newTabletIds.get(1), new TabletRange(Range.ge(makeCanonicalTuple(100))).toProto());
            return result;
        });

        Map<Long, List<Long>> reassignedAdd = new HashMap<>();
        Map<Long, List<Long>> reassignedRemove = new HashMap<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> infos = new ArrayList<>();
                for (long id : shardIds) {
                    // Every child was created in the OLD PACK group (plus the SPREAD group).
                    infos.add(ShardInfo.newBuilder().setShardId(id)
                            .addGroupIds(spreadGroup).addGroupIds(oldPackGroup).build());
                }
                return infos;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                reassignedAdd.put(shardId, new ArrayList<>(addGroupIds));
                reassignedRemove.put(shardId, new ArrayList<>(removeGroupIds));
            }
        };

        runSplitJob();

        List<ColocateRange> ranges = rangeMgr.getColocateRanges(groupId.grpId);
        Assertions.assertEquals(2, ranges.size());
        long newPackGroup = ranges.get(1).getShardGroupId();       // right part of the boundary
        Assertions.assertEquals(oldPackGroup, ranges.get(0).getShardGroupId());

        long boundaryChild = orderedNewTabletIds.get(1);           // lower == canonical (100, NULL)
        long belowChild = orderedNewTabletIds.get(0);              // stays in the old range
        Assertions.assertEquals(List.of(newPackGroup), reassignedAdd.get(boundaryChild),
                "boundary child must be added to the new PACK group at split time");
        Assertions.assertEquals(List.of(oldPackGroup), reassignedRemove.get(boundaryChild),
                "boundary child must be removed from the old PACK group at split time");
        Assertions.assertFalse(reassignedAdd.containsKey(belowChild),
                "the below-boundary child stays in the old PACK group; no reassignment");
    }

    /**
     * Root fix for the pre-split single-CN clustering (StarOS #1275): a pre-split (empty source,
     * rowCount == 0 -> spreadNewShards) must create its new shards with ONLY the SPREAD group, NOT the
     * parent PACK colocate group. Creating a whole batch into one PACK group makes StarOS herd them
     * onto a single CN (PACK +10000 dwarfs SPREAD -100), which funnels the following load into
     * ChannelNum=1. The correct per-bucket PACK groups are established afterwards by the post-publish
     * reconcile (the Level-1 tests above), so colocation still converges.
     */
    @Test
    public void testPreSplitCreatesShardsWithoutPackGroup() throws Exception {
        // The freshly created table has an empty source tablet (rowCount == 0), so this is a pre-split
        // and spreadNewShards is true. Capture the parent PACK group that the old (unsplit) code would
        // have (wrongly) placed every new shard into.
        long parentPackGroup = GlobalStateMgr.getCurrentState().getColocateTableIndex()
                .getColocateRangeMgr().getColocateRanges(groupId.grpId).get(0).getShardGroupId();

        Map<Long, List<Long>> createdGroupsByShard = new HashMap<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public void createShardsForSplit(Invocation inv,
                                             Map<Long, Long> newToOldShardId,
                                             Map<Long, List<Long>> newShardIdToGroupIds,
                                             FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo,
                                             Map<String, String> properties,
                                             ComputeResource computeResource,
                                             boolean spreadNewShards) {
                Assertions.assertTrue(spreadNewShards, "empty-source split must request spread");
                for (Map.Entry<Long, List<Long>> e : newShardIdToGroupIds.entrySet()) {
                    createdGroupsByShard.put(e.getKey(), new ArrayList<>(e.getValue()));
                }
                inv.proceed();  // run the real creation so the rest of the split proceeds
            }
        };

        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0), new TabletRange(Range.lt(makeCanonicalTuple(100))).toProto());
            result.put(newTabletIds.get(1), new TabletRange(Range.ge(makeCanonicalTuple(100))).toProto());
            return result;
        });

        runSplitJob();

        Assertions.assertFalse(createdGroupsByShard.isEmpty(), "createShardsForSplit must have run");
        for (Map.Entry<Long, List<Long>> e : createdGroupsByShard.entrySet()) {
            List<Long> groups = e.getValue();
            Assertions.assertEquals(1, groups.size(),
                    "pre-split shard " + e.getKey() + " must be created with a single (SPREAD) group, "
                            + "not two (SPREAD + PACK); got " + groups);
            Assertions.assertFalse(groups.contains(parentPackGroup),
                    "pre-split shard " + e.getKey() + " must NOT be created in the parent PACK group "
                            + parentPackGroup + "; got " + groups);
        }
    }

    /**
     * BE fallback-to-identical (BE returns only the first child range -> fallbackToIdenticalTablet):
     * because a pre-split created the identical replacement in the SPREAD group only, the post-publish
     * walk must STILL collect it and reconcile it into its owning PACK group (and arm the backstop),
     * even though no boundary was spliced — otherwise the replacement is stranded SPREAD-only and its
     * colocate placement is silently lost.
     */
    @Test
    public void testPreSplitFallbackToIdenticalReconcilesIntoPackGroup() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = idx.getColocateRangeMgr();
        long existingPackGroup = rangeMgr.getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        long spreadGroup = table.getAllPhysicalPartitions().iterator().next()
                .getLatestBaseIndex().getShardGroupId();

        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            // Return ONLY the first child's range → the missing second range triggers BE identical
            // fallback (fallbackToIdenticalTablet keeps a single replacement tablet). An identical
            // replacement has the old tablet's own range, which is contained in the single owning
            // ColocateRange so the reconcile can place it; the identical flag itself is size-based
            // (newTabletIds.size() == 1), not range-based.
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0), new TabletRange().toProto());
            return result;
        });
        Map<Long, List<Long>> reassignedAdd = new HashMap<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> infos = new ArrayList<>();
                for (long id : shardIds) {
                    infos.add(ShardInfo.newBuilder().setShardId(id).addGroupIds(spreadGroup).build());
                }
                return infos;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                reassignedAdd.put(shardId, new ArrayList<>(addGroupIds));
            }
        };

        runSplitJob();

        Assertions.assertEquals(1, rangeMgr.getColocateRanges(groupId.grpId).size(),
                "identical fallback adds no ColocateRange boundary");
        Assertions.assertFalse(reassignedAdd.isEmpty(),
                "the SPREAD-only identical replacement must be reconciled into the existing PACK group");
        for (Map.Entry<Long, List<Long>> e : reassignedAdd.entrySet()) {
            Assertions.assertEquals(List.of(existingPackGroup), e.getValue(),
                    "identical replacement " + e.getKey() + " must be added to the existing owning PACK group");
        }
    }

    /**
     * A failure to create the new PACK shard group during the boundary splice (StarOSAgent.createShardGroup
     * throws) must abort the split rather than silently proceed — the splice wraps the DdlException as a
     * TabletReshardException, which run() turns into an abort.
     */
    @Test
    public void testCreateShardGroupFailureDuringSpliceAbortsSplit() throws Exception {
        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            // Level-1 canonical boundary → applyRangeSplitResult runs → its supplier createShardGroup throws.
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0), new TabletRange(Range.lt(makeCanonicalTuple(100))).toProto());
            result.put(newTabletIds.get(1), new TabletRange(Range.ge(makeCanonicalTuple(100))).toProto());
            return result;
        });
        new MockUp<StarOSAgent>() {
            @Mock
            public long createShardGroup(long dbId, long tableId, long partitionId, long indexId,
                                         PlacementPolicy placementPolicy) throws DdlException {
                throw new DdlException("mocked createShardGroup failure");
            }
        };

        TabletReshardJob job = createTabletReshardJob();
        job.init();
        for (int i = 0; i < 8 && job.getJobState() != TabletReshardJob.JobState.FINISHED
                && job.getJobState() != TabletReshardJob.JobState.ABORTED; i++) {
            job.run();
        }
        Assertions.assertNotEquals(TabletReshardJob.JobState.FINISHED, job.getJobState(),
                "createShardGroup failure during the boundary splice must NOT let the split finish");
    }

    /**
     * The immediate reassignment is a best-effort optimization: a reassignShardGroups failure must not
     * abort the already-published split. The group stays unstable so the backstop reconciles later.
     */
    @Test
    public void testImmediateReassignFailureDoesNotAbortSplit() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long oldPackGroup = idx.getColocateRangeMgr().getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        long spreadGroup = table.getAllPhysicalPartitions().iterator().next()
                .getLatestBaseIndex().getShardGroupId();

        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0), new TabletRange(Range.lt(makeCanonicalTuple(100))).toProto());
            result.put(newTabletIds.get(1), new TabletRange(Range.ge(makeCanonicalTuple(100))).toProto());
            return result;
        });
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> infos = new ArrayList<>();
                for (long id : shardIds) {
                    infos.add(ShardInfo.newBuilder().setShardId(id)
                            .addGroupIds(spreadGroup).addGroupIds(oldPackGroup).build());
                }
                return infos;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds)
                    throws DdlException {
                throw new DdlException("mocked reassign failure");
            }
        };

        TabletReshardJob job = createTabletReshardJob(-2);
        job.init();
        job.run();
        for (int i = 0; i < 6 && job.getJobState() != TabletReshardJob.JobState.FINISHED; i++) {
            job.run();
        }
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, job.getJobState(),
                "a reassign failure must not abort the published split");
        Assertions.assertEquals(2, idx.getColocateRangeMgr().getColocateRanges(groupId.grpId).size());
        Assertions.assertTrue(idx.isGroupUnstable(groupId),
                "group stays unstable so the backstop reconciles the still-misplaced child");
    }

    /**
     * A membership-read (getShardInfo) failure must also fail closed: no reassignment is issued and the
     * published split still completes.
     */
    @Test
    public void testImmediateReassignMembershipReadFailureDoesNotAbortSplit() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        boolean[] reassignCalled = {false};
        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0), new TabletRange(Range.lt(makeCanonicalTuple(100))).toProto());
            result.put(newTabletIds.get(1), new TabletRange(Range.ge(makeCanonicalTuple(100))).toProto());
            return result;
        });
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) throws StarClientException {
                throw new StarClientException(com.staros.proto.StatusCode.INVALID_ARGUMENT, "mocked read failure");
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                reassignCalled[0] = true;
            }
        };

        TabletReshardJob job = createTabletReshardJob(-2);
        job.init();
        job.run();
        for (int i = 0; i < 6 && job.getJobState() != TabletReshardJob.JobState.FINISHED; i++) {
            job.run();
        }
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, job.getJobState(),
                "a membership-read failure must not abort the published split");
        Assertions.assertFalse(reassignCalled[0],
                "fail closed: no reassignment is issued when membership cannot be read");
        Assertions.assertTrue(idx.isGroupUnstable(groupId),
                "group stays unstable so the backstop reconciles later");
    }

    /**
     * A child whose range straddles a pre-existing colocate boundary has no single well-defined PACK
     * group; the immediate reassignment must skip it (leaving it to the backstop) even though its
     * lower-prefix expectation differs from its actual membership. The contained sibling is already
     * correctly placed, so nothing is reassigned.
     */
    @Test
    public void testBoundaryCrossingChildIsNotReassigned() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = idx.getColocateRangeMgr();
        long grpId = groupId.grpId;
        StarOSAgent agent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        long spreadGroup = table.getAllPhysicalPartitions().iterator().next()
                .getLatestBaseIndex().getShardGroupId();

        // Seed a boundary at colocate-prefix 100: [-inf,100) -> packLow, [100,+inf) -> packHigh.
        long packLow = rangeMgr.getColocateRanges(grpId).get(0).getShardGroupId();
        long packHigh = agent.createShardGroup(db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK);
        Tuple boundary = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "100")));
        rangeMgr.setColocateRanges(grpId, Arrays.asList(
                new ColocateRange(Range.lt(boundary), packLow),
                new ColocateRange(Range.ge(boundary), packHigh)));

        // Legacy/mismatched BE: a child (-inf,(150,0)) straddling the boundary at 100, plus a
        // contained high child [(150,0),+inf). Both are reported in packHigh by the mock:
        //   - crossing child is NOT contained -> the filter must skip it (its lower end is -inf, which maps
        //     to packLow, so an UNFILTERED reconcile would wrongly reassign it packHigh -> packLow);
        //   - high child is contained in packHigh and already there -> correctly placed.
        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0),
                    new TabletRange(Range.lt(makeTwoColTuple(150, 0))).toProto());
            result.put(newTabletIds.get(1),
                    new TabletRange(Range.ge(makeTwoColTuple(150, 0))).toProto());
            return result;
        });
        Map<Long, List<Long>> reassignedAdd = new HashMap<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> infos = new ArrayList<>();
                for (long id : shardIds) {
                    infos.add(ShardInfo.newBuilder().setShardId(id)
                            .addGroupIds(spreadGroup).addGroupIds(packHigh).build());
                }
                return infos;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                reassignedAdd.put(shardId, new ArrayList<>(addGroupIds));
            }
        };

        runSplitJob();

        Assertions.assertTrue(reassignedAdd.isEmpty(),
                "a boundary-crossing child must not be reassigned; leave it to the backstop");
    }

    private static Tuple makeCanonicalTuple(int k1) {
        // (k1, NULL) — second sort-key column NULL-padded to match the canonical shape that
        // ColocateRangeUtils.expandToFullSortKey produces from a colocate-range bound.
        return new Tuple(Arrays.asList(
                Variant.of(IntegerType.INT, String.valueOf(k1)),
                Variant.nullVariant(IntegerType.INT)));
    }

    private static Tuple makeTwoColTuple(int k1, int k2) {
        return new Tuple(Arrays.asList(
                Variant.of(IntegerType.INT, String.valueOf(k1)),
                Variant.of(IntegerType.INT, String.valueOf(k2))));
    }

    private interface RangeProducer {
        Map<Long, TabletRangePB> produce(long oldTabletId, List<Long> newTabletIds);
    }

    private static Map<Long, TabletRangePB> applyProducer(List<ReshardingTabletInfoPB> infos,
                                                          RangeProducer producer) {
        Map<Long, TabletRangePB> result = new HashMap<>();
        if (infos == null) {
            return result;
        }
        for (ReshardingTabletInfoPB info : infos) {
            if (info.splittingTabletInfo == null) {
                continue;
            }
            result.putAll(producer.produce(
                    info.splittingTabletInfo.oldTabletId,
                    info.splittingTabletInfo.newTabletIds));
        }
        return result;
    }

    private static PublishVersionResponse okResponse() {
        PublishVersionResponse response = new PublishVersionResponse();
        response.status = new StatusPB();
        response.status.statusCode = TStatusCode.OK.getValue();
        return response;
    }

    private static void installLakeServiceMockReturning(RangeProducer producer) {
        new MockUp<MockLakeService>() {
            @Mock
            public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
                PublishVersionResponse response = okResponse();
                response.tabletRanges = applyProducer(request.reshardingTabletInfos, producer);
                return CompletableFuture.completedFuture(response);
            }

            @Mock
            public Future<PublishVersionResponse> aggregatePublishVersion(AggregatePublishVersionRequest request) {
                PublishVersionResponse response = okResponse();
                response.tabletRanges = new HashMap<>();
                for (PublishVersionRequest pubReq : request.publishReqs) {
                    response.tabletRanges.putAll(applyProducer(pubReq.reshardingTabletInfos, producer));
                }
                return CompletableFuture.completedFuture(response);
            }
        };
    }

    private TabletReshardJob createTabletReshardJob() throws Exception {
        return createTabletReshardJob(-2);
    }

    private TabletReshardJob createTabletReshardJob(long targetSize) throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex idx = physicalPartition.getLatestBaseIndex();
        long tabletId = idx.getTablets().get(0).getId();
        TabletList tabletList = new TabletList(List.of(tabletId));
        Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE,
                String.valueOf(targetSize));
        SplitTabletClause clause = new SplitTabletClause(null, tabletList, properties);
        clause.setTabletReshardTargetSize(targetSize);
        TabletReshardJobFactory factory = new SplitTabletJobFactory(db, table, clause);
        return factory.createTabletReshardJob();
    }

    private void runSplitJob() throws Exception {
        runSplitJob(-2);
    }

    private void runSplitJob(long targetSize) throws Exception {
        TabletReshardJob job = createTabletReshardJob(targetSize);
        job.init();
        job.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, job.getJobState());
        // The classification we care about runs in the next transition (RUNNING → CLEANING).
        job.run();
        // Drain remaining states so the table returns to NORMAL.
        for (int i = 0; i < 5 && job.getJobState() != TabletReshardJob.JobState.FINISHED; i++) {
            job.run();
        }
    }

    /**
     * verifyNewTabletRanges accepts every FE-supplied range whose colocate prefix is
     * covered by some ColocateRange. The default single (-inf, +inf) ColocateRange
     * covers everything, so any reasonable external-boundaries plan passes.
     */
    @Test
    public void testForExternalBoundariesAcceptsCoveredRangeColocateInput() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        Tablet oldTablet = materializedIndex.getTablets().get(0);
        long oldTabletId = oldTablet.getId();
        oldTablet.setRange(new TabletRange());

        Tuple boundaryPrefix = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "100")));
        List<TabletRange> newTabletRanges = Arrays.asList(
                new TabletRange(Range.lt(boundaryPrefix)),
                new TabletRange(Range.ge(boundaryPrefix)));

        // No exception expected — the default ColocateRange covers every colocate prefix.
        TabletReshardJob job = SplitTabletJobFactory.forExternalBoundaries(
                db, table, Map.of(oldTabletId, newTabletRanges));
        Assertions.assertNotNull(job);
    }

    /**
     * verifyNewTabletRanges throws synchronously when an FE-supplied child range's
     * colocate prefix has no covering ColocateRange. Models the failure mode where a
     * BE supplies bad boundaries on a range-colocate table whose ColocateRangeMgr does
     * not extend over the full key space.
     */
    @Test
    public void testForExternalBoundariesRejectsUncoveredColocateRange() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex materializedIndex = physicalPartition.getLatestBaseIndex();
        Tablet oldTablet = materializedIndex.getTablets().get(0);
        long oldTabletId = oldTablet.getId();
        oldTablet.setRange(new TabletRange());

        // Truncate ColocateRangeMgr to (-inf, prefix50) only — anything with prefix >= 50
        // has no covering ColocateRange.
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateRangeMgr rangeMgr = idx.getColocateRangeMgr();
        long shardGroupId = rangeMgr.getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        Tuple prefix50 = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "50")));
        rangeMgr.setColocateRanges(groupId.grpId, Arrays.asList(
                new ColocateRange(Range.lt(prefix50), shardGroupId)));

        // Middle child range has lower-bound prefix 100, which falls outside the truncated
        // ColocateRangeMgr coverage and must be rejected.
        Tuple prefix100 = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "100")));
        Tuple prefix200 = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "200")));
        List<TabletRange> newTabletRanges = Arrays.asList(
                new TabletRange(Range.lt(prefix100)),
                new TabletRange(Range.of(prefix100, prefix200, true, false)),
                new TabletRange(Range.ge(prefix200)));

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> SplitTabletJobFactory.forExternalBoundaries(
                        db, table, Map.of(oldTabletId, newTabletRanges)));
        Assertions.assertTrue(thrown.getMessage().contains("no covering ColocateRange"),
                "expected 'no covering ColocateRange' in message, got: " + thrown.getMessage());
    }

    // ---------------------------------------------------------------------------------------------
    // Split of a rollup tablet whose sort key is shorter or longer than the base sort key
    // ---------------------------------------------------------------------------------------------

    /** A tuple of INT values; a null element is a NULL. */
    private static Tuple intTuple(Integer... values) {
        List<Variant> variants = new ArrayList<>(values.length);
        for (Integer value : values) {
            variants.add(value == null ? Variant.nullVariant(IntegerType.INT) : Variant.of(IntegerType.INT, value.toString()));
        }
        return new Tuple(variants);
    }

    private List<ColocateRange> colocateRanges() {
        return GlobalStateMgr.getCurrentState().getColocateTableIndex().getColocateRanges(groupId.grpId);
    }

    /**
     * Moves this test onto a table (k1, k2, k3, v1) sorted by (k1, k2, k3) with a rollup over {@code rollupColumns}
     * sorted by all of them, and returns the rollup index. The group of this test first gets three ColocateRanges on k1,
     * (-inf, 100), [100, 200) and [200, +inf), each with its own PACK shard group; the new table joins the group aligned
     * with them and the table created by setUp leaves it. The rollup has one tablet per ColocateRange, in range order,
     * each with the range expanded in the rollup's own sort key and its shard in the index SPREAD shard group and the
     * range's PACK shard group, as partition creation lays out an index of a range colocate table. The rollup holds
     * rows, so a split of one of its tablets is an online split.
     */
    private MaterializedIndex useTableWithRollupInThreeRanges(List<String> rollupColumns) throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        StarOSAgent agent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        long firstPackGroup = colocateRanges().get(0).getShardGroupId();
        long secondPackGroup = agent.createShardGroup(db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK);
        long thirdPackGroup = agent.createShardGroup(db.getId(), table.getId(), 0L, 0L, PlacementPolicy.PACK);
        idx.getColocateRangeMgr().setColocateRanges(groupId.grpId, List.of(
                new ColocateRange(Range.lt(intTuple(100)), firstPackGroup),
                new ColocateRange(Range.gelt(intTuple(100), intTuple(200)), secondPackGroup),
                new ColocateRange(Range.ge(intTuple(200)), thirdPackGroup)));
        String seedName = table.getName();
        String tableName = seedName + "_rollup";
        starRocksAssert.withTable("create table " + tableName + " (k1 int, k2 int, k3 int, v1 int)\n"
                + "duplicate key(k1, k2, k3)\n"
                + "order by(k1, k2, k3)\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'p3_grp_" + tableSeq + ":k1');");
        // The table created by setUp has a single tablet against the three ranges: no misaligned member may stay.
        starRocksAssert.dropTable(seedName);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        Assertions.assertEquals(groupId, idx.getGroup(table.getId()));

        List<Column> schema = new ArrayList<>();
        List<Integer> sortKeyIdxes = new ArrayList<>();
        List<Integer> sortKeyUniqueIds = new ArrayList<>();
        for (String columnName : rollupColumns) {
            Column column = new Column(table.getColumn(columnName));
            column.setIsKey(true);
            sortKeyIdxes.add(schema.size());
            sortKeyUniqueIds.add(column.getUniqueId());
            schema.add(column);
        }
        long rollupMetaId = GlobalStateMgr.getCurrentState().getNextId();
        table.setIndexMeta(rollupMetaId, "r_" + rollupColumns.size(), schema, 0, 0, (short) Math.min(schema.size(), 3),
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, sortKeyIdxes, sortKeyUniqueIds);
        List<Column> rollupSortKey = MetaUtils.getRangeDistributionColumns(table, rollupMetaId);
        Assertions.assertEquals(rollupColumns.size(), rollupSortKey.size());

        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        long spreadGroup = agent.createShardGroup(db.getId(), table.getId(), partition.getId(), rollupMetaId);
        MaterializedIndex rollup = new MaterializedIndex(rollupMetaId, rollupMetaId,
                MaterializedIndex.IndexState.NORMAL, spreadGroup);
        TabletMeta tabletMeta = new TabletMeta(db.getId(), table.getId(), partition.getId(), rollup.getId(),
                TStorageMedium.HDD, true);
        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(partition.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(rollup.getId()));
        for (ColocateRange colocateRange : colocateRanges()) {
            long shardId = agent.createShards(1, table.getPartitionFilePathInfo(partition.getId()),
                    table.getPartitionFileCacheInfo(partition.getId()),
                    List.of(spreadGroup, colocateRange.getShardGroupId()), null, properties,
                    WarehouseManager.DEFAULT_RESOURCE).get(0);
            rollup.addTablet(new LakeTablet(shardId, new TabletRange(ColocateRangeUtils.expandToFullSortKey(
                    colocateRange.getRange(), rollupSortKey, 1))), tabletMeta);
        }
        rollup.setRowCount(100);
        partition.createRollupIndex(rollup);
        Assertions.assertFalse(idx.isGroupUnstable(groupId));
        return rollup;
    }

    /** Has the publish split every tablet into {@code left} and {@code right}; returns the new tablet ids in order. */
    private static List<Long> installTwoChildSplit(Range<Tuple> left, Range<Tuple> right) {
        List<Long> children = new ArrayList<>();
        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            children.clear();
            children.addAll(newTabletIds);
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0), new TabletRange(left).toProto());
            result.put(newTabletIds.get(1), new TabletRange(right).toProto());
            return result;
        });
        return children;
    }

    /**
     * Has StarOS report every shard in exactly {@code reportedGroupIds}, and records the shard groups the immediate PACK
     * reconcile after the split adds to ({@code added}) and removes from ({@code removed}) each shard.
     */
    private static void mockShardGroupMembership(List<Long> reportedGroupIds, Map<Long, List<Long>> added,
                                                 Map<Long, List<Long>> removed) {
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId) {
                List<ShardInfo> infos = new ArrayList<>();
                for (long id : shardIds) {
                    infos.add(ShardInfo.newBuilder().setShardId(id).addAllGroupIds(reportedGroupIds).build());
                }
                return infos;
            }

            @Mock
            public void reassignShardGroups(long shardId, List<Long> addGroupIds, List<Long> removeGroupIds) {
                added.put(shardId, new ArrayList<>(addGroupIds));
                removed.put(shardId, new ArrayList<>(removeGroupIds));
            }
        };
    }

    /** Splits {@code tabletId} of the current table in two and runs the job to FINISHED. */
    private void runSplitJobOfTablet(long tabletId) throws Exception {
        Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, "-2");
        SplitTabletClause clause = new SplitTabletClause(null, new TabletList(List.of(tabletId)), properties);
        clause.setTabletReshardTargetSize(-2);
        TabletReshardJob job = new SplitTabletJobFactory(db, table, clause).createTabletReshardJob();
        job.init();
        for (int i = 0; i < 8 && job.getJobState() != TabletReshardJob.JobState.FINISHED; i++) {
            job.run();
        }
        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, job.getJobState());
    }

    /**
     * A within-prefix split of a tablet of a rollup sorted by (k1, k2), shorter than the base sort key, is judged with
     * the rollup's own sort key, like a split of the base index: the child that starts at the lower bound of its
     * ColocateRange lies inside the range, so the group stays stable and both children are placed into the range's PACK
     * group.
     */
    @Test
    public void testWithinPrefixSplitOfShorterRollupKeepsGroupStableAndPlacesChildren() throws Exception {
        MaterializedIndex rollup = useTableWithRollupInThreeRanges(List.of("k1", "k2"));
        Tablet parent = rollup.getTablets().get(1);
        Assertions.assertEquals(Range.gelt(intTuple(100, null), intTuple(200, null)), parent.getRange().getRange());
        long packGroup = colocateRanges().get(1).getShardGroupId();
        List<Long> children = installTwoChildSplit(Range.gelt(intTuple(100, null), intTuple(150, 0)),
                Range.gelt(intTuple(150, 0), intTuple(200, null)));
        Map<Long, List<Long>> added = new HashMap<>();
        mockShardGroupMembership(List.of(rollup.getShardGroupId()), added, new HashMap<>());

        runSplitJobOfTablet(parent.getId());

        Assertions.assertEquals(3, colocateRanges().size(), "a within-prefix split adds no ColocateRange");
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getColocateTableIndex().isGroupUnstable(groupId),
                "a within-prefix split of a rollup tablet must leave the group stable");
        Assertions.assertEquals(2, children.size());
        for (long child : children) {
            Assertions.assertEquals(List.of(packGroup), added.get(child),
                    "child " + child + " must be placed into the PACK group of its ColocateRange");
        }
    }

    /**
     * The same for a rollup sorted by (k1, k2, k3, v1), longer than the base sort key: the child that ends at the upper
     * bound of its ColocateRange lies inside the range, so the group stays stable and both children are placed.
     */
    @Test
    public void testWithinPrefixSplitOfLongerRollupKeepsGroupStable() throws Exception {
        MaterializedIndex rollup = useTableWithRollupInThreeRanges(List.of("k1", "k2", "k3", "v1"));
        Tablet parent = rollup.getTablets().get(1);
        Assertions.assertEquals(Range.gelt(intTuple(100, null, null, null), intTuple(200, null, null, null)),
                parent.getRange().getRange());
        long packGroup = colocateRanges().get(1).getShardGroupId();
        List<Long> children = installTwoChildSplit(Range.gelt(intTuple(100, null, null, null), intTuple(150, 0, 0, 0)),
                Range.gelt(intTuple(150, 0, 0, 0), intTuple(200, null, null, null)));
        Map<Long, List<Long>> added = new HashMap<>();
        mockShardGroupMembership(List.of(rollup.getShardGroupId()), added, new HashMap<>());

        runSplitJobOfTablet(parent.getId());

        Assertions.assertEquals(3, colocateRanges().size(), "a within-prefix split adds no ColocateRange");
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getColocateTableIndex().isGroupUnstable(groupId),
                "a within-prefix split of a rollup tablet must leave the group stable");
        Assertions.assertEquals(2, children.size());
        for (long child : children) {
            Assertions.assertEquals(List.of(packGroup), added.get(child),
                    "child " + child + " must be placed into the PACK group of its ColocateRange");
        }
    }

    /**
     * A child of a rollup tablet whose lower bound is canonical in the rollup's own sort key, (150, NULL) for the sort
     * key (k1, k2), introduces a ColocateRange boundary like a canonical child of the base index: the boundary is
     * spliced, the group is marked unstable so the checker aligns every member, and the child moves to the PACK group of
     * the new ColocateRange.
     */
    @Test
    public void testCanonicalLowerBoundOfRollupChildSplicesColocateRange() throws Exception {
        MaterializedIndex rollup = useTableWithRollupInThreeRanges(List.of("k1", "k2"));
        Tablet parent = rollup.getTablets().get(1);
        long packGroup = colocateRanges().get(1).getShardGroupId();
        List<Long> children = installTwoChildSplit(Range.gelt(intTuple(100, null), intTuple(150, null)),
                Range.gelt(intTuple(150, null), intTuple(200, null)));
        Map<Long, List<Long>> added = new HashMap<>();
        Map<Long, List<Long>> removed = new HashMap<>();
        mockShardGroupMembership(List.of(rollup.getShardGroupId(), packGroup), added, removed);

        runSplitJobOfTablet(parent.getId());

        List<ColocateRange> ranges = colocateRanges();
        Assertions.assertEquals(4, ranges.size(), "the canonical lower bound (150, NULL) must splice a ColocateRange");
        Assertions.assertEquals(Range.gelt(intTuple(150), intTuple(200)), ranges.get(2).getRange());
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getColocateTableIndex().isGroupUnstable(groupId));
        Assertions.assertEquals(2, children.size());
        Assertions.assertEquals(List.of(ranges.get(2).getShardGroupId()), added.get(children.get(1)));
        Assertions.assertEquals(List.of(packGroup), removed.get(children.get(1)));
        Assertions.assertFalse(added.containsKey(children.get(0)), "the child below the new boundary stays in place");
    }
}
