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
import com.staros.proto.PlacementPolicy;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
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
import com.starrocks.thrift.TStatusCode;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockedBackend.MockLakeService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

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

        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public <T> Future<T> submit(Callable<T> task) throws Exception {
                return CompletableFuture.completedFuture(task.call());
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

    @Test
    public void testLevelTwoSplitDoesNotChangeColocateRangeMgr() throws Exception {
        ColocateRangeMgr rangeMgr = GlobalStateMgr.getCurrentState().getColocateTableIndex().getColocateRangeMgr();
        Assertions.assertEquals(1, rangeMgr.getColocateRanges(groupId.grpId).size());

        installLakeServiceMockReturning((oldTabletId, newTabletIds) -> {
            // Two new tablets, both with WITHIN-PREFIX (non-canonical) lower bounds —
            // a Level 2 split that does NOT cross any colocate boundary.
            Map<Long, TabletRangePB> result = new HashMap<>();
            result.put(newTabletIds.get(0),
                    new TabletRange(Range.lt(makeTwoColTuple(100, 50))).toProto());
            result.put(newTabletIds.get(1),
                    new TabletRange(Range.ge(makeTwoColTuple(100, 50))).toProto());
            return result;
        });

        runSplitJob();

        Assertions.assertEquals(1, rangeMgr.getColocateRanges(groupId.grpId).size(),
                "Level 2 split must not add a ColocateRange entry");
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getColocateTableIndex().isGroupUnstable(groupId),
                "Level 2 split must leave the group stable");
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
                    new TabletRange(Range.gelt(makeTwoColTuple(50, 0), makeTwoColTuple(150, 0))).toProto());
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
        job.run();
        Assertions.assertEquals(TabletReshardJob.JobState.RUNNING, job.getJobState());
        // The classification we care about runs in the next transition (RUNNING → CLEANING).
        job.run();
        // Drain remaining states so the table returns to NORMAL.
        for (int i = 0; i < 5 && job.getJobState() != TabletReshardJob.JobState.FINISHED; i++) {
            job.run();
        }
    }
}
