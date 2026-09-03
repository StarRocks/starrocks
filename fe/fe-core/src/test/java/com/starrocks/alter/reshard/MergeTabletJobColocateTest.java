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
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.TabletGroupList;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * Merge must preserve the range-colocate contract: a merge group never spans a
 * {@link ColocateRange}, and every shard a merge creates joins its range's PACK shard group.
 *
 * <p>The fixture registers THREE colocate ranges and puts the mergeable pair in the MIDDLE one, so
 * an implementation that always answered with the first (or the last) colocate range cannot pass.
 */
public class MergeTabletJobColocateTest {

    private static final long TARGET_SIZE = 100L;
    // Well under mergePairThreshold(100) = 80, so any adjacent pair is a merge candidate on size.
    private static final long SMALL_TABLET_SIZE = 30L;
    private static final String PEER_DB = "merge_colocate_peer_db";

    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;
    private static int tableSeq = 0;

    private OlapTable table;
    private ColocateTableIndex.GroupId groupId;
    private ColocateTableIndex.GroupId peerGroupId;
    private PhysicalPartition physicalPartition;
    private MaterializedIndex baseIndex;
    // Tablets tiling the domain: tA in R0, tB and tC in R1 (the mergeable pair), tD in R2.
    private LakeTablet tabletA;
    private LakeTablet tabletB;
    private LakeTablet tabletC;
    private LakeTablet tabletD;
    private long packGroupR0;
    private long packGroupR1;
    private long packGroupR2;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase(PEER_DB);
        starRocksAssert.withDatabase("merge_colocate_test").useDatabase("merge_colocate_test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("merge_colocate_test");

        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public <T> Future<T> submit(Callable<T> task) throws Exception {
                return CompletableFuture.completedFuture(task.call());
            }
        };

        // The reshard daemon ticks ColocateChecker, which would re-stabilize the group underneath
        // the unstable assertions below.
        new MockUp<ColocateChecker>() {
            @Mock
            public void runOneCycle() {
            }
        };
    }

    @BeforeEach
    public void setUp() throws Exception {
        // A fresh table (and colocate group) per test: the cluster shares one ColocateTableIndex.
        String tableName = "t_mc_" + (++tableSeq);
        starRocksAssert.withTable("create table " + tableName + " (k1 int, k2 int)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'mc_grp_" + tableSeq + ":k1');");
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        groupId = GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(table.getId());
        physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        baseIndex = physicalPartition.getLatestBaseIndex();

        // A peer table in ANOTHER database on the same colocate group name: cross-DB peers get
        // distinct GroupIds sharing one grpId, which is the shape the peer-aware guards must handle.
        // Created before the synthetic PACK ids below, because creating a table allocates shards in
        // the group's real PACK shard group.
        starRocksAssert.useDatabase(PEER_DB);
        starRocksAssert.withTable("create table t_mc_peer_" + tableSeq + " (k1 int, k2 int)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1', 'colocate_with' = 'mc_grp_" + tableSeq + ":k1');");
        starRocksAssert.useDatabase(db.getFullName());
        OlapTable peerTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(PEER_DB, "t_mc_peer_" + tableSeq);
        peerGroupId = GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroup(peerTable.getId());

        installThreeColocateRanges();
        installFourTablets();
    }

    @AfterEach
    public void tearDown() {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        for (ColocateTableIndex.GroupId id : Arrays.asList(groupId, peerGroupId)) {
            if (id != null && idx.isGroupUnstable(id)) {
                idx.markGroupStable(id, /* needEditLog */ false);
            }
        }
        if (table != null && table.getState() != OlapTable.OlapTableState.NORMAL) {
            table.setState(OlapTable.OlapTableState.NORMAL);
        }
    }

    /**
     * R0 = (-inf, 200) -> the group's original PACK shard group, R1 = [200, 300), R2 = [300, +inf).
     * R1 and R2 get synthetic PACK ids: this test never talks to a real StarOS shard group, it only
     * asserts which id the merge path selects.
     */
    private void installThreeColocateRanges() {
        ColocateRangeMgr rangeMgr =
                GlobalStateMgr.getCurrentState().getColocateTableIndex().getColocateRangeMgr();
        packGroupR0 = rangeMgr.getColocateRanges(groupId.grpId).get(0).getShardGroupId();
        packGroupR1 = packGroupR0 + 1001L;
        packGroupR2 = packGroupR0 + 1002L;
        rangeMgr.setColocateRanges(groupId.grpId, Arrays.asList(
                new ColocateRange(Range.lt(prefix(200)), packGroupR0),
                new ColocateRange(Range.gelt(prefix(200), prefix(300)), packGroupR1),
                new ColocateRange(Range.ge(prefix(300)), packGroupR2)));
    }

    /**
     * Replaces the freshly created table's single tablet with four small tablets that tile the
     * domain, all fresh enough and small enough to be merge candidates on size alone. tB and tC are
     * the only pair inside one colocate range.
     */
    private void installFourTablets() {
        clearTablets(baseIndex);
        tabletA = addTablet(Range.lt(canonical(200)));                          // R0
        tabletB = addTablet(Range.of(canonical(200), twoCol(250, 0), true, false));   // R1
        tabletC = addTablet(Range.of(twoCol(250, 0), canonical(300), true, false));   // R1
        tabletD = addTablet(Range.ge(canonical(300)));                          // R2
    }

    private static void clearTablets(MaterializedIndex index) {
        for (long tabletId : index.getTabletIdsInOrder()) {
            index.removeTablet(tabletId);
            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId);
        }
    }

    private LakeTablet addTablet(Range<Tuple> range) {
        return addTablet(table, physicalPartition, baseIndex, range);
    }

    /** A merge-candidate tablet: small enough on size, and fresh enough against its partition. */
    private static LakeTablet addTablet(OlapTable owner, PhysicalPartition partition,
            MaterializedIndex index, Range<Tuple> range) {
        LakeTablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId(),
                new TabletRange(range));
        tablet.setDataSize(SMALL_TABLET_SIZE);
        tablet.setDataSizeUpdateTime(partition.getVisibleVersionTime());
        index.addTablet(tablet, new TabletMeta(db.getId(), owner.getId(), partition.getId(),
                index.getId(), TStorageMedium.HDD, true));
        return tablet;
    }

    private static Tuple prefix(int k1) {
        return new Tuple(List.of(Variant.of(IntegerType.INT, String.valueOf(k1))));
    }

    // The (k, NULL) shape expandToFullSortKey produces from a colocate-range bound.
    private static Tuple canonical(int k1) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(k1)),
                Variant.nullVariant(IntegerType.INT)));
    }

    private static Tuple twoCol(int k1, int k2) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(k1)),
                Variant.of(IntegerType.INT, String.valueOf(k2))));
    }

    private MergeTabletJob buildAutoMergeJob() throws Exception {
        MergeTabletClause clause = new MergeTabletClause();
        clause.setTabletReshardTargetSize(TARGET_SIZE);
        return (MergeTabletJob) new MergeTabletJobFactory(db, table, clause).createTabletReshardJob();
    }

    /** The resharding index the job planned for one (partition, index) pair. */
    private static ReshardingMaterializedIndex reshardingIndexOf(MergeTabletJob job, long partitionId,
            long indexId) {
        ReshardingPhysicalPartition reshardingPartition =
                job.getReshardingPhysicalPartitions().get(partitionId);
        Assertions.assertNotNull(reshardingPartition, "the job planned nothing for partition " + partitionId);
        ReshardingMaterializedIndex reshardingIndex =
                reshardingPartition.getReshardingIndexes().get(indexId);
        Assertions.assertNotNull(reshardingIndex, "index " + indexId + " contributed no merge group to "
                + "partition " + partitionId + "; an index whose tablets all classify as uncontained "
                + "produces no entry here");
        return reshardingIndex;
    }

    /** The old-tablet id groups the job will actually merge, in index order. */
    private static List<List<Long>> mergedGroupsOf(MergeTabletJob job, long partitionId, long indexId) {
        List<List<Long>> merged = new ArrayList<>();
        for (ReshardingTablet reshardingTablet : reshardingIndexOf(job, partitionId, indexId)
                .getReshardingTablets()) {
            if (reshardingTablet.getMergingTablet() != null) {
                merged.add(reshardingTablet.getMergingTablet().getOldTabletIds());
            }
        }
        return merged;
    }

    private List<List<Long>> mergedGroupsOf(MergeTabletJob job) {
        return mergedGroupsOf(job, physicalPartition.getId(), baseIndex.getId());
    }

    /**
     * Every adjacent pair is a merge candidate on size, but only tB+tC lie inside one colocate
     * range. Merging tA with tB (or tC with tD) would produce a tablet spanning two ColocateRanges,
     * which de-aligns the group and makes every colocate plan fail closed at
     * {@code RangeColocateScanDispatch.requireAligned}.
     */
    @Test
    public void testAutoMergeGroupsStopAtColocateBoundary() throws Exception {
        Assertions.assertEquals(List.of(List.of(tabletB.getId(), tabletC.getId())),
                mergedGroupsOf(buildAutoMergeJob()),
                "only the pair inside one ColocateRange may merge");
    }

    /**
     * A tablet that already spans a boundary is not a merge candidate at all, and separates the
     * groups on either side of it. The first assertion is the control: it proves the fixture really
     * does produce a group, so the second assertion cannot pass vacuously.
     */
    @Test
    public void testAutoMergeSkipsTabletNotContainedInOneColocateRange() throws Exception {
        Assertions.assertEquals(List.of(List.of(tabletB.getId(), tabletC.getId())),
                mergedGroupsOf(buildAutoMergeJob()));

        // tC now runs from inside R1 past the R1/R2 boundary.
        tabletC.setRange(new TabletRange(Range.of(twoCol(250, 0), twoCol(350, 0), true, false)));

        // EmptyReshardPlanException specifically, not a bare StarRocksException: this is the routine
        // outcome for a range-colocate table (its steady state is one tablet per range, so the
        // size-based signal keeps firing at a permanently empty plan), and TabletReshardJobMgr keys
        // on the type to log it as normal rather than as a failure with a stack trace.
        EmptyReshardPlanException thrown =
                Assertions.assertThrows(EmptyReshardPlanException.class, this::buildAutoMergeJob);
        Assertions.assertTrue(thrown.getMessage().contains("No tablets need to merge"),
                "expected an empty merge plan, got: " + thrown.getMessage());
    }

    /**
     * The colocate boundary flush must not hand back merge budget: an index whose budget allows one
     * tablet to be removed must still emit exactly one group, even when two colocate ranges each
     * hold a mergeable pair. Invoked directly so the parallelism floor is the test's to choose
     * rather than the test cluster's.
     */
    @Test
    public void testAutoMergeRespectsBudgetAcrossColocateRanges() {
        // Two mergeable pairs: tB+tC in R1, and tD plus a new sibling in R2.
        tabletD.setRange(new TabletRange(Range.of(canonical(300), twoCol(350, 0), true, false)));
        addTablet(Range.ge(twoCol(350, 0)));

        List<ColocateRange> colocateRanges = GlobalStateMgr.getCurrentState().getColocateTableIndex()
                .getColocateRanges(groupId.grpId);
        ColocateRangeUtils.Classifier classifier = ColocateRangeUtils.Classifier.of(colocateRanges,
                MetaUtils.getRangeDistributionColumns(table, baseIndex.getMetaId()), 1);
        MergeTabletClause clause = new MergeTabletClause();
        clause.setTabletReshardTargetSize(TARGET_SIZE);
        MergeTabletJobFactory factory = new MergeTabletJobFactory(db, table, clause);

        // 5 tablets, floor 4 -> budget 1: only the first pair may merge.
        List<List<Long>> groups = Deencapsulation.invoke(factory, "createMergeTabletGroups",
                physicalPartition, baseIndex, TARGET_SIZE, 4, classifier);
        Assertions.assertEquals(List.of(List.of(tabletB.getId(), tabletC.getId())), groups,
                "the boundary flush must not replenish the merge budget");

        // Control: with budget to spare both pairs merge, so the assertion above is about the
        // budget and not about the second pair being unmergeable.
        List<List<Long>> unconstrained = Deencapsulation.invoke(factory, "createMergeTabletGroups",
                physicalPartition, baseIndex, TARGET_SIZE, 2, classifier);
        Assertions.assertEquals(2, unconstrained.size());
    }

    /** A range-distribution table with no colocate group keeps merging exactly as before. */
    @Test
    public void testAutoMergeUnchangedForNonColocateRangeTable() throws Exception {
        String tableName = "t_mc_plain_" + (++tableSeq);
        starRocksAssert.withTable("create table " + tableName + " (k1 int, k2 int)\n"
                + "order by(k1, k2)\n"
                + "properties('replication_num' = '1');");
        OlapTable plainTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        PhysicalPartition plainPartition = plainTable.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex plainIndex = plainPartition.getLatestBaseIndex();
        clearTablets(plainIndex);
        // The same four ranges that straddle two colocate boundaries on the colocate table.
        for (Range<Tuple> range : List.of(
                Range.<Tuple>lt(canonical(200)),
                Range.of(canonical(200), twoCol(250, 0), true, false),
                Range.of(twoCol(250, 0), canonical(300), true, false),
                Range.<Tuple>ge(canonical(300)))) {
            addTablet(plainTable, plainPartition, plainIndex, range);
        }

        MergeTabletClause clause = new MergeTabletClause();
        clause.setTabletReshardTargetSize(TARGET_SIZE);
        MergeTabletJobFactory factory = new MergeTabletJobFactory(db, plainTable, clause);
        MergeTabletJob job = (MergeTabletJob) factory.createTabletReshardJob();

        List<Long> orderedTabletIds = plainIndex.getTabletIdsInOrder();
        List<List<Long>> merged = mergedGroupsOf(job, plainPartition.getId(), plainIndex.getId());
        // How far the group extends depends on the cluster's parallelism floor, so assert the
        // property under test instead of an exact grouping: on a table with no colocate group the
        // 200 boundary means nothing, so a group still spans it.
        Assertions.assertEquals(1, merged.size());
        Assertions.assertTrue(merged.get(0).containsAll(
                        List.of(orderedTabletIds.get(0), orderedTabletIds.get(1))),
                "without a colocate group, merging still crosses the 200 boundary: " + merged);
    }

    /**
     * Every other merge case here runs on the base index, whose sort key (k1, k2) has the same arity
     * the colocate ranges are expanded to. A second VISIBLE index with a SHORTER sort key (k1 only)
     * is what separates a per-index resolution from a base-index one: its tablets carry 1-column
     * bounds, and a base-arity expansion produces (200, NULL)-shaped bounds that those bounds sort
     * below, so every rollup tablet would classify as uncontained and its legal pair would vanish.
     */
    @Test
    public void testAutoMergeUsesPerIndexSortKey() throws Exception {
        long rollupMetaId = GlobalStateMgr.getCurrentState().getNextId();
        table.setIndexMeta(rollupMetaId, "r_mc_" + tableSeq,
                List.of(new Column("k1", IntegerType.INT, true, null, "", ""),
                        new Column("v", IntegerType.BIGINT, false, AggregateType.SUM, "0", "")),
                0, 0, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS);
        MaterializedIndex rollupIndex = new MaterializedIndex(rollupMetaId, MaterializedIndex.IndexState.NORMAL);
        physicalPartition.createRollupIndex(rollupIndex);

        // Tiled against the ROLLUP's own 1-column sort key: two tablets inside R1 (the only legal
        // pair), one in R0 and one in R2.
        List<Range<Tuple>> rollupRanges = List.of(
                Range.lt(prefix(200)),
                Range.of(prefix(200), prefix(250), true, false),
                Range.of(prefix(250), prefix(300), true, false),
                Range.ge(prefix(300)));
        List<Long> rollupTabletIds = new ArrayList<>();
        for (Range<Tuple> range : rollupRanges) {
            rollupTabletIds.add(addTablet(table, physicalPartition, rollupIndex, range).getId());
        }

        MergeTabletJob job = buildAutoMergeJob();
        // reshardingIndexOf asserts the entry exists: a base-arity expansion makes every rollup tablet
        // uncontained, the index contributes no merge group, and this map entry disappears.
        List<List<Long>> rollupGroups =
                mergedGroupsOf(job, physicalPartition.getId(), rollupIndex.getId());
        Assertions.assertEquals(List.of(List.of(rollupTabletIds.get(1), rollupTabletIds.get(2))),
                rollupGroups, "only the rollup's own R1 pair may merge");

        // The backstop must classify the rollup against its OWN arity too. With the base arity its
        // 1-column bounds sort below the (200, NULL)-shaped expansion, so every rollup tablet would
        // read as crossing and the group would be marked unstable even though it is perfectly aligned.
        invokeBackstop(job);
        Assertions.assertFalse(groupUnstable(),
                "a correctly aligned shorter-sort-key index must not be marked unstable");
    }

    // ---- explicit ALTER ... MERGE TABLETS groups ----

    private MergeTabletJob buildExplicitMergeJob(Tablet... tablets) throws Exception {
        List<Long> group = new ArrayList<>();
        for (Tablet tablet : tablets) {
            group.add(tablet.getId());
        }
        MergeTabletClause clause = new MergeTabletClause(
                null, new TabletGroupList(List.of(group)), null);
        return (MergeTabletJob) new MergeTabletJobFactory(db, table, clause).createTabletReshardJob();
    }

    @Test
    public void testExplicitMergeGroupInsideOneColocateRangeAccepted() throws Exception {
        Assertions.assertEquals(List.of(List.of(tabletB.getId(), tabletC.getId())),
                mergedGroupsOf(buildExplicitMergeJob(tabletB, tabletC)));
    }

    @Test
    public void testExplicitMergeGroupAcrossColocateBoundaryRejected() {
        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> buildExplicitMergeJob(tabletA, tabletB));
        Assertions.assertTrue(thrown.getMessage().contains("colocate range"),
                "expected a colocate-boundary message, got: " + thrown.getMessage());
    }

    /** A member that already spans a boundary has no single owning range, so the group is refused. */
    @Test
    public void testExplicitMergeGroupWithSpanningTabletRejected() {
        tabletC.setRange(new TabletRange(Range.of(twoCol(250, 0), twoCol(350, 0), true, false)));
        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> buildExplicitMergeJob(tabletB, tabletC));
        Assertions.assertTrue(thrown.getMessage().contains("colocate range"),
                "expected a colocate-boundary message, got: " + thrown.getMessage());
    }

    // ---- unstable-group admission guard ----

    @Test
    public void testMergeRefusedWhileColocateGroupUnstable() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        idx.markGroupUnstable(groupId, /* needEditLog */ false);
        StarRocksException thrown =
                Assertions.assertThrows(StarRocksException.class, this::buildAutoMergeJob);
        Assertions.assertTrue(thrown.getMessage().contains("unstable"),
                "expected an unstable-group message, got: " + thrown.getMessage());

        idx.markGroupStable(groupId, /* needEditLog */ false);
        Assertions.assertNotNull(buildAutoMergeJob(), "a stable group must admit the merge");
    }

    /**
     * Colocate group membership is shared across databases: peer GroupIds differ but share one grpId.
     * With only the PEER marked unstable, an implementation calling isGroupUnstable(myGroupId) instead
     * of isAnyGroupWithSameColocateGroupIdUnstable(grpId) would happily plan a merge against ranges the
     * checker is still aligning.
     */
    @Test
    public void testMergeRefusedWhenOnlyCrossDbPeerUnstable() throws Exception {
        ColocateTableIndex idx = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        Assertions.assertEquals(groupId.grpId, peerGroupId.grpId,
                "the peer table must land in the same colocate group");
        Assertions.assertNotEquals(groupId, peerGroupId, "peers must be distinct GroupIds");

        idx.markGroupUnstable(peerGroupId, /* needEditLog */ false);
        Assertions.assertTrue(idx.isAnyGroupWithSameColocateGroupIdUnstable(groupId.grpId),
                "fixture must actually put a peer of this grpId into the unstable set");
        Assertions.assertFalse(idx.isGroupUnstable(groupId),
                "the table's OWN GroupId must stay stable, or the test proves nothing");

        StarRocksException thrown =
                Assertions.assertThrows(StarRocksException.class, this::buildAutoMergeJob);
        Assertions.assertTrue(thrown.getMessage().contains("unstable"),
                "expected an unstable-group message, got: " + thrown.getMessage());
    }

    // ---- PACK shard group assignment at shard-creation time ----

    /** Captures the per-new-shard group lists {@link MergeTabletJob#createShardsOnStarOS} sends. */
    private Map<Long, List<Long>> captureCreatedShardGroupIds(MergeTabletJob job) {
        Map<Long, List<Long>> captured = new HashMap<>();
        new MockUp<StarOSAgent>() {
            @Mock
            public void createShardsForMerge(Map<Long, List<Long>> newToOldShardIds,
                                             Map<Long, List<Long>> newShardIdToGroupIds,
                                             FilePathInfo pathInfo, FileCacheInfo cacheInfo,
                                             Map<String, String> properties, ComputeResource computeResource) {
                captured.putAll(newShardIdToGroupIds);
            }
        };
        job.createShardsOnStarOS();
        return captured;
    }

    /**
     * Every shard a merge creates -- the merged output AND the untouched tablets it re-mints as
     * identical replacements -- must join its own colocate range's PACK shard group alongside the
     * index SPREAD group. The mergeable pair is in the MIDDLE range, so an implementation that
     * reached for the first or the last colocate range fails here.
     */
    @Test
    public void testCreateShardsOnStarOSAssignsPackShardGroup() throws Exception {
        MergeTabletJob job = buildAutoMergeJob();
        long spreadGroup = newIndexOf(job).getShardGroupId();
        Map<Long, List<Long>> captured = captureCreatedShardGroupIds(job);

        Assertions.assertEquals(3, captured.size(),
                "one shard per resharding tablet: the merged pair plus two identical replacements");
        for (Map.Entry<Long, List<Long>> entry : captured.entrySet()) {
            Tablet newTablet = newIndexOf(job).getTablet(entry.getKey());
            Assertions.assertNotNull(newTablet);
            long expectedPackGroup = expectedPackGroupOf(newTablet);
            Assertions.assertEquals(List.of(spreadGroup, expectedPackGroup), entry.getValue(),
                    "shard " + entry.getKey() + " must join [SPREAD, its own range's PACK]");
        }
        // The three shards must not all land in the same PACK group -- that would pass a
        // constant-PACK implementation.
        Assertions.assertEquals(Set.of(packGroupR0, packGroupR1, packGroupR2),
                captured.values().stream().map(ids -> ids.get(1)).collect(Collectors.toSet()));
    }

    @Test
    public void testCreateShardsOnStarOSNonColocateOnlySpreadGroup() throws Exception {
        // Drop the colocate group membership: the same merge must then create SPREAD-only shards.
        MergeTabletJob job = buildAutoMergeJob();
        long spreadGroup = newIndexOf(job).getShardGroupId();
        GlobalStateMgr.getCurrentState().getColocateTableIndex()
                .removeTable(table.getId(), table, /* isReplay */ false);
        groupId = null;

        for (List<Long> groupIds : captureCreatedShardGroupIds(job).values()) {
            Assertions.assertEquals(List.of(spreadGroup), groupIds);
        }
    }

    private MaterializedIndex newIndexOf(MergeTabletJob job) {
        return reshardingIndexOf(job, physicalPartition.getId(), baseIndex.getId()).getMaterializedIndex();
    }

    private long expectedPackGroupOf(Tablet newTablet) {
        Tuple lower = newTablet.getRange().getRange().isMinimum() ? null
                : newTablet.getRange().getRange().getLowerBound();
        if (lower == null) {
            return packGroupR0;
        }
        int k1 = Integer.parseInt(lower.getValues().get(0).getStringValue());
        return k1 < 200 ? packGroupR0 : (k1 < 300 ? packGroupR1 : packGroupR2);
    }

    /**
     * A registered colocate group whose range record has not been replayed yet reports an EMPTY range
     * list — a topology we cannot see. Merging anyway would create SPREAD-only shards with no boundary
     * checks, and nothing would ever repair them: ColocateChecker only visits UNSTABLE groups, and a
     * merge that never marked one leaves no trace. Refuse instead; merge is an optimization.
     */
    @Test
    public void testMergeRefusedWhileColocateRangesUnavailable() throws Exception {
        // Control: the fixture merges happily while the topology IS available, so the refusal below
        // is attributable to the empty range list and not to the fixture being unmergeable.
        Assertions.assertEquals(List.of(List.of(tabletB.getId(), tabletC.getId())),
                mergedGroupsOf(buildAutoMergeJob()));

        GlobalStateMgr.getCurrentState().getColocateTableIndex().getColocateRangeMgr()
                .setColocateRanges(groupId.grpId, List.of());

        StarRocksException thrown =
                Assertions.assertThrows(StarRocksException.class, this::buildAutoMergeJob);
        Assertions.assertTrue(thrown.getMessage().contains("colocate ranges are not available"),
                "expected a ranges-unavailable refusal, got: " + thrown.getMessage());
    }

    /**
     * The same empty range list can appear AFTER that refusal: shard creation runs in runPendingJob,
     * possibly on a leader promoted in between. Creating SPREAD-only shards there would strand them
     * for the same reason, so shard resolution fails closed instead -- the job is still PENDING, so
     * the throw aborts it cleanly.
     */
    @Test
    public void testCreateShardsFailsClosedWhenColocateRangesVanish() throws Exception {
        MergeTabletJob job = buildAutoMergeJob();
        GlobalStateMgr.getCurrentState().getColocateTableIndex().getColocateRangeMgr()
                .setColocateRanges(groupId.grpId, List.of());

        Assertions.assertThrows(IllegalStateException.class, () -> captureCreatedShardGroupIds(job),
                "an unknowable topology must not silently produce SPREAD-only shards");
    }

    // ---- post-publish backstop ----

    private void invokeBackstop(MergeTabletJob job) {
        Deencapsulation.invoke(job, "applyColocateRangeMergeResult");
    }

    private boolean groupUnstable() {
        return GlobalStateMgr.getCurrentState().getColocateTableIndex().isGroupUnstable(groupId);
    }

    /**
     * A mixed-version or faulty BE can publish a range whose lower tuple is shorter than the colocate
     * prefix; Classifier.indexOf absorbs that into -1. This is the caller that needs it: the backstop
     * runs AFTER updateNextVersions crossed the no-abort boundary, so an escaping exception cannot
     * abort the job -- canAbort() is PENDING-only -- and the scheduler would re-enter runRunningJob
     * every cycle, pinning the table in TABLET_RESHARD forever.
     */
    @Test
    public void testUnclassifiableRangeIsTreatedAsMisalignedNotThrown() throws Exception {
        MergeTabletJob job = buildAutoMergeJob();
        // A one-column lower bound where the colocate prefix needs one value but the sort key has two:
        // extractColocatePrefix asserts values.size() >= colocateColumnCount against the FULL tuple,
        // so an empty tuple is the shape that trips it.
        MaterializedIndex newIndex = newIndexOf(job);
        newIndex.getTablet(mergedNewTabletId(job))
                .setRange(new TabletRange(Range.ge(new Tuple(List.of()))));

        Assertions.assertFalse(groupUnstable());
        // Must not throw: an exception here cannot abort a RUNNING merge job.
        Assertions.assertDoesNotThrow(() -> invokeBackstop(job));
        Assertions.assertTrue(groupUnstable(),
                "an unclassifiable published range must mark the group unstable, not escape");
    }

    /** A published merged range that straddles a boundary must re-arm the colocate checker. */
    @Test
    public void testCrossingMergedTabletMarksGroupUnstable() throws Exception {
        MergeTabletJob job = buildAutoMergeJob();
        MaterializedIndex newIndex = newIndexOf(job);
        Tablet mergedTablet = newIndex.getTablet(mergedNewTabletId(job));
        // Simulate BE publishing a union range that runs from inside R1 past the R1/R2 boundary.
        mergedTablet.setRange(new TabletRange(Range.of(canonical(200), twoCol(350, 0), true, false)));

        Assertions.assertFalse(groupUnstable());
        invokeBackstop(job);
        Assertions.assertTrue(groupUnstable(),
                "a merged tablet spanning two ColocateRanges must mark the group unstable");
    }

    /**
     * The merge itself is legal, but the index carries a pre-existing spanning tablet through as an
     * IdenticalTablet. A backstop that only inspected merged tablets would miss it and leave the
     * group wrongly stable forever.
     */
    @Test
    public void testPreExistingSpanningIdenticalTabletMarksGroupUnstable() throws Exception {
        // tA already spans the R0/R1 boundary before this merge runs.
        tabletA.setRange(new TabletRange(Range.lt(twoCol(250, 0))));

        MergeTabletJob job = buildAutoMergeJob();
        Assertions.assertEquals(List.of(List.of(tabletB.getId(), tabletC.getId())), mergedGroupsOf(job),
                "the spanning tablet must be skipped, and the legal pair must still merge");

        Assertions.assertFalse(groupUnstable());
        invokeBackstop(job);
        Assertions.assertTrue(groupUnstable(),
                "a carried-over spanning IdenticalTablet must mark the group unstable");
    }

    /**
     * The unstable mark must be JOURNALED. An implementation passing needEditLog=false would satisfy
     * every in-memory assertion above and still lose the mark on failover, leaving a misaligned group
     * permanently stable -- the exact state this backstop exists to prevent.
     */
    @Test
    public void testBackstopJournalsTheUnstableMark() throws Exception {
        MergeTabletJob job = buildAutoMergeJob();
        newIndexOf(job).getTablet(mergedNewTabletId(job))
                .setRange(new TabletRange(Range.of(canonical(200), twoCol(350, 0), true, false)));

        List<Boolean> needEditLogArgs = new ArrayList<>();
        new MockUp<ColocateTableIndex>() {
            @Mock
            public void markAllGroupsWithSameColocateGroupIdUnstable(long colocateGroupId, boolean needEditLog) {
                needEditLogArgs.add(needEditLog);
            }
        };

        invokeBackstop(job);

        Assertions.assertEquals(List.of(true), needEditLogArgs,
                "the backstop must journal the unstable mark exactly once");
    }

    @Test
    public void testContainedTabletsKeepGroupStable() throws Exception {
        MergeTabletJob job = buildAutoMergeJob();
        invokeBackstop(job);
        Assertions.assertFalse(groupUnstable(),
                "every new tablet is inside its colocate range, so nothing needs re-aligning");
    }

    /**
     * A peer table's split can splice a new boundary after this merge planned. The merge is then
     * correct against the topology it saw but crossing against the new one, and the backstop is what
     * notices at publish. The new range list is installed directly rather than through
     * {@code applyRangeSplitResult}, because that production path already marks every peer unstable —
     * the test would then pass without the backstop doing anything.
     */
    @Test
    public void testConcurrentPeerSplitBoundaryDetectedAtPublish() throws Exception {
        MergeTabletJob job = buildAutoMergeJob();
        Assertions.assertEquals(List.of(List.of(tabletB.getId(), tabletC.getId())), mergedGroupsOf(job));

        // A peer split cuts R1 at 220, straight through the merged tablet's [200, 250) range.
        ColocateRangeMgr rangeMgr =
                GlobalStateMgr.getCurrentState().getColocateTableIndex().getColocateRangeMgr();
        rangeMgr.setColocateRanges(groupId.grpId, Arrays.asList(
                new ColocateRange(Range.lt(prefix(200)), packGroupR0),
                new ColocateRange(Range.gelt(prefix(200), prefix(220)), packGroupR1),
                new ColocateRange(Range.gelt(prefix(220), prefix(300)), packGroupR1 + 1),
                new ColocateRange(Range.ge(prefix(300)), packGroupR2)));
        MaterializedIndex newIndex = newIndexOf(job);
        newIndex.getTablet(mergedNewTabletId(job))
                .setRange(new TabletRange(Range.of(canonical(200), canonical(300), true, false)));

        Assertions.assertFalse(groupUnstable(), "the concurrent splice alone must not have marked it");
        invokeBackstop(job);
        Assertions.assertTrue(groupUnstable(),
                "the merged tablet now spans the newly spliced boundary and must re-arm the checker");
    }

    private long mergedNewTabletId(MergeTabletJob job) {
        for (ReshardingTablet reshardingTablet : reshardingIndexOf(job, physicalPartition.getId(),
                baseIndex.getId()).getReshardingTablets()) {
            if (reshardingTablet.getMergingTablet() != null) {
                return reshardingTablet.getMergingTablet().getNewTabletId();
            }
        }
        throw new IllegalStateException("fixture produced no merging tablet");
    }
}
