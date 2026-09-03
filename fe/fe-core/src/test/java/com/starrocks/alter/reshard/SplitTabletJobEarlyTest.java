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
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SplitTabletJobEarlyTest {
    // A JMockit @Mock for a static method must itself be static, so it cannot read a local of the
    // enclosing helper; the stubbed count lives here instead.
    private static int stubbedNodeCount;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;
    private long savedTarget;
    private long savedMinSplit;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;
        starRocksAssert.withDatabase("early_split_test").useDatabase("early_split_test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("early_split_test");
        starRocksAssert.withTable("create table t (key1 int, key2 varchar(10)) order by(key1) "
                + "properties('replication_num' = '1');");
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t");
    }

    @BeforeEach
    public void setUp() {
        savedTarget = Config.tablet_reshard_target_size;
        savedMinSplit = Config.tablet_reshard_min_split_size;
        Config.tablet_reshard_target_size = 10L << 30;
        Config.tablet_reshard_min_split_size = 2L << 30;
    }

    @AfterEach
    public void tearDown() {
        // The table is static and shared across tests: drop every non-base index and reset the base
        // index's tablets, so a rollup built by one test cannot leak into the next.
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        for (MaterializedIndex idx : new ArrayList<>(
                partition.getLatestMaterializedIndices(IndexExtState.VISIBLE))) {
            if (idx.getId() != partition.getLatestBaseIndex().getId()) {
                // Remove the tablets first: dropping the index alone leaves inverted-index entries.
                for (Tablet t : new ArrayList<>(idx.getTablets())) {
                    idx.removeTablet(t.getId());
                }
                partition.deleteMaterializedIndexByIndexId(idx.getId());
            }
        }
        for (Tablet existing : new ArrayList<>(partition.getLatestBaseIndex().getTablets())) {
            partition.getLatestBaseIndex().removeTablet(existing.getId());
        }
        Config.tablet_reshard_target_size = savedTarget;
        Config.tablet_reshard_min_split_size = savedMinSplit;
    }

    private MaterializedIndex baseIndex() {
        return table.getAllPhysicalPartitions().iterator().next().getLatestBaseIndex();
    }

    /**
     * Replaces the index's tablets with exactly one LakeTablet per given size, in the order given, and
     * returns their ids positionally. Call this BEFORE capturing any expected ordering.
     */
    private List<Long> setTabletDataSizes(long... sizes) {
        MaterializedIndex index = baseIndex();
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        for (Tablet existing : new ArrayList<>(index.getTablets())) {
            index.removeTablet(existing.getId());
        }
        List<Long> ids = new ArrayList<>();
        for (long size : sizes) {
            long id = GlobalStateMgr.getCurrentState().getNextId();
            LakeTablet tablet = new LakeTablet(id);
            tablet.setDataSize(size);
            TabletMeta meta = new TabletMeta(db.getId(), table.getId(), partition.getId(),
                    index.getId(), TStorageMedium.HDD, true);
            index.addTablet(tablet, meta);
            ids.add(id);
        }
        return ids;
    }

    /**
     * Mirrors what the factory resolves per job: a zero bound is how it says "leave the target alone",
     * which is what an explicit target size, or explicit tablet ids, produce. A warehouse that cannot
     * be resolved does NOT land here -- the factory propagates that instead.
     */
    private Map<Long, Integer> plan(int computeNodeCount, SplitTabletClause clause) {
        boolean explicitTarget = clause.getProperties() != null
                && clause.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE);
        int bound = explicitTarget ? 0 : TabletReshardUtils.adaptiveSplitBound(computeNodeCount);
        return Deencapsulation.invoke(SplitTabletJobFactory.class, "planIndexSplits",
                baseIndex(), clause.getTabletReshardTargetSize(), bound, Config.tablet_reshard_max_split_count);
    }

    /** Base index gets one tablet of earlyOnlySize; a second visible index gets one of normalSize. */
    private void setTwoIndexesWithSizes(long earlyOnlySize, long normalSize) {
        setTabletDataSizes(earlyOnlySize);                       // reshapes the base index
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        long rollupIndexId = GlobalStateMgr.getCurrentState().getNextId();
        MaterializedIndex rollup = new MaterializedIndex(rollupIndexId, rollupIndexId,
                MaterializedIndex.IndexState.NORMAL, partition.getLatestBaseIndex().getShardGroupId());
        long tabletId = GlobalStateMgr.getCurrentState().getNextId();
        LakeTablet tablet = new LakeTablet(tabletId);
        tablet.setDataSize(normalSize);
        rollup.addTablet(tablet, new TabletMeta(db.getId(), table.getId(), partition.getId(),
                rollupIndexId, TStorageMedium.HDD, true));
        partition.createRollupIndex(rollup);
    }

    // Stubs what the factory resolves, expressed as a node count so each test reads in the same terms
    // as the bound it expects. Only the resolution is replaced; the bound is still derived by the real
    // helper, so a change to that derivation reaches these tests.
    private static void stubNodeCount(int nodeCount) {
        stubbedNodeCount = nodeCount;
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int adaptiveSplitBoundForTable(long tableId) {
                return TabletReshardUtils.adaptiveSplitBound(stubbedNodeCount);
            }
        };
    }

    @Test
    public void earlyRuleNeverOverridesANormalSplit() {
        // n=1, cn=8 -> ceiling 8, headroom 7. A 16 GiB tablet clears the 15 GiB normal threshold
        // (kNormal=2) while the early rule alone would ask for kEarly=8 (headroom-capped); the ternary
        // must keep kNormal, not max(kNormal, kEarly).
        List<Long> ids = setTabletDataSizes(16L << 30);
        Map<Long, Integer> plan = plan(8, new SplitTabletClause());
        assertEquals(Map.of(ids.get(0), 2), plan, "kNormal wins outright; the early rule must not enlarge it to 8");
    }

    @Test
    public void earlyRuleFiresOnlyWhereTheNormalRuleDeclines() {
        List<Long> ids = setTabletDataSizes(4L << 30);   // below the 15 GiB normal threshold
        assertEquals(Map.of(ids.get(0), 2), plan(8, new SplitTabletClause()));
        assertTrue(plan(1, new SplitTabletClause()).isEmpty(), "cn=1 -> ceiling 1 -> never applies");
        assertTrue(plan(0, new SplitTabletClause()).isEmpty(), "cn=0 -> unresolved -> never applies");
    }

    @Test
    public void anIndexIsPlannedAgainstOneReadingOfEachTabletSize() {
        // A lake tablet's size is volatile and the stat collector writes it without the table lock, so
        // successive reads inside one plan can disagree. Hand out 14, 14, then 16 GiB: with a single
        // reading the size rule declines 14 GiB (below the 15 GiB threshold) and the adaptive rule
        // takes seven children at a 2 GiB target. Re-reading per pass would let the size rule decline
        // at 14 while the adaptive rule asks for eight at 16 -- more than the size rule would have
        // granted, and a count BE may not be able to honour, which turns the split into a no-op.
        List<Long> ids = setTabletDataSizes(14L << 30);
        long[] readings = {14L << 30, 14L << 30, 16L << 30};
        int[] reads = {0};
        new MockUp<LakeTablet>() {
            @Mock
            public long getDataSize(boolean singleReplica) {
                return readings[Math.min(reads[0]++, readings.length - 1)];
            }
        };
        assertEquals(Map.of(ids.get(0), 7), plan(8, new SplitTabletClause()));
        assertEquals(1, reads[0], "one reading per tablet, for the whole plan");
    }

    @Test
    public void theSizeRuleSpendsHeadroomBeforeTheAdaptiveRuleSeesIt() {
        // A mixed plan: the size rule claims the 15 GiB tablet on its own, and the adaptive rule would
        // claim the 10 GiB one. Charging only the adaptive split against the bound counts the size
        // rule's extra tablet for free, landing on six against a bound of five -- and six is back above
        // the merge floor, so auto-merge takes the index straight back. The size rule keeps its count
        // either way; it is the adaptive rule that must stand down.
        setTabletDataSizes(0L, 0L, 10L << 30, 15L << 30);
        int after = 4 + plan(5, new SplitTabletClause()).values().stream().mapToInt(k -> k - 1).sum();
        assertEquals(5, after, "landed on " + after + ", bound is 5");
    }

    @Test
    public void aSkewedIndexAtTheBoundIsNotWidenedPastIt() {
        // One large tablet beside two small ones, against a bound of four. Flooring bounds the
        // children the adaptive rule asks for -- here three, from 8 GiB over a 2.5 GiB target -- but
        // the small tablets it declines still occupy slots and are absent from that sum. Left
        // uncapped the index would land on five against a bound of four, back inside auto-merge's
        // range, which is the tug of war the bound exists to prevent.
        setTabletDataSizes(8L << 30, 1L << 30, 1L << 30);
        int after = 3 + plan(4, new SplitTabletClause()).values().stream().mapToInt(k -> k - 1).sum();
        assertEquals(4, after, "landed on " + after + ", bound is 4");
    }

    @Test
    public void theIndexNeverPassesTheBoundHoweverItsDataIsSpread() {
        // Uneven sizes are the shape that breaks a per-tablet rule that rounds to nearest: each tablet
        // rounds up independently and the total overshoots. Flooring bounds each tablet's own count;
        // the shared headroom budget is what bounds the index's width, since declined tablets and the
        // size rule's own splits occupy slots too.
        setTabletDataSizes(4L << 30, 12L << 30, 1L << 30);
        int after = 3 + plan(5, new SplitTabletClause()).values().stream().mapToInt(k -> k - 1).sum();
        assertTrue(after <= 5, "landed on " + after + ", bound is 5");

        // Tablets just over one and a half targets are the other shape: splitting them in two would be
        // more tablets than their share and each child still under target, so they must not split.
        setTabletDataSizes(5L << 30, 5L << 30, 5L << 30, 1L << 30, 1L << 30);
        int after2 = 5 + plan(5, new SplitTabletClause()).values().stream().mapToInt(k -> k - 1).sum();
        assertTrue(after2 <= 5, "landed on " + after2 + ", bound is 5");
    }


    @Test
    public void planningNeverReordersTheCatalogTabletList() {
        // Ascending sizes are the worst case for an in-place sort: unlike the all-equal fixture above,
        // reordering here is observable regardless of sort stability.
        List<Long> ids = setTabletDataSizes(1L << 30, 4L << 30, 12L << 30);
        plan(5, new SplitTabletClause());   // ceiling 5, headroom 2
        assertEquals(ids, baseIndex().getTablets().stream().map(Tablet::getId).collect(Collectors.toList()),
                "planning must never sort index.getTablets() in place");
    }

    @Test
    public void indexAtOrAboveTheCeilingIsUntouched() {
        setTabletDataSizes(3L << 30, 3L << 30);
        assertTrue(plan(2, new SplitTabletClause()).isEmpty(), "n == ceiling");
        assertTrue(plan(1, new SplitTabletClause()).isEmpty(), "n > ceiling");
    }


    @Test
    public void explicitTargetSizePropertySuppressesTheEarlyRule() {
        setTabletDataSizes(4L << 30);
        Map<String, String> props =
                Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, String.valueOf(10L << 30));
        SplitTabletClause clause = new SplitTabletClause(null, null, props);
        clause.setTabletReshardTargetSize(10L << 30);
        assertTrue(plan(8, clause).isEmpty(), "an explicitly requested target size gets exactly that policy");
    }

    @Test
    public void manualSplitWithoutPropertiesKeepsTheEarlyRule() {
        // AstBuilder#visitSplitTabletClause always hands over a non-null map, empty when the statement
        // carried no PROPERTIES clause at all; the factory must not read "non-null" as "explicit".
        List<Long> ids = setTabletDataSizes(4L << 30);
        SplitTabletClause clause = new SplitTabletClause(null, null, new HashMap<>());
        clause.setTabletReshardTargetSize(Config.tablet_reshard_target_size);
        assertEquals(Map.of(ids.get(0), 2), plan(8, clause));
    }


    @Test
    public void anEligibleEarlyPlanIsMaterializedIntoAJob() throws Exception {
        // The one POSITIVE end-to-end test: every other test above suppresses, refuses or bypasses the
        // early path, so only this one drives the real factory from clause to materialized job.
        setTabletDataSizes(4L << 30);
        // The factory resolves the node count itself, so stub the resolver.
        stubNodeCount(8);

        TabletReshardJob job =
                new SplitTabletJobFactory(db, table, new SplitTabletClause()).createTabletReshardJob();
        assertEquals(2L, job.getParallelTablets(), "the early plan, not the empty baseline, was materialized");
    }

    @Test
    public void aManualNoPropertiesClauseAlsoGetsTheEarlyPlan() throws Exception {
        // Companion to anEligibleEarlyPlanIsMaterializedIntoAJob, for the clause shape
        // AstBuilder#visitSplitTabletClause produces for `ALTER TABLE t SPLIT TABLET` with no
        // PROPERTIES: getProperties() returns a new empty HashMap, never null.
        setTabletDataSizes(4L << 30);
        stubNodeCount(8);

        SplitTabletClause clause = new SplitTabletClause(null, null, new HashMap<>());
        clause.setTabletReshardTargetSize(Config.tablet_reshard_target_size);
        TabletReshardJob job = new SplitTabletJobFactory(db, table, clause).createTabletReshardJob();
        assertEquals(2L, job.getParallelTablets(), "an empty property map is NOT an explicit target");
    }

    @Test
    public void nonPositiveMinimumSplitSizeIsRefusedByTheFactoryOnBothClauseShapes() throws Exception {
        // The planner-level test (nonPositiveMinimumSplitSizeDisablesTheEarlyRule) covers only
        // `new SplitTabletClause()`. The manual grammar path hands over a non-null empty properties
        // map instead; both shapes must reach the factory and both must refuse the early contribution
        // when the effective early target is non-positive.
        // The two sizes must live in separate indexes: in one index the 100 GiB tablet's normal split
        // exhausts headroom before the 3 GiB tablet is reached, masking the guard regardless of its
        // presence (the same headroom-masking the planner-level test avoids with a single tablet).
        setTwoIndexesWithSizes(/*adaptiveOnly=*/ 4L << 30, /*normal=*/ 100L << 30);
        SplitTabletClause manualClause = new SplitTabletClause(null, null, new HashMap<>());
        manualClause.setTabletReshardTargetSize(Config.tablet_reshard_target_size);
        List<SplitTabletClause> clauses = List.of(new SplitTabletClause(), manualClause);

        for (SplitTabletClause clause : clauses) {
            for (long badMin : new long[] {0L, -8L}) {
                Config.tablet_reshard_min_split_size = badMin;
                TabletReshardJob job =
                        new SplitTabletJobFactory(db, table, clause).createTabletReshardJob();
                assertEquals(10L, job.getParallelTablets(),
                        "only the 100 GiB tablet's normal split, no forced-count contribution "
                                + "from calcSplitCount's negative-target mode");
            }
        }
    }

    @Test
    public void anExplicitTabletListIsPlannedByTheUnchangedBranch() throws Exception {
        List<Long> ids = setTabletDataSizes(4L << 30);
        SplitTabletClause clause = new SplitTabletClause(null, new TabletList(List.of(ids.get(0))), null);
        clause.setTabletReshardTargetSize(Config.tablet_reshard_target_size);
        assertThrows(StarRocksException.class,
                () -> new SplitTabletJobFactory(db, table, clause).createTabletReshardJob(),
                "an explicitly listed under-threshold tablet is not split, cn notwithstanding");
    }

    @Test
    public void everyPhysicalPartitionKeepsItsOwnSplitPlan() throws Exception {
        // Every physical partition of a table initializes its indexes with the SAME index-meta id
        // (LocalMetastore: "initially, index id and index meta id are the same"). A plan map keyed by
        // index id alone therefore lets the second partition overwrite the first, and materialization
        // then looks up the surviving plan with the other partition's tablet ids, matches nothing, and
        // silently emits identical tablets instead of the requested split.
        setTabletDataSizes(20L << 30);
        Partition logical = table.getPartitions().iterator().next();
        PhysicalPartition first = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex firstIndex = first.getLatestBaseIndex();

        // Same index id as the first partition's base index — that collision is the point.
        MaterializedIndex secondIndex = new MaterializedIndex(firstIndex.getId(),
                MaterializedIndex.IndexState.NORMAL, firstIndex.getShardGroupId());
        PhysicalPartition second = new PhysicalPartition(
                GlobalStateMgr.getCurrentState().getNextId(), logical.getId(), secondIndex);
        long secondTabletId = GlobalStateMgr.getCurrentState().getNextId();
        LakeTablet secondTablet = new LakeTablet(secondTabletId);
        secondTablet.setDataSize(20L << 30);
        secondIndex.addTablet(secondTablet, new TabletMeta(db.getId(), table.getId(), second.getId(),
                firstIndex.getId(), TStorageMedium.HDD, true), false);
        logical.addSubPartition(second);
        try {
            TabletReshardJob job =
                    new SplitTabletJobFactory(db, table, new SplitTabletClause()).createTabletReshardJob();
            // Both partitions hold one 20 GiB tablet, so each must split into two by the size rule
            // alone. Keyed by index id only, one partition's plan is lost and the total is 2.
            assertEquals(4L, job.getParallelTablets(),
                    "each physical partition must keep its own split plan");
        } finally {
            for (Tablet t : new ArrayList<>(secondIndex.getTablets())) {
                secondIndex.removeTablet(t.getId());
            }
            logical.removeSubPartition(second.getId());
        }
    }

    @Test
    public void aResolvedZeroBoundLeavesOnlyTheSizeRule() throws Exception {
        // Separate indexes for the same reason the non-positive-minimum test needs them: in one index
        // the 100 GiB tablet's normal split zeroes headroom before the 3 GiB tablet is reached, so the
        // assertion would read 10 for any node count and could not tell a resolved 0 from a count the
        // constructor never resolved at all.
        setTwoIndexesWithSizes(/*adaptiveOnly=*/ 4L << 30, /*normal=*/ 100L << 30);
        stubNodeCount(0);
        TabletReshardJob job =
                new SplitTabletJobFactory(db, table, new SplitTabletClause()).createTabletReshardJob();
        assertEquals(10L, job.getParallelTablets(), "only the 100 GiB tablet splits");
    }

    @Test
    public void anUnresolvableWarehouseFailsTheJobRatherThanPlanningWithoutIt() {
        // A resolution failure must not read as "this index needs nothing". An empty plan is something
        // the caller is entitled to latch as deterministic, and with the layout, the configuration and
        // the signal all unchanged that fingerprint would never move again -- the table would stop
        // splitting for good over one unavailable warehouse.
        setTabletDataSizes(4L << 30);
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int adaptiveSplitBoundForTable(long tableId) {
                throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_UNAVAILABLE, "wh");
            }
        };
        SplitTabletJobFactory factory = new SplitTabletJobFactory(db, table, new SplitTabletClause());
        assertThrows(ErrorReportException.class, factory::createTabletReshardJob,
                "an unresolvable warehouse must propagate, not become an empty plan");
    }
}
