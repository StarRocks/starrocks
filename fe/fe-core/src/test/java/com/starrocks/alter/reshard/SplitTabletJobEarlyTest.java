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
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
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
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;
    private long savedTarget;
    private long savedMinSplit;
    private long savedMaxParallel;
    private boolean savedEnableEarly;

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
        savedMaxParallel = Config.tablet_reshard_max_parallel_tablets;
        savedEnableEarly = Config.tablet_reshard_enable_early_split;
        Config.tablet_reshard_target_size = 10L << 30;
        Config.tablet_reshard_min_split_size = 2L << 30;
        Config.tablet_reshard_enable_early_split = true;
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
        Config.tablet_reshard_max_parallel_tablets = savedMaxParallel;
        Config.tablet_reshard_enable_early_split = savedEnableEarly;
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

    private Map<Long, Integer> plan(int computeNodeCount, SplitTabletClause clause) {
        Object policy = Deencapsulation.invoke(SplitTabletJobFactory.class, "capturePolicy",
                clause, computeNodeCount);
        return Deencapsulation.invoke(SplitTabletJobFactory.class, "planIndexSplits",
                baseIndex(), clause.getTabletReshardTargetSize(), policy);
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

    /** Captures every INFO event logged by SplitTabletJobFactory while attached. */
    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> messages = new ArrayList<>();

        CapturingAppender() {
            super("early-split-deferral-capture", null, null, false, null);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
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
        List<Long> ids = setTabletDataSizes(3L << 30);   // below the 15 GiB normal threshold
        assertEquals(Map.of(ids.get(0), 2), plan(8, new SplitTabletClause()));
        assertTrue(plan(1, new SplitTabletClause()).isEmpty(), "cn=1 -> ceiling 1 -> never applies");
        assertTrue(plan(0, new SplitTabletClause()).isEmpty(), "cn=0 -> unresolved -> never applies");
    }

    @Test
    public void headroomIsSpentLargestFirstAndNeverExceedsTheCeiling() {
        // n=3, cn=5 -> ceiling 5, headroom 2. All three are below the normal threshold.
        List<Long> ids = setTabletDataSizes(4L << 30, 12L << 30, 1L << 30);
        Map<Long, Integer> plan = plan(5, new SplitTabletClause());
        assertEquals(Map.of(ids.get(1), 3), plan, "the 12 GiB tablet takes the whole headroom");
        int added = plan.values().stream().mapToInt(k -> k - 1).sum();
        assertEquals(2, added);
    }

    @Test
    public void equalSizesKeepRangeOrderAndTheCatalogListIsNotReordered() {
        List<Long> ids = setTabletDataSizes(4L << 30, 4L << 30, 4L << 30);
        List<Long> before = baseIndex().getTablets().stream().map(Tablet::getId).collect(Collectors.toList());
        Map<Long, Integer> plan = plan(4, new SplitTabletClause());   // ceiling 4, headroom 1
        assertEquals(Map.of(ids.get(0), 2), plan, "a stable sort gives the tie to the first in range order");
        assertEquals(before, baseIndex().getTablets().stream().map(Tablet::getId).collect(Collectors.toList()),
                "getTablets() returns the live range-ordered list; never sort it in place");
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
    public void nonPositiveMinimumSplitSizeDisablesTheEarlyRule() {
        // A single tablet below the normal threshold: a two-tablet fixture would let a normal split
        // elsewhere in the index exhaust headroom before this tablet is reached, masking the guard.
        setTabletDataSizes(3L << 30);
        for (long badMin : new long[] {0L, -8L}) {
            Config.tablet_reshard_min_split_size = badMin;
            assertTrue(plan(8, new SplitTabletClause()).isEmpty(), "no early split at min_split_size " + badMin);
        }
    }

    @Test
    public void explicitTargetSizePropertySuppressesTheEarlyRule() {
        setTabletDataSizes(3L << 30);
        Map<String, String> props =
                Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, String.valueOf(10L << 30));
        SplitTabletClause clause = new SplitTabletClause(null, null, props);
        clause.setTabletReshardTargetSize(10L << 30);
        assertTrue(plan(8, clause).isEmpty(), "an explicitly requested target size gets exactly that policy");
    }

    @Test
    public void manualSplitWithoutPropertiesKeepsTheEarlyRule() {
        // AstBuilder#visitSplitTabletClause always hands over a non-null map, empty when the statement
        // carried no PROPERTIES clause at all; capturePolicy must not read "non-null" as "explicit".
        List<Long> ids = setTabletDataSizes(3L << 30);
        SplitTabletClause clause = new SplitTabletClause(null, null, new HashMap<>());
        clause.setTabletReshardTargetSize(Config.tablet_reshard_target_size);
        assertEquals(Map.of(ids.get(0), 2), plan(8, clause));
    }

    @Test
    public void disabledFlagSuppressesTheEarlyRule() {
        setTabletDataSizes(3L << 30);
        Config.tablet_reshard_enable_early_split = false;
        assertTrue(plan(8, new SplitTabletClause()).isEmpty());
    }

    @Test
    public void anotherRunningJobSuppressesTheEarlyContribution() {
        setTabletDataSizes(3L << 30);
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public long getTotalParallelTablets() {
                return 4L;
            }
        };
        assertThrows(StarRocksException.class,
                () -> new SplitTabletJobFactory(db, table, new SplitTabletClause(), 8).createTabletReshardJob(),
                "with another job running the plan is the all-normal one, which is empty here");
    }

    @Test
    public void aLaterNormalBaselineIsReservedBeforeAnyEarlyDelta() throws Exception {
        // Two indexes: an early-only one whose early plan has replacement topology 2 against a
        // baseline of 0, i.e. delta 2, and a normal one whose baseline topology is 10. cap 10 is fully
        // consumed by the baseline, so no early budget remains and the job is exactly the normal plan.
        Config.tablet_reshard_max_parallel_tablets = 10L;
        setTwoIndexesWithSizes(/*earlyOnly=*/ 3L << 30, /*normal=*/ 100L << 30);
        long earlyOnlyIndexId = baseIndex().getId();

        // The deferral must be observable: it is the only operational signal that a job planned less
        // than the table was eligible for. fe-core's test config sets <Root level="WARN">, so INFO
        // never reaches an appender without also raising the logger's level for this window.
        Logger coreLogger = (Logger) LogManager.getLogger(SplitTabletJobFactory.class);
        Level savedLevel = coreLogger.getLevel();
        Configurator.setLevel(SplitTabletJobFactory.class.getName(), Level.INFO);
        CapturingAppender appender = new CapturingAppender();
        appender.start();
        coreLogger.addAppender(appender);
        TabletReshardJob job;
        try {
            job = new SplitTabletJobFactory(db, table, new SplitTabletClause(), 8).createTabletReshardJob();
        } finally {
            coreLogger.removeAppender(appender);
            Configurator.setLevel(SplitTabletJobFactory.class.getName(), savedLevel);
        }

        assertEquals(10L, job.getParallelTablets(),
                "only the normal index splits; the early delta must not have been admitted first");
        assertEquals(1, appender.messages.size(), "exactly one index's early contribution is deferred");
        String message = appender.messages.get(0);
        assertTrue(message.contains("index " + earlyOnlyIndexId + " of table "
                        + db.getFullName() + "." + table.getName()),
                "names the deferred index and the table");
        assertTrue(message.contains("needs 2 more replacement tablets"), "names the required topology");
        assertTrue(message.contains("tablet_reshard_max_parallel_tablets 10"), "names the cap");
    }

    @Test
    public void aRefusedEarlyPlanMintsNoIds() {
        Config.tablet_reshard_max_parallel_tablets = 1L;   // topology for one 2-way split is 2
        setTabletDataSizes(3L << 30);
        long before = GlobalStateMgr.getCurrentState().getNextId();
        assertThrows(StarRocksException.class,
                () -> new SplitTabletJobFactory(db, table, new SplitTabletClause(), 8).createTabletReshardJob());
        long after = GlobalStateMgr.getCurrentState().getNextId();
        // Each getNextId() observation itself consumes one id, so two observations with no allocation
        // in between differ by exactly 1.
        assertEquals(1L, after - before, "a refused early plan must not mint replacement tablet ids");
    }

    @Test
    public void aSizeDrivenPlanIsNeverRejectedByTheEarlyBudget() throws Exception {
        Config.tablet_reshard_max_parallel_tablets = 1L;
        setTabletDataSizes(100L << 30);
        TabletReshardJob job =
                new SplitTabletJobFactory(db, table, new SplitTabletClause(), 8).createTabletReshardJob();
        assertEquals(10L, job.getParallelTablets(), "the baseline is never trimmed");
    }

    @Test
    public void anEligibleEarlyPlanIsMaterializedIntoAJob() throws Exception {
        // The one POSITIVE test for this task: every other test above suppresses, refuses or bypasses
        // the early path, so only this one exercises the baseline/early two-pass wiring end to end.
        setTabletDataSizes(3L << 30);
        TabletReshardJob job =
                new SplitTabletJobFactory(db, table, new SplitTabletClause(), 8).createTabletReshardJob();
        assertEquals(2L, job.getParallelTablets(), "the early plan, not the empty baseline, was materialized");
    }

    @Test
    public void aManualNoPropertiesClauseAlsoGetsTheEarlyPlan() throws Exception {
        // Companion to anEligibleEarlyPlanIsMaterializedIntoAJob, for the clause shape
        // AstBuilder#visitSplitTabletClause produces for `ALTER TABLE t SPLIT TABLET` with no
        // PROPERTIES: getProperties() returns a new empty HashMap, never null.
        setTabletDataSizes(3L << 30);
        SplitTabletClause clause = new SplitTabletClause(null, null, new HashMap<>());
        clause.setTabletReshardTargetSize(Config.tablet_reshard_target_size);
        TabletReshardJob job = new SplitTabletJobFactory(db, table, clause, 8).createTabletReshardJob();
        assertEquals(2L, job.getParallelTablets(), "an empty property map is NOT an explicit target");
    }

    @Test
    public void nonPositiveMinimumSplitSizeIsRefusedByTheFactoryOnBothClauseShapes() throws Exception {
        // The planner-level test (nonPositiveMinimumSplitSizeDisablesTheEarlyRule) covers only
        // `new SplitTabletClause()`. The manual grammar path hands over a non-null empty properties
        // map instead; both shapes must reach the factory and both must refuse the early contribution
        // when the effective early target is non-positive.
        setTabletDataSizes(3L << 30, 100L << 30);   // the 100 GiB tablet keeps the factory entered
        SplitTabletClause manualClause = new SplitTabletClause(null, null, new HashMap<>());
        manualClause.setTabletReshardTargetSize(Config.tablet_reshard_target_size);
        List<SplitTabletClause> clauses = List.of(new SplitTabletClause(), manualClause);

        for (SplitTabletClause clause : clauses) {
            for (long badMin : new long[] {0L, -8L}) {
                Config.tablet_reshard_min_split_size = badMin;
                TabletReshardJob job =
                        new SplitTabletJobFactory(db, table, clause, 8).createTabletReshardJob();
                assertEquals(10L, job.getParallelTablets(),
                        "only the 100 GiB tablet's normal split, no forced-count contribution "
                                + "from calcSplitCount's negative-target mode");
            }
        }
    }

    @Test
    public void anExplicitTabletListIsPlannedByTheUnchangedBranch() throws Exception {
        List<Long> ids = setTabletDataSizes(3L << 30);
        SplitTabletClause clause = new SplitTabletClause(null, new TabletList(List.of(ids.get(0))), null);
        clause.setTabletReshardTargetSize(Config.tablet_reshard_target_size);
        assertThrows(StarRocksException.class,
                () -> new SplitTabletJobFactory(db, table, clause, 8).createTabletReshardJob(),
                "an explicitly listed under-threshold tablet is not split, cn notwithstanding");
    }

    @Test
    public void anUnavailableWarehouseFallsBackToTheNormalRule() throws Exception {
        setTabletDataSizes(3L << 30, 100L << 30);
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int safeComputeNodeCountForTable(long tableId) {
                return 0;
            }
        };
        // 3-arg constructor: resolves its own count, gets 0, disables the early rule.
        TabletReshardJob job =
                new SplitTabletJobFactory(db, table, new SplitTabletClause()).createTabletReshardJob();
        assertEquals(10L, job.getParallelTablets(), "only the 100 GiB tablet splits");
    }
}
