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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Test
    public void earlyRuleNeverOverridesANormalSplit() {
        // n=1, ceiling=2, one 100 GiB tablet. Today's rule asks for 10 children and succeeds; the early
        // rule must neither shrink that to 2 nor enlarge it.
        List<Long> ids = setTabletDataSizes(100L << 30);
        Map<Long, Integer> plan = plan(2, new SplitTabletClause());
        assertEquals(Map.of(ids.get(0), 10), plan);
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
    public void indexAtOrAboveTheCeilingIsUntouched() {
        List<Long> ids = setTabletDataSizes(3L << 30, 3L << 30);
        assertTrue(plan(2, new SplitTabletClause()).isEmpty(), "n == ceiling");
        assertTrue(plan(1, new SplitTabletClause()).isEmpty(), "n > ceiling");
    }

    @Test
    public void nonPositiveMinimumSplitSizeDisablesTheEarlyRule() {
        List<Long> ids = setTabletDataSizes(3L << 30, 100L << 30);
        for (long badMin : new long[] {0L, -8L}) {
            Config.tablet_reshard_min_split_size = badMin;
            Map<Long, Integer> plan = plan(8, new SplitTabletClause());
            assertEquals(Map.of(ids.get(1), 10), plan,
                    "normal split unaffected, no early contribution at min_split_size " + badMin);
        }
    }

    @Test
    public void explicitTargetSizePropertySuppressesTheEarlyRule() {
        List<Long> ids = setTabletDataSizes(3L << 30);
        Map<String, String> props =
                Map.of(PropertyAnalyzer.PROPERTIES_TABLET_RESHARD_TARGET_SIZE, String.valueOf(10L << 30));
        SplitTabletClause clause = new SplitTabletClause(null, null, props);
        clause.setTabletReshardTargetSize(10L << 30);
        assertTrue(plan(8, clause).isEmpty(), "an explicitly requested target size gets exactly that policy");
    }

    @Test
    public void disabledFlagSuppressesTheEarlyRule() {
        List<Long> ids = setTabletDataSizes(3L << 30);
        Config.tablet_reshard_enable_early_split = false;
        assertTrue(plan(8, new SplitTabletClause()).isEmpty());
    }
}
