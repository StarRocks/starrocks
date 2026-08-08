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

package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LakeTableTxnLogApplierTest extends LakeTableTestHelper {
    private static final long GIB = 1024L * 1024L * 1024L;
    // Pinned rather than inherited: every early-split assertion below is sensitive to the gap between
    // the two split thresholds these produce.
    private static final long TARGET_SIZE = 10 * GIB;
    private static final long MIN_SPLIT_SIZE = 2 * GIB;
    // A split fires at ceil(1.5 x its target size); both pinned sizes are even, so 3/2 is exact.
    private static final long EARLY_SPLIT_THRESHOLD = MIN_SPLIT_SIZE * 3 / 2;

    private final List<ComputeNode> registeredNodes = new ArrayList<>();
    private long savedTargetSize;
    private long savedMinSplitSize;
    private int savedMaxSplitCount;
    private boolean savedEarlySplitEnabled;

    @BeforeEach
    public void pinReshardConfig() {
        savedTargetSize = Config.tablet_reshard_target_size;
        savedMinSplitSize = Config.tablet_reshard_min_split_size;
        savedMaxSplitCount = Config.tablet_reshard_max_split_count;
        savedEarlySplitEnabled = Config.tablet_reshard_enable_early_split;
        Config.tablet_reshard_target_size = TARGET_SIZE;
        Config.tablet_reshard_min_split_size = MIN_SPLIT_SIZE;
        Config.tablet_reshard_max_split_count = 1024;
        Config.tablet_reshard_enable_early_split = true;
    }

    @AfterEach
    public void restoreGlobalState() {
        Config.tablet_reshard_target_size = savedTargetSize;
        Config.tablet_reshard_min_split_size = savedMinSplitSize;
        Config.tablet_reshard_max_split_count = savedMaxSplitCount;
        Config.tablet_reshard_enable_early_split = savedEarlySplitEnabled;
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        registeredNodes.forEach(clusterInfo::dropComputeNode);
        registeredNodes.clear();
    }

    /**
     * Registers compute nodes so the publish path's O(1) fast-path bound (total backends + total
     * compute nodes) rises above the fixture index's two tablets, and the producer therefore has to
     * resolve that index's real ceiling. Only the nodes created here are removed afterwards, so any
     * pre-existing cluster membership is left untouched.
     */
    private void registerComputeNodes(int count) {
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        for (int i = 0; i < count; i++) {
            ComputeNode node = new ComputeNode(10_000L + i, "fastpath-h" + i, 9050);
            node.setWorkerGroupId(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            clusterInfo.addComputeNode(node);
            registeredNodes.add(node);
        }
    }

    private record PublishFixture(LakeTableTxnLogApplier applier, TransactionState state,
                                  TableCommitInfo tableCommitInfo, Database db) {
    }

    /**
     * A leader-side publish of one range-distributed lake table whose single index holds two tablets,
     * exactly one of which reports a stat of {@code reportedDataSize}.
     */
    private PublishFixture newPublishFixture(long reportedDataSize) {
        MaterializedIndex index = new MaterializedIndex(indexId);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId[0], tabletMeta);
        index.addTablet(new LakeTablet(tabletId[0]), tabletMeta);
        TabletMeta noStatMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId[1], noStatMeta);
        index.addTablet(new LakeTablet(tabletId[1]), noStatMeta);

        LakeTable table = buildLakeTableWithIndex(index);
        // Range distribution is required for the publish-driven reshard path to evaluate the table.
        table.setDefaultDistributionInfo(new RangeDistributionInfo());
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);

        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
        applier.applyCommitLog(state, tableCommitInfo);

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 5L;
        stat.dataSize = reportedDataSize;
        partitionCommitInfo.getTabletStats().put(tabletId[0], stat);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public static boolean isCheckpointThread() {
                return false;
            }
        };
        return new PublishFixture(applier, state, tableCommitInfo, new Database(dbId, "test_db"));
    }

    @Test
    public void skipsNodeCountResolutionBelowTheEarlyThreshold() {
        boolean[] resolved = {false};
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getBackgroundComputeResourceWithoutProbe(long tableId) {
                resolved[0] = true;
                return WarehouseComputeResource.DEFAULT;
            }
        };
        registerComputeNodes(16);
        PublishFixture f = newPublishFixture(EARLY_SPLIT_THRESHOLD - 1);
        f.applier().applyVisibleLog(f.state(), f.tableCommitInfo(), f.db());
        Assertions.assertFalse(resolved[0], "a publish with no early-split-sized tablet must pay nothing");
    }

    @Test
    public void fastPathSkipsResolutionWhenNoIndexCanBeUnderProvisioned() {
        boolean[] resolved = {false};
        long[] captured = {-1L};
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getBackgroundComputeResourceWithoutProbe(long tableId) {
                resolved[0] = true;
                return WarehouseComputeResource.DEFAULT;
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addReshardCandidate(long dbId, long tableId, long maxTabletSize,
                    long minAdjacentTabletPairSize, long maxUnderProvisionedTabletSize) {
                captured[0] = maxUnderProvisionedTabletSize;
            }
        };
        // An isolated two-node view, so the assertion does not depend on ambient cluster membership:
        // the cluster-wide upper bound is exactly the fixture index's tablet count, hence no index can
        // be under-provisioned and the fast path must skip the resolution outright.
        SystemInfoService isolatedClusterInfo = new SystemInfoService();
        isolatedClusterInfo.addComputeNode(new ComputeNode(20_000L, "isolated-h0", 9050));
        isolatedClusterInfo.addComputeNode(new ComputeNode(20_001L, "isolated-h1", 9050));
        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return isolatedClusterInfo;
            }
        };
        PublishFixture f = newPublishFixture(TARGET_SIZE * 2);
        f.applier().applyVisibleLog(f.state(), f.tableCommitInfo(), f.db());
        Assertions.assertFalse(resolved[0],
                "no index can be under-provisioned, so the fast path must skip the resolution");
        Assertions.assertEquals(0L, captured[0], "the skipped fast path must emit no early signal");
    }

    @Test
    public void anIndexBelowTheFastPathBoundButAtItsRealCeilingEmitsNothing() {
        AtomicInteger resolutions = new AtomicInteger(0);
        long[] captured = {-1L};
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getBackgroundComputeResourceWithoutProbe(long tableId) {
                resolutions.incrementAndGet();
                return WarehouseComputeResource.DEFAULT;
            }
        };
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int computeNodeCount(ComputeResource resource) {
                return 2;      // the worker group's real ceiling is 2; the fixture's index has 2 tablets
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addReshardCandidate(long dbId, long tableId, long maxTabletSize,
                    long minAdjacentTabletPairSize, long maxUnderProvisionedTabletSize) {
                captured[0] = maxUnderProvisionedTabletSize;
            }
        };
        registerComputeNodes(16);
        PublishFixture f = newPublishFixture(TARGET_SIZE * 2);
        f.applier().applyVisibleLog(f.state(), f.tableCommitInfo(), f.db());
        // The cluster-wide bound is only an upper bound on the worker group's ceiling: an index below
        // the bound still has to be measured against the ceiling before it counts as under-provisioned.
        Assertions.assertEquals(1, resolutions.get(),
                "an index below the cluster-wide bound must have its real ceiling resolved");
        Assertions.assertEquals(0L, captured[0],
                "an index already at its worker group's ceiling is not under-provisioned");
    }

    @Test
    public void aResolutionFailureSuppressesOnlyTheEarlySignal() {
        long[] captured = {-1L};
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getBackgroundComputeResourceWithoutProbe(long tableId) {
                throw new RuntimeException("boom");
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addReshardCandidate(long dbId, long tableId, long maxTabletSize,
                    long minAdjacentTabletPairSize, long maxUnderProvisionedTabletSize) {
                captured[0] = maxUnderProvisionedTabletSize;
            }
        };
        registerComputeNodes(16);
        PublishFixture f = newPublishFixture(TARGET_SIZE * 2);
        f.applier().applyVisibleLog(f.state(), f.tableCommitInfo(), f.db());   // must NOT throw
        Assertions.assertEquals(0L, captured[0], "a failed resolution must drop the hint, not the publish");
    }

    @Test
    public void emitsTheEarlySignalForAnUnderProvisionedIndex() {
        long[] captured = {-1L};
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getBackgroundComputeResourceWithoutProbe(long tableId) {
                return WarehouseComputeResource.DEFAULT;
            }
        };
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int computeNodeCount(ComputeResource resource) {
                return 8;      // ceiling 8; the fixture's index has 2 tablets
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addReshardCandidate(long dbId, long tableId, long maxTabletSize,
                    long minAdjacentTabletPairSize, long maxUnderProvisionedTabletSize) {
                captured[0] = maxUnderProvisionedTabletSize;
            }
        };
        registerComputeNodes(16);
        // 4 GiB is the band the early rule exists for: at or above ceil(1.5 x tablet_reshard_min_split_size)
        // = 3 GiB, and far below ceil(1.5 x tablet_reshard_target_size) = 15 GiB, where the size-based rule
        // would already split. A producer gated on the size-based threshold emits nothing here.
        long earlyOnlySize = EARLY_SPLIT_THRESHOLD + GIB;
        PublishFixture f = newPublishFixture(earlyOnlySize);
        f.applier().applyVisibleLog(f.state(), f.tableCommitInfo(), f.db());
        Assertions.assertEquals(earlyOnlySize, captured[0],
                "an index below its ceiling must emit its largest tablet as the early signal");
    }

    @Test
    public void testCommitAndApply() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(partitionCommitInfo.getVersionTime(),
                table.getPartition(partitionId).getDefaultPhysicalPartition()
                        .getVisibleVersionTime());
    }

    @Test
    public void testCommitAndApplyCompaction() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newCompactionTransactionState();
        CompactionTxnCommitAttachment attachment = new CompactionTxnCommitAttachment(true);
        state.setTxnCommitAttachment(attachment);
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(3, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(partitionCommitInfo.getVersionTime(),
                table.getPartition(partitionId).getDefaultPhysicalPartition()
                        .getVisibleVersionTime());
    }

    @Test
    public void testApplyVisibleLogUpdatesLakeTabletAndEnqueues() {
        // Build a table with two tablets in the index: one WITH a matching stat entry, one WITHOUT.
        MaterializedIndex index = new MaterializedIndex(indexId);
        LakeTablet lakeTablet = new LakeTablet(tabletId[0]);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId[0], tabletMeta);
        index.addTablet(lakeTablet, tabletMeta);

        // Second tablet has no entry in tabletStats — verifies per-tablet selectivity.
        LakeTablet noStatTablet = new LakeTablet(tabletId[1]);
        TabletMeta noStatMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId[1], noStatMeta);
        index.addTablet(noStatTablet, noStatMeta);

        LakeTable table = buildLakeTableWithIndex(index);
        // Range distribution is required for the publish-driven reshard path to evaluate the table.
        table.setDefaultDistributionInfo(new RangeDistributionInfo());
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);

        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
        applier.applyCommitLog(state, tableCommitInfo);

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        long versionTime = System.currentTimeMillis();
        partitionCommitInfo.setVersionTime(versionTime);

        // Populate tabletStats for tabletId[0] only — tabletId[1] is intentionally absent.
        // Oversize tabletId[0] so the precomputed split signal crosses the threshold and the
        // table is enqueued as a reshard candidate.
        long oversize = Config.tablet_reshard_target_size * 2;
        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 5L;
        stat.dataSize = oversize;
        Map<Long, TabletStatPB> stats = new HashMap<>();
        stats.put(tabletId[0], stat);
        partitionCommitInfo.getTabletStats().putAll(stats);

        // Mock leader=true, checkpoint=false; intercept addReshardCandidate to count calls
        AtomicInteger addCandidateCalls = new AtomicInteger(0);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public static boolean isCheckpointThread() {
                return false;
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addReshardCandidate(long dbId, long tableId, long maxTabletSize,
                    long minAdjacentTabletPairSize, long maxUnderProvisionedTabletSize) {
                addCandidateCalls.incrementAndGet();
            }
        };

        Database db = new Database(dbId, "test_db");
        applier.applyVisibleLog(state, tableCommitInfo, db);

        // Tablet with a stat entry: fields must be updated.
        Assertions.assertEquals(oversize, lakeTablet.getDataSize(true));
        Assertions.assertEquals(5L, lakeTablet.getRowCount(0));
        Assertions.assertEquals(versionTime, lakeTablet.getDataSizeUpdateTime());
        Assertions.assertEquals(1, addCandidateCalls.get(), "addReshardCandidate should be called once");

        // Tablet WITHOUT a stat entry: must remain at default values (per-tablet selectivity).
        Assertions.assertEquals(0L, noStatTablet.getDataSizeUpdateTime(),
                "tablet absent from tabletStats must not have its update-time modified");
        Assertions.assertEquals(0L, noStatTablet.getDataSize(true),
                "tablet absent from tabletStats must not have its data-size modified");
    }

    @Test
    public void testApplyVisibleLogSkippedOnNonLeader() {
        // Use indexId+100 to avoid any ID collision with the positive test's index (indexId).
        long negativeIndexId = indexId + 100;
        MaterializedIndex index = new MaterializedIndex(negativeIndexId);
        LakeTablet lakeTablet = new LakeTablet(tabletId[1]);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, 0, TStorageMedium.HDD, true);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(tabletId[1], tabletMeta);
        index.addTablet(lakeTablet, tabletMeta);

        LakeTable table = buildLakeTableWithIndex(index);
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.COMMITTED);

        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
        applier.applyCommitLog(state, tableCommitInfo);

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());

        TabletStatPB stat = new TabletStatPB();
        stat.numRows = 10L;
        stat.dataSize = 888L;
        partitionCommitInfo.getTabletStats().put(tabletId[1], stat);

        // Mock leader=false; intercept addReshardCandidate to prove it is never called.
        AtomicInteger addCandidateCalls = new AtomicInteger(0);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addReshardCandidate(long dbId, long tableId, long maxTabletSize,
                    long minAdjacentTabletPairSize, long maxUnderProvisionedTabletSize) {
                addCandidateCalls.incrementAndGet();
            }
        };

        long beforeUpdateTime = lakeTablet.getDataSizeUpdateTime();
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);

        // LakeTablet fields must be unchanged on a non-leader node.
        Assertions.assertEquals(beforeUpdateTime, lakeTablet.getDataSizeUpdateTime());
        Assertions.assertEquals(0L, lakeTablet.getDataSize(true));
        // addReshardCandidate must not have been invoked at all.
        Assertions.assertEquals(0, addCandidateCalls.get(),
                "addReshardCandidate must not be called on a non-leader node");
    }

    @Test
    public void testShadowRewriteTxnApplyLogsDoNotTouchPartitionVersion() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        // Shadow-rewrite txns are now identified by LoadJobSourceType.SHADOW_REWRITE on the txn.
        TransactionState state = new TransactionState(dbId, Lists.newArrayList(tableId), nextTxnId++,
                "label_shadow", null, TransactionState.LoadJobSourceType.SHADOW_REWRITE, null, 0, 60_000);
        state.setTransactionStatus(TransactionStatus.COMMITTED);

        // PartitionCommitInfo uses sentinel version -1; no per-partition isShadowRewrite marker needed.
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, -1, 0);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        long vis0 = table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion();
        long next0 = table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion();

        // applyCommitLog must not bump nextVersion for a shadow-rewrite txn.
        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(vis0, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(next0, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        // applyVisibleLog must not advance visibleVersion for a shadow-rewrite txn.
        state.setTransactionStatus(TransactionStatus.VISIBLE);
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(vis0, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(next0, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
    }

    @Test
    public void testApplyCommitLogWithDroppedPartition() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        TransactionState state = newTransactionState();
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId - 1, 2, 0);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyCommitLog(state, tableCommitInfo);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());

        state.setTransactionStatus(TransactionStatus.VISIBLE);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(1, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getNextVersion());
    }

    @Test
    public void testApplyVisibleLogRecordsLastUpdateTimeForUserWrite() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        // A routine-load (user write) visible txn must advance lastUpdateTime on the shared-data path.
        TransactionState state = newTransactionState();
        state.setTransactionStatus(TransactionStatus.VISIBLE);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        long versionTime = System.currentTimeMillis();
        partitionCommitInfo.setVersionTime(versionTime);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(versionTime,
                table.getPartition(partitionId).getDefaultPhysicalPartition().getLastUpdateTime());
    }

    @Test
    public void testApplyVisibleLogSkipsLastUpdateTimeForCompaction() {
        LakeTable table = buildLakeTable();
        LakeTableTxnLogApplier applier = new LakeTableTxnLogApplier(table);
        // Compaction is not a user write: it advances the visible version but must NOT touch
        // lastUpdateTime, which must stay 0 (its initial value).
        TransactionState state = newCompactionTransactionState();
        state.setTxnCommitAttachment(new CompactionTxnCommitAttachment(true));
        state.setTransactionStatus(TransactionStatus.VISIBLE);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(physicalPartitionId, 2, 0);
        partitionCommitInfo.setVersionTime(System.currentTimeMillis());
        TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);

        applier.applyVisibleLog(state, tableCommitInfo, /*unused*/null);
        Assertions.assertEquals(2, table.getPartition(partitionId).getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(0L,
                table.getPartition(partitionId).getDefaultPhysicalPartition().getLastUpdateTime());
    }
}
