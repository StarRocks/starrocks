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

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkLogEntry;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkRange;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.lake.bookmark.IndexEpoch;
import com.starrocks.lake.bookmark.PhysicalPartitionMeta;
import com.starrocks.lake.changes.ChangesMetaDescriptor;
import com.starrocks.planner.AnalyticEvalNode;
import com.starrocks.planner.ChangesScanNode;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TChangeDerivationMode;
import com.starrocks.thrift.TChangeScanSpec;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ChangesScanBuilder#buildScanOperator} — the [_CHANGES_] entry
 * point shared by the SQL analyzer and non-SQL callers (IVM refresh). Covers
 * bookmark resolution, non-trackable-delta messaging, and the scoped-table
 * substitution that RelationTransformer relies on.
 *
 * <p>Also covers net-change gating: {@code applyNetChange} stacks a Window
 * and Filter above the scan only for a primary-key table (the SQL path gates on
 * {@code enable_cdc_net_change}), and is a no-op otherwise.
 *
 * <p>Bookmarks are minted by calling BookmarkManager directly; INSERTs are
 * not available in the FE UT framework, so consecutive create() calls only
 * return distinct bookmarks after bumping physical-partition visibleVersion.
 */
public class ChangesScanBuilderTest extends BookmarkTestBase {

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

    @BeforeAll
    public static void beforeAllResolution() {
        // CREATE VIEW round-trip probe in the framework would reject CHANGES.
        FeConstants.unitTestView = false;
    }

    @AfterAll
    public static void afterAllResolution() {
        FeConstants.unitTestView = true;
    }

    @Test
    public void testRelationTransformerUsesScopedTableForChanges() throws Exception {
        String tableName = "dup_scoped_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        live.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("scoped_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("scoped_head");

        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(live, 7L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                    tableName, base.getBookmarkId(), head.getBookmarkId());
            QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    sql, connectContext);
            LogicalPlan plan = new RelationTransformer(new ColumnRefFactory(), connectContext)
                    .transformWithSelectLimit(stmt.getQueryRelation());

            LogicalChangesScanOperator scan = findChangesScan(plan.getRoot());
            assertNotNull(scan, "expected LogicalChangesScan in transformed plan");

            OlapTable scoped = (OlapTable) scan.getTable();
            // The transformer must hand the operator the scoped table, not
            // the live catalog instance — otherwise the scan would see
            // post-head partition data.
            assertNotSame(live, scoped);

            // The scoped table's partitions must equal the trackable logical
            // partitions in the delta. For a freshly-created table with a
            // DataChanged bump this is the full single partition.
            Set<Long> expected = new HashSet<>();
            for (Map.Entry<Long, List<BookmarkChange.PhysicalPartitionChange>> entry :
                    scan.getDelta().getChanges().entrySet()) {
                for (BookmarkChange.PhysicalPartitionChange c : entry.getValue()) {
                    if (c instanceof BookmarkChange.PartitionAdded
                            || c instanceof BookmarkChange.DataChanged) {
                        expected.add(c.getLogicalPartitionId());
                        break;
                    }
                }
            }
            Set<Long> actual = new HashSet<>();
            for (Partition p : scoped.getPartitions()) {
                actual.add(p.getId());
            }
            assertEquals(expected, actual);
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testBuildRejectsPartitionDropped() throws Exception {
        String tableName = "dup_drop_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        live.maySetDatabaseId(dbId);

        // base carries a phantom physical partition that head/live lacks → PartitionDropped.
        long phantomLogicalId = 999_001L;
        long phantomPhysicalId = 999_002L;
        Map<Long, Map<Long, PhysicalPartitionMeta>> baseParts = liveSnapshot(live);
        baseParts.computeIfAbsent(phantomLogicalId, k -> new HashMap<>())
                .put(phantomPhysicalId, new PhysicalPartitionMeta(1L, 1L, 1L, 0L));
        Bookmark base = synthesizeAndRegister(tableId, baseParts);
        Bookmark head = synthesizeAndRegister(tableId, liveSnapshot(live));

        SemanticException ex = assertThrows(SemanticException.class,
                () -> ChangesScanBuilder.buildScanOperator(
                        live,
                        new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                        new HashMap<>(),
                        new HashMap<>(),
                        List.of(), null, null, null, false));
        String expected = String.format(
                "CHANGES from bookmark %d to %d on table '%s' not trackable: physical partition %d dropped",
                base.getBookmarkId(), head.getBookmarkId(), tableName, phantomPhysicalId);
        assertTrue(ex.getMessage().contains(expected),
                "expected message to contain '" + expected + "', got: " + ex.getMessage());
    }

    @Test
    public void testBuildRejectsIndexReplaced() throws Exception {
        String tableName = "dup_idx_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        live.maySetDatabaseId(dbId);

        Bookmark base = synthesizeAndRegister(tableId, liveSnapshot(live));
        Map<Long, Map<Long, PhysicalPartitionMeta>> headParts = liveSnapshot(live);
        long shiftedPhysicalId = shiftFirstPhysical(headParts, /* shiftMetaId = */ true);
        Bookmark head = synthesizeAndRegister(tableId, headParts);

        SemanticException ex = assertThrows(SemanticException.class,
                () -> ChangesScanBuilder.buildScanOperator(
                        live,
                        new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                        new HashMap<>(),
                        new HashMap<>(),
                        List.of(), null, null, null, false));
        String expected = String.format(
                "CHANGES from bookmark %d to %d on table '%s' not trackable: physical partition %d rewritten",
                base.getBookmarkId(), head.getBookmarkId(), tableName, shiftedPhysicalId);
        assertTrue(ex.getMessage().contains(expected),
                "expected message to contain '" + expected + "', got: " + ex.getMessage());
    }

    @Test
    public void testBuildRejectsTabletReshard() throws Exception {
        String tableName = "dup_resh_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        live.maySetDatabaseId(dbId);

        Bookmark base = synthesizeAndRegister(tableId, liveSnapshot(live));
        Map<Long, Map<Long, PhysicalPartitionMeta>> headParts = liveSnapshot(live);
        long shiftedPhysicalId = shiftFirstPhysical(headParts, /* shiftMetaId = */ false);
        Bookmark head = synthesizeAndRegister(tableId, headParts);

        SemanticException ex = assertThrows(SemanticException.class,
                () -> ChangesScanBuilder.buildScanOperator(
                        live,
                        new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                        new HashMap<>(),
                        new HashMap<>(),
                        List.of(), null, null, null, false));
        String expected = String.format(
                "CHANGES from bookmark %d to %d on table '%s' not trackable: physical partition %d resharded",
                base.getBookmarkId(), head.getBookmarkId(), tableName, shiftedPhysicalId);
        assertTrue(ex.getMessage().contains(expected),
                "expected message to contain '" + expected + "', got: " + ex.getMessage());
    }

    @Test
    public void testReshardedSpecShapes() {
        // Two-generation reshard chain: index 100 (metaId 50), then index 101 (metaId 50) taking
        // over at version 20 -- the same shape ReshardEpochResolverTest/BookmarkChangeTest use.
        MaterializedIndex secondGeneration = new MaterializedIndex(101, 50, MaterializedIndex.IndexState.NORMAL, 7);
        List<IndexEpoch> epochs = List.of(new IndexEpoch(secondGeneration, 5, 35));
        PhysicalPartitionMeta headMeta = new PhysicalPartitionMeta(101, 50, 35, 0L);

        // base at the empty initial version: net-fold short-circuits to a head-only FULL_SCAN even
        // across the reshard.
        PhysicalPartitionMeta emptyBaseMeta =
                new PhysicalPartitionMeta(100, 50, PhysicalPartition.PARTITION_INIT_VERSION, 0L);
        BookmarkChange.ReshardedDataChanged emptyBaseChange =
                new BookmarkChange.ReshardedDataChanged(1L, 10L, emptyBaseMeta, headMeta, epochs);
        assertTrue(ChangesScanBuilder.useHeadOnlyFullScan(emptyBaseChange, true));
        assertFalse(ChangesScanBuilder.isGenerationCrossing(emptyBaseChange, true));
        // Without the net-change fold the shortcut does not apply, so even an empty base crosses.
        assertTrue(ChangesScanBuilder.isGenerationCrossing(emptyBaseChange, false));
        TChangeScanSpec shortcut = ChangesScanBuilder.buildPartitionScanSpec(emptyBaseChange, true).orElseThrow();
        assertEquals(TChangeDerivationMode.FULL_SCAN, shortcut.getDerivation_mode());
        assertEquals(35, shortcut.getHead_version());

        // base at a non-empty version: no single spec -- the scan node builds one per epoch.
        PhysicalPartitionMeta nonEmptyBaseMeta = new PhysicalPartitionMeta(100, 50, 5, 0L);
        BookmarkChange.ReshardedDataChanged nonEmptyBaseChange =
                new BookmarkChange.ReshardedDataChanged(1L, 10L, nonEmptyBaseMeta, headMeta, epochs);
        assertFalse(ChangesScanBuilder.useHeadOnlyFullScan(nonEmptyBaseChange, true));
        assertTrue(ChangesScanBuilder.isGenerationCrossing(nonEmptyBaseChange, true));
        assertTrue(ChangesScanBuilder.buildPartitionScanSpec(nonEmptyBaseChange, true).isEmpty());

        // Neither a plain data change nor an absent change crosses generations.
        assertFalse(ChangesScanBuilder.isGenerationCrossing(
                new BookmarkChange.DataChanged(1L, 10L, nonEmptyBaseMeta, headMeta), false));
        assertFalse(ChangesScanBuilder.isGenerationCrossing(null, false));

        TChangeScanSpec epochSpec = ChangesScanBuilder.buildEpochScanSpec(new IndexEpoch(secondGeneration, 5, 19));
        assertEquals(TChangeDerivationMode.VERSION_CHAIN_DIFF, epochSpec.getDerivation_mode());
        assertEquals(5, epochSpec.getBase_version());
        assertEquals(19, epochSpec.getHead_version());
    }

    @Test
    public void testBuildRejectsTabletHintOnReshardedDelta() throws Exception {
        String tableName = "dup_reshhint_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);
        PhysicalPartition pp = table.getPartitions().iterator().next().getSubPartitions().iterator().next();
        MaterializedIndex baseGeneration = pp.getLatestBaseIndex();

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("resh_hint_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("resh_hint_head");
        bumpVisibleVersion(table, 5L);
        Bookmark base = bm.create(dbId, tableId, hBase);

        // Simulate a tablet reshard: install a new generation on the same base-index meta id,
        // taking over from the original generation at version 20.
        MaterializedIndex newGeneration = new MaterializedIndex(
                GlobalStateMgr.getCurrentState().getNextId(), baseGeneration.getMetaId(),
                MaterializedIndex.IndexState.NORMAL, 7);
        newGeneration.setTakeoverVersion(20);
        newGeneration.setPredecessorIndexId(baseGeneration.getId());
        pp.addMaterializedIndex(newGeneration, true);
        pp.setVisibleVersion(35L, System.currentTimeMillis());
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            SemanticException ex = assertThrows(SemanticException.class,
                    () -> ChangesScanBuilder.buildScanOperator(
                            table,
                            new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                            new HashMap<>(),
                            new HashMap<>(),
                            List.of(), null, List.of(999L), null, false));
            assertTrue(ex.getMessage().contains("does not support a TABLET hint")
                            && ex.getMessage().contains("crosses a tablet reshard"),
                    "actual: " + ex.getMessage());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testBuildRejectsKeyPartitionHint() throws Exception {
        String tableName = "dup_keypart_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("keypart_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("keypart_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 4L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        // PARTITION(dt='2026-02-15') — the key-partition value form. The delta is
        // trackable, so the only rejection must come from the unsupported hint form.
        List<Expr> colValues = new ArrayList<>();
        colValues.add(new StringLiteral("2026-02-15"));
        PartitionRef keyPartitionHint = new PartitionRef(
                new ArrayList<>(), false, List.of("dt"), colValues, NodePosition.ZERO);

        try {
            SemanticException ex = assertThrows(SemanticException.class,
                    () -> ChangesScanBuilder.buildScanOperator(
                            table,
                            new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                            new HashMap<>(),
                            new HashMap<>(),
                            List.of(), keyPartitionHint, null, null, false));
            assertTrue(ex.getMessage().contains("does not support a PARTITION hint by column value"),
                    "got: " + ex.getMessage());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testBuildFromBookmarkRange() throws Exception {
        String tableName = "dup_br_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("range_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("range_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 4L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            LogicalChangesScanOperator op = ChangesScanBuilder.buildScanOperator(
                    table,
                    new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                    new HashMap<>(),
                    new HashMap<>(),
                    List.of(), null, null, null, false);
            assertNotNull(op);
            assertEquals(base.getBookmarkId(), op.getBase().getBookmarkId());
            assertEquals(head.getBookmarkId(), op.getHead().getBookmarkId());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testBuildScanOperatorCarriesDistributionSpec() throws Exception {
        String tableName = "dup_dist_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("dist_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("dist_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 4L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            // table is DISTRIBUTED BY HASH(k); build a ref for the bucket column the way
            // RelationTransformer does, then thread it through as a hash-local DistributionSpec.
            Column bucketColumn = table.getColumn("k");
            ColumnRefOperator bucketRef = new ColumnRefFactory()
                    .create(bucketColumn.getName(), bucketColumn.getType(), bucketColumn.isAllowNull());
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
            colRefToColumnMetaMap.put(bucketRef, bucketColumn);
            Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>();
            columnMetaToColRefMap.put(bucketColumn, bucketRef);
            DistributionSpec hashLocal = DistributionSpec.createHashDistributionSpec(
                    new HashDistributionDesc(List.of(bucketRef.getId()), HashDistributionDesc.SourceType.LOCAL));

            LogicalChangesScanOperator op = ChangesScanBuilder.buildScanOperator(
                    table,
                    new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                    colRefToColumnMetaMap,
                    columnMetaToColRefMap,
                    List.of(), null, null, hashLocal, false);
            assertSame(hashLocal, op.getDistributionSpec());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testGetBucketNumsWithTabletPrunedPartition() throws Exception {
        // A HASH-distributed table whose delta spans two range partitions. Constructing the physical
        // node with only one partition's tablets selected drives getSelectedPhysicalPartitions(true)'s
        // fully-tablet-pruned skip (the other partition contributes no scan tablet), and getBucketNums
        // still reports the table's bucket count without needing computeScanRanges / live backends.
        String tableName = "dup_bucket_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createHashRangeTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("bucket_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("bucket_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            LogicalChangesScanOperator op = ChangesScanBuilder.buildScanOperator(
                    table,
                    new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                    new HashMap<>(),
                    new HashMap<>(),
                    List.of(), null, null, null, false);
            // Both partitions must show up in the delta so one can be fully tablet-pruned.
            assertTrue(op.getDelta().getChanges().size() >= 2,
                    "delta must span both partitions, got: " + op.getDelta().getChanges().keySet());

            List<Long> logicalPartitionIds = new ArrayList<>(op.getDelta().getChanges().keySet());
            long keptLogicalId = logicalPartitionIds.get(0);
            // Only the kept partition's tablets are selected; the other partition's tablets are all
            // pruned away, making it the "empty" partition the skip in getSelectedPhysicalPartitions
            // must drop.
            List<Long> keptTabletIds = new ArrayList<>();
            for (PhysicalPartition pp : table.getPartition(keptLogicalId).getSubPartitions()) {
                for (Tablet t : pp.getLatestBaseIndex().getTablets()) {
                    keptTabletIds.add(t.getId());
                }
            }

            TupleDescriptor tuple = new DescriptorTable().createTupleDescriptor("changes_scan");
            ChangesScanNode prunedNode = new ChangesScanNode(
                    new PlanNodeId(1), tuple, table, op.getDelta(), op.getBase(), op.getHead(),
                    op.getChangesMetaDescriptors(), logicalPartitionIds, keptTabletIds, false);
            // >= 2 selected logical partitions on a HASH table report the table's bucket count (BUCKETS 3),
            // reached only after the empty tablet-pruned partition is skipped.
            assertEquals(3, prunedNode.getBucketNums());
            assertFalse(prunedNode.getBucketProperties().isPresent());

            // No tablet pruning: the selectedTabletIds == null short-circuit keeps every partition, and
            // the bucket count is unchanged.
            ChangesScanNode unprunedNode = new ChangesScanNode(
                    new PlanNodeId(2), tuple, table, op.getDelta(), op.getBase(), op.getHead(),
                    op.getChangesMetaDescriptors(), logicalPartitionIds, null, false);
            assertEquals(3, unprunedNode.getBucketNums());
            assertFalse(unprunedNode.getBucketProperties().isPresent());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        String tableName = "dup_eq_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        // Four ascending bookmarks so base and head can be varied independently.
        // create() only mints a distinct bookmark once visibleVersion advances.
        BookmarkHolder h0 = BookmarkHolder.forEmptyInfo("eq_0");
        Bookmark b0 = bm.create(dbId, tableId, h0);
        bumpVisibleVersion(table, 3L);
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("eq_1");
        Bookmark b1 = bm.create(dbId, tableId, h1);
        bumpVisibleVersion(table, 5L);
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("eq_2");
        Bookmark b2 = bm.create(dbId, tableId, h2);
        bumpVisibleVersion(table, 7L);
        BookmarkHolder h3 = BookmarkHolder.forEmptyInfo("eq_3");
        Bookmark b3 = bm.create(dbId, tableId, h3);

        try {
            BookmarkRange range = new BookmarkRange(b1.getBookmarkId(), b2.getBookmarkId());
            LogicalChangesScanOperator op = ChangesScanBuilder.buildScanOperator(
                    table, range, new HashMap<>(), new HashMap<>(), List.of(), null, null, null, false);
            LogicalChangesScanOperator same = ChangesScanBuilder.buildScanOperator(
                    table, range, new HashMap<>(), new HashMap<>(), List.of(), null, null, null, false);

            // Reflexive, and two unpruned scans over the same (table, base, head)
            // are equal with matching hashCode — the equality the Cascades memo
            // relies on to dedup the pruned scan against the unpruned one.
            assertEquals(op, op);
            assertEquals(op, same);
            assertEquals(op.hashCode(), same.hashCode());

            // A different runtime type (null here) trips the super.equals guard.
            assertNotEquals(op, null);

            // A different bookmark window is a different scan. The scoped table
            // reuses the live table id, so the base/head bookmark ids are the
            // only thing distinguishing these from op at the operator level.
            LogicalChangesScanOperator baseDiff = ChangesScanBuilder.buildScanOperator(
                    table, new BookmarkRange(b0.getBookmarkId(), b2.getBookmarkId()),
                    new HashMap<>(), new HashMap<>(), List.of(), null, null, null, false);
            LogicalChangesScanOperator headDiff = ChangesScanBuilder.buildScanOperator(
                    table, new BookmarkRange(b1.getBookmarkId(), b3.getBookmarkId()),
                    new HashMap<>(), new HashMap<>(), List.of(), null, null, null, false);
            assertNotEquals(op, baseDiff);
            assertNotEquals(op, headDiff);

            // Different CHANGES metadata columns (__CHANGE_TYPE__, __ROW_VERSION__)
            // are part of the scan's output identity.
            LogicalChangesScanOperator metaDiff = ChangesScanBuilder.buildScanOperator(
                    table, range, new HashMap<>(), new HashMap<>(),
                    ChangesMetaDescriptor.resolve(table.getBaseSchema()), null, null, null, false);
            assertNotEquals(op, metaDiff);

            // The selected ids are what make a pruned scan a distinct memo
            // alternative from the unpruned op.
            LogicalChangesScanOperator partitionPruned = new LogicalChangesScanOperator.Builder()
                    .withOperator(op).setSelectedLogicalPartitionId(List.of(1L, 2L)).build();
            LogicalChangesScanOperator tabletPruned = new LogicalChangesScanOperator.Builder()
                    .withOperator(op).setSelectedTabletId(List.of(10L)).build();
            assertNotEquals(op, partitionPruned);
            assertNotEquals(op, tabletPruned);

            // distributionSpec is part of the scan's identity: it decides the advertised output
            // property (LOCAL hash vs none), so two scans differing only in it are unequal AND must
            // hash differently. The hashCode assertion fails if distributionSpec is dropped from
            // hashCode while kept in equals (op here carries a null spec, distDiff a hash-local one).
            Column bucketColumn = table.getColumn("k");
            ColumnRefOperator bucketRef = new ColumnRefFactory()
                    .create(bucketColumn.getName(), bucketColumn.getType(), bucketColumn.isAllowNull());
            DistributionSpec hashLocal = DistributionSpec.createHashDistributionSpec(
                    new HashDistributionDesc(List.of(bucketRef.getId()), HashDistributionDesc.SourceType.LOCAL));
            LogicalChangesScanOperator distDiff = new LogicalChangesScanOperator.Builder()
                    .withOperator(op).setDistributionSpec(hashLocal).build();
            assertNotEquals(op, distDiff);
            assertNotEquals(op.hashCode(), distDiff.hashCode());

            // delta is intentionally excluded from equals: it is a pure function
            // of (table, base, head), so it cannot be varied alone to assert on.
        } finally {
            bm.releaseReference(dbId, tableId, b0.getBookmarkId(), h0.getHolderId());
            bm.releaseReference(dbId, tableId, b1.getBookmarkId(), h1.getHolderId());
            bm.releaseReference(dbId, tableId, b2.getBookmarkId(), h2.getHolderId());
            bm.releaseReference(dbId, tableId, b3.getBookmarkId(), h3.getHolderId());
        }
    }

    @Test
    public void testPhysicalEqualsAndHashCode() throws Exception {
        String tableName = "dup_peq_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        // Four ascending bookmarks so base and head can be varied independently.
        BookmarkHolder h0 = BookmarkHolder.forEmptyInfo("peq_0");
        Bookmark b0 = bm.create(dbId, tableId, h0);
        bumpVisibleVersion(table, 3L);
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("peq_1");
        Bookmark b1 = bm.create(dbId, tableId, h1);
        bumpVisibleVersion(table, 5L);
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("peq_2");
        Bookmark b2 = bm.create(dbId, tableId, h2);
        bumpVisibleVersion(table, 7L);
        BookmarkHolder h3 = BookmarkHolder.forEmptyInfo("peq_3");
        Bookmark b3 = bm.create(dbId, tableId, h3);

        try {
            // The physical operator has no Builder; construct directly, varying one
            // field at a time. delta is excluded from equals (a pure function of
            // base/head), so it is passed as null throughout.
            PhysicalChangesScanOperator op = new PhysicalChangesScanOperator(
                    table, new HashMap<>(), Operator.DEFAULT_LIMIT, null, null,
                    b1, b2, null, List.of(), null, null, null, false);
            PhysicalChangesScanOperator same = new PhysicalChangesScanOperator(
                    table, new HashMap<>(), Operator.DEFAULT_LIMIT, null, null,
                    b1, b2, null, List.of(), null, null, null, false);

            // Reflexive, and two unpruned scans over the same (table, base, head) are
            // equal with matching hashCode — the equality the Cascades memo relies on
            // to dedup the pruned scan against the unpruned one.
            assertEquals(op, op);
            assertEquals(op, same);
            assertEquals(op.hashCode(), same.hashCode());

            // A different runtime type (null here) trips the super.equals guard.
            assertNotEquals(op, null);

            // A different bookmark window is a different scan.
            PhysicalChangesScanOperator baseDiff = new PhysicalChangesScanOperator(
                    table, new HashMap<>(), Operator.DEFAULT_LIMIT, null, null,
                    b0, b2, null, List.of(), null, null, null, false);
            PhysicalChangesScanOperator headDiff = new PhysicalChangesScanOperator(
                    table, new HashMap<>(), Operator.DEFAULT_LIMIT, null, null,
                    b1, b3, null, List.of(), null, null, null, false);
            assertNotEquals(op, baseDiff);
            assertNotEquals(op, headDiff);

            // Different CHANGES metadata columns are part of the scan's output identity.
            PhysicalChangesScanOperator metaDiff = new PhysicalChangesScanOperator(
                    table, new HashMap<>(), Operator.DEFAULT_LIMIT, null, null,
                    b1, b2, null, ChangesMetaDescriptor.resolve(table.getBaseSchema()), null, null, null, false);
            assertNotEquals(op, metaDiff);

            // The selected ids are what make a pruned physical scan a distinct memo
            // alternative from the unpruned op — exactly the dedup bug this guards against.
            PhysicalChangesScanOperator partitionPruned = new PhysicalChangesScanOperator(
                    table, new HashMap<>(), Operator.DEFAULT_LIMIT, null, null,
                    b1, b2, null, List.of(), List.of(1L, 2L), null, null, false);
            PhysicalChangesScanOperator tabletPruned = new PhysicalChangesScanOperator(
                    table, new HashMap<>(), Operator.DEFAULT_LIMIT, null, null,
                    b1, b2, null, List.of(), null, List.of(10L), null, false);
            assertNotEquals(op, partitionPruned);
            assertNotEquals(op, tabletPruned);

            // netChange is part of the scan's identity: an empty-base partition reads FULL_SCAN vs VERSION_CHAIN_DIFF.
            PhysicalChangesScanOperator netChangeDiff = new PhysicalChangesScanOperator(
                    table, new HashMap<>(), Operator.DEFAULT_LIMIT, null, null,
                    b1, b2, null, List.of(), null, null, null, true);
            assertNotEquals(op, netChangeDiff);
        } finally {
            bm.releaseReference(dbId, tableId, b0.getBookmarkId(), h0.getHolderId());
            bm.releaseReference(dbId, tableId, b1.getBookmarkId(), h1.getHolderId());
            bm.releaseReference(dbId, tableId, b2.getBookmarkId(), h2.getHolderId());
            bm.releaseReference(dbId, tableId, b3.getBookmarkId(), h3.getHolderId());
        }
    }

    @Test
    public void testBuildRejectsUnknownBookmarks() throws Exception {
        String tableName = "dup_brnf_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hReal = BookmarkHolder.forEmptyInfo("range_nf_real");
        Bookmark real = bm.create(dbId, tableId, hReal);

        try {
            SemanticException baseEx = assertThrows(SemanticException.class,
                    () -> ChangesScanBuilder.buildScanOperator(
                            table,
                            new BookmarkRange(99999L, real.getBookmarkId()),
                            new HashMap<>(),
                            new HashMap<>(),
                            List.of(), null, null, null, false));
            assertTrue(baseEx.getMessage().contains("bookmark 99999 not found"),
                    "actual: " + baseEx.getMessage());

            SemanticException headEx = assertThrows(SemanticException.class,
                    () -> ChangesScanBuilder.buildScanOperator(
                            table,
                            new BookmarkRange(real.getBookmarkId(), 99998L),
                            new HashMap<>(),
                            new HashMap<>(),
                            List.of(), null, null, null, false));
            assertTrue(headEx.getMessage().contains("bookmark 99998 not found"),
                    "actual: " + headEx.getMessage());
        } finally {
            bm.releaseReference(dbId, tableId, real.getBookmarkId(), hReal.getHolderId());
        }
    }

    @Test
    public void testBuildRejectsMissingDbId() throws Exception {
        // buildScanOperator is the entry point IVM refresh + other non-SQL callers
        // share with the analyzer; production callers always populate dbId, but the
        // method documents the IllegalStateException for callers that skip the step.
        String tableName = "dup_nodbid_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        // Intentionally skip maySetDatabaseId so mayGetDatabaseId stays empty.

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> ChangesScanBuilder.buildScanOperator(
                        table,
                        new BookmarkRange(1L, 2L),
                        new HashMap<>(),
                        new HashMap<>(),
                        List.of(), null, null, null, false));
        assertTrue(ex.getMessage().contains("dbId missing on " + tableName),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testNetChangeOnPkWhenFlagOn() throws Exception {
        String name = "pk_nc_on_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createPkTable(name);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("nc_on_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("nc_on_head");
        // Take base off the empty initial version so the range is a genuine multi-version VERSION_CHAIN_DIFF;
        // a base at PARTITION_INIT_VERSION reads as FULL_SCAN, which needs no net-change fold.
        bumpVisibleVersion(table, 2L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                name, base.getBookmarkId(), head.getBookmarkId());
        boolean old = connectContext.getSessionVariable().isEnableCdcNetChange();
        connectContext.getSessionVariable().setEnableCdcNetChange(true);
        try {
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            List<AnalyticEvalNode> analyticNodes = new ArrayList<>();
            execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, analyticNodes);
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertFalse(analyticNodes.isEmpty(),
                    "flag-on PK table must produce a window/analytic node:\n" + plan);
            // The analytic node must compute both a min and a max (one per function call).
            assertTrue(analyticNodes.get(0).getAnalyticFnCalls().size() >= 2,
                    "window node must carry both MIN and MAX calls:\n" + plan);
            assertTrue(plan.contains("ChangesScanNode"),
                    "plan must include ChangesScanNode:\n" + plan);
            // The window groups by primary key with no ORDER BY, so it is sort-based and needs its
            // input sorted by the key. The optimizer must enforce that sort below the window; if it
            // is missing, a key split across rowsets gets netted over wrong partition boundaries
            // (see ChangesScanBuilder.buildNetChangeOperators enforceSortColumns).
            assertTrue(plan.contains("SORT"),
                    "net-change window must have a SORT enforced below it:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setEnableCdcNetChange(old);
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testNetChangeSkippedWhenBaseAtInitVersion() throws Exception {
        // A base at the empty initial version reads as FULL_SCAN: head's live PK rows already appear
        // once per key as inserts, so the net-change fold is unnecessary and must be skipped even
        // with the flag on (the Window+Filter would be an identity transform at full analytic cost).
        String name = "pk_nc_init_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createPkTable(name);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("nc_init_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("nc_init_head");
        // base captured at the fresh table's empty initial version (PARTITION_INIT_VERSION).
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                name, base.getBookmarkId(), head.getBookmarkId());
        boolean old = connectContext.getSessionVariable().isEnableCdcNetChange();
        connectContext.getSessionVariable().setEnableCdcNetChange(true);
        try {
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            List<AnalyticEvalNode> analyticNodes = new ArrayList<>();
            execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, analyticNodes);
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(analyticNodes.isEmpty(),
                    "FULL_SCAN (base at the initial version) must skip the net-change window:\n" + plan);
            assertTrue(plan.contains("ChangesScanNode"),
                    "plan must include ChangesScanNode:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setEnableCdcNetChange(old);
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testNetChangeUsesHashPartitionWhenModeHash() throws Exception {
        String name = "pk_nc_hash_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createPkTable(name);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("nc_hash_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("nc_hash_head");
        // Take base off the empty initial version so the range is a genuine multi-version VERSION_CHAIN_DIFF;
        // a base at PARTITION_INIT_VERSION reads as FULL_SCAN, which needs no net-change fold.
        bumpVisibleVersion(table, 2L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                name, base.getBookmarkId(), head.getBookmarkId());
        boolean oldFlag = connectContext.getSessionVariable().isEnableCdcNetChange();
        int oldMode = connectContext.getSessionVariable().getWindowPartitionMode();
        connectContext.getSessionVariable().setEnableCdcNetChange(true);
        connectContext.getSessionVariable().setWindowPartitionMode(2);
        try {
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            List<AnalyticEvalNode> analyticNodes = new ArrayList<>();
            execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, analyticNodes);
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertFalse(analyticNodes.isEmpty(),
                    "flag-on PK table must produce a window/analytic node:\n" + plan);
            // window_partition_mode = 2 makes the net-change window hash-based, matching an equivalent
            // no-hint SQL window: the analytic groups by a hash table, so no SORT is enforced below it.
            assertTrue(plan.contains("useHashBasedPartition"),
                    "net-change window must be hash-based at window_partition_mode = 2:\n" + plan);
            assertFalse(plan.contains("SORT"),
                    "hash-based net-change window must not enforce a SORT:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setEnableCdcNetChange(oldFlag);
            connectContext.getSessionVariable().setWindowPartitionMode(oldMode);
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testNoNetChangeWhenFlagOff() throws Exception {
        String name = "pk_nc_off_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createPkTable(name);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("nc_off_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("nc_off_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                name, base.getBookmarkId(), head.getBookmarkId());
        try {
            assertFalse(connectContext.getSessionVariable().isEnableCdcNetChange());
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            List<AnalyticEvalNode> analyticNodes = new ArrayList<>();
            execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, analyticNodes);
            assertTrue(analyticNodes.isEmpty(), "flag-off must not add a window node");
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testNoNetChangeOnDupEvenWhenFlagOn() throws Exception {
        String name = "dup_nc_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(name);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("nc_dup_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("nc_dup_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                name, base.getBookmarkId(), head.getBookmarkId());
        boolean old = connectContext.getSessionVariable().isEnableCdcNetChange();
        connectContext.getSessionVariable().setEnableCdcNetChange(true);
        try {
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            List<AnalyticEvalNode> analyticNodes = new ArrayList<>();
            execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, analyticNodes);
            assertTrue(analyticNodes.isEmpty(), "DUP table is a no-op even with flag on");
        } finally {
            connectContext.getSessionVariable().setEnableCdcNetChange(old);
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testApplyNetChangeOptExpressionOverload() throws Exception {
        // The OptExpression overload (the IVM-rule world) stacks Filter over Window over the scan
        // for a primary-key table, honoring window_partition_mode; a non-PK table comes back as a
        // bare scan. Exercises the overload directly, off the SQL/builder path.
        assertFalse(connectContext.getSessionVariable().isEnableCdcNetChange());

        String pkName = "pk_ovl_" + TABLE_COUNTER.getAndIncrement();
        long pkId = createPkTable(pkName);
        OlapTable pk = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(pkId);
        pk.maySetDatabaseId(dbId);
        String dupName = "dup_ovl_" + TABLE_COUNTER.getAndIncrement();
        long dupId = createDupTable(dupName);
        OlapTable dup = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(dupId);
        dup.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder pkB = BookmarkHolder.forEmptyInfo("ovl_pk_base");
        BookmarkHolder pkH = BookmarkHolder.forEmptyInfo("ovl_pk_head");
        // Take base off the empty initial version so the PK range is a genuine multi-version
        // VERSION_CHAIN_DIFF (base at PARTITION_INIT_VERSION reads as FULL_SCAN, which needs no net-change fold).
        bumpVisibleVersion(pk, 2L);
        Bookmark pkBase = bm.create(dbId, pkId, pkB);
        bumpVisibleVersion(pk, 5L);
        Bookmark pkHead = bm.create(dbId, pkId, pkH);
        BookmarkHolder dupB = BookmarkHolder.forEmptyInfo("ovl_dup_base");
        BookmarkHolder dupH = BookmarkHolder.forEmptyInfo("ovl_dup_head");
        Bookmark dupBase = bm.create(dbId, dupId, dupB);
        bumpVisibleVersion(dup, 5L);
        Bookmark dupHead = bm.create(dbId, dupId, dupH);

        try {
            // PK, sort-based (window_partition_mode = 1): Filter -> Window -> ChangesScan.
            ColumnRefFactory f1 = new ColumnRefFactory();
            LogicalChangesScanOperator pkScan = transformBareChangesScan(pkName, pkBase, pkHead, f1);
            OptExpression sortFold = ChangesScanBuilder.applyNetChange(pkScan, f1, 1);
            assertEquals(OperatorType.LOGICAL_FILTER, sortFold.getOp().getOpType());
            OptExpression win = sortFold.inputAt(0);
            assertEquals(OperatorType.LOGICAL_WINDOW, win.getOp().getOpType());
            assertEquals(OperatorType.LOGICAL_CHANGES_SCAN, win.inputAt(0).getOp().getOpType());
            assertFalse(((LogicalWindowOperator) win.getOp()).isUseHashBasedPartition(),
                    "window_partition_mode = 1 must be sort-based");

            // PK, hash-based (window_partition_mode = 2): same shape, hash-based window.
            ColumnRefFactory f2 = new ColumnRefFactory();
            LogicalChangesScanOperator pkScan2 = transformBareChangesScan(pkName, pkBase, pkHead, f2);
            OptExpression hashFold = ChangesScanBuilder.applyNetChange(pkScan2, f2, 2);
            assertTrue(((LogicalWindowOperator) hashFold.inputAt(0).getOp()).isUseHashBasedPartition(),
                    "window_partition_mode = 2 must be hash-based");

            // DUP: no-op, the bare scan comes back with no window/filter stacked.
            ColumnRefFactory f3 = new ColumnRefFactory();
            LogicalChangesScanOperator dupScan = transformBareChangesScan(dupName, dupBase, dupHead, f3);
            OptExpression noop = ChangesScanBuilder.applyNetChange(dupScan, f3, 1);
            assertEquals(OperatorType.LOGICAL_CHANGES_SCAN, noop.getOp().getOpType());
            assertTrue(noop.getInputs().isEmpty(), "non-PK table must not stack any operator");
        } finally {
            bm.releaseReference(dbId, pkId, pkBase.getBookmarkId(), pkB.getHolderId());
            bm.releaseReference(dbId, pkId, pkHead.getBookmarkId(), pkH.getHolderId());
            bm.releaseReference(dbId, dupId, dupBase.getBookmarkId(), dupB.getHolderId());
            bm.releaseReference(dbId, dupId, dupHead.getBookmarkId(), dupH.getHolderId());
        }
    }

    @Test
    public void testNetChangeSkippedForSingleVersionRange() throws Exception {
        String name = "pk_nc_sv_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createPkTable(name);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("nc_sv_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("nc_sv_head");
        // A single version between base and head: headVersion - baseVersion == 1.
        bumpVisibleVersion(table, 4L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                name, base.getBookmarkId(), head.getBookmarkId());
        boolean old = connectContext.getSessionVariable().isEnableCdcNetChange();
        connectContext.getSessionVariable().setEnableCdcNetChange(true);
        try {
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            List<AnalyticEvalNode> analyticNodes = new ArrayList<>();
            execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, analyticNodes);
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(analyticNodes.isEmpty(),
                    "single-version PK range must skip the net-change window:\n" + plan);
            assertTrue(plan.contains("ChangesScanNode"),
                    "plan must still include ChangesScanNode:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setEnableCdcNetChange(old);
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testNetChangeSkippedForSingleVersionPartitionAdded() throws Exception {
        // A partition that appears only at head (PartitionAdded) with a single load has
        // versionRange (PARTITION_INIT_VERSION, +1) -- a single version -- so the gate must
        // skip the window just as it does for a DataChanged single-version range.
        String name = "pk_nc_pa_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createTable("CREATE TABLE " + name + " (k int NOT NULL, dt date NOT NULL, v int) "
                + "PRIMARY KEY(k, dt) PARTITION BY RANGE(dt) ("
                + "PARTITION p1 VALUES LESS THAN ('2024-02-01')) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("nc_pa_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("nc_pa_head");
        // base has only p1; p2 appears and gets a single load (v1 -> v2) before head.
        Bookmark base = bm.create(dbId, tableId, hBase);
        addPartition(tableId, "p2", "2024-03-01");
        setPartitionVersion(table, "p2", 2L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        String sql = String.format("SELECT k, dt, v FROM %s [_CHANGES_%d_%d_]",
                name, base.getBookmarkId(), head.getBookmarkId());
        boolean old = connectContext.getSessionVariable().isEnableCdcNetChange();
        connectContext.getSessionVariable().setEnableCdcNetChange(true);
        try {
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            List<AnalyticEvalNode> analyticNodes = new ArrayList<>();
            execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, analyticNodes);
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertTrue(analyticNodes.isEmpty(),
                    "single-version PartitionAdded range must skip the net-change window:\n" + plan);
            assertTrue(plan.contains("ChangesScanNode"),
                    "plan must still include ChangesScanNode:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setEnableCdcNetChange(old);
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testNetChangeKeptWhenAnyPartitionMultiVersion() throws Exception {
        String name = "pk_nc_mixed_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createPartitionedPkTable(name);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("nc_mx_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("nc_mx_head");
        // base: both partitions at v3.
        setPartitionVersion(table, "p1", 3L);
        setPartitionVersion(table, "p2", 3L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        // head: p1 single version (3->4), p2 multiple versions (3->6) -> mixed range.
        setPartitionVersion(table, "p1", 4L);
        setPartitionVersion(table, "p2", 6L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        String sql = String.format("SELECT k, dt, v FROM %s [_CHANGES_%d_%d_]",
                name, base.getBookmarkId(), head.getBookmarkId());
        boolean old = connectContext.getSessionVariable().isEnableCdcNetChange();
        connectContext.getSessionVariable().setEnableCdcNetChange(true);
        try {
            ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            List<AnalyticEvalNode> analyticNodes = new ArrayList<>();
            execPlan.getTopFragment().getPlanRoot().collect(AnalyticEvalNode.class, analyticNodes);
            String plan = UtFrameUtils.getFragmentPlan(connectContext, sql);
            assertFalse(analyticNodes.isEmpty(),
                    "a multi-version partition anywhere in the range must keep the window:\n" + plan);
        } finally {
            connectContext.getSessionVariable().setEnableCdcNetChange(old);
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testApplyNetChangeSkipsWindowForSingleVersionPk() throws Exception {
        // The IVM-rule overload must also skip the fold for a single-version PK range:
        // applyNetChange returns the bare scan, no Window/Filter stacked.
        assertFalse(connectContext.getSessionVariable().isEnableCdcNetChange());

        String name = "pk_ovl_sv_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createPkTable(name);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("ovl_sv_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("ovl_sv_head");
        bumpVisibleVersion(table, 4L);
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(table, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            ColumnRefFactory f = new ColumnRefFactory();
            LogicalChangesScanOperator scan = transformBareChangesScan(name, base, head, f);
            OptExpression folded = ChangesScanBuilder.applyNetChange(scan, f, 1);
            assertEquals(OperatorType.LOGICAL_CHANGES_SCAN, folded.getOp().getOpType());
            assertTrue(folded.getInputs().isEmpty(),
                    "single-version PK range must not stack a window/filter");
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /**
     * Transform {@code SELECT k, v FROM table[_CHANGES_base_head_]} with net change off, returning
     * the bare LogicalChangesScanOperator (full column refs, no fold) for direct overload testing.
     */
    private LogicalChangesScanOperator transformBareChangesScan(
            String table, Bookmark base, Bookmark head, ColumnRefFactory factory) throws Exception {
        String sql = String.format("SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                table, base.getBookmarkId(), head.getBookmarkId());
        QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        LogicalPlan plan = new RelationTransformer(factory, connectContext)
                .transformWithSelectLimit(stmt.getQueryRelation());
        return findChangesScan(plan.getRoot());
    }

    /** Capture {@code live}'s current partition state in the map shape a Bookmark holds. */
    private static Map<Long, Map<Long, PhysicalPartitionMeta>> liveSnapshot(OlapTable live) {
        Map<Long, Map<Long, PhysicalPartitionMeta>> parts = new HashMap<>();
        for (Partition p : live.getPartitions()) {
            Map<Long, PhysicalPartitionMeta> inner = new HashMap<>();
            for (PhysicalPartition pp : p.getSubPartitions()) {
                MaterializedIndex idx = pp.getLatestBaseIndex();
                inner.put(pp.getId(), new PhysicalPartitionMeta(
                        idx.getId(), idx.getMetaId(),
                        pp.getVisibleVersion(), pp.getVisibleVersionTime()));
            }
            parts.put(p.getId(), inner);
        }
        return parts;
    }

    /**
     * Shift {@code baseMaterializedIndexMetaId} (when {@code shiftMetaId} is true)
     * or {@code baseMaterializedIndexId} (otherwise) on the first physical partition
     * in {@code parts}. Returns the physical-partition id that was mutated.
     */
    private static long shiftFirstPhysical(Map<Long, Map<Long, PhysicalPartitionMeta>> parts,
                                           boolean shiftMetaId) {
        for (Map<Long, PhysicalPartitionMeta> inner : parts.values()) {
            for (Map.Entry<Long, PhysicalPartitionMeta> e : inner.entrySet()) {
                PhysicalPartitionMeta m = e.getValue();
                long indexId = m.getBaseMaterializedIndexId();
                long metaId = m.getBaseMaterializedIndexMetaId();
                if (shiftMetaId) {
                    metaId += 1;
                } else {
                    indexId += 1;
                }
                e.setValue(new PhysicalPartitionMeta(indexId, metaId,
                        m.getVisibleVersion(), m.getVisibleVersionTimeMs()));
                return e.getKey();
            }
        }
        throw new IllegalStateException("no physical partitions to shift");
    }

    private static Bookmark synthesizeAndRegister(long tableId,
                                                  Map<Long, Map<Long, PhysicalPartitionMeta>> parts) {
        long bookmarkId = GlobalStateMgr.getCurrentState().getNextId();
        long bookmarkTimeMs = System.currentTimeMillis();
        Bookmark b = new Bookmark(dbId, tableId, bookmarkId, bookmarkTimeMs, parts);
        GlobalStateMgr.getCurrentState().getBookmarkManager()
                .replay(BookmarkLogEntry.AddBookmark.of(
                        b, BookmarkHolder.forEmptyInfo("synthetic"), bookmarkTimeMs, -1L));
        return b;
    }

    private static LogicalChangesScanOperator findChangesScan(OptExpression root) {
        if (root.getOp().getOpType() == OperatorType.LOGICAL_CHANGES_SCAN) {
            return (LogicalChangesScanOperator) root.getOp();
        }
        for (OptExpression child : root.getInputs()) {
            LogicalChangesScanOperator found = findChangesScan(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** Create a single-partition DUP cloud-native table and return its id. */
    private long createDupTable(String name) throws Exception {
        String ddl = "CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');";
        return createTable(ddl);
    }

    /** Create a single-partition PK cloud-native table and return its id. */
    private long createPkTable(String name) throws Exception {
        String ddl = "CREATE TABLE " + name + " (k int, v int) "
                + "PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');";
        return createTable(ddl);
    }

    /** Create a two-range-partition PK cloud-native table (p1, p2) and return its id. */
    private long createPartitionedPkTable(String name) throws Exception {
        String ddl = "CREATE TABLE " + name + " (k int NOT NULL, dt date NOT NULL, v int) "
                + "PRIMARY KEY(k, dt) PARTITION BY RANGE(dt) ("
                + "PARTITION p1 VALUES LESS THAN ('2024-02-01'), "
                + "PARTITION p2 VALUES LESS THAN ('2024-03-01')) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');";
        return createTable(ddl);
    }

    /**
     * Create a two-range-partition (p1, p2) DUP cloud-native table HASH-distributed over 3 buckets
     * and return its id. A HASH distribution keeps getBucketNums off the range-colocate alignment path.
     */
    private long createHashRangeTable(String name) throws Exception {
        String ddl = "CREATE TABLE " + name + " (k int NOT NULL, dt date NOT NULL, v int) "
                + "DUPLICATE KEY(k, dt) PARTITION BY RANGE(dt) ("
                + "PARTITION p1 VALUES LESS THAN ('2024-02-01'), "
                + "PARTITION p2 VALUES LESS THAN ('2024-03-01')) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ('replication_num' = '1');";
        return createTable(ddl);
    }

    /** Set one named partition's physical-partition visibleVersion. */
    private static void setPartitionVersion(OlapTable t, String partitionName, long version) {
        Partition p = t.getPartition(partitionName);
        for (PhysicalPartition pp : p.getSubPartitions()) {
            pp.setVisibleVersion(version, System.currentTimeMillis());
        }
    }

    /**
     * Bump every physical partition's visibleVersion to {@code newVersion} so the
     * next BookmarkManager.create() sees a state change and mints a fresh
     * bookmark.
     */
    private static void bumpVisibleVersion(OlapTable t, long newVersion) {
        for (Partition p : t.getPartitions()) {
            for (PhysicalPartition pp : p.getSubPartitions()) {
                pp.setVisibleVersion(newVersion, System.currentTimeMillis());
            }
        }
    }
}
