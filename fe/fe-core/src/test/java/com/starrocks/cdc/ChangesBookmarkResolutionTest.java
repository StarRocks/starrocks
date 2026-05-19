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

package com.starrocks.cdc;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkRange;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.lake.bookmark.PhysicalPartitionMeta;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.transformer.CdcScanHelper;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Planner-level test for the bookmark-resolution side of the _CHANGES_ hint:
 * shadow-table scoping in the transformer, non-trackable-delta messaging, and
 * the {@link CdcScanHelper} entry point that production callers reuse.
 *
 * <p>Bookmarks are minted by calling {@link BookmarkManager} directly; INSERTs
 * are not available in the FE UT framework, so consecutive create() calls only
 * return distinct bookmarks after bumping physical-partition visibleVersion.
 */
public class ChangesBookmarkResolutionTest extends BookmarkTestBase {

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
            // The transformer must hand the operator the scoped (shadow) table,
            // not the live catalog instance.
            assertNotSame(live, scoped);

            // The scoped table's partitions must equal the trackable logical
            // partitions in the delta. For a freshly-created table with a
            // DATA_CHANGED bump this is the full single partition.
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
    public void testDroppedNotTrackableMessage() throws Exception {
        String tableName = "dup_drop_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);

        long logicalId = live.getPartitions().iterator().next().getId();
        String partitionName = live.getPartition(logicalId).getName();
        long physicalId = live.getPartition(logicalId).getSubPartitions().iterator().next().getId();

        BookmarkChange diff = buildSingletonDiff(
                logicalId,
                new BookmarkChange.PartitionDropped(logicalId, physicalId,
                        new PhysicalPartitionMeta(1L, 1L, 1L, 0L)));

        String msg = CdcScanHelper.formatNotTrackableMessage(diff, live);
        String expected = "CHANGES not trackable: physical partition '" + partitionName
                + "' has been dropped or truncated between base and head";
        assertTrue(msg.contains(expected),
                "expected message to contain '" + expected + "', got: " + msg);
    }

    @Test
    public void testIndexReplacedNotTrackableMessage() throws Exception {
        String tableName = "dup_idx_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);

        long logicalId = live.getPartitions().iterator().next().getId();
        String partitionName = live.getPartition(logicalId).getName();
        long physicalId = live.getPartition(logicalId).getSubPartitions().iterator().next().getId();

        BookmarkChange diff = buildSingletonDiff(
                logicalId,
                new BookmarkChange.IndexReplaced(logicalId, physicalId,
                        new PhysicalPartitionMeta(1L, 1L, 1L, 0L),
                        new PhysicalPartitionMeta(2L, 2L, 2L, 0L)));

        String msg = CdcScanHelper.formatNotTrackableMessage(diff, live);
        String expected = "CHANGES not trackable: physical partition '" + partitionName
                + "' has been modified in a way that rewrote its data";
        assertTrue(msg.contains(expected),
                "expected message to contain '" + expected + "', got: " + msg);
    }

    @Test
    public void testTabletReshardNotTrackableMessage() throws Exception {
        String tableName = "dup_resh_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);

        long logicalId = live.getPartitions().iterator().next().getId();
        String partitionName = live.getPartition(logicalId).getName();
        long physicalId = live.getPartition(logicalId).getSubPartitions().iterator().next().getId();

        BookmarkChange diff = buildSingletonDiff(
                logicalId,
                new BookmarkChange.TabletReshard(logicalId, physicalId,
                        new PhysicalPartitionMeta(1L, 1L, 1L, 0L),
                        new PhysicalPartitionMeta(2L, 1L, 1L, 0L)));

        String msg = CdcScanHelper.formatNotTrackableMessage(diff, live);
        String expected = "CHANGES not trackable: physical partition '" + partitionName
                + "' has been redistributed";
        assertTrue(msg.contains(expected),
                "expected message to contain '" + expected + "', got: " + msg);
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
            LogicalChangesScanOperator op = CdcScanHelper.build(
                    table,
                    new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                    new HashMap<>(),
                    new HashMap<>());
            assertNotNull(op);
            assertEquals(base.getBookmarkId(), op.getBase().getBookmarkId());
            assertEquals(head.getBookmarkId(), op.getHead().getBookmarkId());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testBuildFromBookmarkRangeBaseNotFound() throws Exception {
        String tableName = "dup_brnf_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        table.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("range_nf_head");
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            SemanticException ex = assertThrows(SemanticException.class,
                    () -> CdcScanHelper.build(
                            table,
                            new BookmarkRange(99999L, head.getBookmarkId()),
                            new HashMap<>(),
                            new HashMap<>()));
            assertTrue(ex.getMessage().contains("bookmark 99999 not found"),
                    "actual: " + ex.getMessage());
        } finally {
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    @Test
    public void testHintEndToEndPlan() throws Exception {
        String tableName = "dup_hint_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        live.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder hBase = BookmarkHolder.forEmptyInfo("hint_base");
        BookmarkHolder hHead = BookmarkHolder.forEmptyInfo("hint_head");
        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(live, 6L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            // Driven through the parser+analyzer+transformer path so the hint
            // ends up resolving to a BookmarkRange on the TableRelation, which
            // the new RelationTransformer branch dispatches into CdcScanHelper.
            // Skip the cost-based optimizer — the transformer is the load-bearing
            // assertion here, and StatisticsCalculator has no LogicalChangesScan
            // handler yet.
            String sql = String.format(
                    "SELECT k, v FROM %s [_CHANGES_%d_%d_]",
                    tableName, base.getBookmarkId(), head.getBookmarkId());
            QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    sql, connectContext);
            LogicalPlan plan = new RelationTransformer(new ColumnRefFactory(), connectContext)
                    .transformWithSelectLimit(stmt.getQueryRelation());
            assertTrue(containsChangesScan(plan.getRoot()),
                    "expected LogicalChangesScan in transformed plan, got root op: "
                            + plan.getRoot().getOp().getOpType());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase.getHolderId());
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead.getHolderId());
        }
    }

    /** Wrap a single PhysicalPartitionChange in the per-logical-partition map a BookmarkChange expects. */
    private static BookmarkChange buildSingletonDiff(long logicalId,
                                                     BookmarkChange.PhysicalPartitionChange change) {
        Map<Long, List<BookmarkChange.PhysicalPartitionChange>> changes = new HashMap<>();
        List<BookmarkChange.PhysicalPartitionChange> row = new ArrayList<>();
        row.add(change);
        changes.put(logicalId, row);
        return new BookmarkChange(Collections.unmodifiableMap(changes));
    }

    /** Recursively scan an OptExpression tree for a LogicalChangesScan node. */
    private static boolean containsChangesScan(OptExpression root) {
        if (root.getOp().getOpType() == OperatorType.LOGICAL_CHANGES_SCAN) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (containsChangesScan(child)) {
                return true;
            }
        }
        return false;
    }

    /** Return the first LogicalChangesScan operator found in the tree, or null. */
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
