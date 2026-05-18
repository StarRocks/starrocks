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
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.lake.bookmark.PhysicalPartitionMeta;
import com.starrocks.lake.bookmark.ReferenceHolder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
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
 * Analyzer + transformer resolution paths exercised through the bookmark API:
 * not-found bookmark id, base later than head, and the trackable-delta happy
 * path that emits a LogicalChangesScan from the transformer.
 *
 * <p>Bookmarks are minted by calling BookmarkManager directly; INSERTs are not
 * available in the FE UT framework, so we bump physical-partition
 * visibleVersion directly to make consecutive create() calls return distinct
 * bookmarks.
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
    public void testBookmarkNotFound() throws Exception {
        String tableName = "dup_nf_" + TABLE_COUNTER.getAndIncrement();
        createDupTable(tableName);
        String sql = "SELECT k, v FROM " + tableName
                + " CHANGES FROM VERSION 99999 TO VERSION 99998";
        Exception ex = assertThrows(Exception.class,
                () -> UtFrameUtils.getFragmentPlan(connectContext, sql));
        assertTrue(ex.getMessage().contains("not found"),
                "expected not-found message, got: " + ex.getMessage());
    }

    @Test
    public void testBookmarkIdNotFoundMessageMentionsTableName() throws Exception {
        String tableName = "dup_nfmsg_" + TABLE_COUNTER.getAndIncrement();
        createDupTable(tableName);
        long unknownId = 999_999L;
        String sql = String.format(
                "SELECT k, v FROM %s CHANGES FROM VERSION %d TO VERSION %d",
                tableName, unknownId, unknownId + 1);
        Exception ex = assertThrows(Exception.class,
                () -> UtFrameUtils.getFragmentPlan(connectContext, sql));
        String expected = "bookmark " + unknownId + " not found on table '" + tableName + "'";
        assertTrue(ex.getMessage().contains(expected),
                "expected message to contain '" + expected + "', got: " + ex.getMessage());
    }

    @Test
    public void testNoActiveBookmarkAtOrBeforeTsMessage() throws Exception {
        String tableName = "dup_nots_" + TABLE_COUNTER.getAndIncrement();
        createDupTable(tableName);
        String sql = "SELECT k, v FROM " + tableName
                + " CHANGES FROM TIMESTAMP '2020-01-01 00:00:00' TO TIMESTAMP '2099-01-01 00:00:00'";
        Exception ex = assertThrows(Exception.class,
                () -> UtFrameUtils.getFragmentPlan(connectContext, sql));
        String expected = "no bookmark for table '" + tableName + "' at or before";
        assertTrue(ex.getMessage().contains(expected),
                "expected message to contain '" + expected + "', got: " + ex.getMessage());
    }

    @Test
    public void testBaseLaterThanHeadMessage() throws Exception {
        String tableName = "dup_blth_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable t = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        t.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        ReferenceHolder h1 = new ReferenceHolder.Custom("base_later_than_head_1");
        ReferenceHolder h2 = new ReferenceHolder.Custom("base_later_than_head_2");
        Bookmark b1 = bm.create(dbId, tableId, h1);
        bumpVisibleVersion(t, 3L);
        Bookmark b2 = bm.create(dbId, tableId, h2);

        try {
            // base = b2 (larger id), head = b1 (smaller id) — must be rejected with the spec wording.
            String sql = String.format(
                    "SELECT k, v FROM %s CHANGES FROM VERSION %d TO VERSION %d",
                    tableName, b2.getBookmarkId(), b1.getBookmarkId());
            Exception ex = assertThrows(Exception.class,
                    () -> UtFrameUtils.getFragmentPlan(connectContext, sql));
            assertTrue(ex.getMessage().contains("CHANGES base must not be later than head"),
                    "expected spec wording 'CHANGES base must not be later than head', got: " + ex.getMessage());
        } finally {
            bm.releaseReference(dbId, tableId, b1.getBookmarkId(), h1);
            bm.releaseReference(dbId, tableId, b2.getBookmarkId(), h2);
        }
    }

    @Test
    public void testBaseAfterHead() throws Exception {
        String tableName = "dup_order_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable t = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        // LocalMetastore.createTable doesn't stamp dbId; production callers
        // populate it before BookmarkManager runs, so seed it here.
        t.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        ReferenceHolder h1 = new ReferenceHolder.Custom("base_after_head_1");
        ReferenceHolder h2 = new ReferenceHolder.Custom("base_after_head_2");
        Bookmark b1 = bm.create(dbId, tableId, h1);
        // Bump visible version so the next create() observes a state change and
        // returns a distinct bookmark id. PARTITION_INIT_VERSION is 1, so any
        // value > 1 works.
        bumpVisibleVersion(t, 2L);
        Bookmark b2 = bm.create(dbId, tableId, h2);

        try {
            // base = b2 (larger id), head = b1 (smaller id) — must be rejected.
            String sql = String.format(
                    "SELECT k, v FROM %s CHANGES FROM VERSION %d TO VERSION %d",
                    tableName, b2.getBookmarkId(), b1.getBookmarkId());
            Exception ex = assertThrows(Exception.class,
                    () -> UtFrameUtils.getFragmentPlan(connectContext, sql));
            assertTrue(ex.getMessage().contains("CHANGES base must not be later than head"),
                    "expected order-check message, got: " + ex.getMessage());
        } finally {
            bm.releaseReference(dbId, tableId, b1.getBookmarkId(), h1);
            bm.releaseReference(dbId, tableId, b2.getBookmarkId(), h2);
        }
    }

    @Test
    public void testTrackableDeltaTransformsToChangesScan() throws Exception {
        String tableName = "dup_trk_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable t = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        t.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        ReferenceHolder hBase = new ReferenceHolder.Custom("trackable_base");
        ReferenceHolder hHead = new ReferenceHolder.Custom("trackable_head");

        Bookmark base = bm.create(dbId, tableId, hBase);
        // Advance visibleVersion so the head bookmark differs from base; the
        // resulting BookmarkChange has DATA_CHANGED, which is trackable.
        bumpVisibleVersion(t, 5L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            // Run only parser + analyzer + transformer; skip the cost-based
            // optimizer, whose StatisticsCalculator has no handler for
            // LogicalChangesScan yet. The transformer is where bookmark
            // resolution lives, so a successful transform that emits a
            // LogicalChangesScan is the load-bearing assertion.
            String sql = String.format(
                    "SELECT k, v FROM %s CHANGES FROM VERSION %d TO VERSION %d",
                    tableName, base.getBookmarkId(), head.getBookmarkId());
            QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    sql, connectContext);
            LogicalPlan plan = new RelationTransformer(new ColumnRefFactory(), connectContext)
                    .transformWithSelectLimit(stmt.getQueryRelation());
            assertTrue(containsChangesScan(plan.getRoot()),
                    "expected LogicalChangesScan in transformed plan, got root op: "
                            + plan.getRoot().getOp().getOpType());
        } finally {
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase);
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead);
        }
    }

    @Test
    public void testRelationTransformerUsesScopedTableForChanges() throws Exception {
        String tableName = "dup_scoped_" + TABLE_COUNTER.getAndIncrement();
        long tableId = createDupTable(tableName);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        live.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        ReferenceHolder hBase = new ReferenceHolder.Custom("scoped_base");
        ReferenceHolder hHead = new ReferenceHolder.Custom("scoped_head");

        Bookmark base = bm.create(dbId, tableId, hBase);
        bumpVisibleVersion(live, 7L);
        Bookmark head = bm.create(dbId, tableId, hHead);

        try {
            String sql = String.format(
                    "SELECT k, v FROM %s CHANGES FROM VERSION %d TO VERSION %d",
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
            bm.releaseReference(dbId, tableId, base.getBookmarkId(), hBase);
            bm.releaseReference(dbId, tableId, head.getBookmarkId(), hHead);
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

        String msg = CDCPlanHelper.buildNonTrackableMessage(diff, live);
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

        String msg = CDCPlanHelper.buildNonTrackableMessage(diff, live);
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

        String msg = CDCPlanHelper.buildNonTrackableMessage(diff, live);
        String expected = "CHANGES not trackable: physical partition '" + partitionName
                + "' has been redistributed";
        assertTrue(msg.contains(expected),
                "expected message to contain '" + expected + "', got: " + msg);
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
