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

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkLogEntry;
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
 * scoped-table substitution in the transformer, non-trackable-delta messaging,
 * and the CdcScanHelper.build entry point that production callers reuse.
 *
 * <p>Bookmarks are minted by calling BookmarkManager directly; INSERTs are
 * not available in the FE UT framework, so consecutive create() calls only
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
                () -> CdcScanHelper.build(
                        live,
                        new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                        new HashMap<>(),
                        new HashMap<>()));
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
                () -> CdcScanHelper.build(
                        live,
                        new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                        new HashMap<>(),
                        new HashMap<>()));
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
                () -> CdcScanHelper.build(
                        live,
                        new BookmarkRange(base.getBookmarkId(), head.getBookmarkId()),
                        new HashMap<>(),
                        new HashMap<>()));
        String expected = String.format(
                "CHANGES from bookmark %d to %d on table '%s' not trackable: physical partition %d resharded",
                base.getBookmarkId(), head.getBookmarkId(), tableName, shiftedPhysicalId);
        assertTrue(ex.getMessage().contains(expected),
                "expected message to contain '" + expected + "', got: " + ex.getMessage());
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
                        b, BookmarkHolder.forEmptyInfo("synthetic"), bookmarkTimeMs));
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
