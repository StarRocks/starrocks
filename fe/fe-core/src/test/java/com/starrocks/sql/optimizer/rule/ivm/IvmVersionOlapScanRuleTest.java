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

package com.starrocks.sql.optimizer.rule.ivm;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link IvmVersionOlapScanRule}. Uses {@link BookmarkTestBase}'s
 * mini-cluster harness for real bookmark resolution end-to-end.
 */
public class IvmVersionOlapScanRuleTest extends BookmarkTestBase {

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

    @Test
    public void testTransform_fromVersion_resolvesBaseBookmark() throws Exception {
        Setup setup = setupDupBookmarks();
        OptExpression input = buildVersionInput(setup, LogicalVersionOperator.fromVersion());

        List<OptExpression> result = new IvmVersionOlapScanRule().transform(input, setup.context());

        assertEquals(1, result.size());
        assertInstanceOf(LogicalOlapScanOperator.class, result.get(0).getOp());
        LogicalOlapScanOperator newScan = (LogicalOlapScanOperator) result.get(0).getOp();
        assertNotSame(setup.live, newScan.getTable());
        assertTrue(newScan.getTvrVersionRange().isEmpty(),
                "consumed version state should be cleared to the empty sentinel, not null");
    }

    @Test
    public void testTransform_toVersion_resolvesHeadBookmark() throws Exception {
        Setup setup = setupDupBookmarks();
        OptExpression input = buildVersionInput(setup, LogicalVersionOperator.toVersion());

        List<OptExpression> result = new IvmVersionOlapScanRule().transform(input, setup.context());

        assertEquals(1, result.size());
        LogicalOlapScanOperator newScan = (LogicalOlapScanOperator) result.get(0).getOp();
        assertNotSame(setup.live, newScan.getTable());
        assertTrue(newScan.getTvrVersionRange().isEmpty(),
                "consumed version state should be cleared to the empty sentinel, not null");
    }

    @Test
    public void testTransform_minEndpoint_returnsEmptyValues() throws Exception {
        // Endpoint MIN — trial rewrite sentinel or first refresh's from side.
        // Must emit a real empty relation; scan-with-empty-TVR still reads
        // live rows at plan time (PlanFragmentBuilder ignores the TVR).
        Setup setup = setupDupBookmarks();
        Map<ColumnRefOperator, Column> colRefMap = buildColRefMap(setup.live, setup.factory());
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .setTable(setup.live)
                .setColRefToColumnMetaMap(colRefMap)
                .setColumnMetaToColRefMap(invert(colRefMap))
                .setTableVersionRange(TvrTableDelta.of(TvrVersion.MIN, TvrVersion.MIN))
                .build();
        OptExpression input = OptExpression.create(LogicalVersionOperator.fromVersion(),
                OptExpression.create(scan));

        List<OptExpression> result = new IvmVersionOlapScanRule().transform(input, setup.context());

        assertEquals(1, result.size());
        assertInstanceOf(LogicalValuesOperator.class, result.get(0).getOp());
        LogicalValuesOperator values = (LogicalValuesOperator) result.get(0).getOp();
        assertTrue(values.getRows().isEmpty(),
                "MIN endpoint must produce empty rows, not a live scan");
        // Preserve scan's output col-refs so downstream join sees the same refs.
        for (ColumnRefOperator col : scan.getOutputColumns()) {
            assertTrue(values.getColumnRefSet().contains(col));
        }
    }

    @Test
    public void testTransform_realBookmarkNoOpDelta_stillResolves() throws Exception {
        // IVM join refresh: this side has no changes (base == head, but real
        // bookmark ids), so the Version branch still needs resolveById to scope
        // the table. Regression guard for the codex review on #55743.
        Setup setup = setupDupBookmarks();
        Map<ColumnRefOperator, Column> colRefMap = buildColRefMap(setup.live, setup.factory());
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .setTable(setup.live)
                .setColRefToColumnMetaMap(colRefMap)
                .setColumnMetaToColRefMap(invert(colRefMap))
                .setTableVersionRange(TvrTableDelta.of(
                        setup.base.getBookmarkId(), setup.base.getBookmarkId()))
                .build();
        OptExpression input = OptExpression.create(LogicalVersionOperator.fromVersion(),
                OptExpression.create(scan));

        List<OptExpression> result = new IvmVersionOlapScanRule().transform(input, setup.context());

        assertEquals(1, result.size());
        LogicalOlapScanOperator out = (LogicalOlapScanOperator) result.get(0).getOp();
        // resolveById must have been called: scoped table is a different
        // instance from the live table (mirrors fromVersion/toVersion tests).
        assertNotSame(setup.live, out.getTable());
        assertTrue(out.getTvrVersionRange().isEmpty(),
                "consumed version state should be cleared to the empty sentinel");
    }

    @Test
    public void testCheck_nonCloudNativeTable_returnsFalse() throws Exception {
        Setup setup = setupDupBookmarks();
        OlapTable nativeSpy = new OlapTable() {
            @Override
            public boolean isCloudNativeTableOrMaterializedView() {
                return false;
            }
            @Override
            public long getId() {
                return setup.tableId;
            }
            @Override
            public List<Column> getBaseSchema() {
                return setup.live.getBaseSchema();
            }
        };
        Map<ColumnRefOperator, Column> colRefMap = buildColRefMap(nativeSpy, setup.factory());
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .setTable(nativeSpy)
                .setColRefToColumnMetaMap(colRefMap)
                .setColumnMetaToColRefMap(invert(colRefMap))
                .setTableVersionRange(TvrTableDelta.of(
                        setup.base.getBookmarkId(), setup.head.getBookmarkId()))
                .build();
        OptExpression input = OptExpression.create(LogicalVersionOperator.fromVersion(),
                OptExpression.create(scan));

        assertFalse(new IvmVersionOlapScanRule().check(input, setup.context()));
    }

    @Test
    public void testCheck_missingTrait_returnsFalse() throws Exception {
        Setup setup = setupDupBookmarks();
        Map<ColumnRefOperator, Column> colRefMap = buildColRefMap(setup.live, setup.factory());
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .setTable(setup.live)
                .setColRefToColumnMetaMap(colRefMap)
                .setColumnMetaToColRefMap(invert(colRefMap))
                .setTableVersionRange(TvrTableSnapshot.empty())
                .build();
        OptExpression input = OptExpression.create(LogicalVersionOperator.fromVersion(),
                OptExpression.create(scan));

        assertFalse(new IvmVersionOlapScanRule().check(input, setup.context()));
    }

    // -- helpers --

    private static final class Setup {
        final OlapTable live;
        final long tableId;
        final Bookmark base;
        final Bookmark head;
        final ColumnRefFactory factoryRef = new ColumnRefFactory();
        Setup(OlapTable live, long tableId, Bookmark base, Bookmark head) {
            this.live = live;
            this.tableId = tableId;
            this.base = base;
            this.head = head;
        }
        ColumnRefFactory factory() {
            return factoryRef;
        }
        OptimizerContext context() {
            return OptimizerFactory.mockContext(factoryRef);
        }
    }

    private Setup setupDupBookmarks() throws Exception {
        String name = "dup_v_" + TABLE_COUNTER.getAndIncrement();
        String ddl = "CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');";
        long tableId = createTable(ddl);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        live.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        Bookmark base = bm.create(dbId, tableId, BookmarkHolder.forEmptyInfo("ivm_v_base"));
        bumpVisibleVersion(live, 7L);
        Bookmark head = bm.create(dbId, tableId, BookmarkHolder.forEmptyInfo("ivm_v_head"));
        return new Setup(live, tableId, base, head);
    }

    private static OptExpression buildVersionInput(Setup setup, LogicalVersionOperator versionOp) {
        Map<ColumnRefOperator, Column> colRefMap = buildColRefMap(setup.live, setup.factory());
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .setTable(setup.live)
                .setColRefToColumnMetaMap(colRefMap)
                .setColumnMetaToColRefMap(invert(colRefMap))
                .setTableVersionRange(TvrTableDelta.of(
                        setup.base.getBookmarkId(), setup.head.getBookmarkId()))
                .build();
        return OptExpression.create(versionOp, OptExpression.create(scan));
    }

    private static Map<ColumnRefOperator, Column> buildColRefMap(OlapTable table, ColumnRefFactory factory) {
        Map<ColumnRefOperator, Column> map = new HashMap<>();
        for (Column c : table.getBaseSchema()) {
            map.put(factory.create(c.getName(), c.getType(), c.isAllowNull()), c);
        }
        return map;
    }

    private static Map<Column, ColumnRefOperator> invert(Map<ColumnRefOperator, Column> map) {
        Map<Column, ColumnRefOperator> inv = new HashMap<>();
        map.forEach((ref, col) -> inv.put(col, ref));
        return inv;
    }

    private static void bumpVisibleVersion(OlapTable t, long newVersion) {
        for (Partition p : t.getPartitions()) {
            for (PhysicalPartition pp : p.getSubPartitions()) {
                pp.setVisibleVersion(newVersion, System.currentTimeMillis());
            }
        }
    }
}
