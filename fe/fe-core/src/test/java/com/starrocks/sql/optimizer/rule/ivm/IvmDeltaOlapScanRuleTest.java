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
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link IvmDeltaOlapScanRule}. Builds real cloud-native bookmarks via the
 * {@link BookmarkTestBase} mini-cluster harness so {@code ChangesScanBuilder}
 * can resolve them end-to-end; isolating the rule from bookmark resolution
 * would force JMockit / Mockito static stubs and lose more than it'd gain.
 */
public class IvmDeltaOlapScanRuleTest extends BookmarkTestBase {

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

    @Test
    public void testCheck_nonCloudNativeTable_returnsFalse() throws Exception {
        // shared-nothing OlapTable doesn't report cloud-native; rule must skip.
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
        OptExpression input = buildDeltaInput(nativeSpy, setup.base, setup.head, setup.factory());

        assertFalse(new IvmDeltaOlapScanRule().check(input, setup.context()));
    }

    @Test
    public void testCheck_missingTrait_returnsFalse() throws Exception {
        Setup setup = setupDupBookmarks();
        ColumnRefFactory factory = setup.factory();

        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .setTable(setup.live)
                .setColRefToColumnMetaMap(buildColRefMap(setup.live, factory))
                .setColumnMetaToColRefMap(new HashMap<>())
                .setTableVersionRange(TvrTableSnapshot.empty())
                .build();
        ColumnRefOperator action = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME,
                IvmRuleUtils.ACTION_COLUMN_TYPE, false);
        OptExpression input = OptExpression.create(new LogicalDeltaOperator(true, action),
                OptExpression.create(scan));

        assertFalse(new IvmDeltaOlapScanRule().check(input, setup.context()));
    }

    @Test
    public void testTransform_nonEmptyDelta_emitsProjectOverChangesScan() throws Exception {
        Setup setup = setupDupBookmarks();
        OptExpression input = buildDeltaInput(setup.live, setup.base, setup.head, setup.factory());

        List<OptExpression> result = new IvmDeltaOlapScanRule().transform(input, setup.context());

        assertEquals(1, result.size());
        assertInstanceOf(LogicalProjectOperator.class, result.get(0).getOp());
        LogicalProjectOperator project = (LogicalProjectOperator) result.get(0).getOp();
        assertInstanceOf(LogicalChangesScanOperator.class, result.get(0).inputAt(0).getOp());

        ColumnRefOperator action = ((LogicalDeltaOperator) input.getOp()).getActionColumn();
        ScalarOperator actionExpr = project.getColumnRefMap().get(action);
        assertInstanceOf(ColumnRefOperator.class, actionExpr);
        assertEquals(IntegerType.TINYINT, ((ColumnRefOperator) actionExpr).getType());

        boolean projectHasRowVersion = project.getColumnRefMap().keySet().stream()
                .anyMatch(c -> c.getName().equals("__ROW_VERSION__"));
        assertFalse(projectHasRowVersion, "__ROW_VERSION__ must be dropped at the project");
    }

    @Test
    public void testTransform_emptyDelta_emitsValuesOperator() throws Exception {
        Setup setup = setupDupBookmarks();
        ColumnRefFactory factory = setup.factory();
        Map<ColumnRefOperator, Column> colRefMap = buildColRefMap(setup.live, factory);
        ColumnRefOperator action = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME,
                IvmRuleUtils.ACTION_COLUMN_TYPE, false);

        // from == to → empty delta.
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .setTable(setup.live)
                .setColRefToColumnMetaMap(colRefMap)
                .setColumnMetaToColRefMap(invert(colRefMap))
                .setTableVersionRange(TvrTableDelta.of(
                        setup.base.getBookmarkId(), setup.base.getBookmarkId()))
                .build();
        OptExpression input = OptExpression.create(new LogicalDeltaOperator(true, action),
                OptExpression.create(scan));

        List<OptExpression> result = new IvmDeltaOlapScanRule().transform(input, setup.context());

        assertEquals(1, result.size());
        assertInstanceOf(LogicalValuesOperator.class, result.get(0).getOp());
        LogicalValuesOperator values = (LogicalValuesOperator) result.get(0).getOp();
        assertTrue(values.getRows().isEmpty(), "no rows for empty delta");
        assertTrue(values.getColumnRefSet().contains(action),
                "Values output must keep the action col-ref so downstream projects don't break");
        for (ColumnRefOperator businessCol : colRefMap.keySet()) {
            assertTrue(values.getColumnRefSet().contains(businessCol));
        }
    }

    // -- helpers --

    /** Bundle of cross-cutting state per test: live table + two bookmarks +
     *  per-test factory/context (cleared between tests so col-ref ids don't leak). */
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
        String name = "dup_" + TABLE_COUNTER.getAndIncrement();
        String ddl = "CREATE TABLE " + name + " (k int, v int) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1');";
        long tableId = createTable(ddl);
        OlapTable live = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        live.maySetDatabaseId(dbId);

        BookmarkManager bm = GlobalStateMgr.getCurrentState().getBookmarkManager();
        Bookmark base = bm.create(dbId, tableId, BookmarkHolder.forEmptyInfo("ivm_base"));
        bumpVisibleVersion(live, 7L);
        Bookmark head = bm.create(dbId, tableId, BookmarkHolder.forEmptyInfo("ivm_head"));
        return new Setup(live, tableId, base, head);
    }

    private static OptExpression buildDeltaInput(OlapTable table, Bookmark base, Bookmark head,
                                                 ColumnRefFactory factory) {
        Map<ColumnRefOperator, Column> colRefMap = buildColRefMap(table, factory);
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .setTable(table)
                .setColRefToColumnMetaMap(colRefMap)
                .setColumnMetaToColRefMap(invert(colRefMap))
                .setTableVersionRange(TvrTableDelta.of(base.getBookmarkId(), head.getBookmarkId()))
                .build();
        ColumnRefOperator action = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME,
                IvmRuleUtils.ACTION_COLUMN_TYPE, false);
        return OptExpression.create(new LogicalDeltaOperator(true, action),
                OptExpression.create(scan));
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
