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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rewrite.OptDistributionPruner;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Regression tests for the per-index distribution pruning fix:
 * both {@link RangeDistributionPruner} (OlapScanNode path) and
 * {@link OptDistributionPruner} (optimizer-rewrite path) must use the selected
 * index's own sort-key columns instead of the base table's.
 *
 * <p>Bug: both pruners called {@code MetaUtils.getRangeDistributionColumns(olapTable)}
 * (table-level), which returns the BASE index sort key. A rollup with a shorter or
 * reordered key caused either an arity assertion in the {@link RangeDistributionPruner}
 * constructor or incorrect tablet pruning. Fix: use
 * {@code MetaUtils.getRangeDistributionColumns(olapTable, index.getMetaId())}.
 */
public class RangeRollupPrunerTest {

    // ---- helpers -------------------------------------------------------

    private Tablet createTablet(long id, String lower, String upper, Column column) {
        LocalTablet tablet = new LocalTablet(id);
        List<Variant> lowerValues = Lists.newArrayList(Variant.of(column.getType(), lower));
        List<Variant> upperValues = Lists.newArrayList(Variant.of(column.getType(), upper));
        tablet.setRange(new TabletRange(Range.of(
                new Tuple(lowerValues), new Tuple(upperValues), true, true)));
        return tablet;
    }

    // ---- OlapScanNode pruner path tests --------------------------------

    /**
     * When the rollup sort key is a single column (k2) and the base has two (k1, k2),
     * using the rollup's own 1-column list does not crash and prunes correctly.
     */
    @Test
    public void testPrunerUsesRollupOwnColumns_noArityAssertion() {
        Column k2 = new Column("k2", IntegerType.INT, false);

        List<Tablet> tablets = Lists.newArrayList(
                createTablet(1L, "0", "9", k2),
                createTablet(2L, "10", "19", k2),
                createTablet(3L, "20", "29", k2));

        PartitionColumnFilter filter = new PartitionColumnFilter();
        filter.setLowerBound(new StringLiteral("15"), true);
        filter.setUpperBound(new StringLiteral("15"), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("k2", filter);

        // Using only k2 (rollup's sort key), not the base's [k1, k2]
        RangeDistributionPruner pruner =
                new RangeDistributionPruner(tablets, Lists.newArrayList(k2), filters);
        Collection<Long> result = pruner.prune();

        assertEquals(Set.of(2L), new HashSet<>(result));
    }

    /**
     * Documents the pre-fix crash mode: supplying the base's 2-column list against
     * rollup tablets with 1-column ranges raises an arity {@code IllegalStateException}.
     * The fix avoids this by using the rollup's own column list.
     */
    @Test
    public void testPrunerArityMismatchThrows() {
        Column k1 = new Column("k1", IntegerType.INT, false);
        Column k2 = new Column("k2", IntegerType.INT, false);

        assertThrows(IllegalStateException.class, () ->
                new RangeDistributionPruner(
                        Lists.newArrayList(createTablet(1L, "0", "9", k2)),
                        Lists.newArrayList(k1, k2), // base's 2-column list — arity mismatch
                        Maps.newHashMap()));
    }

    // ---- OptDistributionPruner (optimizer-rewrite) path tests ----------

    /**
     * Verifies that {@link OptDistributionPruner} passes the selected index's own
     * {@code indexMetaId} to {@code MetaUtils.getRangeDistributionColumns}.
     *
     * <p>The mock captures which metaId was passed; if the old table-level overload
     * were called instead, the capture would see {@code -1L} (not matched) or the
     * mock would not bind.
     */
    @Test
    public void testOptDistributionPrunerPassesIndexMetaId(
            @Mocked OlapTable olapTable,
            @Mocked Partition partition,
            @Mocked PhysicalPartition physicalPartition,
            @Mocked MaterializedIndex index,
            @Mocked DistributionInfo distributionInfo) {

        final long rollupMetaId = 42L;
        final List<Long> expectedTablets = Lists.newArrayList(2L);
        final Column k2 = new Column("k2", IntegerType.INT, false);
        final List<Column> rollupColumns = Lists.newArrayList(k2);
        final long[] capturedMetaId = {-1L};

        new MockUp<MetaUtils>() {
            @Mock
            public List<Column> getRangeDistributionColumns(OlapTable t, long indexMetaId) {
                capturedMetaId[0] = indexMetaId;
                return rollupColumns;
            }
        };

        new MockUp<RangeDistributionPruner>() {
            @Mock
            public Collection<Long> prune() {
                return expectedTablets;
            }
        };

        new Expectations() {{
            olapTable.getPartition(anyLong);
            result = partition;

            olapTable.getIdToColumn();
            result = Maps.newHashMap();

            partition.getSubPartitions();
            result = Arrays.asList(physicalPartition);

            physicalPartition.getLatestIndex(anyLong);
            result = index;

            partition.getDistributionInfo();
            result = distributionInfo;

            distributionInfo.getType();
            result = DistributionInfo.DistributionInfoType.RANGE;

            index.getMetaId();
            result = rollupMetaId;

            index.getTablets();
            result = Lists.newArrayList();
        }};

        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator k2Ref = factory.create("k2", IntegerType.INT, false);
        Map<ColumnRefOperator, Column> scanColumnMap = Maps.newHashMap();
        scanColumnMap.put(k2Ref, k2);

        LogicalOlapScanOperator operator = new LogicalOlapScanOperator(
                olapTable, scanColumnMap, Maps.newHashMap(), null, -1L, null,
                rollupMetaId, Lists.newArrayList(1L), null, false,
                Lists.newArrayList(), Lists.newArrayList(), null, false);

        List<Long> result = OptDistributionPruner.pruneTabletIds(operator, Lists.newArrayList(1L));

        assertEquals(rollupMetaId, capturedMetaId[0],
                "OptDistributionPruner must pass the selected index's metaId to MetaUtils");
        assertEquals(expectedTablets, result);
    }
}
