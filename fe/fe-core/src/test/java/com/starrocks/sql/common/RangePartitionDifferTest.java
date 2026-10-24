// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.common;

import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.DateType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for the bounded-refresh handling in {@link RangePartitionDiffer#diff}.
 *
 * <p>A refresh with PARTITION START/END must not partially replace a partition set: when a shape
 * change swaps one or more old MV partitions for one or more mapped partitions (fine to coarse, or
 * coarse to fine), the replacement component is applied only if it is fully inside the requested
 * range, otherwise it is left completely untouched.
 */
public class RangePartitionDifferTest {

    private static final Column PARTITION_COLUMN = new Column("k1", DateType.DATETIME);

    private static Range<PartitionKey> range(String lower, String upper) throws AnalysisException {
        PartitionKey lo = PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue(lower)),
                Collections.singletonList(PARTITION_COLUMN));
        PartitionKey hi = PartitionKey.createPartitionKey(Collections.singletonList(new PartitionValue(upper)),
                Collections.singletonList(PARTITION_COLUMN));
        return Range.closedOpen(lo, hi);
    }

    private static PCellSortedSet cells(Map<String, Range<PartitionKey>> ranges) {
        PCellSortedSet result = PCellSortedSet.of();
        for (Map.Entry<String, Range<PartitionKey>> entry : ranges.entrySet()) {
            result.add(entry.getKey(), PRangeCell.of(entry.getValue()));
        }
        return result;
    }

    private static RangePartitionDiffer differ(Range<PartitionKey> rangeToInclude) {
        MaterializedView mv = Mockito.mock(MaterializedView.class);
        Mockito.when(mv.getPartitionInfo()).thenReturn(Mockito.mock(PartitionInfo.class));
        Mockito.when(mv.getTableProperty()).thenReturn(Mockito.mock(TableProperty.class));
        return new RangePartitionDiffer(mv, MVTimelinessArbiter.QueryRewriteParams.ofRefresh(), rangeToInclude);
    }

    private static FunctionCallExpr dateTrunc(String granularity) {
        List<Expr> children = new ArrayList<>();
        children.add(new StringLiteral(granularity));
        children.add(new SlotRef(new TableName("db1", "table1"), "k1"));
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc", children);
        expr.setType(TypeFactory.createType(PrimitiveType.DATETIME));
        return expr;
    }

    private static PCellSortedSet fineToCoarseSrc() throws AnalysisException {
        return cells(Map.of(
                "p20210501_20210601", range("2021-05-01", "2021-06-01"),
                "p20210601_20210701", range("2021-06-01", "2021-07-01")));
    }

    private static PCellSortedSet fineToCoarseDst() throws AnalysisException {
        return cells(Map.of(
                "p20210501_20210502", range("2021-05-01", "2021-05-02"),
                "p20210502_20210503", range("2021-05-02", "2021-05-03"),
                "p20210601_20210602", range("2021-06-01", "2021-06-02"),
                "p20210602_20210603", range("2021-06-02", "2021-06-03")));
    }

    /**
     * Fine -&gt; coarse migration with a bounded refresh that only covers June: the out-of-range May
     * history must not be dropped even though its coarse replacement (May month) is pruned.
     */
    @Test
    public void testFineToCoarseKeepsOutOfRangeHistory() throws Exception {
        RangePartitionDiffer differ = differ(range("2021-06-01", "2021-07-01"));
        PartitionDiff diff = differ.diff(fineToCoarseSrc(), fineToCoarseDst(), true);

        Assertions.assertEquals(Collections.singleton("p20210601_20210701"), diff.getAdds().getPartitionNames());
        // in-range June days are still migrated
        Assertions.assertTrue(diff.getDeletes().containsName("p20210601_20210602"));
        Assertions.assertTrue(diff.getDeletes().containsName("p20210602_20210603"));
        // out-of-range May days must be preserved
        Assertions.assertFalse(diff.getDeletes().containsName("p20210501_20210502"));
        Assertions.assertFalse(diff.getDeletes().containsName("p20210502_20210503"));
    }

    /**
     * Coarse -&gt; fine migration (mirror disabled) with a bounded refresh that only covers one day:
     * the whole-month delete must not be applied while only one day replacement is created.
     */
    @Test
    public void testCoarseToFineKeepsOutOfRangeHistory() throws Exception {
        PCellSortedSet src = cells(Map.of(
                "p20210501_20210502", range("2021-05-01", "2021-05-02"),
                "p20210515_20210516", range("2021-05-15", "2021-05-16")));
        PCellSortedSet dst = cells(Map.of("p20210501_20210601", range("2021-05-01", "2021-06-01")));

        RangePartitionDiffer differ = differ(range("2021-05-15", "2021-05-16"));
        PartitionDiff diff = differ.diff(src, dst, true);

        // the whole replacement component is left untouched: no day created, month not dropped
        Assertions.assertTrue(diff.getAdds().getPartitionNames().isEmpty(), "adds: " + diff.getAdds());
        Assertions.assertTrue(diff.getDeletes().getPartitionNames().isEmpty(), "deletes: " + diff.getDeletes());
    }

    /**
     * Reverse migration through the production entry point: the mirror flag is already off, so the
     * eager mapper is selected, but the MV still holds the coarse month partition created while the
     * flag was on. The coarse-to-fine shape change must still be detected from the existing MV
     * partitions, otherwise a bounded refresh into one day drops the whole month.
     */
    @Test
    public void testCoarseToFineDetectedWhenMirrorDisabled() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = false;
        try {
            // base table still has a merged month partition; eager expands it into daily ranges
            PCellSortedSet base = cells(Map.of("p202105", range("2021-05-01", "2021-06-01")));
            // mv holds the coarse month partition from the mirror era
            PCellSortedSet mv = cells(Map.of("p20210501_20210601", range("2021-05-01", "2021-06-01")));

            PartitionDiff diff = SyncPartitionUtils.getRangePartitionDiffOfExpr(
                    base, mv, dateTrunc("day"), differ(range("2021-05-15", "2021-05-16")));

            // the whole replacement component is left untouched: no day created, month not dropped
            Assertions.assertTrue(diff.getAdds().getPartitionNames().isEmpty(), "adds: " + diff.getAdds());
            Assertions.assertTrue(diff.getDeletes().getPartitionNames().isEmpty(), "deletes: " + diff.getDeletes());
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }

    /**
     * A genuine deletion and a pure addition that only touch at an endpoint must NOT be fused into one
     * replacement component; the old partition must still be deleted even when the refresh range only
     * covers the new partition.
     */
    @Test
    public void testAdjacentDeletionAndAdditionAreSeparate() throws Exception {
        PCellSortedSet src = cells(Map.of("p20210502_20210503", range("2021-05-02", "2021-05-03")));
        PCellSortedSet dst = cells(Map.of("p20210501_20210502", range("2021-05-01", "2021-05-02")));

        RangePartitionDiffer differ = differ(range("2021-05-02", "2021-05-03"));
        PartitionDiff diff = differ.diff(src, dst, true);

        Assertions.assertEquals(Collections.singleton("p20210502_20210503"), diff.getAdds().getPartitionNames());
        Assertions.assertEquals(Collections.singleton("p20210501_20210502"), diff.getDeletes().getPartitionNames());
    }

    /** Aligned range fully covering the replacement component migrates it as usual. */
    @Test
    public void testAlignedRangeMigratesFully() throws Exception {
        PCellSortedSet src = cells(Map.of("p20210501_20210601", range("2021-05-01", "2021-06-01")));
        PCellSortedSet dst = cells(Map.of(
                "p20210501_20210502", range("2021-05-01", "2021-05-02"),
                "p20210502_20210503", range("2021-05-02", "2021-05-03")));

        RangePartitionDiffer differ = differ(range("2021-05-01", "2021-06-01"));
        PartitionDiff diff = differ.diff(src, dst, true);

        Assertions.assertEquals(Collections.singleton("p20210501_20210601"), diff.getAdds().getPartitionNames());
        Assertions.assertEquals(2, diff.getDeletes().size());
        Assertions.assertTrue(diff.getDeletes().containsName("p20210501_20210502"));
        Assertions.assertTrue(diff.getDeletes().containsName("p20210502_20210503"));
    }

    /** A sub-range that only overlaps part of a replacement component leaves the component untouched. */
    @Test
    public void testSubRangeLeavesComponentUntouched() throws Exception {
        PCellSortedSet src = cells(Map.of("p20210501_20210601", range("2021-05-01", "2021-06-01")));
        PCellSortedSet dst = cells(Map.of(
                "p20210501_20210502", range("2021-05-01", "2021-05-02"),
                "p20210502_20210503", range("2021-05-02", "2021-05-03")));

        RangePartitionDiffer differ = differ(range("2021-05-01", "2021-05-02"));
        PartitionDiff diff = differ.diff(src, dst, true);

        Assertions.assertTrue(diff.getAdds().getPartitionNames().isEmpty(), "adds: " + diff.getAdds());
        Assertions.assertTrue(diff.getDeletes().getPartitionNames().isEmpty(), "deletes: " + diff.getDeletes());
    }

    /** A genuine removal without any replacement keeps the existing behavior. */
    @Test
    public void testGenuineDeleteUnaffected() throws Exception {
        PCellSortedSet src = PCellSortedSet.of();
        PCellSortedSet dst = cells(Map.of("p20210501_20210502", range("2021-05-01", "2021-05-02")));

        RangePartitionDiffer differ = differ(range("2021-01-01", "2022-01-01"));
        PartitionDiff diff = differ.diff(src, dst, true);

        Assertions.assertTrue(diff.getAdds().getPartitionNames().isEmpty());
        Assertions.assertEquals(Collections.singleton("p20210501_20210502"), diff.getDeletes().getPartitionNames());
    }

    /** A pure addition with a bounded range and no deletion hits the empty-deletes short-circuit. */
    @Test
    public void testPureAddWithBoundedRangeUnaffected() throws Exception {
        PCellSortedSet src = cells(Map.of("p20210502_20210503", range("2021-05-02", "2021-05-03")));
        PCellSortedSet dst = PCellSortedSet.of();

        RangePartitionDiffer differ = differ(range("2021-05-01", "2021-06-01"));
        PartitionDiff diff = differ.diff(src, dst, true);

        Assertions.assertEquals(Collections.singleton("p20210502_20210503"), diff.getAdds().getPartitionNames());
        Assertions.assertTrue(diff.getDeletes().getPartitionNames().isEmpty());
    }

    /**
     * With the default (granularity-preserving) mapping, a bounded refresh keeps the previous
     * behavior: out-of-range legacy partitions are still deleted even when their replacement is
     * pruned. This is the semantics relied on by {@code PARTITION BY (col)} / {@code str2date} MVs,
     * so the mirror-specific atomic-replacement rule must not apply there.
     */
    @Test
    public void testDefaultMappingDoesNotPreserveOutOfRangeReplacement() throws Exception {
        RangePartitionDiffer differ = differ(range("2021-06-01", "2021-07-01"));
        PartitionDiff diff = differ.diff(fineToCoarseSrc(), fineToCoarseDst(), false);

        Assertions.assertEquals(Collections.singleton("p20210601_20210701"), diff.getAdds().getPartitionNames());
        // default mapping keeps the old behavior: the out-of-range May day partitions are deleted
        Assertions.assertTrue(diff.getDeletes().containsName("p20210501_20210502"));
        Assertions.assertTrue(diff.getDeletes().containsName("p20210502_20210503"));
        Assertions.assertTrue(diff.getDeletes().containsName("p20210601_20210602"));
        Assertions.assertTrue(diff.getDeletes().containsName("p20210602_20210603"));
    }

    /** A full refresh (no range) keeps the existing migration behavior. */
    @Test
    public void testFullRefreshUnaffected() throws Exception {
        RangePartitionDiffer differ = differ(null);
        PartitionDiff diff = differ.diff(fineToCoarseSrc(), fineToCoarseDst(), true);

        Assertions.assertEquals(
                Set.of("p20210501_20210601", "p20210601_20210701"), diff.getAdds().getPartitionNames());
        Assertions.assertEquals(4, diff.getDeletes().size());
    }
}
