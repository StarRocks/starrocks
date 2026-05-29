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

import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Unit tests for {@link RangeColocateScanDispatch} — the scan-time facade that
 * encapsulates range-colocate dispatch policy. Higher-level tests that exercise
 * {@code OlapScanNode.getBucketNums()} and the optimizer-built
 * {@code PlanFragmentBuilder} path live in
 * {@link RangeColocateScanRangeDispatchTest}.
 */
public class RangeColocateScanDispatchTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;

    private static final int COLOCATE_COL_COUNT = 1;
    private static final List<Column> SORT_KEY = Arrays.asList(
            new Column("k1", IntegerType.INT),
            new Column("k2", IntegerType.INT));

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("p4_dispatch_facade").useDatabase("p4_dispatch_facade");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("p4_dispatch_facade");
    }

    private static Tuple makeTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    private static MaterializedIndex baseIndexOf(OlapTable table) {
        PhysicalPartition partition = table.getPartitions().iterator().next()
                .getDefaultPhysicalPartition();
        return partition.getLatestBaseIndex();
    }

    private static LakeTablet makeLakeTabletWithRange(long tabletId, Range<Tuple> colocateRange) {
        Range<Tuple> fullRange = ColocateRangeUtils.expandToFullSortKey(
                colocateRange, SORT_KEY, COLOCATE_COL_COUNT);
        return new LakeTablet(tabletId, new TabletRange(fullRange));
    }

    /**
     * Adds a tablet to the index without touching the inverted index. The shard ids
     * we synthesize do not exist in StarOS, so we must skip the inverted-index path
     * to avoid leaking them across tests.
     */
    private static void addSyntheticTablet(MaterializedIndex index, Tablet tablet) {
        index.addTablet(tablet, null, false);
    }

    // ---- forTable() gate ----

    @Test
    public void testForTableReturnsEmptyForNonRangeTable() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_hash (k1 int, k2 int)\n"
                        + "distributed by hash(k1) buckets 4\n"
                        + "properties('replication_num' = '1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_hash");

        Assertions.assertNull(RangeColocateScanDispatch.forTable(table));
    }

    @Test
    public void testForTableReturnsEmptyForRangeNonColocateTable() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_nonloc (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_nonloc");

        Assertions.assertNull(RangeColocateScanDispatch.forTable(table));
    }

    @Test
    public void testForTableReturnsDispatchForRangeColocateTable() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_colo (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_facade_colo:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_colo");

        RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(table);
        Assertions.assertNotNull(dispatch);
        // Initial state: one ColocateRange = (-inf, +inf).
        Assertions.assertEquals(1, dispatch.bucketCount());
    }

    // ---- bucketCount + computeBucketSeq for the aligned path ----

    @Test
    public void testComputeBucketSeqSingleRangeAligned() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_single (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_facade_single:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_single");
        MaterializedIndex index = baseIndexOf(table);

        RangeColocateScanDispatch dispatch =
                Objects.requireNonNull(RangeColocateScanDispatch.forTable(table));
        Map<Long, Integer> bucketSeq = dispatch.computeBucketSeq(index);
        Assertions.assertNotNull(bucketSeq);
        Assertions.assertEquals(1, bucketSeq.size());
        long tabletId = index.getTabletIdsInOrder().get(0);
        Assertions.assertEquals(0, bucketSeq.get(tabletId));
    }

    @Test
    public void testComputeBucketSeqIntraColocateSplitMapsToSameBucket() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_intra (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_facade_intra:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_intra");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long groupId = colocateTableIndex.getGroup(table.getId()).grpId;

        // Three ColocateRanges. Range 1 ([100, 200)) holds two intra-colocate
        // sub-range tablets; ranges 0 and 2 each hold a tablet that exactly
        // covers the range. This mirrors the post-P3 intra-colocate split state.
        colocateTableIndex.getColocateRangeMgr().setColocateRanges(groupId, Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 9001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 9002L),
                new ColocateRange(Range.ge(makeTuple(200)), 9003L)));

        // Insert in non-range order — bucketSeq must come from ColocateRange index,
        // not insertion position.
        MaterializedIndex index = new MaterializedIndex(99L);
        addSyntheticTablet(index, makeLakeTabletWithRange(902L, Range.ge(makeTuple(200))));
        Tuple lower1 = new Tuple(Arrays.asList(
                Variant.of(IntegerType.INT, "150"), Variant.of(IntegerType.INT, "1")));
        Tuple upper1 = new Tuple(Arrays.asList(
                Variant.of(IntegerType.INT, "150"), Variant.of(IntegerType.INT, "5000")));
        Tuple lower2 = new Tuple(Arrays.asList(
                Variant.of(IntegerType.INT, "150"), Variant.of(IntegerType.INT, "5000")));
        Tuple upper2 = new Tuple(Arrays.asList(
                Variant.of(IntegerType.INT, "150"), Variant.of(IntegerType.INT, "9999")));
        addSyntheticTablet(index, new LakeTablet(911L, new TabletRange(Range.gelt(lower1, upper1))));
        addSyntheticTablet(index, new LakeTablet(912L, new TabletRange(Range.gelt(lower2, upper2))));
        addSyntheticTablet(index, makeLakeTabletWithRange(900L, Range.lt(makeTuple(100))));

        RangeColocateScanDispatch dispatch =
                Objects.requireNonNull(RangeColocateScanDispatch.forTable(table));
        Map<Long, Integer> bucketSeq = dispatch.computeBucketSeq(index);
        Assertions.assertEquals(Map.of(900L, 0, 911L, 1, 912L, 1, 902L, 2), bucketSeq);
    }

    // ---- Unaligned states return empty ----

    @Test
    public void testComputeBucketSeqEmptyOnSpanningTablet() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_span (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_facade_span:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_span");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long groupId = colocateTableIndex.getGroup(table.getId()).grpId;

        // Inject 3 ranges without re-aligning the underlying single tablet that
        // still covers Range.all() — the spanning-tablet case.
        colocateTableIndex.getColocateRangeMgr().setColocateRanges(groupId, Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 9001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 9002L),
                new ColocateRange(Range.ge(makeTuple(200)), 9003L)));

        RangeColocateScanDispatch dispatch =
                Objects.requireNonNull(RangeColocateScanDispatch.forTable(table));
        Assertions.assertNull(dispatch.computeBucketSeq(baseIndexOf(table)));
    }

    @Test
    public void testComputeBucketSeqEmptyOnMissingRange() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_missing (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_facade_missing:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_missing");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long groupId = colocateTableIndex.getGroup(table.getId()).grpId;

        // Three ColocateRanges, but the synthetic index only has a tablet for range 0.
        colocateTableIndex.getColocateRangeMgr().setColocateRanges(groupId, Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 9001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 9002L),
                new ColocateRange(Range.ge(makeTuple(200)), 9003L)));

        MaterializedIndex index = new MaterializedIndex(98L);
        addSyntheticTablet(index, makeLakeTabletWithRange(800L, Range.lt(makeTuple(100))));

        RangeColocateScanDispatch dispatch =
                Objects.requireNonNull(RangeColocateScanDispatch.forTable(table));
        Assertions.assertNull(dispatch.computeBucketSeq(index));
    }

    @Test
    public void testComputeBucketSeqRejectsTabletWithoutRange() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_norange (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_facade_norange:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_norange");

        MaterializedIndex index = new MaterializedIndex(97L);
        addSyntheticTablet(index, new LakeTablet(701L)); // no TabletRange

        RangeColocateScanDispatch dispatch =
                Objects.requireNonNull(RangeColocateScanDispatch.forTable(table));
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> dispatch.computeBucketSeq(index));
        Assertions.assertTrue(exception.getMessage().contains("no TabletRange"),
                "actual: " + exception.getMessage());
    }

    // ---- requireAligned ----

    @Test
    public void testRequireAlignedOnAlignedGroupSucceeds() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_req_ok (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_facade_req_ok:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_req_ok");

        RangeColocateScanDispatch dispatch =
                Objects.requireNonNull(RangeColocateScanDispatch.forTable(table));
        Assertions.assertDoesNotThrow(() -> dispatch.requireAligned(
                List.of(table.getPartitions().iterator().next().getDefaultPhysicalPartition()),
                table.getBaseIndexMetaId()));
    }

    @Test
    public void testRequireAlignedOnUnalignedGroupThrows() throws Exception {
        starRocksAssert.withTable(
                "create table t_facade_req_bad (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'rg_facade_req_bad:k1');");

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_facade_req_bad");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long groupId = colocateTableIndex.getGroup(table.getId()).grpId;

        colocateTableIndex.getColocateRangeMgr().setColocateRanges(groupId, Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 9001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 9002L),
                new ColocateRange(Range.ge(makeTuple(200)), 9003L)));

        RangeColocateScanDispatch dispatch =
                Objects.requireNonNull(RangeColocateScanDispatch.forTable(table));
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> dispatch.requireAligned(
                        List.of(table.getPartitions().iterator().next().getDefaultPhysicalPartition()),
                        table.getBaseIndexMetaId()));
        Assertions.assertTrue(exception.getMessage().contains("unaligned state"),
                "actual: " + exception.getMessage());
    }
}
