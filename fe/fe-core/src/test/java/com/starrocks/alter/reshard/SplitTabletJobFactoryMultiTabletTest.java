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

package com.starrocks.alter.reshard;

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for {@link SplitTabletJobFactory#forExternalBoundariesMultiTablet}.
 *
 * <p>Substrate-only coverage: confirms the factory produces a metadata-only
 * {@link SplitTabletJob} (PENDING state, no shard allocation) whose
 * {@code splittingTablets} list spans every entry of the input
 * {@code Map<Long, List<TabletRange>>} and mirrors the existing single-tablet
 * factory's per-entry validation.
 */
public class SplitTabletJobFactoryMultiTabletTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("multi_tablet_factory_test").useDatabase("multi_tablet_factory_test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("multi_tablet_factory_test");

        String sql = "create table test_table (key1 int, key2 varchar(10))\n"
                + "order by(key1)\n"
                + "properties('replication_num' = '1'); ";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table");

        // The default range-distribution test table is born with one tablet
        // covering Range.all(). The multi-tablet tests below need at least 3
        // tablets in a single index so they can produce a map with 3 entries.
        // Reuse the existing single-tablet factory to grow the index to >= 3
        // tablets by splitting Range.all() into 3 contiguous external-boundaries
        // ranges; that gives us a stable 3-tablet base for every test method.
        ensureMultipleTablets();
    }

    /**
     * Bootstrap the shared static table to have at least 3 tablets in its
     * latest base index by invoking the existing single-tablet
     * {@code forExternalBoundaries} factory followed by a job run that drives
     * the index to its final tablet set. Idempotent: skips when the index
     * already has >= 3 tablets (e.g. the table was created with >1 buckets).
     */
    private static void ensureMultipleTablets() throws Exception {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex index = physicalPartition.getLatestBaseIndex();
        if (index.getTablets().size() >= 3) {
            return;
        }
        // The single-tablet bootstrap path is exercised end-to-end in
        // SplitTabletJobTest#testRunExternalBoundariesTabletReshardJob, which
        // includes a working MockLakeService. To keep this test focused on the
        // multi-tablet factory's validation surface (no BE round-trip), grow
        // the index by directly inserting LakeTablet siblings into the test
        // table's existing materialized index — same trick MergeTabletJobTest
        // uses to set up multi-tablet fixtures.
        for (int i = index.getTablets().size(); i < 3; i++) {
            long tabletId = GlobalStateMgr.getCurrentState().getNextId();
            TabletRange range = new TabletRange();
            Tablet tablet = new com.starrocks.lake.LakeTablet(tabletId, range);
            // updateInvertedIndex=true: the factory looks up each old tablet via
            // TabletInvertedIndex.getTabletMeta — synthetic siblings have to be
            // registered there too, otherwise forExternalBoundariesMultiTablet
            // throws "Cannot find tablet ... in inverted index".
            index.addTablet(tablet, new com.starrocks.catalog.TabletMeta(
                    db.getId(), table.getId(), physicalPartition.getId(), index.getId(),
                    com.starrocks.thrift.TStorageMedium.HDD, true), true);
        }
    }

    private static List<Long> getOldTabletIds(int count) {
        PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex index = physicalPartition.getLatestBaseIndex();
        List<Tablet> tablets = index.getTablets();
        Assertions.assertTrue(tablets.size() >= count,
                "test fixture must have >= " + count + " tablets (has " + tablets.size() + ")");
        List<Long> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            // Reset each chosen tablet to Range.all() so the K=2 external-boundaries
            // ranges in tests below validate against an unbounded parent. Prior
            // test methods on the shared static table can leave bounded ranges.
            tablets.get(i).setRange(new TabletRange());
            result.add(tablets.get(i).getId());
        }
        return result;
    }

    @Test
    public void producesOneJobAcrossMultipleTablets() throws Exception {
        List<Long> oldTabletIds = getOldTabletIds(3);

        Map<Long, List<TabletRange>> input = new LinkedHashMap<>();
        for (long oldTabletId : oldTabletIds) {
            input.put(oldTabletId, twoRanges());
        }

        TabletReshardJob job = SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, table, input);

        Assertions.assertNotNull(job);
        Assertions.assertTrue(job instanceof SplitTabletJob, "factory must return SplitTabletJob");
        SplitTabletJob splitJob = (SplitTabletJob) job;

        // One job spanning N splittingTablets — count them across every
        // reshardingPhysicalPartition / index in the job.
        int splittingCount = 0;
        for (ReshardingPhysicalPartition rpp : splitJob.getReshardingPhysicalPartitions().values()) {
            for (ReshardingMaterializedIndex rmi : rpp.getReshardingIndexes().values()) {
                for (ReshardingTablet rt : rmi.getReshardingTablets()) {
                    if (rt.getSplittingTablet() != null) {
                        splittingCount++;
                    }
                }
            }
        }
        Assertions.assertEquals(3, splittingCount,
                "one job must carry exactly 3 SplittingTablet entries (one per input entry)");
    }

    @Test
    public void eachTabletGetsItsOwnBoundaryList() throws Exception {
        List<Long> oldTabletIds = getOldTabletIds(3);

        Map<Long, List<TabletRange>> input = new LinkedHashMap<>();
        input.put(oldTabletIds.get(0), List.of(tabletRangeUpperOnly(10), tabletRangeLowerOnly(10)));
        input.put(oldTabletIds.get(1),
                List.of(tabletRangeUpperOnly(20), tabletRange(20, 40), tabletRangeLowerOnly(40)));
        input.put(oldTabletIds.get(2),
                List.of(tabletRangeUpperOnly(50), tabletRange(50, 60), tabletRange(60, 70),
                        tabletRangeLowerOnly(70)));

        TabletReshardJob job = SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, table, input);

        // For each input entry, the produced SplittingTablet's
        // newTabletRanges must match the supplied list element-for-element.
        for (Map.Entry<Long, List<TabletRange>> entry : input.entrySet()) {
            SplittingTablet st = findSplittingTablet((SplitTabletJob) job, entry.getKey());
            Assertions.assertEquals(entry.getValue().size(), st.getNewTabletRanges().size(),
                    "newTabletRanges size mismatch for tablet " + entry.getKey());
            for (int i = 0; i < entry.getValue().size(); i++) {
                Assertions.assertEquals(entry.getValue().get(i).getRange(),
                        st.getNewTabletRanges().get(i).getRange(),
                        "newTabletRanges[" + i + "] mismatch for tablet " + entry.getKey());
            }
            Assertions.assertEquals(entry.getValue().size(), st.getNewTabletIds().size(),
                    "newTabletIds size must equal newTabletRanges size for tablet " + entry.getKey());
        }
    }

    @Test
    public void rejectsEntryViolatingPerTabletCap() {
        List<Long> oldTabletIds = getOldTabletIds(2);

        int oversize = Config.tablet_reshard_max_split_count + 1;
        List<TabletRange> tooMany = new ArrayList<>(oversize);
        for (int i = 0; i < oversize; i++) {
            tooMany.add(tabletRange(i * 100, (i + 1) * 100));
        }
        Map<Long, List<TabletRange>> input = new LinkedHashMap<>();
        input.put(oldTabletIds.get(0), twoRanges());
        input.put(oldTabletIds.get(1), tooMany);

        // One bad entry must fail the whole factory call — no partial job is built.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, table, input));
    }

    @Test
    public void factoryDoesNotAllocateShards() throws Exception {
        List<Long> oldTabletIds = getOldTabletIds(2);
        Map<Long, List<TabletRange>> input = new LinkedHashMap<>();
        for (long oldTabletId : oldTabletIds) {
            input.put(oldTabletId, twoRanges());
        }

        // Install a MockUp on StarOSAgent BEFORE invoking the factory. If a
        // regression wires shard allocation into the factory, the MockUp will
        // intercept the call and flip the flag — catching a class of bugs
        // where the job state is left at PENDING but the StarOS RPC fires
        // anyway (e.g. eager-allocation in the factory body).
        AtomicBoolean createShardsCalled = new AtomicBoolean(false);
        new MockUp<StarOSAgent>() {
            @Mock
            public void createShardsForSplit(Map<Long, Long> newToOldShardId,
                                             Map<Long, List<Long>> newShardIdToGroupIds,
                                             FilePathInfo pathInfo,
                                             FileCacheInfo cacheInfo,
                                             Map<String, String> properties,
                                             ComputeResource computeResource) throws DdlException {
                createShardsCalled.set(true);
            }
        };

        TabletReshardJob job = SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, table, input);

        // Two orthogonal checks: (1) StarOSAgent.createShardsForSplit was NOT
        // invoked during the factory call, and (2) the returned job is PENDING
        // (which is what allows the scheduler — not the factory — to allocate
        // shards later via SplitTabletJob.createShardsOnStarOS).
        Assertions.assertFalse(createShardsCalled.get(),
                "factory must not invoke StarOSAgent.createShardsForSplit (parity with forExternalBoundaries)");
        Assertions.assertEquals(TabletReshardJob.JobState.PENDING, job.getJobState(),
                "multi-tablet factory must return a PENDING job (no shard allocation yet)");
    }

    @Test
    public void parityWithSingleTabletFactory() throws Exception {
        List<Long> oldTabletIds = getOldTabletIds(1);
        long oldTabletId = oldTabletIds.get(0);
        List<TabletRange> ranges = twoRanges();

        // Single-entry map through the multi-tablet factory must produce
        // a job structurally equivalent to the single-tablet factory.
        Map<Long, List<TabletRange>> input = new LinkedHashMap<>();
        input.put(oldTabletId, ranges);
        TabletReshardJob multiJob =
                SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, table, input);
        TabletReshardJob singleJob =
                SplitTabletJobFactory.forExternalBoundaries(db, table, oldTabletId, ranges);

        Assertions.assertEquals(singleJob.getClass(), multiJob.getClass(),
                "multi-tablet and single-tablet factories must produce the same job class");

        SplittingTablet multiSt = findSplittingTablet((SplitTabletJob) multiJob, oldTabletId);
        SplittingTablet singleSt = findSplittingTablet((SplitTabletJob) singleJob, oldTabletId);

        Assertions.assertEquals(singleSt.getNewTabletRanges().size(), multiSt.getNewTabletRanges().size(),
                "newTabletRanges size must match the single-tablet factory output");
        for (int i = 0; i < singleSt.getNewTabletRanges().size(); i++) {
            Assertions.assertEquals(singleSt.getNewTabletRanges().get(i).getRange(),
                    multiSt.getNewTabletRanges().get(i).getRange(),
                    "newTabletRanges[" + i + "] must match the single-tablet factory");
        }
    }

    @Test
    public void rejectsEmptyMap() {
        Map<Long, List<TabletRange>> empty = new LinkedHashMap<>();
        // Empty input — no-op submission is rejected at the factory boundary
        // so the operator gets the error synchronously instead of a silently
        // empty job that would later fail on submission.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, table, empty));
    }

    /**
     * Regression for the cross-partition MaterializedIndex reuse bug.
     *
     * <p>A freshly-created physical partition's base {@link MaterializedIndex}
     * id equals the index <i>meta</i> id (see {@code LocalMetastore}: "initially,
     * index id and index meta id are the same"), and the meta id is shared
     * across every physical partition of a table. A combined multi-partition
     * job therefore has two partitions whose base indexes carry the SAME index
     * id but DISTINCT tablet sets. The old flat {@code indexById} map keyed by
     * {@code index.getId()} let a later partition overwrite an earlier
     * partition's index, so the rebuild loop enumerated the wrong partition's
     * tablets. This test submits one old tablet from EACH partition and asserts
     * each produced {@link ReshardingPhysicalPartition} references exactly its
     * OWN tablets — no cross-contamination.
     */
    @Test
    public void eachPartitionUsesItsOwnMaterializedIndex() throws Exception {
        String partitionedTableName = "two_partition_split_test";
        String sql = "create table " + partitionedTableName + " (key1 int, key2 int)\n"
                + "partition by range(key1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "order by(key1)\n"
                + "properties('replication_num' = '1');";
        starRocksAssert.withTable(sql);
        OlapTable partitionedTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), partitionedTableName);

        // Collect the two partitions' base indexes. They must share the same
        // index id (meta id) — that is the precondition that triggered the bug.
        List<PhysicalPartition> physicalPartitions = new ArrayList<>(partitionedTable.getAllPhysicalPartitions());
        Assertions.assertEquals(2, physicalPartitions.size(),
                "fixture must have exactly two physical partitions");
        PhysicalPartition partitionA = physicalPartitions.get(0);
        PhysicalPartition partitionB = physicalPartitions.get(1);
        MaterializedIndex indexA = partitionA.getLatestBaseIndex();
        MaterializedIndex indexB = partitionB.getLatestBaseIndex();
        Assertions.assertEquals(indexA.getId(), indexB.getId(),
                "precondition: the two partitions' base indexes share the same index id");

        // Pick one old tablet from each partition and reset it to Range.all()
        // so the K=2 external-boundaries ranges validate against an unbounded parent.
        Tablet oldTabletA = indexA.getTablets().get(0);
        Tablet oldTabletB = indexB.getTablets().get(0);
        oldTabletA.setRange(new TabletRange());
        oldTabletB.setRange(new TabletRange());
        long oldTabletIdA = oldTabletA.getId();
        long oldTabletIdB = oldTabletB.getId();

        Map<Long, List<TabletRange>> input = new LinkedHashMap<>();
        input.put(oldTabletIdA, twoRanges());
        input.put(oldTabletIdB, twoRanges());

        TabletReshardJob job =
                SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, partitionedTable, input);
        SplitTabletJob splitJob = (SplitTabletJob) job;

        // Each ReshardingPhysicalPartition must enumerate exactly its own
        // partition's tablet ids — never the other partition's.
        java.util.Set<Long> tabletIdsA = tabletIdsOf(indexA);
        java.util.Set<Long> tabletIdsB = tabletIdsOf(indexB);

        ReshardingPhysicalPartition rppA =
                splitJob.getReshardingPhysicalPartitions().get(partitionA.getId());
        ReshardingPhysicalPartition rppB =
                splitJob.getReshardingPhysicalPartitions().get(partitionB.getId());
        Assertions.assertNotNull(rppA, "job must carry partition A");
        Assertions.assertNotNull(rppB, "job must carry partition B");

        Assertions.assertEquals(tabletIdsA, oldTabletIdsOf(rppA),
                "partition A's resharding tablets must be exactly partition A's tablets");
        Assertions.assertEquals(tabletIdsB, oldTabletIdsOf(rppB),
                "partition B's resharding tablets must be exactly partition B's tablets");

        // The splitting tablet in each partition must be that partition's own old tablet.
        Assertions.assertNotNull(findSplittingTablet(splitJob, oldTabletIdA));
        Assertions.assertNotNull(findSplittingTablet(splitJob, oldTabletIdB));
        Assertions.assertTrue(tabletIdsA.contains(oldTabletIdA));
        Assertions.assertTrue(tabletIdsB.contains(oldTabletIdB));
    }

    // -----------------------------------------------------------------
    // Error / validation throw paths
    // -----------------------------------------------------------------

    @Test
    public void rejectsUnknownTabletId() {
        // A tablet id that is not in the inverted index for this table must
        // surface "Cannot find tablet ... in inverted index" synchronously,
        // before any catalog mutation.
        long unknownTabletId = GlobalStateMgr.getCurrentState().getNextId();
        Map<Long, List<TabletRange>> input = new LinkedHashMap<>();
        input.put(unknownTabletId, twoRanges());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, table, input));
        Assertions.assertTrue(thrown.getMessage().contains("Cannot find tablet"),
                "unknown tablet id must report 'Cannot find tablet': " + thrown.getMessage());
    }

    @Test
    public void rejectsNonNormalTableState() throws Exception {
        // A table not in NORMAL state must throw "Unexpected table state" inside
        // the READ-locked block. Restore the state in finally so the shared
        // static table stays usable for sibling tests.
        List<Long> oldTabletIds = getOldTabletIds(2);
        Map<Long, List<TabletRange>> input = new LinkedHashMap<>();
        for (long oldTabletId : oldTabletIds) {
            input.put(oldTabletId, twoRanges());
        }

        OlapTable.OlapTableState savedState = table.getState();
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                    () -> SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, table, input));
            Assertions.assertTrue(thrown.getMessage().contains("Unexpected table state"),
                    "non-NORMAL table must report 'Unexpected table state': " + thrown.getMessage());
        } finally {
            table.setState(savedState);
        }
    }

    @Test
    public void rejectsNonRangeDistributionTable() throws Exception {
        // validateTableLevel rejects a hash-distributed (non-range) table with
        // "Unsupported distribution type" before acquiring the table lock.
        String hashTableName = "hash_distribution_split_test";
        String sql = "create table " + hashTableName + " (key1 int, key2 int)\n"
                + "distributed by hash(key1) buckets 2\n"
                + "properties('replication_num' = '1');";
        starRocksAssert.withTable(sql);
        OlapTable hashTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), hashTableName);

        Map<Long, List<TabletRange>> input = new LinkedHashMap<>();
        input.put(GlobalStateMgr.getCurrentState().getNextId(), twoRanges());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> SplitTabletJobFactory.forExternalBoundariesMultiTablet(db, hashTable, input));
        Assertions.assertTrue(thrown.getMessage().contains("Unsupported distribution type"),
                "hash-distributed table must report 'Unsupported distribution type': " + thrown.getMessage());
    }

    private static java.util.Set<Long> tabletIdsOf(MaterializedIndex index) {
        java.util.Set<Long> ids = new java.util.HashSet<>();
        for (Tablet tablet : index.getTablets()) {
            ids.add(tablet.getId());
        }
        return ids;
    }

    private static java.util.Set<Long> oldTabletIdsOf(ReshardingPhysicalPartition rpp) {
        java.util.Set<Long> ids = new java.util.HashSet<>();
        for (ReshardingMaterializedIndex rmi : rpp.getReshardingIndexes().values()) {
            for (ReshardingTablet rt : rmi.getReshardingTablets()) {
                ids.add(rt.getFirstOldTabletId());
            }
        }
        return ids;
    }

    // -----------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------

    private static List<TabletRange> twoRanges() {
        // K=2 external-boundaries split of a Range.all() parent: first.upper
        // open, last.lower closed, meeting at the same boundary value.
        return List.of(tabletRangeUpperOnly(0), tabletRangeLowerOnly(0));
    }

    private static TabletRange tabletRange(int lowerValue, int upperValue) {
        return new TabletRange(Range.of(createTuple(lowerValue), createTuple(upperValue), true, false));
    }

    private static TabletRange tabletRangeUpperOnly(int upperValue) {
        return new TabletRange(Range.lt(createTuple(upperValue)));
    }

    private static TabletRange tabletRangeLowerOnly(int lowerValue) {
        return new TabletRange(Range.ge(createTuple(lowerValue)));
    }

    private static Tuple createTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    private static SplittingTablet findSplittingTablet(SplitTabletJob job, long oldTabletId) {
        for (ReshardingPhysicalPartition rpp : job.getReshardingPhysicalPartitions().values()) {
            for (ReshardingMaterializedIndex rmi : rpp.getReshardingIndexes().values()) {
                for (ReshardingTablet rt : rmi.getReshardingTablets()) {
                    SplittingTablet st = rt.getSplittingTablet();
                    if (st != null && st.getOldTabletId() == oldTabletId) {
                        return st;
                    }
                }
            }
        }
        throw new AssertionError("SplittingTablet for old tablet " + oldTabletId + " not found");
    }
}
