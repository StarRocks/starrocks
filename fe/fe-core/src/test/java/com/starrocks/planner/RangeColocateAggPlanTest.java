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

import com.staros.proto.PlacementPolicy;
import com.starrocks.alter.reshard.ColocateChecker;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An aggregation or window over a range-colocate table whose grouping / partition keys cover every colocate
 * column runs colocated: one phase in the scan fragment, no shuffle exchange, and the scheduler dispatches that
 * fragment per ColocateRange.
 *
 * <p>The colocate group is given two ColocateRanges before its tables are created, so every partition gets one
 * aligned tablet per range and a scan of two partitions reads four tablets in two buckets (reading two or more
 * tablets also keeps the one-tablet optimization, which plans one phase regardless of colocate, out of the way).
 * Table {@code l2} instead has two tablets inside its group's single ColocateRange, split on the trailing sort
 * key, so one {@code k1} value spans both tablets of one bucket.
 * {@code enable_local_shuffle_agg} is off because the mini cluster has a single compute node, where it would
 * replace the shuffle these tests look for with a local shuffle.
 */
public class RangeColocateAggPlanTest {
    private static final String DB = "range_colocate_agg";
    private static final String TWO_PARTITIONS =
            "partition by range(k2) (partition p1 values less than ('1000'), partition p2 values less than ('2000')) ";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        savedEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
        connectContext.getSessionVariable().setEnableRangeDistribution(true);
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(false);

        starRocksAssert.withDatabase(DB).useDatabase(DB);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);

        // Creates group rca_grp with the single ColocateRange [MIN, MAX). Splitting the group below leaves this
        // table unaligned, so it is dropped once the aligned tables exist.
        starRocksAssert.withTable("create table seed (k1 int, k2 int, v int) order by(k1, k2) "
                + "properties('replication_num' = '1', 'colocate_with' = 'rca_grp:k1')");
        OlapTable seed = getTable("seed");
        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateIndex.getGroup(seed.getId()).grpId;
        long lowPackGroup = colocateIndex.getColocateRangeMgr().getColocateRanges(grpId).get(0).getShardGroupId();
        long highPackGroup = GlobalStateMgr.getCurrentState().getStarOSAgent()
                .createShardGroup(db.getId(), seed.getId(), 0L, 0L, PlacementPolicy.PACK);
        colocateIndex.getColocateRangeMgr().setColocateRanges(grpId, List.of(
                new ColocateRange(Range.lt(intTuple(100)), lowPackGroup),
                new ColocateRange(Range.ge(intTuple(100)), highPackGroup)));

        // Created after the split: each partition gets one tablet per ColocateRange.
        for (String name : List.of("t", "u", "ts")) {
            starRocksAssert.withTable("create table " + name + " (k1 int, k2 int, v int) duplicate key(k1, k2) "
                    + TWO_PARTITIONS + "order by(k1, k2) "
                    + "properties('replication_num' = '1', 'colocate_with' = 'rca_grp:k1')");
        }
        // FORCE: a plain drop parks the table in the recycle bin, where it stays a group member.
        starRocksAssert.dropTable("seed force");

        // A single-range group colocated on two columns, for the partial-cover case.
        starRocksAssert.withTable("create table p (k1 int, k2 int, v int) duplicate key(k1, k2) "
                + TWO_PARTITIONS + "order by(k1, k2) "
                + "properties('replication_num' = '1', 'colocate_with' = 'rca_grp2:k1,k2')");

        createTableSplitInsideColocateRange(db);

        starRocksAssert.withTable("create table h (k1 int, k2 int, v int) duplicate key(k1, k2) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')");
    }

    // Table l2: one partition whose single ColocateRange holds two tablets split at (k1, k2) = (5, 500), the layout
    // a tablet split inside a ColocateRange leaves behind. The tablets are real shards in the range's PACK group.
    private static void createTableSplitInsideColocateRange(Database db) throws Exception {
        starRocksAssert.withTable("create table l2 (k1 int, k2 int, v int) duplicate key(k1, k2) order by(k1, k2) "
                + "properties('replication_num' = '1', 'colocate_with' = 'rca_grp3:k1')");
        OlapTable table = getTable("l2");
        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateIndex.getGroup(table.getId()).grpId;
        long packGroup = colocateIndex.getColocateRangeMgr().getColocateRanges(grpId).get(0).getShardGroupId();
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        MaterializedIndex index = partition.getLatestBaseIndex();
        Map<String, String> properties = Map.of(
                LakeTablet.PROPERTY_KEY_TABLE_ID, String.valueOf(table.getId()),
                LakeTablet.PROPERTY_KEY_PARTITION_ID, String.valueOf(partition.getId()),
                LakeTablet.PROPERTY_KEY_INDEX_ID, String.valueOf(index.getId()));
        List<Long> shardIds = GlobalStateMgr.getCurrentState().getStarOSAgent().createShards(2,
                table.getPartitionFilePathInfo(partition.getId()), table.getPartitionFileCacheInfo(partition.getId()),
                List.of(index.getShardGroupId(), packGroup), null, properties, WarehouseManager.DEFAULT_RESOURCE);
        for (long tabletId : index.getTabletIdsInOrder()) {
            index.removeTablet(tabletId);
        }
        TabletMeta tabletMeta = new TabletMeta(db.getId(), table.getId(), partition.getId(), index.getId(),
                TStorageMedium.HDD, true);
        Tuple splitPoint = intTuple(5, 500);
        index.addTablet(new LakeTablet(shardIds.get(0), new TabletRange(Range.lt(splitPoint))), tabletMeta);
        index.addTablet(new LakeTablet(shardIds.get(1), new TabletRange(Range.ge(splitPoint))), tabletMeta);
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
    }

    private static Tuple intTuple(int... values) {
        return new Tuple(Arrays.stream(values)
                .mapToObj(value -> Variant.of(IntegerType.INT, String.valueOf(value)))
                .toList());
    }

    private static OlapTable getTable(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB, name);
    }

    private static String explain(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.NORMAL);
    }

    // The explain text of the fragment that holds the OlapScanNode(s).
    private static String scanFragment(String plan) {
        for (String fragment : plan.split("PLAN FRAGMENT ")) {
            if (fragment.contains("OlapScanNode")) {
                return fragment;
            }
        }
        throw new AssertionError("no scan fragment in plan:\n" + plan);
    }

    private static ExecutionFragment scheduledScanFragment(String sql) throws Exception {
        return UtFrameUtils.getPlanAndStartScheduling(connectContext, sql).second.getExecutionDAG()
                .getFragmentsInPreorder().stream()
                .filter(fragment -> fragment.getScanNodes().stream().anyMatch(scan -> scan instanceof OlapScanNode))
                .findFirst()
                .orElseThrow();
    }

    // The operator runs in the scan fragment with no shuffle feeding it, and the scheduler dispatches the fragment
    // per ColocateRange, one bucket sequence per range.
    private static void assertColocated(String sql, String operator, Set<Integer> buckets) throws Exception {
        String plan = explain(sql);
        Assertions.assertTrue(scanFragment(plan).contains(operator),
                () -> "expected " + operator + " in the scan fragment, plan was:\n" + plan);
        Assertions.assertFalse(plan.contains("HASH_PARTITIONED"), () -> "expected no shuffle, plan was:\n" + plan);
        Assertions.assertFalse(plan.contains("merge finalize"), () -> "expected one phase, plan was:\n" + plan);
        ExecutionFragment fragment = scheduledScanFragment(sql);
        Assertions.assertTrue(fragment.isColocated(), () -> "expected colocate dispatch, plan was:\n" + plan);
        Assertions.assertEquals(buckets, fragment.getColocatedAssignment().getSeqToWorkerId().keySet());
    }

    private static void assertShuffled(String sql) throws Exception {
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("HASH_PARTITIONED"),
                () -> "expected an aggregation shuffle, plan was:\n" + plan);
    }

    // Each range table is aggregated colocated in its own scan fragment and reaches `operator` through a hash
    // exchange on k1, and the scheduler assigns every tablet of the scanned tables to some instance.
    private static void assertAggregatedThenExchanged(String sql, String operator, String... rangeTables)
            throws Exception {
        String plan = explain(sql);
        List<String> fragments = Arrays.asList(plan.split("PLAN FRAGMENT "));
        List<String> operatorFragments = fragments.stream().filter(fragment -> fragment.contains(operator)).toList();
        Assertions.assertEquals(1, operatorFragments.size(),
                () -> "expected exactly one " + operator + " fragment, plan was:\n" + plan);
        for (String table : rangeTables) {
            String scan = "TABLE: " + table + "\n";
            Assertions.assertFalse(operatorFragments.get(0).contains(scan),
                    () -> table + " is scanned in the " + operator + " fragment, plan was:\n" + plan);
            List<String> scanFragments = fragments.stream().filter(fragment -> fragment.contains(scan)).toList();
            Assertions.assertEquals(1, scanFragments.size(), () -> "expected one scan of " + table + ":\n" + plan);
            Assertions.assertTrue(scanFragments.get(0).contains("AGGREGATE (update finalize)"),
                    () -> table + " is not aggregated colocated in its scan fragment:\n" + plan);
            Assertions.assertTrue(scanFragments.get(0).matches("(?s).*HASH_PARTITIONED: \\d+: k1.*"),
                    () -> table + " does not reach " + operator + " through a hash exchange on k1:\n" + plan);
        }

        Map<String, Integer> scheduled = new HashMap<>();
        for (ExecutionFragment fragment : UtFrameUtils.getPlanAndStartScheduling(connectContext, sql).second
                .getExecutionDAG().getFragmentsInPreorder()) {
            for (ScanNode scan : fragment.getScanNodes()) {
                if (!(scan instanceof OlapScanNode olapScan)) {
                    continue;
                }
                int scanId = scan.getId().asInt();
                int ranges = 0;
                for (FragmentInstance instance : fragment.getInstances()) {
                    ranges += instance.getNode2ScanRanges().getOrDefault(scanId, List.of()).size();
                    ranges += instance.getNode2DriverSeqToScanRanges().getOrDefault(scanId, Map.of()).values()
                            .stream().mapToInt(List::size).sum();
                }
                scheduled.merge(olapScan.getOlapTable().getName(), ranges, Integer::sum);
            }
        }
        Assertions.assertTrue(scheduled.keySet().containsAll(List.of(rangeTables)), () -> "scanned tables in: " + sql);
        for (Map.Entry<String, Integer> entry : scheduled.entrySet()) {
            int tablets = getTable(entry.getKey()).getPhysicalPartitions().stream()
                    .mapToInt(partition -> partition.getLatestBaseIndex().getTablets().size()).sum();
            Assertions.assertEquals(tablets, entry.getValue(),
                    () -> "scan ranges scheduled for " + entry.getKey() + " in: " + sql);
        }
    }

    @Test
    public void groupByColocateColumnIsOnePhase() throws Exception {
        assertColocated("select k1, count(*) from t group by k1", "AGGREGATE (update finalize)", Set.of(0, 1));
    }

    @Test
    public void groupBySupersetOfColocateColumnsIsOnePhase() throws Exception {
        assertColocated("select k1, k2, sum(v) from t group by k2, k1", "AGGREGATE (update finalize)", Set.of(0, 1));
    }

    @Test
    public void countDistinctGroupedByColocateColumnIsOnePhase() throws Exception {
        assertColocated("select k1, count(distinct v) from t group by k1", "multi_distinct_count", Set.of(0, 1));
    }

    @Test
    public void windowPartitionedByColocateColumnIsColocated() throws Exception {
        assertColocated("select k1, v, row_number() over (partition by k1 order by v) from t", "ANALYTIC",
                Set.of(0, 1));
    }

    @Test
    public void groupByColocateColumnOverSplitColocateRangeIsOnePhase() throws Exception {
        // Both tablets of l2 belong to bucket 0, so k1 = 5, which spans them, is aggregated by one instance.
        assertColocated("select k1, count(*) from l2 group by k1", "AGGREGATE (update finalize)", Set.of(0));
    }

    @Test
    public void colocateJoinThenAggregateIsOnePhase() throws Exception {
        String sql = "select t.k1, count(*) from t join u on t.k1 = u.k1 group by t.k1";
        Assertions.assertTrue(explain(sql).contains("COLOCATE"));
        assertColocated(sql, "AGGREGATE (update finalize)", Set.of(0, 1));
    }

    @Test
    public void groupByNonColocateColumnShuffles() throws Exception {
        assertShuffled("select k2, count(*) from t group by k2");
    }

    @Test
    public void groupByPartialColocateColumnsShuffles() throws Exception {
        assertShuffled("select k1, count(*) from p group by k1");
    }

    @Test
    public void groupByExpressionShuffles() throws Exception {
        assertShuffled("select k1 + 1, count(*) from t group by k1 + 1");
    }

    @Test
    public void fullOuterJoinThenAggregateShuffles() throws Exception {
        // t.k1 is NULL-padded for u's unmatched rows, which sit in every bucket.
        String sql = "select t.k1, count(*) from t full outer join u on t.k1 = u.k1 group by t.k1";
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("COLOCATE"), () -> "expected a colocate join, plan was:\n" + plan);
        assertShuffled(sql);
    }

    @Test
    public void fullOuterJoinThenAggregateUnderNotExistsShuffles() throws Exception {
        // NOT EXISTS keeps the NULL groups, so the NULL-padded key must still be merged across buckets although the
        // anti join relaxes the aggregate's NULL requirement.
        String sql = "select * from (select t.k1, count(*) c from t full outer join u on t.k1 = u.k1 group by t.k1) a "
                + "where not exists (select 1 from h where h.k1 = a.k1)";
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("FULL OUTER JOIN (COLOCATE)"), () -> "expected a colocate join:\n" + plan);
        Assertions.assertTrue(plan.contains("merge finalize"), () -> "expected a merged aggregate:\n" + plan);
        assertShuffled(sql);
    }

    @Test
    public void fullOuterJoinThenAggregateUnderShuffledAntiJoinShuffles() throws Exception {
        // A shuffled anti join relaxes its child's NULL requirement, but it keeps the NULL groups: the aggregate over
        // the NULL-padded key must still merge them across buckets.
        String sql = "select * from (select t.k1, count(*) c from t full outer join u on t.k1 = u.k1 group by t.k1) a "
                + "left anti join [shuffle] h on a.k1 = h.k1";
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("LEFT ANTI JOIN") && !plan.contains("LEFT ANTI JOIN (BROADCAST)"),
                () -> "expected a shuffled anti join:\n" + plan);
        Assertions.assertTrue(plan.contains("merge finalize"), () -> "expected a merged aggregate:\n" + plan);
        Assertions.assertFalse(scanFragment(plan).contains("update finalize"),
                () -> "expected no one-phase aggregate over the colocate join:\n" + plan);
    }

    @Test
    public void leftJoinPaddedSideRenamedThenAggregateUnderShuffledAntiJoinShuffles() throws Exception {
        // t.k1 stays null-strict, but the grouped alias of u.k1 is equivalent to it only null-relaxed, and it is NULL
        // for t's unmatched rows in every bucket.
        String sql = "select * from (select x, count(*) c from (select u.k1 as x from t left join u on t.k1 = u.k1) s "
                + "group by x) a left anti join [shuffle] h on a.x = h.k1";
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("LEFT OUTER JOIN (COLOCATE)"), () -> "expected a colocate join:\n" + plan);
        Assertions.assertTrue(plan.contains("LEFT ANTI JOIN") && !plan.contains("LEFT ANTI JOIN (BROADCAST)"),
                () -> "expected a shuffled anti join:\n" + plan);
        Assertions.assertTrue(plan.contains("merge finalize"), () -> "expected a merged aggregate:\n" + plan);
        Assertions.assertFalse(scanFragment(plan).contains("update finalize"),
                () -> "expected no one-phase aggregate over the colocate join:\n" + plan);
    }

    @Test
    public void leftJoinGroupedOnNullPaddedSideShuffles() throws Exception {
        // u.k1 is NULL for t's unmatched rows, which sit in every bucket.
        String sql = "select u.k1, count(*) from t left join u on t.k1 = u.k1 group by u.k1";
        Assertions.assertTrue(explain(sql).contains("COLOCATE"));
        assertShuffled(sql);
    }

    @Test
    public void rollupOverColocateColumnShuffles() throws Exception {
        // The subtotal rows set k1 to NULL for every bucket's rows, so they must be merged across buckets.
        assertShuffled("select k1, k2, count(*) from t group by rollup(k1, k2)");
    }

    @Test
    public void groupingSetsKeepingColocateColumnAreColocated() throws Exception {
        // Every grouping set keeps k1, so each group still lives in one bucket: both aggregate stages run in the scan
        // fragment with no shuffle between them.
        String sql = "select k1, k2, count(*) from t group by grouping sets ((k1, k2), (k1))";
        String plan = explain(sql);
        Assertions.assertTrue(scanFragment(plan).contains("REPEAT_NODE"), () -> "plan was:\n" + plan);
        Assertions.assertTrue(scanFragment(plan).contains("AGGREGATE (merge finalize)"), () -> "plan was:\n" + plan);
        Assertions.assertFalse(plan.contains("HASH_PARTITIONED"), () -> "expected no shuffle, plan was:\n" + plan);
        ExecutionFragment fragment = scheduledScanFragment(sql);
        Assertions.assertTrue(fragment.isColocated(), () -> "expected colocate dispatch, plan was:\n" + plan);
        Assertions.assertEquals(Set.of(0, 1), fragment.getColocatedAssignment().getSeqToWorkerId().keySet());
    }

    @Test
    public void windowOverColocateAggregateIsColocated() throws Exception {
        assertColocated("select k1, c, rank() over (partition by k1 order by c) "
                + "from (select k1, count(*) c from t group by k1) a", "ANALYTIC", Set.of(0, 1));
    }

    @Test
    public void countDistinctColocateColumnWithoutGroupBySchedules() throws Exception {
        // Each k1 value is deduplicated inside its bucket, then one instance counts the values.
        String sql = "select count(distinct k1) from t";
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("UNPARTITIONED"), () -> "expected a final gather:\n" + plan);
        UtFrameUtils.getPlanAndStartScheduling(connectContext, sql);
    }

    // A colocated aggregate still reports its input's range distribution, so the set operation or null-safe join
    // above it must put each such input behind a hash exchange, as it does for a bare range scan.

    @Test
    public void intersectOfColocateAggregatesShuffles() throws Exception {
        assertAggregatedThenExchanged("select k1 from t group by k1 intersect select k1 from u group by k1",
                "INTERSECT", "t", "u");
    }

    @Test
    public void intersectOfColocateAggregatesAcrossGroupsShuffles() throws Exception {
        // t has two ColocateRanges and l2 one; sharing one colocate assignment overran l2's bucket count.
        assertAggregatedThenExchanged("select k1 from t group by k1 intersect select k1 from l2 group by k1",
                "INTERSECT", "t", "l2");
    }

    @Test
    public void exceptOfColocateAggregatesAcrossGroupsShuffles() throws Exception {
        assertAggregatedThenExchanged("select k1 from t group by k1 except select k1 from l2 group by k1",
                "EXCEPT", "t", "l2");
    }

    @Test
    public void colocateAggregateNullSafeJoinWithHashTableShuffles() throws Exception {
        String sql = "select a.k1, a.c, h.v from (select k1, count(*) c from t group by k1) a "
                + "join [shuffle] h on a.k1 <=> h.k1";
        Assertions.assertTrue(explain(sql).contains("INNER JOIN (PARTITIONED)"),
                () -> "expected a partitioned join:\n" + sql);
        assertAggregatedThenExchanged(sql, "HASH JOIN", "t");
    }

    @Test
    public void unstableGroupShuffles() throws Exception {
        // A group that is unstable when the scan is planned gets no range spec at all, so the aggregate shuffles.
        // A group that turns unstable after the spec was built is rejected by the spec itself
        // (RangeDistributionSpecTest.isSatisfyHashShuffleAggRejectsUnstableGroup).
        // Keep the alignment checker from reacting to the unstable group while the test runs.
        new MockUp<ColocateChecker>() {
            @Mock
            public void runOneCycle() {
            }
        };
        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId = colocateIndex.getGroup(getTable("t").getId());
        colocateIndex.markGroupUnstable(groupId, false);
        try {
            assertShuffled("select k1, count(*) from t group by k1");
        } finally {
            colocateIndex.markGroupStable(groupId, false);
        }
    }

    @Test
    public void aggregateOverFullyPrunedScanPlans() throws Exception {
        // k2 > 5000 prunes every partition, which replaces the scan with an empty VALUES: planning and scheduling
        // must still succeed.
        String sql = "select k1, count(*) from t where k2 > 5000 group by k1";
        explain(sql);
        UtFrameUtils.getPlanAndStartScheduling(connectContext, sql);
    }

    @Test
    public void sortAggregateSkipsMultiTabletRangePartition() throws Exception {
        // l2's two tablets share k1 = 5, so a per-tablet sorted aggregate would emit that group once per tablet.
        String sql = "select /*+SET_VAR(enable_sort_aggregate=true)*/ k1, count(*) from l2 group by k1";
        String plan = explain(sql);
        Assertions.assertTrue(scanFragment(plan).contains("AGGREGATE (update finalize)"),
                () -> "expected a one-phase colocate aggregate, plan was:\n" + plan);
        Assertions.assertFalse(plan.contains("sorted streaming: true"),
                () -> "expected no sorted aggregate, plan was:\n" + plan);
    }

    @Test
    public void redundantTwoStageAggregateIsEliminated() throws Exception {
        // With cost-based stage merging off, only the cost model can drop the local stage: many rows per group make
        // the two-stage plan look cheaper, and it has no exchange to justify it.
        OlapTable ts = getTable("ts");
        PlanTestNoneDBBase.setTableStatistics(ts, 10_000_000L);
        GlobalStateMgr.getCurrentState().getStatisticStorage()
                .addColumnStatistic(ts, "k1", new ColumnStatistic(0, 999, 0.0, 4, 1000));
        assertColocated("select /*+SET_VAR(enable_cost_based_multi_stage_agg=false)*/ k1, count(*) from ts group by k1",
                "AGGREGATE (update finalize)", Set.of(0, 1));
    }
}
