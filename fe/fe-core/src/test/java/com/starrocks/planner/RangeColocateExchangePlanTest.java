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
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
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
 * A range-distributed table that feeds a set operation, or a join it cannot colocate with, must reach that operator
 * through an exchange on the operator's keys. A range scan natively satisfies a hash SHUFFLE_JOIN requirement on its
 * colocate columns, so an exchange enforced under that same property loses to the bare scan when the plan is
 * extracted; INTERSECT / EXCEPT and null-safe ({@code <=>}) joins, whose keys are null-strict, then run over data that
 * is not partitioned on their keys.
 *
 * <p>Fixture: {@code t} and {@code u} share a range-colocate group with two ColocateRanges (so each of their two
 * partitions has two tablets), {@code w} and {@code w2} share a single-range group, {@code h} and {@code h2} are
 * hash-distributed over two partitions, {@code hl} is a single-partition hash table. Statistics on {@code t} and
 * {@code h} make the optimizer consider a bucket-shuffle join instead of broadcasting.
 */
public class RangeColocateExchangePlanTest {
    private static final String DB = "range_colocate_exchange";
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

        starRocksAssert.withDatabase(DB).useDatabase(DB);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);

        // Creates group rex_grp with the single ColocateRange [MIN, MAX); splitting the group leaves this table
        // unaligned, so it is dropped once the aligned tables exist.
        starRocksAssert.withTable("create table seed (k1 int, k2 int, v int) order by(k1, k2) "
                + "properties('replication_num' = '1', 'colocate_with' = 'rex_grp:k1')");
        OlapTable seed = getTable("seed");
        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateIndex.getGroup(seed.getId()).grpId;
        long lowPackGroup = colocateIndex.getColocateRangeMgr().getColocateRanges(grpId).get(0).getShardGroupId();
        long highPackGroup = GlobalStateMgr.getCurrentState().getStarOSAgent()
                .createShardGroup(db.getId(), seed.getId(), 0L, 0L, PlacementPolicy.PACK);
        colocateIndex.getColocateRangeMgr().setColocateRanges(grpId, List.of(
                new ColocateRange(Range.lt(intTuple(100)), lowPackGroup),
                new ColocateRange(Range.ge(intTuple(100)), highPackGroup)));
        for (String name : List.of("t", "u")) {
            starRocksAssert.withTable("create table " + name + " (k1 int, k2 int, v int) duplicate key(k1, k2) "
                    + TWO_PARTITIONS + "order by(k1, k2) "
                    + "properties('replication_num' = '1', 'colocate_with' = 'rex_grp:k1')");
        }
        // FORCE: a plain drop parks the table in the recycle bin, where it stays a group member.
        starRocksAssert.dropTable("seed force");

        starRocksAssert.withTable("create table w (k1 int, k2 int, v int) duplicate key(k1, k2) " + TWO_PARTITIONS
                + "order by(k1, k2) properties('replication_num' = '1', 'colocate_with' = 'rex_grp2:k1')");
        starRocksAssert.withTable("create table w2 (k1 int, k2 int, v int) duplicate key(k1, k2) " + TWO_PARTITIONS
                + "order by(k1, k2) properties('replication_num' = '1', 'colocate_with' = 'rex_grp2:k1')");
        starRocksAssert.withTable("create table h (k1 int, k2 int, v int) duplicate key(k1, k2) " + TWO_PARTITIONS
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')");
        // No statistics: the [shuffle] hint, not the cost model, forces the null-safe join to shuffle.
        starRocksAssert.withTable("create table h2 (k1 int, k2 int, v int) duplicate key(k1, k2) " + TWO_PARTITIONS
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')");
        // Single partition: its hash-LOCAL distribution satisfies a set operation's requirement.
        starRocksAssert.withTable("create table hl (k1 int, k2 int, v int) duplicate key(k1, k2) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')");

        for (String name : List.of("t", "h")) {
            OlapTable table = getTable(name);
            PlanTestNoneDBBase.setTableStatistics(table, 100_000_000L);
            GlobalStateMgr.getCurrentState().getStatisticStorage()
                    .addColumnStatistic(table, "k1", new ColumnStatistic(0, 1e7, 0.0, 4, 1e7));
        }
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
    }

    private static Tuple intTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    private static OlapTable getTable(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB, name);
    }

    private static String explain(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.NORMAL);
    }

    // Exactly one fragment runs `operator`, it scans none of `rangeTables`, and each of those tables is scanned in a
    // fragment that sends its rows through a hash exchange on k1 (HASH_PARTITIONED or BUCKET_SHUFFLE_HASH_PARTITIONED).
    private static void assertRangeTablesExchanged(String plan, String operator, String... rangeTables) {
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
            Assertions.assertTrue(scanFragments.get(0).matches("(?s).*HASH_PARTITIONED: \\d+: k1.*"),
                    () -> table + " does not reach " + operator + " through a hash exchange on k1:\n" + plan);
        }
    }

    // Every tablet of each of `tables`, and only of those, is assigned to some fragment instance.
    private static void assertAllTabletsScheduled(String sql, String... tables) throws Exception {
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
        Assertions.assertEquals(Set.of(tables), scheduled.keySet(), () -> "scanned tables in: " + sql);
        for (String name : tables) {
            int tablets = getTable(name).getPhysicalPartitions().stream()
                    .mapToInt(partition -> partition.getLatestBaseIndex().getTablets().size()).sum();
            Assertions.assertEquals(tablets, scheduled.get(name), () -> "scan ranges scheduled for " + name + " in: " + sql);
        }
    }

    @Test
    public void intersectOfRangeTablesShuffles() throws Exception {
        // Same colocate group; without the exchange each instance intersected one table with nothing.
        String sql = "select k1 from t intersect select k1 from u";
        assertRangeTablesExchanged(explain(sql), "INTERSECT", "t", "u");
        assertAllTabletsScheduled(sql, "t", "u");
    }

    @Test
    public void intersectInSingleRangeGroupShuffles() throws Exception {
        // One ColocateRange is enough to lose the exchange: the failure does not depend on the range layout.
        String sql = "select k1 from w intersect select k1 from w2";
        assertRangeTablesExchanged(explain(sql), "INTERSECT", "w", "w2");
        assertAllTabletsScheduled(sql, "w", "w2");
    }

    @Test
    public void exceptAcrossRangeGroupsShuffles() throws Exception {
        String sql = "select k1 from t except select k1 from w";
        assertRangeTablesExchanged(explain(sql), "EXCEPT", "t", "w");
        assertAllTabletsScheduled(sql, "t", "w");
    }

    @Test
    public void intersectOfHashThenRangeShuffles() throws Exception {
        String sql = "select k1 from h intersect select k1 from t";
        assertRangeTablesExchanged(explain(sql), "INTERSECT", "t");
        assertAllTabletsScheduled(sql, "h", "t");
    }

    @Test
    public void intersectOfLocalHashThenRangeBucketShuffles() throws Exception {
        // A hash-LOCAL first child stays; the range child is re-exchanged into its buckets.
        String sql = "select k1 from hl intersect select k1 from t";
        String plan = explain(sql);
        assertRangeTablesExchanged(plan, "INTERSECT", "t");
        List<String> fragments = Arrays.asList(plan.split("PLAN FRAGMENT "));
        List<String> tScanFragments = fragments.stream().filter(fragment -> fragment.contains("TABLE: t\n")).toList();
        Assertions.assertEquals(1, tScanFragments.size(), () -> "expected one scan of t:\n" + plan);
        Assertions.assertTrue(tScanFragments.get(0).contains("BUCKET_SHUFFLE_HASH_PARTITIONED"),
                () -> "expected a bucket shuffle of t, plan was:\n" + plan);
        assertAllTabletsScheduled(sql, "hl", "t");
    }

    @Test
    public void rangeJoinHashNullSafeShuffles() throws Exception {
        // A range table can never be the stay side of a bucket shuffle: hash re-bucketing does not follow its layout.
        String sql = "select * from t join h on t.k1 <=> h.k1";
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("INNER JOIN (PARTITIONED)"), () -> "expected PARTITIONED, plan was:\n" + plan);
        assertRangeTablesExchanged(plan, "HASH JOIN", "t");
        assertAllTabletsScheduled(sql, "t", "h");
    }

    @Test
    public void rangeJoinHashNullSafeWithShuffleHintShuffles() throws Exception {
        // Without statistics the [shuffle] hint forces the same path.
        String sql = "select * from u join [shuffle] h2 on u.k1 <=> h2.k1";
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("INNER JOIN (PARTITIONED)"), () -> "expected PARTITIONED, plan was:\n" + plan);
        assertRangeTablesExchanged(plan, "HASH JOIN", "u");
        assertAllTabletsScheduled(sql, "u", "h2");
    }

    @Test
    public void rangeLeftJoinHashNullSafeShuffles() throws Exception {
        String sql = "select * from t left join h on t.k1 <=> h.k1";
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("LEFT OUTER JOIN (PARTITIONED)"),
                () -> "expected PARTITIONED, plan was:\n" + plan);
        assertRangeTablesExchanged(plan, "HASH JOIN", "t");
        assertAllTabletsScheduled(sql, "t", "h");
    }

    @Test
    public void hashJoinRangeNullSafeShuffles() throws Exception {
        String sql = "select * from h join t on h.k1 <=> t.k1";
        assertRangeTablesExchanged(explain(sql), "HASH JOIN", "t");
        assertAllTabletsScheduled(sql, "h", "t");
    }

    @Test
    public void rangeJoinLocalHashNullSafeShuffles() throws Exception {
        // hl is tiny and would otherwise be broadcast; [bucket] forces the shuffle-based dispatch (isShuffleLike
        // left, isLocal right) without taking the [shuffle]/[skew] early-return branch.
        String sql = "select * from t join [bucket] hl on t.k1 <=> hl.k1";
        assertRangeTablesExchanged(explain(sql), "HASH JOIN", "t");
        assertAllTabletsScheduled(sql, "t", "hl");
    }

    @Test
    public void localHashJoinRangeNullSafeShuffles() throws Exception {
        // [bucket] forces the shuffle-based dispatch (isLocal left, isShuffleLike right).
        String sql = "select * from hl join [bucket] t on hl.k1 <=> t.k1";
        assertRangeTablesExchanged(explain(sql), "HASH JOIN", "t");
        assertAllTabletsScheduled(sql, "hl", "t");
    }

    @Test
    public void rangeJoinHashEqualsUnchanged() throws Exception {
        String sql = "select * from t join h on t.k1 = h.k1";
        String plan = explain(sql);
        Assertions.assertTrue(plan.contains("INNER JOIN (PARTITIONED)"), () -> "expected PARTITIONED, plan was:\n" + plan);
        assertAllTabletsScheduled(sql, "t", "h");
    }

    @Test
    public void unionDistinctOfRangeTablesPlans() throws Exception {
        String sql = "select k1 from t union select k1 from u";
        String plan = explain(sql);
        List<String> fragments = Arrays.asList(plan.split("PLAN FRAGMENT "));
        List<String> unionFragments = fragments.stream().filter(fragment -> fragment.contains(":UNION")).toList();
        Assertions.assertEquals(1, unionFragments.size(), () -> "expected one UNION fragment, plan was:\n" + plan);
        for (String table : List.of("t", "u")) {
            String scan = "TABLE: " + table + "\n";
            Assertions.assertFalse(unionFragments.get(0).contains(scan),
                    () -> table + " is scanned in the UNION fragment, plan was:\n" + plan);
            // UNION DISTINCT keeps its round-robin inputs.
            List<String> scanFragments = fragments.stream().filter(fragment -> fragment.contains(scan)).toList();
            Assertions.assertEquals(1, scanFragments.size(), () -> "expected one scan of " + table + ":\n" + plan);
            Assertions.assertTrue(scanFragments.get(0).matches("(?s).*EXCHANGE ID: \\d+\\s+RANDOM.*"),
                    () -> table + " does not reach the UNION through a round-robin exchange:\n" + plan);
        }
        assertAllTabletsScheduled(sql, "t", "u");
    }

    @Test
    public void rangeColocateJoinStaysColocate() throws Exception {
        String plan = explain("select * from t join u on t.k1 = u.k1");
        Assertions.assertTrue(plan.contains("INNER JOIN (COLOCATE)"), () -> "expected COLOCATE, plan was:\n" + plan);
    }
}
