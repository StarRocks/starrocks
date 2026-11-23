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

package com.starrocks.load;

import com.google.common.collect.Sets;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.LakeTablet;
import com.starrocks.planner.HashDistributionPruner;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.DeleteAnalyzer;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

public class DeleteBucketPrunerTest {
    private static final String DB = "delete_bucket_prune_db";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster(true, RunMode.SHARED_DATA);
        GlobalStateMgr.getCurrentState().getTabletStatMgr().setStop();
        GlobalStateMgr.getCurrentState().getStarMgrMetaSyncer().setStop();
        for (ComputeNode node : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends()) {
            node.setAlive(true);
        }
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setThreadLocalInfo();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB)
                .withTable("CREATE TABLE t_bigint (k BIGINT, u BIGINT, v INT) DUPLICATE KEY(k, u) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 12")
                .withTable("CREATE TABLE t_int (k INT, v INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 7")
                .withTable("CREATE TABLE t_largeint (k LARGEINT, v INT) DUPLICATE KEY(k) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 7")
                .withTable("CREATE TABLE t_varchar (k VARCHAR(32), v INT) DUPLICATE KEY(k) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 7")
                .withTable("CREATE TABLE t_char (k CHAR(8), v INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 7")
                .withTable("CREATE TABLE t_date (k DATE, v INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 7")
                .withTable("CREATE TABLE t_datetime (k DATETIME, v INT) DUPLICATE KEY(k) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 7")
                .withTable("CREATE TABLE t_multi (k1 INT, k2 VARCHAR(16), v INT) DUPLICATE KEY(k1, k2) " +
                        "DISTRIBUTED BY HASH(k1, k2) BUCKETS 9")
                .withTable("CREATE TABLE t_agg (k1 INT, k2 BIGINT, v BIGINT SUM) AGGREGATE KEY(k1, k2) " +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 5")
                .withTable("CREATE TABLE t_random (k INT, v INT) DUPLICATE KEY(k) DISTRIBUTED BY RANDOM BUCKETS 4")
                .withTable("CREATE TABLE t_range (k INT, v INT) DUPLICATE KEY(k)")
                .withTable("CREATE TABLE t_part (dt DATE, k BIGINT, v INT) DUPLICATE KEY(dt, k) " +
                        "PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-02-01'), " +
                        "PARTITION p2 VALUES LESS THAN ('2024-03-01')) DISTRIBUTED BY HASH(k) BUCKETS 4");
        // A partition can have its own bucket number.
        starRocksAssert.alterTable("ALTER TABLE t_part ADD PARTITION p3 VALUES LESS THAN ('2024-04-01') " +
                "DISTRIBUTED BY HASH(k) BUCKETS 3");
    }

    private static OlapTable table(String name) {
        return (OlapTable) starRocksAssert.getTable(DB, name);
    }

    private static List<Predicate> conditions(String table, String where) throws Exception {
        DeleteStmt stmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(
                "DELETE FROM " + table + " WHERE " + where, connectContext);
        return DeleteAnalyzer.replaceParameterInExpr(stmt.getDeleteConditions());
    }

    private static List<MaterializedIndex> visibleIndexes(PhysicalPartition physicalPartition) {
        return physicalPartition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
    }

    /**
     * Tablets that the DELETE keeps, over every physical partition of the table, or null if a partition is not
     * pruned.
     */
    private static Set<Long> deleteTablets(String table, String where) throws Exception {
        OlapTable olapTable = table(table);
        DeleteBucketPruner pruner = DeleteBucketPruner.create(olapTable, conditions(table, where));
        if (pruner == null) {
            return null;
        }
        Set<Long> result = Sets.newHashSet();
        for (Partition partition : olapTable.getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                Set<Long> selected = pruner.selectTablets(partition, visibleIndexes(physicalPartition));
                if (selected == null) {
                    return null;
                }
                result.addAll(selected);
            }
        }
        return result;
    }

    /** Tablets that a query with the same predicate scans, which is how queries find the bucket of a value. */
    private static Set<Long> queryTablets(String table, String where) throws Exception {
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext,
                "SELECT * FROM " + table + " WHERE " + where).second;
        Set<Long> result = Sets.newHashSet();
        for (ScanNode scanNode : plan.getScanNodes()) {
            result.addAll(((OlapScanNode) scanNode).getScanTabletIds());
        }
        return result;
    }

    private static long onlyTabletOf(String table, String where) throws Exception {
        Set<Long> tablets = deleteTablets(table, where);
        Assertions.assertNotNull(tablets, where);
        Assertions.assertEquals(1, tablets.size(), where);
        return tablets.iterator().next();
    }

    private static List<Long> baseTablets(String table) {
        OlapTable olapTable = table(table);
        Partition partition = olapTable.getPartitions().iterator().next();
        return partition.getDefaultPhysicalPartition().getLatestBaseIndex().getTabletIdsInOrder();
    }

    /** The bucket in which BE stores a BIGINT value: CRC32 of its 8 little-endian bytes, modulo the bucket number. */
    private static int bigintBucket(long value, int bucketNum) {
        CRC32 crc = new CRC32();
        crc.update(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array());
        return (int) (crc.getValue() % bucketNum);
    }

    @Test
    public void testEqualPicksTheBucketOfTheValue() throws Exception {
        List<Long> tablets = baseTablets("t_bigint");
        for (long value : new long[] {0, 1, 7, 123, 123457, -5, Long.MAX_VALUE}) {
            String where = "k = " + value;
            long tablet = onlyTabletOf("t_bigint", where);
            Assertions.assertEquals(tablets.get(bigintBucket(value, 12)), tablet, where);
            Assertions.assertEquals(Set.of(tablet), queryTablets("t_bigint", where), where);
        }
    }

    @Test
    public void testOtherConditionsDoNotChangeTheBucket() throws Exception {
        String where = "k = 123457 AND u IN (1, 2, 3) AND v > 0";
        Assertions.assertEquals(Set.of(onlyTabletOf("t_bigint", "k = 123457")), deleteTablets("t_bigint", where));
        // a condition that does not bind the column only narrows the rows of the one that does
        Assertions.assertEquals(deleteTablets("t_bigint", "k IN (1, 2)"),
                deleteTablets("t_bigint", "k IN (1, 2) AND k != 2 AND k NOT IN (3)"));
    }

    @Test
    public void testInPicksTheUnionOfBuckets() throws Exception {
        String where = "k IN (1, 7, 123, 123457)";
        Set<Long> tablets = deleteTablets("t_bigint", where);
        Set<Long> expected = Sets.newHashSet();
        List<Long> all = baseTablets("t_bigint");
        for (long value : new long[] {1, 7, 123, 123457}) {
            expected.add(all.get(bigintBucket(value, 12)));
        }
        Assertions.assertEquals(expected, tablets);
        Assertions.assertEquals(queryTablets("t_bigint", where), tablets);
    }

    @Test
    public void testValueIsHashedAsTheColumnType() throws Exception {
        // The literal of `k = 5` is a TINYINT, and `k = '5'` a string; both must hash as the column type.
        long tablet = baseTablets("t_bigint").get(bigintBucket(5, 12));
        Assertions.assertEquals(tablet, onlyTabletOf("t_bigint", "k = 5"));
        Assertions.assertEquals(tablet, onlyTabletOf("t_bigint", "k = '5'"));
        Assertions.assertEquals(onlyTabletOf("t_int", "k = 1000"), onlyTabletOf("t_int", "k = '1000'"));
        Assertions.assertEquals(onlyTabletOf("t_date", "k = '2024-01-02'"),
                onlyTabletOf("t_date", "k = '2024-01-02 00:00:00'"));
        Assertions.assertEquals(onlyTabletOf("t_datetime", "k = '2024-01-02'"),
                onlyTabletOf("t_datetime", "k = '2024-01-02 00:00:00'"));
    }

    @Test
    public void testSameBucketsAsQueries() throws Exception {
        String[][] cases = {
                {"t_int", "k = 1000"},
                {"t_int", "k IN (-1, 0, 1, 2147483647)"},
                {"t_largeint", "k = 170141183460469231731687303715884105727"},
                {"t_largeint", "k IN (1, 2, 3)"},
                {"t_varchar", "k = 'abc'"},
                {"t_varchar", "k IN ('', 'a', 'xyz')"},
                {"t_date", "k = '2024-01-02'"},
                {"t_date", "k IN ('2024-01-02', '2025-12-31')"},
                {"t_datetime", "k = '2024-01-02 03:04:05'"},
                {"t_datetime", "k IN ('2024-01-02 03:04:05', '2024-01-02')"},
                {"t_multi", "k1 = 1 AND k2 = 'a'"},
                {"t_multi", "k1 IN (1, 2) AND k2 IN ('a', 'b')"},
                {"t_agg", "k2 = 9 AND k1 = 3"},
        };
        for (String[] c : cases) {
            Set<Long> tablets = deleteTablets(c[0], c[1]);
            Assertions.assertNotNull(tablets, c[1]);
            Assertions.assertTrue(tablets.size() < baseTablets(c[0]).size(), c[1]);
            Assertions.assertEquals(queryTablets(c[0], c[1]), tablets, c[1]);
        }
    }

    @Test
    public void testNotPrunedWhenAColumnIsNotBound() throws Exception {
        // one of two distribution columns
        Assertions.assertNull(deleteTablets("t_multi", "k1 = 1"));
        Assertions.assertNull(deleteTablets("t_multi", "k1 = 1 AND k2 > 'a'"));
        // not by = or IN
        Assertions.assertNull(deleteTablets("t_bigint", "k > 1"));
        Assertions.assertNull(deleteTablets("t_bigint", "k != 1"));
        Assertions.assertNull(deleteTablets("t_bigint", "k NOT IN (1, 2)"));
        Assertions.assertNull(deleteTablets("t_bigint", "k IS NULL"));
        Assertions.assertNull(deleteTablets("t_bigint", "u = 1"));
        // bound twice
        Assertions.assertNull(deleteTablets("t_bigint", "k = 1 AND k IN (1, 2)"));
    }

    @Test
    public void testNotPrunedOnNullValues() throws Exception {
        Assertions.assertNull(deleteTablets("t_bigint", "k = NULL"));
        Assertions.assertNull(deleteTablets("t_bigint", "k IN (1, NULL)"));
    }

    @Test
    public void testNotPrunedForOtherDistributionsAndTypes() throws Exception {
        Assertions.assertNull(deleteTablets("t_random", "k = 1"));
        Assertions.assertTrue(table("t_range").isRangeDistribution());
        Assertions.assertNull(deleteTablets("t_range", "k = 1"));
        Assertions.assertNull(deleteTablets("t_char", "k = 'abc'"));
    }

    @Test
    public void testNotPrunedWhenTableIsNotNormal() throws Exception {
        OlapTable olapTable = table("t_bigint");
        olapTable.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            Assertions.assertNull(deleteTablets("t_bigint", "k = 1"));
        } finally {
            olapTable.setState(OlapTable.OlapTableState.NORMAL);
        }
    }

    @Test
    public void testBucketNumberOfEachPartition() throws Exception {
        OlapTable olapTable = table("t_part");
        DeleteBucketPruner pruner = DeleteBucketPruner.create(olapTable, conditions("t_part", "k = 123457"));
        Assertions.assertNotNull(pruner);
        for (String name : new String[] {"p1", "p2", "p3"}) {
            Partition partition = olapTable.getPartition(name);
            MaterializedIndex index = partition.getDefaultPhysicalPartition().getLatestBaseIndex();
            List<Long> tablets = index.getTabletIdsInOrder();
            Set<Long> selected = pruner.selectTablets(partition, List.of(index));
            Assertions.assertEquals(Set.of(tablets.get(bigintBucket(123457, tablets.size()))), selected, name);
        }
        Assertions.assertEquals(3, olapTable.getPartition("p3").getDefaultPhysicalPartition().getLatestBaseIndex()
                .getTablets().size());
        Assertions.assertEquals(queryTablets("t_part", "k = 123457"), deleteTablets("t_part", "k = 123457"));
    }

    @Test
    public void testSameBucketInEveryIndex() throws Exception {
        OlapTable olapTable = table("t_bigint");
        Partition partition = olapTable.getPartitions().iterator().next();
        MaterializedIndex base = partition.getDefaultPhysicalPartition().getLatestBaseIndex();
        DeleteBucketPruner pruner = DeleteBucketPruner.create(olapTable, conditions("t_bigint", "k = 123"));
        Assertions.assertNotNull(pruner);

        // A rollup has one tablet per bucket, in bucket order, like its base index.
        MaterializedIndex rollup = newIndex(base.getTablets().size());
        int bucket = bigintBucket(123, 12);
        Set<Long> selected = pruner.selectTablets(partition, List.of(base, rollup));
        Assertions.assertEquals(Set.of(base.getTabletIdsInOrder().get(bucket), rollup.getTabletIdsInOrder().get(bucket)),
                selected);

        // Do not prune indexes whose tablets do not follow the buckets.
        Assertions.assertNull(pruner.selectTablets(partition, List.of(base, newIndex(base.getTablets().size() - 1))));
        Assertions.assertNull(pruner.selectTablets(partition, List.of(base, newIndex(0))));
    }

    @Test
    public void testLongInListIsNotPruned() throws Exception {
        int saved = Config.max_distribution_pruner_recursion_depth;
        Config.max_distribution_pruner_recursion_depth = 2;
        try {
            Collection<Long> tablets = deleteTablets("t_bigint", "k IN (1, 2, 3)");
            Assertions.assertEquals(Set.copyOf(baseTablets("t_bigint")), tablets);
        } finally {
            Config.max_distribution_pruner_recursion_depth = saved;
        }
    }

    @Test
    public void testNotPrunedWhenPruningFails() throws Exception {
        new MockUp<HashDistributionPruner>() {
            @Mock
            public Collection<Long> prune() {
                throw new IllegalStateException("injected");
            }
        };
        Assertions.assertNull(deleteTablets("t_bigint", "k = 1"));
    }

    private static MaterializedIndex newIndex(int tabletNum) {
        MaterializedIndex index = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                MaterializedIndex.IndexState.NORMAL);
        for (int i = 0; i < tabletNum; i++) {
            index.addTablet(new LakeTablet(GlobalStateMgr.getCurrentState().getNextId()), null, false);
        }
        return index;
    }
}
