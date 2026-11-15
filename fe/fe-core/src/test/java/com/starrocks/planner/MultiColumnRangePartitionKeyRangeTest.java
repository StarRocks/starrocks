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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TKeyRange;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The key ranges FE hands to BE for dynamic partition pruning must describe a superset of the values
 * that can live in the partition, because BE drops a tablet outright when the partition conjuncts
 * evaluate to false over the whole range.
 * <p>
 * A multi-column RANGE partition is an interval in the lexicographic order of the key tuple, so only
 * the leading column is bounded by the endpoints. p1 = [(10,10), (20,20)) holds (11, 0) and (19, 25):
 * id2 has no bound of its own until every column before it is pinned to a single value.
 */
public class MultiColumnRangePartitionKeyRangeTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_mcr").useDatabase("test_mcr")
                .withTable("create table t2r(id1 int not null, id2 int not null, name varchar(25) not null)"
                        + " duplicate key(id1) partition by range(id1, id2) ("
                        + "  partition p0 values [('0', '0'), ('10', '10')),"
                        + "  partition p1 values [('10', '10'), ('20', '20')),"
                        + "  partition p2 values [('20', '20'), ('30', '30')))"
                        + " distributed by hash(name) buckets 2 properties('replication_num' = '1');")
                .withTable("create table t2r_fixed(id1 int not null, id2 int not null, name varchar(25) not null)"
                        + " duplicate key(id1) partition by range(id1, id2) ("
                        + "  partition p0 values [('5', '0'), ('5', '10')),"
                        + "  partition p1 values [('5', '10'), ('5', '20')),"
                        + "  partition p2 values [('6', '0'), ('7', '0')))"
                        + " distributed by hash(name) buckets 2 properties('replication_num' = '1');")
                .withTable("create table t1r_max(id1 int not null, name varchar(25) not null)"
                        + " duplicate key(id1) partition by range(id1) ("
                        + "  partition p0 values [('-200'), ('-100')),"
                        + "  partition p1 values [('-100'), (maxvalue)))"
                        + " distributed by hash(name) buckets 2 properties('replication_num' = '1');")
                .withTable("create table t2r_null(id1 tinyint null, id2 tinyint not null, v int)"
                        + " duplicate key(id1) partition by range(id1, id2) ("
                        + "  partition p0 values less than ('-128', '10'),"
                        + "  partition p1 values less than ('50', '0'))"
                        + " distributed by hash(v) buckets 2 properties('replication_num' = '1');")
                .withTable("create table t2r_bigmax(id1 bigint not null, id2 int not null, v int)"
                        + " duplicate key(id1, id2) partition by range(id1, id2) ("
                        + "  partition p0 values [('9223372036854775806', '0'), ('9223372036854775807', '0')),"
                        + "  partition p1 values [('9223372036854775807', '0'), ('9223372036854775807', '100')))"
                        + " distributed by hash(v) buckets 1 properties('replication_num' = '1');")
                .withTable("create table t2r_nulltrail(id1 tinyint not null, id2 tinyint null, v int)"
                        + " duplicate key(id1, id2) partition by range(id1, id2) ("
                        + "  partition p0 values less than ('5'),"
                        + "  partition p1 values less than ('5', '10'))"
                        + " distributed by hash(v) buckets 2 properties('replication_num' = '1');")
                .withTable("create table t2r_dt(ts datetime not null, id int not null, v int)"
                        + " duplicate key(ts) partition by range(ts, id) ("
                        + "  partition p0 values [('2024-01-01 00:00:00', '0'), ('2024-01-01 00:00:00', '100')),"
                        + "  partition p1 values [('2024-01-02 00:00:00', '0'), ('2024-01-02 00:00:00', '100')))"
                        + " distributed by hash(v) buckets 2 properties('replication_num' = '1');");
    }

    @Test
    public void testNullPartitionPinsNothing() throws Exception {
        // p0 = [(MIN, MIN), (-128, 10)) is the NULL partition: (NULL, 100) lands in it because NULL
        // sorts below every id1, so id2 is unbounded there even though both endpoints read -128 on id1.
        Assertions.assertEquals(List.of(),
                keyRanges("t2r_null", "select v from test_mcr.t2r_null where id2 = 100"));
        Assertions.assertEquals(List.of("p0:id1:-128--128:null", "p1:id1:-128-50"),
                keyRanges("t2r_null", "select v from test_mcr.t2r_null where id1 <> 3 and id2 = 100"));
    }

    @Test
    public void testPinnedPrefixAtTheIntegerMaximumSendsNoRange() throws Exception {
        // BE materializes an integer range as `for (int64_t v = begin; v <= end; v++)`, which never
        // terminates when end is the int64 maximum, so such a range must not be sent - pinned or not.
        // p1 pins id1 to 9223372036854775807; id2 stays bounded because the prefix is still pinned.
        Assertions.assertEquals(List.of("p1:id2:0-100"),
                keyRanges("t2r_bigmax", "select v from test_mcr.t2r_bigmax where id1 > 0 and id2 < 5"));
        Assertions.assertEquals(List.of(),
                keyRanges("t2r_bigmax", "select v from test_mcr.t2r_bigmax where id1 <> 0"));
    }

    @Test
    public void testNullableTrailingColumnBehindAPinnedPrefix() throws Exception {
        // p1 = [(5, -128), (5, 10)) cannot hold (5, NULL): BE orders NULL below the type minimum, so
        // such a row lands in p0 = [(MIN, MIN), (5, -128)) instead. p1's id2 range is therefore
        // exact without a NULL of its own, while p0 is the one that carries has_null.
        // p0's own leading column spans [MIN, 5] and is not pinned, so p0 sends nothing at all.
        Assertions.assertEquals(List.of("p1:id2:-128-10"),
                keyRanges("t2r_nulltrail", "select v from test_mcr.t2r_nulltrail where id2 <> 3"));
    }

    @Test
    public void testPinnedLeadingColumnOfAnUnsupportedTypeStillBoundsTheTrailingColumn() throws Exception {
        // BE cannot enumerate a DATETIME, so ts gets no range of its own, but each partition pins it
        // and id stays bounded.
        Assertions.assertEquals(List.of("p0:id:0-100", "p1:id:0-100"),
                keyRanges("t2r_dt", "select v from test_mcr.t2r_dt where id > 500"));
        Assertions.assertEquals(List.of("p0:id:0-100", "p1:id:0-100"),
                keyRanges("t2r_dt", "select v from test_mcr.t2r_dt where ts <> '2024-01-05 00:00:00' and id > 500"));
    }

    @Test
    public void testOpenUpperBoundGetsNoRange() throws Exception {
        // MaxLiteral reads as 0 through getLongValue(), so a partition [-100, MAXVALUE) used to be
        // described as [-100, 0] and `id1 > 0` pruned every tablet of it. FE itself drops p0 here,
        // so p1 is the only partition left and it must carry no range.
        Assertions.assertEquals(List.of(),
                keyRanges("t1r_max", "select name from test_mcr.t1r_max where id1 > 0"));
        // `<>` keeps both partitions in the plan: the bounded one is described, the open one is not.
        Assertions.assertEquals(List.of("p0:id1:-200--100"),
                keyRanges("t1r_max", "select name from test_mcr.t1r_max where id1 <> -150"));
    }

    @Test
    public void testTrailingColumnGetsNoRangeWhenLeadingColumnIsNotFixed() throws Exception {
        // Before the fix p1 was described as id2 in [10, 20], and `id2 <= 8` pruned the tablet that
        // holds (11, 0) and (11, 1).
        Assertions.assertEquals(List.of(), keyRanges("t2r", "select name from test_mcr.t2r where id2 <= 8"),
                "id2 is unbounded inside every partition of t2r, so no range may be sent for it");
        Assertions.assertEquals(List.of(), keyRanges("t2r", "select name from test_mcr.t2r where id2 = 25"));
    }

    @Test
    public void testLeadingColumnKeepsItsRange() throws Exception {
        // `<>` is not something FE prunes range partitions by, so all three reach the plan.
        Assertions.assertEquals(List.of("p0:id1:0-10", "p1:id1:10-20", "p2:id1:20-30"),
                keyRanges("t2r", "select name from test_mcr.t2r where id1 <> 11"));
        // FE keeps only p1 for id1 = 11; of the two predicate columns only the leading one is bounded.
        Assertions.assertEquals(List.of("p1:id1:10-20"),
                keyRanges("t2r", "select name from test_mcr.t2r where id1 = 11 and id2 <= 8"));
    }

    @Test
    public void testTrailingColumnIsBoundedOnlyBehindAFixedPrefix() throws Exception {
        // p0 and p1 pin id1 to 5, so their id2 bounds are exact; p2 spans id1 in [6, 7] and says
        // nothing about id2. id1 is not in the predicate, so it gets no range of its own.
        Assertions.assertEquals(List.of("p0:id2:0-10", "p1:id2:10-20"),
                keyRanges("t2r_fixed", "select name from test_mcr.t2r_fixed where id2 <= 8"));
        // With id1 pinned FE can prune by id2 too and keeps only p0; it pins id1, so id2 is bounded.
        Assertions.assertEquals(List.of("p0:id1:5-5", "p0:id2:0-10"),
                keyRanges("t2r_fixed", "select name from test_mcr.t2r_fixed where id1 = 5 and id2 <= 8"));
        Assertions.assertEquals(List.of("p0:id1:5-5", "p0:id2:0-10", "p1:id1:5-5", "p1:id2:10-20"),
                keyRanges("t2r_fixed", "select name from test_mcr.t2r_fixed where id1 = 5 and id2 <> 8"));
        // p2 spans id1 in [6, 7], so nothing is said about id2 there.
        Assertions.assertEquals(List.of("p2:id1:6-7"),
                keyRanges("t2r_fixed", "select name from test_mcr.t2r_fixed where id1 = 6 and id2 <= 8"));
    }

    /** Every TKeyRange in the plan as "partition:column:begin-end[:null]", deduplicated and sorted. */
    private static List<String> keyRanges(String table, String sql) throws Exception {
        OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test_mcr", table);
        List<String> ranges = Lists.newArrayList();
        for (Pair<Long, TKeyRange> range : ScanRangeKeyRanges.collect(connectContext, sql)) {
            // The scan range names the physical partition; report the logical one it belongs to.
            PhysicalPartition physical = olapTable.getPhysicalPartition(range.first);
            String partition = olapTable.getPartition(physical.getParentId()).getName();
            TKeyRange keyRange = range.second;
            Assertions.assertTrue(keyRange.isSetBegin_key() && keyRange.isSetEnd_key(),
                    "a RANGE partition column range carries begin/end keys");
            ranges.add(partition + ":" + keyRange.getColumn_name() + ":" + keyRange.getBegin_key() + "-"
                    + keyRange.getEnd_key() + (keyRange.isHas_null() ? ":null" : ""));
        }
        // Each bucket repeats its partition's ranges; the claim is about the set.
        return ranges.stream().distinct().sorted().toList();
    }
}
