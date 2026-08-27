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

import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A range-distributed table must never be the local (stay) side of a bucket-shuffle
 * join: bucket-shuffle re-buckets the other side with crc32, which does not match the
 * range tablet layout, so all matches are silently dropped. This surfaced for a
 * null-safe equi-join ({@code a <=> b}) — whose required distribution is null-strict —
 * where the memo reused the un-exchanged range scan as a {@code SHUFFLE_HASH_BUCKET}
 * stay side. A range/range join that cannot colocate must be PARTITIONED.
 *
 * <p>Genuine range colocate joins ({@code disable_colocate_join=false}, same stable
 * group, colocate key) must still plan as COLOCATE for {@code <=>} as well as {@code =}.
 */
public class RangeColocateNullSafeJoinPlanTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistributionConfig;
    private static boolean savedEnableRangeDistributionSessionVar;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        savedEnableRangeDistributionConfig = Config.enable_range_distribution;
        savedEnableRangeDistributionSessionVar = connectContext.getSessionVariable().isEnableRangeDistribution();
        Config.enable_range_distribution = true;
        connectContext.getSessionVariable().setEnableRangeDistribution(true);

        starRocksAssert.withDatabase("range_null_safe_join").useDatabase("range_null_safe_join");
        // two range-colocate tables in the same group
        starRocksAssert.withTable("create table cl (k1 int, v int) order by(k1) "
                + "properties('replication_num' = '1', 'colocate_with' = 'rns_grp:k1');");
        starRocksAssert.withTable("create table cr (k1 int, v int) order by(k1) "
                + "properties('replication_num' = '1', 'colocate_with' = 'rns_grp:k1');");
        // a range-colocate table in a different group
        starRocksAssert.withTable("create table dg (k1 int, v int) order by(k1) "
                + "properties('replication_num' = '1', 'colocate_with' = 'rns_grp_other:k1');");
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_range_distribution = savedEnableRangeDistributionConfig;
        if (connectContext != null) {
            connectContext.getSessionVariable().setEnableRangeDistribution(savedEnableRangeDistributionSessionVar);
        }
    }

    @BeforeEach
    public void resetSessionState() {
        Deencapsulation.setField(connectContext.getSessionVariable(), "disableColocateJoin", false);
    }

    private String plan(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.NORMAL);
    }

    private void assertPartitionedNotBucketShuffle(String sql) throws Exception {
        String p = plan(sql);
        Assertions.assertTrue(p.contains("(PARTITIONED)"),
                () -> "expected PARTITIONED join, plan was:\n" + p);
        Assertions.assertFalse(p.contains("BUCKET_SHUFFLE"),
                () -> "range/range join that cannot colocate must not be a bucket-shuffle, plan was:\n" + p);
        Assertions.assertFalse(p.contains("colocate: true"),
                () -> "expected non-colocate join, plan was:\n" + p);
    }

    private void assertColocate(String sql) throws Exception {
        String p = plan(sql);
        Assertions.assertTrue(p.contains("COLOCATE"),
                () -> "expected COLOCATE join, plan was:\n" + p);
    }

    // ---- the bug: range/range <=> that cannot colocate must be PARTITIONED, not bucket-shuffle ----

    @Test
    public void fullOuterNullSafeDisableColocate() throws Exception {
        Deencapsulation.setField(connectContext.getSessionVariable(), "disableColocateJoin", true);
        assertPartitionedNotBucketShuffle("select * from cl full outer join cr on cl.k1 <=> cr.k1");
    }

    @Test
    public void innerNullSafeDisableColocate() throws Exception {
        Deencapsulation.setField(connectContext.getSessionVariable(), "disableColocateJoin", true);
        assertPartitionedNotBucketShuffle("select * from cl join cr on cl.k1 <=> cr.k1");
    }

    @Test
    public void leftOuterNullSafeDisableColocate() throws Exception {
        Deencapsulation.setField(connectContext.getSessionVariable(), "disableColocateJoin", true);
        assertPartitionedNotBucketShuffle("select * from cl left join cr on cl.k1 <=> cr.k1");
    }

    @Test
    public void fullOuterNullSafeDifferentGroup() throws Exception {
        // different colocate groups cannot colocate even with disable_colocate_join off
        assertPartitionedNotBucketShuffle("select * from cl full outer join dg on cl.k1 <=> dg.k1");
    }

    // ---- control: plain '=' already partitions correctly in the same situations ----

    @Test
    public void fullOuterEqualsDisableColocate() throws Exception {
        Deencapsulation.setField(connectContext.getSessionVariable(), "disableColocateJoin", true);
        assertPartitionedNotBucketShuffle("select * from cl full outer join cr on cl.k1 = cr.k1");
    }

    // ---- genuine range colocate <=> must still be COLOCATE (optimization preserved) ----

    @Test
    public void fullOuterNullSafeSameGroupStaysColocate() throws Exception {
        assertColocate("select * from cl full outer join cr on cl.k1 <=> cr.k1");
    }

    @Test
    public void innerNullSafeSameGroupStaysColocate() throws Exception {
        assertColocate("select * from cl join cr on cl.k1 <=> cr.k1");
    }
}
