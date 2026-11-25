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

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Auto-mode aggregation push down below a join must not shuffle rows that the plan without it would not move, and
 * must keep the push downs that still reduce.
 */
public class PushDownAggregateLocalReductionTest extends PlanTestBase {
    private int oldMode;
    private int oldDop;
    private String oldPushDownAggregate;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getGlobalStateMgr().setStatisticStorage(new CachedStatisticStorage());
        // fact rows are distributed by id, so grouping by k or dk needs a shuffle
        starRocksAssert.withTable("CREATE TABLE pd_fact (id BIGINT, k INT, dk INT, sk INT) DUPLICATE KEY(id) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 8 PROPERTIES ('replication_num' = '1')");
        // fact rows distributed by dk: an aggregation grouped by dk finishes without a shuffle
        starRocksAssert.withTable("CREATE TABLE pd_fact_dk (id BIGINT, dk INT, sk INT) DUPLICATE KEY(id) " +
                "DISTRIBUTED BY HASH(dk) BUCKETS 8 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE pd_dim (k INT, n INT) DUPLICATE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE pd_small (k INT) DUPLICATE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
    }

    @BeforeEach
    public void before() {
        oldMode = connectContext.getSessionVariable().getCboPushDownAggregateMode();
        oldDop = connectContext.getSessionVariable().getPipelineDop();
        oldPushDownAggregate = connectContext.getSessionVariable().getCboPushDownAggregate();
        connectContext.getSessionVariable().setCboPushDownAggregateMode(0);
        connectContext.getSessionVariable().setPipelineDop(8);
    }

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(oldMode);
        connectContext.getSessionVariable().setPipelineDop(oldDop);
        connectContext.getSessionVariable().setCboPushDownAggregate(oldPushDownAggregate);
    }

    private static ColumnStatistic ndv(double ndv) {
        return ColumnStatistic.builder().setMinValue(1).setMaxValue(ndv).setNullsFraction(0)
                .setDistinctValuesCount(ndv).setAverageRowSize(4).build();
    }

    private void stats(String tableName, long rows, Object... columnNdvs) {
        OlapTable table = getOlapTable(tableName);
        StatisticStorage storage = connectContext.getGlobalStateMgr().getStatisticStorage();
        for (int i = 0; i < columnNdvs.length; i += 2) {
            storage.addColumnStatistic(table, (String) columnNdvs[i], ndv(((Number) columnNdvs[i + 1]).doubleValue()));
        }
        setTableStatistics(table, rows);
    }

    private static int aggNum(String plan) {
        return StringUtils.countMatches(plan, ":AGGREGATE (");
    }

    @Test
    public void testNoPushDownThatShufflesEveryRow() throws Exception {
        // 1M groups of k leave the local phase with as many rows as it read, so the pushed aggregation shuffles
        // all 600M rows, while the aggregation above the join reduces to 25 groups locally.
        stats("pd_fact", 600_000_000L, "id", 600_000_000L, "k", 1_000_000);
        stats("pd_dim", 1_000_000L, "k", 1_000_000, "n", 25);
        String plan = getFragmentPlan("select d.n, count(*) from pd_fact f join pd_dim d on f.k = d.k group by d.n");
        Assertions.assertEquals(2, aggNum(plan), plan);
    }

    @Test
    public void testPushDownWithFewGroupsBelowShuffleJoin() throws Exception {
        // 40K groups fit a small hash table: the local phase reduces ~25x before the join
        stats("pd_fact", 24_000_000L, "id", 24_000_000L, "k", 40_000);
        stats("pd_dim", 1_000_000L, "k", 1_000_000, "n", 25);
        String plan = getFragmentPlan(
                "select d.n, count(*) from pd_fact f join [shuffle] pd_dim d on f.k = d.k group by d.n");
        Assertions.assertTrue(aggNum(plan) > 2, plan);
    }

    @Test
    public void testPushDownAboveSmallBroadcastJoinKeepsDistribution() throws Exception {
        // Pushing to pd_fact_dk itself is refused by the small broadcast join NDV limit, so the aggregation lands on
        // `pd_fact_dk JOIN pd_small`, grouped by dk, which is the distribution of its rows: no shuffle is added.
        stats("pd_fact_dk", 600_000_000L, "id", 600_000_000L, "dk", 1_000_000, "sk", 10);
        stats("pd_small", 10L, "k", 10);
        stats("pd_dim", 1_000_000L, "k", 1_000_000, "n", 25);
        String plan = getFragmentPlan("select d.n, count(*) from pd_fact_dk f join [broadcast] pd_small s on f.sk = s.k " +
                "join [broadcast] pd_dim d on f.dk = d.k group by d.n");
        Assertions.assertTrue(aggNum(plan) > 2, plan);
    }

    @Test
    public void testPushDownBelowForcedShuffleJoin() throws Exception {
        // The hint shuffles all 600M fact rows anyway: the push down shuffles 1M groups of k instead, and saves the
        // join probe.
        stats("pd_fact", 600_000_000L, "id", 600_000_000L, "k", 1_000_000);
        stats("pd_dim", 1_000_000L, "k", 1_000_000, "n", 25);
        String plan = getFragmentPlan(
                "select d.n, count(*) from pd_fact f join [shuffle] pd_dim d on f.k = d.k group by d.n");
        Assertions.assertTrue(aggNum(plan) > 2, plan);
    }

    @Test
    public void testPushDownBelowJoinTooBigToBroadcast() throws Exception {
        // pd_dim is over the broadcast row limit, so the plan without the push down shuffles the fact rows too.
        stats("pd_fact", 600_000_000L, "id", 600_000_000L, "k", 1_000_000);
        stats("pd_dim", 100_000_000L, "k", 1_000_000, "n", 25);
        String plan = getFragmentPlan("select d.n, count(*) from pd_fact f join pd_dim d on f.k = d.k group by d.n");
        Assertions.assertTrue(aggNum(plan) > 2, plan);
    }

    @Test
    public void testLocalPushDownShufflesNothing() throws Exception {
        // a local-only pushed aggregation adds no shuffle, so the check that guards against one does not apply
        connectContext.getSessionVariable().setCboPushDownAggregate("local");
        stats("pd_fact", 600_000_000L, "id", 600_000_000L, "k", 1_000_000);
        stats("pd_dim", 1_000_000L, "k", 1_000_000, "n", 25);
        String plan = getFragmentPlan("select d.n, count(*) from pd_fact f join pd_dim d on f.k = d.k group by d.n");
        Assertions.assertTrue(aggNum(plan) > 2, plan);
    }

    @Test
    public void testCheckReductionOff() throws Exception {
        connectContext.getSessionVariable().setCboPushDownAggregateCheckReduction(false);
        try {
            stats("pd_fact", 600_000_000L, "id", 600_000_000L, "k", 1_000_000);
            stats("pd_dim", 1_000_000L, "k", 1_000_000, "n", 25);
            String plan = getFragmentPlan("select d.n, count(*) from pd_fact f join pd_dim d on f.k = d.k group by d.n");
            Assertions.assertTrue(aggNum(plan) > 2, plan);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregateCheckReduction(true);
        }
    }
}
