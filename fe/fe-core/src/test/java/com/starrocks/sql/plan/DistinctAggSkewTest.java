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

import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DistinctAggSkewTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;

        connectContext.getGlobalStateMgr().setStatisticStorage(new CachedStatisticStorage());

        starRocksAssert.withTable("CREATE TABLE `null_skew_table` (\n" +
                "  `id` bigint NULL COMMENT \"\",\n" +
                "  `group` bigint NULL COMMENT \"\",\n" +
                "  `value` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`, `group`, `value`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    void testDistinctWithNullSkew() throws Exception {
        String sql = "select `group`, count(distinct value) as cnt from null_skew_table group by `group`";

        final var skewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.8).build();
        final var nonSkewedColumnStat = ColumnStatistic.builder().setNullsFraction(0.01).build();

        final var table = getOlapTable("null_skew_table");

        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        statisticStorage.addColumnStatistic(table, "group", skewedColumnStat);
        statisticStorage.addColumnStatistic(table, "value", nonSkewedColumnStat);

        connectContext.getSessionVariable().setEnableDistinctColumnBucketization(true);
        setTableStatistics(table, 1337);

        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 2> : 2: group\n" +
                "  |  <slot 3> : 3: value\n" +
                "  |  <slot 5> : CAST(murmur_hash3_32(CAST(3: value AS VARCHAR)) % 512 AS SMALLINT)");
    }

    @Test
    void testDistinctWithoutNullSkew() throws Exception {
        String sql = "select `group`, count(distinct value) as cnt from null_skew_table group by `group`";

        final var nonSkewedColumnStat = ColumnStatistic.unknown();

        final var table = getOlapTable("null_skew_table");

        final var statisticStorage = connectContext.getGlobalStateMgr().getStatisticStorage();
        statisticStorage.addColumnStatistic(table, "group", nonSkewedColumnStat);
        statisticStorage.addColumnStatistic(table, "value", nonSkewedColumnStat);

        connectContext.getSessionVariable().setEnableDistinctColumnBucketization(true);
        setTableStatistics(table, 1337);

        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "  2:Project\n" +
                "  |  <slot 2> : 2: group\n" +
                "  |  <slot 3> : 3: value\n" +
                "  |  <slot 5> : CAST(murmur_hash3_32(CAST(3: value AS VARCHAR)) % 512 AS SMALLINT)");
    }
}
