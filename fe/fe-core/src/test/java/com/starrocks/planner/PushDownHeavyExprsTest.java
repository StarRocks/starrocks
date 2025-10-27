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

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.util.Utility;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class PushDownHeavyExprsTest {
    protected static ConnectContext ctx;
    protected static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME).useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("test").useDatabase("test");
        Utility.getClickBenchCreateTableSqlList().forEach(createTblSql -> {
            try {
                starRocksAssert.withTable(createTblSql);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void test() throws Exception {
        String q =
                Utility.getClickBenchQueryList().stream().filter(p -> p.first.equals("Q29")).findFirst().get().second;
        String plan = UtFrameUtils.getFragmentPlan(ctx, q);
        Assertions.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: hits\n" + "     heavy exprs: \n" +
                "          <slot 106> : regexp_replace(15: Referer, '^https?://(?:www.)?([^/]+)/.*$', '1')"), plan);

        plan = UtFrameUtils.getVerboseFragmentPlan(ctx, q);
        System.out.println(plan);
        Assertions.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     table: hits, rollup: hits\n" +
                "     heavy exprs: \n" +
                "          106 <-> regexp_replace[([15: Referer, VARCHAR(65533), false], " +
                "'^https?://(?:www.)?([^/]+)/.*$', '1'); args: VARCHAR,VARCHAR,VARCHAR; " +
                "result: VARCHAR; args nullable: false; result nullable: true]\n"), plan);
    }
}