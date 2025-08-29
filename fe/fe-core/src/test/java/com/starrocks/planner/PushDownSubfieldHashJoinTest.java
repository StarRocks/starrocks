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
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class PushDownSubfieldHashJoinTest {
    private static ConnectContext ctx = null;
    private static StarRocksAssert starRocksAssert = null;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        ctx.getSessionVariable().setCboPushDownAggregateMode(-1);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("test_db").useDatabase("test_db");
        String tableA = "create table A (fk largeint(40), col_int bigint(20))properties('replication_num'='1');";
        String tableB = "create table B (fk largeint(40), id varchar(65535))properties('replication_num'='1');";
        starRocksAssert.withTable(tableA);
        starRocksAssert.withTable(tableB);
    }

    @Test
    public void test() throws Exception {
        String sql = "WITH T0 AS (\n" +
                "  SELECT \n" +
                "    A.col_int AS col_int, \n" +
                "    B.id AS id \n" +
                "  FROM \n" +
                "    A \n" +
                "    LEFT JOIN B ON (A.fk = B.fk)\n" +
                "), \n" +
                "T1 AS (\n" +
                "  SELECT \n" +
                "    (\n" +
                "      (\n" +
                "        T0.col_int = CAST(1 AS BIGINT)\n" +
                "      ) \n" +
                "      AND NOT(\n" +
                "        (\n" +
                "          (T0.id) IS NULL\n" +
                "        )\n" +
                "      )\n" +
                "    ) AS col1 \n" +
                "  FROM \n" +
                "    T0\n" +
                "), \n" +
                "T2 AS (\n" +
                "  SELECT \n" +
                "    ARRAY_FILTER([ \"A\", \"B\" ], [0, T1.col1]) AS col2 \n" +
                "  FROM \n" +
                "    T1\n" +
                "), \n" +
                "T3 AS (\n" +
                "  SELECT \n" +
                "    (\n" +
                "      CASE ARRAY_LENGTH(T2.col2) WHEN 0 THEN [ \"C\" ] ELSE T2.col2 END\n" +
                "    ) AS col3 \n" +
                "  FROM \n" +
                "    T2\n" +
                "), \n" +
                "T4 AS (\n" +
                "  SELECT \n" +
                "    ARRAY_SUM(\n" +
                "      ARRAY_MAP(\n" +
                "        (arg_22508)->(\n" +
                "          (`arg_22508` <> \"A\")\n" +
                "        ), \n" +
                "        T3.col3\n" +
                "      )\n" +
                "    ) AS col4 \n" +
                "  FROM \n" +
                "    T3\n" +
                ") \n" +
                "SELECT \n" +
                "  T4.col4 \n" +
                "FROM \n" +
                "  T4 \n" +
                "WHERE \n" +
                "  T4.col4;";
        String plan = UtFrameUtils.getFragmentPlan(ctx, sql);
        Assertions.assertTrue(plan.contains("  4:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: fk = 1: fk\n" +
                "  |  other predicates: CAST(array_sum(array_map(<slot 8> -> <slot 8> != 'A', " +
                "if(array_length(26: array_filter) = 0, ['C'], 26: array_filter))) AS BOOLEAN)\n" +
                "  |    common sub expr:\n" +
                "  |    <slot 20> : 2: col_int = 1\n" +
                "  |    <slot 21> : 4: id IS NOT NULL\n" +
                "  |    <slot 22> : (20: expr) AND (21: expr)\n" +
                "  |    <slot 23> : CAST(22: expr AS TINYINT)\n" +
                "  |    <slot 24> : [0,CAST((2: col_int = 1) AND (4: id IS NOT NULL) AS TINYINT)]\n" +
                "  |    <slot 25> : CAST([0,CAST((2: col_int = 1) AND (4: id IS NOT NULL) AS TINYINT)] AS ARRAY<BOOLEAN>)\n" +
                "  |    <slot 26> : array_filter(['A','B'], 25: cast)"));

    }
}