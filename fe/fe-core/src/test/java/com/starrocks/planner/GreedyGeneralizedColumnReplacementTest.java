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
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GreedyGeneralizedColumnReplacementTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert createStarRocksAssert() {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        FeConstants.runningUnitTest = true;
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        String t0 = "CREATE table t0 (\n" +
                "  col_1 varchar(1048576) NULL COMMENT \"\",\n" +
                "  col_2 varchar(1048576) NULL COMMENT \"\",\n" +
                "  eventTime varchar(1048576) NULL COMMENT \"\",\n" +
                "  col_4 varchar(1048576) NULL COMMENT \"\",\n" +
                "  col_5 varchar(1048576) NULL COMMENT \"\",\n" +
                "  col_6 double NULL COMMENT \"\",\n" +
                "  site varchar(10) NULL COMMENT \"\",\n" +
                "  eventTs datetime AS str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'),\n" +
                "  localEventTs datetime AS CASE \n" +
                "    WHEN site IN ('MY', 'SG', 'PH') " +
                "    THEN hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 8) \n" +
                "    WHEN site IN ('TH', 'VN', 'KH', 'ID') " +
                "    THEN hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 7) \n" +
                "    WHEN site = 'MM' " +
                "    THEN minutes_add(hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 6), 30) \n" +
                "    ELSE str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000') \n" +
                "  END\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(col_1)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY time_slice(eventTs, interval 1 day, FLOOR)\n" +
                "DISTRIBUTED BY HASH(col_5)\n" +
                "PROPERTIES (\n" +
                "  \"compression\" = \"LZ4\",\n" +
                "  \"replication_num\" = \"1\",\n" +
                "  \"time_drift_constraint\" = \"localEventTs between DAYS_ADD(eventTs, -2) and DAYS_ADD(eventTs, 2)\"\n" +
                ");";
        try {
            starRocksAssert.withDatabase("test_db").useDatabase("test_db");
            starRocksAssert.withTable(t0);
        } catch (Throwable ignored) {
            Assertions.fail();
        }
        return starRocksAssert;
    }

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        FeConstants.enablePruneEmptyOutputScan = false;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(createStarRocksAssert());
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeAll
    public static void setUp() throws Exception {
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        UtFrameUtils.mockTimelinessForAsyncMVTest(starRocksAssert.getCtx());
    }

    @Test
    void testGeneralizedColumn1() throws Exception {
        String q = "SELECT *\n" +
                "FROM\n" +
                "\tt0\n" +
                "WHERE\n" +
                "\tDATE_TRUNC('hour', CASE \n" +
                "\tWHEN site IN ('MY', 'SG', 'PH') " +
                "\tTHEN hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 8) \n" +
                "\tWHEN site IN ('TH', 'VN', 'KH', 'ID') " +
                "\tTHEN hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 7) \n" +
                "\tWHEN site = 'MM' " +
                "\tTHEN minutes_add(hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 6), 30) \n" +
                "\tELSE str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000') \n" +
                "\tEND ) >= STR_TO_DATE('2024-07-01 00:00:00', '%Y-%m-%d %H:%i:%S')\n" +
                "\tAND DATE_TRUNC('hour',CASE \n" +
                "\tWHEN site IN ('MY', 'SG', 'PH') " +
                "\tTHEN hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 8) \n" +
                "\tWHEN site IN ('TH', 'VN', 'KH', 'ID') " +
                "\tTHEN hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 7) \n" +
                "\tWHEN site = 'MM' " +
                "\tTHEN minutes_add(hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 6), 30) \n" +
                "\tELSE str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000') \n" +
                "\tEND ) < STR_TO_DATE('2024-07-03 23:00:00', '%Y-%m-%d %H:%i:%S')\n" +
                "\tAND (1 = 1\n" +
                "\t\tAND col_1 IN ('foobar1', 'foobar2', 'foobar3')\n" +
                "\t\t\tAND col_2 IN ('bar1', 'bar2')\n" +
                "\t\t\t\tAND col_5 IN ('foo1', 'foo2', 'foo3')\n" +
                "\t\t\t\t\tAND col_4 NOT IN ('') )";

        String plan = UtFrameUtils.getFragmentPlan(getStarRocksAssert().getCtx(), q);
        plan = plan.replaceAll("\\d+:\\s+(\\b\\w+\\b)", "$1");
        String snippet1 = "date_trunc('hour', localEventTs) >= '2024-07-01 00:00:00', " +
                "date_trunc('hour', localEventTs) < '2024-07-03 23:00:00'";
        String snippet2 = "eventTs < '2024-07-06 00:00:00', eventTs >= '2024-06-29 00:00:00'";
        Assertions.assertTrue(plan.contains(snippet1), plan);
        Assertions.assertTrue(plan.contains(snippet2), plan);
    }

    @Test
    public void testGeneralizedColumn2() throws Exception {
        String q = "select \n" +
                "DATE_TRUNC('hour', CASE \n" +
                "\tWHEN site IN ('MY', 'SG', 'PH') " +
                "\tTHEN hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 8) \n" +
                "\tWHEN site IN ('TH', 'VN', 'KH', 'ID') " +
                "\tTHEN hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 7) \n" +
                "\tWHEN site = 'MM' " +
                "\tTHEN minutes_add(hours_add(str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000'), 6), 30) \n" +
                "\tELSE str_to_date(eventTime, '%Y-%m-%dT%H:%i:%s+0000') \n" +
                "\tEND )\n" +
                "from t0;";
        String plan = UtFrameUtils.getFragmentPlan(getStarRocksAssert().getCtx(), q);
        plan = plan.replaceAll("\\d+:\\s+(\\b\\w+\\b)", "$1");
        Assertions.assertTrue(plan.contains("date_trunc('hour', localEventTs)"), plan);
    }
}
