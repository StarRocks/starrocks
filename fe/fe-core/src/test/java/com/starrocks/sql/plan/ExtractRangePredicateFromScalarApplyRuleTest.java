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
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ExtractRangePredicateFromScalarApplyRuleTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.unitTestView = false;
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE `site_access` (\n" +
                "  `event_day` datetime NOT NULL COMMENT \"\",\n" +
                "  `site_id` int(11) NOT NULL DEFAULT \"10\" COMMENT \"\",\n" +
                "  `event_date` date NOT NULL COMMENT \"\",\n" +
                "  `city_code` varchar(100) NULL COMMENT \"\",\n" +
                "  `user_name` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`event_day`, `site_id`)\n" +
                "PARTITION BY range(event_day)(\n" +
                "    PARTITION p1 VALUES LESS THAN (\"2020-01-01\"),\n" +
                "    PARTITION p2 VALUES LESS THAN (\"2020-01-02\"),\n" +
                "    PARTITION p3 VALUES LESS THAN (\"2020-01-03\"),\n" +
                "    PARTITION p4 VALUES LESS THAN (\"2020-01-04\"),\n" +
                "    PARTITION p5 VALUES LESS THAN (\"2020-01-05\"),\n" +
                "    PARTITION p6 VALUES LESS THAN (\"2020-01-06\"),\n" +
                "    PARTITION p7 VALUES LESS THAN (\"2020-01-07\"),\n" +
                "    PARTITION p8 VALUES LESS THAN (\"2020-01-08\"),\n" +
                "    PARTITION p9 VALUES LESS THAN (\"2020-01-09\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`event_day`, `site_id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `site_access2` (\n" +
                "  `event_day` string NOT NULL COMMENT \"\",\n" +
                "  `site_id` int(11) NOT NULL DEFAULT \"10\" COMMENT \"\",\n" +
                "  `city_code` varchar(100) NULL COMMENT \"\",\n" +
                "  `user_name` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`event_day`, `site_id`)\n" +
                "PARTITION BY list(event_day)(\n" +
                "    PARTITION p1 VALUES IN (\"2020-01-01\"),\n" +
                "    PARTITION p2 VALUES IN (\"2020-01-02\"),\n" +
                "    PARTITION p3 VALUES IN (\"2020-01-03\"),\n" +
                "    PARTITION p4 VALUES IN (\"2020-01-04\"),\n" +
                "    PARTITION p5 VALUES IN (\"2020-01-05\"),\n" +
                "    PARTITION p6 VALUES IN (\"2020-01-06\"),\n" +
                "    PARTITION p7 VALUES IN (\"2020-01-07\"),\n" +
                "    PARTITION p8 VALUES IN (\"2020-01-08\"),\n" +
                "    PARTITION p9 VALUES IN (\"2020-01-09\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`event_day`, `site_id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");        ");
        starRocksAssert.withTable("CREATE TABLE `zones` (\n" +
                "   id int,\n" +
                "   server_tz string,\n" +
                "   txn_tz string\n" +
                ")ENGINE=OLAP \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testDatetimeColumn() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000);
        String sqlFmt = "select * \n" +
                "from  site_access\n" +
                "where event_day {OP} (\n" +
                "    select convert_tz('2020-01-05 01:01:01', server_tz, txn_tz) \n" +
                "    from zones\n" +
                "    where id = 1\n" +
                ");";
        String[][] cases = new String[][] {
                {"=", " Predicates: [1: event_day, DATETIME, false] >= '2020-01-03 23:01:01', " +
                        "[1: event_day, DATETIME, false] <= '2020-01-06 03:01:01'"},
                {"<=", "Predicates: [1: event_day, DATETIME, false] <= '2020-01-06 03:01:01'"},
                {">=", "Predicates: [1: event_day, DATETIME, false] >= '2020-01-03 23:01:01'"},
                {"<", "Predicates: [1: event_day, DATETIME, false] <= '2020-01-06 03:01:01'"},
                {">", "Predicates: [1: event_day, DATETIME, false] >= '2020-01-03 23:01:01'"},
        };
        for (String[] tc : cases) {
            String sql = sqlFmt.replace("{OP}", tc[0]);
            String plan = getCostExplain(sql);
            assertCContains(plan, tc[1]);
        }
    }

    @Test
    public void testStringColumn() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000);
        String sqlFmt = "select * \n" +
                "from  site_access2\n" +
                "where event_day {OP} (\n" +
                "    select convert_tz('2020-01-05 01:01:01', server_tz, txn_tz) \n" +
                "    from zones\n" +
                "    where id = 1\n" +
                ");";

        String[][] cases = new String[][] {
                {"=", "partitionsRatio=3/9"},
                {"<=", "partitionsRatio=6/9"},
                {">=", "partitionsRatio=6/9"},
                {"<", "partitionsRatio=6/9"},
                {">", "partitionsRatio=6/9"},
        };
        for (String[] tc : cases) {
            String sql = sqlFmt.replace("{OP}", tc[0]);
            String plan = getCostExplain(sql);
            assertCContains(plan, tc[1]);
        }
    }

    @Test
    public void testDateColumn() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000);
        String sqlFmt = "select * \n" +
                "from  site_access\n" +
                "where event_date {OP} (\n" +
                "    select convert_tz('2020-01-05 01:01:01', server_tz, txn_tz) \n" +
                "    from zones\n" +
                "    where id = 1\n" +
                ");";
        String[][] cases = new String[][] {
                {"=",
                        "Predicates: [3: event_date, DATE, false] >= '2020-01-04', [3: event_date, DATE, false] <= '2020-01-06'"},
                {"<=", "Predicates: [3: event_date, DATE, false] <= '2020-01-06'"},
                {">=", "Predicates: [3: event_date, DATE, false] >= '2020-01-04'"},
                {"<", "Predicates: [3: event_date, DATE, false] <= '2020-01-06'"},
                {">", "Predicates: [3: event_date, DATE, false] >= '2020-01-04'"},
        };
        for (String[] tc : cases) {
            String sql = sqlFmt.replace("{OP}", tc[0]);
            String plan = getCostExplain(sql);
            assertCContains(plan, tc[1]);
        }
    }
}