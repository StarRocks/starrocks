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

package com.starrocks.analysis;

import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CreateAsyncMaterializedViewTest extends MVTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.unitTestView = false;
        MVTestBase.beforeClass();
    }

    @Test
    public void testCreateMVWithDialect() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_table1 (\n" +
                "  stream_time       DATETIME NOT NULL,\n" +
                "  date_id           INT NOT NULL,\n" +
                "  hour_local        INT NOT NULL,\n" +
                "  minute_local      INT NOT NULL,\n" +
                "  country_id        BIGINT,\n" +
                "  city_id           BIGINT,\n" +
                "  drop_off_poi_id   BIGINT,\n" +
                "  vehicle_type_id   BIGINT,\n" +
                "  event             VARCHAR(64),\n" +
                "  originalsurge     DECIMAL(10,2),\n" +
                "  final_fare        DECIMAL(10,2),\n" +
                "  distance          DECIMAL(10,2),\n" +
                "  surge             DECIMAL(10,2),\n" +
                "  series_id         BIGINT\n" +
                ") ENGINE=OLAP\n" +
                "DISTRIBUTED BY HASH(stream_time) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withRefreshedMaterializedView("CREATE MATERIALIZED VIEW test_mv1 \n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"session.sql_dialect\" = \"trino\"\n" +
                ") AS\n" +
                "SELECT\n" +
                "DATE_TRUNC('day', date_add('minute', (hour_local * 60 + minute_local), " +
                "STR_TO_DATE(CAST(date_id AS VARCHAR), '%Y%m%d'))),\n" +
                "date_id,\n" +
                "sum(originalsurge) as sum_originalsurge,\n" +
                "count(originalsurge) as count_originalsurge\n" +
                "FROM base_table1\n" +
                "group by 1,2\n" +
                ";");
        connectContext.getSessionVariable().setSqlDialect("trino");
        String plan = getFragmentPlan("SELECT\n" +
                "\tCAST(AVG(originalsurge) AS DOUBLE) AS raw_surge_with_poi\n" +
                "FROM base_table1\n" +
                "WHERE\n" +
                "\tDATE_TRUNC('day', date_add('minute', (hour_local * 60 + minute_local), " +
                "STR_TO_DATE(CAST(date_id AS VARCHAR), '%Y%m%d'))) >= STR_TO_DATE('2025-07-01 00:00:00', '%Y-%m-%d %H:%i:%S');");
        PlanTestBase.assertContains(plan, "test_mv1");
    }

    @Test
    public void testCreateMVWithMode() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_table1 (\n" +
                "  stream_time       DATETIME NOT NULL,\n" +
                "  date_id           INT NOT NULL,\n" +
                "  hour_local        INT NOT NULL,\n" +
                "  minute_local      INT NOT NULL,\n" +
                "  country_id        BIGINT,\n" +
                "  city_id           BIGINT,\n" +
                "  drop_off_poi_id   BIGINT,\n" +
                "  vehicle_type_id   BIGINT,\n" +
                "  event             VARCHAR(64),\n" +
                "  originalsurge     DECIMAL(10,2),\n" +
                "  final_fare        DECIMAL(10,2),\n" +
                "  distance          DECIMAL(10,2),\n" +
                "  surge             DECIMAL(10,2),\n" +
                "  series_id         BIGINT\n" +
                ") ENGINE=OLAP\n" +
                "DISTRIBUTED BY HASH(stream_time) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withRefreshedMaterializedView("CREATE MATERIALIZED VIEW test_mv1 \n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"session.sql_dialect\" = \"trino\",\n" +
                "\"session.sql_mode\" = \"8589934626\"\n" +
                ") AS\n" +
                "SELECT\n" +
                "CASE\n" +
                "WHEN city_id = 0 THEN 'Control'\n" +
                "WHEN city_id = 1 THEN 'Treatment'\n" +
                "ELSE 'Treatment-' || lpad(cast(city_id as varchar), 2, '0')\n" +
                "END AS treatment_id," +
                "DATE_TRUNC('day', date_add('minute', (hour_local * 60 + minute_local), " +
                "STR_TO_DATE(CAST(date_id AS VARCHAR), '%Y%m%d'))),\n" +
                "date_id,\n" +
                "sum(originalsurge) as sum_originalsurge,\n" +
                "count(originalsurge) as count_originalsurge\n" +
                "FROM base_table1\n" +
                "group by 1,2,3\n" +
                ";");
        connectContext.getSessionVariable().setSqlDialect("trino");
        connectContext.getSessionVariable().setSqlMode(8589934626L);
        String plan = getFragmentPlan("SELECT\n" +
                "\tCAST(AVG(originalsurge) AS DOUBLE) AS raw_surge_with_poi\n" +
                "FROM base_table1\n" +
                "WHERE\n" +
                "CASE\n" +
                "WHEN city_id = 0 THEN 'Control'\n" +
                "WHEN city_id = 1 THEN 'Treatment'\n" +
                "ELSE 'Treatment-' || lpad(cast(city_id as varchar), 2, '0')\n" +
                "END = 'Treatment'");
        PlanTestBase.assertContains(plan, "test_mv1");
    }
}

