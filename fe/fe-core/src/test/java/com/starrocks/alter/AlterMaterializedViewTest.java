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

package com.starrocks.alter;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.scheduler.mv.ivm.MVIVMTestBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AlterMaterializedViewTest extends MVTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
        starRocksAssert.useDatabase("test");
        starRocksAssert.withTable(cluster, "depts");
        starRocksAssert.withTable(cluster, "emps");
    }

    @Test
    public void testChangeMVRefreshMode1() throws Exception {
        String query = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());

        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"incremental\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());
        }
        // change refresh mode from incremental to auto
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"auto\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());
        }
        // change refresh mode from auto to full
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"full\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
    }

    @Test
    public void testChangeMVRefreshMode2() throws Exception {
        String query = "SELECT id, count(data) over (partition by date)  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());

        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"incremental\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        }
        // change refresh mode to auto
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"auto\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        }
        // change refresh mode to full
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"full\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
    }

    @Test
    public void testChangeMVRefreshMode3() throws Exception {
        String query = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        Assertions.assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());

        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"incremental\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());
        }
        // change refresh mode from incremental to auto
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"auto\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());
        }
        // change refresh mode from auto to full
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"full\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
    }

    @Test
    public void testChangeMVRefreshMode4() throws Exception {
        String query = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "full");
        Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());

        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"incremental\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
        // change refresh mode from incremental to auto
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"auto\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
        // change refresh mode from auto to full
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"full\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
    }

    @Test
    public void testChangeMVRefreshMode5() throws Exception {
        String query = "SELECT id, count(data) over (partition by date)  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "full");
        Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());

        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"incremental\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
        // change refresh mode to auto
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"auto\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
        // change refresh mode to full
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"full\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
    }

    @Test
    public void testChangeMVRefreshMode6() throws Exception {
        String query = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "pct");
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());

        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"incremental\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        }
        // change refresh mode from incremental to auto
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"auto\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        }
        // change refresh mode from auto to full
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"full\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
    }

    @Test
    public void testChangeMVRefreshMode7() throws Exception {
        String query = "SELECT id, count(data) over (partition by date)  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "pct");
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());

        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"incremental\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        }
        // change refresh mode to auto
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"auto\")";
            alterMaterializedView(alterStmt, true);
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        }
        // change refresh mode to full
        {
            String alterStmt = "alter materialized view test_mv1 set (\"refresh_mode\" = \"full\")";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
        }
    }

    @Test
    public void testAutoRefreshInactiveActive1() throws Exception {
        String query = "SELECT date, sum(id), approx_count_distinct(data) " +
                "FROM `iceberg0`.`unpartitioned_db`.`t0` group by date;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        Assertions.assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());
        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 inactive";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());
        }
        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 active";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());
        }
    }

    @Test
    public void testAutoRefreshInactiveActive2() throws Exception {
        String query = "SELECT date, sum(id), approx_count_distinct(data) " +
                "FROM `iceberg0`.`unpartitioned_db`.`t0` group by date;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());
        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 inactive";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());
        }
        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 active";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());
        }
    }

    @Test
    public void testAutoRefreshInactiveActive3() throws Exception {
        String query = "SELECT date, sum(id), approx_count_distinct(data) " +
                "FROM `iceberg0`.`unpartitioned_db`.`t0` group by date;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "pct");
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 inactive";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        }
        // change refresh mode from auto to incremental
        {
            String alterStmt = "alter materialized view test_mv1 active";
            alterMaterializedView(alterStmt, false);
            Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        }
    }
}
