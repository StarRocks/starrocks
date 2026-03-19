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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.scheduler.mv.ivm.MVIVMTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

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

    private static void checkTableStateToNormal(OlapTable tb) throws InterruptedException {
        // waiting table state to normal
        int retryTimes = 5;
        while (tb.getState() != OlapTable.OlapTableState.NORMAL && retryTimes > 0) {
            Thread.sleep(5000);
            retryTimes--;
        }
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, tb.getState());
    }

    private void waitSchemaChangeJobDone(boolean rollupJob, OlapTable tb) throws InterruptedException {
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        if (rollupJob) {
            alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        }
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println(alterJobV2.getType() + " alter job " + alterJobV2.getJobId() + " is done. state: " +
                    alterJobV2.getJobState());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        checkTableStateToNormal(tb);
    }

    public static void alterMVAddColumn(String sql, boolean expectedException) {
        try {
            AlterMaterializedViewStmt alterTableStmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);
            if (expectedException) {
                Assertions.fail();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (!expectedException) {
                Assertions.fail();
            }
        }
    }

    public static void alterMVDropColumn(String sql, boolean expectedException) {
        try {
            AlterMaterializedViewStmt alterTableStmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);
            if (expectedException) {
                Assertions.fail();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (!expectedException) {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testAddMVColumn() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_tbl1\n" +
                "(\n" +
                "    k1 date,\n" +
                "    v1 int, \n" +
                "    v2 int, \n" +
                "    v3 int \n" +
                ")\n" +
                "DUPLICATE KEY(`k1`)" +
                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT k1, sum(v1) from base_tbl1 group by k1;");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable(db.getFullName(), "base_tbl1");

        // mv1
        MaterializedView mv1 = (MaterializedView) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable(db.getFullName(), "mv1");
        {
            String stmt = "alter materialized view mv1 add column count_k1 as count(k1)";
            alterMVAddColumn(stmt, false);
            waitSchemaChangeJobDone(false, tbl);
            Assertions.assertEquals(mv1.getColumns().size(), 3);
        }
        // check query rewrite
        {
            String query = "select count(k1) from base_tbl1 group by k1";
            String plan = UtFrameUtils.getFragmentPlan(connectContext, query);
            PlanTestBase.assertContains(plan, "mv1");
        }

        {
            String stmt = "alter materialized view mv1 add column v2 as v2";
            alterMVAddColumn(stmt, false);
            waitSchemaChangeJobDone(false, tbl);
            Assertions.assertEquals(mv1.getColumns().size(), 4);
        }

        {
            String stmt = "alter materialized view mv1 add column v3_default as v3 default \"10\"";
            alterMVAddColumn(stmt, false);
            waitSchemaChangeJobDone(false, tbl);
            Column column = mv1.getColumn("v3_default");
            Assertions.assertNotNull(column);
            Assertions.assertEquals("10", column.getDefaultValue());
            Assertions.assertEquals(mv1.getColumns().size(), 5);
        }

        {
            String dropStmt = "alter materialized view mv1 drop column count_k1";
            alterMVDropColumn(dropStmt, false);
            waitSchemaChangeJobDone(false, tbl);
            Assertions.assertEquals(mv1.getColumns().size(), 4);
        }
        {
            String dropStmt = "alter materialized view mv1 drop column v2";
            alterMVDropColumn(dropStmt, true);
            Assertions.assertEquals(mv1.getColumns().size(), 4);
        }

    }

    @Test
    public void testAddDropMVColumnEdgeCases() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_tbl_mv_col1\n" +
                "(\n" +
                "    k1 date,\n" +
                "    v1 int \n" +
                ")\n" +
                "DUPLICATE KEY(`k1`)" +
                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_add_drop_col1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT k1, sum(v1) from base_tbl_mv_col1 group by k1;");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable(db.getFullName(), "mv_add_drop_col1");

        String addStmt = "alter materialized view mv_add_drop_col1 add column cnt_k1 as count(k1)";
        alterMVAddColumn(addStmt, false);
        waitSchemaChangeJobDone(false, mv);
        Assertions.assertNotNull(mv.getColumn("cnt_k1"));
        Assertions.assertEquals(3, mv.getColumns().size());

        alterMVAddColumn(addStmt, true);

        String addLiteralStmt = "alter materialized view mv_add_drop_col1 add column c_literal as 1";
        alterMVAddColumn(addLiteralStmt, true);

        String addNonAggStmt = "alter materialized view mv_add_drop_col1 add column c_non_agg as v1 + 1";
        alterMVAddColumn(addNonAggStmt, false);
        waitSchemaChangeJobDone(false, mv);
        Assertions.assertNotNull(mv.getColumn("c_non_agg"));
        Assertions.assertEquals(4, mv.getColumns().size());

        String dropMissingStmt = "alter materialized view mv_add_drop_col1 drop column missing_col";
        alterMVDropColumn(dropMissingStmt, true);

        String dropStmt = "alter materialized view mv_add_drop_col1 drop column cnt_k1";
        alterMVDropColumn(dropStmt, false);
        waitSchemaChangeJobDone(false, mv);
        Assertions.assertNull(mv.getColumn("cnt_k1"));
        Assertions.assertEquals(3, mv.getColumns().size());

        alterMVDropColumn(dropStmt, true);
    }
}
