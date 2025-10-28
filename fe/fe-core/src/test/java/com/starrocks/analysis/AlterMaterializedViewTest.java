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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.alter.AlterMVJobExecutor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ShowMaterializedViewStatus;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.scheduler.PartitionBasedMvRefreshProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class AlterMaterializedViewTest extends MVTestBase  {
    private static GlobalStateMgr currentState;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        currentState = GlobalStateMgr.getCurrentState();
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv1\n" +
                "                DISTRIBUTED BY HASH(v1) BUCKETS 10\n" +
                "                PROPERTIES(\n" +
                "                    \"replication_num\" = \"1\"\n" +
                "                )\n" +
                "                as  select v1, count(v2) as count_c2, sum(v3) as sum_c3\n" +
                "                from t0 group by v1;\n");
    }

    @BeforeEach
    public void before() throws Exception {
        super.before();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testRename() throws Exception {
        MaterializedView mv1 = starRocksAssert.getMv("test", "mv1");
        String taskDefinition = mv1.getTaskDefinition();
        starRocksAssert.ddl("alter materialized view mv1 rename mv2;");
        MaterializedView mv2 = starRocksAssert.getMv("test", "mv2");
        Assertions.assertEquals("insert overwrite `mv2` " +
                "SELECT `test`.`t0`.`v1`, count(`test`.`t0`.`v2`) AS `count_c2`, sum(`test`.`t0`.`v3`) AS `sum_c3`\n" +
                "FROM `test`.`t0`\n" +
                "GROUP BY `test`.`t0`.`v1`", mv2.getTaskDefinition());

        starRocksAssert.ddl("alter materialized view mv2 rename mv1;");
        mv1 = starRocksAssert.getMv("test", "mv1");
        Assertions.assertEquals(taskDefinition, mv1.getTaskDefinition());
    }

    @Test
    public void testRenameSameName() {
        assertThrows(AnalysisException.class, () -> {
            String alterMvSql = "alter materialized view mv1 rename mv1;";
            UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        });
    }

    @Test
    public void testAlterSyncRefresh() {
        assertThrows(AnalysisException.class, () -> {
            String alterMvSql = "alter materialized view mv1 refresh sync";
            AlterMaterializedViewStmt alterMvStmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            RefreshSchemeClause refreshSchemeClause = (RefreshSchemeClause) alterMvStmt.getAlterTableClause();
            Assertions.assertEquals(refreshSchemeClause.getType(), MaterializedView.RefreshType.SYNC);
        });
    }

    @Test
    public void testAlterRefreshScheme() throws Exception {
        List<String> refreshSchemes = Lists.newArrayList(
                "ASYNC START(\"2022-05-23 00:00:00\") EVERY(INTERVAL 1 HOUR)",
                "ASYNC",
                "ASYNC START(\"2022-05-23 01:02:03\") EVERY(INTERVAL 1 DAY)",
                "ASYNC EVERY(INTERVAL 1 DAY)",
                "ASYNC",
                "MANUAL",
                "ASYNC EVERY(INTERVAL 1 DAY)",
                "MANUAL",
                "ASYNC START(\"2022-05-23 01:02:03\") EVERY(INTERVAL 1 DAY)"
        );

        String mvName = "mv1";
        MaterializedView mv = starRocksAssert.getMv("test", mvName);
        String taskDefinition = mv.getTaskDefinition();
        for (String refresh : refreshSchemes) {
            // alter
            String sql = String.format("alter materialized view %s refresh %s", mvName, refresh);
            starRocksAssert.ddl(sql);

            // verify
            mv = starRocksAssert.getMv("test", mvName);
            String showCreateStmt = mv.getMaterializedViewDdlStmt(false);
            Assertions.assertTrue(showCreateStmt.contains(refresh),
                    String.format("alter to %s \nbut got \n%s", refresh, showCreateStmt));
            Assertions.assertEquals(taskDefinition, mv.getTaskDefinition());
        }
    }

    @Test
    public void testAlterMVProperties() throws Exception {
        {
            String alterMvSql = "alter materialized view mv1 set (\"session.query_timeout\" = \"10000\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            currentState.getLocalMetastore().alterMaterializedView(stmt);
        }
        {
            String alterMvSql = "alter materialized view mv1 set (\"session.not_exists\" = \"10000\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            Exception e = Assertions.assertThrows(SemanticException.class,
                    () -> currentState.getLocalMetastore().alterMaterializedView(stmt));
            Assertions.assertEquals("Getting analyzing error. Detail message: " +
                    "Unknown system variable 'not_exists', the most similar variables are " +
                    "{'init_connect', 'connector_max_split_size', 'tx_isolation'}.", e.getMessage());
        }

        {
            String alterMvSql = "alter materialized view mv1 set (\"query_timeout\" = \"10000\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            Assertions.assertThrows(SemanticException.class, () -> currentState.getLocalMetastore().alterMaterializedView(stmt));
        }
    }

    // TODO: consider to support alterjob for mv
    @Test
    public void testAlterMVColocateGroup() throws Exception {
        String alterMvSql = "alter materialized view mv1 set (\"colocate_with\" = \"group1\")";
        AlterMaterializedViewStmt stmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        Assertions.assertThrows(SemanticException.class, () -> currentState.getLocalMetastore().alterMaterializedView(stmt));
    }

    @Test
    public void testAlterMVRewriteStalenessProperties() throws Exception {
        {
            String alterMvSql = "alter materialized view mv1 set (\"mv_rewrite_staleness_second\" = \"60\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            currentState.getLocalMetastore().alterMaterializedView(stmt);
        }

        {
            String alterMvSql = "alter materialized view mv1 set (\"mv_rewrite_staleness_second\" = \"abc\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            Assertions.assertThrows(SemanticException.class, () -> currentState.getLocalMetastore().alterMaterializedView(stmt));
        }
    }


    @Test
    public void testInactiveMV() throws Exception {
        starRocksAssert
                .withTable("CREATE TABLE IF NOT EXISTS par_tbl1\n" +
                        "(\n" +
                        "    datekey DATETIME,\n" +
                        "    item_id STRING,\n" +
                        "    v1      INT\n" +
                        ")PRIMARY KEY (`datekey`,`item_id`)\n" +
                        "    PARTITION BY date_trunc('day', `datekey`);");
        executeInsertSql(connectContext, "INSERT INTO par_tbl1 values ('2025-01-01', '1', 1);");
        executeInsertSql(connectContext, "INSERT INTO par_tbl1 values ('2025-01-02', '1', 1);");

        starRocksAssert
                .withTable("CREATE TABLE IF NOT EXISTS par_tbl2\n" +
                        "(\n" +
                        "    datekey DATETIME,\n" +
                        "    item_id STRING,\n" +
                        "    v1      INT\n" +
                        ")PRIMARY KEY (`datekey`,`item_id`)\n" +
                        "    PARTITION BY date_trunc('day', `datekey`);");
        executeInsertSql(connectContext, "INSERT INTO par_tbl2 values ('2025-01-01', '1', 2);");
        executeInsertSql(connectContext, "INSERT INTO par_tbl2 values ('2025-01-02', '1', 1);");

        starRocksAssert
                .withTable("CREATE TABLE IF NOT EXISTS dim_data\n" +
                        "(\n" +
                        "    item_id STRING,\n" +
                        "    v1 INT\n" +
                        ")PRIMARY KEY (`item_id`);");
        executeInsertSql(connectContext, "INSERT INTO dim_data values ('1', 4);");

        starRocksAssert
                .withMaterializedView("CREATE\n" +
                        "MATERIALIZED VIEW mv_dim_data1\n" +
                        "REFRESH ASYNC EVERY(INTERVAL 60 MINUTE)\n" +
                        "AS\n" +
                        "select *\n" +
                        "from dim_data;");

        starRocksAssert
                .withMaterializedView("CREATE\n" +
                        "MATERIALIZED VIEW mv_test1\n" +
                        "REFRESH ASYNC EVERY(INTERVAL 60 MINUTE)\n" +
                        "PARTITION BY p_time\n" +
                        "PROPERTIES (\n" +
                        "\"excluded_trigger_tables\" = \"mv_dim_data1\",\n" +
                        "\"excluded_refresh_tables\" = \"mv_dim_data1\",\n" +
                        "\"partition_refresh_number\" = \"1\"\n" +
                        ")\n" +
                        "AS\n" +
                        "select date_trunc(\"day\", a.datekey) as p_time, sum(a.v1) + sum(b.v1) as v1\n" +
                        "from par_tbl1 a\n" +
                        "         left join par_tbl2 b on a.datekey = b.datekey and a.item_id = b.item_id\n" +
                        "         left join mv_dim_data1 d on a.item_id = d.item_id\n" +
                        "group by date_trunc(\"day\", a.datekey), a.item_id;");

        starRocksAssert.refreshMV("refresh materialized view mv_test1 with sync mode;");
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "mv_test1");
        Assertions.assertTrue(starRocksAssert.waitRefreshFinished(mv.getId()));

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(mv);
        Assertions.assertNotNull(task);
        Assertions.assertEquals(0, task.getConsecutiveFailCount());

        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assertions.assertTrue(!baseTableVisibleVersionMap.isEmpty());

        String alterMvSql = "alter materialized view mv_test1 INACTIVE";
        AlterMaterializedViewStmt stmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        currentState.getLocalMetastore().alterMaterializedView(stmt);
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(Constants.TaskState.PAUSE, task.getState());
        Assertions.assertEquals(0, task.getConsecutiveFailCount());

        alterMvSql = "alter materialized view mv_test1 ACTIVE";
        stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        currentState.getLocalMetastore().alterMaterializedView(stmt);
        Assertions.assertTrue(starRocksAssert.waitRefreshFinished(mv.getId()));
        Assertions.assertTrue(mv.isActive());
        baseTableVisibleVersionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assertions.assertTrue(!baseTableVisibleVersionMap.isEmpty());
        Assertions.assertNotEquals(Constants.TaskState.PAUSE, task.getState());

        alterMvSql = "alter materialized view mv_test1 INACTIVE";
        stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        currentState.getLocalMetastore().alterMaterializedView(stmt);
        Assertions.assertFalse(mv.isActive());

        alterMvSql = "alter materialized view mv_test1 ACTIVE";
        stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        currentState.getLocalMetastore().alterMaterializedView(stmt);
        Assertions.assertTrue(starRocksAssert.waitRefreshFinished(mv.getId()));
        Assertions.assertTrue(mv.isActive());
        // Don't refresh base table version map
        baseTableVisibleVersionMap = mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assertions.assertTrue(!baseTableVisibleVersionMap.isEmpty());

        // inactive mv when base table's schema changed
        Database db = starRocksAssert.getDb(connectContext.getDatabase());
        Table parTbl1 = starRocksAssert.getTable(connectContext.getDatabase(), "par_tbl1");
        AlterMVJobExecutor.inactiveRelatedMaterializedViews(db, (OlapTable) parTbl1, Set.of("item_id"));
        baseTableVisibleVersionMap = mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assertions.assertTrue(baseTableVisibleVersionMap.isEmpty());
    }

    @Test
    public void testAlterMVOnView() throws Exception {
        final String mvName = "mv_on_view_1";
        starRocksAssert.withView("CREATE VIEW view1 as select v1, sum(v2) as k2 from t0 group by v1");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName +
                "                DISTRIBUTED BY HASH(v1) BUCKETS 10\n" +
                "                PROPERTIES(\n" +
                "                    \"replication_num\" = \"1\"\n" +
                "                )\n" +
                "                as select v1, k2 from view1");

        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), mvName);
        List<String> columns = mv.getColumns().stream().map(Column::getName).sorted().collect(Collectors.toList());
        Assertions.assertEquals(ImmutableList.of("k2", "v1"), columns);

        // alter the view to a different type, cause MV inactive
        connectContext.executeSql("alter view view1 as select v1, avg(v2) as k2 from t0 group by v1");
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals("base-view changed: view1", mv.getInactiveReason());

        // try to active the mv
        connectContext.executeSql(String.format("alter materialized view %s active", mvName));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals("column schema not compatible: (`k2` bigint(20) NULL COMMENT \"\") " +
                "and (`k2` double NULL COMMENT \"\")", mv.getInactiveReason());

        // use a illegal view schema, should active the mv correctly
        connectContext.executeSql("alter view view1 as select v1, max(v2) as k2 from t0 group by v1");
        connectContext.executeSql(String.format("alter materialized view %s active", mvName));
        Assertions.assertTrue(mv.isActive());
        Assertions.assertNull(mv.getInactiveReason());
    }

    /**
     * Reload procedure should work for hierarchical MV
     */
    @Test
    public void testMVOnMVReload() throws Exception {
        MVActiveChecker checker = GlobalStateMgr.getCurrentState().getMvActiveChecker();
        checker.setStop();

        String createBaseTable = "create table treload_1 (c1 int) distributed by hash(c1) " +
                "properties('replication_num'='1')";
        starRocksAssert.withTable(createBaseTable);
        starRocksAssert.withMaterializedView("create materialized view mvreload_1 refresh async " +
                "as select * from treload_1");
        starRocksAssert.withMaterializedView("create materialized view mvreload_2 refresh async " +
                "as select * from treload_1");
        starRocksAssert.withMaterializedView("create materialized view mvreload_3 refresh async " +
                "as select a.c1, b.c1 as bc1 from mvreload_1 a join mvreload_2 b");

        // drop base table would inactive all related MV
        starRocksAssert.dropTable("treload_1");
        starRocksAssert.refreshMV("refresh materialized view mvreload_3");
        Assertions.assertFalse(starRocksAssert.getMv("test", "mvreload_1").isActive());
        Assertions.assertFalse(starRocksAssert.getMv("test", "mvreload_2").isActive());
        Assertions.assertFalse(starRocksAssert.getMv("test", "mvreload_3").isActive());

        // create the table and run the AutoActive
        starRocksAssert.withTable(createBaseTable);
        checker.runForTest(true);
        checker.runForTest(true);
        Assertions.assertTrue(starRocksAssert.getMv("test", "mvreload_1").isActive());
        Assertions.assertTrue(starRocksAssert.getMv("test", "mvreload_2").isActive());
        Assertions.assertTrue(starRocksAssert.getMv("test", "mvreload_3").isActive());

        // create the table and refresh
        starRocksAssert.dropTable("treload_1");
        starRocksAssert.withTable(createBaseTable);
        checker.runForTest(true);
        checker.runForTest(true);
        starRocksAssert.refreshMV("refresh materialized view mvreload_1");
        starRocksAssert.refreshMV("refresh materialized view mvreload_2");
        starRocksAssert.refreshMV("refresh materialized view mvreload_3");
        Assertions.assertTrue(starRocksAssert.getMv("test", "mvreload_1").isActive());
        Assertions.assertTrue(starRocksAssert.getMv("test", "mvreload_2").isActive());
        Assertions.assertTrue(starRocksAssert.getMv("test", "mvreload_3").isActive());

        // create the table and manually active, top-down active
        starRocksAssert.dropTable("treload_1");
        starRocksAssert.withTable(createBaseTable);
        starRocksAssert.ddl("alter materialized view mvreload_1 active");
        starRocksAssert.ddl("alter materialized view mvreload_2 active");
        starRocksAssert.ddl("alter materialized view mvreload_3 active");
        Assertions.assertTrue(starRocksAssert.getMv("test", "mvreload_1").isActive());
        Assertions.assertTrue(starRocksAssert.getMv("test", "mvreload_2").isActive());
        Assertions.assertTrue(starRocksAssert.getMv("test", "mvreload_3").isActive());

        // cleanup
        starRocksAssert.dropTable("treload_1");
        starRocksAssert.dropMaterializedView("mvreload_1");
        starRocksAssert.dropMaterializedView("mvreload_2");
        starRocksAssert.dropMaterializedView("mvreload_3");
        checker.start();
    }

    @Test
    public void testAlterMVOnViewComment() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `tb_order` (\n" +
                "                                 `order_id` bigint(20) NULL COMMENT \"order_id\",\n" +
                "                                 `order_amt` double NULL COMMENT \"order_amt\",\n" +
                "                                 `order_date` date NULL COMMENT \"order_date\",\n" +
                "                                 `description` varchar(255) NULL COMMENT \"description\",\n" +
                "                                 `buyer_id` bigint(20) NULL COMMENT \"buyer_id\",\n" +
                "                                 `seller_id` bigint(20) NULL COMMENT \"seller_id\",\n" +
                "                                 `product_id` bigint(20) NULL COMMENT \"product_id\",\n" +
                "                                 `express_id` bigint(20) NULL COMMENT \"express_id\",\n" +
                "                                 `region_id` bigint(20) NULL COMMENT \"region_id\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`order_id`)\n" +
                "DISTRIBUTED BY HASH(`order_id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ")");
        starRocksAssert.withView("create or replace view pb_view as " +
                "select order_id,order_amt,order_date,description,buyer_id,seller_id,product_id,express_id,region_id\n" +
                "                                       from `tb_order`");
        starRocksAssert.withMaterializedView("create Materialized View mv_pb_view\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "as select order_id,order_amt,order_date,description,buyer_id,seller_id,product_id,express_id,region_id\n" +
                "   from pb_view");

        // replace with exactly same
        starRocksAssert.withView("create or replace view pb_view as " +
                "select order_id,order_amt,order_date,description,buyer_id,seller_id,product_id,express_id,region_id\n" +
                "                                       from `tb_order`");
        starRocksAssert.ddl("ALTER MATERIALIZED VIEW mv_pb_view ACTIVE;");

        MaterializedView mv = starRocksAssert.getMv("test", "mv_pb_view");
        Map<String, String> columnMap =
                mv.getColumns().stream().collect(Collectors.toMap(Column::getName, Column::getComment));
        Assertions.assertEquals(Map.of("order_id", "",
                "order_amt", "",
                "order_date", "",
                "description", "",
                "buyer_id", "",
                "seller_id", "",
                "product_id", "",
                "express_id", "",
                "region_id", ""), columnMap);
    }

    @Test
    public void testActiveChecker() throws Exception {
        MVActiveChecker checker = GlobalStateMgr.getCurrentState().getMvActiveChecker();
        checker.setStop();

        String baseTableName = "base_tbl_active";
        String createTableSql =
                "create table " + baseTableName + " ( k1 int, k2 int) properties('replication_num'='1')";
        starRocksAssert.withTable(createTableSql);
        starRocksAssert.withMaterializedView("create materialized view mv_active " +
                " refresh manual as select * from base_tbl_active");
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "mv_active");
        Assertions.assertTrue(mv.isActive());

        // drop the base table and try to activate it
        starRocksAssert.dropTable(baseTableName);
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals("base-table dropped: base_tbl_active", mv.getInactiveReason());
        checker.runForTest(true);
        Assertions.assertFalse(mv.isActive());
        Assertions.assertTrue(mv.getInactiveReason().contains("base-table dropped: base_tbl_active"));

        // create the table again, and activate it
        connectContext.setThreadLocalInfo();
        starRocksAssert.withTable(createTableSql);
        checker.runForTest(true);
        Assertions.assertTrue(mv.isActive());

        // activate before refresh
        connectContext.setThreadLocalInfo();
        starRocksAssert.dropTable(baseTableName);
        starRocksAssert.withTable(createTableSql);
        Assertions.assertFalse(mv.isActive());
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mv.getName() + " with sync mode");
        Assertions.assertTrue(mv.isActive());

        // manually set to inactive
        mv.setInactiveAndReason(AlterJobMgr.MANUAL_INACTIVE_MV_REASON);
        Assertions.assertFalse(mv.isActive());
        checker.runForTest(true);
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(AlterJobMgr.MANUAL_INACTIVE_MV_REASON, mv.getInactiveReason());
        // manual active
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mv.getName() + " with sync mode");
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(AlterJobMgr.MANUAL_INACTIVE_MV_REASON, mv.getInactiveReason());

        checker.start();
        starRocksAssert.dropTable(baseTableName);
        starRocksAssert.dropMaterializedView(mv.getName());
    }

    @Test
    public void testActiveGracePeriod() throws Exception {
        MVActiveChecker checker = GlobalStateMgr.getCurrentState().getMvActiveChecker();
        checker.setStop();

        String mvName = "mv_active";
        String baseTableName = "base_tbl_active";
        String createTableSql =
                "create table " + baseTableName + " ( k1 int, k2 int) properties('replication_num'='1')";
        starRocksAssert.withTable(createTableSql);
        starRocksAssert.withMaterializedView("create materialized view mv_active " +
                " refresh manual as select * from base_tbl_active");
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "mv_active");
        Assertions.assertTrue(mv.isActive());

        // drop the base table and try to activate it
        starRocksAssert.dropTable(baseTableName);
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals("base-table dropped: base_tbl_active", mv.getInactiveReason());
        checker.runForTest(false);
        for (int i = 0; i < 10; i++) {
            checker.runForTest(false);
            Assertions.assertFalse(mv.isActive());
        }

        // create the table, but in grace period, could not activate it
        connectContext.setThreadLocalInfo();
        starRocksAssert.withTable(createTableSql);
        for (int i = 0; i < 10; i++) {
            checker.runForTest(false);
            Assertions.assertFalse(mv.isActive());
        }

        // foreground active
        starRocksAssert.refreshMV("refresh materialized view " + mvName + " with sync mode");
        Assertions.assertTrue(mv.isActive());

        // clear the grace period and active it again
        starRocksAssert.dropTable(baseTableName);
        starRocksAssert.withTable(createTableSql);
        checker.runForTest(true);
        Assertions.assertTrue(mv.isActive());

        checker.start();
        starRocksAssert.dropTable(baseTableName);
    }

    @Test
    public void testActiveCheckerBackoff() {
        MVActiveChecker.MvActiveInfo activeInfo = MVActiveChecker.MvActiveInfo.firstFailure();
        Assertions.assertTrue(activeInfo.isInGracePeriod());

        LocalDateTime start = LocalDateTime.now(TimeUtils.getSystemTimeZone().toZoneId());
        for (int i = 0; i < 10; i++) {
            activeInfo.next();
        }
        Assertions.assertTrue(activeInfo.isInGracePeriod());
        Duration d = Duration.between(start, activeInfo.getNextActive());
        Assertions.assertEquals(d.toMinutes(), MVActiveChecker.MvActiveInfo.MAX_BACKOFF_MINUTES);
    }

    @Test
    public void testAlterBaseTableWithOptimizePartition() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_t1 (\n" +
                "  k1 int,\n" +
                "  k2 date,\n" +
                "  k3 string\n" +
                "  )\n" +
                "  DUPLICATE KEY(k1)\n" +
                "  PARTITION BY date_trunc(\"day\", k2);");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1 \n" +
                " partition by (date_trunc(\"day\", k2))\n" +
                " REFRESH MANUAL\n" +
                " AS select sum(k1), k2 from base_t1 group by k2;");
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "test_mv1");
        Assertions.assertTrue(mv.isActive());
        String sql = "alter table base_t1 partition by date_trunc(\"month\", k2);";
        starRocksAssert.ddl(sql);
        mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "test_mv1");
        Assertions.assertFalse(mv.isActive());
        Assertions.assertTrue(mv.getInactiveReason().contains("base-table optimized:"));
    }

    @Test
    public void testMaterializedViewRename() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_t1 (\n" +
                "  k1 int,\n" +
                "  k2 date,\n" +
                "  k3 string\n" +
                "  )\n" +
                "  DUPLICATE KEY(k1)\n" +
                "  PARTITION BY date_trunc(\"day\", k2);");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1 \n" +
                " partition by (date_trunc(\"day\", k2))\n" +
                " REFRESH MANUAL\n" +
                " AS select sum(k1), k2 from base_t1 group by k2;");
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "test_mv1");
        Assertions.assertTrue(mv.isActive());
        executeInsertSql(connectContext, "INSERT INTO base_t1 VALUES (1,'2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2," +
                "'2020-07-02','SH');");
        String sql = "ALTER MATERIALIZED VIEW test_mv1 rename test_mv2;";
        starRocksAssert.ddl(sql);
        mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "test_mv2");
        Assertions.assertTrue(mv.isActive());
        starRocksAssert.query("select * from test_mv2");
        starRocksAssert.refreshMV("REFRESH MATERIALIZED VIEW test_mv2 with sync mode;");
    }

    @Test
    public void testMultiPartitionColumnsMaterializedVieSwap() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_t1 (\n" +
                "                    k1 int,\n" +
                "                    k2 date,\n" +
                "                    k3 string\n" +
                "                )\n" +
                "                DUPLICATE KEY(k1)\n" +
                "                PARTITION BY date_trunc(\"day\", k2), k3;");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1\n" +
                "                partition by (date_trunc(\"day\", k2), k3)\n" +
                "                REFRESH MANUAL\n" +
                "                AS select sum(k1), k2, k3 from base_t1 group by k2, k3;");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv2\n" +
                "                partition by (date_trunc(\"day\", k2), k3)\n" +
                "                REFRESH MANUAL\n" +
                "                AS select avg(k1), k2, k3 from base_t1 group by k2, k3;");
        executeInsertSql(connectContext, "INSERT INTO base_t1 VALUES (1,'2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2," +
                "'2020-07-02','SH');");
        String sql = "ALTER MATERIALIZED VIEW test_mv1 SWAP WITH test_mv2;";
        starRocksAssert.ddl(sql);
        MaterializedView mv1 = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "test_mv1");
        Assertions.assertTrue(mv1.isActive());

        MaterializedView mv2 = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "test_mv2");
        Assertions.assertTrue(mv2.isActive());
        starRocksAssert.query("select * from test_mv2");
        starRocksAssert.refreshMV("REFRESH MATERIALIZED VIEW test_mv2 with sync mode;");
    }

    @Test
    public void testAlterMVEnableQueryRewriteProperty() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_t1 (\n" +
                "                    k1 int,\n" +
                "                    k2 date,\n" +
                "                    k3 string\n" +
                "                )\n" +
                "                DUPLICATE KEY(k1)\n" +
                "                PARTITION BY date_trunc(\"day\", k2), k3;");
        starRocksAssert.withRefreshedMaterializedView("CREATE MATERIALIZED VIEW test_mv1\n" +
                "                partition by (date_trunc(\"day\", k2), k3)\n" +
                "                REFRESH MANUAL\n" +
                "                AS select sum(k1), k2, k3 from base_t1 group by k2, k3;");

        String query = "select k2, k3, sum(k1) from base_t1 group by k2, k3 order by k2, k3;";
        starRocksAssert.query(query).explainContains("test_mv1");

        {
            String alterMvSql = "alter materialized view test_mv1 set (\"enable_query_rewrite\" = \"false\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            currentState.getLocalMetastore().alterMaterializedView(stmt);
            starRocksAssert.query(query).explainWithout("test_mv1");
        }

        {
            String alterMvSql = "alter materialized view test_mv1 set (\"enable_query_rewrite\" = \"true\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            currentState.getLocalMetastore().alterMaterializedView(stmt);
            starRocksAssert.query(query).explainContains("test_mv1");
        }
    }

    @Test
    public void testMVPausedWithConsecutiveFailCount1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_t1 (\n" +
                "   k1 int,\n" +
                "   k2 date,\n" +
                "   k3 string\n" +
                ")\n" +
                "DUPLICATE KEY(k1);");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_refresh_fail_mv1\n" +
                "REFRESH MANUAL\n" +
                "AS select sum(k1), k2, k3 from base_t1 group by k2, k3;");
        executeInsertSql("insert into base_t1 values(1, '2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2,'2020-07-02','SH');");
        MaterializedView mv = getMv("test_refresh_fail_mv1");
        Config.max_task_consecutive_fail_count = 3;
        for (int i = 0; i < Config.max_task_consecutive_fail_count; i++) {
            new MockUp<PartitionBasedMvRefreshProcessor>() {
                @Mock
                public Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception {
                    throw new RuntimeException("Mocked exception");
                }
            };
            try {
                refreshMV("test", mv);
            } catch (Exception e) {
                // do nothing
            }
        }
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(mv);
        Assertions.assertEquals(Config.max_task_consecutive_fail_count, task.getConsecutiveFailCount());
        Assertions.assertEquals(Constants.TaskState.PAUSE, task.getState());

        Map<String, List<TaskRunStatus>> taskNameJobStatusMap =
                taskManager.listMVRefreshedTaskRunStatus(DB_NAME, Set.of("test_refresh_fail_mv1"));
        List<TaskRunStatus> taskRunStatuses = taskNameJobStatusMap.getOrDefault(mv.getName(), Lists.newArrayList());
        ShowMaterializedViewStatus mvStatus = ShowMaterializedViewStatus.of("test", mv, taskRunStatuses);
        Assertions.assertFalse(mv.isActive());
        Config.max_task_consecutive_fail_count = 10;
    }

    @Test
    public void testMVPausedWithConsecutiveFailCount2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_t1 (\n" +
                "   k1 int,\n" +
                "   k2 date,\n" +
                "   k3 string\n" +
                ")\n" +
                "DUPLICATE KEY(k1);");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_refresh_fail_mv2\n" +
                "REFRESH MANUAL\n" +
                "AS select sum(k1), k2, k3 from base_t1 group by k2, k3;");
        executeInsertSql("insert into base_t1 values(1, '2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2,'2020-07-02','SH');");
        MaterializedView mv = getMv("test_refresh_fail_mv2");
        Config.max_task_consecutive_fail_count = 3;
        for (int i = 0; i < Config.max_task_consecutive_fail_count - 1; i++) {
            new MockUp<PartitionBasedMvRefreshProcessor>() {
                @Mock
                public Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception {
                    throw new RuntimeException("Mocked exception");
                }
            };
            try {
                refreshMV("test", mv);
            } catch (Exception e) {
                // do nothing
            }
        }
        // refresh success
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception {
                return Constants.TaskRunState.SUCCESS;
            }
        };
        refreshMV("test", mv);
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(mv);
        Assertions.assertEquals(0, task.getConsecutiveFailCount());
        Assertions.assertNotEquals(Constants.TaskState.PAUSE, task.getState());
        Assertions.assertTrue(mv.isActive());
        Config.max_task_consecutive_fail_count = 10;
    }
}
