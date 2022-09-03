// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ShowCreateMaterializedViewStmtTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .useDatabase("test");
    }

    @Test
    public void testNormal() throws Exception {
        ShowCreateTableStmt stmt =
                new ShowCreateTableStmt(new TableName("test", "mv1"),
                        ShowCreateTableStmt.CreateTableType.MATERIALIZED_VIEW);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW CREATE MATERIALIZED VIEW test.mv1", stmt.toString());
        Assert.assertEquals("test", stmt.getDb());
        Assert.assertEquals("mv1", stmt.getTable());
        Assert.assertEquals(2, ShowCreateTableStmt.getMaterializedViewMetaData().getColumnCount());
        Assert.assertEquals("Materialized View",
                ShowCreateTableStmt.getMaterializedViewMetaData().getColumn(0).getName());
        Assert.assertEquals("Create Materialized View",
                ShowCreateTableStmt.getMaterializedViewMetaData().getColumn(1).getName());
    }

    @Test
    public void testShowSimpleCreateMvSql() throws Exception {
        String createMvSql = "create materialized view mv1 " +
                "distributed by hash(k1) " +
                "refresh manual " +
                "as select k1, k2 from tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv1");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals(createTableStmt.get(0), "CREATE MATERIALIZED VIEW `mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 10 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `test`.`tbl1`.`k1` AS `k1`, `test`.`tbl1`.`k2` AS `k2` FROM `test`.`tbl1`;");
        String copySql = createTableStmt.get(0).replaceAll("mv1", "mv1_copy");
        currentState.createMaterializedView(
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(copySql, ctx));
    }

    @Test
    public void testShowPartitionWithAliasCreateMvSql() throws Exception {
        String createMvSql = "create materialized view mv2 " +
                "partition by k3 " +
                "distributed by hash(k3) " +
                "refresh manual " +
                "as select k1 as k3, k2 from tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv2");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals(createTableStmt.get(0), "CREATE MATERIALIZED VIEW `mv2`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`k3`)\n" +
                "DISTRIBUTED BY HASH(`k3`) BUCKETS 10 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `test`.`tbl1`.`k1` AS `k3`, `test`.`tbl1`.`k2` AS `k2` FROM `test`.`tbl1`;");
        String copySql = createTableStmt.get(0).replaceAll("mv2", "mv2_copy");
        currentState.createMaterializedView(
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(copySql, ctx));
    }

    @Test
    public void testShowPartitionWithFunctionCreateMvSql() throws Exception {
        String createMvSql = "create materialized view mv3 " +
                "partition by date_trunc('month',k1)" +
                "distributed by hash(k3) " +
                "refresh manual " +
                "as select k1, k2+v1 as k3 from tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv3");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals(createTableStmt.get(0), "CREATE MATERIALIZED VIEW `mv3`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('month', `k1`))\n" +
                "DISTRIBUTED BY HASH(`k3`) BUCKETS 10 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `test`.`tbl1`.`k1` AS `k1`, `test`.`tbl1`.`k2` + `test`.`tbl1`.`v1` AS `k3` FROM `test`.`tbl1`;");
        String copySql = createTableStmt.get(0).replaceAll("mv3", "mv3_copy");
        currentState.createMaterializedView(
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(copySql, ctx));
    }

    @Test
    public void testShowPartitionWithFunctionAliasCreateMvSql() throws Exception {
        String createMvSql = "create materialized view mv4 " +
                "partition by (date_trunc('month',k3))" +
                "distributed by hash(k3) " +
                "refresh manual " +
                "as select k1 as k3, k2 from tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv4");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals(createTableStmt.get(0), "CREATE MATERIALIZED VIEW `mv4`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('month', `k3`))\n" +
                "DISTRIBUTED BY HASH(`k3`) BUCKETS 10 \n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `test`.`tbl1`.`k1` AS `k3`, `test`.`tbl1`.`k2` AS `k2` FROM `test`.`tbl1`;");
        String copySql = createTableStmt.get(0).replaceAll("mv4", "mv4_copy");
        currentState.createMaterializedView(
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(copySql, ctx));
    }

    @Test
    public void testShowPartitionWithAllPropertiesCreateMvSql() throws Exception {
        String createMvSql = "create materialized view mv5 " +
                "partition by (date_trunc('month',k3))" +
                "distributed by hash(k3) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_time\" = \"2122-12-31 23:59:59\"\n" +
                ")" +
                "as select k1 as k3, k2 from tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv5");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals(createTableStmt.get(0), "CREATE MATERIALIZED VIEW `mv5`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('month', `k3`))\n" +
                "DISTRIBUTED BY HASH(`k3`) BUCKETS 10 \n" +
                "REFRESH ASYNC START(\"2122-12-31 00:00:00\") EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_time\" = \"2122-12-31 23:59:59\"\n" +
                ")\n" +
                "AS SELECT `test`.`tbl1`.`k1` AS `k3`, `test`.`tbl1`.`k2` AS `k2` FROM `test`.`tbl1`;");
        String copySql = createTableStmt.get(0).replaceAll("mv5", "mv5_copy");
        currentState.createMaterializedView(
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(copySql, ctx));
    }

    @Test
    public void testShowRefreshWithNoStartTimeCreateMvSql() throws Exception {
        String createMvSql = "create materialized view mv6 " +
                "partition by (date_trunc('month',k3))" +
                "distributed by hash(k3) " +
                "refresh async " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_time\" = \"2122-12-31 23:59:59\"\n" +
                ")" +
                "as select k1 as k3, k2 from tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv6");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals(createTableStmt.get(0), "CREATE MATERIALIZED VIEW `mv6`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('month', `k3`))\n" +
                "DISTRIBUTED BY HASH(`k3`) BUCKETS 10 \n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_time\" = \"2122-12-31 23:59:59\"\n" +
                ")\n" +
                "AS SELECT `test`.`tbl1`.`k1` AS `k3`, `test`.`tbl1`.`k2` AS `k2` FROM `test`.`tbl1`;");
        String copySql = createTableStmt.get(0).replaceAll("mv6", "mv6_copy");
        currentState.createMaterializedView(
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(copySql, ctx));
    }

    @Test
    public void testShowRefreshWithIntervalCreateMvSql() throws Exception {
        String createMvSql = "create materialized view mv7 " +
                "partition by (date_trunc('month',k3))" +
                "distributed by hash(k3) " +
                "refresh async EVERY(INTERVAL 1 HOUR)" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_time\" = \"2122-12-31 23:59:59\"\n" +
                ")" +
                "as select k1 as k3, k2 from tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(createMvSql, ctx);
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        Table table = currentState.getDb("test").getTable("mv7");
        List<String> createTableStmt = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true);
        Assert.assertEquals(createTableStmt.get(0), "CREATE MATERIALIZED VIEW `mv7`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('month', `k3`))\n" +
                "DISTRIBUTED BY HASH(`k3`) BUCKETS 10 \n" +
                "REFRESH ASYNC EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_time\" = \"2122-12-31 23:59:59\"\n" +
                ")\n" +
                "AS SELECT `test`.`tbl1`.`k1` AS `k3`, `test`.`tbl1`.`k2` AS `k2` FROM `test`.`tbl1`;");
        String copySql = createTableStmt.get(0).replaceAll("mv7", "mv7_copy");
        currentState.createMaterializedView(
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(copySql, ctx));
    }

    @Test(expected = SemanticException.class)
    public void testNoTbl(){
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(null, ShowCreateTableStmt.CreateTableType.MATERIALIZED_VIEW);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No Exception throws.");
    }
}
