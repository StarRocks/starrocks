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


package com.starrocks.catalog;

import com.starrocks.analysis.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TemporaryTableTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Before
    public void setUp() throws Exception {
        GlobalStateMgr.getCurrentState().clear();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext).withDatabase("test").useDatabase("test");
    }

    private String getShowCreateTableResult(String table, ConnectContext ctx) throws Exception {
        String sql = "show create table " + table;
        ShowCreateTableStmt showCreateTableStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowResultSet resultSet = GlobalStateMgr.getCurrentState().getShowExecutor().execute(showCreateTableStmt, ctx);
        return resultSet.getResultRows().get(0).get(1);
    }

    @Test
    @Order(1)
    public void testCreateTemporaryTable() throws Exception {
        starRocksAssert.withTemporaryTable("create temporary table t0(" +
                "c1 int, c2 int) engine=olap " +
                "duplicate key (`c1`) distributed by hash(`c1`) properties ('replication_num'='1')");
        {
            String showCreateTable = getShowCreateTableResult("t0", connectContext);
            Assert.assertEquals("CREATE TEMPORARY TABLE `t0` (\n" +
                    "  `c1` int(11) NULL COMMENT \"\",\n" +
                    "  `c2` int(11) NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP \n" +
                    "DUPLICATE KEY(`c1`)\n" +
                    "DISTRIBUTED BY HASH(`c1`)\n" +
                    "PROPERTIES (\n" +
                    "\"compression\" = \"LZ4\",\n" +
                    "\"fast_schema_evolution\" = \"true\",\n" +
                    "\"replicated_storage\" = \"true\",\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");", showCreateTable);
        }


        // create temporary table with colocate_with property, colocate_with will be ignored
        starRocksAssert.withTemporaryTable("create temporary table t1(c1 int,c2 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1', 'colocate_with'='xx')");

        {
            String showCreateTable = getShowCreateTableResult("t1", connectContext);

            Assert.assertEquals("CREATE TEMPORARY TABLE `t1` (\n" +
                    "  `c1` int(11) NULL COMMENT \"\",\n" +
                    "  `c2` int(11) NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP \n" +
                    "DUPLICATE KEY(`c1`)\n" +
                    "DISTRIBUTED BY HASH(`c1`)\n" +
                    "PROPERTIES (\n" +
                    "\"compression\" = \"LZ4\",\n" +
                    "\"fast_schema_evolution\" = \"true\",\n" +
                    "\"replicated_storage\" = \"true\",\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");", showCreateTable);
        }
    }

    @Test
    @Order(1)
    public void testCreateTemporaryTableLike() throws Exception {
        starRocksAssert.withTable("create table t (c1 int, c2 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) buckets 3 " +
                "properties(\"replication_num\"=\"1\");");

        starRocksAssert.withTemporaryTableLike("t0", "t");
        {
            String showCreateTable = getShowCreateTableResult("t0", connectContext);
            Assert.assertEquals("CREATE TEMPORARY TABLE `t0` (\n" +
                    "  `c1` int(11) NULL COMMENT \"\",\n" +
                    "  `c2` int(11) NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP \n" +
                    "DUPLICATE KEY(`c1`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 3 \n" +
                    "PROPERTIES (\n" +
                    "\"compression\" = \"LZ4\",\n" +
                    "\"fast_schema_evolution\" = \"true\",\n" +
                    "\"replicated_storage\" = \"true\",\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");", showCreateTable);
        }

        // colocate_with property will be ignored
        starRocksAssert.withTable("create table t2 (c1 int, c2 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) buckets 3 " +
                "properties(\"replication_num\"=\"1\", \"colocate_with\"=\"xx\");");
        starRocksAssert.withTemporaryTableLike("t3", "t2");
        {
            String showCreateTable = getShowCreateTableResult("t3", connectContext);
            Assert.assertEquals("CREATE TEMPORARY TABLE `t3` (\n" +
                    "  `c1` int(11) NULL COMMENT \"\",\n" +
                    "  `c2` int(11) NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP \n" +
                    "DUPLICATE KEY(`c1`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 3 \n" +
                    "PROPERTIES (\n" +
                    "\"compression\" = \"LZ4\",\n" +
                    "\"fast_schema_evolution\" = \"true\",\n" +
                    "\"replicated_storage\" = \"true\",\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");", showCreateTable);
        }
    }

    @Test
    @Order(1)
    public void testCreateTemporaryTableAbnormal() throws Exception {
        // temporary table only support olap engine
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "temporary table only support olap engine", () -> {
            starRocksAssert.withTemporaryTable("create temporary table t (c1 int, c2 int) engine = mysql PROPERTIES\n" +
                    "(\n" +
                    "    \"host\" = \"127.0.0.1\",\n" +
                    "    \"port\" = \"3306\",\n" +
                    "    \"user\" = \"mysql_user\",\n" +
                    "    \"password\" = \"mysql_passwd\",\n" +
                    "    \"database\" = \"mysql_db_test\",\n" +
                    "    \"table\" = \"mysql_table_test\"\n" +
                    ");");
        });

        starRocksAssert.withTable("create table t (c1 int, c2 int) engine = mysql PROPERTIES\n" +
                "(\n" +
                "    \"host\" = \"127.0.0.1\",\n" +
                "    \"port\" = \"3306\",\n" +
                "    \"user\" = \"mysql_user\",\n" +
                "    \"password\" = \"mysql_passwd\",\n" +
                "    \"database\" = \"mysql_db_test\",\n" +
                "    \"table\" = \"mysql_table_test\"\n" +
                ");");
        // for create table like, source table must be olap table
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "temporary table only support olap engine", () -> {
            starRocksAssert.withTemporaryTableLike("t0", "t");
        });
    }

    @Test
    @Order(1)
    public void testCreateView() throws Exception {
        starRocksAssert.withTemporaryTable("create temporary table t1(c1 int,c2 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1', 'colocate_with'='xx')");

        // 1. cannot create view based on temporary table
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "View can't base on temporary table.", () -> {
            starRocksAssert.withView("create view v1 as select * from t1");
        });
        // 2. cannot create sync mv based on temporary table
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Materialized view can't base on temporary table.", () -> {
            starRocksAssert.withMaterializedView("create materialized view mv1 as select * from t1");
        });

        // 3. cannot create refresh mv based on temporary table
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Materialized view can't base on temporary table.", () -> {
            starRocksAssert.withRefreshedMaterializedView("create materialized view mv1 " +
                    "properties(\"replication_num\"=\"1\") " +
                    "refresh IMMEDIATE MANUAL as select * from t1");
        });

    }

    @Test
    @Order(1)
    public void testAlterTable() throws Exception {
        starRocksAssert.withTemporaryTable("create temporary table t1(c1 int,c2 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1', 'colocate_with'='xx')");

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "temporary table doesn't support alter table statement.", () -> {
                    starRocksAssert.alterTableProperties("alter table t1 add column c3 int");
            });
    }

    @Test
    @Order(1)
    public void testSubmitTask() throws Exception {
        starRocksAssert.withTemporaryTable("create temporary table t1(c1 int,c2 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1', 'colocate_with'='xx')");

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Cannot submit task based on temporary table.", () -> {
            // submit task
            String sql = "submit task task1 as insert into t1 select * from t1";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        });

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Cannot submit task based on temporary table.", () -> {
            // submit task
            String sql = "submit task task1 as create table t2 as select * from t1";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        });


    }

    @Test
    @Order(1)
    public void testNameConflict() throws Exception {
        starRocksAssert.withTable("create table t1(c1 int,c2 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1')");

        starRocksAssert.withTemporaryTable("create temporary table t1(c1 int,c2 int, c3 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1')");

        {
            String showCreateTable = getShowCreateTableResult("t1", connectContext);
            Assert.assertEquals("CREATE TEMPORARY TABLE `t1` (\n" +
                    "  `c1` int(11) NULL COMMENT \"\",\n" +
                    "  `c2` int(11) NULL COMMENT \"\",\n" +
                    "  `c3` int(11) NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP \n" +
                    "DUPLICATE KEY(`c1`)\n" +
                    "DISTRIBUTED BY HASH(`c1`)\n" +
                    "PROPERTIES (\n" +
                    "\"compression\" = \"LZ4\",\n" +
                    "\"fast_schema_evolution\" = \"true\",\n" +
                    "\"replicated_storage\" = \"true\",\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");", showCreateTable);
        }
        starRocksAssert.dropTemporaryTable("t1", false);
        {
            String showCreateTable = getShowCreateTableResult("t1", connectContext);

            Assert.assertEquals("CREATE TABLE `t1` (\n" +
                    "  `c1` int(11) NULL COMMENT \"\",\n" +
                    "  `c2` int(11) NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP \n" +
                    "DUPLICATE KEY(`c1`)\n" +
                    "DISTRIBUTED BY HASH(`c1`)\n" +
                    "PROPERTIES (\n" +
                    "\"compression\" = \"LZ4\",\n" +
                    "\"fast_schema_evolution\" = \"true\",\n" +
                    "\"replicated_storage\" = \"true\",\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");", showCreateTable);
        }
    }

    @Test
    @Order(1)
    public void testVisibility() throws Exception {
        // 2 session, can't see each other
        ConnectContext connectContext1 = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert1 = new StarRocksAssert(connectContext1).withDatabase("test").useDatabase("test");

        starRocksAssert.withTemporaryTable("create temporary table t1(c1 int,c2 int, c3 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1')");

        starRocksAssert1.withTemporaryTable("create temporary table t2(c1 int,c2 int, c3 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1')");

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class, "Table t1 is not found", () -> {
            getShowCreateTableResult("t1", connectContext1);
        });

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class, "Table t2 is not found", () -> {
            getShowCreateTableResult("t2", connectContext);
        });

    }

    @Test
    @Order(1)
    public void testAbnormalCases() throws Exception {
        ConnectContext connectContext1 = UtFrameUtils.createDefaultCtx();
        String createHiveCatalogStmt = "create external catalog hive_catalog properties (\"type\"=\"hive\", " +
                "\"hive.metastore.uris\"=\"thrift://hms:9083\")";
        StarRocksAssert starRocksAssert1 = new StarRocksAssert(connectContext1)
                .withCatalog(createHiveCatalogStmt).withDatabase("t");
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                "show temporary table is not supported under non-default catalog", () -> {
                starRocksAssert1.useCatalog("hive_catalog").useDatabase("t").show("show temporary tables");
            });

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "", () -> {
            starRocksAssert1.useCatalog("hive_catalog").useDatabase("t")
                    .withTemporaryTable("create temporary table t1(c1 int,c2 int, c3 int) " +
                    "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                    "properties('replication_num'='1')");

        });
    }

    @Test
    @Order(1)
    public void testDropTable() throws Exception {
        starRocksAssert.withTemporaryTable("create temporary table t1(c1 int,c2 int, c3 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1')");
        starRocksAssert.dropTemporaryTable("t1", false);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "", () -> {
            starRocksAssert.dropTemporaryTable("t1", false);
        });
        starRocksAssert.dropTemporaryTable("t1", true);

    }

    @Test
    @Order(1)
    public void testShowData() throws Exception {
        starRocksAssert.withTemporaryTable("create temporary table t1(c1 int,c2 int, c3 int) " +
                "engine=olap duplicate key(`c1`) distributed by hash(`c1`) " +
                "properties('replication_num'='1')");
        List<List<String>> showDataResult = starRocksAssert.show("show data from t1");
        Assert.assertEquals(showDataResult.get(0).get(0), "t1");

    }

    @Test
    @Order(2)
    public void testReplayDropTable() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test").withTemporaryTable(
                "create temporary table tmp(k1 int) duplicate key(k1) " +
                        "distributed by hash(k1) buckets 3 properties('replication_num'='1');");
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database database = starRocksAssert.getDb("test");
        Assert.assertNotNull(database);
        Table table = MetaUtils.getSessionAwareTable(connectContext, database, new TableName("test", "tmp"));
        Assert.assertNotNull(table);
        Assert.assertTrue(table.isOlapTable());
        Assert.assertTrue(((OlapTable) table).isTemporaryTable());
        // non-master node can replay drop temporary table successfully
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.FOLLOWER);
        localMetastore.replayDropTable(database, table.getId(), false);
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
    }
}
