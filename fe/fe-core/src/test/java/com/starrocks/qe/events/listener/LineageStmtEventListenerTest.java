// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe.events.listener;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.events.listener.LineageStmtEventListener.Action;
import com.starrocks.qe.events.listener.LineageStmtEventListener.ActionType;
import com.starrocks.qe.events.listener.LineageStmtEventListener.ColumnLineage;
import com.starrocks.qe.events.listener.LineageStmtEventListener.ColumnSpec;
import com.starrocks.qe.events.listener.LineageStmtEventListener.Lineage;
import com.starrocks.qe.events.listener.LineageStmtEventListener.Target;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class LineageStmtEventListenerTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static String DB_NAME = "test";
    static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private boolean print = false;

    private static LineageStmtEventListener.OutputProcessor lineageProcessor
            = new LineageStmtEventListener.OutputProcessor();
    private static LineageStmtEventListener.ChangeLogProcessor changeLogProcessor
            = new LineageStmtEventListener.ChangeLogProcessor();

    public static void init() throws Exception {
        // create connect context
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        init();
        starRocksAssert.useDatabase("test")
                .withTable("create table t1 (id bigint, name string) PROPERTIES (\"replication_num\" = \"1\")");
        starRocksAssert.useDatabase("test")
                .withTable("create table t2 (id2 bigint, name2 string) PROPERTIES (\"replication_num\" = \"1\")");
        starRocksAssert.useDatabase("test")
                .withTable("create table t3 (id3 bigint, name3 string) PROPERTIES (\"replication_num\" = \"1\")");
        starRocksAssert.useDatabase("test")
                .withTable("create table t21 (id3 bigint, name3 string) PROPERTIES (\"replication_num\" = \"1\")");
        starRocksAssert.useDatabase("test")
                .withTable("create table t22 (id2 bigint, name2 string, addr string) PROPERTIES (\"replication_num\" = \"1\")");
    }

    private void printLineage(Lineage lineage, StatementBase stmt) {
        if (!print) {
            return;
        }
        if (lineage != null) {
            lineage.setQueryText(stmt.getOrigStmt().originStmt);
            lineage.setActionType(ActionType.Lineage);
            lineage.setQueryId("1234568");
            lineage.setTimestamp(System.currentTimeMillis());
            lineage.setCostTime(1);
            lineage.setClientIp("127.0.0.1");
            lineage.setUser("root");
            System.out.println(gson.toJson(lineage));
        }
    }

    private void printChangeLog(Lineage lineage, StatementBase stmt) {
        if (!print) {
            return;
        }
        if (lineage != null) {
            lineage.setQueryText(stmt.getOrigStmt().originStmt);
            lineage.setActionType(ActionType.ChangeLog);
            lineage.setQueryId("1234568");
            lineage.setTimestamp(System.currentTimeMillis());
            lineage.setCostTime(1);
            lineage.setClientIp("127.0.0.1");
            lineage.setUser("root");
            System.out.println(gson.toJson(lineage));
        }
    }

    @Test
    public void testInsertValues() throws Exception {
        String sql = "insert into  t1 values (1, '2'), (2, '3');";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("t1", columnLineages.get(0).getDestTable());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertBySelf() throws Exception {
        String sql = "insert into  t1 select * from t1;";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("test", columnLineages.get(0).getDestDatabase());
        Assert.assertEquals("test", columnLineages.get(0).getSrcDatabase());
        Assert.assertEquals("t1", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Iterator<Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        entry1 = iter1.next();
        Assert.assertEquals("name", entry1.getKey());
        Assert.assertEquals("name", entry1.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertBySelfWithLiteral() throws Exception {
        String sql = "insert into  t22 select id as id3, 'addd' as addr2333, name as name2 from t1;";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());

        Assert.assertEquals("test", columnLineages.get(0).getDestDatabase());
        Assert.assertEquals("test", columnLineages.get(0).getSrcDatabase());
        Assert.assertEquals("t22", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Iterator<Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id2", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        entry1 = iter1.next();
        Assert.assertEquals("addr", entry1.getKey());
        Assert.assertEquals("name", entry1.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertIntoWithLiteral() throws Exception {
        String sql = "CREATE TABLE `asasian_games_medals` (\n"
                + "  `year` varchar(128) NULL COMMENT \"年份，枚举值从1951至2018（注：每四年一届）\",\n"
                + "  `region` varchar(128) NULL COMMENT \"国家或地区的名称（包括缩写），例如：China (CHN)、Indonesia (INA)等\",\n"
                + "  `gold` bigint(20) NULL COMMENT \"金牌数量\",\n"
                + "  `silver` bigint(20) NULL COMMENT \"银牌数量\",\n"
                + "  `bronze` bigint(20) NULL COMMENT \"铜牌数量\",\n"
                + "  `total` bigint(20) NULL COMMENT \"全部奖牌数量\"\n"
                + ") ENGINE=OLAP \n"
                + "DUPLICATE KEY(`year`)\n"
                + "COMMENT \"本数据集包含了从1951年至2018年历届亚运会中各国家和地区获得奖牌的情况。\"\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"enable_persistent_index\" = \"false\",\n"
                + "\"replicated_storage\" = \"true\",\n"
                + "\"compression\" = \"LZ4\"\n"
                + ");";
        starRocksAssert.useDatabase("test").withTable(sql);
        String sql2 = "INSERT INTO asasian_games_medals(`year`, `region`, `gold`, `silver`, `bronze`, `total`)\n"
                + "SELECT\n"
                + "    2026 AS `year`, region, gold, silver, bronze, total\n"
                + "  FROM asasian_games_medals\n"
                + " WHERE region = 'China (CHN)'\n"
                + ";";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());

        Assert.assertEquals("test", columnLineages.get(0).getDestDatabase());
        Assert.assertEquals("test", columnLineages.get(0).getSrcDatabase());
        Assert.assertEquals("asasian_games_medals", columnLineages.get(0).getDestTable());
        Assert.assertEquals("asasian_games_medals", columnLineages.get(0).getSrcTable());
        Iterator<Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("region", entry1.getKey());
        Assert.assertEquals("region", entry1.getValue().iterator().next());
        entry1 = iter1.next();
        Assert.assertEquals("gold", entry1.getKey());
        Assert.assertEquals("gold", entry1.getValue().iterator().next());
        entry1 = iter1.next();
        Assert.assertEquals("silver", entry1.getKey());
        Assert.assertEquals("silver", entry1.getValue().iterator().next());
        entry1 = iter1.next();
        Assert.assertEquals("bronze", entry1.getKey());
        Assert.assertEquals("bronze", entry1.getValue().iterator().next());
        entry1 = iter1.next();
        Assert.assertEquals("total", entry1.getKey());
        Assert.assertEquals("total", entry1.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertWhereIn() throws Exception {
        String sql = " insert into t3 SELECT * from  t1 where id in (select id2 from t2);";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());

        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertWhereNotIn() throws Exception {
        String sql = " insert into t3 SELECT * from  t1 where id not in (select id2 from t2);";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());

        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertWhereExists() throws Exception {
        String sql = " insert into t3 SELECT * from  t1 where exists (select * from t2 where id2=t1.id);";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());

        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertWhereNotExists() throws Exception {
        String sql = " insert into t3 SELECT * from  t1 where not exists (select * from t2 where id2=t1.id);";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());

        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertUnion() throws Exception {
        String sql = "insert into t3(id3) select t1_1.id as id from t1  t1_1 union  select t2_2.id2 as id from t2  t2_2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();

        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getDestCatalog());

        Assert.assertEquals(1, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());

        Assert.assertEquals("t3", columnLineages.get(1).getDestTable());
        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getDestCatalog());

        Assert.assertEquals(1, columnLineages.get(1).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter2 = columnLineages.get(1).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry2 = iter2.next();
        Assert.assertEquals("id3", entry2.getKey());
        Assert.assertEquals("id2", entry2.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertUnionWithLiteral() throws Exception {
        String sql = "insert into t22(id2,name2, addr) select t1_1.id as id1_alias,truncate(t1_1.name,'trunc_col') "
                + " as name_alias, "
                + " 'table1' as addr from t1  t1_1 "
                + "union  select t2_2.id2 as id, t2_2.name2 as name2, t2_2.name2 as addr from t2  t2_2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();

        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("t22", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getDestCatalog());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id2", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        entry1 = iter1.next();
        Assert.assertEquals("name2", entry1.getKey());
        Assert.assertEquals("name", entry1.getValue().iterator().next());

        Assert.assertEquals("t22", columnLineages.get(1).getDestTable());
        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getDestCatalog());

        Assert.assertEquals(3, columnLineages.get(1).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter2 = columnLineages.get(1).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry2 = iter2.next();
        Assert.assertEquals("id2", entry2.getKey());
        Assert.assertEquals("id2", entry2.getValue().iterator().next());
        entry2 = iter2.next();
        Assert.assertEquals("name2", entry2.getKey());
        Assert.assertEquals("name2", entry2.getValue().iterator().next());
        entry2 = iter2.next();
        Assert.assertEquals("addr", entry2.getKey());
        Assert.assertEquals("name2", entry2.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertUnionWithLiteral2() throws Exception {
        String sql = "insert into t22(id2,name2) select id_alias, test_alias from "
                + "(select id as id_alias,'name' as name_alais, truncate('test','tes2') as test_alias from t1 as t3) t1_1";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();

        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("t22", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getDestCatalog());

        Assert.assertEquals(1, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id2", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());

        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertUnionAll() throws Exception {
        String sql = "insert into t3(id3,name3) select t1_1.id as id,t1_1.name as name from t1  t1_1 "
                + "union  all select t2_2.id2 as id , t2_2.name2 as name2 from t2  t2_2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getDestCatalog());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());

        Assert.assertEquals("t3", columnLineages.get(1).getDestTable());
        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getDestCatalog());

        Assert.assertEquals(2, columnLineages.get(1).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter2 = columnLineages.get(1).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry3 = iter2.next();
        Assert.assertEquals("id3", entry3.getKey());
        Assert.assertEquals("id2", entry3.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry4 = iter2.next();
        Assert.assertEquals("name3", entry4.getKey());
        Assert.assertEquals("name2", entry4.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertAsSelectWithExcept() throws Exception {
        String sql = "insert into t3(id3,name3)  select t1_1.id as id,t1_1.name as name from t1  t1_1 "
                + "except (select t2_2.id2 as id , t2_2.name2 as name2 from t2  t2_2)";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getDestCatalog());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());

        Assert.assertEquals("t3", columnLineages.get(1).getDestTable());
        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getDestCatalog());

        Assert.assertEquals(2, columnLineages.get(1).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter2 = columnLineages.get(1).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry3 = iter2.next();
        Assert.assertEquals("id3", entry3.getKey());
        Assert.assertEquals("id2", entry3.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry4 = iter2.next();
        Assert.assertEquals("name3", entry4.getKey());
        Assert.assertEquals("name2", entry4.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertAsSelectWithMinus() throws Exception {
        String sql = "insert into t3(id3,name3)  select t1_1.id as id,t1_1.name as name from t1  t1_1 "
                + "minus (select t2_2.id2 as id , t2_2.name2 as name2 from t2  t2_2)";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getDestCatalog());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());

        Assert.assertEquals("t3", columnLineages.get(1).getDestTable());
        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getDestCatalog());

        Assert.assertEquals(2, columnLineages.get(1).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter2 = columnLineages.get(1).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry3 = iter2.next();
        Assert.assertEquals("id3", entry3.getKey());
        Assert.assertEquals("id2", entry3.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry4 = iter2.next();
        Assert.assertEquals("name3", entry4.getKey());
        Assert.assertEquals("name2", entry4.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertAsSelectWithIntersect() throws Exception {
        String sql = "insert into t3(id3,name3)  select t1_1.id as id,t1_1.name as name from t1  t1_1 "
                + "intersect (select t2_2.id2 as id , t2_2.name2 as name2 from t2  t2_2)";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getDestCatalog());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());

        Assert.assertEquals("t3", columnLineages.get(1).getDestTable());
        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getSrcCatalog());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getDestCatalog());

        Assert.assertEquals(2, columnLineages.get(1).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter2 = columnLineages.get(1).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry3 = iter2.next();
        Assert.assertEquals("id3", entry3.getKey());
        Assert.assertEquals("id2", entry3.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry4 = iter2.next();
        Assert.assertEquals("name3", entry4.getKey());
        Assert.assertEquals("name2", entry4.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertOverwrite() throws Exception {
        String sql = "insert overwrite t3 select * from t1";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testInsertOverwriteWithUnion() throws Exception {
        String sql = "insert overwrite t3 select * from (select *  from t1 ) a union select * from t2 b";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.Insert, lineage.getAction());

        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("t3", columnLineages.get(0).getDestTable());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id3", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name3", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());

        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals("t3", columnLineages.get(1).getDestTable());

        Assert.assertEquals(2, columnLineages.get(1).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter2 = columnLineages.get(1).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry3 = iter2.next();
        Assert.assertEquals("id3", entry3.getKey());
        Assert.assertEquals("id2", entry3.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry4 = iter2.next();
        Assert.assertEquals("name3", entry4.getKey());
        Assert.assertEquals("name2", entry4.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateAsSelect() throws Exception {
        String sql = "/* name=test1 */ create table t3_2 as select  id as id2, name  /* name=test2 */ from t1 b";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateTableAsSelect, lineage.getAction());

        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("t3_2", columnLineages.get(0).getDestTable());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id2", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());

        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateAsLike() throws Exception {
        String sql = "/* name=test1 */ create table t10 like t2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateTableLike, lineage.getAction());

        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("t2", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("t10", columnLineages.get(0).getDestTable());

        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id2", entry1.getKey());
        Assert.assertEquals("id2", entry1.getValue().iterator().next());

        Map.Entry<String, Set<String>> entry2 = iter1.next();
        Assert.assertEquals("name2", entry2.getKey());
        Assert.assertEquals("name2", entry2.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateWithPartitionRange() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS pt_crashes_on_poor_visibility(\n"
                + "    crash_date  DATE  COMMENT '碰撞时间',\n"
                + "    temp_f  STRING  COMMENT '温度（华氏度）',\n"
                + "    visibility  STRING  COMMENT '能见度',\n"
                + "    precipitation  STRING  COMMENT '降水量',\n"
                + "    crash_count  INT  COMMENT '碰撞次数'\n"
                + ")\n"
                + "COMMENT \"the number of crashes when visibility is poo\"\n"
                + "PARTITION BY RANGE(crash_date) (\n"
                + "    START (\"2014-01-01\") END (\"2014-10-01\") EVERY (INTERVAL 1 MONTH)\n"
                + ")\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\"\n"
                + ");";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateTable, lineage.getAction());

        Target target = lineage.getTarget();
        Map<String, Object> map = (Map<String, Object>) target.getExtra().get("partitionDesc");
        Assert.assertEquals("RANGE", map.get("partitionType"));
        Assert.assertEquals("crash_date", ((List<String>) map.get("partitionColNames")).get(0));

        List<Map<String, Object>> partitions = (List<Map<String, Object>>) target.getExtra().get("partitions");
        Assert.assertEquals(9, partitions.size());
        Assert.assertEquals("p201401", partitions.get(0).get("name"));
        Assert.assertEquals("RANGE", partitions.get(0).get("type"));
        Assert.assertEquals(Short.parseShort("1"), partitions.get(0).get("replicationNum"));
        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateWithPartitionList() throws Exception {
        String sql = "CREATE TABLE t_recharge_detail1 (\n"
                + "    id bigint,\n"
                + "    user_id bigint,\n"
                + "    recharge_money decimal(32,2), \n"
                + "    city varchar(20) not null,\n"
                + "    dt varchar(20) not null\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "PARTITION BY LIST (city) (\n"
                + "   PARTITION pLos_Angeles VALUES IN (\"Los Angeles\"),\n"
                + "   PARTITION pSan_Francisco VALUES IN (\"San Francisco\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`id`);";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateTable, lineage.getAction());

        Target target = lineage.getTarget();
        Map<String, Object> map = (Map<String, Object>) target.getExtra().get("partitionDesc");
        Assert.assertEquals("LIST", map.get("partitionType"));
        Assert.assertEquals("city", ((List<String>) map.get("partitionColNames")).get(0));

        List<Map<String, Object>> partitions = (List<Map<String, Object>>) target.getExtra().get("partitions");
        Assert.assertEquals(2, partitions.size());
        Assert.assertEquals("pLos_Angeles", partitions.get(0).get("name"));
        Assert.assertEquals("Los Angeles", ((List<String>) partitions.get(0).get("values")).get(0));
        Assert.assertEquals("pSan_Francisco", partitions.get(1).get("name"));
        Assert.assertEquals("San Francisco", ((List<String>) partitions.get(1).get("values")).get(0));
        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateWithPartitionMultiList() throws Exception {
        String sql = "CREATE TABLE t_recharge_detail4 (\n"
                + "    id bigint,\n"
                + "    user_id bigint,\n"
                + "    recharge_money decimal(32,2), \n"
                + "    city varchar(20) not null,\n"
                + "    dt varchar(20) not null\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "PARTITION BY LIST (dt,city) (\n"
                + "   PARTITION p202204_California VALUES IN (\n"
                + "       (\"2022-04-01\", \"Los Angeles\"),\n"
                + "       (\"2022-04-01\", \"San Francisco\"),\n"
                + "       (\"2022-04-02\", \"Los Angeles\"),\n"
                + "       (\"2022-04-02\", \"San Francisco\")\n"
                + "    ),\n"
                + "   PARTITION p202204_Texas VALUES IN (\n"
                + "       (\"2022-04-01\", \"Houston\"),\n"
                + "       (\"2022-04-01\", \"Dallas\"),\n"
                + "       (\"2022-04-02\", \"Houston\"),\n"
                + "       (\"2022-04-02\", \"Dallas\")\n"
                + "   )\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`id`);";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateTable, lineage.getAction());

        Target target = lineage.getTarget();
        Map<String, Object> map = (Map<String, Object>) target.getExtra().get("partitionDesc");
        Assert.assertEquals("LIST", map.get("partitionType"));
        Assert.assertEquals("dt", ((List<String>) map.get("partitionColNames")).get(0));
        Assert.assertEquals("city", ((List<String>) map.get("partitionColNames")).get(1));

        List<Map<String, Object>> partitions = (List<Map<String, Object>>) target.getExtra().get("partitions");
        Assert.assertEquals(2, partitions.size());
        Assert.assertEquals("p202204_California", partitions.get(0).get("name"));
        Assert.assertEquals("LIST", partitions.get(0).get("type"));
        Assert.assertEquals("2022-04-01", ((List<String>) ((List<List>) partitions.get(0).get("values")).get(0)).get(0));
        Assert.assertEquals("Los Angeles", ((List<String>) ((List<List>) partitions.get(0).get("values")).get(0)).get(1));

        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateWithPartitionNorm() throws Exception {
        String sql = "CREATE TABLE site_access1 (\n"
                + "    event_day DATETIME NOT NULL,\n"
                + "    site_id INT DEFAULT '10',\n"
                + "    city_code VARCHAR(100),\n"
                + "    user_name VARCHAR(32) DEFAULT '',\n"
                + "    pv BIGINT DEFAULT '0'\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n"
                + "PARTITION BY date_trunc('day', event_day)\n"
                + "DISTRIBUTED BY HASH(event_day, site_id);";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTable, lineage.getAction());

        Target target = lineage.getTarget();
        Map<String, Object> map = (Map<String, Object>) target.getExtra().get("partitionDesc");
        Assert.assertEquals("event_day", ((List<String>) map.get("partitionColNames")).get(0));

        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateWithPartitionNormMultiKey() throws Exception {
        String sql = "CREATE TABLE t_recharge_detail1 (\n"
                + "    id bigint,\n"
                + "    user_id bigint,\n"
                + "    recharge_money decimal(32,2), \n"
                + "    city varchar(20) not null,\n"
                + "    dt varchar(20) not null\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "PARTITION BY (dt,city)\n"
                + "DISTRIBUTED BY HASH(`id`)"
                + "PROPERTIES(\n"
                + "    \"partition_live_number\" = \"3\" -- only retains the most recent three partitions\n"
                + ");";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTable, lineage.getAction());

        Target target = lineage.getTarget();
        Map<String, Object> map = (Map<String, Object>) target.getExtra().get("partitionDesc");
        Assert.assertEquals("dt", ((List<String>) map.get("partitionColNames")).get(0));
        Assert.assertEquals("city", ((List<String>) map.get("partitionColNames")).get(1));

        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateWithPartitionDynamic() throws Exception {
        String sql = "CREATE TABLE site_access(\n"
                + "    event_day DATE,\n"
                + "    site_id INT DEFAULT '10',\n"
                + "    city_code VARCHAR(100),\n"
                + "    user_name VARCHAR(32) DEFAULT '',\n"
                + "    pv BIGINT DEFAULT '0'\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n"
                + "PARTITION BY RANGE(event_day)(\n"
                + "    PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n"
                + "    PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n"
                + "    PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n"
                + "    PARTITION p20200324 VALUES LESS THAN (\"2020-03-25\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day, site_id)\n"
                + "PROPERTIES(\n"
                + "    \"dynamic_partition.enable\" = \"true\",\n"
                + "    \"dynamic_partition.time_unit\" = \"DAY\",\n"
                + "    \"dynamic_partition.start\" = \"-3\",\n"
                + "    \"dynamic_partition.end\" = \"3\",\n"
                + "    \"dynamic_partition.prefix\" = \"p\",\n"
                + "    \"dynamic_partition.buckets\" = \"32\",\n"
                + "    \"dynamic_partition.history_partition_num\" = \"0\"\n"
                + ");";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTable, lineage.getAction());

        Target target = lineage.getTarget();
        Map<String, Object> map = (Map<String, Object>) target.getExtra().get("partitionDesc");
        Assert.assertEquals("event_day", ((List<String>) map.get("partitionColNames")).get(0));
        Assert.assertEquals("RANGE", map.get("partitionType"));

        List<Map<String, Object>> partitions = (List<Map<String, Object>>) target.getExtra().get("partitions");
        Assert.assertEquals(4, partitions.size());
        Assert.assertEquals("p20200321", partitions.get(0).get("name"));
        Assert.assertEquals("p20200322", partitions.get(1).get("name"));
        Assert.assertEquals("p20200323", partitions.get(2).get("name"));
        Assert.assertEquals("p20200324", partitions.get(3).get("name"));
        Map<String, String> properties = (Map<String, String>) target.getExtra().get("properties");
        Assert.assertEquals("true", properties.get("dynamic_partition.enable"));
        Assert.assertEquals("DAY", properties.get("dynamic_partition.time_unit"));
        Assert.assertEquals("-3", properties.get("dynamic_partition.start"));
        Assert.assertEquals("3", properties.get("dynamic_partition.end"));
        Assert.assertEquals("p", properties.get("dynamic_partition.prefix"));
        Assert.assertEquals("32", properties.get("dynamic_partition.buckets"));
        Assert.assertEquals("0", properties.get("dynamic_partition.history_partition_num"));

        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateAsSelectByJoin() throws Exception {
        String sql = "create table t11 as ((select a.id as id,b.id2 as id2, b.name2 as name from t1 a "
                + "join t2 b where a.id = b.id2 ))";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateTableAsSelect, lineage.getAction());

        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("t11", columnLineages.get(0).getDestTable());
        Assert.assertEquals(1, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter1 = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter1.next();
        Assert.assertEquals("id", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());

        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals("t11", columnLineages.get(1).getDestTable());
        Assert.assertEquals(2, columnLineages.get(1).getColumnMap().size());

        Iterator<Map.Entry<String, Set<String>>> iter2 = columnLineages.get(1).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry2 = iter2.next();
        Assert.assertEquals("id2", entry2.getKey());
        Assert.assertEquals("id2", entry2.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry3 = iter2.next();
        Assert.assertEquals("name", entry3.getKey());
        Assert.assertEquals("name2", entry3.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateAsSelectByJoinNested() throws Exception {
        String sql = "create table t4 as select * from (select t1_1.id as id,t2_2.name2 as name  from t1  t1_1 "
                + "join t2 t2_2 on t1_1.id = t2_2.id2) c";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateTableAsSelect, lineage.getAction());

        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(2, columnLineages.size());

        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("t4", columnLineages.get(0).getDestTable());
        Assert.assertEquals(1, columnLineages.get(0).getColumnMap().size());

        Iterator<Map.Entry<String, Set<String>>> iter = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter.next();
        Assert.assertEquals("id", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());

        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals("t4", columnLineages.get(1).getDestTable());
        Assert.assertEquals(1, columnLineages.get(1).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter2 = columnLineages.get(1).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry2 = iter2.next();
        Assert.assertEquals("name", entry2.getKey());
        Assert.assertEquals("name2", entry2.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testDataLoading() throws Exception {
        String c1 = "CREATE TABLE user_behavior\n"
                + "(\n"
                + "    UserID int(11),\n"
                + "    ItemID int(11),\n"
                + "    CategoryID int(11),\n"
                + "    BehaviorType varchar(65533),\n"
                + "    Timestamp datetime\n"
                + ")\n"
                + "ENGINE = OLAP \n"
                + "DUPLICATE KEY(UserID)\n"
                + "DISTRIBUTED BY HASH(UserID)\n"
                + "properties (\"replication_num\"=\"1\") \n"
                + "\n";

        starRocksAssert.useDatabase("test").withTable(c1);

        String sql = "LOAD LABEL user_behavior_label\n"
                + "(\n"
                + "    DATA INFILE(\"oss://datamap-dlf-oss/starrocks/test1.parquet\")\n"
                + "    INTO TABLE user_behavior\n"
                + "    FORMAT AS \"parquet\"\n"
                + " )\n"
                + " WITH BROKER\n"
                + " (\n"
                + "    \"fs.oss.endpoint\" = \"oss-cn-xxx.aliyuncs.com\",\n"
                + "    \"fs.oss.accessKeyId\" = \"xxx\",\n"
                + "    \"fs.oss.accessKeySecret\" = \"xxx\"\n"
                + " )\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    \"timeout\" = \"72000\"\n"
                + ");";

        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        LoadStmt stmt = (LoadStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(Action.DataLoading, lineage.getAction());
        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateView() throws Exception {
        String sql = " create view t4 as select * from t1";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateViewStmt stmt = (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateView, lineage.getAction());

        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("t4", columnLineages.get(0).getDestTable());
        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter.next();
        Assert.assertEquals("id", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter.next();
        Assert.assertEquals("name", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateCatalog() throws Exception {
        String sql = "CREATE EXTERNAL CATALOG hive_metastore_catalog\n"
                + "PROPERTIES(\n"
                + "   \"type\"=\"hive\",\n"
                + "   \"hive.metastore.uris\"=\"thrift://101.132.172.61:9083\"\n"
                + ");\n";

        CreateCatalogStmt createCatalogStmt = (CreateCatalogStmt) UtFrameUtils
                .parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog(createCatalogStmt);
        String sql2 = " create MATERIALIZED view mv_1 REFRESH MANUAL as select id,name "
                + "from hive_metastore_catalog.hive_db_1.hive_t_1";
        CreateMaterializedViewStatement stmt = (CreateMaterializedViewStatement) UtFrameUtils
                .parseStmtWithNewParser(sql2, starRocksAssert.getCtx());
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateMaterializedView, lineage.getAction());

        System.out.println(gson.toJson(lineage));
    }

    @Test
    public void testCreateMaterializedTableSync() throws Exception {
        String sql = " create MATERIALIZED view t4 as select id,name from t1";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateMaterializedViewStmt stmt = (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.parseLineage(stmt);
        Assert.assertEquals(Action.CreateMaterializedView, lineage.getAction());

        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals("t4", columnLineages.get(0).getDestTable());
        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Iterator<Map.Entry<String, Set<String>>> iter = columnLineages.get(0).getColumnMap().entrySet().iterator();
        Map.Entry<String, Set<String>> entry1 = iter.next();
        Assert.assertEquals("id", entry1.getKey());
        Assert.assertEquals("id", entry1.getValue().iterator().next());
        Map.Entry<String, Set<String>> entry2 = iter.next();
        Assert.assertEquals("name", entry2.getKey());
        Assert.assertEquals("name", entry2.getValue().iterator().next());
        printLineage(lineage, stmt);
    }

    //output
    @Test
    public void testOutputCreateTable() throws Exception {
        String sql = " create table t2_1(id int, name string)";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTable, lineage.getAction());
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("t2_1", columnLineages.get(0).getDestTable());
        printLineage(lineage, stmt);
    }

    @Test
    public void testOutputInsertValues() throws Exception {
        String sql = "insert into  t1(id) values (1), (2);";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.doProcess(stmt);
        Assert.assertEquals(Action.Insert, lineage.getAction());
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("t1", columnLineages.get(0).getDestTable());
    }

    //function
    @Test
    public void testLineageWithFunction() throws Exception {
        String c1 = "CREATE TABLE `crashdata` (\n"
                + "  `CRASH_DATE` datetime NULL COMMENT \"\",\n"
                + "  `BOROUGH` varchar(65533) NULL COMMENT \"\",\n"
                + "  `ZIP_CODE` varchar(65533) NULL COMMENT \"\",\n"
                + "  `LATITUDE` int(11) NULL COMMENT \"\",\n"
                + "  `LONGITUDE` int(11) NULL COMMENT \"\",\n"
                + "  `LOCATION` varchar(65533) NULL COMMENT \"\",\n"
                + "  `ON_STREET_NAME` varchar(65533) NULL COMMENT \"\",\n"
                + "  `CROSS_STREET_NAME` varchar(65533) NULL COMMENT \"\",\n"
                + "  `OFF_STREET_NAME` varchar(65533) NULL COMMENT \"\",\n"
                + "  `CONTRIBUTING_FACTOR_VEHICLE_1` varchar(65533) NULL COMMENT \"\",\n"
                + "  `CONTRIBUTING_FACTOR_VEHICLE_2` varchar(65533) NULL COMMENT \"\",\n"
                + "  `COLLISION_ID` int(11) NULL COMMENT \"\",\n"
                + "  `VEHICLE_TYPE_CODE_1` varchar(65533) NULL COMMENT \"\",\n"
                + "  `VEHICLE_TYPE_CODE_2` varchar(65533) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP \n"
                + "DUPLICATE KEY(`CRASH_DATE`, `BOROUGH`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"enable_persistent_index\" = \"false\",\n"
                + "\"replicated_storage\" = \"true\",\n"
                + "\"compression\" = \"LZ4\"\n"
                + ");";

        String c2 = "CREATE TABLE `weatherdata` (\n"
                + "  `DATE` datetime NULL COMMENT \"\",\n"
                + "  `NAME` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyDewPointTemperature` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyDryBulbTemperature` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyPrecipitation` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyPresentWeatherType` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyPressureChange` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyPressureTendency` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyRelativeHumidity` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlySkyConditions` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyVisibility` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyWetBulbTemperature` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyWindDirection` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyWindGustSpeed` varchar(65533) NULL COMMENT \"\",\n"
                + "  `HourlyWindSpeed` varchar(65533) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP \n"
                + "DUPLICATE KEY(`DATE`, `NAME`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"enable_persistent_index\" = \"false\",\n"
                + "\"replicated_storage\" = \"true\",\n"
                + "\"compression\" = \"LZ4\"\n"
                + ");";

        starRocksAssert.useDatabase("test").withTable(c1);
        starRocksAssert.useDatabase("test").withTable(c2);

        String sql = "CREATE TABLE IF NOT EXISTS crashes_on_poor_visivility_1234\n"
                + "AS SELECT COUNT(DISTINCT c.COLLISION_ID) AS Crashes,\n"
                + "       truncate(avg(w.HourlyDryBulbTemperature), 1) AS Temp_F,\n"
                + "       truncate(avg(w.HourlyVisibility), 2) AS Visibility,\n"
                + "       max(w.HourlyPrecipitation) AS Precipitation,\n"
                + "       date_format((date_trunc(\"hour\", c.CRASH_DATE)), '%d %b %Y %H:%i') AS Hour\n"
                + "FROM crashdata c\n"
                + "LEFT JOIN weatherdata w\n"
                + "ON date_trunc(\"hour\", c.CRASH_DATE)=date_trunc(\"hour\", w.DATE)\n"
                + "WHERE w.HourlyVisibility BETWEEN 0.0 AND 1.0\n"
                + "GROUP BY Hour\n"
                + "ORDER BY Crashes DESC\n"
                + ";";
        CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) UtFrameUtils
                .parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = lineageProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTableAsSelect, lineage.getAction());

        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("default_catalog", columnLineages.get(0).getDestCatalog());
        Assert.assertEquals("test", columnLineages.get(0).getDestDatabase());
        Assert.assertEquals("crashes_on_poor_visivility_1234", columnLineages.get(0).getDestTable());

        Assert.assertEquals("default_catalog", columnLineages.get(0).getSrcCatalog());
        Assert.assertEquals("test", columnLineages.get(0).getSrcDatabase());
        Assert.assertEquals("crashdata", columnLineages.get(0).getSrcTable());
        Map<String, Set<String>> columnMap1 = columnLineages.get(0).getColumnMap();
        Iterator<Entry<String, Set<String>>> iterator1 = columnMap1.entrySet().iterator();
        Entry<String, Set<String>> entry = iterator1.next();
        Assert.assertEquals("Crashes", entry.getKey());
        Assert.assertEquals("COLLISION_ID", entry.getValue().iterator().next());
        entry = iterator1.next();
        Assert.assertEquals("Hour", entry.getKey());
        Assert.assertEquals("CRASH_DATE", entry.getValue().iterator().next());

        Assert.assertEquals("default_catalog", columnLineages.get(1).getDestCatalog());
        Assert.assertEquals("test", columnLineages.get(1).getDestDatabase());
        Assert.assertEquals("crashes_on_poor_visivility_1234", columnLineages.get(1).getDestTable());
        Assert.assertEquals("default_catalog", columnLineages.get(1).getSrcCatalog());
        Assert.assertEquals("test", columnLineages.get(1).getSrcDatabase());
        Assert.assertEquals("weatherdata", columnLineages.get(1).getSrcTable());

        Map<String, Set<String>> columnMap2 = columnLineages.get(1).getColumnMap();
        Iterator<Entry<String, Set<String>>> iterator2 = columnMap2.entrySet().iterator();
        Entry<String, Set<String>> entry2 = iterator2.next();
        Assert.assertEquals("Temp_F", entry2.getKey());
        Assert.assertEquals("HourlyDryBulbTemperature", entry2.getValue().iterator().next());
        entry2 = iterator2.next();
        Assert.assertEquals("Visibility", entry2.getKey());
        Assert.assertEquals("HourlyVisibility", entry2.getValue().iterator().next());
        entry2 = iterator2.next();
        Assert.assertEquals("Precipitation", entry2.getKey());
        Assert.assertEquals("HourlyPrecipitation", entry2.getValue().iterator().next());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testCreateAsSelectWithFunction() throws Exception {
        String sql = "create table trunc1 as select t1.id as id3, truncate(t1.name, t2.name2) as name3 from t1 "
                + "left join t2 on t1.id = t2.id2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTableAsSelect, lineage.getAction());
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(2, columnLineages.size());
        Assert.assertEquals("trunc1", columnLineages.get(0).getDestTable());
        Assert.assertEquals("t1", columnLineages.get(0).getSrcTable());
        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Assert.assertEquals("id", columnLineages.get(0).getColumnMap().get("id3").iterator().next());
        Assert.assertEquals("name", columnLineages.get(0).getColumnMap().get("name3").iterator().next());
        Assert.assertEquals("t2", columnLineages.get(1).getSrcTable());
        Assert.assertEquals(1, columnLineages.get(1).getColumnMap().size());
        Assert.assertEquals("id", columnLineages.get(0).getColumnMap().get("id3").iterator().next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testCreateAsSelectWithFunctionMultiProjection() throws Exception {
        String sql = "create table trunc1 as select id2 as id, truncate(name2, addr) as data from t22 ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTableAsSelect, lineage.getAction());
        List<ColumnLineage> columnLineages = lineage.getColumnLineages();
        Assert.assertEquals(1, columnLineages.size());
        Assert.assertEquals("trunc1", columnLineages.get(0).getDestTable());
        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().size());
        Assert.assertEquals(2, columnLineages.get(0).getColumnMap().get("data").size());
        Assert.assertEquals("id2", columnLineages.get(0).getColumnMap().get("id").iterator().next());
        Iterator<String> iterator = columnLineages.get(0).getColumnMap().get("data").iterator();
        Assert.assertEquals("name2", iterator.next());
        Assert.assertEquals("addr", iterator.next());
        printLineage(lineage, stmt);
    }

    @Test
    public void testAlterTableAddPartition() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("create table t7 (every_day date not null, id bigint not null, name string not null) "
                        + "duplicate key (every_day, id) partition by (every_day) distributed by hash(every_day,id) "
                        + "buckets 32 properties (\"replication_num\"=\"1\");");
        String sql = "ALTER TABLE t7 ADD PARTITION p2023 values less than ('2023-11-12') ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = lineageProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AddPartition, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("t7", target.getTable());
        Assert.assertEquals("test", target.getDatabase());
        Map<String, Object> extra = target.getExtra();
        List<Map<String, Object>> partitions = (List<Map<String, Object>>) extra.get("partitions");
        Assert.assertEquals(1, partitions.size());
        Map<String, Object> partition = partitions.get(0);
        Assert.assertEquals("p2023", partition.get("name"));
        Assert.assertEquals("RANGE", partition.get("type"));
        Assert.assertEquals(false, extra.get("isTempPartition"));
        printLineage(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateTable() throws Exception {
        String sql = "create table t3_4(id int comment 'c1', name string comment 'c2') comment 'test1' ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTable, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("t3_4", target.getTable());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test1", target.getComment());
        Assert.assertEquals(2, target.getColumns().size());
        Assert.assertEquals("id", target.getColumns().get(0).getName());
        Assert.assertEquals("INT", target.getColumns().get(0).getType());
        Assert.assertEquals("name", target.getColumns().get(1).getName());
        Assert.assertEquals("VARCHAR(65533)", target.getColumns().get(1).getType());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangelogCreateTable() throws Exception {
        String sql = "CREATE TABLE `user_access` (\n"
                + "  `uid` int(11) NULL COMMENT \"\",\n"
                + "  `name` varchar(64) NULL COMMENT \"\",\n"
                + "  `age` int(11) NULL COMMENT \"\",\n"
                + "  `phone` varchar(16) NULL COMMENT \"\",\n"
                + "  `last_access` datetime NULL COMMENT \"\",\n"
                + "  `credits` double NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP \n"
                + "DUPLICATE KEY(`uid`, `name`)\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"3\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"enable_persistent_index\" = \"false\",\n"
                + "\"replicated_storage\" = \"true\",\n"
                + "\"compression\" = \"LZ4\"\n"
                + ");";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTable, lineage.getAction());
        Target target = lineage.getTarget();
        Map<String, Object> extra = target.getExtra();
        Assert.assertEquals("olap", extra.get("engineName"));
        Assert.assertEquals("RANDOM", ((Map<String, String>) extra.get("distributionInfo")).get("type"));
        Assert.assertEquals("3", ((Map<String, String>) extra.get("properties")).get("replication_num"));
        Assert.assertEquals("false", ((Map<String, String>) extra.get("properties")).get("in_memory"));
        Assert.assertEquals("false", ((Map<String, String>) extra.get("properties")).get("enable_persistent_index"));
        Assert.assertEquals("true", ((Map<String, String>) extra.get("properties")).get("replicated_storage"));
        Assert.assertEquals("LZ4", ((Map<String, String>) extra.get("properties")).get("compression"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateTableAsSelect() throws Exception {
        String sql = "create table t3_5 as ((select a.id as id,b.id2 as id2, b.name2 as name from t1 a "
                + "join t2 b where a.id = b.id2 ))";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTableAsSelect, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("t3_5", target.getTable());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals(3, target.getColumns().size());
        Assert.assertEquals("id", target.getColumns().get(0).getName());
        Assert.assertEquals("BIGINT", target.getColumns().get(0).getType());
        Assert.assertEquals("id2", target.getColumns().get(1).getName());
        Assert.assertEquals("BIGINT", target.getColumns().get(1).getType());
        Assert.assertEquals("name", target.getColumns().get(2).getName());
        Assert.assertEquals("VARCHAR(65533)", target.getColumns().get(2).getType());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateTableLike() throws Exception {
        String sql = "create table t6 like t2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateTableLike, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("t6", target.getTable());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("default_catalog", target.getCatalog());

        Assert.assertEquals(2, target.getColumns().size());
        Assert.assertEquals("id2", target.getColumns().get(0).getName());
        Assert.assertEquals("BIGINT", target.getColumns().get(0).getType());
        Assert.assertEquals("name2", target.getColumns().get(1).getName());
        Assert.assertEquals("VARCHAR(65533)", target.getColumns().get(1).getType());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateView() throws Exception {
        String sql = " create view t4 as select * from t1";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateViewStmt stmt = (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("t4", target.getTable());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals(2, target.getColumns().size());
        Assert.assertEquals("id", target.getColumns().get(0).getName());
        Assert.assertEquals("BIGINT", target.getColumns().get(0).getType());
        Assert.assertEquals("name", target.getColumns().get(1).getName());
        Assert.assertEquals("VARCHAR(65533)", target.getColumns().get(1).getType());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateMaterializedViewAsync() throws Exception {
        String c1 = "CREATE TABLE `crashdata3` (\n"
                + "  `CRASH_DATE` datetime not null COMMENT \"\",\n"
                + "  `BOROUGH` varchar(65533) NULL COMMENT \"\",\n"
                + "  `ZIP_CODE` varchar(65533) not null COMMENT \"\",\n"
                + "  `LATITUDE` int(11) NULL COMMENT \"\",\n"
                + "  `LONGITUDE` int(11) NULL COMMENT \"\",\n"
                + "  `LOCATION` varchar(65533) NULL COMMENT \"\",\n"
                + "  `ON_STREET_NAME` varchar(65533) NULL COMMENT \"\",\n"
                + "  `COLLISION_ID` int(11) NULL COMMENT \"\",\n"
                + "  `VEHICLE_TYPE_CODE_1` varchar(65533) NULL COMMENT \"\",\n"
                + "  `VEHICLE_TYPE_CODE_2` varchar(65533) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP \n"
                + "PARTITION BY RANGE(CRASH_DATE)(\n"
                + "    PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n"
                + "    PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n"
                + "    PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n"
                + "    PARTITION p20200324 VALUES LESS THAN (\"2020-03-25\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`CRASH_DATE`)\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"enable_persistent_index\" = \"false\",\n"
                + "\"replicated_storage\" = \"true\",\n"
                + "\"compression\" = \"LZ4\"\n"
                + ");";

        starRocksAssert.useDatabase("test").withTable(c1);

        String sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS crashes_in_nyc_broadway\n"
                + "COMMENT \"Crashes in NYC during date of 20140101\"\n"
                + "REFRESH ASYNC\n"
                + "PARTITION BY `CRASH_DATE`\n"
                + "DISTRIBUTED BY HASH(`CRASH_DATE`)"
                + "PROPERTIES (\"replication_num\" = \"1\")\n"
                + "AS\n"
                + "SELECT\n"
                + "    CRASH_DATE,\n"
                + "    BOROUGH,\n"
                + "    ZIP_CODE,\n"
                + "    VEHICLE_TYPE_CODE_2\n"
                + "FROM crashdata3\n"
                + "WHERE ON_STREET_NAME = 'BROADWAY'\n"
                + ";";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateMaterializedViewStatement stmt = (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Target target = lineage.getTarget();
        Assert.assertEquals("crashes_in_nyc_broadway", target.getTable());
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("Crashes in NYC during date of 20140101", target.getComment());
        Assert.assertEquals("ASYNC", ((Map<String, String>) target.getExtra().get("refreshSchema")).get("refreshType"));
        Assert.assertEquals("IMMEDIATE", ((Map<String, String>) target.getExtra().get("refreshSchema")).get("refreshMoment"));
        Assert.assertNotNull(((Map<String, String>) target.getExtra().get("refreshSchema")).get("startTime"));
        Assert.assertEquals("1", ((Map<String, String>) target.getExtra().get("properties")).get("replication_num"));
        List<ColumnSpec> columnSpecs = target.getColumns();
        Assert.assertEquals(4, columnSpecs.size());
        Assert.assertEquals("CRASH_DATE", columnSpecs.get(0).getName());
        Assert.assertEquals("BOROUGH", columnSpecs.get(1).getName());
        Assert.assertEquals("ZIP_CODE", columnSpecs.get(2).getName());
        Assert.assertEquals("VEHICLE_TYPE_CODE_2", columnSpecs.get(3).getName());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateMaterializedViewSync() throws Exception {
        String sql = " create MATERIALIZED view t4 as select id,name from t1 ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        CreateMaterializedViewStmt stmt = (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateMaterializedView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("t4", target.getTable());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals(2, target.getColumns().size());
        Assert.assertEquals("id", target.getColumns().get(0).getName());
        Assert.assertEquals("BIGINT", target.getColumns().get(0).getType());
        Assert.assertEquals("name", target.getColumns().get(1).getName());
        Assert.assertEquals("VARCHAR(65533)", target.getColumns().get(1).getType());

        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterMaterializedViewRename() throws Exception {
        MaterializedView table = new MaterializedView();
        table.setName("mv9");
        table.setState(OlapTableState.NORMAL);
        table.setId(123);
        GlobalStateMgr.getCurrentState().getDb("test").registerTableUnlocked(table);
        String sql2 = "alter materialized view mv9 rename mv10";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterMaterializedViewStmt stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterMaterializedView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("mv10", target.getExtra().get("newMvName"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterMaterializedViewAsyncInterval() throws Exception {
        MaterializedView table = new MaterializedView();
        table.setName("mv4");
        table.setState(OlapTableState.NORMAL);
        table.setId(123);
        GlobalStateMgr.getCurrentState().getDb("test").registerTableUnlocked(table);
        String sql2 = "ALTER MATERIALIZED VIEW mv4 REFRESH ASYNC EVERY(INTERVAL 1 DAY);";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterMaterializedViewStmt stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterMaterializedView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("ASYNC", ((Map<String, String>) target.getExtra().get("refreshSchema")).get("refreshType"));
        Assert.assertEquals("IMMEDIATE", ((Map<String, String>) target.getExtra().get("refreshSchema")).get("refreshMoment"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterMaterializedViewAsyncManual() throws Exception {
        MaterializedView table = new MaterializedView();
        table.setName("mv5");
        table.setState(OlapTableState.NORMAL);
        table.setId(123);
        GlobalStateMgr.getCurrentState().getDb("test").registerTableUnlocked(table);
        String sql2 = "ALTER MATERIALIZED VIEW mv5 REFRESH  Manual;";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterMaterializedViewStmt stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterMaterializedView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("MANUAL", ((Map<String, String>) target.getExtra().get("refreshSchema")).get("refreshType"));
        Assert.assertEquals("IMMEDIATE", ((Map<String, String>) target.getExtra().get("refreshSchema")).get("refreshMoment"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterMaterializedViewStatus() throws Exception {
        MaterializedView table = new MaterializedView();
        table.setName("mv11");
        table.setState(OlapTableState.NORMAL);
        table.setId(123);
        GlobalStateMgr.getCurrentState().getDb("test").registerTableUnlocked(table);
        String sql2 = "ALTER MATERIALIZED VIEW mv11 ACTIVE;";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterMaterializedViewStmt stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterMaterializedView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("ACTIVE", target.getExtra().get("status"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterMaterializedViewSwap() throws Exception {
        MaterializedView table = new MaterializedView();
        table.setName("mv6");
        table.setState(OlapTableState.NORMAL);
        table.setId(123);
        GlobalStateMgr.getCurrentState().getDb("test").registerTableUnlocked(table);
        MaterializedView table2 = new MaterializedView();
        table2.setName("mv7");
        table2.setState(OlapTableState.NORMAL);
        table2.setId(124);
        GlobalStateMgr.getCurrentState().getDb("test").registerTableUnlocked(table2);

        String sql2 = "ALTER MATERIALIZED VIEW mv6 SWAP WITH mv7;";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterMaterializedViewStmt stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterMaterializedView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("mv7", ((Map<String, String>) target.getExtra().get("swapTable")).get("tableName"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterMaterializedViewSet() throws Exception {
        String sql = " create MATERIALIZED view mv8 refresh manual as select id,name from t1";
        starRocksAssert.useDatabase(DB_NAME).withMaterializedView(sql);
        OlapTable table = new OlapTable();
        table.setName("mv8");
        table.setState(OlapTableState.NORMAL);
        table.setId(123);
        GlobalStateMgr.getCurrentState().getDb("test").registerTableUnlocked(table);
        String sql2 = "ALTER MATERIALIZED VIEW mv8   SET (\"session.query_timeout\" = \"40000\");";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterMaterializedViewStmt stmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterMaterializedView, lineage.getAction());
        Target target = lineage.getTarget();
        Map<String, String> properties = (Map<String, String>) target.getExtra().get("modifyTableProperties");
        Assert.assertEquals("40000", properties.get("session.query_timeout"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableAddPartition() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("create table t3_9 (every_day date not null, id bigint not null, name string not null) "
                        + "duplicate key (every_day, id) partition by (every_day) distributed by hash(every_day,id) buckets 32 "
                        + "properties (\"replication_num\"=\"1\");");
        String sql = "ALTER TABLE t3_9 ADD PARTITION p2023 values less than ('2023-11-12') ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AddPartition, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t3_9", target.getTable());
        Map<String, Object> extra = target.getExtra();
        List<Map<String, Object>> partitions = (List<Map<String, Object>>) extra.get("partitions");
        Assert.assertEquals(1, partitions.size());
        Map<String, Object> partition = partitions.get(0);
        Assert.assertEquals("p2023", partition.get("name"));
        Assert.assertEquals("RANGE", partition.get("type"));
        Assert.assertEquals(false, extra.get("isTempPartition"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableModifyPartition() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("create table t9 (every_day date not null, id bigint not null, name string not null) "
                        + "duplicate key (every_day, id) partition by (every_day) distributed by hash(every_day,id) buckets 32 "
                        + "properties (\"replication_num\"=\"1\");");
        String sql = "ALTER TABLE t9 MODIFY PARTITION p2023 SET(\"replication_num\"=\"1\") ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.ModifyPartition, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t9", target.getTable());
        Map<String, Object> extra = target.getExtra();
        List<String> partitionNames = (List<String>) extra.get("partitionNames");
        Assert.assertEquals(1, partitionNames.size());
        Assert.assertEquals("p2023", partitionNames.get(0));

        Map<String, String> properties = (Map<String, String>) extra.get("properties");
        Assert.assertEquals(1, properties.size());
        Assert.assertEquals("1", properties.get("replication_num"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableDropPartition() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("create table t3_1 (every_day date not null, id bigint not null, name string not null) "
                        + "duplicate key (every_day, id) partition by (every_day) distributed by hash(every_day,id) buckets 32 "
                        + "properties (\"replication_num\"=\"1\");");
        String sql = "ALTER TABLE t3_1 drop PARTITION p2023  ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropPartition, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t3_1", target.getTable());
        Assert.assertEquals("p2023", ((List<String>) target.getExtra().get("partitionNames")).get(0));
        Assert.assertEquals(false, target.getExtra().get("isTempPartition"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableRenamePartition() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE IF NOT EXISTS pt_crashes_on_poor_visibility(\n"
                        + "    crash_date  DATE  COMMENT '碰撞时间',\n"
                        + "    temp_f  STRING  COMMENT '温度（华氏度）',\n"
                        + "    visibility  STRING  COMMENT '能见度',\n"
                        + "    precipitation  STRING  COMMENT '降水量',\n"
                        + "    crash_count  INT  COMMENT '碰撞次数'\n"
                        + ")\n"
                        + "COMMENT \"the number of crashes when visibility is poo\"\n"
                        + "PARTITION BY RANGE(crash_date) (\n"
                        + "    START (\"2014-01-01\") END (\"2016-01-01\") EVERY (INTERVAL 1 MONTH)\n"
                        + ")\n"
                        + "PROPERTIES (\n"
                        + "    \"replication_num\" = \"1\"\n"
                        + ");");
        String sql = "ALTER TABLE pt_crashes_on_poor_visibility RENAME PARTITION p20140101 p7_new;";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.RenamePartition, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("pt_crashes_on_poor_visibility", target.getTable());
        Map<String, Object> extra = target.getExtra();
        Assert.assertEquals("p20140101", extra.get("partitionName"));
        Assert.assertEquals("p7_new", extra.get("newPartitionName"));

        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableAddTempPartition() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("create table t4 (every_day date not null, id bigint not null, name string not null) "
                        + "duplicate key (every_day, id) partition by (every_day) distributed by hash(every_day,id) buckets 32 "
                        + "properties (\"replication_num\"=\"1\");");
        String sql = "ALTER TABLE t4 ADD TEMPORARY PARTITION p2023 values less than ('2023-11-12') ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AddPartition, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t4", target.getTable());
        Map<String, Object> extra = target.getExtra();
        List<Map<String, Object>> partitions = (List<Map<String, Object>>) extra.get("partitions");
        Assert.assertEquals(1, partitions.size());
        Map<String, Object> partition = partitions.get(0);
        Assert.assertEquals("p2023", partition.get("name"));
        Assert.assertEquals("RANGE", partition.get("type"));
        Assert.assertEquals(true, extra.get("isTempPartition"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableDropTempPartition() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("create table t3_3 (every_day date not null, id bigint not null, name string not null) "
                        + "duplicate key (every_day, id) partition by (every_day) distributed by hash(every_day,id) buckets 32 "
                        + "properties (\"replication_num\"=\"1\");");
        String sql = "ALTER TABLE t3_3 drop TEMPORARY PARTITION p2023  ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropPartition, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t3_3", target.getTable());
        Assert.assertEquals("p2023", ((List<String>) target.getExtra().get("partitionNames")).get(0));
        Assert.assertEquals(true, target.getExtra().get("isTempPartition"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableRename() throws Exception {
        String sql = "ALTER TABLE t2 rename t22";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.RenameTableName, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        Assert.assertEquals("t22", target.getExtra().get("newTableName"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableCreateRollupIndex() throws Exception {
        String sql = "ALTER TABLE t2 ADD ROLLUP r1(id2)";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AddRollup, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List<Map<String, Object>> rollups = (List<Map<String, Object>>) target.getExtra().get("rollups");
        Assert.assertEquals(1, rollups.size());
        Assert.assertEquals("r1", rollups.get(0).get("rollupName"));
        Assert.assertEquals("id2", ((List) rollups.get(0).get("columns")).get(0));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableCreateRollupIndexBatch() throws Exception {
        String sql = "ALTER TABLE t2 ADD ROLLUP r1(id2), r2(name2)";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AddRollup, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List<Map<String, Object>> rollups = (List<Map<String, Object>>) target.getExtra().get("rollups");
        Assert.assertEquals(2, rollups.size());
        Assert.assertEquals("r1", rollups.get(0).get("rollupName"));
        Assert.assertEquals("id2", ((List) rollups.get(0).get("columns")).get(0));
        Assert.assertEquals("r2", rollups.get(1).get("rollupName"));
        Assert.assertEquals("name2", ((List) rollups.get(1).get("columns")).get(0));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableDropRollup() throws Exception {
        String sql = "ALTER TABLE t2 Drop ROLLUP r1";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropRollup, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List names = (List) target.getExtra().get("rollupNames");
        Assert.assertEquals("r1", names.get(0));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableDropRollupBatch() throws Exception {
        String sql = "ALTER TABLE t2 Drop ROLLUP r1,r2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropRollup, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List names = (List) target.getExtra().get("rollupNames");
        Assert.assertEquals("r1", names.get(0));
        Assert.assertEquals("r2", names.get(1));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableCreateAddBitmapIndex() throws Exception {
        String sql = "ALTER TABLE t2 add index r1 (id2) using bitmap comment 'bab'";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateIndex, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List indexes = (List) target.getExtra().get("indexes");
        Assert.assertEquals("r1", ((Map<String, Object>) indexes.get(0)).get("indexName"));
        Assert.assertEquals("BITMAP", ((Map<String, Object>) indexes.get(0)).get("indexType"));
        Assert.assertEquals("id2", ((List) ((Map<String, Object>) indexes.get(0)).get("columns")).get(0));
        Assert.assertEquals("bab", ((Map<String, Object>) indexes.get(0)).get("comment"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateIndex() throws Exception {
        String sql = "CREATE index r1 on t2 (id2) using bitmap comment 'abc'";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateIndex, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List indexes = (List) target.getExtra().get("indexes");
        Assert.assertEquals("r1", ((Map<String, Object>) indexes.get(0)).get("indexName"));
        Assert.assertEquals("BITMAP", ((Map<String, Object>) indexes.get(0)).get("indexType"));
        Assert.assertEquals("id2", ((List) ((Map<String, Object>) indexes.get(0)).get("columns")).get(0));
        Assert.assertEquals("abc", ((Map<String, Object>) indexes.get(0)).get("comment"));
    }

    @Test
    public void testChangeLogAlterTableCreateDropIndex() throws Exception {
        String sql = "ALTER TABLE t2 drop index r1 ";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropIndex, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List indexes = (List) target.getExtra().get("indexNames");
        Assert.assertEquals("r1", indexes.get(0));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogDropIndex() throws Exception {
        String sql = "Drop index r1 on t2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropIndex, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List indexes = (List) target.getExtra().get("indexNames");
        Assert.assertEquals("r1", indexes.get(0));
    }

    @Test
    public void testChangeLogAlterTableAddColumn() throws Exception {
        String sql = "ALTER TABLE t2 add column newC int default '0' after id2 , add column newC2 int default '1'";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AddColumn, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List columns = (List) target.getExtra().get("columns");
        Assert.assertEquals(2, columns.size());
        Assert.assertEquals("newC", ((Map<String, Object>) columns.get(0)).get("name"));
        Assert.assertEquals("INT", ((Map<String, Object>) columns.get(0)).get("type"));

        Assert.assertEquals("newC2", ((Map<String, Object>) columns.get(1)).get("name"));
        Assert.assertEquals("INT", ((Map<String, Object>) columns.get(1)).get("type"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableAddColumns() throws Exception {
        String sql = "ALTER TABLE t2 \n"
                + "add column (pk_TINYINT TINYINT COMMENT \"range [-128, 127]\",\n"
                + "c_smallINT SMALLINT COMMENT \"range [-32768, 32767]\",\n"
                + "c_int INT COMMENT \"range [-2147483648, 2147483647]\",\n"
                + "c_bigINT BIGINT(20)  COMMENT \"[-9223372036854775808, 9223372036854775807]\",\n"
                + "c_largeInt LARGEINT COMMENT \"range [-2^127 + 1 ~ 2^127 - 1]\",\n"
                + "account_decimal DECIMAL(20,10) COMMENT \"\",\n"
                + "income_double DOUBLE COMMENT \"8 bytes\",\n"
                + "channelFloat FLOAT COMMENT \"4 bytes\",\n"
                + "ispass_boolean BOOLEAN COMMENT \"true/false\",\n"
                + "us_detail_string STRING COMMENT \"upper limit value 65533 bytes\",\n"
                + "s_VARBINARY  VARBINARY COMMENT \"\",\n"
                + "pd_type_Char CHAR(20) COMMENT \"range char(m),m in (1-255) \",\n"
                + "pd_typeVARCHAR VARCHAR(20) COMMENT \"range char(m),m in (1-65533) \",\n"
                + "make_date DATE COMMENT \"YYYY-MM-DD\",\n"
                + "relTime_DATETIME DATETIME COMMENT \"YYYY-MM-DD HH:MM:SS\",\n"
                + "c_array ARRAY<INT>,\n"
                + "c_json  JSON COMMENT \"\",\n"
                + "map1 MAP<INT,INT>,\n"
                + "struct1 STRUCT<a INT, b INT>)";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AddColumn, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        List columns = (List) target.getExtra().get("columns");
        Assert.assertEquals(19, columns.size());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableDropColumn() throws Exception {
        String sql = "ALTER TABLE t2 drop column id2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropColumn, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        Assert.assertEquals("id2", ((List) target.getExtra().get("columnNames")).get(0));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableSet() throws Exception {
        String sql = "ALTER TABLE t2 set (\"bloom_filter_columns\"=\"id2\")";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.ModifyTableProperties, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        Map<String, String> properties = (Map<String, String>) target.getExtra().get("properties");
        Assert.assertEquals("id2", properties.get("bloom_filter_columns"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterTableComment() throws Exception {
        String sql = "ALTER TABLE t2 comment='test1234'";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterTableComment, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        Assert.assertEquals("test1234", target.getExtra().get("newComment"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogDropTable() throws Exception {
        String sql = "Drop table t2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        DropTableStmt stmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropTable, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("t2", target.getTable());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateResource() throws Exception {
        String sql = "CREATE EXTERNAL RESOURCE \"hive0\"\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    \"type\" = \"hive\",\n"
                + "    \"hive.metastore.uris\" = \"thrift://x.x.x.x:9083\"\n"
                + ");";
        CreateResourceStmt stmt = (CreateResourceStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateResource, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("HIVE", target.getExtra().get("resourceType"));
        Assert.assertEquals("hive0", target.getExtra().get("resourceName"));
        Assert.assertEquals("thrift://x.x.x.x:9083", ((Map<String, String>) target.getExtra().get("properties")).get("hive.metastore.uris"));
        Assert.assertEquals("hive", ((Map<String, String>) target.getExtra().get("properties")).get("type"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterResource() throws Exception {
        String sql = "ALTER RESOURCE 'hive0' SET PROPERTIES (\"hive.metastore.uris\" = \"thrift://xx.xx.xx.xx:9083\")\n"
                + "\n";
        AlterResourceStmt stmt = (AlterResourceStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterResource, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("hive0", target.getExtra().get("resourceName"));
        Assert.assertEquals("thrift://xx.xx.xx.xx:9083", ((Map<String, String>) target.getExtra().get("properties")).get("hive.metastore.uris"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogDropResource() throws Exception {
        String sql = "DROP RESOURCE 'hive0';\n";
        DropResourceStmt stmt = (DropResourceStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropResource, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("hive0", target.getExtra().get("resourceName"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateCatalog() throws Exception {
        String sql = "CREATE EXTERNAL CATALOG hive_glue_catalog\n"
                + "PROPERTIES(\n"
                + "    \"type\"=\"hive\", \n"
                + "    \"hive.metastore.type\"=\"glue\",\n"
                + "    \"aws.hive.metastore.glue.aws-access-key\"=\"xxxxxx\",\n"
                + "    \"aws.hive.metastore.glue.aws-secret-key\"=\"xxxxxxxxxxxx\",\n"
                + "    \"aws.hive.metastore.glue.endpoint\"=\"https://glue.x-x-x.amazonaws.com\"\n"
                + ");";
        CreateCatalogStmt stmt = (CreateCatalogStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateCatalog, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("hive_glue_catalog", target.getCatalog());
        Assert.assertEquals("hive", target.getExtra().get("catalogType"));
        Map<String, String> properties = (Map<String, String>) target.getExtra().get("properties");
        Assert.assertEquals("hive", properties.get("type"));
        Assert.assertEquals("glue", properties.get("hive.metastore.type"));
        Assert.assertEquals("https://glue.x-x-x.amazonaws.com", properties.get("aws.hive.metastore.glue.endpoint"));
        Assert.assertEquals("xxxxxx", properties.get("aws.hive.metastore.glue.aws-access-key"));
        Assert.assertEquals("xxxxxxxxxxxx", properties.get("aws.hive.metastore.glue.aws-secret-key"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateCatalogIceberg() throws Exception {
        String sql = "CREATE EXTERNAL CATALOG iceberg_metastore_catalog\n"
                + "PROPERTIES(\n"
                + "    \"type\"=\"iceberg\",\n"
                + "    \"iceberg.catalog.type\"=\"hive\",\n"
                + "    \"iceberg.catalog.hive.metastore.uris\"=\"thrift://xx.xx.xx.xx:9083\"\n"
                + ");";
        CreateCatalogStmt stmt = (CreateCatalogStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateCatalog, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("iceberg_metastore_catalog", target.getCatalog());
        Assert.assertEquals("iceberg", target.getExtra().get("catalogType"));
        Map<String, String> properties = (Map<String, String>) target.getExtra().get("properties");
        Assert.assertEquals("iceberg", properties.get("type"));
        Assert.assertEquals("hive", properties.get("iceberg.catalog.type"));
        Assert.assertEquals("thrift://xx.xx.xx.xx:9083", properties.get("iceberg.catalog.hive.metastore.uris"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogDropCatalog() throws Exception {
        String sql = "Drop catalog hive_test_catalog";
        DropCatalogStmt stmt = (DropCatalogStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropCatalog, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("hive_test_catalog", target.getCatalog());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogCreateDatabase() throws Exception {
        String sql = "CREATE DATABASE db_test;";
        CreateDbStmt stmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.CreateDb, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("db_test", target.getDatabase());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterDatabase() throws Exception {
        String sql = "ALTER DATABASE test SET DATA QUOTA 10M;";
        AlterDatabaseQuotaStmt stmt = (AlterDatabaseQuotaStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterDatabaseQuota, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("DATA", target.getExtra().get("quotaType"));
        Assert.assertEquals("10M", target.getExtra().get("quotaValue"));
        Assert.assertEquals(10485760L, target.getExtra().get("quota"));
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterDatabaseRename() throws Exception {
        String sql = "ALTER DATABASE test rename test2;";
        AlterDatabaseRenameStatement stmt = (AlterDatabaseRenameStatement)
                UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.RenameDatabase, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("test2", target.getExtra().get("newDbName"));

        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogDropDatabase() throws Exception {
        String sql = "drop database test2";
        DropDbStmt stmt = (DropDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropDb, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test2", target.getDatabase());

        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogDropView() throws Exception {
        starRocksAssert.useDatabase("test").withView("create view v2 as select * from t1");
        String sql = "drop view v2";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        DropTableStmt stmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("v2", target.getTable());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogDropMaterializedView() throws Exception {
        String sql = "DROP MATERIALIZED View v1";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        DropMaterializedViewStmt stmt = (DropMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.DropMaterializedView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("v1", target.getTable());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogAlterView() throws Exception {
        starRocksAssert.useDatabase("test").withView("create view v1 as select * from t1");
        String sql = "alter view v1 (id comment 'id comment', name comment 'comment2',"
                + " data comment 'comment3' ) as select id, name, truncate('test_', name) from t1";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        AlterViewStmt stmt = (AlterViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Assert.assertEquals(Action.AlterView, lineage.getAction());
        Target target = lineage.getTarget();
        Assert.assertEquals("default_catalog", target.getCatalog());
        Assert.assertEquals("test", target.getDatabase());
        Assert.assertEquals("v1", target.getTable());
        List<ColumnSpec> columnSpecs = target.getColumns();
        Assert.assertEquals(3, columnSpecs.size());
        Assert.assertEquals("id", columnSpecs.get(0).getName());
        Assert.assertEquals("BIGINT", columnSpecs.get(0).getType());
        Assert.assertEquals("id comment", columnSpecs.get(0).getComment());
        Assert.assertEquals("name", columnSpecs.get(1).getName());
        Assert.assertEquals("VARCHAR(65533)", columnSpecs.get(1).getType());
        Assert.assertEquals("comment2", columnSpecs.get(1).getComment());
        Assert.assertEquals("data", columnSpecs.get(2).getName());
        Assert.assertEquals("comment3", columnSpecs.get(2).getComment());
        printChangeLog(lineage, stmt);
    }

    @Test
    public void testChangeLogDropFunction() throws Exception {
        Deencapsulation.setField(Config.class, "enable_udf", true);
        Type[] argTypes = {Type.INT, Type.INT};
        starRocksAssert.useDatabase("test");
        Function desc = new ScalarFunction(new FunctionName("test", "sumint"), argTypes, Type.INT, false);
        GlobalStateMgr.getCurrentState().getDb("test").addFunction(desc);
        String sql = "drop function sumint(int,int)";
        ConnectContext ctx = starRocksAssert.useDatabase("test").getCtx();
        DropFunctionStmt stmt = (DropFunctionStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Lineage lineage = changeLogProcessor.doProcess(stmt);
        Target target = lineage.getTarget();
        Assert.assertEquals("sumint", target.getExtra().get("functionName"));
        Assert.assertEquals("INT", ((List<String>) target.getExtra().get("functionArgsType")).get(0));
        Assert.assertEquals("INT", ((List<String>) target.getExtra().get("functionArgsType")).get(1));
        printChangeLog(lineage, stmt);
    }
}