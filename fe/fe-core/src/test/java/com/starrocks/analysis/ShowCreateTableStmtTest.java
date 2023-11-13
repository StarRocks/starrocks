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

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowCreateTableStmtTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
    }

    @Test
    public void testShowCreateViewUseMV() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.base\n" +
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
                .withMaterializedView("create materialized view test_mv distributed by hash(k1)" +
                        " as select k1 from base");
        String sql = "show create view test_mv";
        ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, showCreateTableStmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals("test_mv", resultSet.getResultRows().get(0).get(0));
        Assert.assertEquals("CREATE VIEW `test_mv` AS SELECT `test`.`base`.`k1`\n" +
                "FROM `test`.`base`", resultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testNormal() throws Exception {
        ctx.setDatabase("testDb");
        ShowCreateTableStmt stmt =
                new ShowCreateTableStmt(new TableName("testDb", "testTbl"), ShowCreateTableStmt.CreateTableType.TABLE);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTable());
        Assert.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Table", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Create Table", stmt.getMetaData().getColumn(1).getName());
    }

    @Test(expected = SemanticException.class)
    public void testNoTbl() throws Exception {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(null, ShowCreateTableStmt.CreateTableType.TABLE);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No Exception throws.");
    }

    @Test
    public void testPKShouldShowDefault() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE `test_pk_current_timestamp` (\n" +
                        "  `id` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "PRIMARY KEY(`id`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 5 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
        String sql = "show create table test_pk_current_timestamp";
        ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, showCreateTableStmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals("test_pk_current_timestamp", resultSet.getResultRows().get(0).get(0));
        Assert.assertTrue(resultSet.getResultRows().get(0).get(1).contains("datetime NOT NULL DEFAULT CURRENT_TIMESTAMP"));
    }

    @Test
    public void testHiveTableMapProperties() {
        List<Column> fullSchema = new ArrayList<>();
        fullSchema.add(new Column("id", Type.INT));
        Map<String, String> props = new HashMap<>();
        props.put("COLUMN_STATS_ACCURATE", "{\"BASIC_STATS\":\"true\"}");

        HiveTable table = new HiveTable(100, "test", fullSchema, "aa", "bb", "cc", "dd", "hdfs://xxx",
                0, new ArrayList<>(), fullSchema.stream().map(x -> x.getName()).collect(Collectors.toList()),
                props, HiveStorageFormat.ORC);
        List<String> result = new ArrayList<>();
        GlobalStateMgr.getDdlStmt(table, result, null, null, false, true);
        Assert.assertEquals(result.size(), 1);
        String value = result.get(0);
        System.out.println(value);
        Assert.assertTrue(value.contains("\"COLUMN_STATS_ACCURATE\"  =  \"{\\\"BASIC_STATS\\\":\\\"true\\\"}\""));
    }
}