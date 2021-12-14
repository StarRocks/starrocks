// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/planner/QueryPlanTest.java

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

package com.starrocks.planner;

import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.ShowCreateDbStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.meta.SqlBlackList;
import com.starrocks.meta.BlackListSql;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState.MysqlStateType;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Map;

public class QueryPlannerTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/QueryPlannerTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static String DB_NAME = "test";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        starRocksAssert.withTable("CREATE TABLE test.`baseall` (\n" +
                "  `k1` tinyint(4) NULL COMMENT \"\",\n" +
                "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                "  `k3` int(11) NULL COMMENT \"\",\n" +
                "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                "  `k6` char(5) NULL COMMENT \"\",\n" +
                "  `k10` date NULL COMMENT \"\",\n" +
                "  `k11` datetime NULL COMMENT \"\",\n" +
                "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                "  `k8` double MAX NULL COMMENT \"\",\n" +
                "  `k9` float SUM NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testMultiStmts() throws Exception {
        String sql = "SHOW VARIABLES LIKE 'lower_case_%'; SHOW VARIABLES LIKE 'sql_mode'";
        List<StatementBase> stmts = UtFrameUtils.parseAndAnalyzeStmts(sql, connectContext);
        Assert.assertEquals(2, stmts.size());

        sql = "SHOW VARIABLES LIKE 'lower_case_%';;;";
        stmts = UtFrameUtils.parseAndAnalyzeStmts(sql, connectContext);
        Assert.assertEquals(1, stmts.size());

        sql = "SHOW VARIABLES LIKE 'lower_case_%';;;SHOW VARIABLES LIKE 'lower_case_%';";
        stmts = UtFrameUtils.parseAndAnalyzeStmts(sql, connectContext);
        Assert.assertEquals(4, stmts.size());

        sql = "SHOW VARIABLES LIKE 'lower_case_%'";
        stmts = UtFrameUtils.parseAndAnalyzeStmts(sql, connectContext);
        Assert.assertEquals(1, stmts.size());
    }

    @Test
    public void testCreateDbQueryPlanWithSchemaSyntax() throws Exception {
        String createSchemaSql = "create schema if not exists test";
        String createDbSql = "create database if not exists test";
        CreateDbStmt createSchemaStmt =
                (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createSchemaSql, connectContext);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbSql, connectContext);
        Assert.assertEquals(createDbStmt.toSql(), createSchemaStmt.toSql());
    }

    @Test
    public void testDropDbQueryPlanWithSchemaSyntax() throws Exception {
        String dropSchemaSql = "drop schema if exists test";
        String dropDbSql = "drop database if exists test";
        DropDbStmt dropSchemaStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(dropSchemaSql, connectContext);
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(dropDbSql, connectContext);
        Assert.assertEquals(dropDbStmt.toSql(), dropSchemaStmt.toSql());
    }

    @Test
    public void testShowCreateDbQueryPlanWithSchemaSyntax() throws Exception {
        String showCreateSchemaSql = "show create schema test";
        String showCreateDbSql = "show create database test";
        ShowCreateDbStmt showCreateSchemaStmt =
                (ShowCreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(showCreateSchemaSql, connectContext);
        ShowCreateDbStmt showCreateDbStmt =
                (ShowCreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(showCreateDbSql, connectContext);
        Assert.assertEquals(showCreateDbStmt.toSql(), showCreateSchemaStmt.toSql());
    }

    @Test
    public void testSqlBlackList() throws Exception {
        String setEnableSqlBlacklist = "admin set frontend config (\"enable_sql_blacklist\" = \"true\")";
        StmtExecutor stmtExecutor0 = new StmtExecutor(connectContext, setEnableSqlBlacklist);
        stmtExecutor0.execute();

        String addBlackListSql = "add sqlblacklist \"select k1 from .+\"";
        StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, addBlackListSql);
        stmtExecutor1.execute();

        Assert.assertEquals(SqlBlackList.getInstance().sqlBlackListMap.entrySet().size(), 1);
        long id = -1;
        for (Map.Entry<String, BlackListSql> entry : SqlBlackList.getInstance().sqlBlackListMap.entrySet()) {
        id = entry.getValue().id;
            Assert.assertEquals("select k1 from .+", entry.getKey());
        }

        String sql = "select k1 from test.baseall";
        StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, sql);
        stmtExecutor2.execute();
        Assert.assertEquals("Access denied; This sql is in blacklist, please contact your admin", connectContext.getState().getErrorMessage());

        String deleteBlackListSql = "delete sqlblacklist " + String.valueOf(id);
        StmtExecutor stmtExecutor3 = new StmtExecutor(connectContext, deleteBlackListSql);
        stmtExecutor3.execute();
        Assert.assertEquals(0, SqlBlackList.getInstance().sqlBlackListMap.entrySet().size());
    }

    @Test
    public void testSqlBlackListUseWhere() throws Exception {
        String setEnableSqlBlacklist = "admin set frontend config (\"enable_sql_blacklist\" = \"true\")";
        StmtExecutor stmtExecutor0 = new StmtExecutor(connectContext, setEnableSqlBlacklist);
        stmtExecutor0.execute();

        String addBlackListSql = "add sqlblacklist \"((?!where).)*\"";
        StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, addBlackListSql);
        stmtExecutor1.execute();

        Assert.assertEquals(SqlBlackList.getInstance().sqlBlackListMap.entrySet().size(), 1);
        long id = -1;
        for (Map.Entry<String, BlackListSql> entry : SqlBlackList.getInstance().sqlBlackListMap.entrySet()) {
            id = entry.getValue().id;
            Assert.assertEquals("((?!where).)*", entry.getKey());
        }

        String sql = "select k1 from test.baseall where k1 > 0";
        StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, sql);
        stmtExecutor2.execute();
        Assert.assertEquals("Access denied; This sql is in blacklist, please contact your admin", connectContext.getState().getErrorMessage());

        String deleteBlackListSql = "delete sqlblacklist " + String.valueOf(id);
        StmtExecutor stmtExecutor3 = new StmtExecutor(connectContext, deleteBlackListSql);
        stmtExecutor3.execute();
        Assert.assertEquals(0, SqlBlackList.getInstance().sqlBlackListMap.entrySet().size());
    }

    @Test
    public void testCreateView() throws Exception {
        String sql = "create view view_test (dt) as select k10 from baseall where " +
                "k10 = ( SELECT max(b.k3) FROM baseall as b );";
        StmtExecutor stmtExecutor0 = new StmtExecutor(connectContext, sql);
        stmtExecutor0.execute();
        Assert.assertNotSame(connectContext.getState().getStateType(), MysqlStateType.ERR);
    }
}
