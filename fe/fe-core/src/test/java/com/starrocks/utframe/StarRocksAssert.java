// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/utframe/StarRocksAssert.java

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

package com.starrocks.utframe;

import com.google.common.base.Preconditions;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.BackendCoreStat;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class StarRocksAssert {

    private ConnectContext ctx;

    public StarRocksAssert() throws IOException {
        this(UtFrameUtils.createDefaultCtx());
    }

    public StarRocksAssert(ConnectContext ctx) {
        Config.tablet_create_timeout_second = 60;
        this.ctx = ctx;
        this.ctx.setQueryId(UUIDUtil.genUUID());
    }

    public ConnectContext getCtx() {
        return this.ctx;
    }

    public StarRocksAssert withEnableMV() {
        Config.enable_materialized_view = true;
        return this;
    }

    public StarRocksAssert withDatabase(String dbName) throws Exception {
        DropDbStmt dropDbStmt =
                (DropDbStmt) UtFrameUtils.parseStmtWithNewParser("drop database if exists " + dbName + ";", ctx);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        } catch (MetaNotFoundException e) {
            if (!dropDbStmt.isSetIfExists()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
        }

        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database " + dbName + ";", ctx);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        return this;
    }

    public StarRocksAssert withRole(String roleName) throws Exception {
        CreateRoleStmt createRoleStmt =
                (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser("create role " + roleName + ";", ctx);
        GlobalStateMgr.getCurrentState().getAuth().createRole(createRoleStmt);
        return this;
    }

    public StarRocksAssert withUser(String user, String roleName) throws Exception {
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                        "create user " + user + " default role '" + roleName + "';", ctx);
        GlobalStateMgr.getCurrentState().getAuth().createUser(createUserStmt);
        return this;
    }

    public StarRocksAssert withDatabaseWithoutAnalyze(String dbName) throws Exception {
        CreateDbStmt dbStmt = new CreateDbStmt(false, dbName);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(dbStmt.getFullDbName());
        return this;
    }

    public boolean databaseExist(String dbName) {
        Database db = GlobalStateMgr.getCurrentState().getDb("" + dbName);
        return db != null;
    }

    public StarRocksAssert useDatabase(String dbName) {
        ctx.setDatabase(dbName);
        return this;
    }

    public StarRocksAssert withoutUseDatabase() {
        ctx.setDatabase("");
        return this;
    }

    public StarRocksAssert withResource(String sql) throws Exception {
        CreateResourceStmt createResourceStmt = (CreateResourceStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        if (!GlobalStateMgr.getCurrentState().getResourceMgr().containsResource(createResourceStmt.getResourceName())) {
            GlobalStateMgr.getCurrentState().getResourceMgr().createResource(createResourceStmt);
        }
        return this;
    }

    public StarRocksAssert withTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        return this;
    }

    public StarRocksAssert withView(String sql) throws Exception {
        CreateViewStmt createTableStmt = (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().createView(createTableStmt);
        return this;
    }

    public StarRocksAssert dropView(String viewName) throws Exception {
        DropTableStmt dropViewStmt =
                (DropTableStmt) UtFrameUtils.parseStmtWithNewParser("drop view " + viewName + ";", ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropViewStmt);
        return this;
    }

    public StarRocksAssert dropDatabase(String dbName) throws Exception {
        DropDbStmt dropDbStmt =
                (DropDbStmt) UtFrameUtils.parseStmtWithNewParser("drop database " + dbName + ";", ctx);
        GlobalStateMgr.getCurrentState().getMetadata().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        return this;
    }

    public StarRocksAssert dropTable(String tableName) throws Exception {
        DropTableStmt dropTableStmt =
                (DropTableStmt) UtFrameUtils.parseStmtWithNewParser("drop table " + tableName + ";", ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
        return this;
    }

    public StarRocksAssert dropMaterializedView(String materializedViewName) throws Exception {
        DropMaterializedViewStmt dropMaterializedViewStmt = (DropMaterializedViewStmt) UtFrameUtils.
                parseStmtWithNewParser("drop materialized view " + materializedViewName + ";", ctx);
        GlobalStateMgr.getCurrentState().dropMaterializedView(dropMaterializedViewStmt);
        return this;
    }

    // Add materialized view to the schema (use cup)
    public StarRocksAssert withMaterializedView(String sql) throws Exception {
        CreateMaterializedViewStmt createMaterializedViewStmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().createMaterializedView(createMaterializedViewStmt);
        checkAlterJob();
        return this;
    }

    // Add materialized view to the schema (use antlr)
    public StarRocksAssert withMaterializedStatementView(String sql) throws Exception {
        CreateMaterializedViewStatement createMaterializedViewStmt =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().createMaterializedView(createMaterializedViewStmt);
        checkAlterJob();
        return this;
    }

    // why create this ? because query statement before property in old mv grammar
    public StarRocksAssert withNewMaterializedView(String sql) throws Exception {
        CreateMaterializedViewStatement createMaterializedViewStatement =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().createMaterializedView(createMaterializedViewStatement);
        return this;
    }

    // Add rollup
    public StarRocksAssert withRollup(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
        checkAlterJob();
        return this;
    }

    // With catalog
    public StarRocksAssert withCatalog(String sql) throws Exception {
        CreateCatalogStmt createCatalogStmt = (CreateCatalogStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog(createCatalogStmt);
        return this;
    }

    public void executeResourceGroupDdlSql(String sql) throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 32);
        StatementBase statement = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(statement, ctx);

        Assert.assertTrue(statement.getClass().getSimpleName().contains("ResourceGroupStmt"));
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        DDLStmtExecutor.execute((DdlStmt) statement, connectCtx);
    }

    public List<List<String>> executeResourceGroupShowSql(String sql) throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 32);

        StatementBase statement = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statement, ctx);

        Assert.assertTrue(statement instanceof ShowResourceGroupStmt);
        return GlobalStateMgr.getCurrentState().getResourceGroupMgr().showResourceGroup((ShowResourceGroupStmt) statement);
    }

    private void checkAlterJob() throws InterruptedException {
        // check alter job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            if (alterJobV2.getJobState().isFinalState()) {
                continue;
            }
            Database database = GlobalStateMgr.getCurrentState().getDb(alterJobV2.getDbId());
            Table table = database.getTable(alterJobV2.getTableId());
            Preconditions.checkState(table instanceof OlapTable);
            OlapTable olapTable = (OlapTable) table;
            int retry = 0;
            while (olapTable.getState() != OlapTable.OlapTableState.NORMAL && retry++ < 6000) {
                Thread.sleep(10);
            }
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
    }

    public QueryAssert query(String sql) {
        return new QueryAssert(ctx, sql);
    }

    public class QueryAssert {
        private final ConnectContext connectContext;
        private final String sql;

        public QueryAssert(ConnectContext connectContext, String sql) {
            this.connectContext = connectContext;
            this.sql = sql;
        }

        public void explainContains(String... keywords) throws Exception {
            String plan = explainQuery();
            Assert.assertTrue(plan, Stream.of(keywords).allMatch(plan::contains));
        }

        public void explainContains(String keywords, int count) throws Exception {
            Assert.assertEquals(StringUtils.countMatches(explainQuery(), keywords), count);
        }

        public void explainWithout(String s) throws Exception {
            Assert.assertFalse(explainQuery().contains(s));
        }

        public String explainQuery() throws Exception {
            return UtFrameUtils.getFragmentPlan(connectContext, sql);
        }

        // Assert true only if analysisException.getMessage() contains all keywords.
        public void analysisError(String... keywords) {
            try {
                explainQuery();
            } catch (AnalysisException | StarRocksPlannerException analysisException) {
                Assert.assertTrue(Stream.of(keywords).allMatch(analysisException.getMessage()::contains));
                return;
            } catch (Exception ex) {
                Assert.fail();
            }
            Assert.fail();
        }
    }
}
