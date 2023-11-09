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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.BackendCoreStat;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StarRocksAssert {
    private static final Logger LOG = LogManager.getLogger(StarRocksAssert.class);

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
        GlobalStateMgr.getCurrentState().getAuthorizationMgr().createRole(createRoleStmt);
        return this;
    }

    public StarRocksAssert withUser(String user) throws Exception {
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                        "create user " + user + " identified by '';", ctx);
        GlobalStateMgr.getCurrentState().getAuthenticationMgr().createUser(createUserStmt);
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

    // When you want use this func, you need write mock method 'getAllKafkaPartitions' before call this func.
    // example:
    // new MockUp<KafkaUtil>() {
    //     @Mock
    //     public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
    //                                                ImmutableMap<String, String> properties) {
    //         return Lists.newArrayList(0, 1, 2);
    //     }
    // };
    public StarRocksAssert withRoutineLoad(String sql) throws Exception {
        CreateRoutineLoadStmt createRoutineLoadStmt = (CreateRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getRoutineLoadMgr().createRoutineLoadJob(createRoutineLoadStmt);
        return this;
    }

    public StarRocksAssert withLoad(String sql) throws Exception {
        LoadStmt loadStmt = (LoadStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLoadMgr().createLoadJobFromStmt(loadStmt, ctx);
        return this;
    }

    // When you want use this func, you need write mock method 'getAllKafkaPartitions' before call this func.
    // example:
    // new MockUp<BrokerMgr>() {
    //     @Mock
    //     public FsBroker getAnyBroker(String brokerName) {
    //         return new FsBroker();
    //     }
    // };
    public StarRocksAssert withExport(String sql) throws Exception {
        StatementBase createExport = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        StmtExecutor executor = new StmtExecutor(ctx, createExport);
        executor.execute();
        return this;
    }

    public StarRocksAssert withTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        return this;
    }

    public Table getTable(String dbName, String tableName) {
        return ctx.getGlobalStateMgr().mayGetDb(dbName).map(db -> db.getTable(tableName)).orElse(null);
    }

    public StarRocksAssert withSingleReplicaTable(String sql) throws Exception {
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        if (statementBase instanceof CreateTableStmt) {
            CreateTableStmt createTableStmt = (CreateTableStmt) statementBase;
            createTableStmt.getProperties().put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
            return this.withTable(sql);
        } else if (statementBase instanceof CreateMaterializedViewStatement) {
            return this.withMaterializedView(sql, true);
        } else {
            throw new AnalysisException("Sql is not supported in withSingleReplicaTable:" + sql);
        }
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

    public StarRocksAssert dropCatalog(String catalogName) throws Exception {
        DropCatalogStmt dropCatalogStmt =
                (DropCatalogStmt) UtFrameUtils.parseStmtWithNewParser("drop catalog " + catalogName + ";", ctx);
        GlobalStateMgr.getCurrentState().getCatalogMgr().dropCatalog(dropCatalogStmt);
        return this;
    }

    public StarRocksAssert dropDatabase(String dbName) throws Exception {
        DropDbStmt dropDbStmt =
                (DropDbStmt) UtFrameUtils.parseStmtWithNewParser("drop database " + dbName + ";", ctx);
        GlobalStateMgr.getCurrentState().getMetadata().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
        return this;
    }

    public StarRocksAssert alterMvProperties(String sql) throws Exception {
        AlterMaterializedViewStmt alterMvStmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().alterMaterializedView(alterMvStmt);
        return this;
    }

    public StarRocksAssert alterTableProperties(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertFalse(alterTableStmt.getOps().isEmpty());
        Assert.assertTrue(alterTableStmt.getOps().get(0) instanceof ModifyTablePropertiesClause);
        Analyzer.analyze(alterTableStmt, ctx);
        GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
        return this;
    }

    public StarRocksAssert dropTables(List<String> tableNames) throws Exception {
        for (String tableName : tableNames) {
            dropTable(tableName);
        }
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

    // Add materialized view to the schema
    public StarRocksAssert withMaterializedView(String sql) throws Exception {
        return withMaterializedView(sql, false);
    }

    public void assertMVWithoutComplexExpression(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(tableName);
        if (!(table instanceof OlapTable)) {
            return;
        }
        OlapTable olapTable = (OlapTable) table;
        for (MaterializedIndexMeta indexMeta : olapTable.getIndexIdToMeta().values()) {
            Assert.assertFalse(MVUtils.containComplexExpresses(indexMeta));
        }
    }

    public StarRocksAssert withMaterializedView(String sql, boolean isOnlySingleReplica) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        if (stmt instanceof CreateMaterializedViewStmt) {
            CreateMaterializedViewStmt createMaterializedViewStmt = (CreateMaterializedViewStmt) stmt;
            if (isOnlySingleReplica) {
                createMaterializedViewStmt.getProperties().put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
            }
            GlobalStateMgr.getCurrentState().createMaterializedView(createMaterializedViewStmt);
            checkAlterJob();
        } else {
            Preconditions.checkState(stmt instanceof CreateMaterializedViewStatement);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            if (isOnlySingleReplica) {
                createMaterializedViewStatement.getProperties().put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
            }
            GlobalStateMgr.getCurrentState().createMaterializedView(createMaterializedViewStatement);
            String mvName = createMaterializedViewStatement.getTableName().getTbl();
        }
        return this;
    }

    public StarRocksAssert refreshMvByName(String mvName) throws Exception {
        String sql = String.format("refresh materialized view %s with sync mode", mvName);
        getCtx().executeSql(sql);
        return this;
    }

    public StarRocksAssert refreshMvPartition(String sql) throws Exception {
        if (!sql.contains("sync")) {
            sql += " with sync mode";
        }
        getCtx().executeSql(sql);
        return this;
    }

    public void updateTablePartitionVersion(String dbName, String tableName, long version) {
        Table table = GlobalStateMgr.getCurrentState().getDb(dbName).getTable(tableName);
        for (Partition partition : table.getPartitions()) {
            partition.setVisibleVersion(version, System.currentTimeMillis());
            MaterializedIndex baseIndex = partition.getBaseIndex();
            List<Tablet> tablets = baseIndex.getTablets();
            for (Tablet tablet : tablets) {
                List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                for (Replica replica : replicas) {
                    replica.updateVersionInfo(version, -1, version);
                }
            }
        }
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

    public String executeShowResourceUsageSql(String sql) throws DdlException, AnalysisException {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(stmt, ctx);

        ShowExecutor showExecutor = new ShowExecutor(ctx, (ShowStmt) stmt);
        ShowResultSet res =  showExecutor.execute();
        String header = res.getMetaData().getColumns().stream().map(Column::getName).collect(Collectors.joining("|"));
        String body = res.getResultRows().stream()
                .map(row -> String.join("|", row))
                .collect(Collectors.joining("\n"));
        return header + "\n" + body;
    }

    public List<List<String>> show(String sql) throws Exception {
        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Assert.assertTrue(stmt instanceof ShowStmt);
        Analyzer.analyze(stmt, ctx);
        ShowExecutor showExecutor = new ShowExecutor(ctx, (ShowStmt) stmt);
        return showExecutor.execute().getResultRows();
    }

    public void ddl(String sql) throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
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
                Assert.assertTrue(analysisException.getMessage(),
                        Stream.of(keywords).allMatch(analysisException.getMessage()::contains));
                return;
            } catch (Exception ex) {
                Assert.fail();
            }
            Assert.fail();
        }
    }

    public ShowResultSet showTablet(String db, String table) throws DdlException, AnalysisException {
        TableName tableName = new TableName(db, table);
        ShowTabletStmt showTabletStmt = new ShowTabletStmt(tableName, -1, NodePosition.ZERO);
        ShowExecutor showExecutor = new ShowExecutor(getCtx(), showTabletStmt);
        return showExecutor.execute();
    }
}
