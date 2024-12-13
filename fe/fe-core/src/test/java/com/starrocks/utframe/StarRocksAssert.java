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
<<<<<<< HEAD
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
=======
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
<<<<<<< HEAD
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
=======
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AlreadyExistsException;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
<<<<<<< HEAD
=======
import com.starrocks.common.Pair;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.PartitionBasedMvRefreshProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
<<<<<<< HEAD
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
=======
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.TaskRunScheduler;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.schema.MSchema;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDbStmt;
<<<<<<< HEAD
=======
import com.starrocks.sql.ast.CreateFunctionStmt;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateTableStmt;
<<<<<<< HEAD
=======
import com.starrocks.sql.ast.CreateTemporaryTableLikeStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
<<<<<<< HEAD
=======
import com.starrocks.sql.ast.DropTemporaryTableStmt;
import com.starrocks.sql.ast.FunctionArgsDef;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
<<<<<<< HEAD
import com.starrocks.sql.ast.ShowResourceGroupUsageStmt;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.plan.ExecPlan;
<<<<<<< HEAD
import com.starrocks.system.BackendCoreStat;
=======
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.thrift.TFunctionBinaryType;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
<<<<<<< HEAD
import org.apache.parquet.Strings;
=======
import org.assertj.core.util.Sets;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.junit.Assert;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
=======
import java.util.Set;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

    public interface ExceptionRunnable {
        public abstract void run() throws Exception;
    }

    public interface ExceptionConsumer<T> {
        public abstract void accept(T name) throws Exception;
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
<<<<<<< HEAD
                (DropDbStmt) UtFrameUtils.parseStmtWithNewParser("drop database if exists `" + dbName + "`;", ctx);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
=======
                    (DropDbStmt) UtFrameUtils.parseStmtWithNewParser("drop database if exists `" + dbName + "`;", ctx);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        } catch (MetaNotFoundException e) {
            if (!dropDbStmt.isSetIfExists()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
        }

        CreateDbStmt createDbStmt =
<<<<<<< HEAD
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database `" + dbName + "`;", ctx);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        return this;
    }

    public String showCreateTable(String sql) throws Exception {
        return show(sql).get(0).get(1);
=======
                    (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database `" + dbName + "`;", ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        return this;
    }

    public StarRocksAssert createDatabaseIfNotExists(String dbName) throws Exception {
        try {
            CreateDbStmt createDbStmt =
                        (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database if not exists `"
                                    + dbName + "`;", ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        } catch (AlreadyExistsException e) {
            // ignore
        }
        return this;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public StarRocksAssert withRole(String roleName) throws Exception {
        CreateRoleStmt createRoleStmt =
<<<<<<< HEAD
                (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser("create role " + roleName + ";", ctx);
=======
                    (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser("create role " + roleName + ";", ctx);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        GlobalStateMgr.getCurrentState().getAuthorizationMgr().createRole(createRoleStmt);
        return this;
    }

    public StarRocksAssert withUser(String user) throws Exception {
        CreateUserStmt createUserStmt =
<<<<<<< HEAD
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                        "create user " + user + " identified by '';", ctx);
=======
                    (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                                "create user " + user + " identified by '';", ctx);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        GlobalStateMgr.getCurrentState().getAuthenticationMgr().createUser(createUserStmt);
        return this;
    }

    public StarRocksAssert withDatabaseWithoutAnalyze(String dbName) throws Exception {
        CreateDbStmt dbStmt = new CreateDbStmt(false, dbName);
<<<<<<< HEAD
        GlobalStateMgr.getCurrentState().getMetadata().createDb(dbStmt.getFullDbName());
=======
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(dbStmt.getFullDbName());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return this;
    }

    public boolean databaseExist(String dbName) {
<<<<<<< HEAD
        Database db = GlobalStateMgr.getCurrentState().getDb("" + dbName);
=======
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("" + dbName);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return db != null;
    }

    public StarRocksAssert useDatabase(String dbName) {
        ctx.setDatabase(dbName);
        return this;
    }

    public StarRocksAssert useCatalog(String catalogName) throws DdlException {
<<<<<<< HEAD
        ctx.getGlobalStateMgr().changeCatalog(ctx, catalogName);
=======
        ctx.changeCatalog(catalogName);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
        RoutineLoadMgr routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();
<<<<<<< HEAD
        Map<Long, Integer> beTasksNum = routineLoadManager.getBeTasksNum();
=======
        Map<Long, Integer> beTasksNum = routineLoadManager.getNodeTasksNum();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        beTasksNum.put(1L, 100);
        routineLoadManager.createRoutineLoadJob(createRoutineLoadStmt);
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

    public static void utCreateTableWithRetry(CreateTableStmt createTableStmt) throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase(createTableStmt.getDbName());
        utCreateTableWithRetry(createTableStmt, connectContext);
    }

<<<<<<< HEAD
    // retry 3 times when create table in ut to avoid
    // table creation timeout caused by heavy load of testing machine
    public static void utCreateTableWithRetry(CreateTableStmt createTableStmt, ConnectContext ctx) throws Exception {
        int retryTime = 0;
        final int MAX_RETRY_TIME = 3;

        while (retryTime < MAX_RETRY_TIME) {
            try {
                CreateTableStmt createTableStmtCopied = createTableStmt;
                if (retryTime > 0) {
                    // copy `createTableStmt` after the first retry, because the state of `createTableStmt`
                    // may change and throw different error after retry
                    createTableStmtCopied = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                            createTableStmt.getOrigStmt().originStmt, ctx);
                }
                GlobalStateMgr.getCurrentState().createTable(createTableStmtCopied);
                break;
            } catch (Exception e) {
                if (retryTime == MAX_RETRY_TIME - 1) {
=======
    public static void utDropTableWithRetry(DropTableStmt dropTableStmt) throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase(dropTableStmt.getDbName());
        utDropTableWithRetry(dropTableStmt, connectContext);
    }

    @FunctionalInterface
    public interface RetryableOperation {
        void execute(int retryTime) throws Exception;
    }

    // retry 3 times when operator table in ut to avoid
    // table operation timeout caused by heavy load of testing machine
    public static void executeWithRetry(RetryableOperation operation, String operationMsg, int maxRetryTime) throws Exception {
        int retryTime = 0;
        while (retryTime < maxRetryTime) {
            try {
                operation.execute(retryTime);
                break;
            } catch (Exception e) {
                if (retryTime == maxRetryTime - 1) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    if (e.getCause() instanceof DdlException) {
                        throw new DdlException(e.getMessage());
                    } else if (e.getCause() instanceof SemanticException) {
                        throw new SemanticException(e.getMessage());
                    } else if (e.getCause() instanceof AnalysisException) {
                        throw new AnalysisException(e.getMessage());
                    } else {
                        throw e;
                    }
                }
                retryTime++;
<<<<<<< HEAD
                System.out.println("ut create table failed with " + retryTime + " time retry, msg: " +
                        e.getMessage() + ", " + Arrays.toString(e.getStackTrace()));
=======
                System.out.println(operationMsg + " failed with " + retryTime + " time retry, msg: " +
                            e.getMessage() + ", " + Arrays.toString(e.getStackTrace()));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                Thread.sleep(500);
            }
        }
    }

<<<<<<< HEAD
=======
    public static void utCreateFunctionMock(CreateFunctionStmt createFunctionStmt, ConnectContext ctx) throws Exception {
        FunctionName functionName = createFunctionStmt.getFunctionName();
        functionName.analyze(ctx.getDatabase());
        FunctionArgsDef argsDef = createFunctionStmt.getArgsDef();
        TypeDef returnType = createFunctionStmt.getReturnType();
        // check argument
        argsDef.analyze();
        returnType.analyze();

        Function function = ScalarFunction.createUdf(
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(), TFunctionBinaryType.SRJAR,
                "", "", "", "", !"shared".equalsIgnoreCase(""));

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(ctx.getDatabase());
        db.addFunction(function, true, false);
    }

    public static void utCreateTableWithRetry(CreateTableStmt createTableStmt, ConnectContext ctx) throws Exception {
        executeWithRetry((retryTime) -> {
            CreateTableStmt createTableStmtCopied = createTableStmt;
            if (retryTime > 0) {
                createTableStmtCopied = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                            createTableStmt.getOrigStmt().originStmt, ctx);
            }
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmtCopied);
        }, "Create Table", 3);
    }

    public static void utDropTableWithRetry(DropTableStmt dropTableStmt, ConnectContext ctx) throws Exception {
        executeWithRetry((retryTime) -> {
            DropTableStmt dropTableStmtCopied = dropTableStmt;
            if (retryTime > 0) {
                dropTableStmtCopied = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(
                            dropTableStmt.getOrigStmt().originStmt, ctx);
            }
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmtCopied);
        }, "Drop Table", 3);
    }

    public StarRocksAssert withFunction(String sql) throws Exception {
        Config.enable_udf = true;
        CreateFunctionStmt createFunctionStmt =
                    (CreateFunctionStmt) UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, ctx);
        Config.enable_udf = false;
        utCreateFunctionMock(createFunctionStmt, ctx);
        return this;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public StarRocksAssert withTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        utCreateTableWithRetry(createTableStmt, ctx);
        return this;
    }

<<<<<<< HEAD
    public StarRocksAssert withTable(MTable mTable,
                                      ExceptionRunnable action) {
        return withTables(ImmutableList.of(mTable), action);
    }

    public StarRocksAssert withTables(List<MTable> mTables,
                                     ExceptionRunnable action) {
        List<String> names = Lists.newArrayList();
        try {
            for (MTable mTable : mTables) {
                String sql = mTable.getCreateTableSql();
                System.out.println(sql);
                CreateTableStmt createTableStmt =
                        (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
                utCreateTableWithRetry(createTableStmt, ctx);
                names.add(mTable.getTableName());
=======
    public StarRocksAssert withTable(MTable mTable) throws Exception {
        String sql = mTable.getCreateTableSql();
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        utCreateTableWithRetry(createTableStmt, ctx);
        return this;
    }

    public StarRocksAssert withTemporaryTable(String sql) throws Exception {
        CreateTemporaryTableStmt createTableStmt =
                    (CreateTemporaryTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        createTableStmt.setSessionId(ctx.getSessionId());
        utCreateTableWithRetry(createTableStmt, ctx);
        return this;
    }

    public StarRocksAssert withTemporaryTableLike(String dstTable, String srcTable) throws Exception {
        String sql = "create temporary table " + dstTable + " like " + srcTable;
        CreateTemporaryTableLikeStmt createTemporaryTableLikeStmt =
                    (CreateTemporaryTableLikeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        createTemporaryTableLikeStmt.setSessionId(ctx.getSessionId());
        utCreateTableWithRetry(createTemporaryTableLikeStmt.getCreateTableStmt(), ctx);
        return this;
    }

    public StarRocksAssert withTable(MTable mTable,
                                     ExceptionRunnable action) {
        return this.withMTables(List.of(mTable), action);
    }

    public StarRocksAssert withMTable(String mTable,
                                      ExceptionRunnable action) {
        return withMTableNames(null, ImmutableList.of(mTable), action);
    }

    public StarRocksAssert withMTables(List<MTable> mTables,
                                       ExceptionRunnable action) {
        return withMTables(null, mTables, action);
    }

    /**
     * Create tables by using MTables and do insert datas and later actions.
     *
     * @param cluster pseudo cluster
     * @param mTables mtables to be used for creating tables
     * @param action  action after creating base tables
     * @return return this
     */
    public StarRocksAssert withMTables(PseudoCluster cluster,
                                       List<MTable> mTables,
                                       ExceptionRunnable action) {
        List<Pair<String, String>> names = Lists.newArrayList();
        try {
            for (MTable mTable : mTables) {
                String dbName = mTable.getDbName();
                if (!Strings.isNullOrEmpty(dbName)) {
                    this.createDatabaseIfNotExists(dbName);
                    this.useDatabase(dbName);
                }
                String sql = mTable.getCreateTableSql();
                CreateTableStmt createTableStmt =
                            (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
                utCreateTableWithRetry(createTableStmt, ctx);
                names.add(Pair.create(dbName, mTable.getTableName()));

                if (cluster != null) {
                    dbName = createTableStmt.getDbName();
                    String insertSQL = mTable.getGenerateDataSQL();
                    if (!Strings.isNullOrEmpty(insertSQL)) {
                        cluster.runSql(dbName, insertSQL);
                    }
                }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            }
            if (action != null) {
                action.run();
            }
        } catch (Exception e) {
<<<<<<< HEAD
            e.printStackTrace();
            Assert.fail("create table failed");
        } finally {
            for (String t : names) {
                try {
                    dropTable(t);
                } catch (Exception e) {
                    e.printStackTrace();
=======
            Assert.fail("create table failed");
        } finally {
            for (Pair<String, String> t : names) {
                try {
                    if (!Strings.isNullOrEmpty(t.first)) {
                        useDatabase(t.first);
                    }
                    dropTable(t.second);
                } catch (Exception e) {
                    // ignore exceptions
                }
            }
        }
        return this;
    }

    /**
     * With input base table's create table sqls and actions.
     *
     * @param sqls   the base table's create table sqls
     * @param action action after creating base tables
     * @return return this
     */
    public StarRocksAssert withTables(List<String> sqls,
                                      ExceptionRunnable action) {
        Set<String> tables = Sets.newHashSet();
        try {
            for (String sql : sqls) {
                CreateTableStmt createTableStmt =
                            (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
                utCreateTableWithRetry(createTableStmt, ctx);
                tables.add(createTableStmt.getTableName());
            }
            if (action != null) {
                action.run();
            }
        } catch (Exception e) {
            Assert.fail("Do action failed:" + e.getMessage());
        } finally {
            if (action != null) {
                for (String table : tables) {
                    try {
                        dropTable(table);
                    } catch (Exception e) {
                        // ignore exceptions
                    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                }
            }
        }
        return this;
    }

    /**
     * Create tables and do insert datas and later actions.
<<<<<<< HEAD
=======
     *
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
     * @param cluster : insert into datas into table if it's not null.
     * @param tables  : tables which needs to be created and inserted.
     * @param action  : actions which will be used to do later.
     * @return
     */
<<<<<<< HEAD
    public StarRocksAssert withTables(PseudoCluster cluster,
                                      List<String> tables,
                                      ExceptionRunnable action) {
=======
    public StarRocksAssert withMTableNames(PseudoCluster cluster,
                                           List<String> tables,
                                           ExceptionRunnable action) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        try {
            for (String table : tables) {
                MTable mTable = MSchema.getTable(table);
                String sql = mTable.getCreateTableSql();
                CreateTableStmt createTableStmt =
<<<<<<< HEAD
                        (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
=======
                            (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                utCreateTableWithRetry(createTableStmt, ctx);

                if (cluster != null) {
                    String dbName = createTableStmt.getDbName();
                    String insertSQL = mTable.getGenerateDataSQL();
                    if (!Strings.isNullOrEmpty(insertSQL)) {
                        cluster.runSql(dbName, insertSQL);
                    }
                }
            }
            if (action != null) {
                action.run();
            }
        } catch (Exception e) {
<<<<<<< HEAD
            e.printStackTrace();
            Assert.fail("Do action failed:" +  e.getMessage());
=======
            Assert.fail("Do action failed:" + e.getMessage());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        } finally {
            if (action != null) {
                for (String table : tables) {
                    try {
                        dropTable(table);
                    } catch (Exception e) {
<<<<<<< HEAD
                        e.printStackTrace();
=======
                        // ignore exceptions
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    }
                }
            }
        }
        return this;
    }

    /**
     * Create the table and insert datas into the table with no actions later.
     */
    public StarRocksAssert withTable(PseudoCluster cluster,
                                     String tableName) {
<<<<<<< HEAD
        return withTables(cluster, ImmutableList.of(tableName), null);
=======
        return withMTableNames(cluster, List.of(tableName), null);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    /**
     * Create table only and no insert datas into the table and do actions.
     */
    public StarRocksAssert withTable(String table, ExceptionRunnable action) {
<<<<<<< HEAD
        return withTables(null, ImmutableList.of(table), action);
=======
        return withTables(List.of(table), action);
    }

    public StarRocksAssert withTable(String sql, ExceptionConsumer action) {
        return withTables(sql, action);
    }

    public StarRocksAssert withTables(String sql,
                                      ExceptionConsumer action) {
        Set<String> tables = Sets.newHashSet();
        try {
            CreateTableStmt createTableStmt =
                    (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            utCreateTableWithRetry(createTableStmt, ctx);
            tables.add(createTableStmt.getTableName());
            if (action != null) {
                action.accept(createTableStmt.getTableName());
            }
        } catch (Exception e) {
            Assert.fail("Do action failed:" + e.getMessage());
        } finally {
            if (action != null) {
                for (String table : tables) {
                    try {
                        dropTable(table);
                    } catch (Exception e) {
                        // ignore exceptions
                    }
                }
            }
        }
        return this;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    /**
     * Create table and insert datas into the table and do actions.
     */
    public StarRocksAssert withTable(PseudoCluster cluster,
                                     String table,
                                     ExceptionRunnable action) {

<<<<<<< HEAD
        return withTables(cluster, ImmutableList.of(table), action);
=======
        return withMTableNames(cluster, List.of(table), action);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    /**
     * To distinguish `withTable(sql)`, call it `useTable` to select existed table from MSchema.
     */
    public StarRocksAssert useTable(String table) {
<<<<<<< HEAD
        return withTables(null, ImmutableList.of(table), null);
    }

    public Table getTable(String dbName, String tableName) {
        return ctx.getGlobalStateMgr().mayGetDb(dbName).map(db -> db.getTable(tableName)).orElse(null);
    }

    public MaterializedView getMv(String dbName, String tableName) {
        return (MaterializedView) ctx.getGlobalStateMgr().mayGetDb(dbName).map(db -> db.getTable(tableName))
                .orElse(null);
=======
        return withMTableNames(null, List.of(table), null);
    }

    public Table getTable(String dbName, String tableName) {
        return ctx.getGlobalStateMgr().getLocalMetastore().mayGetDb(dbName)
                    .map(db -> GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName))
                    .orElse(null);
    }

    public MaterializedView getMv(String dbName, String tableName) {
        return (MaterializedView) ctx.getGlobalStateMgr().getLocalMetastore()
                    .mayGetDb(dbName)
                    .map(db -> GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName))
                    .orElse(null);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public StarRocksAssert withSingleReplicaTable(String sql) throws Exception {
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            if (!(statementBase instanceof CreateTableStmt)) {
                return this;
            }
            CreateTableStmt createTableStmt = (CreateTableStmt) statementBase;
            createTableStmt.getProperties().put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
            GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        } catch (Exception e) {
            LOG.warn("create table failed, sql:{}", sql, e);
            throw e;
        }
        return this;
    }

    public StarRocksAssert withAsyncMvAndRefresh(String sql) throws Exception {
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            if (!(statementBase instanceof CreateMaterializedViewStatement)) {
                return this;
            }
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) statementBase;
            withAsyncMv(createMaterializedViewStatement, false, true);
        } catch (Exception e) {
            LOG.warn("create mv failed, sql:{}", sql, e);
            throw e;
        }
        return this;
    }

    public void withAsyncMv(
<<<<<<< HEAD
            CreateMaterializedViewStatement createMaterializedViewStatement,
            boolean isOnlySingleReplica,
            boolean isRefresh) throws Exception {
=======
                CreateMaterializedViewStatement createMaterializedViewStatement,
                boolean isOnlySingleReplica,
                boolean isRefresh) throws Exception {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (isOnlySingleReplica) {
            createMaterializedViewStatement.getProperties().put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
        }
        GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMaterializedViewStatement);
        if (isRefresh) {
            new MockUp<StmtExecutor>() {
                @Mock
                public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                    if (stmt instanceof InsertStmt) {
                        InsertStmt insertStmt = (InsertStmt) stmt;
                        TableName tableName = insertStmt.getTableName();
<<<<<<< HEAD
                        Database testDb = GlobalStateMgr.getCurrentState().getDb(stmt.getTableName().getDb());
                        OlapTable tbl = ((OlapTable) testDb.getTable(tableName.getTbl()));
                        for (Partition partition : tbl.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                long version = partition.getVisibleVersion() + 1;
                                partition.setVisibleVersion(version, System.currentTimeMillis());
                                MaterializedIndex baseIndex = partition.getBaseIndex();
=======
                        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(stmt.getTableName().getDb());
                        OlapTable tbl = ((OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTable(testDb.getFullName(), tableName.getTbl()));
                        for (Partition partition : tbl.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                long version = partition.getDefaultPhysicalPartition().getVisibleVersion() + 1;
                                partition.getDefaultPhysicalPartition().setVisibleVersion(version, System.currentTimeMillis());
                                MaterializedIndex baseIndex = partition.getDefaultPhysicalPartition().getBaseIndex();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                                List<Tablet> tablets = baseIndex.getTablets();
                                for (Tablet tablet : tablets) {
                                    List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                                    for (Replica replica : replicas) {
                                        replica.updateVersionInfo(version, -1, version);
                                    }
                                }
                            }
                        }
                    }
                }
            };
            String refreshSql = String.format("refresh materialized view `%s`.`%s` with sync mode;",
<<<<<<< HEAD
                    createMaterializedViewStatement.getTableName().getDb(),
                    createMaterializedViewStatement.getTableName().getTbl());
=======
                        createMaterializedViewStatement.getTableName().getDb(),
                        createMaterializedViewStatement.getTableName().getTbl());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            ctx.executeSql(refreshSql);
        }
    }

    public StarRocksAssert withView(String sql) throws Exception {
        CreateViewStmt createTableStmt = (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
<<<<<<< HEAD
        GlobalStateMgr.getCurrentState().createView(createTableStmt);
=======
        GlobalStateMgr.getCurrentState().getLocalMetastore().createView(createTableStmt);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return this;
    }

    public StarRocksAssert withView(String sql, ExceptionRunnable action) throws Exception {
        CreateViewStmt createTableStmt = (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        String viewName = createTableStmt.getTable();
        try {
            withView(sql);
            action.run();
        } catch (Exception e) {
            Assert.fail("With view " + viewName + " failed:" + e.getMessage());
        } finally {
            dropView(viewName);
        }
        return this;
    }

    public StarRocksAssert dropView(String viewName) throws Exception {
        DropTableStmt dropViewStmt =
<<<<<<< HEAD
                (DropTableStmt) UtFrameUtils.parseStmtWithNewParser("drop view " + viewName + ";", ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropViewStmt);
=======
                    (DropTableStmt) UtFrameUtils.parseStmtWithNewParser("drop view " + viewName + ";", ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropViewStmt);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return this;
    }

    public StarRocksAssert dropCatalog(String catalogName) throws Exception {
        DropCatalogStmt dropCatalogStmt =
<<<<<<< HEAD
                (DropCatalogStmt) UtFrameUtils.parseStmtWithNewParser("drop catalog " + catalogName + ";", ctx);
=======
                    (DropCatalogStmt) UtFrameUtils.parseStmtWithNewParser("drop catalog " + catalogName + ";", ctx);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        GlobalStateMgr.getCurrentState().getCatalogMgr().dropCatalog(dropCatalogStmt);
        return this;
    }

    public StarRocksAssert dropDatabase(String dbName) throws Exception {
        DropDbStmt dropDbStmt =
<<<<<<< HEAD
                (DropDbStmt) UtFrameUtils.parseStmtWithNewParser("drop database " + dbName + ";", ctx);
        GlobalStateMgr.getCurrentState().getMetadata().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
=======
                    (DropDbStmt) UtFrameUtils.parseStmtWithNewParser("drop database " + dbName + ";", ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(dropDbStmt.getDbName(), dropDbStmt.isForceDrop());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return this;
    }

    public StarRocksAssert alterMvProperties(String sql) throws Exception {
        AlterMaterializedViewStmt alterMvStmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
<<<<<<< HEAD
        GlobalStateMgr.getCurrentState().alterMaterializedView(alterMvStmt);
=======
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterMaterializedView(alterMvStmt);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return this;
    }

    public StarRocksAssert alterTableProperties(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
<<<<<<< HEAD
        Assert.assertFalse(alterTableStmt.getOps().isEmpty());
        Assert.assertTrue(alterTableStmt.getOps().get(0) instanceof ModifyTablePropertiesClause);
        Analyzer.analyze(alterTableStmt, ctx);
        GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
=======
        Assert.assertFalse(alterTableStmt.getAlterClauseList().isEmpty());
        Assert.assertTrue(alterTableStmt.getAlterClauseList().get(0) instanceof ModifyTablePropertiesClause);
        Analyzer.analyze(alterTableStmt, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
        return this;
    }

    public StarRocksAssert alterTable(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Analyzer.analyze(alterTableStmt, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
                (DropTableStmt) UtFrameUtils.parseStmtWithNewParser("drop table " + tableName + ";", ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
=======
                    (DropTableStmt) UtFrameUtils.parseStmtWithNewParser("drop table " + tableName + ";", ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        return this;
    }

    public StarRocksAssert dropTemporaryTable(String tableName, boolean ifExists) throws Exception {
        DropTemporaryTableStmt dropTemporaryTableStmt = (DropTemporaryTableStmt)
                    UtFrameUtils.parseStmtWithNewParser("drop temporary table " + (ifExists ? "if exists " : "")
                                + tableName + ";", ctx);
        GlobalStateMgr.getCurrentState().getMetadataMgr().dropTemporaryTable(dropTemporaryTableStmt);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return this;
    }

    public StarRocksAssert dropMaterializedView(String materializedViewName) throws Exception {
        DropMaterializedViewStmt dropMaterializedViewStmt = (DropMaterializedViewStmt) UtFrameUtils.
<<<<<<< HEAD
                parseStmtWithNewParser("drop materialized view if exists " + materializedViewName + ";", ctx);
        GlobalStateMgr.getCurrentState().dropMaterializedView(dropMaterializedViewStmt);
=======
                    parseStmtWithNewParser("drop materialized view if exists " + materializedViewName + ";", ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropMaterializedView(dropMaterializedViewStmt);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return this;
    }

    // Add materialized view to the schema
    public StarRocksAssert withMaterializedView(String sql) throws Exception {
        // Only test mv rewrite in default 1 replica to avoid more occasional errors.
        return withMaterializedView(sql, true, false);
    }

    public StarRocksAssert withMaterializedView(String sql, ExceptionConsumer action) {
        String mvName = null;
        try {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Preconditions.checkState(stmt instanceof CreateMaterializedViewStatement);
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
            mvName = createMaterializedViewStatement.getTableName().getTbl();
            withMaterializedView(sql);
            action.accept(mvName);
        } catch (Exception e) {
<<<<<<< HEAD
            e.printStackTrace();
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            Assert.fail();
        } finally {
            // Create mv may fail.
            if (!Strings.isNullOrEmpty(mvName)) {
                try {
                    dropMaterializedView(mvName);
                } catch (Exception e) {
<<<<<<< HEAD
                    // e.printStackTrace();
=======
                    // ignore exceptions
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                }
            }
        }
        return this;
    }

    public StarRocksAssert withRefreshedMaterializedViews(List<String> sqls, ExceptionConsumer action) {
        return withMaterializedViews(sqls, action, true);
    }

    public StarRocksAssert withMaterializedViews(List<String> sqls, ExceptionConsumer action) {
        return withMaterializedViews(sqls, action, false);
    }

    /**
     * Create mvs with using sqls and refresh it or not by {@code isRefresh}, and then do {@code action} after
     * create it success.
     */
    private StarRocksAssert withMaterializedViews(List<String> sqls, ExceptionConsumer action, boolean isRefresh) {
        List<String> mvNames = Lists.newArrayList();
        try {
            for (String sql : sqls) {
                StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
                Preconditions.checkState(stmt instanceof CreateMaterializedViewStatement);
                CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
                String mvName = createMaterializedViewStatement.getTableName().getTbl();
<<<<<<< HEAD
                System.out.println(sql);
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                mvNames.add(mvName);
                withMaterializedView(sql, true, isRefresh);
            }
            action.accept(mvNames);
        } catch (Exception e) {
            Assert.fail();
        } finally {
            // Create mv may fail.
            for (String mvName : mvNames) {
                if (!Strings.isNullOrEmpty(mvName)) {
                    try {
                        dropMaterializedView(mvName);
                    } catch (Exception e) {
<<<<<<< HEAD
                        e.printStackTrace();
=======
                        // ignore exceptions
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    }
                }
            }
        }
        return this;
    }

    public StarRocksAssert withMaterializedView(String sql, ExceptionRunnable action) {
        String mvName = null;
        try {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Preconditions.checkState(stmt instanceof CreateMaterializedViewStatement);
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
            mvName = createMaterializedViewStatement.getTableName().getTbl();
<<<<<<< HEAD
            System.out.println(sql);
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            withMaterializedView(sql);
            action.run();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
<<<<<<< HEAD
            Preconditions.checkState(!Strings.isNullOrEmpty(mvName));
            try {
                dropMaterializedView(mvName);
            } catch (Exception e) {
                e.printStackTrace();
=======
            // Create mv may fail.
            if (!Strings.isNullOrEmpty(mvName)) {
                try {
                    dropMaterializedView(mvName);
                } catch (Exception e) {
                    // ignore exceptions
                }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            }
        }
        return this;
    }

    public void assertMVWithoutComplexExpression(String dbName, String tableName) {
<<<<<<< HEAD
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(tableName);
=======
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (!(table instanceof OlapTable)) {
            return;
        }
        OlapTable olapTable = (OlapTable) table;
        for (MaterializedIndexMeta indexMeta : olapTable.getIndexIdToMeta().values()) {
            Assert.assertFalse(MVUtils.containComplexExpresses(indexMeta));
        }
    }

    public String getMVName(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        if (stmt instanceof CreateMaterializedViewStmt) {
            CreateMaterializedViewStmt createMaterializedViewStmt = (CreateMaterializedViewStmt) stmt;
            return createMaterializedViewStmt.getMVName();
        } else {
            Preconditions.checkState(stmt instanceof CreateMaterializedViewStatement);
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
            return createMaterializedViewStatement.getTableName().getTbl();
        }
    }

    public StarRocksAssert withRefreshedMaterializedView(String sql) throws Exception {
        return withMaterializedView(sql, true, true);
    }

    public StarRocksAssert withMaterializedView(String sql,
                                                boolean isOnlySingleReplica,
                                                boolean isRefresh) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        if (stmt instanceof CreateMaterializedViewStmt) {
            CreateMaterializedViewStmt createMaterializedViewStmt = (CreateMaterializedViewStmt) stmt;
            if (isOnlySingleReplica) {
                createMaterializedViewStmt.getProperties().put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
            }
<<<<<<< HEAD
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
            if (isRefresh) {
=======
            GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMaterializedViewStmt);
            checkAlterJob();
        } else {
            Preconditions.checkState(stmt instanceof CreateMaterializedViewStatement);
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
            if (isOnlySingleReplica) {
                createMaterializedViewStatement.getProperties().put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
            }
            GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMaterializedViewStatement);
            if (isRefresh) {
                String mvName = createMaterializedViewStatement.getTableName().getTbl();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                refreshMvPartition(String.format("refresh materialized view %s", mvName));
            }
        }
        return this;
    }

<<<<<<< HEAD
    public void refreshMV(String sql) throws Exception {
        if (!sql.toLowerCase().endsWith("with sync mode")) {
            sql += " with sync mode";
        }
        ddl(sql);
    }

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public StarRocksAssert refreshMvPartition(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        if (stmt instanceof RefreshMaterializedViewStatement) {
            RefreshMaterializedViewStatement refreshMaterializedViewStatement = (RefreshMaterializedViewStatement) stmt;

            TableName mvName = refreshMaterializedViewStatement.getMvName();
<<<<<<< HEAD
            Database db = GlobalStateMgr.getCurrentState().getDb(mvName.getDb());
            Table table = db.getTable(mvName.getTbl());
=======
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvName.getDb());
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), mvName.getTbl());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            Assert.assertNotNull(table);
            Assert.assertTrue(table instanceof MaterializedView);
            MaterializedView mv = (MaterializedView) table;

            HashMap<String, String> taskRunProperties = new HashMap<>();
            PartitionRangeDesc range = refreshMaterializedViewStatement.getPartitionRangeDesc();
            taskRunProperties.put(TaskRun.PARTITION_START, range == null ? null : range.getPartitionStart());
            taskRunProperties.put(TaskRun.PARTITION_END, range == null ? null : range.getPartitionEnd());
            taskRunProperties.put(TaskRun.FORCE, "true");

            Task task = TaskBuilder.rebuildMvTask(mv, mvName.getDb(), taskRunProperties, null);
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).properties(taskRunProperties).build();
            taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
            taskRun.executeTaskRun();
            waitingTaskFinish(taskRun);
        }
        return this;
    }

<<<<<<< HEAD
=======
    /**
     * Wait the input mv refresh task finished.
     *
     * @param mvId: mv id
     * @return true if the mv refresh task finished, otherwise false.
     */
    public boolean waitRefreshFinished(long mvId) {
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = tm.getTask(TaskBuilder.getMvTaskName(mvId));
        Assert.assertTrue(task != null);
        TaskRunManager taskRunManager = tm.getTaskRunManager();
        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();
        TaskRun taskRun = taskRunScheduler.getRunnableTaskRun(task.getId());
        int maxTimes = 1200;
        int count = 0;
        while (taskRun != null && count < maxTimes) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(500L);
            taskRun = taskRunScheduler.getRunnableTaskRun(task.getId());
            count += 1;
        }
        return taskRun == null;
    }

    /**
     * Refresh materialized view asynchronously.
     *
     * @param ctx    connnect context
     * @param mvName mv's name
     */
    public StarRocksAssert refreshMV(ConnectContext ctx, String mvName) throws Exception {
        String sql = "REFRESH MATERIALIZED VIEW " + mvName;
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        RefreshMaterializedViewStatement refreshMaterializedViewStatement = (RefreshMaterializedViewStatement) stmt;
        TableName tableName = refreshMaterializedViewStatement.getMvName();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(tableName.getDb());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName.getTbl());
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof MaterializedView);
        ctx.executeSql(sql);
        return this;
    }

    public StarRocksAssert refreshMV(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        RefreshMaterializedViewStatement refreshMaterializedViewStatement = (RefreshMaterializedViewStatement) stmt;
        TableName mvName = refreshMaterializedViewStatement.getMvName();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvName.getDb());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), mvName.getTbl());
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        getCtx().executeSql(sql);
        waitRefreshFinished(mv.getId());
        return this;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    private void waitingTaskFinish(TaskRun taskRun) {
        MvTaskRunContext mvContext = ((PartitionBasedMvRefreshProcessor) taskRun.getProcessor()).getMvContext();
        int retryCount = 0;
        int maxRetry = 5;
        while (retryCount < maxRetry) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            if (mvContext.getNextPartitionStart() == null && mvContext.getNextPartitionEnd() == null) {
                break;
            }
            retryCount++;
        }
    }

    public void updateTablePartitionVersion(String dbName, String tableName, long version) {
<<<<<<< HEAD
        Table table = GlobalStateMgr.getCurrentState().getDb(dbName).getTable(tableName);
        for (Partition partition : table.getPartitions()) {
=======
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName).getTable(tableName);
        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
=======
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 32);
=======
        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 32);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        StatementBase statement = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(statement, ctx);

        Assert.assertTrue(statement.getClass().getSimpleName().contains("ResourceGroupStmt"));
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        DDLStmtExecutor.execute((DdlStmt) statement, connectCtx);
    }

    public List<List<String>> executeResourceGroupShowSql(String sql) throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
<<<<<<< HEAD
        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 32);
=======
        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 32);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        StatementBase statement = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statement, ctx);

        Assert.assertTrue(statement instanceof ShowResourceGroupStmt);
        return GlobalStateMgr.getCurrentState().getResourceGroupMgr().showResourceGroup((ShowResourceGroupStmt) statement);
    }

<<<<<<< HEAD
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

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public String executeShowResourceUsageSql(String sql) throws DdlException, AnalysisException {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(stmt, ctx);

<<<<<<< HEAD
        ShowExecutor showExecutor = new ShowExecutor(ctx, (ShowResourceGroupUsageStmt) stmt);
        ShowResultSet res = showExecutor.execute();
        String header = res.getMetaData().getColumns().stream().map(Column::getName).collect(Collectors.joining("|"));
        String body = res.getResultRows().stream()
                .map(row -> String.join("|", row))
                .collect(Collectors.joining("\n"));
        return header + "\n" + body;
    }

=======
        ShowResultSet res = ShowExecutor.execute((ShowStmt) stmt, ctx);
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
        return ShowExecutor.execute((ShowStmt) stmt, ctx).getResultRows();
    }

    public String showCreateTable(String sql) throws Exception {
        return show(sql).get(0).get(1);
    }

    public void ddl(String sql) throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
    }

    public void dropAnalyzeForTable(String tableName) throws Exception {
        List<String> showJob = show(String.format("show analyze job where `table` = '%s' ", tableName)).get(0);
        if (showJob.isEmpty()) {
            return;
        }
        long jobId = Long.parseLong(showJob.get(0));
        ddl("drop analyze " + jobId);
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    private void checkAlterJob() throws InterruptedException {
        // check alter job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            if (alterJobV2.getJobState().isFinalState()) {
                continue;
            }
<<<<<<< HEAD
            Database database = GlobalStateMgr.getCurrentState().getDb(alterJobV2.getDbId());
            Table table = database.getTable(alterJobV2.getTableId());
=======
            Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(alterJobV2.getDbId());
            Table table =
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), alterJobV2.getTableId());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

    /**
     * Enumerate all test cases from combinations
     */
    public static class TestCaseEnumerator {

        private List<List<Integer>> testCases;

        public TestCaseEnumerator(List<Integer> space) {
            this.testCases = Lists.newArrayList();
            List<Integer> testCase = Lists.newArrayList();
            search(space, testCase, 0);
        }

        private void search(List<Integer> space, List<Integer> testCase, int k) {
            if (k >= space.size()) {
                testCases.add(Lists.newArrayList(testCase));
                return;
            }

            int n = space.get(k);
            for (int i = 0; i < n; i++) {
                testCase.add(i);
                search(space, testCase, k + 1);
                testCase.remove(testCase.size() - 1);
            }
        }

        public Stream<List<Integer>> enumerate() {
            return testCases.stream();
        }

        @SafeVarargs
        public static <T> Arguments ofArguments(List<Integer> permutation, List<T>... args) {
            Object[] objects = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                int index = permutation.get(i);
                objects[i] = args[i].get(index);
            }
            return Arguments.of(objects);
        }
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
<<<<<<< HEAD
            Assert.assertTrue(plan, Stream.of(keywords).allMatch(plan::contains));
=======
            for (String keyword : keywords) {
                PlanTestBase.assertContains(plan, keyword);
            }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }

        public void explainContains(String keywords, int count) throws Exception {
            Assert.assertEquals(StringUtils.countMatches(explainQuery(), keywords), count);
        }

        public void explainWithout(String s) throws Exception {
<<<<<<< HEAD
            Assert.assertFalse(explainQuery().contains(s));
=======
            PlanTestBase.assertNotContains(explainQuery(), s);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
                        Stream.of(keywords).allMatch(analysisException.getMessage()::contains));
=======
                            Stream.of(keywords).allMatch(analysisException.getMessage()::contains));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        ShowExecutor showExecutor = new ShowExecutor(getCtx(), showTabletStmt);
        return showExecutor.execute();
=======
        return ShowExecutor.execute(showTabletStmt, getCtx());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }
}
