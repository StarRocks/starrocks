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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/utframe/UtFrameUtils.java

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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.LogUtil;
import com.starrocks.connector.hive.ReplayMetadataMgr;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalTask;
import com.starrocks.meta.MetaContext;
import com.starrocks.persist.EditLog;
import com.starrocks.planner.PlanFragment;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.Explain;
import com.starrocks.sql.InsertPlanner;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.SqlDigestBuilder;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.dump.MockDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.Backend;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.starrocks.sql.plan.PlanTestBase.setPartitionStatistics;

public class UtFrameUtils {
    private static final AtomicInteger INDEX = new AtomicInteger(0);
    private static final AtomicBoolean CREATED_MIN_CLUSTER = new AtomicBoolean(false);

    public static final String CREATE_STATISTICS_TABLE_STMT = "CREATE TABLE `table_statistic_v1` (\n" +
            "  `table_id` bigint(20) NOT NULL COMMENT \"\",\n" +
            "  `column_name` varchar(65530) NOT NULL COMMENT \"\",\n" +
            "  `db_id` bigint(20) NOT NULL COMMENT \"\",\n" +
            "  `table_name` varchar(65530) NOT NULL COMMENT \"\",\n" +
            "  `db_name` varchar(65530) NOT NULL COMMENT \"\",\n" +
            "  `row_count` bigint(20) NOT NULL COMMENT \"\",\n" +
            "  `data_size` bigint(20) NOT NULL COMMENT \"\",\n" +
            "  `distinct_count` bigint(20) NOT NULL COMMENT \"\",\n" +
            "  `null_count` bigint(20) NOT NULL COMMENT \"\",\n" +
            "  `max` varchar(65530) NOT NULL COMMENT \"\",\n" +
            "  `min` varchar(65530) NOT NULL COMMENT \"\",\n" +
            "  `update_time` datetime NOT NULL COMMENT \"\"\n" +
            ") ENGINE=OLAP\n" +
            "UNIQUE KEY(`table_id`, `column_name`, `db_id`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`table_id`, `column_name`, `db_id`) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ");";

    // Help to create a mocked ConnectContext.
    public static ConnectContext createDefaultCtx() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setThreadLocalInfo();
        ctx.setDumpInfo(new MockDumpInfo());
        return ctx;
    }

    // Parse an origin stmt . Return a StatementBase instance.
    public static StatementBase parseStmtWithNewParser(String originStmt, ConnectContext ctx)
            throws Exception {
        StatementBase statementBase;
        try {
            statementBase =
                    com.starrocks.sql.parser.SqlParser.parse(originStmt, ctx.getSessionVariable().getSqlMode()).get(0);
            com.starrocks.sql.analyzer.Analyzer.analyze(statementBase, ctx);
        } catch (ParsingException | SemanticException e) {
            System.err.println("parse failed: " + e.getMessage());
            if (e.getMessage() == null) {
                throw e;
            } else {
                throw new AnalysisException(e.getMessage(), e);
            }
        }

        return statementBase;
    }

    public static StatementBase parseStmtWithNewParserNotIncludeAnalyzer(String originStmt, ConnectContext ctx)
            throws Exception {
        StatementBase statementBase;
        try {
            statementBase =
                    com.starrocks.sql.parser.SqlParser.parse(originStmt, ctx.getSessionVariable().getSqlMode()).get(0);
        } catch (ParsingException e) {
            if (e.getMessage() == null) {
                throw e;
            } else {
                throw new AnalysisException(e.getMessage(), e);
            }
        }

        return statementBase;
    }

    private static void startFEServer(String runningDir, boolean startBDB) throws Exception {
        // get STARROCKS_HOME
        String starRocksHome = System.getenv("STARROCKS_HOME");
        if (Strings.isNullOrEmpty(starRocksHome)) {
            starRocksHome = Files.createTempDirectory("STARROCKS_HOME").toAbsolutePath().toString();
        }

        Config.plugin_dir = starRocksHome + "/plugins";
        // start fe in "STARROCKS_HOME/fe/mocked/"
        MockedFrontend frontend = MockedFrontend.getInstance();
        Map<String, String> feConfMap = Maps.newHashMap();
        // set additional fe config

        if (startBDB) {
            feConfMap.put("edit_log_port", String.valueOf(findValidPort()));
        }
        feConfMap.put("tablet_create_timeout_second", "10");
        frontend.init(starRocksHome + "/" + runningDir, feConfMap);
        frontend.start(startBDB, new String[0]);
    }

    public static synchronized void createMinStarRocksCluster(boolean startBDB) {
        // to avoid call createMinStarRocksCluster multiple times
        if (CREATED_MIN_CLUSTER.get()) {
            return;
        }
        try {
            ClientPool.heartbeatPool = new MockGenericPool.HeatBeatPool("heartbeat");
            ClientPool.backendPool = new MockGenericPool.BackendThriftPool("backend");

            startFEServer("fe/mocked/test/" + UUID.randomUUID().toString() + "/", startBDB);

            addMockBackend(10001);

            // sleep to wait first heartbeat
            int retry = 0;
            while (GlobalStateMgr.getCurrentSystemInfo().getBackend(10001).getBePort() == -1 &&
                    retry++ < 600) {
                Thread.sleep(100);
            }
            CREATED_MIN_CLUSTER.set(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createMinStarRocksCluster() {
        createMinStarRocksCluster(false);
    }

    public static Backend addMockBackend(int backendId) throws Exception {
        // start be
        MockedBackend backend = new MockedBackend("127.0.0.1");

        // add be
        Backend be = new Backend(backendId, backend.getHost(), backend.getHeartBeatPort());
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo(backendId + "/path1");
        diskInfo1.setTotalCapacityB(1000000);
        diskInfo1.setAvailableCapacityB(500000);
        diskInfo1.setDataUsedCapacityB(480000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);
        be.setDisks(ImmutableMap.copyOf(disks));
        be.setAlive(true);
        be.setBePort(backend.getBeThriftPort());
        be.setBrpcPort(backend.getBrpcPort());
        be.setHttpPort(backend.getHttpPort());
        GlobalStateMgr.getCurrentSystemInfo().addBackend(be);

        return be;
    }

    public static void addBroker(String brokerName) throws Exception {
        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<>("127.0.0.1", 8080);
        addresses.add(pair);
        String location = "bos://backup-cmy";
        GlobalStateMgr.getCurrentState().getBrokerMgr().addBrokers(brokerName, addresses);
    }

    public static void dropMockBackend(int backendId) throws DdlException {
        GlobalStateMgr.getCurrentSystemInfo().dropBackend(backendId);
    }

    public static int findValidPort() {
        String starRocksHome = System.getenv("STARROCKS_HOME");
        File portDir = new File(starRocksHome + "/fe/ut_ports");
        if (!portDir.exists()) {
            Preconditions.checkState(portDir.mkdirs());
        }
        for (int i = 0; i < 10; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                int port = socket.getLocalPort();
                File file = new File(starRocksHome + "/fe/ut_ports/" + port);
                if (file.exists()) {
                    continue;
                }

                RandomAccessFile accessFile = new RandomAccessFile(file, "rws");
                FileLock lock = accessFile.getChannel().tryLock();
                if (lock == null) {
                    continue;
                }

                System.out.println("find valid port " + port + new Date());
                return port;
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException("Could not find a free TCP/IP port " + e.getMessage());
            }
        }

        throw new RuntimeException("can not find valid port");
    }

    /**
     * Validate whether all the fragments belong to the fragment tree.
     *
     * @param plan The plan need to validate.
     */
    public static void validatePlanConnectedness(ExecPlan plan) {
        PlanFragment root = plan.getTopFragment();

        Queue<PlanFragment> queue = Lists.newLinkedList();
        Set<PlanFragment> visitedFragments = Sets.newHashSet();
        visitedFragments.add(root);
        queue.add(root);
        while (!queue.isEmpty()) {
            PlanFragment fragment = queue.poll();
            for (PlanFragment child : fragment.getChildren()) {
                if (!visitedFragments.contains(child)) {
                    visitedFragments.add(child);
                    queue.add(child);
                }
            }
        }

        Assert.assertEquals("Some fragments do not belong to the fragment tree",
                plan.getFragments().size(), visitedFragments.size());
    }

    /*
     * Return analyzed statement and execution plan for MV maintenance
     */
    public static Pair<CreateMaterializedViewStatement, ExecPlan> planMVMaintenance(ConnectContext connectContext,
                                                                                    String sql)
            throws DdlException, CloneNotSupportedException {
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));

        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode());
        connectContext.getDumpInfo().setOriginStmt(sql);
        SessionVariable oldSessionVariable = connectContext.getSessionVariable();
        StatementBase statementBase = statements.get(0);

        try {
            // update session variable by adding optional hints.
            if (statementBase instanceof QueryStatement &&
                    ((QueryStatement) statementBase).getQueryRelation() instanceof SelectRelation) {
                SelectRelation selectRelation = (SelectRelation) ((QueryStatement) statementBase).getQueryRelation();
                Map<String, String> optHints = selectRelation.getSelectList().getOptHints();
                if (optHints != null) {
                    SessionVariable sessionVariable = (SessionVariable) oldSessionVariable.clone();
                    for (String key : optHints.keySet()) {
                        VariableMgr.setSystemVariable(sessionVariable,
                                new SystemVariable(key, new StringLiteral(optHints.get(key))), true);
                    }
                    connectContext.setSessionVariable(sessionVariable);
                }
            }

            ExecPlan execPlan = StatementPlanner.plan(statementBase, connectContext);
            Assert.assertTrue(statementBase instanceof CreateMaterializedViewStatement);
            CreateMaterializedViewStatement createMVStmt = (CreateMaterializedViewStatement) statementBase;
            return Pair.create(createMVStmt, createMVStmt.getMaintenancePlan());
        } finally {
            // before returning we have to restore session variable.
            connectContext.setSessionVariable(oldSessionVariable);
        }
    }

    public static Pair<String, ExecPlan> getPlanAndFragment(ConnectContext connectContext, String originStmt)
            throws Exception {
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        originStmt = LogUtil.removeCommentAndLineSeparator(originStmt);

        List<StatementBase> statements;
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Parser")) {
            statements = SqlParser.parse(originStmt, connectContext.getSessionVariable());
        }
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        SessionVariable oldSessionVariable = connectContext.getSessionVariable();
        StatementBase statementBase = statements.get(0);

        try {
            // update session variable by adding optional hints.
            if (statementBase instanceof QueryStatement &&
                    ((QueryStatement) statementBase).getQueryRelation() instanceof SelectRelation) {
                SelectRelation selectRelation = (SelectRelation) ((QueryStatement) statementBase).getQueryRelation();
                Map<String, String> optHints = selectRelation.getSelectList().getOptHints();
                if (optHints != null) {
                    SessionVariable sessionVariable = (SessionVariable) oldSessionVariable.clone();
                    for (String key : optHints.keySet()) {
                        VariableMgr.setSystemVariable(sessionVariable,
                                new SystemVariable(key, new StringLiteral(optHints.get(key))), true);
                    }
                    connectContext.setSessionVariable(sessionVariable);
                }
            }

            ExecPlan execPlan;
            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Planner")) {
                execPlan = StatementPlanner.plan(statementBase, connectContext);
            }

            testView(connectContext, originStmt, statementBase);

            validatePlanConnectedness(execPlan);
            return new Pair<>(printPhysicalPlan(execPlan.getPhysicalPlan()), execPlan);
        } finally {
            // before returning we have to restore session variable.
            connectContext.setSessionVariable(oldSessionVariable);
        }
    }

    public static String printPhysicalPlan(OptExpression execPlan) {
        return LogicalPlanPrinter.print(execPlan, isPrintPlanTableNames());
    }

    public static boolean isPrintPlanTableNames() {
        return false;
    }

    private static void testView(ConnectContext connectContext, String originStmt, StatementBase statementBase)
            throws Exception {
        if (statementBase instanceof QueryStatement && !connectContext.getDatabase().isEmpty() &&
                !statementBase.isExplain()) {
            String viewName = "view" + INDEX.getAndIncrement();
            String createView = "create view " + viewName + " as " + originStmt;
            CreateViewStmt createTableStmt;
            try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("Test View")) {
                createTableStmt = (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(createView, connectContext);
                try {
                    StatementBase viewStatement =
                            SqlParser.parse(createTableStmt.getInlineViewDef(),
                                    connectContext.getSessionVariable().getSqlMode()).get(0);
                    com.starrocks.sql.analyzer.Analyzer.analyze(viewStatement, connectContext);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    throw e;
                }
            } catch (SemanticException | AnalysisException e) {
                if (!e.getMessage().contains("Duplicate column name")) {
                    throw e;
                }
            }
        }
    }

    public static String printPlan(ExecPlan plan) {
        return Explain.toString(plan.getPhysicalPlan(), plan.getOutputColumns());
    }

    public static String getStmtDigest(ConnectContext connectContext, String originStmt) throws Exception {
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable())
                        .get(0);
        Preconditions.checkState(statementBase instanceof QueryStatement);
        QueryStatement queryStmt = (QueryStatement) statementBase;
        String digest = SqlDigestBuilder.build(queryStmt);
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.reset();
            md.update(digest.getBytes());
            return Hex.encodeHexString(md.digest());
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

    private static String initMockEnv(ConnectContext connectContext, QueryDumpInfo replayDumpInfo) throws Exception {
        // mock statistics table
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        if (!starRocksAssert.databaseExist("_statistics_")) {
            starRocksAssert.withDatabaseWithoutAnalyze(StatsConstants.STATISTICS_DB_NAME)
                    .useDatabase(StatsConstants.STATISTICS_DB_NAME);
            starRocksAssert.withTable(CREATE_STATISTICS_TABLE_STMT);
        }
        // prepare dump mock environment
        // statement
        String replaySql = replayDumpInfo.getOriginStmt();
        // session variable
        connectContext.setSessionVariable(replayDumpInfo.getSessionVariable());
        // create resource
        for (String createResourceStmt : replayDumpInfo.getCreateResourceStmtList()) {
            starRocksAssert.withResource(createResourceStmt);
        }

        // mock replay external table info
        if (!replayDumpInfo.getHmsTableMap().isEmpty()) {
            ReplayMetadataMgr replayMetadataMgr = new ReplayMetadataMgr(
                    GlobalStateMgr.getCurrentState().getLocalMetastore(),
                    GlobalStateMgr.getCurrentState().getConnectorMgr(),
                    GlobalStateMgr.getCurrentState().getResourceMgr(),
                    replayDumpInfo.getHmsTableMap(),
                    replayDumpInfo.getTableStatisticsMap());
            GlobalStateMgr.getCurrentState().setMetadataMgr(replayMetadataMgr);
        }

        // create table
        int backendId = 10002;
        int backendIdSize = GlobalStateMgr.getCurrentSystemInfo().getAliveBackendNumber();
        for (int i = 1; i < backendIdSize; ++i) {
            UtFrameUtils.dropMockBackend(backendId++);
        }

        Set<String> dbSet = replayDumpInfo.getCreateTableStmtMap().keySet().stream().map(key -> key.split("\\.")[0])
                .collect(Collectors.toSet());
        dbSet.forEach(db -> {
            if (starRocksAssert.databaseExist(db)) {
                try {
                    starRocksAssert.dropDatabase(db);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        for (Map.Entry<String, String> entry : replayDumpInfo.getCreateTableStmtMap().entrySet()) {
            String dbName = entry.getKey().split("\\.")[0];
            if (!starRocksAssert.databaseExist(dbName)) {
                starRocksAssert.withDatabase(dbName);
            }
            starRocksAssert.useDatabase(dbName);
            starRocksAssert.withSingleReplicaTable(entry.getValue());
        }
        // create view
        for (Map.Entry<String, String> entry : replayDumpInfo.getCreateViewStmtMap().entrySet()) {
            String dbName = entry.getKey().split("\\.")[0];
            if (!starRocksAssert.databaseExist(dbName)) {
                starRocksAssert.withDatabase(dbName);
            }
            starRocksAssert.useDatabase(dbName);
            String createView = "create view " + entry.getKey() + " as " + entry.getValue();
            starRocksAssert.withView(createView);
        }
        // mock be num
        backendId = 10002;
        for (int i = 1; i < replayDumpInfo.getBeNum(); ++i) {
            UtFrameUtils.addMockBackend(backendId++);
        }
        // mock be core stat
        for (Map.Entry<Long, Integer> entry : replayDumpInfo.getNumOfHardwareCoresPerBe().entrySet()) {
            BackendCoreStat.setNumOfHardwareCoresOfBe(entry.getKey(), entry.getValue());
        }
        BackendCoreStat.setCachedAvgNumOfHardwareCores(replayDumpInfo.getCachedAvgNumOfHardwareCores());

        // mock table row count
        for (Map.Entry<String, Map<String, Long>> entry : replayDumpInfo.getPartitionRowCountMap().entrySet()) {
            String dbName = entry.getKey().split("\\.")[0];
            OlapTable replayTable = (OlapTable) GlobalStateMgr.getCurrentState().getDb("" + dbName)
                    .getTable(entry.getKey().split("\\.")[1]);

            for (Map.Entry<String, Long> partitionEntry : entry.getValue().entrySet()) {
                setPartitionStatistics(replayTable, partitionEntry.getKey(), partitionEntry.getValue());
            }
        }
        // mock table column statistics
        for (Map.Entry<String, Map<String, ColumnStatistic>> entry : replayDumpInfo.getTableStatisticsMap()
                .entrySet()) {
            String dbName = entry.getKey().split("\\.")[0];
            Table replayTable = GlobalStateMgr.getCurrentState().getDb("" + dbName)
                    .getTable(entry.getKey().split("\\.")[1]);
            for (Map.Entry<String, ColumnStatistic> columnStatisticEntry : entry.getValue().entrySet()) {
                GlobalStateMgr.getCurrentStatisticStorage()
                        .addColumnStatistic(replayTable, columnStatisticEntry.getKey(),
                                columnStatisticEntry.getValue());
            }
        }
        return replaySql;
    }

    private static void tearMockEnv() {
        int backendId = 10002;
        int backendIdSize = GlobalStateMgr.getCurrentSystemInfo().getAliveBackendNumber();
        for (int i = 1; i < backendIdSize; ++i) {
            try {
                UtFrameUtils.dropMockBackend(backendId++);
            } catch (DdlException e) {
                e.printStackTrace();
            }
        }
    }

    private static Pair<String, ExecPlan> getQueryExecPlan(QueryStatement statement, ConnectContext connectContext) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();

        LogicalPlan logicalPlan;
        try (PlannerProfile.ScopedTimer t = PlannerProfile.getScopedTimer("Transformer")) {
            logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                    .transform((statement).getQueryRelation());

        }

        OptExpression optimizedPlan;
        try (PlannerProfile.ScopedTimer t = PlannerProfile.getScopedTimer("Optimizer")) {
            Optimizer optimizer = new Optimizer();
            optimizedPlan = optimizer.optimize(
                    connectContext,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);
        }

        ExecPlan execPlan;
        try (PlannerProfile.ScopedTimer t = PlannerProfile.getScopedTimer("Builder")) {
            execPlan = PlanFragmentBuilder
                    .createPhysicalPlan(optimizedPlan, connectContext,
                            logicalPlan.getOutputColumn(), columnRefFactory, new ArrayList<>(),
                            TResultSinkType.MYSQL_PROTOCAL, true);
        }

        return new Pair<>(LogicalPlanPrinter.print(optimizedPlan), execPlan);
    }

    private static Pair<String, ExecPlan> getInsertExecPlan(InsertStmt statement, ConnectContext connectContext) {
        PlannerProfile.ScopedTimer t = PlannerProfile.getScopedTimer("Planner");
        ExecPlan execPlan = new InsertPlanner().plan(statement, connectContext);
        t.close();
        return new Pair<>(LogicalPlanPrinter.print(execPlan.getPhysicalPlan()), execPlan);
    }

    public static Pair<String, ExecPlan> getNewPlanAndFragmentFromDump(ConnectContext connectContext,
                                                                       QueryDumpInfo replayDumpInfo) throws Exception {
        String replaySql = initMockEnv(connectContext, replayDumpInfo);
        replaySql = LogUtil.removeCommentAndLineSeparator(replaySql);
        Map<String, Database> dbs = null;
        try {
            StatementBase statementBase;
            try (PlannerProfile.ScopedTimer st = PlannerProfile.getScopedTimer("Parse")) {
                statementBase = com.starrocks.sql.parser.SqlParser.parse(replaySql,
                        connectContext.getSessionVariable()).get(0);
                if (statementBase instanceof QueryStatement) {
                    replaceTableCatalogName(statementBase);
                }
            }

            try (PlannerProfile.ScopedTimer st1 = PlannerProfile.getScopedTimer("Analyze")) {
                com.starrocks.sql.analyzer.Analyzer.analyze(statementBase, connectContext);
            }

            dbs = AnalyzerUtils.collectAllDatabase(connectContext, statementBase);
            lock(dbs);

            if (statementBase instanceof QueryStatement) {
                return getQueryExecPlan((QueryStatement) statementBase, connectContext);
            } else if (statementBase instanceof InsertStmt) {
                return getInsertExecPlan((InsertStmt) statementBase, connectContext);
            } else {
                Preconditions.checkState(false, "Do not support the statement");
                return null;
            }
        } finally {
            unLock(dbs);
            tearMockEnv();
        }
    }

    private static void replaceTableCatalogName(StatementBase statementBase) {
        List<TableRelation> tableRelations = AnalyzerUtils.collectTableRelations(statementBase);
        for (TableRelation tableRelation : tableRelations) {
            if (tableRelation.getName().getCatalog() != null) {
                String catalogName = tableRelation.getName().getCatalog();
                tableRelation.getName().setCatalog(
                        CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(catalogName, "hive"));
            }
        }
    }

    private static String getThriftString(List<PlanFragment> fragments) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < fragments.size(); ++i) {
            if (i > 0) {
                // a blank line between plan fragments
                str.append("\n");
            }
            str.append(fragments.get(i).toThrift());
        }
        return str.toString();
    }

    public static String getFragmentPlan(ConnectContext connectContext, String sql) throws Exception {
        return getPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.NORMAL);
    }

    public static String getVerboseFragmentPlan(ConnectContext connectContext, String sql) throws Exception {
        return getPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.VERBOSE);
    }

    public static String getPlanThriftString(ConnectContext ctx, String queryStr) throws Exception {
        return UtFrameUtils.getThriftString(UtFrameUtils.getPlanAndFragment(ctx, queryStr).second.getFragments());
    }

    // Lock all database before analyze
    private static void lock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readLock();
        }
    }

    // unLock all database after analyze
    private static void unLock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readUnlock();
        }
    }

    public static void setUpForPersistTest() {
        PseudoJournalReplayer.setUp();
        PseudoImage.setUpImageVersion();
    }

    public static void tearDownForPersisTest() {
        PseudoJournalReplayer.tearDown();
    }

    /**
     * pseudo image is used to test if image is wrote correctly.
     */
    public static class PseudoImage {
        private static AtomicBoolean isSetup = new AtomicBoolean(false);
        private DataOutputBuffer buffer;
        private static final int OUTPUT_BUFFER_INIT_SIZE = 128;

        public static void setUpImageVersion() {
            MetaContext metaContext = new MetaContext();
            metaContext.setStarRocksMetaVersion(StarRocksFEMetaVersion.VERSION_CURRENT);
            metaContext.setThreadLocalInfo();
            isSetup.set(true);
        }

        public PseudoImage() throws IOException {
            assert (isSetup.get());
            buffer = new DataOutputBuffer(OUTPUT_BUFFER_INIT_SIZE);
        }

        public DataOutputStream getDataOutputStream() {
            return buffer;
        }

        /**
         * this can be called multiple times to restore from a snapshot
         */
        public DataInputStream getDataInputStream() throws IOException {
            return new DataInputStream(new ByteArrayInputStream(buffer.getData(), 0, buffer.getLength()));
        }
    }

    /**
     * pseudo journal replayer is used to test if replayed correctly
     */
    public static class PseudoJournalReplayer {
        // master journal queue
        private static BlockingQueue<JournalTask> masterJournalQueue =
                new ArrayBlockingQueue<>(Config.metadata_journal_queue_size);
        // follower journal queue
        private static BlockingQueue<JournalTask> followerJournalQueue =
                new ArrayBlockingQueue<>(Config.metadata_journal_queue_size);
        // constantly move master journal to follower and mark succeed
        private static Thread fakeJournalWriter = null;

        protected static synchronized void setUp() {
            assert (fakeJournalWriter == null);
            GlobalStateMgr.getCurrentState().setEditLog(new EditLog(masterJournalQueue));

            // simulate the process of master journal synchronizing to the follower
            fakeJournalWriter = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            JournalTask journalTask = masterJournalQueue.take();
                            followerJournalQueue.put(journalTask);
                            journalTask.markSucceed();
                        } catch (InterruptedException e) {
                            System.err.println("fakeJournalWriter got an InterruptedException and exit!");
                            break;
                        }
                    }
                }
            });
            fakeJournalWriter.start();
        }

        /**
         * if you want to replay just a few log, please call this before master operate
         */
        public static synchronized void resetFollowerJournalQueue() throws InterruptedException {
            assert (followerJournalQueue != null);
            followerJournalQueue.clear();
        }

        public static synchronized Writable replayNextJournal(short expectCode) throws Exception {
            assert (followerJournalQueue != null);
            while (true) {
                if (followerJournalQueue.isEmpty()) {
                    throw new Exception("cannot replay next journal because queue is empty!");
                }
                DataOutputBuffer buffer = followerJournalQueue.take().getBuffer();
                DataInputStream dis =
                        new DataInputStream(new ByteArrayInputStream(buffer.getData(), 0, buffer.getLength()));
                try {
                    JournalEntity je = new JournalEntity();
                    je.readFields(dis);
                    if (je.getOpCode() == expectCode) {
                        return je.getData();
                    } else {
                        System.err.println("ignore irrelevant journal id " + je.getOpCode());
                    }
                } finally {
                    dis.close();
                }
            }
        }

        protected static synchronized void tearDown() {
            if (fakeJournalWriter != null) {
                try {
                    fakeJournalWriter.interrupt();
                    fakeJournalWriter.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                fakeJournalWriter = null;
            }
        }
    }

    public static ConnectContext initCtxForNewPrivilege(UserIdentity userIdentity) throws Exception {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setQualifiedUser(userIdentity.getQualifiedUser());
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.initAuth(true);
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setThreadLocalInfo();
        ctx.setDumpInfo(new MockDumpInfo());
        return ctx;
    }

}
