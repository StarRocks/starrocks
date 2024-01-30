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
import com.staros.starlet.StarletAgentFactory;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Writable;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.hive.ReplayMetadataMgr;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.meta.MetaContext;
import com.starrocks.persist.EditLog;
import com.starrocks.planner.PlanFragment;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.Explain;
import com.starrocks.sql.InsertPlanner;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DmlStmt;
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
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.Backend;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.Warehouse;
import mockit.Mock;
import mockit.MockUp;
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

    private static void startFEServer(String runningDir, boolean startBDB, RunMode runMode) throws Exception {
        // get STARROCKS_HOME
        String starRocksHome = System.getenv("STARROCKS_HOME");
        if (Strings.isNullOrEmpty(starRocksHome)) {
            starRocksHome = Files.createTempDirectory("STARROCKS_HOME").toAbsolutePath().toString();
        }

        Config.plugin_dir = starRocksHome + "/plugins";
        Config.max_create_table_timeout_second = 180;
        // start fe in "STARROCKS_HOME/fe/mocked/"
        MockedFrontend frontend = MockedFrontend.getInstance();
        Map<String, String> feConfMap = Maps.newHashMap();
        // TODO: support startBDB = false in shared-data mode
        if (!startBDB && runMode == RunMode.SHARED_DATA) {
            throw new NotImplementedException("Have to set 'startBDB = true' when creating a shared-data cluster");
        }
        // set additional fe config
        if (startBDB) {
            feConfMap.put("edit_log_port", String.valueOf(findValidPort()));
        }

        feConfMap.put("run_mode", runMode.getName());
        if (runMode == RunMode.SHARED_DATA) {
            feConfMap.put("cloud_native_meta_port", String.valueOf(findValidPort()));
            feConfMap.put("enable_load_volume_from_conf", "true");
            feConfMap.put("cloud_native_storage_type", "S3");
            feConfMap.put("aws_s3_path", "dummy_unittest_bucket/dummy_sub_path");
            feConfMap.put("aws_s3_region", "dummy_region");
            feConfMap.put("aws_s3_endpoint", "http://localhost:55555");
            feConfMap.put("aws_s3_access_key", "dummy_access_key");
            feConfMap.put("aws_s3_secret_key", "dummy_secret_key");
            // turn on mock starletAgent inside StarOS
            StarletAgentFactory.forTest = true;
        }
        feConfMap.put("tablet_create_timeout_second", "10");
        frontend.init(starRocksHome + "/" + runningDir, feConfMap);
        frontend.start(startBDB, runMode, new String[0]);
    }

    public static synchronized void createMinStarRocksCluster(boolean startBDB, RunMode runMode) {
        // to avoid call createMinStarRocksCluster multiple times
        if (CREATED_MIN_CLUSTER.get()) {
            return;
        }
        try {
            ClientPool.beHeartbeatPool = new MockGenericPool.HeatBeatPool("heartbeat");
            ClientPool.backendPool = new MockGenericPool.BackendThriftPool("backend");

            startFEServer("fe/mocked/test/" + UUID.randomUUID().toString() + "/", startBDB, runMode);

            addMockBackend(10001);

            // sleep to wait first heartbeat
            int retry = 0;
            while (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(10001).getBePort() == -1 &&
                    retry++ < 600) {
                Thread.sleep(100);
            }
            CREATED_MIN_CLUSTER.set(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createMinStarRocksCluster() {
        createMinStarRocksCluster(false, RunMode.SHARED_NOTHING);
    }

    // create a min starrocks cluster with the given runMode
    public static void createMinStarRocksCluster(RunMode runMode) {
        // TODO: support creating shared-data cluster without the real BDBJE journal
        boolean startBdb = (runMode == RunMode.SHARED_DATA);
        createMinStarRocksCluster(startBdb, runMode);
    }

    public static Backend addMockBackend(int backendId, String host, int beThriftPort) throws Exception {
        // start be
        MockedBackend backend = new MockedBackend(host, beThriftPort);
        // add be
        return addMockBackend(backend, backendId);
    }

    public static Backend addMockBackend(int backendId) throws Exception {
        // start be
        MockedBackend backend = new MockedBackend("127.0.0.1");
        // add be
        return addMockBackend(backend, backendId);
    }

    private static Backend addMockBackend(MockedBackend backend, int backendId) {
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
        be.setStarletPort(backend.getStarletPort());
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(be);
        if (RunMode.isSharedDataMode()) {
            int starletPort = backend.getStarletPort();
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getDefaultWarehouse();
            long workerGroupId = warehouse.getAnyAvailableCluster().getWorkerGroupId();
            String workerAddress = backend.getHost() + ":" + starletPort;
            GlobalStateMgr.getCurrentState().getStarOSAgent().addWorker(be.getId(), workerAddress, workerGroupId);
        }
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
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackend(backendId);
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

    private interface GetPlanHook<R> {
        R apply(ConnectContext context, StatementBase statementBase, ExecPlan execPlan) throws Exception;
    }

    private static <R> R buildPlan(ConnectContext connectContext, String originStmt,
                                   GetPlanHook<R> returnedSupplier) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        // connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        connectContext.setThreadLocalInfo();
        if (connectContext.getSessionVariable().getEnableQueryDump()) {
            connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
            connectContext.getDumpInfo().setOriginStmt(originStmt);
        }
        originStmt = LogUtil.removeLineSeparator(originStmt);

        List<StatementBase> statements;
        try (Timer ignored = Tracers.watchScope("Parser")) {
            statements = SqlParser.parse(originStmt, connectContext.getSessionVariable());
        }
        if (connectContext.getDumpInfo() != null) {
            connectContext.getDumpInfo().setOriginStmt(originStmt);
        }
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
            try (Timer ignored = Tracers.watchScope("Planner")) {
                execPlan = StatementPlanner.plan(statementBase, connectContext);
            }

            testView(connectContext, originStmt, statementBase);

            validatePlanConnectedness(execPlan);
            return returnedSupplier.apply(connectContext, statementBase, execPlan);
        } finally {
            // before returning we have to restore session variable.
            connectContext.setSessionVariable(oldSessionVariable);
        }
    }

    public static Pair<String, Pair<ExecPlan, String>> getFragmentPlanWithTrace(
            ConnectContext connectContext, String sql, String module) throws Exception {
        if (Strings.isNullOrEmpty(module)) {
            Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
            Pair<ExecPlan, String> planAndTrace = Pair.create(planPair.second, "");
            return Pair.create(planPair.first, planAndTrace);
        } else {
            Tracers.register(connectContext);
            Tracers.init(connectContext, Tracers.Mode.LOGS, module);
            try {

                Pair<String, ExecPlan> planPair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
                String pr = Tracers.printLogs();
                Pair<ExecPlan, String> planAndTrace = Pair.create(planPair.second, pr);
                return Pair.create(planPair.first, planAndTrace);
            } catch (Exception e) {
                String pr = Tracers.printLogs();
                if (!Strings.isNullOrEmpty(pr)) {
                    System.out.println(pr);
                }
                throw e;
            } finally {
                Tracers.close();
            }
        }
    }

    public static Pair<String, ExecPlan> getPlanAndFragment(ConnectContext connectContext, String originStmt)
            throws Exception {
        return buildPlan(connectContext, originStmt,
                (context, statementBase, execPlan) -> new Pair<>(printPhysicalPlan(execPlan.getPhysicalPlan()),
                        execPlan));
    }

    public static DefaultCoordinator startScheduling(ConnectContext connectContext, String originStmt) throws Exception {
        return buildPlan(connectContext, originStmt,
                (context, statementBase, execPlan) -> {
                    DefaultCoordinator scheduler = createScheduler(context, statementBase, execPlan);

                    scheduler.startScheduling();

                    return scheduler;
                });
    }

    public static Pair<String, DefaultCoordinator> getPlanAndStartScheduling(ConnectContext connectContext, String originStmt)
            throws Exception {
        return buildPlan(connectContext, originStmt,
                (context, statementBase, execPlan) -> {
                    DefaultCoordinator scheduler = createScheduler(context, statementBase, execPlan);

                    scheduler.startSchedulingWithoutDeploy();
                    String plan = scheduler.getSchedulerExplain();

                    return Pair.create(plan, scheduler);
                });
    }

    public static DefaultCoordinator getScheduler(ConnectContext connectContext, String originStmt) throws Exception {
        return buildPlan(connectContext, originStmt, UtFrameUtils::createScheduler);
    }

    private static DefaultCoordinator createScheduler(ConnectContext context, StatementBase statementBase, ExecPlan execPlan) {
        context.setExecutionId(new TUniqueId(1, 2));
        DefaultCoordinator scheduler;
        if (statementBase instanceof DmlStmt) {
            if (statementBase instanceof InsertStmt) {
                scheduler = new DefaultCoordinator.Factory().createInsertScheduler(context,
                        execPlan.getFragments(), execPlan.getScanNodes(),
                        execPlan.getDescTbl().toThrift());
            } else {
                throw new RuntimeException("can only handle insert DML");
            }
        } else {
            scheduler = new DefaultCoordinator.Factory().createQueryScheduler(context,
                    execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift());
        }

        return scheduler;
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
            try (Timer ignored = Tracers.watchScope("Test View")) {
                createTableStmt = (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(createView, connectContext);
                try {
                    StatementBase viewStatement =
                            SqlParser.parse(createTableStmt.getInlineViewDef(),
                                    connectContext.getSessionVariable().getSqlMode()).get(0);
                    com.starrocks.sql.analyzer.Analyzer.analyze(viewStatement, connectContext);
                } catch (Exception e) {
                    System.out.println("invalid view def: " + createTableStmt.getInlineViewDef()
                            + "\nError msg:"  + e.getMessage());
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

    public static String explainLogicalPlan(ConnectContext ctx, String sql) throws Exception {
        Pair<String, ExecPlan> planAndExplain = getPlanAndFragment(ctx, sql);
        return printPlan(planAndExplain.second);
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
        int backendIdSize = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber();
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
                GlobalStateMgr.getCurrentState().getStatisticStorage()
                        .addColumnStatistic(replayTable, columnStatisticEntry.getKey(),
                                columnStatisticEntry.getValue());
            }
        }
        return replaySql;
    }

    private static void tearMockEnv() {
        int backendId = 10002;
        int backendIdSize = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber();
        for (int i = 1; i < backendIdSize; ++i) {
            try {
                UtFrameUtils.dropMockBackend(backendId++);
            } catch (DdlException e) {
                e.printStackTrace();
            }
        }
    }

    private static Pair<String, ExecPlan> getQueryExecPlan(QueryStatement statement, ConnectContext connectContext)
            throws Exception {
        Map<String, String> optHints = null;
        SessionVariable sessionVariableBackup = connectContext.getSessionVariable();
        if (statement.getQueryRelation() instanceof SelectRelation) {
            SelectRelation selectRelation = (SelectRelation) statement.getQueryRelation();
            optHints = selectRelation.getSelectList().getOptHints();
        }

        if (optHints != null) {
            SessionVariable sessionVariable = (SessionVariable) sessionVariableBackup.clone();
            for (String key : optHints.keySet()) {
                VariableMgr.setSystemVariable(sessionVariable,
                        new SystemVariable(key, new StringLiteral(optHints.get(key))), true);
            }
            connectContext.setSessionVariable(sessionVariable);
        }

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();

        LogicalPlan logicalPlan;
        try (Timer t = Tracers.watchScope("Transformer")) {
            logicalPlan = new RelationTransformer(columnRefFactory, connectContext)
                    .transform((statement).getQueryRelation());

        }

        OptExpression optimizedPlan;
        try (Timer t = Tracers.watchScope("Optimizer")) {
            Optimizer optimizer = new Optimizer();
            optimizedPlan = optimizer.optimize(
                    connectContext,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);
        }

        ExecPlan execPlan;
        try (Timer t = Tracers.watchScope("Builder")) {
            execPlan = PlanFragmentBuilder
                    .createPhysicalPlan(optimizedPlan, connectContext,
                            logicalPlan.getOutputColumn(), columnRefFactory, new ArrayList<>(),
                            TResultSinkType.MYSQL_PROTOCAL, true);
        }

        return new Pair<>(LogicalPlanPrinter.print(optimizedPlan), execPlan);
    }

    private static Pair<String, ExecPlan> getInsertExecPlan(InsertStmt statement, ConnectContext connectContext) {
        Timer t = Tracers.watchScope("Planner");
        ExecPlan execPlan = new InsertPlanner().plan(statement, connectContext);
        t.close();
        return new Pair<>(LogicalPlanPrinter.print(execPlan.getPhysicalPlan()), execPlan);
    }

    public static String setUpTestDump(ConnectContext connectContext, QueryDumpInfo replayDumpInfo) throws Exception {
        String replaySql = initMockEnv(connectContext, replayDumpInfo);
        replaySql = LogUtil.removeLineSeparator(replaySql);
        return replaySql;
    }

    public static Pair<String, ExecPlan> replaySql(ConnectContext connectContext, String sql) throws Exception {
        StatementBase statementBase;
        try (Timer st = Tracers.watchScope("Parse")) {
            statementBase = com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            if (statementBase instanceof QueryStatement) {
                replaceTableCatalogName(statementBase);
            }
        }

        com.starrocks.sql.analyzer.Analyzer.analyze(statementBase, connectContext);

        if (statementBase instanceof QueryStatement) {
            return getQueryExecPlan((QueryStatement) statementBase, connectContext);
        } else if (statementBase instanceof InsertStmt) {
            return getInsertExecPlan((InsertStmt) statementBase, connectContext);
        } else {
            Preconditions.checkState(false, "Do not support the statement");
            return null;
        }
    }

    public static void tearDownTestDump() {
        tearMockEnv();
    }

    public static Pair<String, ExecPlan> getNewPlanAndFragmentFromDump(ConnectContext connectContext,
                                                                       QueryDumpInfo replayDumpInfo) throws Exception {
        String replaySql = initMockEnv(connectContext, replayDumpInfo);
        replaySql = LogUtil.removeLineSeparator(replaySql);
        Map<String, Database> dbs = null;
        try {
            StatementBase statementBase;
            try (Timer st = Tracers.watchScope("Parse")) {
                statementBase = com.starrocks.sql.parser.SqlParser.parse(replaySql,
                        connectContext.getSessionVariable()).get(0);
                if (statementBase instanceof QueryStatement) {
                    replaceTableCatalogName(statementBase);
                }
            }

            try (Timer st1 = Tracers.watchScope("Analyze")) {
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
            GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);

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

        public static synchronized void replayJournalToEnd() throws InterruptedException, IOException {
            int count = followerJournalQueue.size();
            while (!followerJournalQueue.isEmpty()) {
                DataOutputBuffer buffer = followerJournalQueue.take().getBuffer();
                JournalEntity je = new JournalEntity();
                try (DataInputStream dis = new DataInputStream(
                        new ByteArrayInputStream(buffer.getData(), 0, buffer.getLength()))) {
                    je.readFields(dis);
                    EditLog.loadJournal(GlobalStateMgr.getCurrentState(), je);
                    // System.out.println("replayed journal type: " + je.getOpCode());
                } catch (JournalInconsistentException e) {
                    System.err.println("load journal failed, type: " + je.getOpCode() + " , error: " + e.getMessage());
                    e.printStackTrace();
                    Assert.fail();
                }
            }
            System.out.println("replayed " + count + " journal(s) from begin to end");
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
        ctx.setQualifiedUser(userIdentity.getUser());
        ctx.setQueryId(UUIDUtil.genUUID());
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.initAuth(true);
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setThreadLocalInfo();
        ctx.setDumpInfo(new MockDumpInfo());
        return ctx;
    }

    public static boolean matchPlanWithoutId(String expect, String actual) {
        String trimedExpect = expect.replaceAll("\\d+:\\s*", "")
                .replaceAll("\\[\\d+,", "[")
                .replaceAll("<slot \\d+>", "<slot>");
        String trimedActual = actual.replaceAll("\\d+:\\s*", "")
                .replaceAll("\\[\\d+,", "[")
                .replaceAll("<slot \\d+>", "<slot>");
        boolean ret = trimedActual.contains(trimedExpect);
        if (!ret) {
            System.out.println("trimedExpect:");
            System.out.println(trimedExpect);
            System.out.println("trimedActual:");
            System.out.println(trimedActual);
        }
        return ret;
    }

    public static void setDefaultConfigForAsyncMVTest(ConnectContext connectContext) {
        Config.dynamic_partition_check_interval_seconds = 10;
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;

        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        Config.alter_scheduler_interval_millisecond = 100;

        // Use sync analyze
        Config.mv_auto_analyze_async = false;

        // Default REFRESH DEFERRED
        Config.default_mv_refresh_immediate = false;
        // default replication num: 1
        Config.default_replication_num = 1;

        // build a small cache for test
        Config.mv_plan_cache_max_size = 10;

        FeConstants.enablePruneEmptyOutputScan = false;
        FeConstants.runningUnitTest = true;

        if (connectContext != null) {
            // 300s: 5min
            connectContext.getSessionVariable().setOptimizerExecuteTimeout(300 * 1000);
            // 300s: 5min
            connectContext.getSessionVariable().setOptimizerMaterializedViewTimeLimitMillis(300 * 1000);
            // 300s: 5min
            connectContext.getSessionVariable().setOptimizerMaterializedViewTimeLimitMillis(300 * 1000);

            connectContext.getSessionVariable().setEnableShortCircuit(false);
            connectContext.getSessionVariable().setEnableQueryCache(false);
            connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);
            connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
            connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        }

        new MockUp<PlanTestBase>() {
            /**
             * {@link com.starrocks.sql.plan.PlanTestNoneDBBase#isIgnoreExplicitColRefIds()}
             */
            @Mock
            boolean isIgnoreExplicitColRefIds() {
                return true;
            }
        };

    }
}
