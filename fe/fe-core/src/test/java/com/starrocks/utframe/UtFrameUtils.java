// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.SelectStmt;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.SqlParser;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.Planner;
import com.starrocks.planner.PlannerContext;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.VariableMgr;
import com.starrocks.sql.analyzer.relation.Relation;
import com.starrocks.sql.optimizer.OperatorStrings;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.statistic.Constants;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.MockedFrontend.EnvVarNotSetException;
import com.starrocks.utframe.MockedFrontend.FeStartException;
import com.starrocks.utframe.MockedFrontend.NotInitException;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.ServerSocket;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.plan.PlanTestBase.setPartitionStatistics;

public class UtFrameUtils {
    private static final Logger LOG = LogManager.getLogger(UtFrameUtils.class);

    public static final String createStatisticsTableStmt = "CREATE TABLE `table_statistic_v1` (\n" +
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
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"DEFAULT\"\n" +
            ");";

    // Help to create a mocked ConnectContext.
    public static ConnectContext createDefaultCtx() throws IOException {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQualifiedUser(Auth.ROOT_USER);
        ctx.setCatalog(Catalog.getCurrentCatalog());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    // Help to create a mocked test ConnectContext.
    public static ConnectContext createTestUserCtx(UserIdentity testUser) throws IOException {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setCurrentUserIdentity(testUser);
        ctx.setQualifiedUser(testUser.getQualifiedUser());
        ctx.setCatalog(Catalog.getCurrentCatalog());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    // Parse an origin stmt . Return a StatementBase instance.
    public static StatementBase parseStmtWithNewAnalyzer(String originStmt, ConnectContext ctx)
            throws Exception {
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        com.starrocks.sql.analyzer.Analyzer analyzer =
                new com.starrocks.sql.analyzer.Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = null;
        try {
            statementBase = SqlParserUtils.getFirstStmt(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            System.err.println("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        }
        Relation relation = analyzer.analyze(statementBase);
        return statementBase;
    }

    // Parse an origin stmt and analyze it. Return a StatementBase instance.
    public static StatementBase parseAndAnalyzeStmt(String originStmt, ConnectContext ctx)
            throws Exception {
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = null;
        try {
            statementBase = SqlParserUtils.getFirstStmt(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            System.err.println("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        }
        statementBase.analyze(analyzer);
        return statementBase;
    }

    // for analyzing multi statements
    public static List<StatementBase> parseAndAnalyzeStmts(String originStmt, ConnectContext ctx) throws Exception {
        System.out.println("begin to parse stmts: " + originStmt);
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        List<StatementBase> statementBases = null;
        try {
            statementBases = SqlParserUtils.getMultiStmts(parser);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            System.err.println("parse failed: " + errorMessage);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        }
        for (StatementBase stmt : statementBases) {
            stmt.analyze(analyzer);
        }
        return statementBases;
    }

    public static void startFEServer(String runningDir) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        // get STARROCKS_HOME
        String starRocksHome = System.getenv("STARROCKS_HOME");
        if (Strings.isNullOrEmpty(starRocksHome)) {
            starRocksHome = Files.createTempDirectory("STARROCKS_HOME").toAbsolutePath().toString();
        }
        Config.plugin_dir = starRocksHome + "/plugins";

        // fe only need edit_log_port
        int fe_edit_log_port = findValidPort();

        // start fe in "STARROCKS_HOME/fe/mocked/"
        MockedFrontend frontend = MockedFrontend.getInstance();
        Map<String, String> feConfMap = Maps.newHashMap();
        // set additional fe config
        feConfMap.put("edit_log_port", String.valueOf(fe_edit_log_port));
        feConfMap.put("tablet_create_timeout_second", "10");
        frontend.init(starRocksHome + "/" + runningDir, feConfMap);
        frontend.start(new String[0]);
    }

    public static void createMinStarRocksCluster(String runningDir) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        startFEServer(runningDir);
        addMockBackend(10001);

        // sleep to wait first heartbeat
        int retry = 0;
        while (Catalog.getCurrentSystemInfo().getBackend(10001).getBePort() == -1 &&
                retry++ < 600) {
            Thread.sleep(1000);
        }
    }

    public static void addMockBackend(int backendId) throws IOException {
        int fe_rpc_port = MockedFrontend.getInstance().getRpcPort();
        // start be
        MockedBackend backend = null;
        for (int retry = 1; retry <= 5; retry++) {
            int be_heartbeat_port = findValidPort();
            int be_thrift_port = findValidPort();
            int be_brpc_port = findValidPort();
            int be_http_port = findValidPort();

            backend = MockedBackendFactory.createBackend("127.0.0.1",
                    be_heartbeat_port, be_thrift_port, be_brpc_port, be_http_port,
                    new MockedBackendFactory.DefaultHeartbeatServiceImpl(be_thrift_port, be_http_port, be_brpc_port),
                    new MockedBackendFactory.DefaultBeThriftServiceImpl(),
                    new MockedBackendFactory.DefaultPBackendServiceImpl());
            backend.setFeAddress(new TNetworkAddress("127.0.0.1", fe_rpc_port));
            try {
                backend.start();
                break;
            } catch (IOException ex) {
                System.out.println("start be fail, message : " + ex.getMessage());
                if (retry == 5) {
                    throw ex;
                }
            }
        }

        // add be
        Backend be = new Backend(backendId, backend.getHost(), backend.getHeartbeatPort());
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo(backendId + "/path1");
        diskInfo1.setTotalCapacityB(1000000);
        diskInfo1.setAvailableCapacityB(500000);
        diskInfo1.setDataUsedCapacityB(480000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);
        be.setDisks(ImmutableMap.copyOf(disks));
        be.setAlive(true);
        be.setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);
        Catalog.getCurrentSystemInfo().addBackend(be);
    }

    public static void dropMockBackend(int backendId) throws DdlException {
        Catalog.getCurrentSystemInfo().dropBackend(backendId);
    }

    public static void cleanStarRocksFeDir(String baseDir) {
        try {
            FileUtils.deleteDirectory(new File(baseDir));
        } catch (IOException e) {
        }
    }

    public static int findValidPort() {
        String starRocksHome = System.getenv("STARROCKS_HOME");
        File portDir = new File(starRocksHome + "/fe/ut_ports");
        if (!portDir.exists()){
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

    public static String getSQLPlanOrErrorMsg(ConnectContext ctx, String queryStr) throws Exception {
        ctx.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        if (ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            Planner planner = stmtExecutor.planner();
            return planner.getExplainString(planner.getFragments(), TExplainLevel.NORMAL);
        } else {
            return ctx.getState().getErrorMessage();
        }
    }

    public static String getPlanThriftStringForNewPlanner(ConnectContext ctx, String queryStr) throws Exception {
        return UtFrameUtils.getThriftString(UtFrameUtils.getNewPlanAndFragment(ctx, queryStr).second.getFragments());
    }

    public static Pair<String, ExecPlan> getNewPlanAndFragment(ConnectContext connectContext, String originStmt)
            throws Exception {
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext.getSessionVariable()));
        SqlScanner input =
                new SqlScanner(new StringReader(originStmt), connectContext.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);
        connectContext.getDumpInfo().setOriginStmt(originStmt);

        SessionVariable oldSessionVariable = connectContext.getSessionVariable();
        try {
            // update session variable by adding optional hints.
            if (statementBase instanceof SelectStmt) {
                Map<String, String> optHints = ((SelectStmt) statementBase).getSelectList().getOptHints();
                if (optHints != null) {
                    SessionVariable sessionVariable = (SessionVariable) oldSessionVariable.clone();
                    for (String key : optHints.keySet()) {
                        VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(optHints.get(key))));
                    }
                    connectContext.setSessionVariable(sessionVariable);
                }
            }

            com.starrocks.sql.analyzer.Analyzer analyzer =
                    new com.starrocks.sql.analyzer.Analyzer(Catalog.getCurrentCatalog(), connectContext);
            Relation relation = analyzer.analyze(statementBase);

            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory).transform(relation);

            Optimizer optimizer = new Optimizer();
            OptExpression optimizedPlan = optimizer.optimize(
                    connectContext,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);

            PlannerContext plannerContext =
                    new PlannerContext(null, null, connectContext.getSessionVariable().toThrift(), null);
            ExecPlan execPlan = new PlanFragmentBuilder()
                    .createPhysicalPlan(optimizedPlan, plannerContext, connectContext,
                            logicalPlan.getOutputColumn(), columnRefFactory, new ArrayList<>());
            execPlan.setPlanCount(optimizedPlan.getPlanCount());

            OperatorStrings operatorPrinter = new OperatorStrings();
            return new Pair<>(operatorPrinter.printOperator(optimizedPlan), execPlan);
        } finally {
            // before returing we have to restore session varibale.
            connectContext.setSessionVariable(oldSessionVariable);
        }
    }

    public static Pair<String, ExecPlan> getNewPlanAndFragmentFromDump(ConnectContext connectContext,
                                                                       QueryDumpInfo replayDumpInfo) throws Exception {
        // mock statistics table
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        if (!starRocksAssert.databaseExist("_statistics_")) {
            starRocksAssert.withDatabaseWithoutAnalyze(Constants.StatisticsDBName).useDatabase(Constants.StatisticsDBName);
            starRocksAssert.withTable(createStatisticsTableStmt);
        }
        // prepare dump mock environment
        // statement
        String replaySql = replayDumpInfo.getOriginStmt();
        // session variable
        connectContext.setSessionVariable(replayDumpInfo.getSessionVariable());
        // create table
        int backendId = 10002;
        int backendIdSize = Catalog.getCurrentSystemInfo().getBackendIds(true).size();
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
            starRocksAssert.withTable(entry.getValue());
        }
        // mock be num
        backendId = 10002;
        for (int i = 1; i < replayDumpInfo.getBeNum(); ++i) {
            UtFrameUtils.addMockBackend(backendId++);
        }
        // mock table row count
        for (Map.Entry<String, Map<String, Long>> entry : replayDumpInfo.getPartitionRowCountMap().entrySet()) {
            String dbName = entry.getKey().split("\\.")[0];
            OlapTable replayTable = (OlapTable) connectContext.getCatalog().getDb("default_cluster:" + dbName)
                    .getTable(entry.getKey().split("\\.")[1]);
            for (Map.Entry<String, Long> partitionEntry : entry.getValue().entrySet()) {
                setPartitionStatistics(replayTable, partitionEntry.getKey(), partitionEntry.getValue());
            }
        }
        // mock table column statistics
        for (Map.Entry<String, Map<String, ColumnStatistic>> entry : replayDumpInfo.getTableStatisticsMap()
                .entrySet()) {
            String dbName = entry.getKey().split("\\.")[0];
            OlapTable replayTable = (OlapTable) connectContext.getCatalog().getDb("default_cluster:" + dbName)
                    .getTable(entry.getKey().split("\\.")[1]);
            for (Map.Entry<String, ColumnStatistic> columnStatisticEntry : entry.getValue().entrySet()) {
                Catalog.getCurrentStatisticStorage().addColumnStatistic(replayTable, columnStatisticEntry.getKey(),
                        columnStatisticEntry.getValue());
            }
        }

        SqlScanner input =
                new SqlScanner(new StringReader(replaySql), replayDumpInfo.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

        com.starrocks.sql.analyzer.Analyzer analyzer =
                new com.starrocks.sql.analyzer.Analyzer(Catalog.getCurrentCatalog(), connectContext);
        Relation relation = analyzer.analyze(statementBase);

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory).transform(relation);

        Optimizer optimizer = new Optimizer();
        OptExpression optimizedPlan = optimizer.optimize(
                connectContext,
                logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()),
                columnRefFactory);

        PlannerContext plannerContext =
                new PlannerContext(null, null, connectContext.getSessionVariable().toThrift(), null);
        ExecPlan execPlan = new PlanFragmentBuilder()
                .createPhysicalPlan(optimizedPlan, plannerContext, connectContext,
                        logicalPlan.getOutputColumn(), columnRefFactory, new ArrayList<>());

        OperatorStrings operatorPrinter = new OperatorStrings();
        return new Pair<>(operatorPrinter.printOperator(optimizedPlan), execPlan);
    }

    public static String getThriftString(List<PlanFragment> fragments) {
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

    public static String getNewFragmentPlan(ConnectContext connectContext, String sql) throws Exception {
        return getNewPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.NORMAL);
    }
}
