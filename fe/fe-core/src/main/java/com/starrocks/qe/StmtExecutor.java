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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/StmtExecutor.java

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

package com.starrocks.qe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;
import com.starrocks.alter.AlterJobException;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.Parameter;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.SetVarHint;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UserVariableHint;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.Pair;
import com.starrocks.common.QueryDumpLog;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.TimeoutException;
import com.starrocks.common.Version;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.ProfilingExecPlan;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.RuntimeProfileParser;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.HttpResultSender;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.ExportJob;
import com.starrocks.load.InsertOverwriteJob;
import com.starrocks.load.InsertOverwriteJobMgr;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.load.loadv2.LoadErrorUtils;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlEofPacket;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.persist.CreateInsertOverwriteJobLog;
import com.starrocks.persist.DeleteSqlBlackLists;
import com.starrocks.persist.SqlBlackListPersistInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.FileScanNode;
import com.starrocks.planner.HiveTableSink;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.QueryStatisticsItemPB;
import com.starrocks.qe.QueryState.MysqlStateType;
import com.starrocks.qe.feedback.OperatorTuningGuides;
import com.starrocks.qe.feedback.PlanAdvisorExecutor;
import com.starrocks.qe.feedback.PlanTuningAdvisor;
import com.starrocks.qe.feedback.analyzer.PlanTuningAnalyzer;
import com.starrocks.qe.feedback.skeleton.SkeletonBuilder;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.FeExecuteCoordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.GracefulExitFlag;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext;
import com.starrocks.sql.ExplainAnalyzer;
import com.starrocks.sql.PrepareStmtPlanner;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.AddBackendBlackListStmt;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AnalyzeProfileStmt;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AnalyzeTypeDesc;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTemporaryTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DeallocateStmt;
import com.starrocks.sql.ast.DelBackendBlackListStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropTemporaryTableStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UnsupportedStmt;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.feedback.PlanAdvisorStmt;
import com.starrocks.sql.ast.translate.TranslateStmt;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.common.AuditEncryptionChecker;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.cost.feature.CostPredictor;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.ExternalHistogramStatisticsCollectJob;
import com.starrocks.statistic.HistogramStatisticsCollectJob;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatisticsCollectJobFactory;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.RemoteTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionCommitFailedException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TransactionStmtExecutor;
import com.starrocks.transaction.VisibleStateWaiter;
import com.starrocks.warehouse.WarehouseIdleChecker;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.common.ErrorCode.ERR_NO_PARTITIONS_HAVE_DATA_LOAD;
import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

// Do one COM_QUERY process.
// first: Parse receive byte array to statement struct.
// second: Do handle function for statement.
public class StmtExecutor {
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);
    private static final Logger PROFILE_LOG = LogManager.getLogger("profile");
    private static final Gson GSON = new Gson();

    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);

    private final ConnectContext context;
    private final MysqlSerializer serializer;
    private final OriginStatement originStmt;
    private StatementBase parsedStmt;
    private RuntimeProfile profile;
    private Coordinator coord = null;
    private LeaderOpExecutor leaderOpExecutor = null;
    private RedirectStatus redirectStatus = null;
    /**
     * When isProxy is true, it proves that the current FE is acting as the
     * Leader and executing the statement forwarded by the Follower
     */
    private boolean isProxy;
    private List<ByteBuffer> proxyResultBuffer = null;
    private ShowResultSet proxyResultSet = null;
    private PQueryStatistics statisticsForAuditLog;
    private List<StmtExecutor> subStmtExecutors;
    private Optional<Boolean> isForwardToLeaderOpt = Optional.empty();
    private HttpResultSender httpResultSender;
    private PrepareStmtContext prepareStmtContext;
    private boolean isInternalStmt = false;

    public StmtExecutor(ConnectContext ctx, StatementBase parsedStmt) {
        this(ctx, parsedStmt, false);
    }

    public static StmtExecutor newInternalExecutor(ConnectContext ctx, StatementBase parsedStmt) {
        return new StmtExecutor(ctx, parsedStmt, true);
    }

    private StmtExecutor(ConnectContext ctx, StatementBase parsedStmt, boolean isInternalStmt) {
        this.context = ctx;
        this.parsedStmt = Preconditions.checkNotNull(parsedStmt);
        this.originStmt = parsedStmt.getOrigStmt();
        this.serializer = context.getSerializer();
        this.isProxy = false;
        this.isInternalStmt = isInternalStmt;
    }

    public void setProxy() {
        isProxy = true;
        proxyResultBuffer = new ArrayList<>();
    }

    public Coordinator getCoordinator() {
        return this.coord;
    }

    private RuntimeProfile buildTopLevelProfile() {
        RuntimeProfile profile = new RuntimeProfile("Query");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(context.getExecutionId()));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(context.getStartTime()));

        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - context.getStartTime();
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(currentTimestamp));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, context.getState().toProfileString());
        summaryProfile.addInfoString("StarRocks Version",
                String.format("%s-%s", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH));
        summaryProfile.addInfoString(ProfileManager.USER, context.getQualifiedUser());
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, context.getDatabase());
        if (AuditEncryptionChecker.needEncrypt(parsedStmt)) {
            summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT,
                    AstToSQLBuilder.toSQLOrDefault(parsedStmt, originStmt.originStmt));
        } else {
            summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, originStmt.originStmt);
        }

        // Add some import variables in profile
        SessionVariable variables = context.getSessionVariable();
        if (variables != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(SessionVariable.PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM).append("=")
                    .append(variables.getParallelExecInstanceNum()).append(",");
            sb.append(SessionVariable.MAX_PARALLEL_SCAN_INSTANCE_NUM).append("=")
                    .append(variables.getMaxParallelScanInstanceNum()).append(",");
            sb.append(SessionVariable.PIPELINE_DOP).append("=").append(variables.getPipelineDop()).append(",");
            sb.append(SessionVariable.ENABLE_ADAPTIVE_SINK_DOP).append("=")
                    .append(variables.getEnableAdaptiveSinkDop())
                    .append(",");
            sb.append(SessionVariable.ENABLE_RUNTIME_ADAPTIVE_DOP).append("=")
                    .append(variables.isEnableRuntimeAdaptiveDop())
                    .append(",");
            sb.append(SessionVariable.RUNTIME_PROFILE_REPORT_INTERVAL).append("=")
                    .append(variables.getRuntimeProfileReportInterval())
                    .append(",");
            if (context.getResourceGroup() != null) {
                sb.append(SessionVariable.RESOURCE_GROUP).append("=").append(context.getResourceGroup().getName())
                        .append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            summaryProfile.addInfoString(ProfileManager.VARIABLES, sb.toString());

            summaryProfile.addInfoString("NonDefaultSessionVariables", variables.getNonDefaultVariablesJson());
            String hitMvs = context.getAuditEventBuilder().getHitMvs();
            if (StringUtils.isNotEmpty(hitMvs)) {
                summaryProfile.addInfoString("HitMaterializedViews", hitMvs);
            }
        }

        profile.addChild(summaryProfile);

        RuntimeProfile plannerProfile = new RuntimeProfile("Planner");
        profile.addChild(plannerProfile);
        Tracers.toRuntimeProfile(plannerProfile);
        return profile;
    }

    /**
     * Whether to forward to leader from follower which should be the same in the StmtExecutor's lifecycle.
     */
    public boolean isForwardToLeader() {
        return getIsForwardToLeaderOrInit(true);
    }

    public boolean getIsForwardToLeaderOrInit(boolean isInitIfNoPresent) {
        if (!isForwardToLeaderOpt.isPresent()) {
            if (!isInitIfNoPresent) {
                return false;
            }
            isForwardToLeaderOpt = Optional.of(initForwardToLeaderState());
        }
        return isForwardToLeaderOpt.get();
    }

    private boolean initForwardToLeaderState() {
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            return false;
        }

        // If this node is transferring to the leader, we should wait for it to complete to avoid forwarding to its own node.
        if (GlobalStateMgr.getCurrentState().isInTransferringToLeader()) {
            long lastPrintTime = -1L;
            while (true) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }

                if (System.currentTimeMillis() - lastPrintTime > 1000L) {
                    lastPrintTime = System.currentTimeMillis();
                    LOG.info("waiting for current FE node transferring to LEADER state");
                }

                if (GlobalStateMgr.getCurrentState().isLeader()) {
                    return false;
                }
            }
        }

        // this is a query stmt, but this non-master FE can not read, forward it to master
        if (parsedStmt instanceof QueryStatement) {
            // When FollowerQueryForwardMode is not default, forward it to leader or follower by default.
            if (context != null && context.getSessionVariable() != null &&
                    context.getSessionVariable().isFollowerForwardToLeaderOpt().isPresent()) {
                return context.getSessionVariable().isFollowerForwardToLeaderOpt().get();
            }

            if (!GlobalStateMgr.getCurrentState().canRead()) {
                return true;
            }
        }

        if (redirectStatus == null) {
            return false;
        } else {
            return redirectStatus.isForwardToLeader();
        }
    }

    public LeaderOpExecutor getLeaderOpExecutor() {
        return leaderOpExecutor;
    }

    public ByteBuffer getOutputPacket() {
        if (leaderOpExecutor == null) {
            return null;
        } else {
            return leaderOpExecutor.getOutputPacket();
        }
    }

    public ShowResultSet getProxyResultSet() {
        return proxyResultSet;
    }

    public ShowResultSet getShowResultSet() {
        if (leaderOpExecutor == null) {
            return null;
        } else {
            return leaderOpExecutor.getProxyResultSet();
        }
    }

    public boolean sendResultToChannel(MysqlChannel channel) throws IOException {
        if (leaderOpExecutor == null) {
            return false;
        } else {
            return leaderOpExecutor.sendResultToChannel(channel);
        }
    }

    public StatementBase getParsedStmt() {
        return parsedStmt;
    }

    public int getExecTimeout() {
        return parsedStmt.getTimeout();
    }

    public String getExecType() {
        if (parsedStmt instanceof InsertStmt || parsedStmt instanceof CreateTableAsSelectStmt) {
            return "Insert";
        } else if (parsedStmt instanceof UpdateStmt) {
            return "Update";
        } else if (parsedStmt instanceof DeleteStmt) {
            return "Delete";
        } else {
            return "Query";
        }
    }

    public boolean isExecLoadType() {
        return parsedStmt instanceof DmlStmt || parsedStmt instanceof CreateTableAsSelectStmt;
    }

    // Execute one statement.
    // Exception:
    //  IOException: talk with client failed.
    public void execute() throws Exception {
        long beginTimeInNanoSecond = TimeUtils.getStartTime();
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());
        context.setIsForward(false);
        context.setIsLeaderTransferred(false);

        // set execution id.
        // Try to use query id as execution id when execute first time.
        UUID uuid = context.getQueryId();
        context.setExecutionId(UUIDUtil.toTUniqueId(uuid));
        SessionVariable sessionVariableBackup = context.getSessionVariable();

        // if use http protocol, use httpResultSender to send result to netty channel
        if (context instanceof HttpConnectContext) {
            httpResultSender = new HttpResultSender((HttpConnectContext) context);
        }

        final boolean shouldMarkIdleCheck = shouldMarkIdleCheck(parsedStmt);
        final long originWarehouseId = context.getCurrentWarehouseId();
        if (shouldMarkIdleCheck) {
            WarehouseIdleChecker.increaseRunningSQL(originWarehouseId);
        }
        try {
            context.getState().setIsQuery(parsedStmt instanceof QueryStatement);
            if (parsedStmt.isExistQueryScopeHint()) {
                processQueryScopeHint();
            }

            // set warehouse for auditLog
            context.getAuditEventBuilder().setWarehouse(context.getCurrentWarehouseName());
            LOG.debug("set warehouse {} for stmt: {}", context.getCurrentWarehouseName(), parsedStmt);

            if (parsedStmt.isExplain()) {
                context.setExplainLevel(parsedStmt.getExplainLevel());
            } else {
                // reset the explain level to avoid the previous explain level affect the current query.
                context.setExplainLevel(null);
            }

            // execPlan is the output of planner
            ExecPlan execPlan = null;
            try (Timer ignored = Tracers.watchScope("Total")) {
                redirectStatus = parsedStmt.getRedirectStatus();
                if (!isForwardToLeader()) {
                    if (context.shouldDumpQuery()) {
                        if (context.getDumpInfo() == null) {
                            context.setDumpInfo(new QueryDumpInfo(context));
                        } else {
                            context.getDumpInfo().reset();
                        }
                        context.getDumpInfo().setOriginStmt(parsedStmt.getOrigStmt().originStmt);
                        context.getDumpInfo().setStatement(parsedStmt);
                    }
                    if (parsedStmt instanceof ShowStmt) {
                        com.starrocks.sql.analyzer.Analyzer.analyze(parsedStmt, context);
                        Authorizer.check(parsedStmt, context);

                        QueryStatement selectStmt = ((ShowStmt) parsedStmt).toSelectStmt();
                        if (selectStmt != null) {
                            parsedStmt = selectStmt;
                            execPlan = StatementPlanner.plan(parsedStmt, context);
                        }
                    } else if (parsedStmt instanceof ExecuteStmt) {
                        ExecuteStmt executeStmt = (ExecuteStmt) parsedStmt;
                        com.starrocks.sql.analyzer.Analyzer.analyze(executeStmt, context);
                        prepareStmtContext = context.getPreparedStmt(executeStmt.getStmtName());
                        if (null == prepareStmtContext) {
                            throw new StarRocksPlannerException(ErrorType.INTERNAL_ERROR,
                                    "prepare statement can't be found @ %s, maybe has expired",
                                    executeStmt.getStmtName());
                        }
                        PrepareStmt prepareStmt = prepareStmtContext.getStmt();
                        parsedStmt = prepareStmt.assignValues(executeStmt.getParamsExpr());
                        parsedStmt.setOrigStmt(originStmt);

                        if (prepareStmt.getInnerStmt().isExistQueryScopeHint()) {
                            processQueryScopeHint();
                        }

                        try {
                            execPlan = PrepareStmtPlanner.plan(executeStmt, parsedStmt, context);
                        } catch (SemanticException e) {
                            if (e.getMessage().contains("Unknown partition")) {
                                throw new SemanticException(e.getMessage() +
                                        " maybe table partition changed after prepared statement creation");
                            } else {
                                throw e;
                            }
                        }
                    } else {
                        execPlan = StatementPlanner.plan(parsedStmt, context);
                        if (parsedStmt instanceof QueryStatement && context.shouldDumpQuery()) {
                            context.getDumpInfo().setExplainInfo(execPlan.getExplainString(TExplainLevel.COSTS));
                        }
                    }
                }
            } catch (SemanticException e) {
                dumpException(e);
                throw new AnalysisException(e.getMessage());
            } catch (StarRocksPlannerException e) {
                dumpException(e);
                if (e.getType().equals(ErrorType.USER_ERROR)) {
                    throw e;
                } else {
                    LOG.warn("Planner error: " + originStmt.originStmt, e);
                    throw e;
                }
            }

            // no need to execute http query dump request in BE
            if (context.isHTTPQueryDump) {
                return;
            }

            // For follower: verify sql in BlackList before forward to leader
            // For leader: if this is a proxy sql, no need to verify sql in BlackList because every fe has its own blacklist
            if ((parsedStmt instanceof QueryStatement || parsedStmt instanceof InsertStmt)
                    && Config.enable_sql_blacklist && !parsedStmt.isExplain() && !isProxy) {
                OriginStatement origStmt = parsedStmt.getOrigStmt();
                if (origStmt != null) {
                    String originSql = origStmt.originStmt.trim()
                            .toLowerCase().replaceAll(" +", " ");
                    // If this sql is in blacklist, show message.
                    GlobalStateMgr.getCurrentState().getSqlBlackList().verifying(originSql);
                }
            }

            if (isForwardToLeader()) {
                context.setIsForward(true);
                forwardToLeader();
                return;
            } else {
                LOG.debug("no need to transfer to Leader. stmt: {}", context.getStmtId());
            }

            if (parsedStmt instanceof QueryStatement) {
                final boolean isStatisticsJob = AnalyzerUtils.isStatisticsJob(context, parsedStmt);
                context.setStatisticsJob(isStatisticsJob);

                // Record planner costs in audit log
                Preconditions.checkNotNull(execPlan, "query must has a plan");

                int retryTime = Config.max_query_retry_time;
                ExecuteExceptionHandler.RetryContext retryContext =
                        new ExecuteExceptionHandler.RetryContext(0, execPlan, context, parsedStmt);
                for (int i = 0; i < retryTime; i++) {
                    boolean needRetry = false;
                    retryContext.setRetryTime(i);
                    try {
                        //reset query id for each retry
                        if (i > 0) {
                            uuid = UUID.randomUUID();
                            LOG.info("transfer QueryId: {} to {}", DebugUtil.printId(context.getQueryId()),
                                    DebugUtil.printId(uuid));
                            context.setExecutionId(
                                    new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                        }

                        handleQueryStmt(retryContext.getExecPlan());
                        break;
                    } catch (Exception e) {
                        if (i == retryTime - 1) {
                            throw e;
                        }
                        ExecuteExceptionHandler.handle(e, retryContext);
                        if (!context.getMysqlChannel().isSend()) {
                            String originStmt;
                            if (parsedStmt.getOrigStmt() != null) {
                                originStmt = parsedStmt.getOrigStmt().originStmt;
                            } else {
                                originStmt = this.originStmt.originStmt;
                            }
                            needRetry = true;
                            LOG.warn("retry {} times. stmt: {}", (i + 1), originStmt);
                        } else {
                            throw e;
                        }
                    } finally {
                        boolean isAsync = false;
                        try {
                            if (needRetry) {
                                // If the runtime profile is enabled, then we need to clean up the profile record related
                                // to this failed execution.
                                String queryId = DebugUtil.printId(context.getExecutionId());
                                ProfileManager.getInstance().removeProfile(queryId);
                            } else {
                                // Release all resources after the query finish as soon as possible, as query profile is
                                // asynchronous which can be delayed a long time.
                                if (coord != null) {
                                    coord.onReleaseSlots();
                                }

                                if (context instanceof ArrowFlightSqlConnectContext) {
                                    isAsync = true;
                                    tryProcessProfileAsync(execPlan, i);
                                } else if (context.isProfileEnabled()) {
                                    isAsync = tryProcessProfileAsync(execPlan, i);
                                    if (parsedStmt.isExplain() &&
                                            StatementBase.ExplainLevel.ANALYZE.equals(parsedStmt.getExplainLevel())) {
                                        if (coord != null && coord.isShortCircuit()) {
                                            throw new StarRocksException(
                                                    "short circuit point query doesn't suppot explain analyze stmt, " +
                                                            "you can set it off by using  set enable_short_circuit=false");
                                        }
                                        handleExplainStmt(ExplainAnalyzer.analyze(
                                                ProfilingExecPlan.buildFrom(execPlan), profile, null,
                                                context.getSessionVariable().getColorExplainOutput()));
                                    }
                                }
                            }

                            if (context.getState().isError()) {
                                RuntimeProfile plannerProfile = new RuntimeProfile("Planner");
                                Tracers.toRuntimeProfile(plannerProfile);
                                LOG.warn("Query {} failed. Planner profile : {}",
                                        context.getQueryId().toString(), plannerProfile);
                            }
                        } finally {
                            if (isAsync) {
                                QeProcessorImpl.INSTANCE.monitorQuery(context.getExecutionId(),
                                        System.currentTimeMillis() +
                                                context.getSessionVariable().getProfileTimeout() * 1000L);
                            } else {
                                QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
                            }
                        }
                    }
                }
            } else if (parsedStmt instanceof SetStmt) {
                handleSetStmt();
            } else if (parsedStmt instanceof UseDbStmt) {
                handleUseDbStmt();
            } else if (parsedStmt instanceof SetWarehouseStmt) {
                handleSetWarehouseStmt();
            } else if (parsedStmt instanceof UseCatalogStmt) {
                handleUseCatalogStmt();
            } else if (parsedStmt instanceof SetCatalogStmt) {
                handleSetCatalogStmt();
            } else if (parsedStmt instanceof CreateTableAsSelectStmt) {
                handleCreateTableAsSelectStmt(beginTimeInNanoSecond);
            } else if (parsedStmt instanceof DmlStmt) {
                handleDMLStmtWithProfile(execPlan, (DmlStmt) parsedStmt);
            } else if (parsedStmt instanceof DdlStmt) {
                handleDdlStmt();
            } else if (parsedStmt instanceof ShowStmt) {
                handleShow();
            } else if (parsedStmt instanceof KillStmt) {
                handleKill();
            } else if (parsedStmt instanceof ExportStmt) {
                handleExportStmt(context.getQueryId());
            } else if (parsedStmt instanceof UnsupportedStmt) {
                handleUnsupportedStmt();
            } else if (parsedStmt instanceof AnalyzeStmt) {
                handleAnalyzeStmt();
            } else if (parsedStmt instanceof AnalyzeProfileStmt) {
                handleAnalyzeProfileStmt();
            } else if (parsedStmt instanceof DropHistogramStmt) {
                handleDropHistogramStmt();
            } else if (parsedStmt instanceof DropStatsStmt) {
                handleDropStatsStmt();
            } else if (parsedStmt instanceof KillAnalyzeStmt) {
                handleKillAnalyzeStmt();
            } else if (parsedStmt instanceof AddSqlBlackListStmt) {
                handleAddSqlBlackListStmt();
            } else if (parsedStmt instanceof DelSqlBlackListStmt) {
                handleDelSqlBlackListStmt();
            } else if (parsedStmt instanceof ExecuteAsStmt) {
                handleExecAsStmt();
            } else if (parsedStmt instanceof ExecuteScriptStmt) {
                handleExecScriptStmt();
            } else if (parsedStmt instanceof SetRoleStmt) {
                handleSetRole();
            } else if (parsedStmt instanceof SetDefaultRoleStmt) {
                handleSetDefaultRole();
            } else if (parsedStmt instanceof UpdateFailPointStatusStatement) {
                handleUpdateFailPointStatusStmt();
            } else if (parsedStmt instanceof PrepareStmt) {
                handlePrepareStmt(execPlan);
            } else if (parsedStmt instanceof DeallocateStmt) {
                handleDeallocateStmt();
            } else if (parsedStmt instanceof AddBackendBlackListStmt) {
                handleAddBackendBlackListStmt();
            } else if (parsedStmt instanceof DelBackendBlackListStmt) {
                handleDelBackendBlackListStmt();
            } else if (parsedStmt instanceof PlanAdvisorStmt) {
                handlePlanAdvisorStmt();
            } else if (parsedStmt instanceof TranslateStmt) {
                handleTranslateStmt();
            } else if (parsedStmt instanceof BeginStmt) {
                TransactionStmtExecutor.beginStmt(context, (BeginStmt) parsedStmt);
            } else if (parsedStmt instanceof CommitStmt) {
                TransactionStmtExecutor.commitStmt(context, (CommitStmt) parsedStmt);
            } else if (parsedStmt instanceof RollbackStmt) {
                TransactionStmtExecutor.rollbackStmt(context, (RollbackStmt) parsedStmt);
            } else {
                context.getState().setError("Do not support this query.");
            }
        } catch (IOException e) {
            LOG.warn("execute IOException ", e);
            // the exception happens when interact with client
            // this exception shows the connection is gone
            context.getState().setError(e.getMessage());
        } catch (StarRocksException e) {
            String sql = originStmt != null ? originStmt.originStmt : "";
            // analysis exception only print message, not print the stack
            LOG.info("execute Exception, sql: {}, error: {}", sql, e.getMessage());
            context.getState().setError(e.getMessage());
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                context.getState().setErrType(QueryState.ErrType.IGNORE_ERR);
            } else if (e instanceof TimeoutException) {
                context.getState().setErrType(QueryState.ErrType.EXEC_TIME_OUT);
            } else if (e instanceof NoAliveBackendException) {
                context.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
            } else {
                // TODO: some StarRocksException doesn't belong to analysis error
                // we should set such error type to internal error
                context.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            }
        } catch (Throwable e) {
            String sql = originStmt != null ? originStmt.originStmt : "";
            LOG.warn("execute Exception, sql " + sql, e);
            context.getState().setError(e.getMessage());
            context.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
        } finally {
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
            if (context.getState().isError() && coord != null) {
                coord.cancel(PPlanFragmentCancelReason.INTERNAL_ERROR, context.getState().getErrorMessage());
            }

            if (parsedStmt != null && parsedStmt.isExistQueryScopeHint()) {
                clearQueryScopeHintContext();
            }

            // restore session variable in connect context
            context.setSessionVariable(sessionVariableBackup);

            if (shouldMarkIdleCheck) {
                WarehouseIdleChecker.decreaseRunningSQL(originWarehouseId);
            }

            recordExecStatsIntoContext();

            if (GracefulExitFlag.isGracefulExit() && context.isLeaderTransferred() && !isInternalStmt) {
                LOG.info("leader is transferred during executing, forward to new leader");
                isForwardToLeaderOpt = Optional.of(true);
                forwardToLeader();
            }
        }
    }

    /**
     * record execution stats for all kinds of statement
     * some statements may execute multiple statement which will also create multiple StmtExecutor, so here
     * we accumulate them into the ConnectContext instead of using the last one
     */
    private void recordExecStatsIntoContext() {
        PQueryStatistics execStats = getQueryStatisticsForAuditLog();
        context.getAuditEventBuilder().addCpuCostNs(execStats.getCpuCostNs() != null ? execStats.getCpuCostNs() : 0);
        context.getAuditEventBuilder()
                .addMemCostBytes(execStats.getMemCostBytes() != null ? execStats.getMemCostBytes() : 0);
        context.getAuditEventBuilder().addScanBytes(execStats.getScanBytes() != null ? execStats.getScanBytes() : 0);
        context.getAuditEventBuilder().addScanRows(execStats.getScanRows() != null ? execStats.getScanRows() : 0);
        context.getAuditEventBuilder().addSpilledBytes(execStats.spillBytes != null ? execStats.spillBytes : 0);
        context.getAuditEventBuilder().setReturnRows(execStats.returnedRows == null ? 0 : execStats.returnedRows);
    }

    private void clearQueryScopeHintContext() {
        Iterator<Map.Entry<String, UserVariable>> iterator = context.userVariables.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, UserVariable> entry = iterator.next();
            if (entry.getValue().isFromHint()) {
                iterator.remove();
            }
        }
    }

    // support select hint e.g. select /*+ SET_VAR(query_timeout=1) */ sleep(3);
    @VisibleForTesting
    public void processQueryScopeHint() throws DdlException {
        SessionVariable clonedSessionVariable = null;
        UUID queryId = context.getQueryId();
        final TUniqueId executionId = context.getExecutionId();
        Map<String, UserVariable> clonedUserVars = new ConcurrentHashMap<>();
        clonedUserVars.putAll(context.getUserVariables());
        boolean hasUserVariableHint = parsedStmt.getAllQueryScopeHints()
                .stream().anyMatch(hint -> hint instanceof UserVariableHint);
        if (hasUserVariableHint) {
            context.modifyUserVariablesCopyInWrite(clonedUserVars);
        }
        boolean executeSuccess = true;
        try {
            for (HintNode hint : parsedStmt.getAllQueryScopeHints()) {
                if (hint instanceof SetVarHint) {
                    if (clonedSessionVariable == null) {
                        clonedSessionVariable = (SessionVariable) context.sessionVariable.clone();
                    }
                    for (Map.Entry<String, String> entry : hint.getValue().entrySet()) {
                        GlobalStateMgr.getCurrentState().getVariableMgr().setSystemVariable(clonedSessionVariable,
                                new SystemVariable(entry.getKey(), new StringLiteral(entry.getValue())), true);
                    }
                }

                if (hint instanceof UserVariableHint) {
                    UserVariableHint userVariableHint = (UserVariableHint) hint;
                    for (Map.Entry<String, UserVariable> entry : userVariableHint.getUserVariables().entrySet()) {
                        if (context.userVariables.containsKey(entry.getKey())) {
                            throw new SemanticException(PARSER_ERROR_MSG.invalidUserVariableHint(entry.getKey(),
                                    "the user variable name in the hint must not match any existing variable names"));
                        }
                        SetStmtAnalyzer.analyzeUserVariable(entry.getValue());
                        SetStmtAnalyzer.calcuteUserVariable(entry.getValue());
                        if (entry.getValue().getEvaluatedExpression() == null) {
                            try {
                                final UUID uuid = UUIDUtil.genUUID();
                                context.setQueryId(uuid);
                                context.setExecutionId(UUIDUtil.toTUniqueId(uuid));
                                entry.getValue().deriveUserVariableExpressionResult(context);
                            } finally {
                                context.setQueryId(queryId);
                                context.setExecutionId(executionId);
                                context.resetReturnRows();
                                context.getState().reset();
                            }
                        }
                        clonedUserVars.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        } catch (Throwable t) {
            executeSuccess = false;
            throw t;
        } finally {
            if (hasUserVariableHint) {
                context.resetUserVariableCopyInWrite();
                if (executeSuccess) {
                    context.modifyUserVariables(clonedUserVars);
                }
            }
        }

        if (clonedSessionVariable != null) {
            context.setSessionVariable(clonedSessionVariable);
        }
    }

    private boolean createTableCreatedByCTAS(CreateTableAsSelectStmt stmt) throws Exception {
        try {
            if (stmt instanceof CreateTemporaryTableAsSelectStmt) {
                CreateTemporaryTableStmt createTemporaryTableStmt =
                        (CreateTemporaryTableStmt) stmt.getCreateTableStmt();
                createTemporaryTableStmt.setSessionId(context.getSessionId());
                return context.getGlobalStateMgr().getMetadataMgr().createTemporaryTable(createTemporaryTableStmt);
            } else {
                return context.getGlobalStateMgr().getMetadataMgr().createTable(stmt.getCreateTableStmt());
            }
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    private void dropTableCreatedByCTAS(CreateTableAsSelectStmt stmt) throws Exception {
        if (stmt instanceof CreateTemporaryTableAsSelectStmt) {
            DropTemporaryTableStmt dropTemporaryTableStmt =
                    new DropTemporaryTableStmt(true, stmt.getCreateTableStmt().getDbTbl(), true);
            dropTemporaryTableStmt.setSessionId(context.getSessionId());
            DDLStmtExecutor.execute(dropTemporaryTableStmt, context);
        } else {
            DDLStmtExecutor.execute(new DropTableStmt(
                    true, stmt.getCreateTableStmt().getDbTbl(), true), context);
        }
    }

    private void handleCreateTableAsSelectStmt(long beginTimeInNanoSecond) throws Exception {
        CreateTableAsSelectStmt createTableAsSelectStmt = (CreateTableAsSelectStmt) parsedStmt;

        if (!createTableCreatedByCTAS(createTableAsSelectStmt)) {
            return;
        }

        // if create table failed should not drop table. because table may already exist,
        // and for other cases the exception will throw and the rest of the code will not be executed.
        try {
            InsertStmt insertStmt = createTableAsSelectStmt.getInsertStmt();
            ExecPlan execPlan = StatementPlanner.plan(insertStmt, context);
            handleDMLStmtWithProfile(execPlan, ((CreateTableAsSelectStmt) parsedStmt).getInsertStmt());
            if (context.getState().getStateType() == MysqlStateType.ERR) {
                dropTableCreatedByCTAS(createTableAsSelectStmt);
            }
        } catch (Throwable t) {
            LOG.warn("handle create table as select stmt fail", t);
            dropTableCreatedByCTAS(createTableAsSelectStmt);
            throw t;
        }
    }

    private void dumpException(Exception e) {
        if (context.isHTTPQueryDump()) {
            context.getDumpInfo().addException(ExceptionUtils.getStackTrace(e));
        } else if (context.getSessionVariable().getEnableQueryDump()) {
            QueryDumpLog.getQueryDump().log(GsonUtils.GSON.toJson(context.getDumpInfo()));
        }
    }

    private void forwardToLeader() throws Exception {
        if (parsedStmt instanceof ExecuteStmt) {
            throw new AnalysisException("ExecuteStmt Statement don't support statement need to be forward to leader");
        }
        try {
            context.incPendingForwardRequest();
            leaderOpExecutor = new LeaderOpExecutor(parsedStmt, originStmt, context, redirectStatus);
            LOG.debug("need to transfer to Leader. stmt: {}", context.getStmtId());
            leaderOpExecutor.execute();
        } finally {
            context.decPendingForwardRequest();
        }
    }

    private boolean tryProcessProfileAsync(ExecPlan plan, int retryIndex) {
        if (coord == null) {
            return false;
        }

        // Disable runtime profile processing after the query is finished.
        coord.setTopProfileSupplier(null);

        if (coord.getQueryProfile() == null) {
            return false;
        }

        // This process will get information from the context, so it must be executed synchronously.
        // Otherwise, the context may be changed, for example, containing the wrong query id.
        profile = buildTopLevelProfile();

        long profileCollectStartTime = System.currentTimeMillis();
        long startTime = context.getStartTime();
        TUniqueId executionId = context.getExecutionId();
        QueryDetail queryDetail = context.getQueryDetail();
        boolean needMerge = context.needMergeProfile();

        // DO NOT use context int the async task, because the context is shared among consecutive queries.
        // profile of query1 maybe executed when query2 is under execution.
        Consumer<Boolean> task = (Boolean isAsync) -> {
            RuntimeProfile summaryProfile = profile.getChild("Summary");
            summaryProfile.addInfoString(ProfileManager.PROFILE_COLLECT_TIME,
                    DebugUtil.getPrettyStringMs(System.currentTimeMillis() - profileCollectStartTime));
            summaryProfile.addInfoString("IsProfileAsync", String.valueOf(isAsync));
            profile.addChild(coord.buildQueryProfile(needMerge));

            // Update TotalTime to include the Profile Collect Time and the time to build the profile.
            long now = System.currentTimeMillis();
            long totalTimeMs = now - startTime;
            summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(now));
            summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));
            if (retryIndex > 0) {
                summaryProfile.addInfoString(ProfileManager.RETRY_TIMES, Integer.toString(retryIndex + 1));
            }

            ProfilingExecPlan profilingPlan;
            if (coord.isShortCircuit()) {
                profilingPlan = null;
            } else {
                profilingPlan = plan == null ? null : plan.getProfilingPlan();
            }
            String profileContent = ProfileManager.getInstance().pushProfile(profilingPlan, profile);
            if (queryDetail != null) {
                queryDetail.setProfile(profileContent);
            }
            QeProcessorImpl.INSTANCE.unMonitorQuery(executionId);
            QeProcessorImpl.INSTANCE.unregisterQuery(executionId);
            if (Config.enable_collect_query_detail_info && Config.enable_profile_log) {
                String jsonString = GSON.toJson(queryDetail);
                if (Config.enable_profile_log_compress) {
                    byte[] jsonBytes;
                    try {
                        jsonBytes = CompressionUtils.gzipCompressString(jsonString);
                        PROFILE_LOG.info(jsonBytes);
                    } catch (IOException e) {
                        LOG.warn("Compress queryDetail string failed, length: {}, reason: {}",
                                jsonString.length(), e.getMessage());
                    }
                } else {
                    PROFILE_LOG.info(jsonString);
                }
            }
        };
        return coord.tryProcessProfileAsync(task);
    }

    public void registerSubStmtExecutor(StmtExecutor subStmtExecutor) {
        if (subStmtExecutors == null) {
            subStmtExecutors = Lists.newArrayList();
        }
        subStmtExecutors.add(subStmtExecutor);
    }

    public List<StmtExecutor> getSubStmtExecutors() {
        if (subStmtExecutors == null) {
            return Lists.newArrayList();
        }
        return subStmtExecutors;
    }

    // Because this is called by other thread
    public void cancel(String cancelledMessage) {
        if (parsedStmt instanceof DeleteStmt && ((DeleteStmt) parsedStmt).shouldHandledByDeleteHandler()) {
            DeleteStmt deleteStmt = (DeleteStmt) parsedStmt;
            long jobId = deleteStmt.getJobId();
            if (jobId != -1) {
                GlobalStateMgr.getCurrentState().getDeleteMgr().killJob(jobId);
            }
        } else {
            if (subStmtExecutors != null && !subStmtExecutors.isEmpty()) {
                for (StmtExecutor sub : subStmtExecutors) {
                    sub.cancel(cancelledMessage);
                }
            }
            Coordinator coordRef = coord;
            if (coordRef != null) {
                coordRef.cancel(cancelledMessage);
            }
        }
    }

    // Handle kill statement.
    private void handleKill() throws DdlException {
        KillStmt killStmt = (KillStmt) parsedStmt;
        if (killStmt.getQueryId() != null) {
            handleKillQuery(killStmt.getQueryId());
        } else {
            long id = killStmt.getConnectionId();
            ConnectContext killCtx = null;
            if (isProxy) {
                final String hostName = context.getProxyHostName();
                killCtx = ProxyContextManager.getInstance().getContext(hostName, (int) id);
            } else {
                killCtx = context.getConnectScheduler().getContext(id);
            }
            if (killCtx == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_THREAD, id);
            }
            handleKill(killCtx, killStmt.isConnectionKill() && !isProxy);
        }
    }

    // Handle kill statement.
    private void handleKill(ConnectContext killCtx, boolean killConnection) {
        try {
            if (killCtx.hasPendingForwardRequest()) {
                forwardToLeader();
                return;
            }
        } catch (Exception e) {
            LOG.warn("failed to kill connection", e);
        }

        Preconditions.checkNotNull(killCtx);
        if (context == killCtx) {
            // Suicide
            context.setKilled();
        } else {
            if (!Objects.equals(killCtx.getQualifiedUser(), context.getQualifiedUser())) {
                try {
                    Authorizer.checkSystemAction(context, PrivilegeType.OPERATE);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
                }
            }
            killCtx.kill(killConnection, "killed manually: " + originStmt.getOrigStmt());
        }
        context.getState().setOk();
    }

    // Handle kill running query statement.
    private void handleKillQuery(String queryId) throws DdlException {
        if (StringUtils.isEmpty(queryId)) {
            context.getState().setOk();
            return;
        }
        // > 0 means it is forwarded from other fe
        if (context.getForwardTimes() == 0) {
            String errorMsg = null;
            // forward to all fe
            for (Frontend fe : GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null /* all */)) {
                LeaderOpExecutor leaderOpExecutor =
                        new LeaderOpExecutor(Pair.create(fe.getHost(), fe.getRpcPort()), parsedStmt, originStmt,
                                context, redirectStatus);
                try {
                    leaderOpExecutor.execute();
                    // if query is successfully killed by this fe, it can return now
                    if (context.getState().getStateType() == MysqlStateType.OK) {
                        context.getState().setOk();
                        return;
                    }
                    errorMsg = context.getState().getErrorMessage();
                } catch (TTransportException e) {
                    errorMsg = "Failed to connect to fe " + fe.getHost() + ":" + fe.getRpcPort();
                    LOG.warn(errorMsg, e);
                } catch (Exception e) {
                    errorMsg = "Failed to connect to fe " + fe.getHost() + ":" + fe.getRpcPort() + " due to " +
                            e.getMessage();
                    LOG.warn(e.getMessage(), e);
                }
            }
            // if the queryId is not found in any fe, print the error message
            context.getState().setError(errorMsg == null ? ErrorCode.ERR_UNKNOWN_ERROR.formatErrorMsg() : errorMsg);
            return;
        }
        ConnectContext killCtx = ExecuteEnv.getInstance().getScheduler().findContextByCustomQueryId(queryId);
        if (killCtx == null) {
            killCtx = ExecuteEnv.getInstance().getScheduler().findContextByQueryId(queryId);
        }
        if (killCtx == null) {
            killCtx = ProxyContextManager.getInstance().getContextByQueryId(queryId);
        }
        if (killCtx == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_QUERY, queryId);
        }
        handleKill(killCtx, false);
    }

    // Process set statement.
    private void handleSetStmt() {
        try {
            SetStmt setStmt = (SetStmt) parsedStmt;
            SetExecutor executor = new SetExecutor(context, setStmt);
            executor.execute();
        } catch (DdlException e) {
            // Return error message to client.
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private Coordinator.Factory getCoordinatorFactory() {
        return new DefaultCoordinator.Factory();
    }

    private CostPredictor getCostPredictor() {
        return CostPredictor.getServiceBasedCostPredictor();
    }

    // Process a select statement.
    private void handleQueryStmt(ExecPlan execPlan) throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();

        boolean isExplainAnalyze = parsedStmt.isExplain()
                && StatementBase.ExplainLevel.ANALYZE.equals(parsedStmt.getExplainLevel());
        boolean isSchedulerExplain = parsedStmt.isExplain()
                && StatementBase.ExplainLevel.SCHEDULER.equals(parsedStmt.getExplainLevel());

        boolean isPlanAdvisorAnalyze = StatementBase.ExplainLevel.PLAN_ADVISOR.equals(parsedStmt.getExplainLevel());

        boolean isOutfileQuery =
                (parsedStmt instanceof QueryStatement) && ((QueryStatement) parsedStmt).hasOutFileClause();

        if (isOutfileQuery) {
            boolean hasTemporaryTable = AnalyzerUtils.hasTemporaryTables(parsedStmt);
            if (hasTemporaryTable) {
                throw new SemanticException("temporary table doesn't support select outfile statement");
            }
        }

        boolean executeInFe = !isExplainAnalyze && !isSchedulerExplain && !isOutfileQuery
                && canExecuteInFe(context, execPlan.getPhysicalPlan());

        if (isExplainAnalyze) {
            context.getSessionVariable().setEnableProfile(true);
            context.getSessionVariable().setEnableAsyncProfile(false);
            context.getSessionVariable().setPipelineProfileLevel(1);
        } else if (isSchedulerExplain) {
            // Do nothing.
        } else if (parsedStmt.isExplain()) {
            String explainString = buildExplainString(execPlan, parsedStmt, context, ResourceGroupClassifier.QueryType.SELECT,
                    parsedStmt.getExplainLevel());
            if (executeInFe) {
                explainString = "EXECUTE IN FE\n" + explainString;
            }
            handleExplainStmt(explainString);
            return;
        }

        List<PlanFragment> fragments = execPlan.getFragments();
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        TDescriptorTable descTable = execPlan.getDescTbl().toThrift();
        List<String> colNames = execPlan.getColNames();
        List<Expr> outputExprs = execPlan.getOutputExprs();

        if (executeInFe) {
            coord = new FeExecuteCoordinator(context, execPlan);
        } else {
            coord = getCoordinatorFactory().createQueryScheduler(context, fragments, scanNodes, descTable);
        }

        // Predict the cost of this query
        if (Config.enable_query_cost_prediction) {
            CostPredictor predictor = getCostPredictor();
            long memBytes = predictor.predictMemoryBytes(execPlan);
            coord.setPredictedCost(memBytes);
            context.getAuditEventBuilder().setPredictMemBytes(memBytes);
        }

        QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(),
                new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));

        if (isSchedulerExplain) {
            coord.execWithoutDeploy();
            handleExplainStmt(coord.getSchedulerExplain());
            return;
        }

        coord.execWithQueryDeployExecutor();
        coord.setTopProfileSupplier(this::buildTopLevelProfile);
        coord.setExecPlan(execPlan);

        RowBatch batch = null;
        if (context instanceof HttpConnectContext) {
            batch = httpResultSender.sendQueryResult(coord, execPlan, parsedStmt.getOrigStmt().getOrigStmt());
        } else if (context instanceof ArrowFlightSqlConnectContext) {
            ArrowFlightSqlConnectContext ctx = (ArrowFlightSqlConnectContext) context;
            ctx.setReturnFromFE(false);
            ctx.setExecPlan(execPlan);
            ctx.setCoordinator(coord);
            ctx.getState().setEof();
        } else {
            boolean needSendResult = !isPlanAdvisorAnalyze && !isExplainAnalyze
                    && !context.getSessionVariable().isEnableExecutionOnly();
            // send mysql result
            // 1. If this is a query with OUTFILE clause, eg: select * from tbl1 into outfile xxx,
            //    We will not send real query result to client. Instead, we only send OK to client with
            //    number of rows selected. For example:
            //          mysql> select * from tbl1 into outfile xxx;
            //          Query OK, 10 rows affected (0.01 sec)
            //
            // 2. If this is a query, send the result expr fields first, and send result data back to client.
            MysqlChannel channel = context.getMysqlChannel();
            boolean isSendFields = false;
            do {
                batch = coord.getNext();
                // for outfile query, there will be only one empty batch send back with eos flag
                if (batch.getBatch() != null && !isOutfileQuery && needSendResult) {
                    // For some language driver, getting error packet after fields packet will be recognized as a success result
                    // so We need to send fields after first batch arrived
                    if (!isSendFields) {
                        sendFields(colNames, outputExprs);
                        isSendFields = true;
                    }
                    if (!isProxy && channel.isSendBufferNull()) {
                        int bufferSize = 0;
                        for (ByteBuffer row : batch.getBatch().getRows()) {
                            bufferSize += (row.position() - row.limit());
                        }
                        // +8 for header size
                        channel.initBuffer(bufferSize + 8);
                    }

                    for (ByteBuffer row : batch.getBatch().getRows()) {
                        if (isProxy) {
                            proxyResultBuffer.add(row);
                        } else {
                            channel.sendOnePacket(row);
                        }
                    }
                    context.updateReturnRows(batch.getBatch().getRows().size());
                }
            } while (!batch.isEos());
            if (!isSendFields && !isOutfileQuery && !isExplainAnalyze && !isPlanAdvisorAnalyze) {
                sendFields(colNames, outputExprs);
            }
        }

        processQueryStatisticsFromResult(batch, execPlan, isOutfileQuery);
    }

    /**
     * The query result batch will piggyback query statistics in it
     */
    private void processQueryStatisticsFromResult(RowBatch batch, ExecPlan execPlan, boolean isOutfileQuery) {
        if (batch != null) {
            statisticsForAuditLog = batch.getQueryStatistics();
            if (!isOutfileQuery) {
                context.getState().setEof();
            } else {
                context.getState().setOk(statisticsForAuditLog.returnedRows, 0, "");
            }

            if (null != statisticsForAuditLog) {
                analyzePlanWithExecStats(execPlan);
            }

            if (null == statisticsForAuditLog || null == statisticsForAuditLog.statsItems ||
                    statisticsForAuditLog.statsItems.isEmpty()) {
                return;
            }

            // collect table-level metrics
            Set<Long> tableIds = Sets.newHashSet();
            for (QueryStatisticsItemPB item : statisticsForAuditLog.statsItems) {
                if (item == null) {
                    continue;
                }
                TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(item.tableId);
                entity.counterScanRowsTotal.increase(item.scanRows);
                entity.counterScanBytesTotal.increase(item.scanBytes);
                tableIds.add(item.tableId);
            }
            for (Long tableId : tableIds) {
                TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(tableId);
                entity.counterScanFinishedTotal.increase(1L);
            }
        }
    }

    private void analyzePlanWithExecStats(ExecPlan execPlan) {
        SessionVariable sessionVariable = context.getSessionVariable();
        if (!sessionVariable.isEnablePlanAdvisor() ||
                CollectionUtils.isEmpty(statisticsForAuditLog.getNodeExecStatsItems())) {
            return;
        }
        long elapseMs = System.currentTimeMillis() - context.getStartTime();
        OperatorTuningGuides usedTuningGuides =
                PlanTuningAdvisor.getInstance().getOperatorTuningGuides(context.getQueryId());
        if (usedTuningGuides != null) {
            usedTuningGuides.addOptimizedRecord(context.getQueryId(), elapseMs);
            PlanTuningAdvisor.getInstance().removeOptimizedQueryRecord(context.getQueryId());
        } else if (sessionVariable.isEnablePlanAnalyzer() || elapseMs > Config.slow_query_analyze_threshold) {
            try (Timer ignored = Tracers.watchScope(Tracers.Module.OPTIMIZER, "AnalyzeExecStats")) {
                SkeletonBuilder builder = new SkeletonBuilder(statisticsForAuditLog.getNodeExecStatsItems());
                Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = builder.buildSkeleton(execPlan.getPhysicalPlan());
                OperatorTuningGuides tuningGuides = new OperatorTuningGuides(context.getQueryId(), elapseMs);
                PlanTuningAnalyzer.getInstance().analyzePlan(execPlan.getPhysicalPlan(), pair.second, tuningGuides);
                PlanTuningAdvisor.getInstance()
                        .putTuningGuides(parsedStmt.getOrigStmt().getOrigStmt(), pair.first, tuningGuides);
                if (!tuningGuides.isEmpty()) {
                    Tracers.record(Tracers.Module.BASE, "BuildTuningGuides", tuningGuides.getFullTuneGuidesInfo());
                }
            }
        }
    }

    // TODO: move to DdlExecutor
    private void handleAnalyzeStmt() throws IOException {
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) parsedStmt;
        TableName tableName = analyzeStmt.getTableName();
        Database db =
                GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(tableName.getCatalog(), tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tableName.getCatalogAndDb());
        }
        Table table = MetaUtils.getSessionAwareTable(context, db, analyzeStmt.getTableName());
        if (StatisticUtils.isEmptyTable(table)) {
            return;
        }

        StatsConstants.AnalyzeType analyzeType;
        AnalyzeTypeDesc analyzeTypeDesc = analyzeStmt.getAnalyzeTypeDesc();
        if (analyzeTypeDesc.isHistogram()) {
            analyzeType = StatsConstants.AnalyzeType.HISTOGRAM;
        } else {
            analyzeType = analyzeStmt.isSample() ? StatsConstants.AnalyzeType.SAMPLE : StatsConstants.AnalyzeType.FULL;
        }

        AnalyzeStatus analyzeStatus;
        if (analyzeStmt.isExternal()) {
            String catalogName = analyzeStmt.getTableName().getCatalog();
            analyzeStatus = new ExternalAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                    catalogName, db.getOriginName(), table.getName(),
                    table.getUUID(),
                    analyzeStmt.getColumnNames(),
                    analyzeType, StatsConstants.ScheduleType.ONCE, analyzeStmt.getProperties(), LocalDateTime.now());
        } else {
            //Only for send sync command to client
            analyzeStatus = new NativeAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                    db.getId(), table.getId(), analyzeStmt.getColumnNames(),
                    analyzeType, StatsConstants.ScheduleType.ONCE, analyzeStmt.getProperties(), LocalDateTime.now());
        }
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.PENDING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);

        int queryTimeout = context.getSessionVariable().getQueryTimeoutS();
        int insertTimeout = context.getSessionVariable().getInsertTimeoutS();
        try {
            Future<?> future = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeTaskThreadPool()
                    .submit(() -> executeAnalyze(analyzeStmt, analyzeStatus, db, table));

            if (!analyzeStmt.isAsync()) {
                // sync statistics collection doesn't be interrupted by query timeout, but
                // will print warning log if timeout, so we update timeout temporarily to avoid
                // warning log
                context.getSessionVariable().setQueryTimeoutS((int) Config.statistic_collect_query_timeout);
                context.getSessionVariable().setInsertTimeoutS((int) Config.statistic_collect_query_timeout);
                future.get();
            }
        } catch (RejectedExecutionException e) {
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            analyzeStatus.setReason("The statistics tasks running concurrently exceed the upper limit");
            LOG.warn("analyze statement exceed concurrency limit {}", analyzeStmt.toString(), e);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        } catch (ExecutionException | InterruptedException e) {
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            analyzeStatus.setReason("analyze failed due to " + e.getMessage());
            LOG.warn("analyze statement failed {}", analyzeStmt.toString(), e);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        } finally {
            context.getSessionVariable().setQueryTimeoutS(queryTimeout);
            context.getSessionVariable().setInsertTimeoutS(insertTimeout);
        }

        ShowResultSet resultSet = analyzeStatus.toShowResult();
        if (isProxy) {
            proxyResultSet = resultSet;
            context.getState().setEof();
            return;
        }

        sendShowResult(resultSet);
    }

    private void handleAnalyzeProfileStmt() throws IOException, StarRocksException {
        AnalyzeProfileStmt analyzeProfileStmt = (AnalyzeProfileStmt) parsedStmt;
        String queryId = analyzeProfileStmt.getQueryId();
        List<Integer> planNodeIds = analyzeProfileStmt.getPlanNodeIds();
        ProfileManager.ProfileElement profileElement = ProfileManager.getInstance().getProfileElement(queryId);
        Preconditions.checkNotNull(profileElement, "query not exists");
        // For short circuit query, 'ProfileElement#plan' is null
        if (profileElement.plan == null && profileElement.infoStrings.get(ProfileManager.QUERY_TYPE) != null &&
                !profileElement.infoStrings.get(ProfileManager.QUERY_TYPE).equals("Load")) {
            throw new StarRocksException(
                    "short circuit point query doesn't suppot analyze profile stmt, " +
                            "you can set it off by using  set enable_short_circuit=false");
        }
        handleExplainStmt(ExplainAnalyzer.analyze(profileElement.plan,
                RuntimeProfileParser.parseFrom(CompressionUtils.gzipDecompressString(profileElement.profileContent)),
                planNodeIds, context.getSessionVariable().getColorExplainOutput()));
    }

    private void executeAnalyze(AnalyzeStmt analyzeStmt, AnalyzeStatus analyzeStatus, Database db, Table table) {
        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        if (table.isTemporaryTable()) {
            statsConnectCtx.setSessionId(context.getSessionId());
        }
        // from current session, may execute analyze stmt
        statsConnectCtx.getSessionVariable().setStatisticCollectParallelism(
                context.getSessionVariable().getStatisticCollectParallelism());
        statsConnectCtx.setStatisticsConnection(true);
        try (var guard = statsConnectCtx.bindScope()) {
            executeAnalyze(statsConnectCtx, analyzeStmt, analyzeStatus, db, table);
        } finally {
            // copy the stats to current context
            AuditEvent event = statsConnectCtx.getAuditEventBuilder().build();
            context.getAuditEventBuilder().copyExecStatsFrom(event);
        }
    }

    private void executeAnalyze(ConnectContext statsConnectCtx, AnalyzeStmt analyzeStmt,
                                AnalyzeStatus analyzeStatus, Database db, Table table) {
        AnalyzeTypeDesc analyzeTypeDesc = analyzeStmt.getAnalyzeTypeDesc();
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        if (analyzeStmt.isExternal()) {
            if (analyzeTypeDesc.isHistogram()) {
                statisticExecutor.collectStatistics(statsConnectCtx,
                        new ExternalHistogramStatisticsCollectJob(analyzeStmt.getTableName().getCatalog(),
                                db, table, analyzeStmt.getColumnNames(), analyzeStmt.getColumnTypes(),
                                StatsConstants.AnalyzeType.HISTOGRAM, StatsConstants.ScheduleType.ONCE,
                                analyzeStmt.getProperties()),
                        analyzeStatus,
                        false);
            } else {
                StatsConstants.AnalyzeType analyzeType = analyzeStmt.isSample() ? StatsConstants.AnalyzeType.SAMPLE :
                        StatsConstants.AnalyzeType.FULL;
                statisticExecutor.collectStatistics(statsConnectCtx,
                        StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(
                                analyzeStmt.getTableName().getCatalog(),
                                db, table, null,
                                analyzeStmt.getColumnNames(),
                                analyzeStmt.getColumnTypes(),
                                analyzeType,
                                StatsConstants.ScheduleType.ONCE, analyzeStmt.getProperties()),
                        analyzeStatus,
                        false);
            }
        } else {
            if (analyzeTypeDesc.isHistogram()) {
                statisticExecutor.collectStatistics(statsConnectCtx,
                        new HistogramStatisticsCollectJob(db, table, analyzeStmt.getColumnNames(),
                                analyzeStmt.getColumnTypes(), StatsConstants.ScheduleType.ONCE,
                                analyzeStmt.getProperties()),
                        analyzeStatus,
                        // Sync load cache, auto-populate column statistic cache after Analyze table manually
                        false);
            } else {
                StatsConstants.AnalyzeType analyzeType = analyzeStatus.getType();
                statisticExecutor.collectStatistics(statsConnectCtx,
                        StatisticsCollectJobFactory.buildStatisticsCollectJob(db, table, analyzeStmt.getPartitionIds(),
                                analyzeStmt.getColumnNames(),
                                analyzeStmt.getColumnTypes(),
                                analyzeType,
                                StatsConstants.ScheduleType.ONCE,
                                analyzeStmt.getProperties(),
                                analyzeTypeDesc.getStatsTypes(),
                                analyzeStmt.getColumnNames() != null ? List.of(analyzeStmt.getColumnNames()) : null),
                                analyzeStatus,
                                // Sync load cache, auto-populate column statistic cache after Analyze table manually
                                false);
            }
        }
    }

    private void handleDropStatsStmt() {
        DropStatsStmt dropStatsStmt = (DropStatsStmt) parsedStmt;
        TableName tableName = dropStatsStmt.getTableName();
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName.getCatalog(),
                tableName.getDb(), tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName.toString());
        }

        AnalyzeMgr analyzeMgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
        StatisticStorage statisticStorage = GlobalStateMgr.getCurrentState().getStatisticStorage();
        if (dropStatsStmt.isExternal()) {
            analyzeMgr.dropExternalAnalyzeStatus(table.getUUID());
            analyzeMgr.dropExternalBasicStatsData(table.getUUID());
            analyzeMgr.removeExternalBasicStatsMeta(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            List<String> columns = table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());
            statisticStorage.expireConnectorTableColumnStatistics(table, columns);
        } else {
            List<String> columns = table.getBaseSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                    .collect(Collectors.toList());
            analyzeMgr.dropMultiColumnStatsMetaAndData(StatisticUtils.buildConnectContext(), Sets.newHashSet(table.getId()));
            statisticStorage.expireMultiColumnStatistics(table.getId());

            if (!dropStatsStmt.isMultiColumn()) {
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropAnalyzeStatus(table.getId());
                GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                        .dropBasicStatsMetaAndData(StatisticUtils.buildConnectContext(), Sets.newHashSet(table.getId()));
                GlobalStateMgr.getCurrentState().getStatisticStorage().expireTableAndColumnStatistics(table, columns);
            }
        }
    }

    private void handleDropHistogramStmt() {
        DropHistogramStmt dropHistogramStmt = (DropHistogramStmt) parsedStmt;
        TableName tableName = dropHistogramStmt.getTableName();
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName.getCatalog(),
                tableName.getDb(), tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName.toString());
        }

        if (dropHistogramStmt.isExternal()) {
            List<String> columns = dropHistogramStmt.getColumnNames();

            GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropExternalAnalyzeStatus(table.getUUID());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropExternalHistogramStatsMetaAndData(
                    StatisticUtils.buildConnectContext(), dropHistogramStmt.getTableName(), table, columns);
            GlobalStateMgr.getCurrentState().getStatisticStorage().expireConnectorHistogramStatistics(table, columns);
        } else {
            List<String> columns = table.getBaseSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                    .collect(Collectors.toList());

            GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropAnalyzeStatus(table.getId());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                    .dropHistogramStatsMetaAndData(StatisticUtils.buildConnectContext(),
                            Sets.newHashSet(table.getId()));
            GlobalStateMgr.getCurrentState().getStatisticStorage().expireHistogramStatistics(table.getId(), columns);
        }
    }

    private void handleKillAnalyzeStmt() {
        KillAnalyzeStmt killAnalyzeStmt = (KillAnalyzeStmt) parsedStmt;
        long analyzeId = killAnalyzeStmt.getAnalyzeId();
        AnalyzeMgr analyzeManager = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
        checkPrivilegeForKillAnalyzeStmt(context, analyzeId);
        // Try to kill the job anyway.
        analyzeManager.killConnection(analyzeId);
    }

    private void checkTblPrivilegeForKillAnalyzeStmt(ConnectContext context, String catalogName, String dbName,
                                                     String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName, dbName, tableName);
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName);
        }
        Authorizer.checkActionForAnalyzeStatement(context, new TableName(catalogName, dbName, tableName));
    }

    public void checkPrivilegeForKillAnalyzeStmt(ConnectContext context, long analyzeId) {
        AnalyzeMgr analyzeManager = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
        AnalyzeStatus analyzeStatus = analyzeManager.getAnalyzeStatus(analyzeId);
        AnalyzeJob analyzeJob = analyzeManager.getAnalyzeJob(analyzeId);
        if (analyzeStatus != null) {
            try {
                String catalogName = analyzeStatus.getCatalogName();
                String dbName = analyzeStatus.getDbName();
                String tableName = analyzeStatus.getTableName();
                checkTblPrivilegeForKillAnalyzeStmt(context, catalogName, dbName, tableName);
            } catch (MetaNotFoundException ignore) {
                // If the db or table doesn't exist anymore, we won't check privilege on it
            }
        } else if (analyzeJob != null && analyzeJob.isNative()) {
            NativeAnalyzeJob nativeAnalyzeJob = (NativeAnalyzeJob) analyzeJob;
            Set<TableName> tableNames = AnalyzerUtils.getAllTableNamesForAnalyzeJobStmt(nativeAnalyzeJob.getDbId(),
                    nativeAnalyzeJob.getTableId());
            tableNames.forEach(tableName ->
                    checkTblPrivilegeForKillAnalyzeStmt(context, tableName.getCatalog(), tableName.getDb(),
                            tableName.getTbl())
            );
        }
    }

    private void handleAddSqlBlackListStmt() {
        AddSqlBlackListStmt addSqlBlackListStmt = (AddSqlBlackListStmt) parsedStmt;
        long id = GlobalStateMgr.getCurrentState().getSqlBlackList().put(addSqlBlackListStmt.getSqlPattern());
        GlobalStateMgr.getCurrentState().getEditLog()
                .logAddSQLBlackList(new SqlBlackListPersistInfo(id, addSqlBlackListStmt.getSqlPattern().pattern()));
    }

    private void handleDelSqlBlackListStmt() {
        DelSqlBlackListStmt delSqlBlackListStmt = (DelSqlBlackListStmt) parsedStmt;
        List<Long> indexs = delSqlBlackListStmt.getIndexs();
        if (indexs != null) {
            for (long id : indexs) {
                GlobalStateMgr.getCurrentState().getSqlBlackList().delete(id);
            }
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logDeleteSQLBlackList(new DeleteSqlBlackLists(indexs));
        }
    }

    private void handleAddBackendBlackListStmt() throws StarRocksException {
        AddBackendBlackListStmt addBackendBlackListStmt = (AddBackendBlackListStmt) parsedStmt;
        Authorizer.check(addBackendBlackListStmt, context);
        for (Long beId : addBackendBlackListStmt.getBackendIds()) {
            SystemInfoService sis = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            if (sis.getBackend(beId) == null) {
                throw new StarRocksException("Not found backend: " + beId);
            }
            SimpleScheduler.getHostBlacklist().addByManual(beId);
        }
    }

    private void handleDelBackendBlackListStmt() {
        DelBackendBlackListStmt delBackendBlackListStmt = (DelBackendBlackListStmt) parsedStmt;
        Authorizer.check(delBackendBlackListStmt, context);
        for (Long backendId : delBackendBlackListStmt.getBackendIds()) {
            SimpleScheduler.getHostBlacklist().remove(backendId);
        }
    }

    private void handleExecAsStmt() throws StarRocksException {
        ExecuteAsExecutor.execute((ExecuteAsStmt) parsedStmt, context);
    }

    private void handleExecScriptStmt() throws IOException, StarRocksException {
        ShowResultSet resultSet = ExecuteScriptExecutor.execute((ExecuteScriptStmt) parsedStmt, context);
        if (isProxy) {
            proxyResultSet = resultSet;
            context.getState().setEof();
            return;
        }
        sendShowResult(resultSet);
    }

    private void handleSetRole() throws PrivilegeException, StarRocksException {
        SetRoleExecutor.execute((SetRoleStmt) parsedStmt, context);
    }

    private void handleSetDefaultRole() throws PrivilegeException, StarRocksException {
        SetDefaultRoleExecutor.execute((SetDefaultRoleStmt) parsedStmt, context);
    }

    private void handleUnsupportedStmt() {
        context.getMysqlChannel().reset();
        // do nothing
        context.getState().setOk();
    }

    // Process use [catalog].db statement.
    private void handleUseDbStmt() throws AnalysisException {
        UseDbStmt useDbStmt = (UseDbStmt) parsedStmt;
        try {
            context.changeCatalogDb(useDbStmt.getIdentifier());
        } catch (Exception e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    // Process use catalog statement
    private void handleUseCatalogStmt() {
        UseCatalogStmt useCatalogStmt = (UseCatalogStmt) parsedStmt;
        try {
            String catalogName = useCatalogStmt.getCatalogName();
            context.changeCatalog(catalogName);
        } catch (Exception e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private void handleSetCatalogStmt() throws AnalysisException {
        SetCatalogStmt setCatalogStmt = (SetCatalogStmt) parsedStmt;
        try {
            String catalogName = setCatalogStmt.getCatalogName();
            context.changeCatalog(catalogName);
        } catch (Exception e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    // Process use warehouse statement
    private void handleSetWarehouseStmt() throws StarRocksException {
        SetWarehouseStmt setWarehouseStmt = (SetWarehouseStmt) parsedStmt;
        try {
            WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            String newWarehouseName = setWarehouseStmt.getWarehouseName();
            if (!warehouseMgr.warehouseExists(newWarehouseName)) {
                throw new StarRocksException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, newWarehouseName);
            }
            context.setCurrentWarehouse(newWarehouseName);
        } catch (Exception e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private void handleTranslateStmt() throws IOException {
        ShowResultSet resultSet = TranslateExecutor.execute((TranslateStmt) parsedStmt);
        sendShowResult(resultSet);
    }

    private void sendMetaData(ShowResultSetMetaData metaData) throws IOException {
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(metaData.getColumnCount());
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (Column col : metaData.getColumns()) {
            serializer.reset();
            // TODO(zhaochun): only support varchar type
            serializer.writeField(col.getName(), col.getType());
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    private void sendFields(List<String> colNames, List<Expr> exprs) throws IOException {
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(colNames.size());
        if (isProxy) {
            proxyResultBuffer.add(serializer.toByteBuffer());
        } else {
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send field one by one
        for (int i = 0; i < colNames.size(); ++i) {
            serializer.reset();
            serializer.writeField(colNames.get(i), exprs.get(i).getOriginType());
            if (isProxy) {
                proxyResultBuffer.add(serializer.toByteBuffer());
            } else {
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        if (isProxy) {
            proxyResultBuffer.add(serializer.toByteBuffer());
        } else {
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
    }

    public void sendShowResult(ShowResultSet resultSet) throws IOException {
        context.updateReturnRows(resultSet.getResultRows().size());
        // Send result set for http.
        if (context instanceof HttpConnectContext) {
            httpResultSender.sendShowResult(resultSet);
            return;
        }

        // Send result set for Arrow Flight SQL.
        if (context instanceof ArrowFlightSqlConnectContext) {
            ((ArrowFlightSqlConnectContext) context).addShowResult(resultSet);
            context.getState().setEof();
            return;
        }

        // Send meta data.
        sendMetaData(resultSet.getMetaData());

        // Send result set.
        for (List<String> row : resultSet.getResultRows()) {
            serializer.reset();
            for (String item : row) {
                if (item == null || item.equals(FeConstants.NULL_STRING)) {
                    serializer.writeNull();
                } else {
                    serializer.writeLenEncodedString(item);
                }
            }
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }

        context.getState().setEof();
    }

    // Process show statement
    private void handleShow() throws IOException, AnalysisException, DdlException {
        ShowResultSet resultSet =
                GlobalStateMgr.getCurrentState().getShowExecutor().execute((ShowStmt) parsedStmt, context);
        if (resultSet == null) {
            // state changed in execute
            return;
        }
        if (isProxy) {
            proxyResultSet = resultSet;
            context.getState().setEof();
            return;
        }

        sendShowResult(resultSet);
    }

    private void handleExplainStmt(String explainString) throws IOException {
        if (context instanceof HttpConnectContext) {
            httpResultSender.sendExplainResult(explainString);
            return;
        }

        if (context.getQueryDetail() != null) {
            context.getQueryDetail().setExplain(explainString);
        }

        ShowResultSetMetaData metaData =
                ShowResultSetMetaData.builder()
                        .addColumn(new Column("Explain String", ScalarType.createVarchar(20)))
                        .build();
        sendMetaData(metaData);

        if (isProxy) {
            proxyResultSet = new ShowResultSet(metaData,
                    Arrays.stream(explainString.split("\n")).map(Collections::singletonList).collect(
                            Collectors.toList()));
        } else {
            // Send result set.
            for (String item : explainString.split("\n")) {
                serializer.reset();
                serializer.writeLenEncodedString(item);
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
        }
        context.getState().setEof();
    }

    public static String buildExplainString(ExecPlan execPlan, StatementBase parsedStmt, ConnectContext context,
                                            ResourceGroupClassifier.QueryType queryType,
                                            StatementBase.ExplainLevel explainLevel) {
        String explainString = "";
        if (parsedStmt.getExplainLevel() == StatementBase.ExplainLevel.VERBOSE) {
            TWorkGroup resourceGroup = CoordinatorPreprocessor.prepareResourceGroup(context, queryType);
            String resourceGroupStr =
                    resourceGroup != null ? resourceGroup.getName() : ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME;
            explainString += "RESOURCE GROUP: " + resourceGroupStr + "\n\n";
        }
        // marked delete will get execPlan null
        if (execPlan == null) {
            explainString += "NOT AVAILABLE";
        } else {
            if (parsedStmt.getTraceMode() == Tracers.Mode.TIMER) {
                explainString += Tracers.printScopeTimer();
            } else if (parsedStmt.getTraceMode() == Tracers.Mode.VARS) {
                explainString += Tracers.printVars();
            } else if (parsedStmt.getTraceMode() == Tracers.Mode.TIMING) {
                explainString += Tracers.printTiming();
            } else if (parsedStmt.getTraceMode() == Tracers.Mode.LOGS) {
                explainString += Tracers.printLogs();
            } else if (parsedStmt.getTraceMode() == Tracers.Mode.REASON) {
                explainString += Tracers.printReasons();
            } else {
                OperatorTuningGuides.OptimizedRecord optimizedRecord = PlanTuningAdvisor.getInstance()
                        .getOptimizedRecord(context.getQueryId());
                if (optimizedRecord != null) {
                    explainString += optimizedRecord.getExplainString();
                }
                explainString += execPlan.getExplainString(explainLevel);
            }
        }
        return explainString;
    }

    private void handleDdlStmt() throws DdlException {
        try {
            ShowResultSet resultSet = DDLStmtExecutor.execute(parsedStmt, context);
            if (resultSet == null) {
                context.getState().setOk();
            } else {
                if (isProxy) {
                    proxyResultSet = resultSet;
                    context.getState().setEof();
                } else {
                    sendShowResult(resultSet);
                }
            }
        } catch (QueryStateException e) {
            if (e.getQueryState().getStateType() != MysqlStateType.OK) {
                String sql = AstToStringBuilder.toString(parsedStmt);
                if (sql == null) {
                    sql = originStmt.originStmt;
                }
                LOG.warn("DDL statement (" + sql + ") process failed.", e);
            }
            context.setState(e.getQueryState());
        } catch (DdlException | AlterJobException e) {
            // let StmtExecutor.execute catch this exception
            // which will set error as ANALYSIS_ERR
            throw e;
        } catch (Throwable e) {
            // Maybe our bug or wrong input parameters
            String sql = AstToStringBuilder.toString(parsedStmt);
            if (sql == null || sql.isEmpty()) {
                sql = originStmt.originStmt;
            }
            LOG.warn("DDL statement (" + sql + ") process failed.", e);
            context.getState().setError(e.getMessage());
        }
    }

    private void handleExportStmt(UUID queryId) throws Exception {
        ExportStmt exportStmt = (ExportStmt) parsedStmt;
        exportStmt.setExportStartTime(context.getStartTime());
        context.getGlobalStateMgr().getExportMgr().addExportJob(queryId, exportStmt);

        if (!exportStmt.getSync()) {
            return;
        }
        // poll and check export job state
        int timeoutSeconds = Config.export_task_default_timeout_second + Config.export_checker_interval_second;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < (start + timeoutSeconds * 1000L)) {
            ExportJob exportJob = context.getGlobalStateMgr().getExportMgr().getExportByQueryId(queryId);
            if (exportJob != null) {
                timeoutSeconds = exportJob.getTimeoutSecond() + Config.export_checker_interval_second;
            }
            if (exportJob == null ||
                    (exportJob.getState() != ExportJob.JobState.FINISHED
                            && exportJob.getState() != ExportJob.JobState.CANCELLED)) {
                try {
                    Thread.sleep(Config.export_checker_interval_second * 1000L);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            break;
        }

        // proxy send show export result
        String db = exportStmt.getTblName().getDb();
        String showStmt = String.format("SHOW EXPORT FROM %s WHERE QueryId = \"%s\";", db, queryId);
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(showStmt, context.getSessionVariable()).get(0);
        ShowExportStmt showExportStmt = (ShowExportStmt) statementBase;
        showExportStmt.setQueryId(queryId);
        ShowResultSet resultSet = GlobalStateMgr.getCurrentState().getShowExecutor().execute(showExportStmt, context);
        if (resultSet == null) {
            // state changed in execute
            return;
        }
        if (isProxy) {
            proxyResultSet = resultSet;
            context.getState().setEof();
            return;
        }

        sendShowResult(resultSet);
    }

    private void handleUpdateFailPointStatusStmt() throws Exception {
        FailPointExecutor executor = new FailPointExecutor(context, parsedStmt);
        executor.execute();
    }

    private void handleDeallocateStmt() throws Exception {
        DeallocateStmt deallocateStmt = (DeallocateStmt) parsedStmt;
        String stmtName = deallocateStmt.getStmtName();
        if (context.getPreparedStmt(stmtName) == null) {
            throw new StarRocksException("PrepareStatement `" + stmtName + "` not exist");
        }
        context.removePreparedStmt(stmtName);
        context.getState().setOk();
    }

    private void handlePrepareStmt(ExecPlan execPlan) throws Exception {
        PrepareStmt prepareStmt = (PrepareStmt) parsedStmt;
        boolean isBinaryRowFormat = context.getCommand() == MysqlCommand.COM_STMT_PREPARE;
        // register prepareStmt
        LOG.debug("add prepared statement {}, isBinaryProtocol {}", prepareStmt.getName(), isBinaryRowFormat);
        context.putPreparedStmt(prepareStmt.getName(), new PrepareStmtContext(prepareStmt, context, execPlan));
        if (isBinaryRowFormat) {
            sendStmtPrepareOK(prepareStmt);
        }
    }

    private void sendStmtPrepareOK(PrepareStmt prepareStmt) throws IOException {
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response
        QueryStatement query = (QueryStatement) prepareStmt.getInnerStmt();
        int numColumns = query.getQueryRelation().getRelationFields().size();
        int numParams = prepareStmt.getParameters().size();
        serializer.reset();
        // 0x00 OK
        serializer.writeInt1(0);
        // statement_id
        serializer.writeInt4(Integer.valueOf(prepareStmt.getName()));
        // num_columns
        serializer.writeInt2(numColumns);
        // num_params
        serializer.writeInt2(numParams);
        // reserved_1
        serializer.writeInt1(0);
        // warning_count
        serializer.writeInt2(0);

        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());

        if (numParams > 0) {
            List<String> colNames = prepareStmt.getParameterLabels();
            List<Parameter> parameters = prepareStmt.getParameters();
            for (int i = 0; i < colNames.size(); ++i) {
                serializer.reset();
                serializer.writeField(colNames.get(i), parameters.get(i).getType());
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
            // send EOF
            serializer.reset();
            MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
            eofPacket.writeTo(serializer);
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }

        if (numColumns > 0) {
            for (Field field : query.getQueryRelation().getRelationFields().getAllFields()) {
                serializer.reset();
                serializer.writeField(field.getName(), field.getType());
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
            // send EOF
            serializer.reset();
            MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
            eofPacket.writeTo(serializer);
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }

        context.getMysqlChannel().flush();
        context.getState().setStateType(MysqlStateType.NOOP);
    }

    public void setQueryStatistics(PQueryStatistics statistics) {
        this.statisticsForAuditLog = statistics;
    }

    public PQueryStatistics getQueryStatisticsForAuditLog() {
        if (statisticsForAuditLog == null && coord != null) {
            statisticsForAuditLog = coord.getAuditStatistics();
        }
        if (statisticsForAuditLog == null) {
            statisticsForAuditLog = new PQueryStatistics();
        }
        if (statisticsForAuditLog.scanBytes == null) {
            statisticsForAuditLog.scanBytes = 0L;
        }
        if (statisticsForAuditLog.scanRows == null) {
            statisticsForAuditLog.scanRows = 0L;
        }
        if (statisticsForAuditLog.cpuCostNs == null) {
            statisticsForAuditLog.cpuCostNs = 0L;
        }
        if (statisticsForAuditLog.memCostBytes == null) {
            statisticsForAuditLog.memCostBytes = 0L;
        }
        if (statisticsForAuditLog.spillBytes == null) {
            statisticsForAuditLog.spillBytes = 0L;
        }
        return statisticsForAuditLog;
    }

    public void handleInsertOverwrite(InsertStmt insertStmt) throws Exception {
        TableName tableName = insertStmt.getTableName();
        Database db =
                GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(tableName.getCatalog(), tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tableName.getCatalogAndDb());
        }

        Locker locker = new Locker();
        Table table = insertStmt.getTargetTable();
        if (!(table instanceof OlapTable)) {
            LOG.warn("insert overwrite table:{} type:{} is not supported", table.getName(), table.getClass());
            throw new RuntimeException("not supported table type for insert overwrite");
        }
        OlapTable olapTable = (OlapTable) insertStmt.getTargetTable();
        InsertOverwriteJob job = new InsertOverwriteJob(GlobalStateMgr.getCurrentState().getNextId(),
                insertStmt, db.getId(), olapTable.getId(), context.getCurrentWarehouseId(),
                insertStmt.isDynamicOverwrite());
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
            throw new DmlException("database:%s does not exist.", db.getFullName());
        }
        try {
            // add an edit log
            CreateInsertOverwriteJobLog info = new CreateInsertOverwriteJobLog(job.getJobId(),
                    job.getTargetDbId(), job.getTargetTableId(), job.getSourcePartitionIds(),
                    job.isDynamicOverwrite());
            GlobalStateMgr.getCurrentState().getEditLog().logCreateInsertOverwrite(info);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
        insertStmt.setOverwriteJobId(job.getJobId());
        InsertOverwriteJobMgr manager = GlobalStateMgr.getCurrentState().getInsertOverwriteJobMgr();
        manager.executeJob(context, this, job);
    }

    /**
     * `handleDMLStmtWithProfile` executes DML statement and write profile at the end.
     * NOTE: `writeProfile` can only be called once, otherwise the profile detail will be lost.
     */
    public void handleDMLStmtWithProfile(ExecPlan execPlan, DmlStmt stmt) throws Exception {
        try {
            handleDMLStmt(execPlan, stmt);
        } catch (Throwable t) {
            LOG.warn("DML statement(" + originStmt.originStmt + ") process failed.", t);
            throw t;
        } finally {
            boolean isAsync = false;
            if (context.isProfileEnabled() || LoadErrorUtils.enableProfileAfterError(coord)) {
                isAsync = tryProcessProfileAsync(execPlan, 0);
                if (parsedStmt.isExplain() &&
                        StatementBase.ExplainLevel.ANALYZE.equals(parsedStmt.getExplainLevel())) {
                    handleExplainStmt(ExplainAnalyzer.analyze(ProfilingExecPlan.buildFrom(execPlan),
                            profile, null, context.getSessionVariable().getColorExplainOutput()));
                }
            }
            if (isAsync) {
                QeProcessorImpl.INSTANCE.monitorQuery(context.getExecutionId(), System.currentTimeMillis() +
                        context.getSessionVariable().getProfileTimeout() * 1000L);
            } else {
                QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
            }
        }
    }

    /**
     * `handleDMLStmt` only executes DML statement and no write profile at the end.
     */
    public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
        boolean isExplainAnalyze = parsedStmt.isExplain()
                && StatementBase.ExplainLevel.ANALYZE.equals(parsedStmt.getExplainLevel());
        boolean isSchedulerExplain = parsedStmt.isExplain()
                && StatementBase.ExplainLevel.SCHEDULER.equals(parsedStmt.getExplainLevel());

        if (isExplainAnalyze) {
            context.getSessionVariable().setEnableProfile(true);
            context.getSessionVariable().setEnableAsyncProfile(false);
            context.getSessionVariable().setPipelineProfileLevel(1);
        } else if (isSchedulerExplain) {
            // Do nothing.
        } else if (stmt.isExplain()) {
            handleExplainStmt(buildExplainString(execPlan, parsedStmt, context, ResourceGroupClassifier.QueryType.INSERT,
                    parsedStmt.getExplainLevel()));
            return;
        }

        // special handling for delete of non-primary key table, using old handler
        if (stmt instanceof DeleteStmt && ((DeleteStmt) stmt).shouldHandledByDeleteHandler()) {
            try {
                context.getGlobalStateMgr().getDeleteMgr().process((DeleteStmt) stmt);
                context.getState().setOk();
            } catch (QueryStateException e) {
                if (e.getQueryState().getStateType() != MysqlStateType.OK) {
                    LOG.warn("DDL statement(" + originStmt.originStmt + ") process failed.", e);
                }
                context.setState(e.getQueryState());
            }
            return;
        }

        DmlType dmlType = DmlType.fromStmt(stmt);
        stmt.getTableName().normalization(context);
        String catalogName = stmt.getTableName().getCatalog();
        String dbName = stmt.getTableName().getDb();
        String tableName = stmt.getTableName().getTbl();
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        final Table targetTable;
        if (stmt instanceof InsertStmt && ((InsertStmt) stmt).getTargetTable() != null) {
            targetTable = ((InsertStmt) stmt).getTargetTable();
        } else {
            targetTable = MetaUtils.getSessionAwareTable(context, database, stmt.getTableName());
        }

        if (isExplainAnalyze) {
            Preconditions.checkState(targetTable instanceof OlapTable,
                    "explain analyze only supports insert into olap native table");
        }

        if (dmlType == DmlType.INSERT_OVERWRITE && !((InsertStmt) parsedStmt).hasOverwriteJob() &&
                !(targetTable.isIcebergTable() || targetTable.isHiveTable())) {
            handleInsertOverwrite((InsertStmt) parsedStmt);
            return;
        }

        MetricRepo.COUNTER_LOAD_ADD.increase(1L);

        if (context.getExplicitTxnState() != null) {
            TransactionStmtExecutor.loadData(database, targetTable, execPlan, stmt, originStmt, context);
            return;
        }

        long transactionId = stmt.getTxnId();
        TransactionState txnState = null;
        String label = DebugUtil.printId(context.getExecutionId());
        if (targetTable instanceof ExternalOlapTable) {
            Preconditions.checkState(stmt instanceof InsertStmt,
                    "External OLAP table only supports insert statement");
            String stmtLabel = ((InsertStmt) stmt).getLabel();
            label = Strings.isNullOrEmpty(stmtLabel) ? MetaUtils.genInsertLabel(context.getExecutionId()) : stmtLabel;
        } else if (targetTable instanceof OlapTable) {
            txnState = transactionMgr.getTransactionState(database.getId(), transactionId);
            if (txnState == null) {
                throw new DdlException("txn does not exist: " + transactionId);
            }
            label = txnState.getLabel();
        }
        // Every time set no send flag and clean all data in buffer
        if (context.getMysqlChannel() != null) {
            context.getMysqlChannel().reset();
        }
        long createTime = System.currentTimeMillis();

        long loadedRows = 0;
        // filteredRows is stored in int64_t in the backend, so use long here.
        long filteredRows = 0;
        long loadedBytes = 0;
        long jobId = -1;
        long estimateScanRows = -1;
        int estimateFileNum = 0;
        long estimateScanFileSize = 0;
        TransactionStatus txnStatus = TransactionStatus.ABORTED;
        boolean insertError = false;
        String trackingSql = "";

        try {
            coord = getCoordinatorFactory().createInsertScheduler(
                    context, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift());

            List<ScanNode> scanNodes = execPlan.getScanNodes();

            boolean needQuery = false;
            for (ScanNode scanNode : scanNodes) {
                if (scanNode instanceof OlapScanNode) {
                    estimateScanRows += ((OlapScanNode) scanNode).getActualRows();
                    needQuery = true;
                }
                if (scanNode instanceof FileScanNode) {
                    estimateFileNum += ((FileScanNode) scanNode).getFileNum();
                    estimateScanFileSize += ((FileScanNode) scanNode).getFileTotalSize();
                    needQuery = true;
                }
            }

            if (needQuery) {
                coord.setLoadJobType(TLoadJobType.INSERT_QUERY);
            } else {
                estimateScanRows = execPlan.getFragments().get(0).getPlanRoot().getCardinality();
                coord.setLoadJobType(TLoadJobType.INSERT_VALUES);
            }

            context.setStatisticsJob(AnalyzerUtils.isStatisticsJob(context, parsedStmt));
            InsertLoadJob loadJob = null;
            if (!(targetTable.isIcebergTable() || targetTable.isHiveTable() || targetTable.isTableFunctionTable() ||
                    targetTable.isBlackHoleTable())) {
                // insert, update and delete job
                loadJob = context.getGlobalStateMgr().getLoadMgr().registerInsertLoadJob(
                        label,
                        database.getFullName(),
                        targetTable.getId(),
                        transactionId,
                        DebugUtil.printId(context.getExecutionId()),
                        context.getQualifiedUser(),
                        EtlJobType.INSERT,
                        createTime,
                        new LoadMgr.EstimateStats(estimateScanRows, estimateFileNum, estimateScanFileSize),
                        getExecTimeout(),
                        context.getCurrentWarehouseId(),
                        coord);
                loadJob.setJobProperties(stmt.getProperties());
                jobId = loadJob.getId();
                if (txnState != null) {
                    txnState.addCallbackId(jobId);
                }
            }

            coord.setLoadJobId(jobId);
            trackingSql = "select tracking_log from information_schema.load_tracking_logs where job_id=" + jobId;

            QeProcessorImpl.QueryInfo queryInfo = new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord);
            QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(), queryInfo);

            if (isSchedulerExplain) {
                coord.execWithoutDeploy();
                handleExplainStmt(coord.getSchedulerExplain());
                return;
            }

            coord.exec();
            coord.setTopProfileSupplier(this::buildTopLevelProfile);
            coord.setExecPlan(execPlan);

            int timeout = getExecTimeout();
            long jobDeadLineMs = System.currentTimeMillis() + timeout * 1000;
            coord.join(timeout);
            if (!coord.isDone()) {
                /*
                 * In this case, There are two factors that lead query cancelled:
                 * 1: TIMEOUT
                 * 2: BE EXCEPTION
                 * So we should distinguish these two factors.
                 */
                if (!coord.checkBackendState()) {
                    // When enable_collect_query_detail_info is set to true, the plan will be recorded in the query detail,
                    // and hence there is no need to log it here.
                    if (Config.log_plan_cancelled_by_crash_be && context.getQueryDetail() == null) {
                        LOG.warn("Query cancelled by crash of backends [QueryId={}] [SQL={}] [Plan={}]",
                                DebugUtil.printId(context.getExecutionId()),
                                originStmt == null ? "" : originStmt.originStmt,
                                execPlan.getExplainString(TExplainLevel.COSTS));
                    }

                    coord.cancel(ErrorCode.ERR_QUERY_CANCELLED_BY_CRASH.formatErrorMsg());
                    ErrorReport.reportNoAliveBackendException(ErrorCode.ERR_QUERY_CANCELLED_BY_CRASH);
                } else {
                    coord.cancel(ErrorCode.ERR_TIMEOUT.formatErrorMsg(getExecType(), timeout, ""));
                    if (coord.isThriftServerHighLoad()) {
                        ErrorReport.reportTimeoutException(ErrorCode.ERR_TIMEOUT, getExecType(), timeout,
                                "Please check the thrift-server-pool metrics, " +
                                        "if the pool size reaches thrift_server_max_worker_threads(default is 4096), " +
                                        "you can set the config to a higher value in fe.conf, " +
                                        "or set parallel_fragment_exec_instance_num to a lower value in session variable");
                    } else {
                        ErrorReport.reportTimeoutException(ErrorCode.ERR_TIMEOUT, getExecType(), timeout,
                                String.format("please increase the '%s' session variable or the '%s' property for " +
                                                "insert statement and retry",
                                        SessionVariable.INSERT_TIMEOUT, LoadStmt.TIMEOUT_PROPERTY));
                    }
                }
            }

            if (!coord.getExecStatus().ok()) {
                String errMsg = coord.getExecStatus().getErrorMsg();
                if (errMsg.length() == 0) {
                    errMsg = coord.getExecStatus().getErrorCodeString();
                }
                LOG.warn("insert failed: {}", errMsg);
                ErrorReport.reportDdlException("%s", ErrorCode.ERR_FAILED_WHEN_INSERT, errMsg);
            }

            LOG.debug("delta files is {}", coord.getDeltaUrls());

            if (coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL) != null) {
                loadedRows = Long.parseLong(coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL));
            }
            if (coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL) != null) {
                filteredRows = Long.parseLong(coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL));
            }

            if (coord.getLoadCounters().get(LoadJob.LOADED_BYTES) != null) {
                loadedBytes = Long.parseLong(coord.getLoadCounters().get(LoadJob.LOADED_BYTES));
            }

            if (loadJob != null) {
                loadJob.updateLoadingStatus(coord.getLoadCounters());
            }

            // insert will fail if 'filtered rows / total rows' exceeds max_filter_ratio
            // for native table and external catalog table(without insert load job)
            if (filteredRows > (filteredRows + loadedRows) * stmt.getMaxFilterRatio()) {
                if (targetTable instanceof ExternalOlapTable) {
                    ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
                    RemoteTransactionMgr.abortRemoteTransaction(externalTable.getSourceTableDbId(), transactionId,
                            externalTable.getSourceTableHost(), externalTable.getSourceTablePort(),
                            TransactionCommitFailedException.FILTER_DATA_ERR + ", tracking sql = " + trackingSql,
                            coord == null ? Collections.emptyList() : coord.getCommitInfos(),
                            coord == null ? Collections.emptyList() : coord.getFailInfos());
                } else if (targetTable instanceof SystemTable || targetTable.isHiveTable() ||
                        targetTable.isIcebergTable() ||
                        targetTable.isTableFunctionTable() || targetTable.isBlackHoleTable()) {
                    // schema table does not need txn
                } else {
                    transactionMgr.abortTransaction(database.getId(), transactionId,
                            TransactionCommitFailedException.FILTER_DATA_ERR + ", tracking sql = " + trackingSql,
                            Coordinator.getCommitInfos(coord), Coordinator.getFailInfos(coord), null);
                }
                context.getState().setError(
                        TransactionCommitFailedException.FILTER_DATA_ERR + ", txn_id = " + transactionId +
                                ", tracking sql = " + trackingSql);
                insertError = true;
                return;
            }

            if (loadedRows == 0 && filteredRows == 0 && (stmt instanceof DeleteStmt || stmt instanceof InsertStmt
                    || stmt instanceof UpdateStmt)) {
                // when the target table is not ExternalOlapTable or OlapTable
                // if there is no data to load, the result of the insert statement is success
                // otherwise, the result of the insert statement is failed
                GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
                if (!(targetTable instanceof ExternalOlapTable || targetTable instanceof OlapTable)) {
                    if (!(targetTable instanceof SystemTable || targetTable.isIcebergTable() ||
                            targetTable.isHiveTable() || targetTable.isTableFunctionTable() ||
                            targetTable.isBlackHoleTable())) {
                        // schema table and iceberg table does not need txn
                        mgr.abortTransaction(database.getId(), transactionId,
                                ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg(),
                                Coordinator.getCommitInfos(coord), Coordinator.getFailInfos(coord), null);
                    }
                    context.getState().setOk();
                    insertError = true;
                    return;
                }
            }

            if (targetTable instanceof ExternalOlapTable) {
                ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
                if (RemoteTransactionMgr.commitRemoteTransaction(
                        externalTable.getSourceTableDbId(), transactionId,
                        externalTable.getSourceTableHost(),
                        externalTable.getSourceTablePort(),
                        coord.getCommitInfos())) {
                    txnStatus = TransactionStatus.VISIBLE;
                    MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
                } else {
                    txnStatus = TransactionStatus.COMMITTED;
                }
            } else if (targetTable instanceof SystemTable) {
                // schema table does not need txn
                txnStatus = TransactionStatus.VISIBLE;
            } else if (targetTable.isIcebergTable()) {
                // TODO(stephen): support abort interface and delete data files when aborting.
                List<TSinkCommitInfo> commitInfos = coord.getSinkCommitInfos();
                if (stmt instanceof InsertStmt && ((InsertStmt) stmt).isOverwrite()) {
                    for (TSinkCommitInfo commitInfo : commitInfos) {
                        commitInfo.setIs_overwrite(true);
                    }
                }

                IcebergTableSink sink = (IcebergTableSink) execPlan.getFragments().get(0).getSink();
                context.getGlobalStateMgr().getMetadataMgr().finishSink(
                        catalogName, dbName, tableName, commitInfos, sink.getTargetBranch());
                txnStatus = TransactionStatus.VISIBLE;
                label = "FAKE_ICEBERG_SINK_LABEL";
            } else if (targetTable.isHiveTable()) {
                List<TSinkCommitInfo> commitInfos = coord.getSinkCommitInfos();
                HiveTableSink hiveTableSink = (HiveTableSink) execPlan.getFragments().get(0).getSink();
                String stagingDir = hiveTableSink.getStagingDir();
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    for (TSinkCommitInfo commitInfo : commitInfos) {
                        commitInfo.setStaging_dir(stagingDir);
                        if (insertStmt.isOverwrite()) {
                            commitInfo.setIs_overwrite(true);
                        }
                    }
                }
                context.getGlobalStateMgr().getMetadataMgr()
                        .finishSink(catalogName, dbName, tableName, commitInfos, null);
                txnStatus = TransactionStatus.VISIBLE;
                label = "FAKE_HIVE_SINK_LABEL";
            } else if (targetTable.isTableFunctionTable()) {
                txnStatus = TransactionStatus.VISIBLE;
                label = "FAKE_TABLE_FUNCTION_TABLE_SINK_LABEL";
            } else if (targetTable.isBlackHoleTable()) {
                txnStatus = TransactionStatus.VISIBLE;
                label = "FAKE_BLACKHOLE_TABLE_SINK_LABEL";
            } else if (isExplainAnalyze) {
                transactionMgr.abortTransaction(database.getId(), transactionId, "Explain Analyze",
                        Coordinator.getCommitInfos(coord), Coordinator.getFailInfos(coord), null);
                txnStatus = TransactionStatus.ABORTED;
            } else {
                InsertTxnCommitAttachment attachment = null;
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    if (insertStmt.isVersionOverwrite()) {
                        Map<Long, Long> partitionVersionMap = Maps.newHashMap();
                        Map<PlanNodeId, ScanNode> scanNodeMap = execPlan.getFragments().get(0).collectScanNodes();
                        for (Map.Entry<PlanNodeId, ScanNode> entry : scanNodeMap.entrySet()) {
                            if (entry.getValue() instanceof OlapScanNode) {
                                OlapScanNode olapScanNode = (OlapScanNode) entry.getValue();
                                partitionVersionMap.putAll(olapScanNode.getScanPartitionVersions());
                            }
                        }
                        Preconditions.checkState(partitionVersionMap.size() <= 1);
                        if (partitionVersionMap.size() == 1) {
                            attachment = new InsertTxnCommitAttachment(
                                    loadedRows, partitionVersionMap.values().iterator().next());
                        } else if (partitionVersionMap.size() == 0) {
                            attachment = new InsertTxnCommitAttachment(loadedRows);
                        }
                        LOG.debug("insert overwrite txn {} with partition version map {}", transactionId,
                                partitionVersionMap);
                    } else {
                        attachment = new InsertTxnCommitAttachment(loadedRows);
                    }
                } else {
                    attachment = new InsertTxnCommitAttachment(loadedRows);
                }
                VisibleStateWaiter visibleWaiter = transactionMgr.retryCommitOnRateLimitExceeded(
                        database,
                        transactionId,
                        TabletCommitInfo.fromThrift(coord.getCommitInfos()),
                        TabletFailInfo.fromThrift(coord.getFailInfos()),
                        attachment,
                        jobDeadLineMs - System.currentTimeMillis());

                long publishWaitMs = Config.enable_sync_publish ? jobDeadLineMs - System.currentTimeMillis() :
                        context.getSessionVariable().getTransactionVisibleWaitTimeout() * 1000;

                if (visibleWaiter.await(publishWaitMs, TimeUnit.MILLISECONDS)) {
                    txnStatus = TransactionStatus.VISIBLE;
                    MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
                    // collect table-level metrics
                    TableMetricsEntity entity =
                            TableMetricsRegistry.getInstance().getMetricsEntity(targetTable.getId());
                    entity.counterInsertLoadFinishedTotal.increase(1L);
                    entity.counterInsertLoadRowsTotal.increase(loadedRows);
                    entity.counterInsertLoadBytesTotal.increase(loadedBytes);
                } else {
                    txnStatus = TransactionStatus.COMMITTED;
                }
            }
        } catch (Throwable t) {
            // if any throwable being thrown during insert operation, first we should abort this txn
            String failedSql = "";
            if (originStmt != null && originStmt.originStmt != null) {
                failedSql = originStmt.originStmt;
            }
            LOG.warn("failed to handle stmt [{}] label: {}", failedSql, label, t);
            String errMsg = t.getMessage();
            if (errMsg == null) {
                errMsg = "A problem occurred while executing the [ " + failedSql + "] statement with label:" + label;
            }

            try {
                if (targetTable instanceof ExternalOlapTable) {
                    ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
                    RemoteTransactionMgr.abortRemoteTransaction(
                            externalTable.getSourceTableDbId(), transactionId,
                            externalTable.getSourceTableHost(),
                            externalTable.getSourceTablePort(),
                            errMsg,
                            coord == null ? Collections.emptyList() : coord.getCommitInfos(),
                            coord == null ? Collections.emptyList() : coord.getFailInfos());
                } else if (targetTable.isExternalTableWithFileSystem()) {
                    GlobalStateMgr.getCurrentState().getMetadataMgr().abortSink(
                            catalogName, dbName, tableName, coord.getSinkCommitInfos());
                } else if (targetTable.isBlackHoleTable()) {
                    // black hole table does not need txn
                } else {
                    transactionMgr.abortTransaction(database.getId(), transactionId, errMsg,
                            Coordinator.getCommitInfos(coord), Coordinator.getFailInfos(coord), null);
                }
            } catch (Exception abortTxnException) {
                // just print a log if abort txn failed. This failure do not need to pass to user.
                // user only concern abort how txn failed.
                LOG.warn("errors when abort txn", abortTxnException);
            }

            // if not using old load usage pattern, error will be returned directly to user
            StringBuilder sb = new StringBuilder(errMsg);
            if (coord != null && !Strings.isNullOrEmpty(coord.getTrackingUrl())) {
                sb.append(". tracking sql: ").append(trackingSql);
            }
            context.getState().setError(sb.toString());

            // cancel insert load job
            try {
                if (jobId != -1) {
                    Preconditions.checkNotNull(coord);
                    context.getGlobalStateMgr().getLoadMgr()
                            .recordFinishedOrCancelledLoadJob(jobId, EtlJobType.INSERT,
                                    "Cancelled, msg: " + t.getMessage(), coord.getTrackingUrl());
                    jobId = -1;
                }
            } catch (Exception abortTxnException) {
                LOG.warn("errors when cancel insert load job {}", jobId);
            }
            throw new StarRocksException(t.getMessage(), t);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
            if (insertError) {
                try {
                    if (jobId != -1) {
                        context.getGlobalStateMgr().getLoadMgr()
                                .recordFinishedOrCancelledLoadJob(jobId, EtlJobType.INSERT,
                                        "Cancelled", coord.getTrackingUrl());
                        jobId = -1;
                    }
                } catch (Exception abortTxnException) {
                    LOG.warn("errors when cancel insert load job {}", jobId);
                }
            } else if (txnState != null) {
                GlobalStateMgr.getCurrentState().getOperationListenerBus()
                        .onDMLStmtJobTransactionFinish(txnState, database, targetTable, dmlType);
            }
            recordExecStatsIntoContext();
        }

        String errMsg = "";
        if (txnStatus.equals(TransactionStatus.COMMITTED)) {
            String timeoutInfo = transactionMgr.getTxnPublishTimeoutDebugInfo(database.getId(), transactionId);
            LOG.warn("txn {} publish timeout {}", transactionId, timeoutInfo);
            if (timeoutInfo.length() > 240) {
                timeoutInfo = timeoutInfo.substring(0, 240) + "...";
            }
            errMsg = "Publish timeout " + timeoutInfo;
        }
        try {
            if (jobId != -1) {
                context.getGlobalStateMgr().getLoadMgr().recordFinishedOrCancelledLoadJob(jobId,
                        EtlJobType.INSERT,
                        "",
                        coord.getTrackingUrl());
            }
        } catch (StarRocksException e) {
            LOG.warn("Record info of insert load with error {}", e.getMessage(), e);
            errMsg = "Record info of insert load with error " + e.getMessage();
        }

        StringBuilder sb = new StringBuilder();
        if (!label.startsWith("FAKE")) {
            sb.append("{'label':'").append(label).append("', 'status':'").append(txnStatus.name());
            sb.append("', 'txnId':'").append(transactionId).append("'");
            if (!Strings.isNullOrEmpty(errMsg)) {
                sb.append(", 'err':'").append(errMsg).append("'");
            }
            sb.append("}");
        }

        // filterRows may be overflow when to convert it into int, use `saturatedCast` to avoid overflow
        context.getState().setOk(loadedRows, Ints.saturatedCast(filteredRows), sb.toString());
    }

    private void handlePlanAdvisorStmt() throws IOException {
        ShowResultSet resultSet = PlanAdvisorExecutor.execute(parsedStmt, context);
        if (resultSet != null) {
            sendShowResult(resultSet);
        }
    }

    public String getOriginStmtInString() {
        if (originStmt == null) {
            return "";
        }
        return originStmt.originStmt;
    }

    public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan) {
        List<TResultBatch> sqlResult = Lists.newArrayList();
        try {
            UUID uuid = context.getQueryId();
            context.setExecutionId(UUIDUtil.toTUniqueId(uuid));

            coord = getCoordinatorFactory().createQueryScheduler(
                    context, plan.getFragments(), plan.getScanNodes(), plan.getDescTbl().toThrift());
            QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(), coord);

            coord.exec();
            RowBatch batch;
            do {
                batch = coord.getNext();
                if (batch.getBatch() != null) {
                    sqlResult.add(batch.getBatch());
                }
            } while (!batch.isEos());
            processQueryStatisticsFromResult(batch, plan, false);
        } catch (Exception e) {
            LOG.warn("Failed to execute executeStmtWithExecPlan", e);
            coord.getExecStatus().setInternalErrorStatus(e.getMessage());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
            recordExecStatsIntoContext();
        }
        return Pair.create(sqlResult, coord.getExecStatus());
    }

    public List<ByteBuffer> getProxyResultBuffer() {
        return proxyResultBuffer;
    }

    // scenes can execute in FE should meet all these requirements:
    // 1. enable_constant_execute_in_fe = true
    // 2. is mysql text protocol
    // 3. all values are constantOperator
    private boolean canExecuteInFe(ConnectContext context, OptExpression optExpression) {
        if (!context.getSessionVariable().isEnableConstantExecuteInFE()) {
            return false;
        }

        if (context instanceof HttpConnectContext || context.getCommand() == MysqlCommand.COM_STMT_EXECUTE) {
            return false;
        }

        if (optExpression.getOp() instanceof PhysicalValuesOperator) {
            PhysicalValuesOperator valuesOperator = (PhysicalValuesOperator) optExpression.getOp();
            boolean isAllConstants = true;
            if (valuesOperator.getProjection() != null) {
                isAllConstants = valuesOperator.getProjection().getColumnRefMap().values().stream()
                        .allMatch(ScalarOperator::isConstantRef);
            } else if (CollectionUtils.isNotEmpty(valuesOperator.getRows())) {
                isAllConstants = valuesOperator.getRows().stream().allMatch(row ->
                        row.stream().allMatch(ScalarOperator::isConstantRef));
            }

            return isAllConstants;
        }
        return false;
    }

    public void executeStmtWithResultQueue(ConnectContext context, ExecPlan plan, Queue<TResultBatch> sqlResult) {
        try {
            UUID uuid = context.getQueryId();
            context.setExecutionId(UUIDUtil.toTUniqueId(uuid));
            coord = getCoordinatorFactory().createQueryScheduler(
                    context, plan.getFragments(), plan.getScanNodes(), plan.getDescTbl().toThrift());
            QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(), coord);

            coord.exec();
            RowBatch batch;
            do {
                batch = coord.getNext();
                if (batch.getBatch() != null) {
                    sqlResult.add(batch.getBatch());
                }
            } while (!batch.isEos());
            context.getState().setEof();
            processQueryStatisticsFromResult(batch, plan, false);
        } catch (Exception e) {
            LOG.error("Failed to execute metadata collection job", e);
            if (coord.getExecStatus().ok()) {
                context.getState().setError(e.getMessage());
            }
            coord.getExecStatus().setInternalErrorStatus(e.getMessage());
        } finally {
            if (context.isProfileEnabled()) {
                tryProcessProfileAsync(plan, 0);
                QeProcessorImpl.INSTANCE.monitorQuery(context.getExecutionId(), System.currentTimeMillis() +
                        context.getSessionVariable().getProfileTimeout() * 1000L);
            } else {
                QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
            }
            recordExecStatsIntoContext();
        }
    }

    /**
     * Record this sql into the query details, which would be collected by external query history system
     */
    public void addRunningQueryDetail(StatementBase parsedStmt) {
        if (!Config.enable_collect_query_detail_info) {
            return;
        }
        String sql;
        if (AuditEncryptionChecker.needEncrypt(parsedStmt)) {
            sql = AstToSQLBuilder.toSQLOrDefault(parsedStmt, parsedStmt.getOrigStmt().originStmt);
        } else {
            sql = parsedStmt.getOrigStmt().originStmt;
        }

        boolean isQuery = context.isQueryStmt(parsedStmt);
        QueryDetail queryDetail = new QueryDetail(
                DebugUtil.printId(context.getQueryId()),
                isQuery,
                context.connectionId,
                context.getMysqlChannel() != null ? context.getMysqlChannel().getRemoteIp() : "System",
                context.getStartTime(), -1, -1,
                QueryDetail.QueryMemState.RUNNING,
                context.getDatabase(),
                sql,
                context.getQualifiedUser(),
                Optional.ofNullable(context.getResourceGroup()).map(TWorkGroup::getName).orElse(""),
                context.getCurrentWarehouseName(),
                context.getCurrentCatalog());
        context.setQueryDetail(queryDetail);
        // copy queryDetail, cause some properties can be changed in future
        QueryDetailQueue.addQueryDetail(queryDetail.copy());
    }

    /*
     * Record this finished sql into the query details, which would be collected by external query history system
     */
    public void addFinishedQueryDetail() {
        if (!Config.enable_collect_query_detail_info) {
            return;
        }
        ConnectContext ctx = context;
        QueryDetail queryDetail = ctx.getQueryDetail();
        if (queryDetail == null || !queryDetail.getQueryId().equals(DebugUtil.printId(ctx.getQueryId()))) {
            return;
        }

        long endTime = System.currentTimeMillis();
        long elapseMs = endTime - ctx.getStartTime();

        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            queryDetail.setState(QueryDetail.QueryMemState.FAILED);
            queryDetail.setErrorMessage(ctx.getState().getErrorMessage());
        } else {
            queryDetail.setState(QueryDetail.QueryMemState.FINISHED);
        }
        queryDetail.setEndTime(endTime);
        queryDetail.setLatency(elapseMs);
        long pendingTime = ctx.getAuditEventBuilder().build().pendingTimeMs;
        pendingTime = pendingTime < 0 ? 0 : pendingTime;
        queryDetail.setPendingTime(pendingTime);
        queryDetail.setNetTime(elapseMs - pendingTime);
        long parseTime = Tracers.getSpecifiedTimer("Parser").map(Timer::getTotalTime).orElse(0L);
        long planTime = Tracers.getSpecifiedTimer("Total").map(Timer::getTotalTime).orElse(0L);
        long prepareTime = Tracers.getSpecifiedTimer("Prepare").map(Timer::getTotalTime).orElse(0L);
        long deployTime = Tracers.getSpecifiedTimer("Deploy").map(Timer::getTotalTime).orElse(0L);
        queryDetail.setNetComputeTime(elapseMs - parseTime - planTime - prepareTime - pendingTime - deployTime);
        queryDetail.setResourceGroupName(ctx.getResourceGroup() != null ? ctx.getResourceGroup().getName() : "");
        // add execution statistics into queryDetail
        queryDetail.setReturnRows(ctx.getReturnRows());
        queryDetail.setDigest(ctx.getAuditEventBuilder().build().digest);
        PQueryStatistics statistics = getQueryStatisticsForAuditLog();
        if (statistics != null) {
            queryDetail.setScanBytes(statistics.scanBytes);
            queryDetail.setScanRows(statistics.scanRows);
            queryDetail.setCpuCostNs(statistics.cpuCostNs == null ? -1 : statistics.cpuCostNs);
            queryDetail.setMemCostBytes(statistics.memCostBytes == null ? -1 : statistics.memCostBytes);
            queryDetail.setSpillBytes(statistics.spillBytes == null ? -1 : statistics.spillBytes);
        }
        queryDetail.setCatalog(ctx.getCurrentCatalog());

        QueryDetailQueue.addQueryDetail(queryDetail);
    }

    private boolean shouldMarkIdleCheck(StatementBase parsedStmt) {
        return !isInternalStmt
                && !(parsedStmt instanceof ShowStmt)
                && !(parsedStmt instanceof AdminSetConfigStmt);
    }
}
