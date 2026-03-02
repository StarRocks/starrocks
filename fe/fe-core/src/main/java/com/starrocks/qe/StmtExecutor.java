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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;
import com.starrocks.alter.AlterJobException;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.cluster.ClusterNamespace;
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
import com.starrocks.common.profile.RawScopedTimer;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.ProfilingExecPlan;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.RuntimeProfileParser;
import com.starrocks.common.util.SqlCredentialRedactor;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.failpoint.FailPointExecutor;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.HttpResultSender;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.ExportJob;
import com.starrocks.load.InsertOverwriteJob;
import com.starrocks.load.InsertOverwriteJobMgr;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.load.loadv2.InsertLoadTxnCallback;
import com.starrocks.load.loadv2.InsertLoadTxnCallbackFactory;
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
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.FileScanNode;
import com.starrocks.planner.HiveTableSink;
import com.starrocks.planner.IcebergDeleteSink;
import com.starrocks.planner.IcebergMetadataDeleteNode;
import com.starrocks.planner.IcebergScanNode;
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
import com.starrocks.qe.recursivecte.RecursiveCTEAstCheck;
import com.starrocks.qe.recursivecte.RecursiveCTEExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.FeExecuteCoordinator;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.GracefulExitFlag;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlResultDescriptor;
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
import com.starrocks.sql.analyzer.ShowStmtToSelectStmtConverter;
import com.starrocks.sql.ast.AddBackendBlackListStmt;
import com.starrocks.sql.ast.AddComputeNodeBlackListStmt;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.AddSqlDigestBlackListStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AnalyzeProfileStmt;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AnalyzeTypeDesc;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTemporaryTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DeallocateStmt;
import com.starrocks.sql.ast.DelBackendBlackListStmt;
import com.starrocks.sql.ast.DelComputeNodeBlackListStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.DelSqlDigestBlackListStmt;
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
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.IcebergRewriteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshConnectionsStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.UnsupportedStmt;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.Parameter;
import com.starrocks.sql.ast.expression.SetVarHint;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.ast.expression.UserVariableHint;
import com.starrocks.sql.ast.feedback.PlanAdvisorStmt;
import com.starrocks.sql.ast.translate.TranslateStmt;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.common.AuditEncryptionChecker;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.formatter.FormatOptions;
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
import com.starrocks.statistic.CancelableAnalyzeTask;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.ExternalHistogramStatisticsCollectJob;
import com.starrocks.statistic.HistogramStatisticsCollectJob;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatisticsCollectJobFactory;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.ComputeNode;
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
import com.starrocks.type.TypeFactory;
import com.starrocks.warehouse.WarehouseIdleChecker;
import org.apache.arrow.vector.types.pojo.Schema;
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
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static com.starrocks.common.ErrorCode.ERR_NO_ROWS_IMPORTED;
import static com.starrocks.service.arrow.flight.sql.ArrowFlightSqlServiceImpl.buildSchema;
import static com.starrocks.sql.parser.ErrorMsgProxy.PARSER_ERROR_MSG;
import static com.starrocks.statistic.AnalyzeMgr.IS_MULTI_COLUMN_STATS;

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
    private PrepareStmtContext prepareStmtContext = null;
    private boolean isInternalStmt = false;
    
    // Store table query timeout info for error message
    private String tableQueryTimeoutTableName = null;
    private int tableQueryTimeoutValue = -1;

    private final CompletableFuture<ArrowFlightSqlResultDescriptor> deploymentFinished;

    public StmtExecutor(ConnectContext ctx, StatementBase parsedStmt) {
        this(ctx, parsedStmt, false);
    }

    public StmtExecutor(ConnectContext ctx, StatementBase parsedStmt,
                        CompletableFuture<ArrowFlightSqlResultDescriptor> deploymentFinished) {
        this(ctx, parsedStmt, false, deploymentFinished);
    }

    public static StmtExecutor newInternalExecutor(ConnectContext ctx, StatementBase parsedStmt) {
        // Set query source to INTERNAL for system/internal queries only if not already set
        if (ctx.getQuerySource().equals(QueryDetail.QuerySource.EXTERNAL)) {
            ctx.setQuerySource(QueryDetail.QuerySource.INTERNAL);
        }
        return new StmtExecutor(ctx, parsedStmt, true);
    }

    private StmtExecutor(ConnectContext ctx, StatementBase parsedStmt, boolean isInternalStmt) {
        this(ctx, parsedStmt, isInternalStmt, null);
    }

    private StmtExecutor(ConnectContext ctx, StatementBase parsedStmt, boolean isInternalStmt,
                         CompletableFuture<ArrowFlightSqlResultDescriptor> deploymentFinished) {
        this.context = ctx;
        this.parsedStmt = Preconditions.checkNotNull(parsedStmt);
        this.originStmt = parsedStmt.getOrigStmt();
        this.serializer = context.getSerializer();
        this.isProxy = false;
        this.isInternalStmt = isInternalStmt;
        this.deploymentFinished = deploymentFinished;
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
        // only print the sepecific sql in multi statement
        String sql = context.isSingleStmt() ? originStmt.originStmt :
                AstToSQLBuilder.toSQLOrDefault(parsedStmt, originStmt.originStmt);
        if (AuditEncryptionChecker.needEncrypt(parsedStmt)) {
            summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT,
                    AstToSQLBuilder.toSQLOrDefault(parsedStmt, originStmt.originStmt));
        } else {
            summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, sql);
        }
        summaryProfile.addInfoString(ProfileManager.WAREHOUSE_CNGROUP, GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getWarehouseComputeResourceName(context.getCurrentComputeResource()));

        // Add some import variables in profile
        SessionVariable variables = context.getSessionVariable();
        if (variables != null) {
            // Add SQL dialect as a separate info string
            summaryProfile.addInfoString(ProfileManager.SQL_DIALECT, variables.getSqlDialect());

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

        // keep explicit transaction semantics consistent: let the leader run any read statements so it can enforce
        // validation (for example, checking tables already modified in the same transaction)
        if (context != null && context.getTxnId() != 0 && context.isQueryStmt(parsedStmt)) {
            return true;
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

    public String getPreparedStmtId() {
        // For EXECUTE, either `parsedStmt` is an `ExecuteStmt`, or `prepareStmtContext` has already been set.
        // Initially, `parsedStmt` is an `ExecuteStmt`. After it matches the corresponding `PrepareStmt`, `parsedStmt` is replaced
        // with a `QueryStatement`, while `prepareStmtContext` stores the associated `PrepareStmt` information.
        if (parsedStmt != null && parsedStmt instanceof ExecuteStmt) {
            ExecuteStmt executeStmt = (ExecuteStmt) parsedStmt;
            return executeStmt.getStmtName();
        }
        if (prepareStmtContext != null) {
            return prepareStmtContext.getStmt().getName();
        }

        // For PREPARE.
        if (parsedStmt != null && parsedStmt instanceof PrepareStmt) {
            PrepareStmt prepareStmt = (PrepareStmt) parsedStmt;
            return prepareStmt.getName();
        }
        return null;
    }

    /**
     * The execution timeout varies among different statements:
     * 1. SELECT:
     *    - If session query_timeout is not explicitly overridden, table_query_timeout (if set) can take effect.
     *    - If session query_timeout is explicitly overridden, always use session query_timeout.
     * 2. DML: use insert_timeout or statement-specified timeout
     * 3. ANALYZE: use fe_conf.statistic_collect_query_timeout
     */
    public int getExecTimeout() {
        if (parsedStmt instanceof CreateTableAsSelectStmt ctas) {
            Map<String, String> properties = ctas.getInsertStmt().getProperties();
            if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
                try {
                    return Integer.parseInt(properties.get(LoadStmt.TIMEOUT_PROPERTY));
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
            return ConnectContext.get().getSessionVariable().getInsertTimeoutS();
        } else if (parsedStmt instanceof DmlStmt) {
            Map<String, String> properties = ((DmlStmt) parsedStmt).getProperties();
            if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
                try {
                    return Integer.parseInt(properties.get(LoadStmt.TIMEOUT_PROPERTY));
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
            return ConnectContext.get().getSessionVariable().getInsertTimeoutS();
        } else if (parsedStmt instanceof AnalyzeStmt) {
            return (int) Config.statistic_collect_query_timeout;
        } else {
            // For SELECT queries:
            // session query_timeout is authoritative if explicitly overridden
            ConnectContext ctx = ConnectContext.get();
            int sessionQueryTimeout = ctx.getSessionVariable().getQueryTimeoutS();
            boolean sessionTimeoutOverridden = ctx.isSessionQueryTimeoutOverridden();
            if (parsedStmt instanceof QueryStatement) {
                try {
                    Map<TableName, Table> tables = AnalyzerUtils.collectAllTable(parsedStmt);
                    int minTableTimeout = -1;
                    String tableName = null;

                    // get minimum timeout among all tables
                    for (Map.Entry<TableName, Table> entry : tables.entrySet()) {
                        Table table = entry.getValue();
                        if (table instanceof OlapTable) {
                            OlapTable olapTable = (OlapTable) table;
                            int tableTimeout = olapTable.getTableQueryTimeout();
                            if (tableTimeout > 0) {
                                if (minTableTimeout < 0 || tableTimeout < minTableTimeout) {
                                    minTableTimeout = tableTimeout;
                                    tableName = entry.getKey() != null ? entry.getKey().toString() : table.getName();
                                }
                            }
                        }
                    }

                    // If table timeout is set, use it when session timeout is not explicitly overridden.
                    if (minTableTimeout > 0) {
                        tableQueryTimeoutTableName = tableName;
                        tableQueryTimeoutValue = minTableTimeout;
                        if (!sessionTimeoutOverridden) {
                            return minTableTimeout;
                        }
                        return sessionQueryTimeout;
                    } else {
                        tableQueryTimeoutTableName = null;
                        tableQueryTimeoutValue = -1;
                    }
                } catch (Exception e) {
                    LOG.warn("Cannot collect tables for table_query_timeout check, use cluster timeout", e);
                    tableQueryTimeoutTableName = null;
                    tableQueryTimeoutValue = -1;
                }
            }
            return sessionQueryTimeout;
        }
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

    public Pair<String, Integer> getTableQueryTimeoutInfo() {
        if (tableQueryTimeoutTableName != null && tableQueryTimeoutValue > 0) {
            return Pair.create(tableQueryTimeoutTableName, tableQueryTimeoutValue);
        }
        return null;
    }

    private ExecPlan generateExecPlan() throws Exception {
        ExecPlan execPlan = null;

        // Collect optimizer timing only when explicitly enabled to avoid overhead on normal queries.
        // When enabled, we only dump the trace to logs on planning failure.
        if (Config.enable_dump_optimizer_trace_on_error) {
            Tracers.enableTraceMode(Tracers.Mode.TIMER);
            Tracers.enableTraceModule(Tracers.Module.BASE);
            Tracers.enableTraceModule(Tracers.Module.OPTIMIZER);
        }
        try (Timer ignored = Tracers.watchScope("Total")) {
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

                    QueryStatement selectStmt =
                            ShowStmtToSelectStmtConverter.toSelectStmt((ShowStmt) parsedStmt);
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
            logOptimizerTraceOnGenerateExecPlanFailure(e);
            dumpException(e);
            throw new AnalysisException(e.getMessage(), e);
        } catch (StarRocksPlannerException e) {
            logOptimizerTraceOnGenerateExecPlanFailure(e);
            dumpException(e);
            if (e.getType().equals(ErrorType.USER_ERROR)) {
                throw e;
            } else {
                LOG.warn("Planner error: " + originStmt.originStmt, e);
                throw e;
            }
        } catch (Exception e) {
            logOptimizerTraceOnGenerateExecPlanFailure(e);
            throw e;
        }
        return execPlan;
    }

    private void logOptimizerTraceOnGenerateExecPlanFailure(Throwable e) {
        String qid = DebugUtil.printId(context.getQueryId());
        String sql = originStmt == null ? "" : SqlCredentialRedactor.redact(originStmt.originStmt);
        String err = e == null ? "" : (e.getClass().getSimpleName() + ": " + StringUtils.defaultString(e.getMessage()));

        if (Config.enable_dump_optimizer_trace_on_error) {
            String trace = Tracers.printScopeTimer();
            if (StringUtils.isNotBlank(trace)) {
                final int maxLen = 64 * 1024;
                if (trace.length() > maxLen) {
                    trace = trace.substring(0, maxLen) + "\n... truncated ...";
                }
                LOG.warn("Generate exec plan failed, dump optimizer trace (trace times optimizer)." +
                                " query_id={}, sql={}, err={}\n{}", qid, sql, err, trace);
                return;
            }
        }

        RuntimeProfile plannerProfile = new RuntimeProfile("Planner");
        Tracers.toRuntimeProfile(plannerProfile);
        LOG.warn("Generate exec plan failed. Planner profile: query_id={}, sql={}, err={}, profile={}",
                qid, sql, err, plannerProfile);
    }

    // Execute one statement.
    // Exception:
    // IOException: talk with client failed.
    public void execute() throws Exception {
        long beginTimeInNanoSecond = TimeUtils.getStartTime();
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());
        context.setIsForward(false);
        context.setIsLeaderTransferred(false);
        context.setCurrentThreadId(Thread.currentThread().getId());

        SessionVariable sessionVariableBackup = context.getSessionVariable();
        // set true to change session variable
        boolean skipRestore = false;
        // set execution id.
        // For statements other than `cache select`, try to use query id as execution id when execute first time.
        UUID uuid = context.getQueryId();
        if (!sessionVariableBackup.isEnableCacheSelect()) {
            context.setExecutionId(UUIDUtil.toTUniqueId(uuid));
        }

        // if use http protocol, use httpResultSender to send result to netty channel
        if (context instanceof HttpConnectContext) {
            httpResultSender = new HttpResultSender((HttpConnectContext) context);
        }

        final boolean shouldMarkIdleCheck = shouldMarkIdleCheck(parsedStmt);
        final Long originWarehouseId = context.getCurrentWarehouseIdAllowNull();
        if (shouldMarkIdleCheck && originWarehouseId != null) {
            WarehouseIdleChecker.increaseRunningSQL(originWarehouseId);
        }

        final boolean originSkipIcebergCache = context.isOnlyReadIcebergCache();
        if (parsedStmt instanceof InsertStmt || parsedStmt instanceof CreateTableAsSelectStmt) {
            context.setOnlyReadIcebergCache(true);
        } else if (parsedStmt instanceof RefreshMaterializedViewStatement 
                || parsedStmt instanceof CreateMaterializedViewStatement) {
            context.setOnlyReadIcebergCache(true);
        }

        RecursiveCTEExecutor cteExecutor = null;
        try {
            context.getState().setIsQuery(context.isQueryStmt(parsedStmt));
            if (parsedStmt.isExistQueryScopeHint()) {
                processQueryScopeHint();
            }

            // set warehouse for auditLog
            context.getAuditEventBuilder()
                    .setWarehouse(context.getCurrentWarehouseName())
                    .setCNGroup(context.getCurrentComputeResourceName());
            LOG.debug("set warehouse {} for stmt: {}", context.getCurrentWarehouseName(), parsedStmt);
            if (parsedStmt.isExplain()) {
                // reset the explain level to avoid the previous explain level affect the current query.
                context.setExplainLevel(parsedStmt.getExplainLevel());
            } else {
                context.setExplainLevel(null);
            }

            if (RecursiveCTEAstCheck.hasRecursiveCte(parsedStmt, context)) {
                redirectStatus = RedirectStatus.FORWARD_WITH_SYNC;
                if (isForwardToLeader()) {
                    context.setIsForward(true);
                    forwardToLeader();
                    return;
                }
                cteExecutor = new RecursiveCTEExecutor(context);
                parsedStmt = cteExecutor.splitOuterStmt(parsedStmt);
                if (parsedStmt.isExplain()) {
                    // Every time set no send flag and clean all data in buffer
                    context.getMysqlChannel().reset();
                    String explainString = cteExecutor.explainCTE(parsedStmt);
                    handleExplainStmt(explainString);
                    return;
                } else {
                    cteExecutor.prepareRecursiveCTE();
                }
            }

            redirectStatus = RedirectStatus.getRedirectStatus(parsedStmt);
            // for explain query, we can trace even if optimizer fails and execPlan is null
            // for refresh materialized view, use handleDdlStmt instead.
            if (parsedStmt.isExplainTrace() && !(parsedStmt instanceof RefreshMaterializedViewStatement)) {
                if (isForwardToLeader()) {
                    context.setIsForward(true);
                    forwardToLeader();
                    return;
                }

                ExecPlan execPlan = null;
                try {
                    execPlan = generateExecPlan();
                } catch (Exception e) {
                    LOG.warn("Generate exec plan failed for explain stmt: {}",
                            parsedStmt.getOrigStmt().originStmt, e);
                }
                handleExplainExecPlan(execPlan);
                return;
            }

            // Register as a planning query so it is visible in current_queries during optimization.
            // The planning entry is removed before handleQueryStmt/handleDMLStmt re-registers
            // with the real Coordinator, avoiding AlreadyExistsException from putIfAbsent.
            ExecPlan execPlan;
            context.setPlanning(true);
            try {
                QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(),
                        QeProcessorImpl.QueryInfo.fromPlanningQuery(context, originStmt.originStmt));
            } catch (Exception e) {
                LOG.warn("Failed to register planning query: {}", DebugUtil.printId(context.getExecutionId()), e);
            }
            try {
                execPlan = generateExecPlan();
            } finally {
                context.setPlanning(false);
                QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
            }

            // no need to execute http query dump request in BE
            if (context.isHTTPQueryDump) {
                return;
            }

            // For follower: verify sql in BlackList before forward to leader
            // For leader: if this is a proxy sql, no need to verify sql in BlackList because every fe has its own blacklist
            if ((parsedStmt instanceof QueryStatement || parsedStmt instanceof InsertStmt
                    || parsedStmt instanceof CreateTableAsSelectStmt)
                    && Config.enable_sql_blacklist && !parsedStmt.isExplain() && !isProxy) {
                OriginStatement origStmt = parsedStmt.getOrigStmt();
                if (context.isStatisticsConnection() || context.isStatisticsJob()) {
                    // For statistics connection or job, we trust it and skip sql blacklist check
                    LOG.debug("skip sql blacklist check for statistics connection or job. stmt: {}",
                            origStmt.originStmt);
                } else if (origStmt != null) {
                    String originSql = origStmt.originStmt.trim()
                            .toLowerCase().replaceAll(" +", " ");
                    // If this sql is in blacklist, show message.
                    GlobalStateMgr.getCurrentState().getSqlBlackList().verifying(originSql);
                    if (Config.enable_sql_digest || context.getSessionVariable().isEnableSQLDigest()) {
                        String digest = ConnectProcessor.computeStatementDigest(parsedStmt);
                        if (!digest.isEmpty()) {
                            GlobalStateMgr.getCurrentState().getSqlDigestBlackList().verifying(digest);
                        }
                    }
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
                            uuid = UUIDUtil.genUUID();
                            LOG.info("transfer QueryId: {} to {}", DebugUtil.printId(context.getQueryId()),
                                    DebugUtil.printId(uuid));
                            context.setExecutionId(UUIDUtil.toTUniqueId(uuid));
                        }

                        handleQueryStmt(retryContext.getExecPlan());
                        break;
                    } catch (Exception e) {
                        // For Arrow Flight SQL, FE doesn't know whether the client has already pull data from BE.
                        // So FE cannot decide whether it is able to retry.
                        if (i == retryTime - 1 || context.isArrowFlightSql()) {
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
                                // reset compute resource
                                context.ensureCurrentComputeResourceAvailable();
                            } else {
                                // Release all resources after the query finish as soon as possible, as query profile is
                                // asynchronous which can be delayed a long time.
                                if (coord != null) {
                                    coord.onReleaseSlots();
                                }

                                if (context.isProfileEnabled()) {
                                    isAsync = tryProcessProfileAsync(execPlan, i);
                                    if (parsedStmt.isExplainAnalyze()) {
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
                // set statement will ignore hint.
                handleSetStmt();
                skipRestore = context.getState().getStateType().equals(MysqlStateType.OK);
            } else if (parsedStmt instanceof RefreshConnectionsStmt) {
                handleRefreshConnectionsStmt();
            } else if (parsedStmt instanceof UseDbStmt) {
                handleUseDbStmt();
            } else if (parsedStmt instanceof SetWarehouseStmt) {
                handleSetWarehouseStmt();
                skipRestore = context.getState().getStateType().equals(MysqlStateType.OK);
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
            } else if (parsedStmt instanceof AddSqlDigestBlackListStmt) {
                handleAddSqlDigestBlackListStmt();
            } else if (parsedStmt instanceof DelSqlDigestBlackListStmt) {
                handleDelSqlDigestBlackListStmt();
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
            } else if (parsedStmt instanceof AddComputeNodeBlackListStmt) {
                handleAddComputeNodeBlackListStmt();
            } else if (parsedStmt instanceof DelComputeNodeBlackListStmt) {
                handleDelComputeNodeBlackListStmt();
            } else if (parsedStmt instanceof PlanAdvisorStmt) {
                handlePlanAdvisorStmt();
            } else if (parsedStmt instanceof TranslateStmt) {
                handleTranslateStmt();
            } else if (parsedStmt instanceof BeginStmt) {
                if (context.getSessionVariable().isEnableSqlTransaction()) {
                    TransactionStmtExecutor.beginStmt(context, (BeginStmt) parsedStmt);
                } else {
                    // transaction disabled: keep syntax only, do nothing and return OK
                    context.getState().setOk(0, 0, "");
                }
            } else if (parsedStmt instanceof CommitStmt) {
                if (context.getSessionVariable().isEnableSqlTransaction() || context.getTxnId() != 0) {
                    TransactionStmtExecutor.commitStmt(context, (CommitStmt) parsedStmt);
                } else {
                    // transaction disabled: keep syntax only, do nothing and return OK
                    context.getState().setOk(0, 0, "");
                }
            } else if (parsedStmt instanceof RollbackStmt) {
                if (context.getSessionVariable().isEnableSqlTransaction() || context.getTxnId() != 0) {
                    TransactionStmtExecutor.rollbackStmt(context, (RollbackStmt) parsedStmt);
                } else {
                    // transaction disabled: keep syntax only, do nothing and return OK
                    context.getState().setOk(0, 0, "");
                }
            } else {
                context.getState().setError("Do not support this query.");
            }
        } catch (IOException e) {
            LOG.warn("execute IOException ", e);
            // the exception happens when interact with client
            // this exception shows the connection is gone
            context.getState().setError(e.getMessage());
        } catch (LargeInPredicateException e) {
            // Re-throw LargeInPredicateException to trigger a full retry from parser stage
            // The query will be re-parsed and re-executed with enable_large_in_predicate=false
            // to use the traditional expression-based approach instead of raw constant optimization
            String sql = originStmt != null ? originStmt.originStmt : "";
            String truncatedSql = sql.length() > 200 ? sql.substring(0, 200) + "..." : sql;
            LOG.error("LargeInPredicate optimization failed, sql: {}, error: {}. Will retry with" +
                    " enable_large_in_predicate=false.", truncatedSql, e.getMessage());
            throw e;
        } catch (StarRocksException e) {
            String sql = originStmt != null ? originStmt.originStmt : "";
            // analysis exception only print message, not print the stack
            LOG.info("execute Exception, sql: {}, error: {}", SqlCredentialRedactor.redact(sql), e.getMessage());
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
            LOG.warn("execute Exception, sql: {}, " + SqlCredentialRedactor.redact(sql), e);
            context.getState().setError(e.getMessage());
            context.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
        } finally {
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
            if (context.getState().isError() && coord != null) {
                coord.cancel(PPlanFragmentCancelReason.INTERNAL_ERROR, context.getState().getErrorMessage());
            }

            if (coord != null) {
                coord.clearExternalResources();
            }

            if (parsedStmt != null && parsedStmt.isExistQueryScopeHint()) {
                clearQueryScopeHintContext();
            }

            // restore session variable in connect context
            if (!skipRestore) {
                context.setSessionVariable(sessionVariableBackup);
            }

            if (shouldMarkIdleCheck && originWarehouseId != null) {
                WarehouseIdleChecker.decreaseRunningSQL(originWarehouseId,
                        originStmt == null ? "" : originStmt.originStmt);
            }

            recordExecStatsIntoContext();

            if (GracefulExitFlag.isGracefulExit() && context.isLeaderTransferred() && !isInternalStmt) {
                LOG.info("leader is transferred during executing, forward to new leader");
                isForwardToLeaderOpt = Optional.of(true);
                forwardToLeader();
            }

            // process post-action after query is finished
            context.onQueryFinished();
            context.setOnlyReadIcebergCache(originSkipIcebergCache);
            if (cteExecutor != null) {
                cteExecutor.finalizeRecursiveCTE();
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
        context.getAuditEventBuilder().addReadLocalCnt(execStats.readLocalCnt != null ? execStats.readLocalCnt : 0);
        context.getAuditEventBuilder().addReadRemoteCnt(execStats.readRemoteCnt != null ? execStats.readRemoteCnt : 0);
        context.getAuditEventBuilder().setReturnRows(execStats.returnedRows == null ? 0 : execStats.returnedRows);
        context.getAuditEventBuilder().addTransmittedBytes(execStats.transmittedBytes != null ? execStats.transmittedBytes : 0);
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

    public void processQueryScopeSetVarHint() throws DdlException {
        if (!parsedStmt.isExistQueryScopeHint()) {
            return;
        }

        Map<String, String> hintSvs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (HintNode hint : parsedStmt.getAllQueryScopeHints()) {
            if (!(hint instanceof SetVarHint)) {
                continue;
            }
            hintSvs.putAll(hint.getValue());
        }

        if (hintSvs.isEmpty()) {
            return;
        }

        SessionVariable clonedSessionVariable = context.sessionVariable.clone();
        // apply warehouse
        if (hintSvs.containsKey(SessionVariable.WAREHOUSE_NAME)) {
            context.applyWarehouseSessionVariable(hintSvs.get(SessionVariable.WAREHOUSE_NAME), clonedSessionVariable);
        }
        GlobalStateMgr.getCurrentState().getVariableMgr().applySessionVariable(hintSvs, clonedSessionVariable);
        context.setSessionVariable(clonedSessionVariable);
    }

    // support select hint e.g. select /*+ SET_VAR(query_timeout=1) */ sleep(3);
    public void processQueryScopeHint() throws DdlException {
        SessionVariable clonedSessionVariable = null;
        UUID queryId = context.getQueryId();
        final TUniqueId executionId = context.getExecutionId();

        Map<String, UserVariable> clonedUserVars = new ConcurrentHashMap<>(context.getUserVariables());
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
                        clonedSessionVariable = context.sessionVariable.clone();
                    }
                    for (Map.Entry<String, String> entry : hint.getValue().entrySet()) {
                        if (entry.getKey().equalsIgnoreCase(SessionVariable.WAREHOUSE_NAME)) {
                            context.applyWarehouseSessionVariable(entry.getValue(), clonedSessionVariable);
                        }
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
                CreateTemporaryTableStmt createTemporaryTableStmt = (CreateTemporaryTableStmt) stmt.getCreateTableStmt();
                createTemporaryTableStmt.setSessionId(context.getSessionId());
                return context.getGlobalStateMgr().getMetadataMgr().createTemporaryTable(context, createTemporaryTableStmt);
            } else {
                return context.getGlobalStateMgr().getMetadataMgr().createTable(context, stmt.getCreateTableStmt());
            }
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    private void dropTableCreatedByCTAS(CreateTableAsSelectStmt stmt) throws Exception {
        if (stmt instanceof CreateTemporaryTableAsSelectStmt) {
            DropTemporaryTableStmt dropTemporaryTableStmt =
                    new DropTemporaryTableStmt(true, stmt.getCreateTableStmt().getTableRef(), true);
            dropTemporaryTableStmt.setSessionId(context.getSessionId());
            DDLStmtExecutor.execute(dropTemporaryTableStmt, context);
        } else {
            DDLStmtExecutor.execute(new DropTableStmt(
                    true, stmt.getCreateTableStmt().getTableRef(), true), context);
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
            // Set CTAS flag for metrics tracking
            context.setCTAS(true);
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
        } finally {
            // Reset CTAS flag
            context.setCTAS(false);
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
            leaderOpExecutor =
                    new LeaderOpExecutor(parsedStmt, originStmt, context, redirectStatus, isInternalStmt, deploymentFinished);
            LOG.debug("need to transfer to Leader. stmt: {}", context.getStmtId());
            leaderOpExecutor.execute();
        } finally {
            context.decPendingForwardRequest();
        }
    }

    /**
     * Try to process profile async without exception.
     */
    private boolean tryProcessProfileAsync(ExecPlan plan, int retryIndex) {
        try {
            return processProfileAsync(plan, retryIndex);
        } catch (Exception e) {
            LOG.warn("process profile async failed", e);
            return false;
        }
    }

    private boolean processProfileAsync(ExecPlan plan, int retryIndex) {
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
                long latencyThresholdMs = Config.profile_log_latency_threshold_ms;
                if (context.getSessionVariable().getProfileLogLatencyThresholdMs() >= 0) {
                    latencyThresholdMs = context.getSessionVariable().getProfileLogLatencyThresholdMs();
                }
                if (totalTimeMs >= latencyThresholdMs) {
                    String jsonString = GSON.toJson(queryDetail);
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
                killCtx = ExecuteEnv.getInstance().getScheduler().getContext(id);
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
            if (!killConnection) {
                // Expose a structured cancellation signal for query-level kill.
                killCtx.getState().setErrorCode(ErrorCode.ERR_QUERY_INTERRUPTED);
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
                                context, redirectStatus, false);
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

    // Process refresh connections statement.
    private void handleRefreshConnectionsStmt() {
        try {
            RefreshConnectionsStmt stmt = (RefreshConnectionsStmt) parsedStmt;
            GlobalStateMgr.getCurrentState().getVariableMgr().refreshConnections(stmt.isForce());
        } catch (Exception e) {
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

    /**
     * Handle Explain stmt. NOTE: we can record some traces even if execPlan is null.
     */
    private void handleExplainExecPlan(ExecPlan execPlan) throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();
        String explainString = buildExplainString(execPlan, parsedStmt, context, ResourceGroupClassifier.QueryType.SELECT,
                parsedStmt.getExplainLevel());
        handleExplainStmt(explainString);
    }

    // Process a select statement.
    private void handleQueryStmt(ExecPlan execPlan) throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();

        boolean isExplainQuery = parsedStmt.isExplain();
        boolean isExplainAnalyze = isExplainQuery
                && StatementBase.ExplainLevel.ANALYZE.equals(parsedStmt.getExplainLevel());
        boolean isSchedulerExplain = isExplainQuery
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

        // TODO(liuzihe): support execute in FE for Arrow Flight SQL.
        boolean executeInFe = !isExplainAnalyze && !isSchedulerExplain && !isOutfileQuery
                && canExecuteInFe(context, execPlan.getPhysicalPlan()) && !(context.isArrowFlightSql());

        if (isExplainAnalyze) {
            context.getSessionVariable().setEnableProfile(true);
            context.getSessionVariable().setEnableAsyncProfile(false);
            context.getSessionVariable().setPipelineProfileLevel(1);
        } else if (isSchedulerExplain) {
            // Do nothing.
        } else if (isExplainQuery) {
            String explainString =
                    buildExplainString(execPlan, parsedStmt, context, ResourceGroupClassifier.QueryType.SELECT,
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
            coord = getCoordinatorFactory().createQueryScheduler(context, fragments, scanNodes, descTable, execPlan);
        }

        // Predict the cost of this query
        if (Config.enable_query_cost_prediction) {
            CostPredictor predictor = getCostPredictor();
            long memBytes = predictor.tryPredictMemoryBytes(execPlan);
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

        coord.execWithQueryDeployExecutor(context);
        coord.setTopProfileSupplier(this::buildTopLevelProfile);
        coord.setExecPlan(execPlan);

        RowBatch batch = null;
        if (context instanceof HttpConnectContext) {
            batch = httpResultSender.sendQueryResult(coord, execPlan, parsedStmt.getOrigStmt().getOrigStmt());
        } else {
            final boolean isArrowFlight = context.isArrowFlightSql() && deploymentFinished != null;
            if (isArrowFlight && !isExplainAnalyze && !isOutfileQuery) {
                Preconditions.checkState(coord instanceof DefaultCoordinator,
                        "Coordinator is not DefaultCoordinator, cannot proceed with BE execution.");
                DefaultCoordinator defaultCoordinator = (DefaultCoordinator) coord;

                ExecutionFragment rootFragment = defaultCoordinator.getExecutionDAG().getRootFragment();
                FragmentInstance rootFragmentInstance = rootFragment.getInstances().get(0);
                ComputeNode worker = rootFragmentInstance.getWorker();
                TUniqueId rootFragmentInstanceId = rootFragmentInstance.getInstanceId();
                Schema schema = buildSchema(execPlan);

                ArrowFlightSqlResultDescriptor backendResultDesc =
                        new ArrowFlightSqlResultDescriptor(worker.getId(), rootFragmentInstanceId, schema);

                deploymentFinished.complete(backendResultDesc);
            }

            final boolean needSendResult = !isPlanAdvisorAnalyze && !isExplainAnalyze
                    && !context.getSessionVariable().isEnableExecutionOnly() && !isArrowFlight;
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
            final RawScopedTimer rawScopedTimer = new RawScopedTimer();
            do {
                batch = coord.getNext();
                // for outfile query, there will be only one empty batch send back with eos flag
                if (batch.getBatch() != null && !isOutfileQuery && needSendResult) {
                    // For some language driver, getting error packet after fields packet will be recognized as a success result
                    // so We need to send fields after first batch arrived
                    if (!isSendFields) {
                        responseFields(rawScopedTimer, colNames, outputExprs);
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

                    responseRowBatch(rawScopedTimer, batch, channel);
                }
                if (batch.getBatch() != null) {
                    context.updateReturnRows(batch.getBatch().getRows().size());
                }
            } while (!batch.isEos());

            if (!isSendFields && !isOutfileQuery && !isExplainAnalyze && !isPlanAdvisorAnalyze && !isArrowFlight) {
                responseFields(rawScopedTimer, colNames, outputExprs);
            }
            
            context.getAuditEventBuilder().setWriteClientTimeMs(rawScopedTimer.getTotalTime());
        }

        processQueryStatisticsFromResult(batch, execPlan, isOutfileQuery);
        GlobalStateMgr.getCurrentState().getQueryHistoryMgr().addQueryHistory(context, execPlan);
    }

    private void responseRowBatch(RawScopedTimer timer, RowBatch batch, MysqlChannel channel) throws IOException {
        try (final RawScopedTimer.Guard ignore = timer.start()) {
            for (ByteBuffer row : batch.getBatch().getRows()) {
                if (isProxy) {
                    proxyResultBuffer.add(row);
                } else {
                    channel.sendOnePacket(row);
                }
            }
        }
    }

    private void responseFields(RawScopedTimer timer, List<String> colNames, List<Expr> exprs) throws IOException {
        try (final RawScopedTimer.Guard ignore = timer.start()) {
            sendFields(colNames, exprs);
        }
    }

    /**
     * The query result batch will piggyback query statistics in it
     */
    private void processQueryStatisticsFromResult(RowBatch batch, ExecPlan execPlan, boolean isOutfileQuery) {
        if (batch != null && parsedStmt.getOrigStmt() != null && parsedStmt.getOrigStmt().getOrigStmt() != null) {
            statisticsForAuditLog = batch.getQueryStatistics();
            if (!isOutfileQuery) {
                context.getState().setEof();
            } else {
                context.getState().setOk(statisticsForAuditLog.returnedRows, 0, "");
            }

            if (null == statisticsForAuditLog) {
                return;
            }

            analyzePlanWithExecStats(execPlan);
            if (context.isArrowFlightSql()) {
                context.updateReturnRows(statisticsForAuditLog.getReturnedRows());
            }

            if (null == statisticsForAuditLog.statsItems || statisticsForAuditLog.statsItems.isEmpty()) {
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
        TableRef tableRef = analyzeStmt.getTableRef();
        TableName tableName = new TableName(tableRef.getCatalogName(), tableRef.getDbName(),
                tableRef.getTableName(), tableRef.getPos());
        Database db =
                GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, tableName.getCatalog(), tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tableName.getCatalogAndDb());
        }
        Table table = MetaUtils.getSessionAwareTable(context, db, tableName);
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
            String catalogName = analyzeStmt.getCatalogName();
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

        // TODO(stephen): we need to persist statisticsTypes to analyzeStatus when supporting auto collect multi_columns stats
        // Currently temporarily identified by properties
        if (!analyzeTypeDesc.getStatsTypes().isEmpty()) {
            analyzeStatus.getProperties().put(IS_MULTI_COLUMN_STATS, "true");
        }

        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.PENDING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);

        int queryTimeout = context.getSessionVariable().getQueryTimeoutS();
        int insertTimeout = context.getSessionVariable().getInsertTimeoutS();
        try {
            Runnable originalTask = () -> executeAnalyze(analyzeStmt, analyzeStatus, db, table);
            CancelableAnalyzeTask cancelableTask = new CancelableAnalyzeTask(originalTask, analyzeStatus);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeTaskThreadPool().execute(cancelableTask);

            if (!analyzeStmt.isAsync()) {
                // sync statistics collection doesn't be interrupted by query timeout, but
                // will print warning log if timeout, so we update timeout temporarily to avoid
                // warning log
                context.getSessionVariable().setQueryTimeoutS((int) Config.statistic_collect_query_timeout);
                context.getSessionVariable().setInsertTimeoutS((int) Config.statistic_collect_query_timeout);
                cancelableTask.get();
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
        if (profileElement == null) {
            throw new StarRocksException("Query profile not found for query_id: " + queryId +
                ". The query may not have generated a profile, or the profile has been evicted from memory.");
        }
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

    protected void executeAnalyze(AnalyzeStmt analyzeStmt, AnalyzeStatus analyzeStatus, Database db, Table table) {
        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        if (table.isTemporaryTable()) {
            statsConnectCtx.setSessionId(context.getSessionId());
        }
        // from current session, may execute analyze stmt
        statsConnectCtx.getSessionVariable().setStatisticCollectParallelism(
                context.getSessionVariable().getStatisticCollectParallelism());
        statsConnectCtx.setStatisticsConnection(true);
        // honor session variable for ANALYZE
        statsConnectCtx.setCurrentWarehouse(context.getCurrentWarehouseName());
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
                List<com.starrocks.type.Type> columnTypes = analyzeStmt.getColumnTypes();
                if (CollectionUtils.isEmpty(columnTypes) && CollectionUtils.isNotEmpty(analyzeStmt.getColumnNames())) {
                    columnTypes = analyzeStmt.getColumnNames().stream()
                            .map(col -> table.getColumn(col).getType())
                            .collect(Collectors.toList());
                }
                statisticExecutor.collectStatistics(statsConnectCtx,
                        new ExternalHistogramStatisticsCollectJob(analyzeStmt.getCatalogName(),
                                db, table, analyzeStmt.getColumnNames(), columnTypes,
                                StatsConstants.AnalyzeType.HISTOGRAM, StatsConstants.ScheduleType.ONCE,
                                analyzeStmt.getProperties()),
                        analyzeStatus,
                        false, false /* resetWarehouse */);
            } else {
                StatsConstants.AnalyzeType analyzeType = analyzeStmt.isSample() ? StatsConstants.AnalyzeType.SAMPLE :
                        StatsConstants.AnalyzeType.FULL;
                statisticExecutor.collectStatistics(statsConnectCtx,
                        StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(
                                analyzeStmt.getCatalogName(),
                                db, table, null,
                                analyzeStmt.getColumnNames(),
                                analyzeStmt.getColumnTypes(),
                                analyzeType,
                                StatsConstants.ScheduleType.ONCE, analyzeStmt.getProperties()),
                        analyzeStatus,
                        false, false /* resetWarehouse */);
            }
        } else {
            if (analyzeTypeDesc.isHistogram()) {
                List<com.starrocks.type.Type> columnTypes = analyzeStmt.getColumnTypes();
                if (CollectionUtils.isEmpty(columnTypes) && CollectionUtils.isNotEmpty(analyzeStmt.getColumnNames())) {
                    columnTypes = analyzeStmt.getColumnNames().stream()
                            .map(col -> table.getColumn(col).getType())
                            .collect(Collectors.toList());
                }
                statisticExecutor.collectStatistics(statsConnectCtx,
                        new HistogramStatisticsCollectJob(db, table, analyzeStmt.getColumnNames(),
                                columnTypes, StatsConstants.ScheduleType.ONCE,
                                analyzeStmt.getProperties()),
                        analyzeStatus,
                        // Sync load cache, auto-populate column statistic cache after Analyze table manually
                        false, false /* resetWarehouse */);
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
                                analyzeStmt.getColumnNames() != null ? List.of(analyzeStmt.getColumnNames()) : null,
                                true),
                        analyzeStatus,
                        // Sync load cache, auto-populate column statistic cache after Analyze table manually
                        false, false /* resetWarehouse */);
            }
        }
    }

    private void handleDropStatsStmt() {
        DropStatsStmt dropStatsStmt = (DropStatsStmt) parsedStmt;
        TableRef tableRef = dropStatsStmt.getTableRef();
        if (tableRef == null) {
            throw new SemanticException("Table ref is null");
        }
        TableName tableName = new TableName(tableRef.getCatalogName(), tableRef.getDbName(),
                tableRef.getTableName(), tableRef.getPos());
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, tableName.getCatalog(),
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
            analyzeMgr.dropMultiColumnStatsMetaAndData(StatisticUtils.buildConnectContext(), List.of(table.getId()));
            statisticStorage.expireMultiColumnStatistics(table.getId());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropAnalyzeStatus(table.getId());

            if (!dropStatsStmt.isMultiColumn()) {
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropAnalyzeStatus(table.getId());
                GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                        .dropBasicStatsMetaAndData(StatisticUtils.buildConnectContext(), List.of(table.getId()));
                GlobalStateMgr.getCurrentState().getStatisticStorage().expireTableAndColumnStatistics(table, columns);
            }
        }
    }

    private void handleDropHistogramStmt() {
        DropHistogramStmt dropHistogramStmt = (DropHistogramStmt) parsedStmt;
        TableRef tableRef = dropHistogramStmt.getTableRef();
        if (tableRef == null) {
            throw new SemanticException("Table ref is null");
        }
        TableName tableName = new TableName(tableRef.getCatalogName(), tableRef.getDbName(),
                tableRef.getTableName(), tableRef.getPos());
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, tableName.getCatalog(),
                tableName.getDb(), tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName.toString());
        }

        if (dropHistogramStmt.isExternal()) {
            List<String> columns = dropHistogramStmt.getColumnNames();

            GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropExternalAnalyzeStatus(table.getUUID());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropExternalHistogramStatsMetaAndData(
                    StatisticUtils.buildConnectContext(), tableName, table, columns);
            GlobalStateMgr.getCurrentState().getStatisticStorage().expireConnectorHistogramStatistics(table, columns);
        } else {
            List<String> columns = table.getBaseSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                    .collect(Collectors.toList());

            GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropAnalyzeStatus(table.getId());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                    .dropHistogramStatsMetaAndData(StatisticUtils.buildConnectContext(),
                            List.of(table.getId()));
            GlobalStateMgr.getCurrentState().getStatisticStorage().expireHistogramStatistics(table.getId(), columns);
        }
    }

    private void handleKillAnalyzeStmt() {
        KillAnalyzeStmt killAnalyzeStmt = (KillAnalyzeStmt) parsedStmt;
        AnalyzeMgr analyzeManager = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
        if (killAnalyzeStmt.isKillAllPendingTasks()) {
            analyzeManager.killAllPendingTasks();
        } else {
            long analyzeId;
            if (killAnalyzeStmt.hasUserVariable()) {
                UserVariableExpr userVariableExpr = killAnalyzeStmt.getUserVariableExpr();
                // Analyze the user variable expression to get its value
                com.starrocks.sql.analyzer.ExpressionAnalyzer.analyzeExpressionIgnoreSlot(userVariableExpr, context);
                Expr value = userVariableExpr.getValue();
                if (value instanceof com.starrocks.sql.ast.expression.NullLiteral) {
                    throw new SemanticException("User variable '%s' is not set", userVariableExpr.getName());
                }
                if (!(value instanceof IntLiteral)) {
                    throw new SemanticException("User variable '%s' must be an integer, but got %s",
                            userVariableExpr.getName(), value.getType().toSql());
                }
                analyzeId = ((IntLiteral) value).getLongValue();
            } else {
                analyzeId = killAnalyzeStmt.getAnalyzeId();
            }
            checkPrivilegeForKillAnalyzeStmt(context, analyzeId);
            // Try to kill the job anyway.
            analyzeManager.killConnection(analyzeId);
        }
    }

    private void checkTblPrivilegeForKillAnalyzeStmt(ConnectContext context, String catalogName, String dbName,
                                                     String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, catalogName, dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, catalogName, dbName, tableName);
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
        Pattern sqlPattern = null;
        String sql = addSqlBlackListStmt.getSql().trim().toLowerCase().replaceAll(" +", " ")
                .replace("\r", " ")
                .replace("\n", " ")
                .replaceAll("\\s+", " ");
        if (!sql.isEmpty()) {
            try {
                sqlPattern = Pattern.compile(sql);
            } catch (PatternSyntaxException e) {
                throw new SemanticException("Sql syntax error: %s", e.getMessage());
            }
        }

        if (sqlPattern == null) {
            throw new SemanticException("Sql pattern cannot be empty");
        }

        GlobalStateMgr.getCurrentState().getSqlBlackList().put(sqlPattern);
    }

    private void handleDelSqlBlackListStmt() {
        GlobalStateMgr.getCurrentState().getSqlBlackList().deleteBlackSql((DelSqlBlackListStmt) parsedStmt);
    }

    private void handleAddSqlDigestBlackListStmt() {
        AddSqlDigestBlackListStmt stmt = (AddSqlDigestBlackListStmt) parsedStmt;
        String digest = stmt.getDigest();
        if (!digest.matches("^[a-f0-9]{32}$")) {
            throw new SemanticException("Invalid digest format. Expected 32-character hex string: " + digest);
        }
        GlobalStateMgr.getCurrentState().getSqlDigestBlackList().addDigest(stmt.getDigest());
    }

    private void handleDelSqlDigestBlackListStmt() {
        DelSqlDigestBlackListStmt delSqlDigestBlackListStmt = (DelSqlDigestBlackListStmt) parsedStmt;
        List<String> digests = delSqlDigestBlackListStmt.getDigests();
        if (digests != null) {
            GlobalStateMgr.getCurrentState().getSqlDigestBlackList().deleteDigests(digests);
        }
    }

    private void handleAddBackendBlackListStmt() throws StarRocksException {
        GlobalStateMgr.getCurrentState().getSqlBlackList().addBlackSql((AddBackendBlackListStmt) parsedStmt, context);
    }

    private void handleDelBackendBlackListStmt() throws StarRocksException {
        DelBackendBlackListStmt delBackendBlackListStmt = (DelBackendBlackListStmt) parsedStmt;
        Authorizer.check(delBackendBlackListStmt, context);
        for (Long backendId : delBackendBlackListStmt.getBackendIds()) {
            SystemInfoService sis = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            if (sis.getBackend(backendId) == null) {
                throw new StarRocksException("Not found backend: " + backendId);
            }
            SimpleScheduler.getHostBlacklist().remove(backendId);
        }
    }

    private void handleAddComputeNodeBlackListStmt() throws StarRocksException {
        AddComputeNodeBlackListStmt addComputeNodeBlackListStmt = (AddComputeNodeBlackListStmt) parsedStmt;
        Authorizer.check(addComputeNodeBlackListStmt, context);
        for (Long computeNodeId : addComputeNodeBlackListStmt.getComputeNodeIds()) {
            SystemInfoService sis = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            if (sis.getComputeNode(computeNodeId) == null) {
                throw new StarRocksException("Not found compute node: " + computeNodeId);
            }
            SimpleScheduler.getHostBlacklist().addByManual(computeNodeId);
        }
    }

    private void handleDelComputeNodeBlackListStmt() throws StarRocksException {
        DelComputeNodeBlackListStmt delComputeNodeBlackListStmt = (DelComputeNodeBlackListStmt) parsedStmt;
        Authorizer.check(delComputeNodeBlackListStmt, context);
        for (Long computeNodeId : delComputeNodeBlackListStmt.getComputeNodeIds()) {
            SystemInfoService sis = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            if (sis.getComputeNode(computeNodeId) == null) {
                throw new StarRocksException("Not found compute node: " + computeNodeId);
            }
            SimpleScheduler.getHostBlacklist().remove(computeNodeId);
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
        if (context.isArrowFlightSql()) {
            ArrowFlightSqlConnectContext ctx = (ArrowFlightSqlConnectContext) context;
            ctx.addShowResult(DebugUtil.printId(ctx.getExecutionId()), resultSet);
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

        if (context.getQueryDetail() != null) {
            context.getQueryDetail().setExplain(explainString);
        }

        if (context instanceof HttpConnectContext) {
            httpResultSender.sendExplainResult(explainString);
        } else if (context.isArrowFlightSql()) {
            ArrowFlightSqlConnectContext ctx = (ArrowFlightSqlConnectContext) context;
            ctx.addExplainResult(DebugUtil.printId(ctx.getExecutionId()), explainString);
        } else {
            ShowResultSetMetaData metaData =
                    ShowResultSetMetaData.builder()
                            .addColumn(new Column("Explain String", TypeFactory.createVarcharType(20)))
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
        }

        context.getState().setEof();
    }

    /**
     * Build explain string for explain statement.
     * NOTE: execPlan maybe null but we can still trace some broken optimizer messages for better debug.
     */
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
            explainString += "PLAN NOT AVAILABLE\n";
        }

        Tracers.Mode mode = Tracers.Mode.NONE;
        try {
            mode = Tracers.Mode.valueOf(parsedStmt.getTraceMode());
        } catch (Exception e) {
            // pass
        }
        if (mode == Tracers.Mode.TIMER) {
            explainString += Tracers.printScopeTimer();
        } else if (mode == Tracers.Mode.VARS) {
            explainString += Tracers.printVars();
        } else if (mode == Tracers.Mode.TIMING) {
            explainString += Tracers.printTiming();
        } else if (mode == Tracers.Mode.LOGS) {
            explainString += Tracers.printLogs();
        } else if (mode == Tracers.Mode.REASON) {
            explainString += Tracers.printReasons();
        } else {
            OperatorTuningGuides.OptimizedRecord optimizedRecord = PlanTuningAdvisor.getInstance()
                    .getOptimizedRecord(context.getQueryId());
            if (optimizedRecord != null) {
                explainString += optimizedRecord.getExplainString();
            }
            if (execPlan != null) {
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
        String db = exportStmt.getDbName();
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
        FailPointExecutor executor = new FailPointExecutor(parsedStmt);
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
        if (statisticsForAuditLog.readLocalCnt == null) {
            statisticsForAuditLog.readLocalCnt = 0L;
        }
        if (statisticsForAuditLog.readRemoteCnt == null) {
            statisticsForAuditLog.readRemoteCnt = 0L;
        }
        return statisticsForAuditLog;
    }

    public void handleInsertOverwrite(InsertStmt insertStmt) throws Exception {
        TableRef tableRef = insertStmt.getTableRef();
        Database db =
                GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, tableRef.getCatalogName(), tableRef.getDbName());
        if (db == null) {
            String catalogAndDb = tableRef.getCatalogName() != null ?
                    tableRef.getCatalogName() + "." + tableRef.getDbName() : tableRef.getDbName();
            throw new SemanticException("Database %s is not found", catalogAndDb);
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
        if (!locker.lockTableAndCheckDbExist(db, olapTable.getId(), LockType.WRITE)) {
            throw new DmlException("database:%s does not exist.", db.getFullName());
        }
        try {
            // add an edit log
            CreateInsertOverwriteJobLog info = new CreateInsertOverwriteJobLog(job.getJobId(),
                    job.getTargetDbId(), job.getTargetTableId(), job.getSourcePartitionIds(),
                    job.isDynamicOverwrite());
            GlobalStateMgr.getCurrentState().getEditLog().logCreateInsertOverwrite(info);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.WRITE);
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
            LOG.warn("DML statement({}) process failed.", originStmt.originStmt, t);
            throw t;
        } finally {
            boolean isAsync = false;
            try {
                if (context.isProfileEnabled() || LoadErrorUtils.enableProfileAfterError(coord)) {
                    isAsync = tryProcessProfileAsync(execPlan, 0);
                    if (parsedStmt.isExplain() &&
                            StatementBase.ExplainLevel.ANALYZE.equals(parsedStmt.getExplainLevel())) {
                        handleExplainStmt(ExplainAnalyzer.analyze(ProfilingExecPlan.buildFrom(execPlan),
                                profile, null, context.getSessionVariable().getColorExplainOutput()));
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to process profile async", e);
            } finally {
                if (isAsync) {
                    QeProcessorImpl.INSTANCE.monitorQuery(context.getExecutionId(), System.currentTimeMillis() +
                            context.getSessionVariable().getProfileTimeout() * 1000L);
                } else {
                    QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
                }
            }
        }
    }

    public IcebergMetadata.IcebergSinkExtra fillRewriteFiles(DmlStmt stmt, ExecPlan execPlan,
                                                             List<TSinkCommitInfo> commitInfos,
                                                             IcebergMetadata.IcebergSinkExtra extra) {
        if (stmt instanceof IcebergRewriteStmt) {
            for (TSinkCommitInfo commitInfo : commitInfos) {
                commitInfo.setIs_rewrite(true);
            }
            if (extra == null) {
                extra = new IcebergMetadata.IcebergSinkExtra();
            }
            for (PlanFragment fragment : execPlan.getFragments()) {
                for (ScanNode scan : fragment.collectScanNodes().values()) {
                    if (scan instanceof IcebergScanNode && scan.getPlanNodeName().equals("IcebergScanNode")) {
                        extra.addAppliedDeleteFiles(((IcebergScanNode) scan).getPosAppliedDeleteFiles());
                        extra.addScannedDataFiles(((IcebergScanNode) scan).getScannedDataFiles());
                        if (((IcebergRewriteStmt) stmt).rewriteAll()) {
                            extra.addAppliedDeleteFiles(((IcebergScanNode) scan).getEqualAppliedDeleteFiles());
                        }
                    }
                }
            }
        }
        return extra;
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

        // Handle metadata-level delete for Iceberg
        if (stmt instanceof DeleteStmt && execPlan != null && execPlan.isIcebergMetadataDelete()) {
            // Execute metadata-level delete
            IcebergMetadataDeleteNode node = (IcebergMetadataDeleteNode) execPlan.getTopFragment().getPlanRoot();
            IcebergTable table = node.getTable();
            ScalarOperator predicate = node.getPredicate();

            Optional<ConnectorMetadata> connectorMetadata = GlobalStateMgr.getCurrentState()
                    .getMetadataMgr().getOptionalMetadata(table.getCatalogName());
            if (connectorMetadata.isPresent()) {
                connectorMetadata.get().executeMetadataDelete(table, predicate, context);
            } else {
                throw new StarRocksConnectorException("Can not find {} connector metadata for table: {}",
                        table.getCatalogName(), table.getName());
            }
            context.getState().setOk();
            return;
        }

        DmlType dmlType = DmlType.fromStmt(stmt);
        TableRef tableRef = stmt.getTableRef();
        if (tableRef == null) {
            throw new SemanticException("Table ref is null");
        }
        String catalogName = tableRef.getCatalogName();
        String dbName = tableRef.getDbName();
        String tableName = tableRef.getTableName();
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, catalogName, dbName);
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        final Table targetTable;
        if (stmt instanceof InsertStmt && ((InsertStmt) stmt).getTargetTable() != null) {
            targetTable = ((InsertStmt) stmt).getTargetTable();
        } else {
            TableName tableNameObj = new TableName(catalogName, dbName, tableName, tableRef.getPos());
            targetTable = MetaUtils.getSessionAwareTable(context, database, tableNameObj);
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

        if (context.getTxnId() != 0) {
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
                    context, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift(), execPlan);
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
                InsertLoadTxnCallback insertLoadTxnCallback =
                        InsertLoadTxnCallbackFactory.of(context, database.getId(), targetTable);
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
                        coord,
                        insertLoadTxnCallback);
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
            if (filteredRows > (filteredRows + loadedRows) * getMaxFilterRatio(stmt)) {
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
                        mgr.abortTransaction(database.getId(), transactionId, ERR_NO_ROWS_IMPORTED.formatErrorMsg(),
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

                DataSink dataSink = execPlan.getFragments().get(0).getSink();
                if (dataSink instanceof IcebergDeleteSink deleteSink) {
                    // This is a DELETE operation, use IcebergDeleteSink
                    IcebergMetadata.IcebergSinkExtra extra = deleteSink.getSinkExtraInfo();
                    context.getGlobalStateMgr().getMetadataMgr().finishSink(
                            catalogName, dbName, tableName, commitInfos, null, extra, context);
                } else if (dataSink instanceof IcebergTableSink) {
                    // This is an INSERT/OVERWRITE operation, use IcebergTableSink
                    IcebergTableSink sink = (IcebergTableSink) dataSink;
                    if (stmt instanceof InsertStmt && ((InsertStmt) stmt).isOverwrite()) {
                        for (TSinkCommitInfo commitInfo : commitInfos) {
                            commitInfo.setIs_overwrite(true);
                        }
                    }

                    IcebergMetadata.IcebergSinkExtra extra = null;
                    extra = fillRewriteFiles(stmt, execPlan, commitInfos, extra);
                    if (context.getSkipFinishSink()) {
                        context.getFinishSinkHandler()
                                .finish(catalogName, dbName, tableName, commitInfos, sink.getTargetBranch(), (Object) extra);
                    } else {
                        context.getGlobalStateMgr().getMetadataMgr().finishSink(
                                catalogName, dbName, tableName, commitInfos, sink.getTargetBranch(), (Object) extra, context);
                    }
                } else {
                    throw new RuntimeException("Unsupported sink type for Iceberg table: " + dataSink.getClass().getName());
                }
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

                txnStatus = TransactionStatus.COMMITTED;
                final long waitInterval = 300;
                while (publishWaitMs > 0) {
                    if (GlobalStateMgr.getCurrentState().isLeaderTransferred()) {
                        break;
                    }

                    if (visibleWaiter.await(Math.min(publishWaitMs, waitInterval), TimeUnit.MILLISECONDS)) {
                        txnStatus = TransactionStatus.VISIBLE;
                        MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
                        // collect table-level metrics
                        TableMetricsEntity entity =
                                TableMetricsRegistry.getInstance().getMetricsEntity(targetTable.getId());
                        entity.counterInsertLoadFinishedTotal.increase(1L);
                        entity.counterInsertLoadRowsTotal.increase(loadedRows);
                        entity.counterInsertLoadBytesTotal.increase(loadedBytes);
                        break;
                    } else {
                        publishWaitMs -= Math.min(publishWaitMs, waitInterval);
                    }
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
        } catch (Exception e) {
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
                    context, plan.getFragments(), plan.getScanNodes(), plan.getDescTbl().toThrift(), plan);
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
                    context, plan.getFragments(), plan.getScanNodes(), plan.getDescTbl().toThrift(), plan);
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
            try {
                if (context.isProfileEnabled()) {
                    tryProcessProfileAsync(plan, 0);
                    QeProcessorImpl.INSTANCE.monitorQuery(context.getExecutionId(), System.currentTimeMillis() +
                            context.getSessionVariable().getProfileTimeout() * 1000L);
                } else {
                    QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
                }
                recordExecStatsIntoContext();
            } catch (Exception e) {
                LOG.warn("Failed to unregister query", e);
            }
        }
    }

    /**
     * Record this sql into the query details, which would be collected by external query history system
     */
    public void addRunningQueryDetail(StatementBase parsedStmt) {
        if (!Config.enable_collect_query_detail_info) {
            return;
        }

        SessionVariable sessionVariableBackup = context.getSessionVariable();
        try {
            processQueryScopeSetVarHint();
        } catch (DdlException e) {
            LOG.warn("Failed to process query scope set variable.", e);
        }

        try {
            String sql = parsedStmt.getOrigStmt().originStmt;
            boolean needEncrypt = AuditEncryptionChecker.needEncrypt(parsedStmt);
            if (needEncrypt || Config.enable_sql_desensitize_in_log) {
                sql = AstToSQLBuilder.toSQL(parsedStmt, FormatOptions.allEnable()
                                .setColumnSimplifyTableName(false)
                                .setHideCredential(needEncrypt)
                                .setEnableDigest(Config.enable_sql_desensitize_in_log))
                        .orElse("this is a desensitized sql");
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
                    context.getCurrentCatalog(),
                    context.getCommandStr(),
                    getPreparedStmtId());
            // Set query source from context
            queryDetail.setQuerySource(context.getQuerySource());
            queryDetail.setImpersonatedUser(resolveImpersonatedUser());
            context.setQueryDetail(queryDetail);
            // copy queryDetail, cause some properties can be changed in future
            QueryDetailQueue.addQueryDetail(queryDetail.copy());
        } finally {
            context.setSessionVariable(sessionVariableBackup);
        }
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
        long queryFeMemory =
                ConnectProcessor.getThreadAllocatedBytes(Thread.currentThread().getId()) -
                        ctx.getCurrentThreadAllocatedMemory();

        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            queryDetail.setState(QueryDetail.QueryMemState.FAILED);
            queryDetail.setErrorMessage(ctx.getState().getErrorMessage());
        } else {
            queryDetail.setState(QueryDetail.QueryMemState.FINISHED);
        }
        queryDetail.setEndTime(endTime);
        queryDetail.setLatency(elapseMs);
        queryDetail.setQueryFeMemory(queryFeMemory);
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
            queryDetail.calculateCacheMissRatio(statistics.readLocalCnt == null ? 0 : statistics.readLocalCnt,
                    statistics.readRemoteCnt == null ? 0 : statistics.readRemoteCnt);
        }
        queryDetail.setCatalog(ctx.getCurrentCatalog());

        QueryDetailQueue.addQueryDetail(queryDetail);
    }

    protected boolean shouldMarkIdleCheck(StatementBase parsedStmt) {
        boolean isPreQuerySQL = false;
        boolean isInformationQuery = false;
        try {
            isPreQuerySQL = SqlUtils.isPreQuerySQL(parsedStmt);
            isInformationQuery = StatsConstants.INFORMATION_SCHEMA.equalsIgnoreCase(context.currentDb)
                    || SqlUtils.isInformationQuery(parsedStmt);
        } catch (Exception e) {
            LOG.warn("check isPreQuerySQL failed", e);
        }
        return !isInternalStmt
                && !isPreQuerySQL
                && !isInformationQuery
                && !(parsedStmt instanceof ShowStmt)
                && !(parsedStmt instanceof AdminSetConfigStmt);
    }

    private String resolveImpersonatedUser() {
        String qualifiedUser = ClusterNamespace.getNameFromFullName(context.getQualifiedUser());
        String currentUser = context.getCurrentUserIdentity() == null ? null :
                ClusterNamespace.getNameFromFullName(context.getCurrentUserIdentity().getUser());
        if (currentUser == null || qualifiedUser == null || currentUser.equals(qualifiedUser)) {
            return null;
        }
        return currentUser;
    }

    public double getMaxFilterRatio(DmlStmt dmlStmt) {
        Map<String, String> properties = dmlStmt.getProperties();
        if (properties.containsKey(LoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
            try {
                return Double.parseDouble(properties.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY));
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return ConnectContext.get().getSessionVariable().getInsertMaxFilterRatio();
    }
}
