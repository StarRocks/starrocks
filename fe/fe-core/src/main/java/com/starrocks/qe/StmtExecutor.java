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
import com.google.common.collect.Sets;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.Parameter;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.QueryDumpLog;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
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
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.HttpResultSender;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.InsertOverwriteJob;
import com.starrocks.load.InsertOverwriteJobMgr;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.meta.SqlBlackList;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlEofPacket;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.persist.CreateInsertOverwriteJobLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HiveTableSink;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.QueryStatisticsItemPB;
import com.starrocks.qe.QueryState.MysqlStateType;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ExplainAnalyzer;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeProfileStmt;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.DeallocateStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UnsupportedStmt;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.HistogramStatisticsCollectJob;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatisticsCollectJobFactory;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionCommitFailedException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.VisibleStateWaiter;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

// Do one COM_QUERY process.
// first: Parse receive byte array to statement struct.
// second: Do handle function for statement.
public class StmtExecutor {
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);

    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);

    private final ConnectContext context;
    private final MysqlSerializer serializer;
    private final OriginStatement originStmt;
    private StatementBase parsedStmt;
    private RuntimeProfile profile;
    private Coordinator coord = null;
    private LeaderOpExecutor leaderOpExecutor = null;
    private RedirectStatus redirectStatus = null;
    private final boolean isProxy;
    private List<ByteBuffer> proxyResultBuffer = null;
    private ShowResultSet proxyResultSet = null;
    private PQueryStatistics statisticsForAuditLog;
    private List<StmtExecutor> subStmtExecutors;

    private HttpResultSender httpResultSender;

    private PrepareStmtContext prepareStmtContext;


    // this constructor is mainly for proxy
    public StmtExecutor(ConnectContext context, OriginStatement originStmt, boolean isProxy) {
        this.context = context;
        this.originStmt = originStmt;
        this.serializer = context.getSerializer();
        this.isProxy = isProxy;
        if (isProxy) {
            proxyResultBuffer = new ArrayList<>();
        }
    }

    @VisibleForTesting
    public StmtExecutor(ConnectContext context, String stmt) {
        this(context, new OriginStatement(stmt, 0), false);
    }

    // constructor for receiving parsed stmt from connect processor
    public StmtExecutor(ConnectContext ctx, StatementBase parsedStmt) {
        this.context = ctx;
        this.parsedStmt = parsedStmt;
        this.originStmt = parsedStmt.getOrigStmt();
        this.serializer = context.getSerializer();
        this.isProxy = false;
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
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, originStmt.originStmt);

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
        Tracers.toRuntimeProfile(plannerProfile);;
        return profile;
    }

    public boolean isForwardToLeader() {
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            return false;
        }

        // this is a query stmt, but this non-master FE can not read, forward it to master
        if (parsedStmt instanceof QueryStatement && !GlobalStateMgr.getCurrentState().isLeader()
                && !GlobalStateMgr.getCurrentState().canRead()) {
            return true;
        }

        if (redirectStatus == null) {
            return false;
        } else {
            return redirectStatus.isForwardToLeader();
        }
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

    // Execute one statement.
    // Exception:
    //  IOException: talk with client failed.
    public void execute() throws Exception {
        long beginTimeInNanoSecond = TimeUtils.getStartTime();
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());

        // set execution id.
        // Try to use query id as execution id when execute first time.
        UUID uuid = context.getQueryId();
        context.setExecutionId(UUIDUtil.toTUniqueId(uuid));
        SessionVariable sessionVariableBackup = context.getSessionVariable();

        // if use http protocal, use httpResultSender to send result to netty channel
        if (context instanceof HttpConnectContext) {
            httpResultSender = new HttpResultSender((HttpConnectContext) context);
        }

        try {
            // parsedStmt may already by set when constructing this StmtExecutor();
            resolveParseStmtForForward();

            // support select hint e.g. select /*+ SET_VAR(query_timeout=1) */ sleep(3);
            if (parsedStmt != null) {
                Map<String, String> optHints = null;
                if (parsedStmt instanceof QueryStatement &&
                        ((QueryStatement) parsedStmt).getQueryRelation() instanceof SelectRelation) {
                    SelectRelation selectRelation = (SelectRelation) ((QueryStatement) parsedStmt).getQueryRelation();
                    optHints = selectRelation.getSelectList().getOptHints();
                }

                if (optHints != null) {
                    SessionVariable sessionVariable = (SessionVariable) sessionVariableBackup.clone();
                    for (String key : optHints.keySet()) {
                        VariableMgr.setSystemVariable(sessionVariable,
                                new SystemVariable(key, new StringLiteral(optHints.get(key))), true);
                    }
                    context.setSessionVariable(sessionVariable);
                }

                if (parsedStmt.isExplain()) {
                    context.setExplainLevel(parsedStmt.getExplainLevel());
                }
            }

            // execPlan is the output of new planner
            ExecPlan execPlan = null;
            boolean execPlanBuildByNewPlanner = false;

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
                                    "prepare statement can't be found @ %s, maybe has expired", executeStmt.getStmtName());
                        }
                        PrepareStmt prepareStmt = prepareStmtContext.getStmt();
                        parsedStmt = prepareStmt.assignValues(executeStmt.getParamsExpr());
                        parsedStmt.setOrigStmt(originStmt);
                        execPlan = StatementPlanner.plan(parsedStmt, context);
                    } else {
                        execPlan = StatementPlanner.plan(parsedStmt, context);
                        if (parsedStmt instanceof QueryStatement && context.shouldDumpQuery()) {
                            context.getDumpInfo().setExplainInfo(execPlan.getExplainString(TExplainLevel.COSTS));
                        }
                    }
                    execPlanBuildByNewPlanner = true;
                }
            } catch (SemanticException e) {
                dumpException(e);
                throw new AnalysisException(e.getMessage());
            } catch (StarRocksPlannerException e) {
                dumpException(e);
                if (e.getType().equals(ErrorType.USER_ERROR)) {
                    throw e;
                } else if (e.getType().equals(ErrorType.UNSUPPORTED) && e.getMessage().contains("UDF function")) {
                    LOG.warn("New planner not implement : " + originStmt.originStmt, e);
                    analyze(context.getSessionVariable().toThrift());
                } else {
                    LOG.warn("New planner error: " + originStmt.originStmt, e);
                    throw e;
                }
            }

            // no need to execute http query dump request in BE
            if (context.isHTTPQueryDump) {
                return;
            }
            if (isForwardToLeader()) {
                forwardToLeader();
                return;
            } else {
                LOG.debug("no need to transfer to Leader. stmt: {}", context.getStmtId());
            }

            if (parsedStmt instanceof QueryStatement) {
                context.getState().setIsQuery(true);
                final boolean isStatisticsJob = AnalyzerUtils.isStatisticsJob(context, parsedStmt);
                context.setStatisticsJob(isStatisticsJob);

                // sql's blacklist is enabled through enable_sql_blacklist.
                if (Config.enable_sql_blacklist && !parsedStmt.isExplain()) {
                    OriginStatement origStmt = parsedStmt.getOrigStmt();
                    if (origStmt != null) {
                        String originSql = origStmt.originStmt.trim()
                                .toLowerCase().replaceAll(" +", " ");
                        // If this sql is in blacklist, show message.
                        SqlBlackList.verifying(originSql);
                    }
                }

                // Record planner costs in audit log
                Preconditions.checkNotNull(execPlan, "query must has a plan");

                int retryTime = Config.max_query_retry_time;
                for (int i = 0; i < retryTime; i++) {
                    boolean needRetry = false;
                    try {
                        //reset query id for each retry
                        if (i > 0) {
                            uuid = UUID.randomUUID();
                            LOG.info("transfer QueryId: {} to {}", DebugUtil.printId(context.getQueryId()),
                                    DebugUtil.printId(uuid));
                            context.setExecutionId(
                                    new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                        }

                        Preconditions.checkState(execPlanBuildByNewPlanner, "must use new planner");

                        handleQueryStmt(execPlan);
                        break;
                    } catch (RemoteFileNotFoundException e) {
                        // If modifications are made to the partition files of a Hive table by user,
                        // such as through "insert overwrite partition", the Frontend couldn't be aware of these changes.
                        // As a result, queries may use the file information cached in the FE for execution.
                        // When the Backend cannot find the corresponding files, it returns a "Status::ACCESS_REMOTE_FILE_ERROR."
                        // To handle this exception, we perform a retry. Before initiating the retry, we need to
                        // refresh the metadata cache for the table and clear the query-level metadata cache.
                        if (i == retryTime - 1) {
                            throw e;
                        }

                        List<ScanNode> scanNodes = execPlan.getScanNodes();
                        boolean existExternalCatalog = false;
                        for (ScanNode scanNode : scanNodes) {
                            if (scanNode instanceof HdfsScanNode) {
                                HiveTable hiveTable = ((HdfsScanNode) scanNode).getHiveTable();
                                String catalogName = hiveTable.getCatalogName();
                                if (CatalogMgr.isExternalCatalog(catalogName)) {
                                    existExternalCatalog = true;
                                    ConnectorMetadata metadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                                            .getOptionalMetadata(hiveTable.getCatalogName()).get();
                                    // refresh catalog level metadata cache
                                    metadata.refreshTable(hiveTable.getDbName(), hiveTable, new ArrayList<>(), true);
                                    // clear query level metadata cache
                                    metadata.clear();
                                }
                            }
                        }

                        if (!existExternalCatalog) {
                            throw e;
                        }

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
                        Tracers.record(Tracers.Module.EXTERNAL, "HMS.RETRY", String.valueOf(i + 1));
                    } catch (RpcException e) {
                        // When enable_collect_query_detail_info is set to true, the plan will be recorded in the query detail,
                        // and hence there is no need to log it here.
                        if (i == 0 && context.getQueryDetail() == null && Config.log_plan_cancelled_by_crash_be) {
                            LOG.warn(
                                    "Query cancelled by crash of backends or RpcException, [QueryId={}] [SQL={}] [Plan={}]",
                                    DebugUtil.printId(context.getExecutionId()),
                                    originStmt == null ? "" : originStmt.originStmt,
                                    execPlan.getExplainString(TExplainLevel.COSTS),
                                    e);
                        }
                        if (i == retryTime - 1) {
                            throw e;
                        }
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
                        if (!needRetry && context.isProfileEnabled()) {
                            isAsync = tryProcessProfileAsync(execPlan);
                            if (parsedStmt.isExplain() &&
                                    StatementBase.ExplainLevel.ANALYZE.equals(parsedStmt.getExplainLevel())) {
                                handleExplainStmt(ExplainAnalyzer.analyze(
                                        ProfilingExecPlan.buildFrom(execPlan), profile, null));
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
            } else if (parsedStmt instanceof SetStmt) {
                handleSetStmt();
            } else if (parsedStmt instanceof UseDbStmt) {
                handleUseDbStmt();
            } else if (parsedStmt instanceof UseCatalogStmt) {
                handleUseCatalogStmt();
            } else if (parsedStmt instanceof SetCatalogStmt) {
                handleSetCatalogStmt();
            } else if (parsedStmt instanceof CreateTableAsSelectStmt) {
                if (execPlanBuildByNewPlanner) {
                    handleCreateTableAsSelectStmt(beginTimeInNanoSecond);
                } else {
                    throw new AnalysisException("old planner does not support CTAS statement");
                }
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
            } else {
                context.getState().setError("Do not support this query.");
            }
        } catch (IOException e) {
            LOG.warn("execute IOException ", e);
            // the exception happens when interact with client
            // this exception shows the connection is gone
            context.getState().setError(e.getMessage());
            throw e;
        } catch (UserException e) {
            String sql = originStmt != null ? originStmt.originStmt : "";
            // analysis exception only print message, not print the stack
            LOG.info("execute Exception, sql: {}, error: {}", sql, e.getMessage());
            context.getState().setError(e.getMessage());
            context.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } catch (Throwable e) {
            String sql = originStmt != null ? originStmt.originStmt : "";
            LOG.warn("execute Exception, sql " + sql, e);
            context.getState().setError(e.getMessage());
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                context.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            }
        } finally {
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
            if (context.getState().isError() && coord != null) {
                coord.cancel(context.getState().getErrorMessage());
            }

            if (parsedStmt instanceof InsertStmt && !parsedStmt.isExplain()) {
                // sql's blacklist is enabled through enable_sql_blacklist.
                if (Config.enable_sql_blacklist) {
                    OriginStatement origStmt = parsedStmt.getOrigStmt();
                    if (origStmt != null) {
                        String originSql = origStmt.originStmt.trim()
                                .toLowerCase().replaceAll(" +", " ");
                        // If this sql is in blacklist, show message.
                        SqlBlackList.verifying(originSql);
                    }
                }
            }

            context.setSessionVariable(sessionVariableBackup);
        }
    }

    private void handleCreateTableAsSelectStmt(long beginTimeInNanoSecond) throws Exception {
        CreateTableAsSelectStmt createTableAsSelectStmt = (CreateTableAsSelectStmt) parsedStmt;

        if (!createTableAsSelectStmt.createTable(context)) {
            return;
        }
        // if create table failed should not drop table. because table may already exist,
        // and for other cases the exception will throw and the rest of the code will not be executed.
        try {
            InsertStmt insertStmt = createTableAsSelectStmt.getInsertStmt();
            ExecPlan execPlan = new StatementPlanner().plan(insertStmt, context);
            handleDMLStmtWithProfile(execPlan, ((CreateTableAsSelectStmt) parsedStmt).getInsertStmt());
            if (context.getState().getStateType() == MysqlStateType.ERR) {
                ((CreateTableAsSelectStmt) parsedStmt).dropTable(context);
            }
        } catch (Throwable t) {
            LOG.warn("handle create table as select stmt fail", t);
            ((CreateTableAsSelectStmt) parsedStmt).dropTable(context);
            throw t;
        }
    }

    private void resolveParseStmtForForward() throws AnalysisException {
        if (parsedStmt == null) {
            List<StatementBase> stmts;
            try {
                stmts = com.starrocks.sql.parser.SqlParser.parse(originStmt.originStmt,
                        context.getSessionVariable());
                parsedStmt = stmts.get(originStmt.idx);
                parsedStmt.setOrigStmt(originStmt);
            } catch (ParsingException parsingException) {
                throw new AnalysisException(parsingException.getMessage());
            }
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
        leaderOpExecutor = new LeaderOpExecutor(parsedStmt, originStmt, context, redirectStatus);
        LOG.debug("need to transfer to Leader. stmt: {}", context.getStmtId());
        leaderOpExecutor.execute();
    }

    private boolean tryProcessProfileAsync(ExecPlan plan) {
        if (coord == null || coord.getQueryProfile() == null) {
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

            ProfilingExecPlan profilingPlan = plan == null ? null : plan.getProfilingPlan();
            String profileContent = ProfileManager.getInstance().pushProfile(profilingPlan, profile);
            if (queryDetail != null) {
                queryDetail.setProfile(profileContent);
            }
            QeProcessorImpl.INSTANCE.unMonitorQuery(executionId);
            QeProcessorImpl.INSTANCE.unregisterQuery(executionId);
        };
        return coord.tryProcessProfileAsync(task);
    }

    // Analyze one statement to structure in memory.
    public void analyze(TQueryOptions tQueryOptions) throws UserException {
        LOG.info("begin to analyze stmt: {}, forwarded stmt id: {}", context.getStmtId(), context.getForwardedStmtId());

        // parsedStmt may already by set when constructing this StmtExecutor();
        resolveParseStmtForForward();
        redirectStatus = parsedStmt.getRedirectStatus();

        // yiguolei: insertstmt's grammar analysis will write editlog, so that we check if the stmt should be forward to master here
        // if the stmt should be forward to master, then just return here and the master will do analysis again
        if (isForwardToLeader()) {
            return;
        }

        // Convert show statement to select statement here
        if (parsedStmt instanceof ShowStmt) {
            QueryStatement selectStmt = ((ShowStmt) parsedStmt).toSelectStmt();
            if (selectStmt != null) {
                Preconditions.checkState(false, "Shouldn't reach here");
            }
        }

        try {
            parsedStmt.analyze(new Analyzer(context.getGlobalStateMgr(), context));
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Analyze failed because ", e);
            throw new AnalysisException("Unexpected exception: " + e.getMessage());
        }

    }

    public void registerSubStmtExecutor(StmtExecutor subStmtExecutor) {
        if (subStmtExecutors == null) {
            subStmtExecutors = Lists.newArrayList();
        }
        subStmtExecutors.add(subStmtExecutor);
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
        long id = killStmt.getConnectionId();
        ConnectContext killCtx = context.getConnectScheduler().getContext(id);
        if (killCtx == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_THREAD, id);
        }
        Preconditions.checkNotNull(killCtx);
        if (context == killCtx) {
            // Suicide
            context.setKilled();
        } else {
            if (!Objects.equals(killCtx.getQualifiedUser(), context.getQualifiedUser())) {
                try {
                    Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.OPERATE);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
                }
            }
            killCtx.kill(killStmt.isConnectionKill(), "killed by kiil statement : " + originStmt);
        }
        context.getState().setOk();
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

    // Process a select statement.
    private void handleQueryStmt(ExecPlan execPlan) throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();

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
        } else if (parsedStmt.isExplain()) {
            handleExplainStmt(buildExplainString(execPlan, ResourceGroupClassifier.QueryType.SELECT));
            return;
        }
        if (context.getQueryDetail() != null) {
            context.getQueryDetail().setExplain(buildExplainString(execPlan, ResourceGroupClassifier.QueryType.SELECT));
        }

        StatementBase queryStmt = parsedStmt;
        List<PlanFragment> fragments = execPlan.getFragments();
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        TDescriptorTable descTable = execPlan.getDescTbl().toThrift();
        List<String> colNames = execPlan.getColNames();
        List<Expr> outputExprs = execPlan.getOutputExprs();

        coord = getCoordinatorFactory().createQueryScheduler(context, fragments, scanNodes, descTable);

        QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(),
                new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));

        if (isSchedulerExplain) {
            coord.startSchedulingWithoutDeploy();
            handleExplainStmt(coord.getSchedulerExplain());
            return;
        }

        coord.exec();
        coord.setTopProfileSupplier(this::buildTopLevelProfile);
        coord.setExecPlan(execPlan);

        RowBatch batch;
        boolean isOutfileQuery = false;
        if (queryStmt instanceof QueryStatement) {
            isOutfileQuery = ((QueryStatement) queryStmt).hasOutFileClause();
        }

        if (context instanceof HttpConnectContext) {
            batch = httpResultSender.sendQueryResult(coord, execPlan);
        } else {
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
                if (batch.getBatch() != null && !isOutfileQuery && !isExplainAnalyze) {
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
            if (!isSendFields && !isOutfileQuery && !isExplainAnalyze) {
                sendFields(colNames, outputExprs);
            }
        }

        statisticsForAuditLog = batch.getQueryStatistics();
        if (!isOutfileQuery) {
            context.getState().setEof();
        } else {
            context.getState().setOk(statisticsForAuditLog.returnedRows, 0, "");
        }
        if (null == statisticsForAuditLog || null == statisticsForAuditLog.statsItems ||
                statisticsForAuditLog.statsItems.isEmpty()) {
            return;
        }
        // collect table-level metrics
        Set<Long> tableIds = Sets.newHashSet();
        for (QueryStatisticsItemPB item : statisticsForAuditLog.statsItems) {
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

    private void handleAnalyzeStmt() throws IOException {
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) parsedStmt;
        Database db = MetaUtils.getDatabase(context, analyzeStmt.getTableName());
        Table table = MetaUtils.getTable(context, analyzeStmt.getTableName());
        if (StatisticUtils.isEmptyTable(table)) {
            return;
        }

        StatsConstants.AnalyzeType analyzeType;
        if (analyzeStmt.getAnalyzeTypeDesc() instanceof AnalyzeHistogramDesc) {
            analyzeType = StatsConstants.AnalyzeType.HISTOGRAM;
        } else {
            if (analyzeStmt.isSample()) {
                analyzeType = StatsConstants.AnalyzeType.SAMPLE;
            } else {
                analyzeType = StatsConstants.AnalyzeType.FULL;
            }
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
        GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.PENDING);
        GlobalStateMgr.getCurrentAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);

        int timeout = context.getSessionVariable().getQueryTimeoutS();
        try {
            Future<?> future = GlobalStateMgr.getCurrentAnalyzeMgr().getAnalyzeTaskThreadPool()
                    .submit(() -> executeAnalyze(analyzeStmt, analyzeStatus, db, table));

            if (!analyzeStmt.isAsync()) {
                // sync statistics collection doesn't be interrupted by query timeout, but
                // will print warning log if timeout, so we update timeout temporarily to avoid
                // warning log
                context.getSessionVariable().setQueryTimeoutS((int) Config.statistic_collect_query_timeout);
                future.get();
            }
        } catch (RejectedExecutionException e) {
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            analyzeStatus.setReason("The statistics tasks running concurrently exceed the upper limit");
            GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        } catch (ExecutionException | InterruptedException e) {
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            analyzeStatus.setReason("The statistics tasks running failed");
            GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        } finally {
            context.getSessionVariable().setQueryTimeoutS(timeout);
        }

        ShowResultSet resultSet = analyzeStatus.toShowResult();
        if (isProxy) {
            proxyResultSet = resultSet;
            context.getState().setEof();
            return;
        }

        sendShowResult(resultSet);
    }

    private void handleAnalyzeProfileStmt() throws IOException {
        AnalyzeProfileStmt analyzeProfileStmt = (AnalyzeProfileStmt) parsedStmt;
        String queryId = analyzeProfileStmt.getQueryId();
        List<Integer> planNodeIds = analyzeProfileStmt.getPlanNodeIds();
        ProfileManager.ProfileElement profileElement = ProfileManager.getInstance().getProfileElement(queryId);
        Preconditions.checkNotNull(profileElement, "query not exists");
        handleExplainStmt(ExplainAnalyzer.analyze(profileElement.plan,
                RuntimeProfileParser.parseFrom(CompressionUtils.gzipDecompressString(profileElement.profileContent)),
                planNodeIds));
    }

    private void executeAnalyze(AnalyzeStmt analyzeStmt, AnalyzeStatus analyzeStatus, Database db, Table table) {
        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        // from current session, may execute analyze stmt
        statsConnectCtx.getSessionVariable().setStatisticCollectParallelism(
                context.getSessionVariable().getStatisticCollectParallelism());
        statsConnectCtx.setThreadLocalInfo();
        executeAnalyze(statsConnectCtx, analyzeStmt, analyzeStatus, db, table);
    }

    private void executeAnalyze(ConnectContext statsConnectCtx, AnalyzeStmt analyzeStmt, AnalyzeStatus analyzeStatus,
                                Database db, Table table) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        if (analyzeStmt.isExternal()) {
            StatsConstants.AnalyzeType analyzeType = analyzeStmt.isSample() ? StatsConstants.AnalyzeType.SAMPLE :
                    StatsConstants.AnalyzeType.FULL;
            // TODO: we should check old statistic and confirm paritionlist
            statisticExecutor.collectStatistics(statsConnectCtx,
                    StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(
                            analyzeStmt.getTableName().getCatalog(),
                            db, table, null,
                            analyzeStmt.getColumnNames(),
                            analyzeType,
                            StatsConstants.ScheduleType.ONCE, analyzeStmt.getProperties()),
                    analyzeStatus,
                    false);
        } else {
            if (analyzeStmt.getAnalyzeTypeDesc() instanceof AnalyzeHistogramDesc) {
                statisticExecutor.collectStatistics(statsConnectCtx,
                        new HistogramStatisticsCollectJob(db, table, analyzeStmt.getColumnNames(),
                                StatsConstants.AnalyzeType.HISTOGRAM, StatsConstants.ScheduleType.ONCE,
                                analyzeStmt.getProperties()),
                        analyzeStatus,
                        // Sync load cache, auto-populate column statistic cache after Analyze table manually
                        false);
            } else {
                StatsConstants.AnalyzeType analyzeType = analyzeStmt.isSample() ? StatsConstants.AnalyzeType.SAMPLE :
                        StatsConstants.AnalyzeType.FULL;
                statisticExecutor.collectStatistics(statsConnectCtx,
                        StatisticsCollectJobFactory.buildStatisticsCollectJob(db, table, null,
                                analyzeStmt.getColumnNames(),
                                analyzeType,
                                StatsConstants.ScheduleType.ONCE, analyzeStmt.getProperties()),
                        analyzeStatus,
                        // Sync load cache, auto-populate column statistic cache after Analyze table manually
                        false);
            }
        }
    }

    private void handleDropStatsStmt() {
        DropStatsStmt dropStatsStmt = (DropStatsStmt) parsedStmt;
        Table table = MetaUtils.getTable(context, dropStatsStmt.getTableName());
        if (dropStatsStmt.isExternal()) {
            GlobalStateMgr.getCurrentAnalyzeMgr().dropExternalAnalyzeStatus(table.getUUID());
            GlobalStateMgr.getCurrentAnalyzeMgr().dropExternalBasicStatsData(table.getUUID());

            TableName tableName = dropStatsStmt.getTableName();
            GlobalStateMgr.getCurrentAnalyzeMgr().removeExternalBasicStatsMeta(tableName.getCatalog(),
                    tableName.getDb(), tableName.getTbl());
            List<String> columns = table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());
            GlobalStateMgr.getCurrentStatisticStorage().expireConnectorTableColumnStatistics(table, columns);
        } else {
            List<String> columns = table.getBaseSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                    .collect(Collectors.toList());

            GlobalStateMgr.getCurrentAnalyzeMgr().dropAnalyzeStatus(table.getId());
            GlobalStateMgr.getCurrentAnalyzeMgr()
                    .dropBasicStatsMetaAndData(StatisticUtils.buildConnectContext(), Sets.newHashSet(table.getId()));
            GlobalStateMgr.getCurrentStatisticStorage().expireTableAndColumnStatistics(table, columns);
        }
    }

    private void handleDropHistogramStmt() {
        DropHistogramStmt dropHistogramStmt = (DropHistogramStmt) parsedStmt;
        OlapTable table = (OlapTable) MetaUtils.getTable(context, dropHistogramStmt.getTableName());
        List<String> columns = table.getBaseSchema().stream().filter(d -> !d.isAggregated()).map(Column::getName)
                .collect(Collectors.toList());

        GlobalStateMgr.getCurrentAnalyzeMgr().dropAnalyzeStatus(table.getId());
        GlobalStateMgr.getCurrentAnalyzeMgr()
                .dropHistogramStatsMetaAndData(StatisticUtils.buildConnectContext(), Sets.newHashSet(table.getId()));
        GlobalStateMgr.getCurrentStatisticStorage().expireHistogramStatistics(table.getId(), columns);
    }

    private void handleKillAnalyzeStmt() {
        KillAnalyzeStmt killAnalyzeStmt = (KillAnalyzeStmt) parsedStmt;
        long analyzeId = killAnalyzeStmt.getAnalyzeId();
        AnalyzeMgr analyzeManager = GlobalStateMgr.getCurrentAnalyzeMgr();
        checkPrivilegeForKillAnalyzeStmt(context, analyzeId);
        // Try to kill the job anyway.
        analyzeManager.killConnection(analyzeId);
    }

    private void checkTblPrivilegeForKillAnalyzeStmt(ConnectContext context, String catalogName, String dbName,
                                                     String tableName, long analyzeId) {
        MetaUtils.getDatabase(catalogName, dbName);
        MetaUtils.getTable(catalogName, dbName, tableName);

        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    catalogName, dbName, tableName, PrivilegeType.SELECT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    catalogName,
                    ConnectContext.get().getCurrentUserIdentity(),
                    ConnectContext.get().getCurrentRoleIds(), PrivilegeType.SELECT.name(), ObjectType.TABLE.name(), tableName);
        }

        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    catalogName, dbName, tableName, PrivilegeType.INSERT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    catalogName,
                    ConnectContext.get().getCurrentUserIdentity(),
                    ConnectContext.get().getCurrentRoleIds(), PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName);
        }
    }

    public void checkPrivilegeForKillAnalyzeStmt(ConnectContext context, long analyzeId) {
        AnalyzeMgr analyzeManager = GlobalStateMgr.getCurrentAnalyzeMgr();
        AnalyzeStatus analyzeStatus = analyzeManager.getAnalyzeStatus(analyzeId);
        AnalyzeJob analyzeJob = analyzeManager.getAnalyzeJob(analyzeId);
        if (analyzeStatus != null) {
            try {
                String catalogName = analyzeStatus.getCatalogName();
                String dbName = analyzeStatus.getDbName();
                String tableName = analyzeStatus.getTableName();
                checkTblPrivilegeForKillAnalyzeStmt(context, catalogName, dbName, tableName, analyzeId);
            } catch (MetaNotFoundException ignore) {
                // If the db or table doesn't exist anymore, we won't check privilege on it
            }
        } else if (analyzeJob != null && analyzeJob.isNative()) {
            NativeAnalyzeJob nativeAnalyzeJob = (NativeAnalyzeJob) analyzeJob;
            Set<TableName> tableNames = AnalyzerUtils.getAllTableNamesForAnalyzeJobStmt(nativeAnalyzeJob.getDbId(),
                    nativeAnalyzeJob.getTableId());
            tableNames.forEach(tableName ->
                    checkTblPrivilegeForKillAnalyzeStmt(context, tableName.getCatalog(), tableName.getDb(),
                            tableName.getTbl(), analyzeId)
            );
        }
    }

    private void handleAddSqlBlackListStmt() {
        AddSqlBlackListStmt addSqlBlackListStmt = (AddSqlBlackListStmt) parsedStmt;
        SqlBlackList.getInstance().put(addSqlBlackListStmt.getSqlPattern());
    }

    private void handleDelSqlBlackListStmt() {
        DelSqlBlackListStmt delSqlBlackListStmt = (DelSqlBlackListStmt) parsedStmt;
        List<Long> indexs = delSqlBlackListStmt.getIndexs();
        if (indexs != null) {
            for (long id : indexs) {
                SqlBlackList.getInstance().delete(id);
            }
        }
    }

    private void handleExecAsStmt() throws UserException {
        ExecuteAsExecutor.execute((ExecuteAsStmt) parsedStmt, context);
    }

    private void handleExecScriptStmt() throws IOException, UserException {
        ShowResultSet resultSet = ExecuteScriptExecutor.execute((ExecuteScriptStmt) parsedStmt, context);
        if (isProxy) {
            proxyResultSet = resultSet;
            context.getState().setEof();
            return;
        }
        sendShowResult(resultSet);
    }

    private void handleSetRole() throws PrivilegeException, UserException {
        SetRoleExecutor.execute((SetRoleStmt) parsedStmt, context);
    }

    private void handleSetDefaultRole() throws PrivilegeException, UserException {
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
            context.getGlobalStateMgr().changeCatalogDb(context, useDbStmt.getIdentifier());
        } catch (Exception e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    // Process use catalog statement
    private void handleUseCatalogStmt() throws AnalysisException {
        UseCatalogStmt useCatalogStmt = (UseCatalogStmt) parsedStmt;
        try {
            String catalogName = useCatalogStmt.getCatalogName();
            context.getGlobalStateMgr().changeCatalog(context, catalogName);
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
            context.getGlobalStateMgr().changeCatalog(context, catalogName);
        } catch (Exception e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
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
        ShowExecutor executor = new ShowExecutor(context, (ShowStmt) parsedStmt);
        ShowResultSet resultSet = executor.execute();
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

    private String buildExplainString(ExecPlan execPlan, ResourceGroupClassifier.QueryType queryType) {
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
            } else {
                explainString += execPlan.getExplainString(parsedStmt.getExplainLevel());
            }
        }
        return explainString;
    }

    private void handleDdlStmt() {
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
        } catch (Throwable e) {
            // Maybe our bug or wrong input parameters
            String sql = AstToStringBuilder.toString(parsedStmt);
            if (sql == null || sql.isEmpty()) {
                sql = originStmt.originStmt;
            }
            LOG.warn("DDL statement (" + sql + ") process failed.", e);
            context.getState().setError("Unexpected exception: " + e.getMessage());
        }
    }

    private void handleExportStmt(UUID queryId) throws Exception {
        ExportStmt exportStmt = (ExportStmt) parsedStmt;
        exportStmt.setExportStartTime(context.getStartTime());
        context.getGlobalStateMgr().getExportMgr().addExportJob(queryId, exportStmt);
    }

    private void handleUpdateFailPointStatusStmt() throws Exception {
        FailPointExecutor executor = new FailPointExecutor(context, parsedStmt);
        executor.execute();
    }

    private void handleDeallocateStmt() throws Exception {
        DeallocateStmt deallocateStmt = (DeallocateStmt) parsedStmt;
        String stmtName = deallocateStmt.getStmtName();
        if (context.getPreparedStmt(stmtName) == null) {
            throw new UserException("PrepareStatement `" + stmtName + "` not exist");
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
        serializer.reset();
        // 0x00 OK
        serializer.writeInt1(0);
        // statement_id
        serializer.writeInt4(Integer.valueOf(prepareStmt.getName()));
        // num_columns
        int numColumns = 0;
        serializer.writeInt2(numColumns);
        // num_params
        int numParams = prepareStmt.getParameters().size();
        serializer.writeInt2(numParams);
        // reserved_1
        serializer.writeInt1(0);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        if (numParams > 0) {
            List<String> colNames = prepareStmt.getParameterLabels();
            List<Parameter> parameters = prepareStmt.getParameters();
            for (int i = 0; i < colNames.size(); ++i) {
                serializer.reset();
                serializer.writeField(colNames.get(i), parameters.get(i).getType());
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
        }
        context.getState().setEof();
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
        Database database = MetaUtils.getDatabase(context, insertStmt.getTableName());
        Table table = insertStmt.getTargetTable();
        if (!(table instanceof OlapTable)) {
            LOG.warn("insert overwrite table:{} type:{} is not supported", table.getName(), table.getClass());
            throw new RuntimeException("not supported table type for insert overwrite");
        }
        OlapTable olapTable = (OlapTable) insertStmt.getTargetTable();
        InsertOverwriteJob job = new InsertOverwriteJob(GlobalStateMgr.getCurrentState().getNextId(),
                insertStmt, database.getId(), olapTable.getId());
        if (!database.writeLockAndCheckExist()) {
            throw new DmlException("database:%s does not exist.", database.getFullName());
        }
        try {
            // add an edit log
            CreateInsertOverwriteJobLog info = new CreateInsertOverwriteJobLog(job.getJobId(),
                    job.getTargetDbId(), job.getTargetTableId(), job.getSourcePartitionIds());
            GlobalStateMgr.getCurrentState().getEditLog().logCreateInsertOverwrite(info);
        } finally {
            database.writeUnlock();
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
            if (context.isProfileEnabled()) {
                isAsync = tryProcessProfileAsync(execPlan);
                if (parsedStmt.isExplain() &&
                        StatementBase.ExplainLevel.ANALYZE.equals(parsedStmt.getExplainLevel())) {
                    handleExplainStmt(ExplainAnalyzer.analyze(ProfilingExecPlan.buildFrom(execPlan), profile, null));
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
            handleExplainStmt(buildExplainString(execPlan, ResourceGroupClassifier.QueryType.INSERT));
            return;
        }
        if (context.getQueryDetail() != null) {
            context.getQueryDetail().setExplain(buildExplainString(execPlan, ResourceGroupClassifier.QueryType.INSERT));
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

        MetaUtils.normalizationTableName(context, stmt.getTableName());
        String catalogName = stmt.getTableName().getCatalog();
        String dbName = stmt.getTableName().getDb();
        String tableName = stmt.getTableName().getTbl();
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();

        final Table targetTable;
        if (stmt instanceof InsertStmt && ((InsertStmt) stmt).getTargetTable() != null) {
            targetTable = ((InsertStmt) stmt).getTargetTable();
        } else {
            targetTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName, dbName, tableName);
        }

        if (isExplainAnalyze) {
            Preconditions.checkState(targetTable instanceof OlapTable,
                    "explain analyze only supports insert into olap native table");
        }

        if (parsedStmt instanceof InsertStmt && ((InsertStmt) parsedStmt).isOverwrite() &&
                !((InsertStmt) parsedStmt).hasOverwriteJob() &&
                !(targetTable.isIcebergTable() || targetTable.isHiveTable())) {
            handleInsertOverwrite((InsertStmt) parsedStmt);
            return;
        }

        String label = DebugUtil.printId(context.getExecutionId());
        if (stmt instanceof InsertStmt) {
            String stmtLabel = ((InsertStmt) stmt).getLabel();
            label = Strings.isNullOrEmpty(stmtLabel) ? "insert_" + label : stmtLabel;
        } else if (stmt instanceof UpdateStmt) {
            label = "update_" + label;
        } else if (stmt instanceof DeleteStmt) {
            label = "delete_" + label;
        } else {
            throw unsupportedException(
                    "Unsupported dml statement " + parsedStmt.getClass().getSimpleName());
        }

        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);
        long transactionId = -1;
        TransactionState txnState = null;
        if (targetTable instanceof ExternalOlapTable) {
            ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
            TAuthenticateParams authenticateParams = new TAuthenticateParams();
            authenticateParams.setUser(externalTable.getSourceTableUser());
            authenticateParams.setPasswd(externalTable.getSourceTablePassword());
            authenticateParams.setHost(context.getRemoteIP());
            authenticateParams.setDb_name(externalTable.getSourceTableDbName());
            authenticateParams.setTable_names(Lists.newArrayList(externalTable.getSourceTableName()));
            transactionId = transactionMgr.beginRemoteTransaction(externalTable.getSourceTableDbId(),
                                    Lists.newArrayList(externalTable.getSourceTableId()), label,
                                    externalTable.getSourceTableHost(),
                                    externalTable.getSourceTablePort(),
                                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                                            FrontendOptions.getLocalHostAddress()),
                                    sourceType,
                                    context.getSessionVariable().getQueryTimeoutS(),
                                    authenticateParams);
        } else if (targetTable instanceof SystemTable || targetTable.isIcebergTable() || targetTable.isHiveTable()
                || targetTable.isTableFunctionTable()) {
            // schema table and iceberg and hive table does not need txn
        } else {
            transactionId = transactionMgr.beginTransaction(
                    database.getId(),
                    Lists.newArrayList(targetTable.getId()),
                    label,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                            FrontendOptions.getLocalHostAddress()),
                    sourceType,
                    context.getSessionVariable().getQueryTimeoutS());

            // The metadata may be changed between plan() and beginTransaction().
            // When to beginTransaction(), the plan is not ready and the tablet is dropped by balance.
            // So we need to get txn first, to make the dropping tablet procedure to wait.
            // This is the re-plan of the insert to fix the issue

            if (Config.replan_on_insert) {
                execPlan = StatementPlanner.plan(stmt, context);
            }

            // add table indexes to transaction state
            txnState = transactionMgr.getTransactionState(database.getId(), transactionId);
            if (txnState == null) {
                throw new DdlException("txn does not exist: " + transactionId);
            }
            if (targetTable instanceof OlapTable) {
                txnState.addTableIndexes((OlapTable) targetTable);
            }
        }
        // Every time set no send flag and clean all data in buffer
        if (context.getMysqlChannel() != null) {
            context.getMysqlChannel().reset();
        }
        long createTime = System.currentTimeMillis();

        long loadedRows = 0;
        int filteredRows = 0;
        long loadedBytes = 0;
        long jobId = -1;
        long estimateScanRows = -1;
        TransactionStatus txnStatus = TransactionStatus.ABORTED;
        boolean insertError = false;
        String trackingSql = "";
        try {
            if (execPlan.getFragments().get(0).getSink() instanceof OlapTableSink) {
                // if sink is OlapTableSink Assigned to Be execute this sql [cn execute OlapTableSink will crash]
                context.getSessionVariable().setPreferComputeNode(false);
                context.getSessionVariable().setUseComputeNodes(0);
                OlapTableSink dataSink = (OlapTableSink) execPlan.getFragments().get(0).getSink();
                dataSink.init(context.getExecutionId(), transactionId, database.getId(),
                        ConnectContext.get().getSessionVariable().getQueryTimeoutS());
                dataSink.complete();
            }

            coord = getCoordinatorFactory().createInsertScheduler(
                    context, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift());

            List<ScanNode> scanNodes = execPlan.getScanNodes();

            boolean containOlapScanNode = false;
            for (ScanNode scanNode : scanNodes) {
                if (scanNode instanceof OlapScanNode) {
                    estimateScanRows += ((OlapScanNode) scanNode).getActualRows();
                    containOlapScanNode = true;
                }
            }

            TLoadJobType type;
            if (containOlapScanNode) {
                coord.setLoadJobType(TLoadJobType.INSERT_QUERY);
                type = TLoadJobType.INSERT_QUERY;
            } else {
                estimateScanRows = execPlan.getFragments().get(0).getPlanRoot().getCardinality();
                coord.setLoadJobType(TLoadJobType.INSERT_VALUES);
                type = TLoadJobType.INSERT_VALUES;
            }

            context.setStatisticsJob(AnalyzerUtils.isStatisticsJob(context, parsedStmt));
            if (!(targetTable.isIcebergTable() || targetTable.isHiveTable() || targetTable.isTableFunctionTable())) {
                jobId = context.getGlobalStateMgr().getLoadMgr().registerLoadJob(
                        label,
                        database.getFullName(),
                        targetTable.getId(),
                        EtlJobType.INSERT,
                        createTime,
                        estimateScanRows,
                        type,
                        ConnectContext.get().getSessionVariable().getQueryTimeoutS());
            }

            coord.setLoadJobId(jobId);
            trackingSql = "select tracking_log from information_schema.load_tracking_logs where job_id=" + jobId;

            QeProcessorImpl.QueryInfo queryInfo = new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord);
            QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(), queryInfo);

            if (isSchedulerExplain) {
                coord.startSchedulingWithoutDeploy();
                handleExplainStmt(coord.getSchedulerExplain());
                return;
            }

            coord.exec();
            coord.setTopProfileSupplier(this::buildTopLevelProfile);
            coord.setExecPlan(execPlan);

            long jobDeadLineMs = System.currentTimeMillis() + context.getSessionVariable().getQueryTimeoutS() * 1000;
            coord.join(context.getSessionVariable().getQueryTimeoutS());
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

                    coord.cancel(ErrorCode.ERR_QUERY_EXCEPTION.formatErrorMsg());
                    ErrorReport.reportDdlException(ErrorCode.ERR_QUERY_EXCEPTION);
                } else {
                    coord.cancel(ErrorCode.ERR_QUERY_TIMEOUT.formatErrorMsg());
                    if (coord.isThriftServerHighLoad()) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_QUERY_TIMEOUT,
                                "Please check the thrift-server-pool metrics, " +
                                        "if the pool size reaches thrift_server_max_worker_threads(default is 4096), " +
                                        "you can set the config to a higher value in fe.conf, " +
                                        "or set parallel_fragment_exec_instance_num to a lower value in session variable");
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_QUERY_TIMEOUT,
                                "Increase the query_timeout session variable and retry");
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
                filteredRows = Integer.parseInt(coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL));
            }

            if (coord.getLoadCounters().get(LoadJob.LOADED_BYTES) != null) {
                loadedBytes = Long.parseLong(coord.getLoadCounters().get(LoadJob.LOADED_BYTES));
            }

            // if in strict mode, insert will fail if there are filtered rows
            if (context.getSessionVariable().getEnableInsertStrict()) {
                if (filteredRows > 0) {
                    if (targetTable instanceof ExternalOlapTable) {
                        ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
                        transactionMgr.abortRemoteTransaction(
                                externalTable.getSourceTableDbId(), transactionId,
                                externalTable.getSourceTableHost(),
                                externalTable.getSourceTablePort(),
                                TransactionCommitFailedException.FILTER_DATA_IN_STRICT_MODE + ", tracking sql = " +
                                        trackingSql
                        );
                    } else if (targetTable instanceof SystemTable || targetTable.isHiveTable() ||
                            targetTable.isIcebergTable() || targetTable.isTableFunctionTable()) {
                        // schema table does not need txn
                    } else {
                        transactionMgr.abortTransaction(
                                database.getId(),
                                transactionId,
                                TransactionCommitFailedException.FILTER_DATA_IN_STRICT_MODE + ", tracking sql = " +
                                        trackingSql,
                                TabletFailInfo.fromThrift(coord.getFailInfos())
                        );
                    }
                    context.getState().setError("Insert has filtered data in strict mode, txn_id = " + transactionId +
                            " tracking sql = " + trackingSql);
                    insertError = true;
                    return;
                }
            }

            if (loadedRows == 0 && filteredRows == 0 && (stmt instanceof DeleteStmt || stmt instanceof InsertStmt
                    || stmt instanceof UpdateStmt)) {
                // when the target table is not ExternalOlapTable or OlapTable
                // if there is no data to load, the result of the insert statement is success
                // otherwise, the result of the insert statement is failed
                GlobalTransactionMgr mgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
                String errorMsg = TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG;
                if (!(targetTable instanceof ExternalOlapTable || targetTable instanceof OlapTable)) {
                    if (!(targetTable instanceof SystemTable || targetTable instanceof IcebergTable ||
                            targetTable instanceof HiveTable)) {
                        // schema table and iceberg table does not need txn
                        mgr.abortTransaction(database.getId(), transactionId, errorMsg);
                    }
                    context.getState().setOk();
                    insertError = true;
                    return;
                }
            }

            if (targetTable instanceof ExternalOlapTable) {
                ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
                if (transactionMgr.commitRemoteTransaction(
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
            } else if (targetTable instanceof IcebergTable) {
                // TODO(stephen): support abort interface and delete data files when aborting.
                List<TSinkCommitInfo> commitInfos = coord.getSinkCommitInfos();
                if (stmt instanceof InsertStmt && ((InsertStmt) stmt).isOverwrite()) {
                    for (TSinkCommitInfo commitInfo : commitInfos) {
                        commitInfo.setIs_overwrite(true);
                    }
                }

                context.getGlobalStateMgr().getMetadataMgr().finishSink(catalogName, dbName, tableName, commitInfos);
                txnStatus = TransactionStatus.VISIBLE;
                label = "FAKE_ICEBERG_SINK_LABEL";
            } else if (targetTable instanceof HiveTable) {
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
                context.getGlobalStateMgr().getMetadataMgr().finishSink(catalogName, dbName, tableName, commitInfos);
                txnStatus = TransactionStatus.VISIBLE;
                label = "FAKE_HIVE_SINK_LABEL";
            } else if (targetTable instanceof TableFunctionTable) {
                txnStatus = TransactionStatus.VISIBLE;
                label = "FAKE_TABLE_FUNCTION_TABLE_SINK_LABEL";
            } else if (isExplainAnalyze) {
                transactionMgr.abortTransaction(database.getId(), transactionId, "Explain Analyze");
                txnStatus = TransactionStatus.ABORTED;
            } else {
                VisibleStateWaiter visibleWaiter = transactionMgr.retryCommitOnRateLimitExceeded(
                        database,
                        transactionId,
                        TabletCommitInfo.fromThrift(coord.getCommitInfos()),
                        TabletFailInfo.fromThrift(coord.getFailInfos()),
                        new InsertTxnCommitAttachment(loadedRows),
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
                    transactionMgr.abortRemoteTransaction(
                            externalTable.getSourceTableDbId(), transactionId,
                            externalTable.getSourceTableHost(),
                            externalTable.getSourceTablePort(),
                            errMsg);
                } else if (targetTable.isExternalTableWithFileSystem()) {
                    // ignored
                } else {
                    transactionMgr.abortTransaction(
                            database.getId(), transactionId,
                            errMsg,
                            coord == null ? Lists.newArrayList() : TabletFailInfo.fromThrift(coord.getFailInfos()));
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
                            .recordFinishedOrCacnelledLoadJob(jobId, EtlJobType.INSERT,
                                    "Cancelled, msg: " + t.getMessage(), coord.getTrackingUrl());
                    jobId = -1;
                }
            } catch (Exception abortTxnException) {
                LOG.warn("errors when cancel insert load job {}", jobId);
            }
            throw new UserException(t.getMessage(), t);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
            if (insertError) {
                try {
                    if (jobId != -1) {
                        context.getGlobalStateMgr().getLoadMgr()
                                .recordFinishedOrCacnelledLoadJob(jobId, EtlJobType.INSERT,
                                        "Cancelled", coord.getTrackingUrl());
                        jobId = -1;
                    }
                } catch (Exception abortTxnException) {
                    LOG.warn("errors when cancel insert load job {}", jobId);
                }
            } else if (txnState != null) {
                StatisticUtils.triggerCollectionOnFirstLoad(txnState, database, targetTable, true);
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
                context.getGlobalStateMgr().getLoadMgr().recordFinishedOrCacnelledLoadJob(jobId,
                        EtlJobType.INSERT,
                        "",
                        coord.getTrackingUrl());
            }
        } catch (MetaNotFoundException e) {
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

        context.getState().setOk(loadedRows, filteredRows, sb.toString());
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
        } catch (Exception e) {
            LOG.warn(e);
            coord.getExecStatus().setInternalErrorStatus(e.getMessage());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
        }
        return Pair.create(sqlResult, coord.getExecStatus());
    }

    public List<ByteBuffer> getProxyResultBuffer() {
        return proxyResultBuffer;
    }
}
