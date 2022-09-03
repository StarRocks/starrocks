// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.Sets;
import com.starrocks.analysis.AddSqlBlackListStmt;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DelSqlBlackListStmt;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DmlStmt;
import com.starrocks.analysis.EnterStmt;
import com.starrocks.analysis.ExportStmt;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.KillStmt;
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.SelectStmt;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.UnsupportedStmt;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.WorkGroup;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.QueryDumpLog;
import com.starrocks.common.UserException;
import com.starrocks.common.Version;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.meta.SqlBlackList;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlEofPacket;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.QueryStatisticsItemPB;
import com.starrocks.qe.QueryState.MysqlStateType;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.UseStmt;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.Constants;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionCommitFailedException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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
    private MasterOpExecutor masterOpExecutor = null;
    private RedirectStatus redirectStatus = null;
    private final boolean isProxy;
    private List<ByteBuffer> proxyResultBuffer = null;
    private ShowResultSet proxyResultSet = null;
    private PQueryStatistics statisticsForAuditLog;

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

    // this constructor is only for test now.
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

    // At the end of query execution, we begin to add up profile
    public void initProfile(long beginTimeInNanoSecond) {
        profile = new RuntimeProfile("Query");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(context.getExecutionId()));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(context.getStartTime()));

        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - context.getStartTime();
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(currentTimestamp));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, context.getState().toString());
        summaryProfile.addInfoString("StarRocks Version", Version.STARROCKS_VERSION);
        summaryProfile.addInfoString(ProfileManager.USER, context.getQualifiedUser());
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, context.getDatabase());
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, originStmt.originStmt);

        PQueryStatistics statistics = getQueryStatisticsForAuditLog();
        long memCostBytes = statistics == null || statistics.memCostBytes == null ? 0 : statistics.memCostBytes;
        long cpuCostNs = statistics == null || statistics.cpuCostNs == null ? 0 : statistics.cpuCostNs;
        summaryProfile.addInfoString(ProfileManager.QUERY_CPU_COST, DebugUtil.getPrettyStringNs(cpuCostNs));
        summaryProfile.addInfoString(ProfileManager.QUERY_MEM_COST, DebugUtil.getPrettyStringBytes(memCostBytes));

        // Add some import variables in profile
        SessionVariable variables = context.getSessionVariable();
        if (variables != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(SessionVariable.PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM).append("=")
                    .append(variables.getParallelExecInstanceNum()).append(",");
            sb.append(SessionVariable.MAX_PARALLEL_SCAN_INSTANCE_NUM).append("=")
                    .append(variables.getMaxParallelScanInstanceNum()).append(",");
            sb.append(SessionVariable.PIPELINE_DOP).append("=").append(variables.getPipelineDop()).append(",");
            if (context.getWorkGroup() != null) {
                sb.append(SessionVariable.RESOURCE_GROUP).append("=").append(context.getWorkGroup().getName()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            summaryProfile.addInfoString(ProfileManager.VARIABLES, sb.toString());
        }

        profile.addChild(summaryProfile);

        RuntimeProfile plannerProfile = new RuntimeProfile("Planner");
        profile.addChild(plannerProfile);
        context.getPlannerProfile().build(plannerProfile);

        if (coord != null) {
            if (coord.getQueryProfile() != null) {
                coord.getQueryProfile().getCounterTotalTime().setValue(TimeUtils.getEstimatedTime(beginTimeInNanoSecond));
                coord.endProfile();
                coord.mergeIsomorphicProfiles();
                profile.addChild(coord.getQueryProfile());
            }
            coord = null;
        }
    }

    public boolean isForwardToMaster() {
        if (GlobalStateMgr.getCurrentState().isMaster()) {
            return false;
        }

        // this is a query stmt, but this non-master FE can not read, forward it to master
        if ((parsedStmt instanceof QueryStmt || parsedStmt instanceof QueryStatement) &&
                !GlobalStateMgr.getCurrentState().isMaster()
                && !GlobalStateMgr.getCurrentState().canRead()) {
            return true;
        }

        if (redirectStatus == null) {
            return false;
        } else {
            return redirectStatus.isForwardToMaster();
        }
    }

    public ByteBuffer getOutputPacket() {
        if (masterOpExecutor == null) {
            return null;
        } else {
            return masterOpExecutor.getOutputPacket();
        }
    }

    public ShowResultSet getProxyResultSet() {
        return proxyResultSet;
    }

    public ShowResultSet getShowResultSet() {
        if (masterOpExecutor == null) {
            return null;
        } else {
            return masterOpExecutor.getProxyResultSet();
        }
    }

    public boolean isQueryStmt() {
        return parsedStmt != null && (parsedStmt instanceof QueryStmt || parsedStmt instanceof QueryStatement);
    }

    public boolean sendResultToChannel(MysqlChannel channel) throws IOException {
        if (masterOpExecutor == null) {
            return false;
        } else {
            return masterOpExecutor.sendResultToChannel(channel);
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
        try {
            // parsedStmt may already by set when constructing this StmtExecutor();
            resolveParseStmtForForward();

            // support select hint e.g. select /*+ SET_VAR(query_timeout=1) */ sleep(3);
            if (parsedStmt != null) {
                Map<String, String> optHints = null;
                if (parsedStmt instanceof SelectStmt) {
                    SelectStmt selectStmt = (SelectStmt) parsedStmt;
                    optHints = selectStmt.getSelectList().getOptHints();
                } else if (parsedStmt instanceof QueryStatement &&
                        ((QueryStatement) parsedStmt).getQueryRelation() instanceof SelectRelation) {
                    SelectRelation selectRelation = (SelectRelation) ((QueryStatement) parsedStmt).getQueryRelation();
                    optHints = selectRelation.getSelectList().getOptHints();
                }

                if (optHints != null) {
                    SessionVariable sessionVariable = (SessionVariable) sessionVariableBackup.clone();
                    for (String key : optHints.keySet()) {
                        VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(optHints.get(key))),
                                true);
                    }
                    context.setSessionVariable(sessionVariable);
                }
            }

            // execPlan is the output of new planner
            ExecPlan execPlan = null;
            boolean execPlanBuildByNewPlanner = false;

            // Entrance to the new planner
            if (isStatisticsOrAnalyzer(parsedStmt, context) ||
                    StatementPlanner.supportedByNewPlanner(parsedStmt)) {
                try (PlannerProfile.ScopedTimer _ = PlannerProfile.getScopedTimer("Total")) {
                    redirectStatus = parsedStmt.getRedirectStatus();
                    if (!isForwardToMaster()) {
                        context.getDumpInfo().reset();
                        context.getDumpInfo().setOriginStmt(parsedStmt.getOrigStmt().originStmt);
                        if (parsedStmt instanceof ShowStmt) {
                            com.starrocks.sql.analyzer.Analyzer.analyze(parsedStmt, context);
                            PrivilegeChecker.check(parsedStmt, context);

                            QueryStatement selectStmt = ((ShowStmt) parsedStmt).toSelectStmt();
                            if (selectStmt != null) {
                                parsedStmt = selectStmt;
                                execPlan = new StatementPlanner().plan(parsedStmt, context);
                            }
                        } else {
                            execPlan = new StatementPlanner().plan(parsedStmt, context);
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
            } else {
                // analyze this query
                analyze(context.getSessionVariable().toThrift());
            }

            if (context.isQueryDump()) {
                return;
            }
            if (isForwardToMaster()) {
                forwardToMaster();
                return;
            } else {
                LOG.debug("no need to transfer to Master. stmt: {}", context.getStmtId());
            }

            if (parsedStmt instanceof QueryStmt || parsedStmt instanceof QueryStatement) {
                context.getState().setIsQuery(true);

                // sql's blacklist is enabled through enable_sql_blacklist.
                if (Config.enable_sql_blacklist) {
                    String originSql = parsedStmt.getOrigStmt().originStmt.trim().toLowerCase().replaceAll(" +", " ");

                    // If this sql is in blacklist, show message.
                    SqlBlackList.verifying(originSql);
                }

                // Record planner costs in audit log
                Preconditions.checkNotNull(execPlan, "query must has a plan");

                int retryTime = Config.max_query_retry_time;
                for (int i = 0; i < retryTime; i++) {
                    try {
                        //reset query id for each retry
                        if (i > 0) {
                            uuid = UUID.randomUUID();
                            context.setExecutionId(
                                    new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                        }

                        Preconditions.checkState(execPlanBuildByNewPlanner, "must use new planner");

                        handleQueryStmt(execPlan);

                        if (context.getSessionVariable().isReportSucc()) {
                            writeProfile(beginTimeInNanoSecond);
                        }
                        break;
                    } catch (RpcException e) {
                        if (i == retryTime - 1) {
                            throw e;
                        }
                        if (!context.getMysqlChannel().isSend()) {
                            LOG.warn("retry {} times. stmt: {}", (i + 1), parsedStmt.getOrigStmt().originStmt);
                        } else {
                            throw e;
                        }
                    } finally {
                        QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
                    }
                }
            } else if (parsedStmt instanceof SetStmt) {
                handleSetStmt();
            } else if (parsedStmt instanceof EnterStmt) {
                handleEnterStmt();
            } else if (parsedStmt instanceof UseStmt) {
                handleUseStmt();
            } else if (parsedStmt instanceof CreateTableAsSelectStmt) {
                if (execPlanBuildByNewPlanner) {
                    handleCreateTableAsSelectStmt(beginTimeInNanoSecond);
                } else {
                    throw new AnalysisException("old planner does not support CTAS statement");
                }
            } else if (parsedStmt instanceof DmlStmt) {
                try {
                    handleDMLStmt(execPlan, (DmlStmt) parsedStmt);
                    if (context.getSessionVariable().isReportSucc()) {
                        writeProfile(beginTimeInNanoSecond);
                    }
                } catch (Throwable t) {
                    LOG.warn("DML statement(" + originStmt.originStmt + ") process failed.", t);
                    throw t;
                } finally {
                    QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
                }
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
            } else if (parsedStmt instanceof AddSqlBlackListStmt) {
                handleAddSqlBlackListStmt();
            } else if (parsedStmt instanceof DelSqlBlackListStmt) {
                handleDelSqlBlackListStmt();
            } else if (parsedStmt instanceof ExecuteAsStmt) {
                handleExecAsStmt();
            } else if (parsedStmt instanceof ExecuteScriptStmt) {
                handleExecScriptStmt();
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
            if (context.getState().isError() && coord != null) {
                coord.cancel();
            }

            if (parsedStmt instanceof InsertStmt) {
                InsertStmt insertStmt = (InsertStmt) parsedStmt;
                // The transaction of an insert operation begin at analyze phase.
                // So we should abort the transaction at this finally block if it encounters exception.
                if (insertStmt.isTransactionBegin() && context.getState().getStateType() == MysqlStateType.ERR) {
                    try {
                        String errMsg = Strings.emptyToNull(context.getState().getErrorMessage());
                        if (insertStmt.getTargetTable() instanceof ExternalOlapTable) {
                            ExternalOlapTable externalTable = (ExternalOlapTable) (insertStmt.getTargetTable());
                            GlobalStateMgr.getCurrentGlobalTransactionMgr().abortRemoteTransaction(
                                    externalTable.getSourceTableDbId(), insertStmt.getTransactionId(),
                                    externalTable.getSourceTableHost(),
                                    externalTable.getSourceTablePort(),
                                    errMsg == null ? "unknown reason" : errMsg);
                        } else {
                            GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(
                                    insertStmt.getDbObj().getId(), insertStmt.getTransactionId(),
                                    (errMsg == null ? "unknown reason" : errMsg));
                        }
                    } catch (Exception abortTxnException) {
                        LOG.warn("errors when abort txn", abortTxnException);
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
            handleDMLStmt(execPlan, ((CreateTableAsSelectStmt) parsedStmt).getInsertStmt());
            if (context.getSessionVariable().isReportSucc()) {
                writeProfile(beginTimeInNanoSecond);
            }
            if (context.getState().getStateType() == MysqlStateType.ERR) {
                ((CreateTableAsSelectStmt) parsedStmt).dropTable(context);
            }
        } catch (Throwable t) {
            LOG.warn("handle create table as select stmt fail", t);
            ((CreateTableAsSelectStmt) parsedStmt).dropTable(context);
            throw t;
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
        }
    }

    private void resolveParseStmtForForward() throws AnalysisException {
        if (parsedStmt == null) {
            List<StatementBase> stmts;
            try {
                stmts = com.starrocks.sql.parser.SqlParser.parse(originStmt.originStmt,
                        context.getSessionVariable().getSqlMode());
                parsedStmt = stmts.get(originStmt.idx);
                parsedStmt.setOrigStmt(originStmt);
            } catch (ParsingException parsingException) {
                throw new AnalysisException(parsingException.getMessage());
            } catch (Exception e) {
                parsedStmt = com.starrocks.sql.parser.SqlParser.parseWithOldParser(originStmt.originStmt,
                        context.getSessionVariable().getSqlMode(), originStmt.idx);
                parsedStmt.setOrigStmt(originStmt);
            }
        }
    }

    private void dumpException(Exception e) {
        context.getDumpInfo().addException(ExceptionUtils.getStackTrace(e));
        if (context.getSessionVariable().getEnableQueryDump() && !context.isQueryDump()) {
            QueryDumpLog.getQueryDump().log(GsonUtils.GSON.toJson(context.getDumpInfo()));
        }
    }

    private void forwardToMaster() throws Exception {
        masterOpExecutor = new MasterOpExecutor(parsedStmt, originStmt, context, redirectStatus);
        LOG.debug("need to transfer to Master. stmt: {}", context.getStmtId());
        masterOpExecutor.execute();
    }

    private void writeProfile(long beginTimeInNanoSecond) {
        long profileBeginTime = System.currentTimeMillis();
        initProfile(beginTimeInNanoSecond);
        profile.computeTimeInChildProfile();
        long profileEndTime = System.currentTimeMillis();
        profile.getChild("Summary")
                .addInfoString(ProfileManager.PROFILE_TIME,
                        DebugUtil.getPrettyStringMs(profileEndTime - profileBeginTime));
        StringBuilder builder = new StringBuilder();
        profile.prettyPrint(builder, "");
        String profileContent = ProfileManager.getInstance().pushProfile(profile);
        if (context.getQueryDetail() != null) {
            context.getQueryDetail().setProfile(profileContent);
        }
    }

    // Analyze one statement to structure in memory.
    public void analyze(TQueryOptions tQueryOptions) throws UserException {
        LOG.info("begin to analyze stmt: {}, forwarded stmt id: {}", context.getStmtId(), context.getForwardedStmtId());

        // parsedStmt may already by set when constructing this StmtExecutor();
        resolveParseStmtForForward();
        redirectStatus = parsedStmt.getRedirectStatus();

        // yiguolei: insertstmt's grammar analysis will write editlog, so that we check if the stmt should be forward to master here
        // if the stmt should be forward to master, then just return here and the master will do analysis again
        if (isForwardToMaster()) {
            return;
        }

        // Convert show statement to select statement here
        if (parsedStmt instanceof ShowStmt) {
            QueryStatement selectStmt = ((ShowStmt) parsedStmt).toSelectStmt();
            if (selectStmt != null) {
                Preconditions.checkState(false, "Shouldn't reach here");
            }
        }

        if (parsedStmt instanceof QueryStmt
                || parsedStmt instanceof QueryStatement
                || parsedStmt instanceof DmlStmt
                || parsedStmt instanceof CreateTableAsSelectStmt) {
            Preconditions.checkState(false, "Shouldn't reach here");
        } else {
            try {
                parsedStmt.analyze(new Analyzer(context.getGlobalStateMgr(), context));
            } catch (AnalysisException e) {
                throw e;
            } catch (Exception e) {
                LOG.warn("Analyze failed because ", e);
                throw new AnalysisException("Unexpected exception: " + e.getMessage());
            }
        }
    }

    // Because this is called by other thread
    public void cancel() {
        if (parsedStmt instanceof DeleteStmt && !((DeleteStmt) parsedStmt).supportNewPlanner()) {
            DeleteStmt deleteStmt = (DeleteStmt) parsedStmt;
            long jobId = deleteStmt.getJobId();
            if (jobId != -1) {
                GlobalStateMgr.getCurrentState().getDeleteHandler().killJob(jobId);
            }
        } else {
            Coordinator coordRef = coord;
            if (coordRef != null) {
                coordRef.cancel();
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
        if (context == killCtx) {
            // Suicide
            context.setKilled();
        } else {
            // Check auth
            // Only user itself and user with admin priv can kill connection
            if (!killCtx.getQualifiedUser().equals(ConnectContext.get().getQualifiedUser())
                    && !GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(),
                    PrivPredicate.ADMIN)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_KILL_DENIED_ERROR, id);
            }

            killCtx.kill(killStmt.isConnectionKill());
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

    // Process a select statement.
    private void handleQueryStmt(ExecPlan execPlan) throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();

        if (parsedStmt.isExplain()) {
            handleExplainStmt(buildExplainString(execPlan));
            return;
        }
        if (context.getQueryDetail() != null) {
            context.getQueryDetail().setExplain(buildExplainString(execPlan));
        }

        StatementBase queryStmt = parsedStmt;
        List<PlanFragment> fragments = execPlan.getFragments();
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        TDescriptorTable descTable = execPlan.getDescTbl().toThrift();
        List<String> colNames = execPlan.getColNames();
        List<Expr> outputExprs = execPlan.getOutputExprs();

        coord = new Coordinator(context, fragments, scanNodes, descTable);

        QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(),
                new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));

        coord.exec();

        // send result
        // 1. If this is a query with OUTFILE clause, eg: select * from tbl1 into outfile xxx,
        //    We will not send real query result to client. Instead, we only send OK to client with
        //    number of rows selected. For example:
        //          mysql> select * from tbl1 into outfile xxx;
        //          Query OK, 10 rows affected (0.01 sec)
        //
        // 2. If this is a query, send the result expr fields first, and send result data back to client.
        RowBatch batch;
        MysqlChannel channel = context.getMysqlChannel();
        boolean isOutfileQuery = false;
        if (queryStmt instanceof QueryStmt) {
            isOutfileQuery = ((QueryStmt) queryStmt).hasOutFileClause();
        } else if (queryStmt instanceof QueryStatement) {
            isOutfileQuery = ((QueryStatement) queryStmt).hasOutFileClause();
        }
        boolean isSendFields = false;
        while (true) {
            batch = coord.getNext();
            // for outfile query, there will be only one empty batch send back with eos flag
            if (batch.getBatch() != null && !isOutfileQuery) {
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
            if (batch.isEos()) {
                break;
            }
        }
        if (!isSendFields && !isOutfileQuery) {
            sendFields(colNames, outputExprs);
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

    private void handleAnalyzeStmt() throws Exception {
        AnalyzeStmt analyzeStmt = (AnalyzeStmt) parsedStmt;
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        Database db = MetaUtils.getStarRocks(context, analyzeStmt.getTableName());
        Table table = MetaUtils.getStarRocksTable(context, analyzeStmt.getTableName());

        AnalyzeJob job = new AnalyzeJob();
        job.setDbId(db.getId());
        job.setTableId(table.getId());
        job.setColumns(analyzeStmt.getColumnNames());
        job.setType(analyzeStmt.isSample() ? Constants.AnalyzeType.SAMPLE : Constants.AnalyzeType.FULL);
        job.setScheduleType(Constants.ScheduleType.ONCE);
        job.setWorkTime(LocalDateTime.now());
        job.setStatus(Constants.ScheduleStatus.FINISH);
        job.setProperties(analyzeStmt.getProperties());

        try {
            statisticExecutor.collectStatisticSync(db.getId(), table.getId(), analyzeStmt.getColumnNames(),
                    analyzeStmt.isSample(), job.getSampleCollectRows());
            GlobalStateMgr.getCurrentStatisticStorage().expireColumnStatistics(table, job.getColumns());
            GlobalStateMgr.getCurrentStatisticStorage().getColumnStatisticsSync(table, job.getColumns());
        } catch (Exception e) {
            job.setReason(e.getMessage());
            throw e;
        }

        GlobalStateMgr.getCurrentState().getAnalyzeManager().addAnalyzeJob(job);
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

    private void handleExecAsStmt() {
        ExecuteAsExecutor.execute((ExecuteAsStmt) parsedStmt, context);
    }

    private void handleExecScriptStmt() throws UserException {
        ExecuteScriptExecutor.execute((ExecuteScriptStmt) parsedStmt, context);
    }

    private void handleUnsupportedStmt() {
        context.getMysqlChannel().reset();
        // do nothing
        context.getState().setOk();
    }

    // Process use statement.
    private void handleUseStmt() throws AnalysisException {
        UseStmt useStmt = (UseStmt) parsedStmt;
        try {
            context.getGlobalStateMgr().changeCatalogDb(context, useStmt.getIdentifier());
        } catch (DdlException e) {
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
        // Send meta data.
        sendMetaData(resultSet.getMetaData());

        // Send result set.
        for (List<String> row : resultSet.getResultRows()) {
            serializer.reset();
            for (String item : row) {
                if (item == null || item.equals(FeConstants.null_string)) {
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
        if (context.getQueryDetail() != null) {
            context.getQueryDetail().setExplain(explainString);
        }

        ShowResultSetMetaData metaData =
                ShowResultSetMetaData.builder()
                        .addColumn(new Column("Explain String", ScalarType.createVarchar(20)))
                        .build();
        sendMetaData(metaData);

        // Send result set.
        for (String item : explainString.split("\n")) {
            serializer.reset();
            serializer.writeLenEncodedString(item);
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        context.getState().setEof();
    }

    private String buildExplainString(ExecPlan execPlan) {
        String explainString = "";
        if (parsedStmt.getExplainLevel() == StatementBase.ExplainLevel.VERBOSE) {
            if (context.getSessionVariable().isEnableResourceGroup()) {
                TWorkGroup workGroup = Coordinator.prepareWorkGroup(context);
                String workGroupStr = workGroup != null ? workGroup.getName() : WorkGroup.DEFAULT_WORKGROUP_NAME;
                explainString += "RESOURCE GROUP: " + workGroupStr + "\n\n";
            }
        }
        // marked delete will get execPlan null
        if (execPlan == null) {
            explainString += "NOT AVAILABLE";
        } else {
            explainString += execPlan.getExplainString(parsedStmt.getExplainLevel());
        }
        return explainString;
    }

    private void handleDdlStmt() {
        try {
            ShowResultSet resultSet = DdlExecutor.execute(context.getGlobalStateMgr(), (DdlStmt) parsedStmt);
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
                LOG.warn("DDL statement(" + originStmt.originStmt + ") process failed.", e);
            }
            context.setState(e.getQueryState());
        } catch (UserException e) {
            LOG.warn("DDL statement(" + originStmt.originStmt + ") process failed.", e);
            // Return message to info client what happened.
            context.getState().setError(e.getMessage());
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("DDL statement(" + originStmt.originStmt + ") process failed.", e);
            context.getState().setError("Unexpected exception: " + e.getMessage());
        }
    }

    // process enter cluster
    private void handleEnterStmt() {
        final EnterStmt enterStmt = (EnterStmt) parsedStmt;
        try {
            context.getGlobalStateMgr().changeCluster(context, enterStmt.getClusterName());
            context.setDatabase("");
        } catch (DdlException e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private void handleExportStmt(UUID queryId) throws Exception {
        ExportStmt exportStmt = (ExportStmt) parsedStmt;
        exportStmt.setExportStartTime(context.getStartTime());
        context.getGlobalStateMgr().getExportMgr().addExportJob(queryId, exportStmt);
    }

    public PQueryStatistics getQueryStatisticsForAuditLog() {
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
        return statisticsForAuditLog;
    }

    /**
     * Below function is added by new analyzer
     */
    private boolean isStatisticsOrAnalyzer(StatementBase statement, ConnectContext context) {
        return (statement instanceof InsertStmt && context.getDatabase().equalsIgnoreCase(Constants.StatisticsDBName))
                || statement instanceof AnalyzeStmt
                || statement instanceof CreateAnalyzeJobStmt;
    }

    public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
        if (stmt.isExplain()) {
            handleExplainStmt(buildExplainString(execPlan));
            return;
        }
        if (context.getQueryDetail() != null) {
            context.getQueryDetail().setExplain(buildExplainString(execPlan));
        }

        // special handling for delete of non-primary key table, using old handler
        if (stmt instanceof DeleteStmt && !((DeleteStmt) stmt).supportNewPlanner()) {
            try {
                context.getGlobalStateMgr().getDeleteHandler().process((DeleteStmt) stmt);
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
        Database database = MetaUtils.getStarRocks(context, stmt.getTableName());
        Table targetTable;
        if (stmt instanceof InsertStmt && ((InsertStmt) stmt).getTargetTable() != null) {
            targetTable = ((InsertStmt) stmt).getTargetTable();
        } else {
            targetTable = MetaUtils.getStarRocksTable(context, stmt.getTableName());
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
        if (targetTable instanceof ExternalOlapTable) {
            ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
            TAuthenticateParams authenticateParams = new TAuthenticateParams();
            authenticateParams.setUser(externalTable.getSourceTableUser());
            authenticateParams.setPasswd(externalTable.getSourceTablePassword());
            authenticateParams.setHost(context.getRemoteIP());
            authenticateParams.setDb_name(externalTable.getSourceTableDbName());
            authenticateParams.setTable_names(Lists.newArrayList(externalTable.getSourceTableName()));
            transactionId =
                    GlobalStateMgr.getCurrentGlobalTransactionMgr()
                            .beginRemoteTransaction(externalTable.getSourceTableDbId(),
                                    Lists.newArrayList(externalTable.getSourceTableId()), label,
                                    externalTable.getSourceTableHost(),
                                    externalTable.getSourceTablePort(),
                                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                                            FrontendOptions.getLocalHostAddress()),
                                    sourceType,
                                    ConnectContext.get().getSessionVariable().getQueryTimeoutS(),
                                    authenticateParams);
        } else {
            transactionId = GlobalStateMgr.getCurrentGlobalTransactionMgr().beginTransaction(
                    database.getId(),
                    Lists.newArrayList(targetTable.getId()),
                    label,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                            FrontendOptions.getLocalHostAddress()),
                    sourceType,
                    ConnectContext.get().getSessionVariable().getQueryTimeoutS());

            // add table indexes to transaction state
            TransactionState txnState =
                    GlobalStateMgr.getCurrentGlobalTransactionMgr()
                            .getTransactionState(database.getId(), transactionId);
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
        TransactionStatus txnStatus = TransactionStatus.ABORTED;
        try {
            if (execPlan.getFragments().get(0).getSink() instanceof OlapTableSink) {
                OlapTableSink dataSink = (OlapTableSink) execPlan.getFragments().get(0).getSink();
                dataSink.init(context.getExecutionId(), transactionId, database.getId(),
                        ConnectContext.get().getSessionVariable().getQueryTimeoutS());
                dataSink.complete();
            }

            coord = new Coordinator(context, execPlan.getFragments(), execPlan.getScanNodes(),
                    execPlan.getDescTbl().toThrift());
            coord.setQueryType(TQueryType.LOAD);
            QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(), coord);
            coord.exec();

            coord.join(context.getSessionVariable().getQueryTimeoutS());
            if (!coord.isDone()) {
                /*
                 * In this case, There are two factors that lead query cancelled:
                 * 1: TIMEOUT
                 * 2: BE EXCEPTION
                 * So we should distinguish these two factors.
                 */
                if (!coord.checkBackendState()) {
                    coord.cancel();
                    ErrorReport.reportDdlException(ErrorCode.ERR_QUERY_EXCEPTION);
                } else {
                    coord.cancel();
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
                LOG.warn("insert failed: {}", errMsg);
                ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
            }

            LOG.debug("delta files is {}", coord.getDeltaUrls());

            if (coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL) != null) {
                loadedRows = Long.parseLong(coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL));
            }
            if (coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL) != null) {
                filteredRows = Integer.parseInt(coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL));
            }

            // if in strict mode, insert will fail if there are filtered rows
            if (context.getSessionVariable().getEnableInsertStrict()) {
                if (filteredRows > 0) {
                    if (targetTable instanceof ExternalOlapTable) {
                        ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
                        GlobalStateMgr.getCurrentGlobalTransactionMgr().abortRemoteTransaction(
                                externalTable.getSourceTableDbId(), transactionId,
                                externalTable.getSourceTableHost(),
                                externalTable.getSourceTablePort(),
                                TransactionCommitFailedException.FILTER_DATA_IN_STRICT_MODE  + ", tracking_url = " 
                                + coord.getTrackingUrl()
                        );
                    } else {
                        GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(
                                database.getId(),
                                transactionId,
                                TransactionCommitFailedException.FILTER_DATA_IN_STRICT_MODE + ", tracking_url = " 
                                + coord.getTrackingUrl()
                        );
                    }
                    context.getState().setError("Insert has filtered data in strict mode, txn_id = " + 
                            transactionId + " tracking_url = " + coord.getTrackingUrl());
                    
                    return;
                }
            }

            if (loadedRows == 0 && filteredRows == 0 && (stmt instanceof DeleteStmt || stmt instanceof InsertStmt
                    || stmt instanceof UpdateStmt)) {
                if (targetTable instanceof ExternalOlapTable) {
                    ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().abortRemoteTransaction(
                            externalTable.getSourceTableDbId(), transactionId,
                            externalTable.getSourceTableHost(),
                            externalTable.getSourceTablePort(),
                            TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG);
                } else {
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(
                            database.getId(),
                            transactionId,
                            TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG
                    );
                }
                context.getState().setOk();
                return;
            }

            if (targetTable instanceof ExternalOlapTable) {
                ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
                if (GlobalStateMgr.getCurrentGlobalTransactionMgr().commitRemoteTransaction(
                        externalTable.getSourceTableDbId(), transactionId,
                        externalTable.getSourceTableHost(),
                        externalTable.getSourceTablePort(),
                        coord.getCommitInfos())) {
                    txnStatus = TransactionStatus.VISIBLE;
                    MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
                }
                // TODO: wait remote txn finished
            } else {
                if (GlobalStateMgr.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                        database,
                        transactionId,
                        TabletCommitInfo.fromThrift(coord.getCommitInfos()),
                        context.getSessionVariable().getTransactionVisibleWaitTimeout() * 1000)) {
                    txnStatus = TransactionStatus.VISIBLE;
                    MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
                    // collect table-level metrics
                    if (null != targetTable) {
                        TableMetricsEntity entity =
                                TableMetricsRegistry.getInstance().getMetricsEntity(targetTable.getId());
                        entity.counterInsertLoadFinishedTotal.increase(1L);
                        entity.counterInsertLoadRowsTotal.increase(loadedRows);
                        entity.counterInsertLoadBytesTotal
                                .increase(Long.valueOf(coord.getLoadCounters().get(LoadJob.LOADED_BYTES)));
                    }
                } else {
                    txnStatus = TransactionStatus.COMMITTED;
                }
            }
        } catch (Throwable t) {
            // if any throwable being thrown during insert operation, first we should abort this txn
            LOG.warn("handle insert stmt fail: {}", label, t);
            try {
                if (targetTable instanceof ExternalOlapTable) {
                    ExternalOlapTable externalTable = (ExternalOlapTable) targetTable;
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().abortRemoteTransaction(
                            externalTable.getSourceTableDbId(), transactionId,
                            externalTable.getSourceTableHost(),
                            externalTable.getSourceTablePort(),
                            t.getMessage() == null ? "Unknown reason" : t.getMessage());
                } else {
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(
                            database.getId(), transactionId,
                            t.getMessage() == null ? "Unknown reason" : t.getMessage());
                }
            } catch (Exception abortTxnException) {
                // just print a log if abort txn failed. This failure do not need to pass to user.
                // user only concern abort how txn failed.
                LOG.warn("errors when abort txn", abortTxnException);
            }

            // if not using old load usage pattern, error will be returned directly to user
            StringBuilder sb = new StringBuilder(t.getMessage());
            if (coord != null && !Strings.isNullOrEmpty(coord.getTrackingUrl())) {
                sb.append(". url: ").append(coord.getTrackingUrl());
            }
            context.getState().setError(sb.toString());
            return;
        }

        String errMsg = "";
        try {
            context.getGlobalStateMgr().getLoadManager().recordFinishedLoadJob(
                    label,
                    database.getFullName(),
                    targetTable.getId(),
                    EtlJobType.INSERT,
                    createTime,
                    "",
                    coord.getTrackingUrl());
        } catch (MetaNotFoundException e) {
            LOG.warn("Record info of insert load with error {}", e.getMessage(), e);
            errMsg = "Record info of insert load with error " + e.getMessage();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(label).append("', 'status':'").append(txnStatus.name());
        sb.append("', 'txnId':'").append(transactionId).append("'");
        if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }
        sb.append("}");

        context.getState().setOk(loadedRows, filteredRows, sb.toString());
    }

    public String getOriginStmtInString() {
        if (originStmt == null) {
            return "";
        }
        return originStmt.originStmt;
    }

    public List<ByteBuffer> getProxyResultBuffer() {
        return proxyResultBuffer;
    }
}
