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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectProcessor.java

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

import com.google.common.base.Strings;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authentication.UserProperty;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.AuditStatisticsUtil;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.SqlCredentialRedactor;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTimeTravelQueryAnalyzer;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.ResourceGroupMetricMgr;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlPacket;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.mysql.MysqlServerStatusFlag;
import com.starrocks.mysql.RequestPackage;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.AuditEvent.EventType;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.common.AuditEncryptionChecker;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.formatter.FormatOptions;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.transaction.ExplicitTxnStatementValidator;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousCloseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Process one mysql connection, receive one pakcet, process, send one packet.
 */
public class ConnectProcessor {
    private static final Logger LOG = LogManager.getLogger(ConnectProcessor.class);

    protected final ConnectContext ctx;
    private ByteBuffer packetBuf;

    protected StmtExecutor executor = null;

    public ConnectProcessor(ConnectContext context) {
        this.ctx = context;
    }

    // COM_INIT_DB: change current database of this session.
    private void handleInitDb() {
        String identifier = new String(packetBuf.array(), 1, packetBuf.limit() - 1);
        try {
            String[] parts = identifier.trim().split("\\s+");
            if (parts.length == 2) {
                if (parts[0].equalsIgnoreCase("catalog")) {
                    ctx.changeCatalog(parts[1]);
                } else if (parts[0].equalsIgnoreCase("warehouse")) {
                    WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                    String newWarehouseName = parts[1];
                    if (!warehouseMgr.warehouseExists(newWarehouseName)) {
                        throw new StarRocksException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, newWarehouseName);
                    }
                    ctx.setCurrentWarehouse(newWarehouseName);
                } else {
                    ctx.getState().setError("not supported command");
                }
            } else {
                ctx.changeCatalogDb(identifier);
            }
        } catch (Exception e) {
            ctx.getState().setError(e.getMessage());
            return;
        }

        ctx.getState().setOk();
    }

    // COM_QUIT: set killed flag and then return OK packet.
    private void handleQuit() {
        ctx.setKilled();
        ctx.getState().setOk();
    }

    // COM_CHANGE_USER: change current user within this connection
    private void handleChangeUser() throws IOException {
        if (!MysqlProto.changeUser(ctx, packetBuf)) {
            LOG.warn("Failed to execute command `Change user`.");
            return;
        }
        handleResetConnection();
    }

    // COM_RESET_CONNECTION: reset current connection session variables
    private void handleResetConnection() throws IOException {
        resetConnectionSession();
        ctx.getState().setOk();
    }

    // process COM_PING statement, do nothing, just return one OK packet.
    private void handlePing() {
        ctx.getState().setOk();
    }

    private void resetConnectionSession() {
        // reconstruct serializer
        ctx.getSerializer().reset();
        ctx.getSerializer().setCapability(ctx.getCapability());
        // reset session variable
        ctx.resetSessionVariable();
    }

    public static long getThreadAllocatedBytes(long threadId) {
        try {
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            if (threadMXBean instanceof com.sun.management.ThreadMXBean) {
                com.sun.management.ThreadMXBean casted = (com.sun.management.ThreadMXBean) threadMXBean;
                if (casted.isThreadAllocatedMemorySupported() && casted.isThreadAllocatedMemoryEnabled()) {
                    long allocatedBytes = casted.getThreadAllocatedBytes(threadId);
                    if (allocatedBytes != -1) {
                        return allocatedBytes;
                    }
                }
            }
            return 0;
        } catch (Exception e) {
            return 0;
        }
    }

    public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics) {
        auditAfterExec(origStmt, parsedStmt, statistics, null);
    }

    public void auditBeforeExec(String origStmt, StatementBase parsedStmt) {
        if (!Config.audit_stmt_before_execute) {
            return;
        }

        ctx.getAuditEventBuilder().setEventType(EventType.BEFORE_QUERY)
                .setState(ctx.getState().toString())
                .setErrorCode(ctx.getNormalizedErrorCode())
                .setReturnRows(ctx.getReturnRows())
                .setStmtId(ctx.getStmtId())
                .setQueryId(ctx.getQueryId() == null ? "NaN" : ctx.getQueryId().toString())
                .setSessionId(ctx.getSessionId().toString())
                .setCNGroup(ctx.getCurrentComputeResourceName())
                .setQuerySource(ctx.getQuerySource().toString())
                .setCommand(ctx.getCommandStr())
                .setPreparedStmtId(null);

        ctx.getAuditEventBuilder().setIsQuery(ctx.getState().isQuery());
        ctx.getAuditEventBuilder().setFeIp(FrontendOptions.getLocalHostAddress());
        ctx.getAuditEventBuilder().setStmt(formatStmt(origStmt, parsedStmt));
        GlobalStateMgr.getCurrentState().getAuditEventProcessor().handleAuditEvent(
                ctx.getAuditEventBuilder().buildSnapshot());
    }

    public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                               String digestFromLeader) {
        // slow query
        long endTime = System.currentTimeMillis();
        long elapseMs = endTime - ctx.getStartTime();

        boolean isForwardToLeader = (executor != null) ? executor.getIsForwardToLeaderOrInit(false) : false;

        // ignore recording some failed stmt like kill connection
        if (ctx.getState().getErrType() == QueryState.ErrType.IGNORE_ERR) {
            return;
        }

        ctx.getAuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                .setState(ctx.getState().toString())
                .setErrorCode(ctx.getNormalizedErrorCode())
                .setQueryTime(elapseMs)
                .setReturnRows(ctx.getReturnRows())
                .setStmtId(ctx.getStmtId())
                .setIsForwardToLeader(isForwardToLeader)
                .setQueryId(ctx.getQueryId() == null ? "NaN" : ctx.getQueryId().toString())
                .setSessionId(ctx.getSessionId().toString())
                .setCNGroup(ctx.getCurrentComputeResourceName())
                .setQuerySource(ctx.getQuerySource().toString())
                .setCommand(ctx.getCommandStr())
                .setPreparedStmtId(executor == null ? null : executor.getPreparedStmtId());

        if (ctx.getState().isQuery()) {
            if (ctx.getState().getErrType() != QueryState.ErrType.BLACKLISTED) {
                MetricRepo.COUNTER_QUERY_ALL.increase(1L);
                EnumSet<IcebergTimeTravelQueryAnalyzer.TimeTravelType> timeTravelQueryTypes =
                        IcebergTimeTravelQueryAnalyzer.collectTimeTravelTypes(parsedStmt);
                if (!timeTravelQueryTypes.isEmpty()) {
                    MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL.increase(1L);
                    timeTravelQueryTypes.forEach(type ->
                            MetricRepo.COUNTER_ICEBERG_TIME_TRAVEL_QUERY_TOTAL_BY_TYPE.getMetric(type.getMetricLabel())
                                    .increase(1L));
                }
                ResourceGroupMetricMgr.increaseQuery(ctx, 1L);
                if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                    // err query
                    MetricRepo.COUNTER_QUERY_ERR.increase(1L);
                    ResourceGroupMetricMgr.increaseQueryErr(ctx, 1L);
                    //represent analysis err
                    if (ctx.getState().getErrType() == QueryState.ErrType.ANALYSIS_ERR) {
                        MetricRepo.COUNTER_QUERY_ANALYSIS_ERR.increase(1L);
                    } else if (ctx.getState().getErrType() == QueryState.ErrType.EXEC_TIME_OUT) {
                        MetricRepo.COUNTER_QUERY_TIMEOUT.increase(1L);
                    } else {
                        MetricRepo.COUNTER_QUERY_INTERNAL_ERR.increase(1L);
                    }
                } else {
                    // ok query
                    MetricRepo.COUNTER_QUERY_SUCCESS.increase(1L);
                    MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
                    ResourceGroupMetricMgr.updateQueryLatency(ctx, elapseMs);
                    if (elapseMs > Config.qe_slow_log_ms) {
                        MetricRepo.COUNTER_SLOW_QUERY.increase(1L);
                    }
                }

                // Catalog-type metrics
                if (executor != null) {
                    Set<String> catalogTypes = executor.getCatalogTypesInvolved();
                    for (String catalogType : catalogTypes) {
                        MetricRepo.COUNTER_CATALOG_QUERY_TOTAL.getMetric(catalogType).increase(1L);
                        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                            MetricRepo.COUNTER_CATALOG_QUERY_ERR.getMetric(catalogType).increase(1L);
                            if (ctx.getState().getErrType() == QueryState.ErrType.ANALYSIS_ERR) {
                                MetricRepo.COUNTER_CATALOG_QUERY_ANALYSIS_ERR.getMetric(catalogType).increase(1L);
                            } else if (ctx.getState().getErrType() == QueryState.ErrType.EXEC_TIME_OUT) {
                                MetricRepo.COUNTER_CATALOG_QUERY_TIMEOUT.getMetric(catalogType).increase(1L);
                            } else {
                                MetricRepo.COUNTER_CATALOG_QUERY_INTERNAL_ERR.getMetric(catalogType).increase(1L);
                            }
                        } else {
                            MetricRepo.COUNTER_CATALOG_QUERY_SUCCESS.getMetric(catalogType).increase(1L);
                            MetricRepo.getOrCreateCatalogQueryLatencyHistogram(catalogType).update(elapseMs);
                            if (elapseMs > Config.qe_slow_log_ms) {
                                MetricRepo.COUNTER_CATALOG_SLOW_QUERY.getMetric(catalogType).increase(1L);
                            }
                        }
                    }
                }
            }
            ctx.getAuditEventBuilder().setIsQuery(true);
            if (ctx.getSessionVariable().isEnableBigQueryLog()) {
                ctx.getAuditEventBuilder().setBigQueryLogCPUSecondThreshold(
                        ctx.getSessionVariable().getBigQueryLogCPUSecondThreshold());
                ctx.getAuditEventBuilder().setBigQueryLogScanBytesThreshold(
                        ctx.getSessionVariable().getBigQueryLogScanBytesThreshold());
                ctx.getAuditEventBuilder().setBigQueryLogScanRowsThreshold(
                        ctx.getSessionVariable().getBigQueryLogScanRowsThreshold());
            }
        } else {
            ctx.getAuditEventBuilder().setIsQuery(false);
        }

        // Build Digest and queryFeMemory for SELECT/INSERT/UPDATE/DELETE
        if (ctx.getState().isQuery() || parsedStmt instanceof DmlStmt) {
            String digest = digestFromLeader;
            if (digest == null && (Config.enable_sql_digest || ctx.getSessionVariable().isEnableSQLDigest())) {
                digest = computeStatementDigest(parsedStmt);
            }
            if (digest != null) {
                ctx.getAuditEventBuilder().setDigest(digest);
            }
            long threadAllocatedMemory =
                    getThreadAllocatedBytes(Thread.currentThread().getId()) - ctx.getCurrentThreadAllocatedMemory();
            ctx.getAuditEventBuilder().setQueryFeMemory(threadAllocatedMemory);
        }

        ctx.getAuditEventBuilder().setFeIp(FrontendOptions.getLocalHostAddress());
        ctx.getAuditEventBuilder().setQueriedRelations(
                AnalyzerUtils.collectAllTableAndViewRelationNamesForAudit(parsedStmt));

        ctx.getAuditEventBuilder().setStmt(formatStmt(origStmt, parsedStmt));

        AuditEvent auditEvent = ctx.getAuditEventBuilder().buildSnapshot();
        if (ctx.getState().isQuery() && ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            // multiply LONG by 10 so in metric system we can have more accurate result
            MetricRepo.HISTO_CACHE_MISS_RATIO.update((long) (auditEvent.getCacheMissRatio() * 10));
        }
        GlobalStateMgr.getCurrentState().getAuditEventProcessor().handleAuditEvent(auditEvent);
    }

    private String formatStmt(String origStmt, StatementBase parsedStmt) {
        if (!Config.enable_audit_sql) {
            return "?";
        }
        boolean needEncrypt = AuditEncryptionChecker.needEncrypt(parsedStmt);
        if (parsedStmt == null) {
            // invalid sql, record the original statement to avoid audit log can't replay
            // but redact sensitive credentials first
            if (Config.enable_sql_desensitize_in_log) {
                return "this is a desensitized invalid sql";
            }
            return SqlCredentialRedactor.redact(origStmt);
        } else if (AuditEncryptionChecker.needEncrypt(parsedStmt)) {
            return SqlCredentialRedactor.redact(origStmt);
        } else if (Config.enable_sql_desensitize_in_log) {
            // Some information like username, password in the stmt should not be printed.
            return AstToSQLBuilder.toSQL(parsedStmt, FormatOptions.allEnable()
                            .setColumnSimplifyTableName(false)
                            .setHideCredential(needEncrypt)
                            .setEnableDigest(Config.enable_sql_desensitize_in_log))
                    .orElse("this is a desensitized sql");
        } else {
            // Always redact credentials as defense in depth - raw SQL may contain
            // credentials that AuditEncryptionChecker does not yet recognize
            return SqlCredentialRedactor.redact(LogUtil.removeLineSeparator(origStmt));
        }
    }

    public static String computeStatementDigest(StatementBase queryStmt) {
        if (queryStmt == null) {
            return "";
        }

        try {
            String digest = AstToSQLBuilder.toDigest(queryStmt);
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.reset();
            md.update(digest.getBytes());
            return Hex.encodeHexString(md.digest());
        } catch (Exception e) {
            LOG.warn("Failed to compute statement digest", e);
            return "";
        }
    }

    private void resetAuditEventBuilder() {
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(ctx.getMysqlChannel().getRemoteHostPortString())
                .setUser(ctx.getQualifiedUser())
                .setAuthorizedUser(
                        ctx.getCurrentUserIdentity() == null ? "null" : ctx.getCurrentUserIdentity().toString())
                .setDb(ctx.getDatabase())
                .setCatalog(ctx.getCurrentCatalog())
                .setWarehouse(ctx.getCurrentWarehouseName())
                .setCustomQueryId(ctx.getCustomQueryId())
                .setCustomSessionName(ctx.getCustomSessionName())
                .setCNGroup(ctx.getCurrentComputeResourceName());
    }

    private String getAuditSql(StatementBase parsedStmt, String originStmt) {
        if (parsedStmt == null) {
            return originStmt;
        }
        if (!ctx.isMultiStmt()) {
            return originStmt;
        }
        return AstToSQLBuilder.toSQLOrDefault(parsedStmt, originStmt);
    }

    private String getDigestFromLeader(StmtExecutor stmtExecutor) {
        if (stmtExecutor == null || !stmtExecutor.getIsForwardToLeaderOrInit(false)
                || stmtExecutor.getLeaderOpExecutor() == null) {
            return null;
        }
        TMasterOpResult leaderResult = stmtExecutor.getLeaderOpExecutor().getResult();
        if (leaderResult != null && leaderResult.isSetSql_digest()) {
            return leaderResult.getSql_digest();
        }
        return null;
    }

    private void setStmtFailure(Throwable e, String auditSql) {
        if (e instanceof AnalysisException || e instanceof ErrorReportException
                || e instanceof SemanticException
                || (e instanceof StarRocksPlannerException
                && ((StarRocksPlannerException) e).getType() == ErrorType.USER_ERROR)) {
            LOG.info("Process one statement failed. SQL: {}, error: {}",
                    SqlCredentialRedactor.redact(auditSql), e.getMessage());
            ctx.getState().setError(e.getMessage());
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            return;
        }
        LOG.warn("Process one statement failed. SQL: {}", SqlCredentialRedactor.redact(auditSql), e);
        ctx.getState().setError(e.getMessage());
        ctx.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
    }

    private void auditCurrentStmt(String auditSql, StatementBase parsedStmt) {
        auditAfterExec(auditSql, parsedStmt,
                executor == null ? null : executor.getQueryStatisticsForAuditLog(),
                getDigestFromLeader(executor));
        if (executor != null) {
            executor.addFinishedQueryDetail();
        }
    }

    private void auditStmtFailure(Throwable e, StatementBase parsedStmt, String auditSql) {
        setStmtFailure(e, auditSql);
        auditCurrentStmt(auditSql, parsedStmt);
    }

    private void auditStmtFailureForStmt(Throwable e, StatementBase parsedStmt, String originStmt) {
        auditStmtFailure(e, parsedStmt, getAuditSql(parsedStmt, originStmt));
    }

    private List<StatementBase> parseStatements(String originStmt) throws AnalysisException {
        try (Timer ignored = Tracers.watchScope(Tracers.Module.PARSER, "Parser")) {
            return SqlParser.parse(originStmt, ctx.getSessionVariable());
        } catch (ParsingException parsingException) {
            throw new AnalysisException(parsingException.getMessage());
        }
    }

    private void resetStmtExecutionContext(boolean refreshQueryId) {
        executor = null;
        ctx.setExecutor(null);
        ctx.setQueryDetail(null);
        ctx.getState().reset();
        ctx.resetReturnRows();
        ctx.setStartTime();
        ctx.setCurrentThreadAllocatedMemory(getThreadAllocatedBytes(Thread.currentThread().getId()));
        resetAuditEventBuilder();
        if (refreshQueryId) {
            ctx.setQueryId(UUIDUtil.genUUID());
            // Keep executionId aligned with the current stmt before StmtExecutor.execute():
            // 1. StmtExecutor skips resetting executionId for cache-select.
            // 2. A stmt can fail before entering StmtExecutor.execute(), but we still need
            //    the current stmt's identity for the failure path and audit.
            ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        }
    }

    private void resetStmtRetryContext() {
        executor = null;
        ctx.setExecutor(null);
        ctx.getState().reset();
        ctx.resetReturnRows();
        resetAuditEventBuilder();
    }

    private StatementBase prepareStmtForExecution(StatementBase parsedStmt, String originStmt,
                                                  int stmtIdx, int stmtCount) throws AnalysisException {
        // from jdbc no params like that. COM_STMT_PREPARE + select 1
        if (ctx.getCommand() == MysqlCommand.COM_STMT_PREPARE && !(parsedStmt instanceof PrepareStmt)) {
            parsedStmt = new PrepareStmt("", parsedStmt, new ArrayList<>());
        }
        // only for JDBC, COM_STMT_PREPARE bundled with jdbc
        if (ctx.getCommand() == MysqlCommand.COM_STMT_PREPARE && (parsedStmt instanceof PrepareStmt)) {
            ((PrepareStmt) parsedStmt).setName(String.valueOf(ctx.getStmtId()));
            if (!(((PrepareStmt) parsedStmt).getInnerStmt() instanceof QueryStatement)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNSUPPORTED_PS, ErrorType.UNSUPPORTED);
            }
        }

        parsedStmt.setOrigStmt(new OriginStatement(originStmt, stmtIdx));
        Tracers.init(ctx, parsedStmt.getTraceMode(), parsedStmt.getTraceModule());
        ctx.setIsLastStmt(stmtIdx == stmtCount - 1);
        ctx.setMultiStmt(stmtCount > 1);
        // auditAfterExec() reads ctx.getState().isQuery() even when the stmt fails before
        // StmtExecutor.execute(), so we must classify the stmt here as well.
        ctx.getState().setIsQuery(ctx.isQueryStmt(parsedStmt));
        return parsedStmt;
    }

    // return true if validate failed, which means it should terminate
    private boolean validateStmtBeforeExecution(StatementBase parsedStmt, String originStmt) {
        try {
            if (ctx.getTxnId() != 0) {
                ExplicitTxnStatementValidator.validate(parsedStmt, ctx);
            }
            AuthenticationProvider authenticationProvider = ctx.getAuthenticationProvider();
            if (authenticationProvider == null) {
                ErrorReport.report("Unknown authentication method");
                ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
                return true;
            }
            authenticationProvider.checkLoginSuccess(ctx.getConnectionId(), ctx.getAccessControlContext());
            return false;
        } catch (AuthenticationException authenticationException) {
            if (authenticationException.getErrorCode() != null) {
                ErrorReport.report(authenticationException.getErrorCode(), authenticationException.getMessage());
            } else {
                ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED, authenticationException.getMessage());
            }
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            return true;
        } catch (Throwable e) {
            auditStmtFailureForStmt(e, parsedStmt, originStmt);
            throw e;
        }
    }

    // execute this sql and audit current sql unless throw LargeInPredicateException
    private void executeStmtWithAudit(StatementBase parsedStmt, String auditSql) throws Exception {
        try {
            executor = new StmtExecutor(ctx, parsedStmt);
            ctx.setExecutor(executor);

            //Build View SQL without Policy Rewrite
            new AstTraverser<Void, Void>() {
                @Override
                public Void visitRelation(Relation relation, Void context) {
                    relation.setNeedRewrittenByPolicy(true);
                    return null;
                }
            }.visit(parsedStmt);

            if (ctx.getQueryDetail() == null) {
                executor.addRunningQueryDetail(parsedStmt);
            }
            executor.execute();
        } catch (LargeInPredicateException e) {
            // we will retry this sql later, so don't audit here
            throw e;
        } catch (Throwable e) {
            auditStmtFailure(e, parsedStmt, auditSql);
            throw e;
        }
        auditCurrentStmt(auditSql, parsedStmt);
    }

    private StatementBase prepareRetriedStmt(StatementBase parsedStmt, String originStmt, int stmtCount)
            throws AnalysisException {
        // reparse the whole sql
        StatementBase retriedStmt = parseStatements(originStmt).get(parsedStmt.getOrigStmt().idx);
        resetStmtRetryContext();
        return prepareStmtForExecution(retriedStmt, originStmt, parsedStmt.getOrigStmt().idx, stmtCount);
    }

    private StatementBase prepareRetriedStmtWithAudit(StatementBase parsedStmt, String originStmt, int stmtCount)
            throws Exception {
        try {
            return prepareRetriedStmt(parsedStmt, originStmt, stmtCount);
        } catch (Throwable e) {
            // since this is the retry stmt, we have to audit
            auditStmtFailureForStmt(e, parsedStmt, originStmt);
            throw e;
        }
    }

    private void runWithStmtParserStageRetry(StatementBase parsedStmt, String originStmt, int stmtCount) throws Exception {
        try {
            executeStmtWithAudit(parsedStmt, getAuditSql(parsedStmt, originStmt));
        } catch (LargeInPredicateException e) {
            boolean originalEnableLargeInPredicate = ctx.getSessionVariable().enableLargeInPredicate();
            try {
                ctx.getSessionVariable().setEnableLargeInPredicate(false);
                LOG.warn("Retrying statement with enable_large_in_predicate=false, stmt idx: {}",
                        parsedStmt.getOrigStmt().idx);
                Tracers.record(Tracers.Module.BASE, "retry_with_large_in_predicate_exception", "true");
                StatementBase retriedStmt = prepareRetriedStmtWithAudit(parsedStmt, originStmt, stmtCount);
                // retry this sql, and audit
                executeStmtWithAudit(retriedStmt, getAuditSql(retriedStmt, originStmt));
            } finally {
                ctx.getSessionVariable().setEnableLargeInPredicate(originalEnableLargeInPredicate);
            }
        }
    }

    // process COM_QUERY statement,
    protected void handleQuery() {
        MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
        long beginMemory = getThreadAllocatedBytes(Thread.currentThread().getId());
        ctx.setCurrentThreadAllocatedMemory(beginMemory);

        // convert statement to Java string
        String originStmt = null;
        byte[] bytes = packetBuf.array();
        int ending = packetBuf.limit() - 1;
        while (ending >= 1 && bytes[ending] == '\0') {
            ending--;
        }
        originStmt = new String(bytes, 1, ending, StandardCharsets.UTF_8);
        resetAuditEventBuilder();
        Tracers.register(ctx);
        // set isQuery before `forwardToLeader` to make it right for audit log.
        ctx.getState().setIsQuery(true);

        // execute this query.
        boolean allStatementsAreSet = true;
        boolean parseSucceeded = false;
        try {
            List<StatementBase> stmts = parseStatements(originStmt);
            parseSucceeded = true;
            QueryAttemptResult attemptResult = executeQueryAttempt(originStmt, stmts);
            allStatementsAreSet = attemptResult.allStatementsAreSet;
            if (attemptResult.shouldTerminate) {
                return;
            }
        } catch (AnalysisException e) {
            if (!parseSucceeded) {
                LOG.warn("Failed to parse SQL: " + SqlCredentialRedactor.redact(originStmt) + ", because.", e);
            }
            ctx.getState().setError(e.getMessage());
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            // if parse failed, audit stmts together once
            if (!parseSucceeded) {
                auditAfterExec(originStmt, null, null, null);
            }
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe StarRocks bug.
            LOG.warn("Process one query failed. SQL: " + SqlCredentialRedactor.redact(originStmt) +
                    ", because unknown reason: ", e);
            ctx.getState().setError(e.getMessage());
            ctx.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
            // for safety
            if (!parseSucceeded) {
                auditAfterExec(originStmt, null, null, null);
            }
        } finally {
            Tracers.close();
            if (!allStatementsAreSet) {
                // custom_query_id session is temporary, should be cleared after query finished
                ctx.getSessionVariable().setCustomQueryId("");
            }
        }
    }

    private static class QueryAttemptResult {
        final boolean allStatementsAreSet;
        final boolean shouldTerminate;

        QueryAttemptResult(boolean allStatementsAreSet, boolean shouldTerminate) {
            this.allStatementsAreSet = allStatementsAreSet;
            this.shouldTerminate = shouldTerminate;
        }
    }

    private QueryAttemptResult executeQueryAttempt(String originStmt, List<StatementBase> stmts) throws Throwable {
        boolean allStatementsAreSet = true;

        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        if (Config.enable_print_sql) {
            LOG.info("Begin to execute sql, type: query, query id:{}, sql:{}", ctx.getQueryId(), originStmt);
        }

        for (int i = 0; i < stmts.size(); ++i) {
            resetStmtExecutionContext(i > 0);
            StatementBase parsedStmt = stmts.get(i);
            try {
                parsedStmt = prepareStmtForExecution(parsedStmt, originStmt, i, stmts.size());
            } catch (Throwable e) {
                auditStmtFailureForStmt(e, parsedStmt, originStmt);
                throw e;
            }
            if (!(parsedStmt instanceof SetStmt)) {
                allStatementsAreSet = false;
            }
            auditBeforeExec(getAuditSql(parsedStmt, originStmt), parsedStmt);
            if (validateStmtBeforeExecution(parsedStmt, originStmt)) {
                auditCurrentStmt(getAuditSql(parsedStmt, originStmt), parsedStmt);
                return new QueryAttemptResult(allStatementsAreSet, true);
            }

            runWithStmtParserStageRetry(parsedStmt, originStmt, stmts.size());
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                break;
            }

            if (i != stmts.size() - 1) {
                // NOTE: set serverStatus after executor.execute(),
                // because when execute() throws exception, the following stmt will not execute
                // and the serverStatus with MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS will
                // cause client error: Packet sequence number wrong
                ctx.getState().serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
                finalizeCommand();
            }
        }

        return new QueryAttemptResult(allStatementsAreSet, false);
    }

    // Get the column definitions of a table
    private void handleFieldList() throws IOException {
        // Already get command code.
        String tableName = new String(MysqlCodec.readNulTerminateString(packetBuf), StandardCharsets.UTF_8);
        if (Strings.isNullOrEmpty(tableName)) {
            ctx.getState().setError("Empty tableName");
            return;
        }
        Database db = ctx.getGlobalStateMgr().getMetadataMgr().getDb(ctx, ctx.getCurrentCatalog(), ctx.getDatabase());
        if (db == null) {
            ctx.getState().setError("Unknown database(" + ctx.getDatabase() + ")");
            return;
        }
        // Internal catalog: lookup resolves to Database.nameToTable (a ConcurrentHashMap,
        // lock-free safe). External catalog: lookup routes through ConnectorMetadata, whose
        // concurrency is connector-managed. No FE write path takes the LockManager lock on
        // external db ids, so the intensive lock acquired below is harmless overhead on
        // external catalogs and the unlocked lookup changes nothing for them.
        Table table;
        try {
            table = ctx.getGlobalStateMgr().getMetadataMgr().getTable(
                    ctx, ctx.getCurrentCatalog(), ctx.getDatabase(), tableName);
        } catch (StarRocksConnectorException e) {
            LOG.error("errors happened when getting table {}", tableName, e);
            ctx.getState().setEof();
            return;
        }
        if (table == null) {
            ctx.getState().setError("Unknown table(" + tableName + ")");
            return;
        }

        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            // Revalidate by name to detect concurrent DROP/RENAME between unlocked lookup
            // and lock acquisition. The table id is stable, so id-based revalidation would
            // miss a RENAME that re-binds the name to a different table. Internal-catalog
            // only: LocalMetastore doesn't track connector tables, so a re-fetch for
            // external catalogs would always return null and falsely fail.
            if (db.getCatalogName() == null && db.getTable(tableName) != table) {
                ctx.getState().setError("Unknown table(" + tableName + ")");
                return;
            }
            MysqlSerializer serializer = ctx.getSerializer();
            MysqlChannel channel = ctx.getMysqlChannel();

            // Send fields
            // NOTE: Field list doesn't send number of fields
            List<Column> baseSchema = table.getBaseSchema();
            for (Column column : baseSchema) {
                serializer.reset();
                serializer.writeField(db.getOriginName(), table.getName(), column, true);
                channel.sendOnePacket(serializer.toByteBuffer());
            }
        } catch (StarRocksConnectorException e) {
            LOG.error("errors happened when getting table {}", tableName, e);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }
        ctx.getState().setEof();
    }

    // prepared statement cmd COM_EXECUT
    // protocol
    // Type             Name Description
    // int<1>           status [0x17] COM_STMT_EXECUTE
    // int<4>           statement_id ID of the prepared statement to execute
    // int<1>           flags  Flags. See enum_cursor_type
    // int<4>           iteration_count Number of times to execute the statement. Currently always 1.
    // binary<var>      null_bitmap   NULL bitmap, length= (paramater_count + 7) / 8
    // int<1>           new_params_bind_flag  Flag if parameters must be re-bound
    // int<2>           parameter_type  Type of the parameter value. See enum_field_type
    // string<lenenc>   parameter_name Name of the parameter or empty if not present
    // binary<var>      parameter_values  value of each parameter
    // detail https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
    private void handleExecute() {
        packetBuf = packetBuf.order(ByteOrder.LITTLE_ENDIAN);
        // stmt_id
        int stmtId = packetBuf.getInt();
        // flag
        packetBuf.get();
        packetBuf.getInt();
        // cache statement
        PrepareStmtContext prepareCtx = ctx.getPreparedStmt(String.valueOf(stmtId));
        if (null == prepareCtx) {
            ctx.getState().setError("msg: Not Found prepared statement, stmtName: " + stmtId);
            return;
        }
        int numParams = prepareCtx.getStmt().getParameters().size();
        // null bitmap
        byte[] nullBitmap = new byte[(numParams + 7) / 8];
        packetBuf.get(nullBitmap);
        boolean enableAudit = false;
        String originStmt = null;
        ExecuteStmt executeStmt = null;
        boolean needAddFinishQueryDetail = false;
        try {
            ctx.setQueryId(UUIDUtil.genUUID());
            ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));

            // new_params_bind_flag
            if (packetBuf.hasRemaining() && (int) packetBuf.get() != 0) {
                // parse params types
                for (int i = 0; i < numParams; ++i) {
                    prepareCtx.getStmt().getMysqlTypeCodes().set(i, (int) packetBuf.getChar());
                }
            }
            // gene exprs
            List<Expr> exprs = new ArrayList<>();
            for (int i = 0; i < numParams; ++i) {
                if (isNull(nullBitmap, i)) {
                    exprs.add(new NullLiteral());
                    continue;
                }
                LiteralExpr literal = MysqlParamParser.createLiteral(
                        prepareCtx.getStmt().getMysqlTypeCodes().get(i), packetBuf);
                exprs.add(literal);
            }
            executeStmt = new ExecuteStmt(String.valueOf(stmtId), exprs);
            // audit will affect performance
            enableAudit = ctx.getSessionVariable().isAuditExecuteStmt();
            originStmt = AstToSQLBuilder.toSQL(executeStmt);
            executeStmt.setOrigStmt(new OriginStatement(originStmt, 0));
            ctx.setIsLastStmt(true);

            boolean isQuery = ctx.isQueryStmt(executeStmt);
            ctx.getState().setIsQuery(isQuery);

            if (isQuery) {
                // for query stmt, we should register and init tracer.
                Tracers.register(ctx);
                Tracers.init(ctx, executeStmt.getTraceMode(), executeStmt.getTraceModule());
                // set original statement to original query
                PrepareStmtContext prepareStmtContext = ctx.getPreparedStmt(executeStmt.getStmtName());
                if (prepareStmtContext != null) {
                    if (prepareStmtContext.getStmt().getInnerStmt() instanceof QueryStatement) {
                        PrepareStmt prepareStmt = prepareStmtContext.getStmt();
                        StatementBase deparameterizedStmt = prepareStmt.assignValues(executeStmt.getParamsExpr());
                        originStmt = AstToSQLBuilder.toSQL(deparameterizedStmt);
                        executeStmt.setOrigStmt(new OriginStatement(originStmt, 0));
                    }
                }
            }

            executor = new StmtExecutor(ctx, executeStmt);
            ctx.setExecutor(executor);
            if (enableAudit) {
                resetAuditEventBuilder();
                auditBeforeExec(originStmt, executeStmt);
            }

            if (enableAudit && isQuery) {
                executor.addRunningQueryDetail(executeStmt);
                needAddFinishQueryDetail = true;
                executor.execute();
                executor.addFinishedQueryDetail();
                needAddFinishQueryDetail = false;
            } else {
                // Clear query detail. Otherwise, after collecting the profile, it will be mistakenly added to ctx.queryDetail,
                // which still belongs to the previous query.
                ctx.setQueryDetail(null);
                executor.execute();
            }

            if (enableAudit) {
                auditAfterExec(originStmt, executor.getParsedStmt(),
                              executor.getQueryStatisticsForAuditLog(), getDigestFromLeader(executor));
            }
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe palo bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(e.getMessage());
            ctx.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
            if (enableAudit && executeStmt != null) {
                if (needAddFinishQueryDetail && executor != null) {
                    executor.addFinishedQueryDetail();
                }
                auditAfterExec(originStmt, executor == null ? executeStmt : executor.getParsedStmt(),
                        executor == null ? null : executor.getQueryStatisticsForAuditLog(),
                         getDigestFromLeader(executor));
            }
        }
    }

    private void handleStmtReset() {
        ctx.getState().setOk();
    }

    private void handleStmtClose() {
        int stmtId = packetBuf.getInt();
        ctx.removePreparedStmt(String.valueOf(stmtId));
        ctx.getState().setStateType(QueryState.MysqlStateType.NOOP);
    }

    private static boolean isNull(byte[] bitmap, int position) {
        return (bitmap[position / 8] & (0xff & (1 << (position & 7)))) != 0;
    }

    private void dispatch() throws IOException {
        int code = packetBuf.get();
        MysqlCommand command = MysqlCommand.fromCode(code);
        if (command == null) {
            ErrorReport.report(ErrorCode.ERR_UNKNOWN_COM_ERROR);
            ctx.getState().setError("Unknown command(" + command + ")");
            LOG.debug("Unknown MySQL protocol command");
            return;
        }
        ctx.setCommand(command);
        ctx.setStartTime();
        ctx.setUseConnectorMetadataCache(Optional.empty());
        ctx.setResourceGroup(null);
        ctx.resetErrorCode();

        switch (command) {
            case COM_INIT_DB:
                handleInitDb();
                break;
            case COM_QUIT:
                handleQuit();
                break;
            case COM_QUERY:
            case COM_STMT_PREPARE:
                handleQuery();
                ctx.setStartTime();
                break;
            case COM_STMT_RESET:
                handleStmtReset();
                break;
            case COM_STMT_CLOSE:
                handleStmtClose();
                break;
            case COM_FIELD_LIST:
                handleFieldList();
                break;
            case COM_CHANGE_USER:
                handleChangeUser();
                break;
            case COM_RESET_CONNECTION:
                handleResetConnection();
                break;
            case COM_PING:
                handlePing();
                break;
            case COM_STMT_EXECUTE:
                handleExecute();
                break;
            default:
                ctx.getState().setError("Unsupported command(" + command + ")");
                LOG.debug("Unsupported command: {}", command);
                break;
        }
    }

    private ByteBuffer getResultPacket() {
        MysqlPacket packet = ctx.getState().toResponsePacket();
        if (packet == null) {
            // possible two cases:
            // 1. handler has send response
            // 2. this command need not to send response
            return null;
        }

        MysqlSerializer serializer = ctx.getSerializer();
        serializer.reset();
        packet.writeTo(serializer);
        return serializer.toByteBuffer();
    }

    // use to return result packet to user
    private void finalizeCommand() throws IOException {
        ByteBuffer packet = null;
        if (executor != null && executor.isForwardToLeader()) {
            // for ERR State, set packet to remote packet(executor.getOutputPacket())
            //      because remote packet has error detail
            // but for not ERR (OK or EOF) State, we should check whether stmt is ShowStmt,
            //      because there is bug in remote packet for ShowStmt on lower fe version
            //      bug is: Success ShowStmt should be EOF package but remote packet is not
            // so we should use local packet(getResultPacket()),
            // there is no difference for Success ShowStmt between remote package and local package in new version fe
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                packet = executor.getOutputPacket();
                // Protective code
                if (packet == null) {
                    packet = getResultPacket();
                    if (packet == null) {
                        LOG.debug("packet == null");
                        return;
                    }
                }
            } else {
                ShowResultSet resultSet = executor.getShowResultSet();
                // for lower version fe, all forwarded command is OK or EOF State, so we prefer to use remote packet for
                // compatible
                // ShowResultSet is null means this is not ShowStmt, use remote packet(executor.getOutputPacket())
                // or use local packet (getResultPacket())
                if (resultSet == null) {
                    if (executor.sendResultToChannel(ctx.getMysqlChannel())) {  // query statement result
                        packet = getResultPacket();
                    } else { // for lower version, in consideration of compatibility
                        packet = executor.getOutputPacket();
                    }
                } else { // show statement result
                    executor.sendShowResult(resultSet);
                    packet = getResultPacket();
                    if (packet == null) {
                        LOG.debug("packet == null");
                        return;
                    }
                }
            }
        } else {
            packet = getResultPacket();
            if (packet == null) {
                LOG.debug("packet == null");
                return;
            }
        }

        MysqlChannel channel = ctx.getMysqlChannel();
        channel.sendAndFlush(packet);

        // only change lastQueryId when current command is COM_QUERY
        MysqlCommand cmd = ctx.getCommand();
        if (cmd == MysqlCommand.COM_QUERY || cmd == MysqlCommand.COM_STMT_PREPARE || cmd == MysqlCommand.COM_STMT_EXECUTE) {
            boolean skipSetLastQueryId = executor != null && 
                    executor.getParsedStmt() instanceof com.starrocks.sql.ast.AnalyzeProfileStmt;
            if (!skipSetLastQueryId) {
                ctx.setLastQueryId(ctx.getQueryId());
            }
            ctx.setQueryId(null);
        }
    }

    public TMasterOpResult proxyExecute(TMasterOpRequest request, Frontend requestFE) {
        ctx.setCurrentCatalog(request.catalog);
        if (ctx.getCurrentCatalog() == null) {
            // if we upgrade Master FE first, the request from old FE does not set "catalog".
            // so ctx.getCurrentCatalog() will get null,
            // return error directly.
            TMasterOpResult result = new TMasterOpResult();
            ctx.getState().setError(
                    "Missing current catalog. You need to upgrade this Frontend to the same version as Leader Frontend.");
            result.setMaxJournalId(GlobalStateMgr.getCurrentState().getMaxJournalId());
            result.setPacket(getResultPacket());
            result.setState(ctx.getState().getStateType().toString());
            result.setErrorMsg(ctx.getState().getErrorMessage());
            return result;
        }
        ctx.setDatabase(request.db);
        ctx.setQualifiedUser(request.user);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        ctx.getState().reset();
        ctx.setMultiStmt(false);
        if (request.isSetResourceInfo()) {
            ctx.getSessionVariable().setResourceGroup(request.getResourceInfo().getGroup());
        }
        if (request.isSetUser_ip()) {
            ctx.setRemoteIP(request.getUser_ip());
        }
        if (request.isSetTime_zone()) {
            ctx.getSessionVariable().setTimeZone(request.getTime_zone());
        }
        if (request.isSetStmt_id()) {
            ctx.setForwardedStmtId(request.getStmt_id());
        }
        if (request.isSetSqlMode()) {
            ctx.getSessionVariable().setSqlMode(request.sqlMode);
        }
        if (request.isSetEnableStrictMode()) {
            ctx.getSessionVariable().setEnableInsertStrict(request.enableStrictMode);
        }
        if (request.isSetCurrent_user_ident()) {
            UserIdentity currentUserIdentity = UserIdentityUtils.fromThrift(request.getCurrent_user_ident());
            ctx.setCurrentUserIdentity(currentUserIdentity);
        }

        if (ctx.getCurrentUserIdentity() == null) {
            TMasterOpResult result = new TMasterOpResult();
            ctx.getState().setError(
                    "Missing current user identity. You need to upgrade this Frontend to the same version as Leader Frontend.");
            result.setMaxJournalId(GlobalStateMgr.getCurrentState().getMaxJournalId());
            result.setPacket(getResultPacket());
            result.setState(ctx.getState().getStateType().toString());
            result.setErrorMsg(ctx.getState().getErrorMessage());
            return result;
        }

        if (request.isSetUser_groups()) {
            ctx.setGroups(new HashSet<>(request.getUser_groups()));
        }

        if (request.isSetUser_roles()) {
            List<Long> roleIds = request.getUser_roles().getRole_id_list();
            ctx.setCurrentRoleIds(new HashSet<>(roleIds));
        } else {
            ctx.setCurrentRoleIds(new HashSet<>());
        }

        UserIdentity userIdentity = ctx.getCurrentUserIdentity();
        if (!userIdentity.isEphemeral()) {
            UserProperty userProperty = GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                    .getUserProperty(ctx.getCurrentUserIdentity().getUser());
            ctx.updateByUserProperty(userProperty);
        }

        // after https://github.com/StarRocks/starrocks/pull/43162, we support temporary tables.
        // DDL/DML operations related to temporary tables are bound to a specific session,
        // so the request forwarded by the follower needs to specifically set the session id.

        //  During the grayscale upgrade process,
        //  if the leader is a new version and the follower is an old version,
        //  the forwarded request won't have a session id.
        //  Considering that the old version FE does not support operations related to temporary tables,
        //  the session id is not necessary at this time.
        //  in this case, we just set a random session id to ensure that subsequent processing can be processed normally.
        if (request.isSetSession_id()) {
            ctx.setSessionId(UUID.fromString(request.getSession_id()));
        } else {
            ctx.setSessionId(UUIDUtil.genUUID());
        }

        if (request.isSetIsLastStmt()) {
            ctx.setIsLastStmt(request.isIsLastStmt());
        } else {
            // if the caller is lower version fe, request.isSetIsLastStmt() may return false.
            // in this case, set isLastStmt to true, because almost stmt is single stmt
            // but when the original stmt is multi stmt the caller will encounter mysql error: Packet sequence number wrong
            ctx.setIsLastStmt(true);
        }

        if (request.isSetQuery_options()) {
            TQueryOptions queryOptions = request.getQuery_options();
            if (queryOptions.isSetMem_limit()) {
                ctx.getSessionVariable().setMaxExecMemByte(queryOptions.getMem_limit());
            }
            if (queryOptions.isSetQuery_timeout()) {
                ctx.getSessionVariable().setQueryTimeoutS(queryOptions.getQuery_timeout());
            }
            if (queryOptions.isSetLoad_mem_limit()) {
                ctx.getSessionVariable().setLoadMemLimit(queryOptions.getLoad_mem_limit());
            }
            if (queryOptions.isSetMax_scan_key_num()) {
                ctx.getSessionVariable().setMaxScanKeyNum(queryOptions.getMax_scan_key_num());
            }
            if (queryOptions.isSetMax_pushdown_conditions_per_column()) {
                ctx.getSessionVariable().setMaxPushdownConditionsPerColumn(
                        queryOptions.getMax_pushdown_conditions_per_column());
            }
        } else {
            // for compatibility, all following variables are moved to TQueryOptions.
            if (request.isSetExecMemLimit()) {
                ctx.getSessionVariable().setMaxExecMemByte(request.getExecMemLimit());
            }
            if (request.isSetQueryTimeout()) {
                ctx.getSessionVariable().setQueryTimeoutS(request.getQueryTimeout());
            }
            if (request.isSetLoadMemLimit()) {
                ctx.getSessionVariable().setLoadMemLimit(request.loadMemLimit);
            }
        }

        if (request.isSetQueryId()) {
            ctx.setQueryId(UUIDUtil.fromTUniqueid(request.getQueryId()));
        }

        if (request.isSetWarehouse_id()) {
            ctx.setCurrentWarehouseId(request.getWarehouse_id());
        }

        if (request.isSetForward_times()) {
            ctx.setForwardTimes(request.getForward_times());
        }

        if (request.isSetConnectionId()) {
            ctx.setConnectionId(request.getConnectionId());
        }

        if (request.isSetTxn_id()) {
            ctx.setTxnId(request.getTxn_id());
        }

        ctx.setThreadLocalInfo();

        TMasterOpResult result = new TMasterOpResult();
        StmtExecutor executor = null;
        try {
            // set session variables first
            if (request.isSetModified_variables_sql()) {
                LOG.info("Set session variables first: {}", request.modified_variables_sql);

                StatementBase statement = SqlParser.parseSingleStatement(request.modified_variables_sql,
                        ctx.getSessionVariable().getSqlMode());
                executor = StmtExecutor.newInternalExecutor(ctx, statement);
                executor.setProxy();
                executor.execute();
            }
            // 0 for compatibility.
            int idx = request.isSetStmtIdx() ? request.getStmtIdx() : 0;

            List<StatementBase> stmts = SqlParser.parse(request.getSql(), ctx.getSessionVariable());
            ctx.setMultiStmt(stmts.size() > 1);
            StatementBase statement = stmts.get(idx);
            //Build View SQL without Policy Rewrite
            new AstTraverser<Void, Void>() {
                @Override
                public Void visitRelation(Relation relation, Void context) {
                    relation.setNeedRewrittenByPolicy(true);
                    return null;
                }
            }.visit(statement);
            statement.setOrigStmt(new OriginStatement(request.getSql(), idx));

            executor = doProxyExecute(result, request, statement, requestFE);
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError("StarRocks process failed: " + e.getMessage());
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe StarRocks bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(e.getMessage());
        } finally {
            if (executor != null) {
                executor.addFinishedQueryDetail();
            }
        }

        // If stmt is also forwarded during execution, just return the forward result.
        if (executor != null && executor.getIsForwardToLeaderOrInit(false)) {
            return executor.getLeaderOpExecutor().getResult();
        }

        // no matter the master execute success or fail, the master must transfer the result to follower
        // and tell the follower the current journalID.
        result.setMaxJournalId(GlobalStateMgr.getCurrentState().getMaxJournalId());
        if (RunMode.isSharedDataMode()) {
            result.setMaxStarMgrJournalId(StarMgrServer.getCurrentState().getMaxJournalId());
        }
        // following stmt will not be executed, when current stmt is failed,
        // so only set SERVER_MORE_RESULTS_EXISTS Flag when stmt executed successfully
        if (!ctx.getIsLastStmt()
                && ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            ctx.getState().serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
        }
        result.setPacket(getResultPacket());
        result.setState(ctx.getState().getStateType().toString());
        result.setErrorMsg(ctx.getState().getErrorMessage());
        //Put the txnId in connectContext into result and pass it back to the follower node
        result.setTxn_id(ctx.getTxnId());

        if (executor != null) {
            if (executor.getProxyResultSet() != null) {  // show statement
                result.setResultSet(executor.getProxyResultSet().tothrift());
            } else if (executor.getProxyResultBuffer() != null) {  // query statement
                result.setChannelBufferList(executor.getProxyResultBuffer());
            }

            String resourceGroupName = ctx.getAuditEventBuilder().build().resourceGroup;
            if (StringUtils.isNotEmpty(resourceGroupName)) {
                result.setResource_group_name(resourceGroupName);
            }

            PQueryStatistics audit = executor.getQueryStatisticsForAuditLog();
            if (audit != null) {
                result.setAudit_statistics(AuditStatisticsUtil.toThrift(audit));
            }
        }
        return result;
    }

    protected StmtExecutor doProxyExecute(TMasterOpResult result, TMasterOpRequest request, StatementBase statement,
                                          Frontend requestFE)
            throws Exception {
        StmtExecutor executor;
        if (request.isIsInternalStmt()) {
            executor = StmtExecutor.newInternalExecutor(ctx, statement);
        } else {
            executor = new StmtExecutor(ctx, statement);
        }
        ctx.setExecutor(executor);
        executor.addRunningQueryDetail(statement);
        executor.setProxy();
        executor.execute();

        if (executor.getParsedStmt() != null) {
            StatementBase parsedStmt = executor.getParsedStmt();
            if ((Config.enable_sql_digest || ctx.getSessionVariable().isEnableSQLDigest()) &&
                    (ctx.getState().isQuery() || parsedStmt instanceof DmlStmt)) {
                String digest = computeStatementDigest(parsedStmt);
                if (!digest.isEmpty()) {
                    result.setSql_digest(digest);
                }
            }
        }

        return executor;
    }

    public void processOnce(RequestPackage req) throws Exception {
        // set status of query to OK.
        ctx.getState().reset();
        ctx.setMultiStmt(false);
        executor = null;

        packetBuf = req.byteBuffer();

        final MysqlChannel channel = ctx.getMysqlChannel();
        channel.setSequenceId(req.packageId());
        channel.accSequenceId();

        dispatch();

        finalizeCommand();

        ctx.setCommand(MysqlCommand.COM_SLEEP);
        ctx.setEndTime();
    }

    // handle one process
    public void processOnce() throws IOException {
        // set status of query to OK.
        ctx.getState().reset();
        ctx.setMultiStmt(false);
        executor = null;

        // reset sequence id of MySQL protocol
        final MysqlChannel channel = ctx.getMysqlChannel();
        channel.setSequenceId(0);
        // read packet from channel
        try {
            packetBuf = channel.fetchOnePacket();
            if (packetBuf == null) {
                throw new RpcException(ctx.getRemoteIP(), "Error happened when receiving packet.");
            }
        } catch (AsynchronousCloseException e) {
            // when this happened, timeout checker close this channel
            // killed flag in ctx has been already set, just return
            return;
        }

        // dispatch
        dispatch();
        // finalize
        finalizeCommand();

        ctx.setCommand(MysqlCommand.COM_SLEEP);
        ctx.setEndTime();
    }

    protected void loopForTest() {
        while (!ctx.isKilled()) {
            try {
                processOnce();
            } catch (RpcException rpce) {
                LOG.debug("Exception happened in one session(" + ctx + ").", rpce);
                ctx.setKilled();
                break;
            } catch (Exception e) {
                // TODO(zhaochun): something wrong
                LOG.warn("Exception happened in one seesion(" + ctx + ").", e);
                ctx.setKilled();
                break;
            }
        }
    }

    public StmtExecutor getExecutor() {
        return executor;
    }
}
