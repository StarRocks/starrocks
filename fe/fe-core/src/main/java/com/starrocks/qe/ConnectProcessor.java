// This file is made available under Elastic License 2.0.
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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.AuditStatisticsUtil;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.StarRocksIcebergException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.ResourceGroupMetricMgr;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlPacket;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.mysql.MysqlServerStatusFlag;
import com.starrocks.plugin.AuditEvent.EventType;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.SqlDigestBuilder;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TWorkGroup;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;

/**
 * Process one mysql connection, receive one pakcet, process, send one packet.
 */
public class ConnectProcessor {
    private static final Logger LOG = LogManager.getLogger(ConnectProcessor.class);

    private final ConnectContext ctx;
    private ByteBuffer packetBuf;

    private StmtExecutor executor = null;

    public ConnectProcessor(ConnectContext context) {
        this.ctx = context;
    }

    // COM_INIT_DB: change current database of this session.
    private void handleInitDb() {
        String identifier = new String(packetBuf.array(), 1, packetBuf.limit() - 1);
        try {
            String[] parts = identifier.trim().split("\\s+");
            if (parts.length == 2) {
                Preconditions.checkState(parts[0].equalsIgnoreCase("catalog"),
                        "You might want to use \"USE 'CATALOG <catalog_name>'\"");
                ctx.getGlobalStateMgr().changeCatalog(ctx, parts[1]);
            } else {
                ctx.getGlobalStateMgr().changeCatalogDb(ctx, identifier);
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

    public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics) {
        // slow query
        long endTime = System.currentTimeMillis();
        long elapseMs = endTime - ctx.getStartTime();

        boolean isForwardToLeader = (executor != null) ? executor.getIsForwardToLeaderOrInit(false) : false;

        // ignore recording some failed stmt like kill connection
        if (ctx.getState().getErrType() == QueryState.ErrType.IGNORE_ERR) {
            return;
        }

        // TODO how to unify TStatusCode, ErrorCode, ErrType, ConnectContext.errorCode
        String errorCode = StringUtils.isNotEmpty(ctx.getErrorCode()) ? ctx.getErrorCode() : ctx.getState().getErrType().name();
        ctx.getAuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                .setState(ctx.getState().toString())
                .setErrorCode(errorCode)
                .setQueryTime(elapseMs)
                .setReturnRows(ctx.getReturnRows())
                .setStmtId(ctx.getStmtId())
                .setIsForwardToLeader(isForwardToLeader)
                .setQueryId(ctx.getQueryId() == null ? "NaN" : ctx.getQueryId().toString());
        if (statistics != null) {
            ctx.getAuditEventBuilder().setScanBytes(statistics.scanBytes);
            ctx.getAuditEventBuilder().setScanRows(statistics.scanRows);
            ctx.getAuditEventBuilder().setCpuCostNs(statistics.cpuCostNs == null ? -1 : statistics.cpuCostNs);
            ctx.getAuditEventBuilder().setMemCostBytes(statistics.memCostBytes == null ? -1 : statistics.memCostBytes);
            ctx.getAuditEventBuilder().setReturnRows(statistics.returnedRows == null ? 0 : statistics.returnedRows);
        }

        if (ctx.getState().isQuery()) {
            MetricRepo.COUNTER_QUERY_ALL.increase(1L);
            ResourceGroupMetricMgr.increaseQuery(ctx, 1L);
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                // err query
                MetricRepo.COUNTER_QUERY_ERR.increase(1L);
                ResourceGroupMetricMgr.increaseQueryErr(ctx, 1L);
            } else {
                // ok query
                MetricRepo.COUNTER_QUERY_SUCCESS.increase(1L);
                MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
                ResourceGroupMetricMgr.updateQueryLatency(ctx, elapseMs);
                if (elapseMs > Config.qe_slow_log_ms || ctx.getSessionVariable().isEnableSQLDigest()) {
                    MetricRepo.COUNTER_SLOW_QUERY.increase(1L);
                    ctx.getAuditEventBuilder().setDigest(computeStatementDigest(parsedStmt));
                }
            }
            ctx.getAuditEventBuilder().setIsQuery(true);
        } else {
            ctx.getAuditEventBuilder().setIsQuery(false);
        }

        ctx.getAuditEventBuilder().setFeIp(FrontendOptions.getLocalHostAddress());

        if (!ctx.getState().isQuery() && (parsedStmt != null && parsedStmt.needAuditEncryption())) {
            // Some information like username, password in the stmt should not be printed.
            ctx.getAuditEventBuilder().setStmt(AstToSQLBuilder.toSQL(parsedStmt));
        } else if (parsedStmt == null) {
            // invalid sql, record the original statement to avoid audit log can't replay
            ctx.getAuditEventBuilder().setStmt(origStmt);
        } else {
            ctx.getAuditEventBuilder().setStmt(LogUtil.removeLineSeparator(origStmt));
        }

        GlobalStateMgr.getCurrentAuditEventProcessor().handleAuditEvent(ctx.getAuditEventBuilder().build());
    }

    public String computeStatementDigest(StatementBase queryStmt) {
        if (queryStmt == null) {
            return "";
        }

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

    private boolean containsComment(String sql) {
        return (sql.contains("--")) || sql.contains("#");
    }

    private void addFinishedQueryDetail() {
        if (!Config.enable_collect_query_detail_info) {
            return;
        }
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
        queryDetail.setResourceGroupName(ctx.getResourceGroup() != null ? ctx.getResourceGroup().getName() : "");
        QueryDetailQueue.addQueryDetail(queryDetail);
    }

    protected void addRunningQueryDetail(StatementBase parsedStmt) {
        if (!Config.enable_collect_query_detail_info) {
            return;
        }
        String sql;
        if (!ctx.getState().isQuery() && parsedStmt.needAuditEncryption()) {
            sql = AstToSQLBuilder.toSQL(parsedStmt);
        } else {
            sql = parsedStmt.getOrigStmt().originStmt;
        }

        boolean isQuery = parsedStmt instanceof QueryStatement;
        QueryDetail queryDetail = new QueryDetail(
                DebugUtil.printId(ctx.getQueryId()),
                isQuery,
                ctx.connectionId,
                ctx.getMysqlChannel() != null ?
                        ctx.getMysqlChannel().getRemoteIp() : "System",
                ctx.getStartTime(), -1, -1,
                QueryDetail.QueryMemState.RUNNING,
                ctx.getDatabase(),
                sql,
                ctx.getQualifiedUser(),
                Optional.ofNullable(ctx.getResourceGroup()).map(TWorkGroup::getName).orElse(""));
        ctx.setQueryDetail(queryDetail);
        // copy queryDetail, cause some properties can be changed in future
        QueryDetailQueue.addQueryDetail(queryDetail.copy());
    }

    // process COM_QUERY statement,
    private void handleQuery() {
        MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
        // convert statement to Java string
        String originStmt = null;
        byte[] bytes = packetBuf.array();
        int ending = packetBuf.limit() - 1;
        while (ending >= 1 && bytes[ending] == '\0') {
            ending--;
        }
        originStmt = new String(bytes, 1, ending, StandardCharsets.UTF_8);
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(ctx.getMysqlChannel().getRemoteHostPortString())
                .setUser(ctx.getQualifiedUser())
                .setAuthorizedUser(
                        ctx.getCurrentUserIdentity() == null ? "null" : ctx.getCurrentUserIdentity().toString())
                .setDb(ctx.getDatabase())
                .setCatalog(ctx.getCurrentCatalog());
        ctx.getPlannerProfile().reset();

        // execute this query.
        StatementBase parsedStmt = null;
        try {
            ctx.setQueryId(UUIDUtil.genUUID());
            List<StatementBase> stmts;
            try {
                stmts = com.starrocks.sql.parser.SqlParser.parse(originStmt, ctx.getSessionVariable());
            } catch (ParsingException parsingException) {
                throw new AnalysisException(parsingException.getMessage());
            }

            for (int i = 0; i < stmts.size(); ++i) {
                ctx.getState().reset();
                if (i > 0) {
                    ctx.resetReturnRows();
                    ctx.setQueryId(UUIDUtil.genUUID());
                }
                parsedStmt = stmts.get(i);
                parsedStmt.setOrigStmt(new OriginStatement(originStmt, i));

                // Only add the last running stmt for multi statement,
                // because the audit log will only show the last stmt.
                if (i == stmts.size() - 1) {
                    addRunningQueryDetail(parsedStmt);
                }

                executor = new StmtExecutor(ctx, parsedStmt);
                ctx.setExecutor(executor);

                ctx.setIsLastStmt(i == stmts.size() - 1);

                executor.execute();

                // do not execute following stmt when current stmt failed, this is consistent with mysql server
                if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                    break;
                }

                if (i != stmts.size() - 1) {
                    // NOTE: set serverStatus after executor.execute(),
                    //      because when execute() throws exception, the following stmt will not execute
                    //      and the serverStatus with MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS will
                    //      cause client error: Packet sequence number wrong
                    ctx.getState().serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
                    finalizeCommand();
                }
            }
        } catch (AnalysisException e) {
            LOG.warn("Failed to parse SQL: " + originStmt + ", because.", e);
            ctx.getState().setError(e.getMessage());
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe StarRocks bug.
            LOG.warn("Process one query failed. SQL: " + originStmt + ", because unknown reason: ", e);
            ctx.getState().setError("Unexpected exception: " + e.getMessage());
            ctx.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
        }
        
        // audit after exec
        // replace '\n' to '\\n' to make string in one line
        // TODO(cmy): when user send multi-statement, the executor is the last statement's executor.
        // We may need to find some way to resolve this.
        if (executor != null) {
            auditAfterExec(originStmt, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
        } else {
            // executor can be null if we encounter analysis error.
            auditAfterExec(originStmt, null, null);
        }

        addFinishedQueryDetail();
    }

    // Get the column definitions of a table
    private void handleFieldList() throws IOException {
        // Already get command code.
        String tableName = new String(MysqlProto.readNulTerminateString(packetBuf), StandardCharsets.UTF_8);
        if (Strings.isNullOrEmpty(tableName)) {
            ctx.getState().setError("Empty tableName");
            return;
        }
        Database db = ctx.getGlobalStateMgr().getMetadataMgr().getDb(ctx.getCurrentCatalog(), ctx.getDatabase());
        if (db == null) {
            ctx.getState().setError("Unknown database(" + ctx.getDatabase() + ")");
            return;
        }
        db.readLock();
        try {
            // we should get table through metadata manager
            Table table = ctx.getGlobalStateMgr().getMetadataMgr().getTable(
                    ctx.getCurrentCatalog(), ctx.getDatabase(), tableName);
            if (table == null) {
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

        } catch (StarRocksIcebergException e) {
            LOG.error("errors happened when getting Iceberg table {}", tableName, e);
        } catch (StarRocksConnectorException e) {
            LOG.error("errors happened when getting table {}", tableName, e);
        } finally {
            db.readUnlock();
        }
        ctx.getState().setEof();
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
        ctx.setResourceGroup(null);
        ctx.setErrorCode("");

        switch (command) {
            case COM_INIT_DB:
                handleInitDb();
                break;
            case COM_QUIT:
                handleQuit();
                break;
            case COM_QUERY:
                handleQuery();
                ctx.setStartTime();
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
                // for lower version fe, all forwarded command is OK or EOF State, so we prefer to use remote packet for compatible
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
        if (ctx.getCommand() == MysqlCommand.COM_QUERY) {
            ctx.setLastQueryId(ctx.queryId);
            ctx.setQueryId(null);
        }
    }

    public TMasterOpResult proxyExecute(TMasterOpRequest request) {
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
            return result;
        }
        ctx.setDatabase(request.db);
        ctx.setQualifiedUser(request.user);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.getState().reset();
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
            UserIdentity currentUserIdentity = UserIdentity.fromThrift(request.getCurrent_user_ident());
            ctx.setCurrentUserIdentity(currentUserIdentity);
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

        ctx.setThreadLocalInfo();

        if (ctx.getCurrentUserIdentity() == null) {
            // if we upgrade Master FE first, the request from old FE does not set "current_user_ident".
            // so ctx.getCurrentUserIdentity() will get null, and causing NullPointerException after using it.
            // return error directly.
            TMasterOpResult result = new TMasterOpResult();
            ctx.getState().setError(
                    "Missing current user identity. You need to upgrade this Frontend to the same version as Leader Frontend.");
            result.setMaxJournalId(GlobalStateMgr.getCurrentState().getMaxJournalId());
            result.setPacket(getResultPacket());
            return result;
        }

        StmtExecutor executor = null;
        try {
            // set session variables first
            if (request.isSetModified_variables_sql()) {
                LOG.info("Set session variables first: {}", request.modified_variables_sql);
                new StmtExecutor(ctx, new OriginStatement(request.modified_variables_sql, 0), true).execute();
            }
            // 0 for compatibility.
            int idx = request.isSetStmtIdx() ? request.getStmtIdx() : 0;
            executor = new StmtExecutor(ctx, new OriginStatement(request.getSql(), idx), true);
            executor.execute();
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError("StarRocks process failed: " + e.getMessage());
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe StarRocks bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError("Unexpected exception: " + e.getMessage());
        }
        // no matter the master execute success or fail, the master must transfer the result to follower
        // and tell the follower the current jounalID.
        TMasterOpResult result = new TMasterOpResult();
        result.setMaxJournalId(GlobalStateMgr.getCurrentState().getMaxJournalId());
        // following stmt will not be executed, when current stmt is failed,
        // so only set SERVER_MORE_RESULTS_EXISTS Flag when stmt executed successfully
        if (!ctx.getIsLastStmt()
                && ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            ctx.getState().serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
        }
        result.setPacket(getResultPacket());
        result.setState(ctx.getState().getStateType().toString());
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

    // handle one process
    public void processOnce() throws IOException {
        // set status of query to OK.
        ctx.getState().reset();
        executor = null;

        // reset sequence id of MySQL protocol
        final MysqlChannel channel = ctx.getMysqlChannel();
        channel.setSequenceId(0);
        // read packet from channel
        try {
            packetBuf = channel.fetchOnePacket();
            if (packetBuf == null) {
                throw new IOException("Error happened when receiving packet.");
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
    }

    public void loop() {
        while (!ctx.isKilled()) {
            try {
                processOnce();
            } catch (Exception e) {
                // TODO(zhaochun): something wrong
                LOG.warn("Exception happened in one seesion(" + ctx + ").", e);
                ctx.setKilled();
                break;
            }
        }
    }
}
