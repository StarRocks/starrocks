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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TNotifyForwardDeploymentFinishedRequest;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

// inherit ConnectProcessor to record the audit log and Query Detail
public class ArrowFlightSqlConnectProcessor extends ConnectProcessor {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlConnectProcessor.class);

    private final String originStmt;

    private final CompletableFuture<ArrowFlightSqlResultDescriptor> deploymentFinished = new CompletableFuture<>();
    CompletableFuture<Void> processorFinished = new CompletableFuture<>();

    public ArrowFlightSqlConnectProcessor(ConnectContext context, String originStmt) {
        super(context);
        this.originStmt = originStmt;
    }

    public ArrowFlightSqlResultDescriptor execute() throws InterruptedException, StarRocksException, ExecutionException {
        ArrowFlightSqlConnectContext arrowCtx = (ArrowFlightSqlConnectContext) ctx;
        long waitTimeoutMs = Math.max(0, Config.arrow_flight_sql_connection_query_wait_timeout_ms);
        if (!arrowCtx.acquireRunningToken(waitTimeoutMs)) {
            throw new StarRocksException(
                    "Another query is already in progress on this connection (waited " + waitTimeoutMs + " ms)");
        }
        arrowCtx.resetForStatement();

        try {
            ArrowFlightSqlServiceImpl.submitTask(() -> {
                final boolean prevUseLowCardOptimizeOnLake = arrowCtx.getSessionVariable().isUseLowCardinalityOptimizeOnLake();
                try {
                    // The Lake low-cardinality optimization relies on a retry mechanism on the FE side: when the BE discovers at
                    // execution time that the dictionary cannot be used, it reports this to the FE, and the FE re-plans the query.
                    // However, with Arrow Flight SQL the client talks directly to the BE, so this retry mechanism is not available.
                    arrowCtx.getSessionVariable().setUseLowCardinalityOptimizeOnLake(false);
                    arrowCtx.setThreadLocalInfo();

                    processOnce();

                    deploymentFinished.complete(null);
                    processorFinished.complete(null);
                } catch (Throwable t) {
                    deploymentFinished.completeExceptionally(t);
                    processorFinished.completeExceptionally(t);
                } finally {
                    arrowCtx.getSessionVariable().setUseLowCardinalityOptimizeOnLake(prevUseLowCardOptimizeOnLake);
                    arrowCtx.releaseRunningToken();
                }
            });
        } catch (Exception t) {
            // Task submission failed, ensure state is cleaned up so callers don't hang.
            deploymentFinished.completeExceptionally(t);
            processorFinished.completeExceptionally(t);
            arrowCtx.releaseRunningToken();
            throw t;
        }

        // Wait util deployment finished or the whole process finished.
        ArrowFlightSqlResultDescriptor backendResult = deploymentFinished.get();

        // Query task will wait until deployment to BE is finished and return BE as endpoint.
        if (backendResult != null) {
            if (ctx.getState().isError()) {
                reportError(ctx);
            }
            return backendResult;
        }

        // FE task will return FE as endpoint.
        processorFinished.get(); // Wait `processor.processOnce()` to finish.

        if (ctx.getState().isError()) {
            reportError(ctx);
        }

        if (executor != null && executor.isForwardToLeader()) {
            ShowResultSet showResultSet = executor.getShowResultSet();
            if (showResultSet != null) {
                arrowCtx.addShowResult(DebugUtil.printId(arrowCtx.getExecutionId()), showResultSet);
            }
        }

        String queryId = DebugUtil.printId(arrowCtx.getExecutionId());
        VectorSchemaRoot result = arrowCtx.getResult(queryId);
        if (result == null) {
            arrowCtx.setEmptyResultIfNotExist(queryId);
            result = arrowCtx.getResult(queryId);
        }

        return new ArrowFlightSqlResultDescriptor(result.getSchema());
    }

    @Override
    public void processOnce() {
        // Set status of query to OK.
        ctx.getState().reset();
        executor = null;

        // Only handle query，so no need to dispatch
        ctx.setCommand(MysqlCommand.COM_QUERY);
        ctx.setStartTime();
        ctx.setResourceGroup(null);
        ctx.resetErrorCode();

        this.handleQuery();

        ctx.setLastQueryId(ctx.getQueryId());
        ctx.setQueryId(null);

        // Set command as sleep, so timeCheck will close the connection.
        // When client's last query is long long ago (controlled by waitTimeout session variable).
        ctx.setStartTime();
        ctx.setCommand(MysqlCommand.COM_SLEEP);
    }

    @Override
    protected void handleQuery() {
        MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(ctx.getRemoteIP())
                .setUser(ctx.getQualifiedUser())
                .setAuthorizedUser(
                        ctx.getCurrentUserIdentity() == null ? "null" : ctx.getCurrentUserIdentity().toString())
                .setDb(ctx.getDatabase())
                .setCatalog(ctx.getCurrentCatalog());
        Tracers.register(ctx);

        StatementBase parsedStmt = null;
        try {
            parsedStmt = runWithParserStageRetry();
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError("StarRocks process failed");
            ctx.getState().setErrType(QueryState.ErrType.IO_ERR);
        } catch (StarRocksException e) {
            LOG.warn("Process one query failed. SQL: {}, because ", originStmt, e);
            ctx.getState().setError(e.getMessage());
            // set is as ANALYSIS_ERR so that it won't be treated as a query failure.
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe StarRocks bug.
            LOG.warn("Process one query failed. SQL: {}, because unknown reason: ", originStmt, e);
            ctx.getState().setError("Unexpected exception: " + e.getMessage());
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                ctx.getState().setErrType(QueryState.ErrType.IGNORE_ERR);
            } else {
                ctx.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
            }
        } finally {
            Tracers.close();
        }

        if (executor != null) {
            auditAfterExec(originStmt, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
            executor.addFinishedQueryDetail();
        } else {
            auditAfterExec(originStmt, parsedStmt, null);
        }
    }

    @Override
    protected StmtExecutor doProxyExecute(TMasterOpResult result, TMasterOpRequest request, StatementBase statement,
                                          Frontend requestFE) throws Exception {
        if (request.isIsInternalStmt()) {
            throw new IllegalArgumentException("Arrow Flight SQL does not support internal statement");
        }

        StmtExecutor executor = new StmtExecutor(ctx, statement, deploymentFinished);
        ctx.setExecutor(executor);
        executor.setProxy();
        executor.addRunningQueryDetail(statement);

        try {
            ArrowFlightSqlServiceImpl.submitTask(() -> {
                final boolean prevUseLowCardinalityOptimizeOnLake = ctx.getSessionVariable().isUseLowCardinalityOptimizeOnLake();
                try {
                    ctx.getSessionVariable().setUseLowCardinalityOptimizeOnLake(false);
                    ctx.setThreadLocalInfo();
                    executor.execute();
                    deploymentFinished.complete(null);
                    processorFinished.complete(null);
                } catch (Exception e) {
                    deploymentFinished.completeExceptionally(e);
                    processorFinished.completeExceptionally(e);
                } finally {
                    ctx.getSessionVariable().setUseLowCardinalityOptimizeOnLake(prevUseLowCardinalityOptimizeOnLake);
                }
            });

            ArrowFlightSqlResultDescriptor backendResult = deploymentFinished.get();
            if (backendResult == null || ctx.getState().isError()) {
                return executor;
            }

            notifyForwardDeploymentFinished(requestFE, backendResult);
            return executor;
        } catch (Exception e) {
            LOG.warn("Process one query failed. SQL: {}", originStmt, e);
            ctx.kill(true, "Proxy execute one query failed. SQL: " + originStmt + ", error: " + e.getMessage());
            throw e;
        } finally {
            processorFinished.get(); // Wait for `executor.execute()` to finish
        }
    }

    private void notifyForwardDeploymentFinished(Frontend requestFE, ArrowFlightSqlResultDescriptor backendResult)
            throws TException {
        TNetworkAddress address = new TNetworkAddress(requestFE.getHost(), requestFE.getRpcPort());
        int timeoutMs = ctx.getExecTimeout() * 1000 + Config.thrift_rpc_timeout_ms;
        if (timeoutMs < 0) {
            timeoutMs = ctx.getExecTimeout() * 1000;
        }

        TNotifyForwardDeploymentFinishedRequest request = new TNotifyForwardDeploymentFinishedRequest();

        request.setQuery_id(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        request.setArrow_flight_sql_result_backend_id(backendResult.getBackendId());
        request.setArrow_flight_sql_result_fragment_id(backendResult.getFragmentInstanceId());
        request.setArrow_flight_sql_result_schema(backendResult.getSchema().serializeAsMessage());

        ThriftRPCRequestExecutor.call(
                ThriftConnectionPool.frontendPool,
                address,
                timeoutMs,
                client -> client.notifyForwardDeploymentFinished(request));
    }

    private void reportError(ConnectContext ctx) throws StarRocksException {
        String errMsg = ctx.getState().getErrorMessage();
        if (StringUtils.isEmpty(errMsg)) {
            errMsg = "Unknown error";
        }
        throw new StarRocksException(String.format("failed to process query [queryID=%s] [error=%s]",
                DebugUtil.printId(ctx.getExecutionId()), errMsg));
    }

    private StatementBase runWithParserStageRetry() throws Exception {
        try {
            return executeQueryAttempt();
        } catch (LargeInPredicateException e) {
            final boolean originalEnableLargeInPredicate = ctx.getSessionVariable().enableLargeInPredicate();
            try {
                ctx.getSessionVariable().setEnableLargeInPredicate(false);
                LOG.warn("Retrying query with enable_large_in_predicate=false");
                Tracers.record(Tracers.Module.BASE, "retry_with_large_in_predicate_exception", "true");
                ((ArrowFlightSqlConnectContext) ctx).resetForStatement();
                return executeQueryAttempt();
            } finally {
                ctx.getSessionVariable().setEnableLargeInPredicate(originalEnableLargeInPredicate);
            }
        }
    }

    private StatementBase executeQueryAttempt() throws Exception {
        StatementBase parsedStmt = parse(originStmt, ctx.getSessionVariable());
        Tracers.init(ctx, parsedStmt.getTraceMode(), parsedStmt.getTraceModule());

        executor = new StmtExecutor(ctx, parsedStmt, deploymentFinished);
        ctx.setIsLastStmt(true);
        ctx.setExecutor(executor);

        executor.addRunningQueryDetail(parsedStmt);
        executor.execute();

        return parsedStmt;
    }

    private StatementBase parse(String sql, SessionVariable sessionVariables) throws StarRocksException {
        List<StatementBase> stmts;
        try (Timer ignored = Tracers.watchScope(Tracers.Module.PARSER, "Parser")) {
            stmts = com.starrocks.sql.parser.SqlParser.parse(sql, sessionVariables);
        }

        if (stmts.size() > 1) {
            throw new StarRocksException("arrow flight sql query does not support execute multiple query");
        }

        StatementBase parsedStmt = stmts.get(0);
        parsedStmt.setOrigStmt(new OriginStatement(sql));
        return parsedStmt;
    }

}
