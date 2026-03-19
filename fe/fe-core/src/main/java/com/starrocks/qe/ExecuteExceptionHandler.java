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

package com.starrocks.qe;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.Config;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.common.QueryDumpLog;
import com.starrocks.common.QueryPlanLog;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.SqlCredentialRedactor;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.GlobalDictNotMatchException;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotId;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumper;
import com.starrocks.sql.optimizer.statistics.IRelaxDictManager;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ExecuteExceptionHandler {

    private static final Logger LOG = LogManager.getLogger(ExecuteExceptionHandler.class);

    private static final Set<String> SCHEMA_NOT_MATCH_ERROR = ImmutableSet.of("invalid field name");

    public static void handle(Exception e, RetryContext context) throws Exception {
        if (e instanceof RemoteFileNotFoundException) {
            handleRemoteFileNotFound((RemoteFileNotFoundException) e, context);
        } else if (e instanceof RpcException) {
            handleRpcException((RpcException) e, context);
        } else if (e instanceof StarRocksException) {
            handleUserException((StarRocksException) e, context);
        } else if (e instanceof GlobalDictNotMatchException) {
            handleGlobalDictNotMatchException((GlobalDictNotMatchException) e, context);
        } else {
            throw e;
        }
    }

    /**
     * Writes the COSTS plan to fe.plan.log when {@code log_plan_on_query_failure} is enabled
     * and the error type is listed in {@code plan_log_failure_types}.
     * Called from the finally block of {@code StmtExecutor.execute()} after error state is set.
     * Safe to call when {@code execPlan} is null (e.g. planning itself failed).
     */
    public static void logFailedQueryPlan(ExecPlan execPlan, ConnectContext connectContext,
                                          OriginStatement originStmt) {
        QueryState state = connectContext.getState();
        logFailedQueryPlan(execPlan, connectContext, originStmt,
                state.getErrorMessage(), state.getErrType());
    }

    /**
     * Same as above, but accepts explicit {@code errorMsg} and {@code errType}.
     * Used during retry handling (e.g. RpcException), where the context error state is not yet set.
     */
    public static void logFailedQueryPlan(ExecPlan execPlan, ConnectContext connectContext,
                                          OriginStatement originStmt, String errorMsg,
                                          QueryState.ErrType errType) {
        if (!shouldLog(errType, errorMsg)) {
            return;
        }
        // Write COSTS plan to fe.plan.log.
        // When enable_collect_query_detail_info is set to true, the plan will be recorded in the query
        // detail, and hence there is no need to log it here.
        if (Config.log_plan_on_query_failure && execPlan != null && connectContext.getQueryDetail() == null) {
            try {
                String sql = originStmt != null ? originStmt.originStmt : "";
                String queryId = DebugUtil.printId(connectContext.getQueryId());
                String plan = execPlan.getExplainString(TExplainLevel.COSTS);
                QueryPlanLog.getQueryPlan().log(String.format(
                        "Query failed. queryId=%s, errType=%s, error=%s, sql=%s\n%s",
                        queryId, errType, errorMsg, SqlCredentialRedactor.redact(sql), plan));
            } catch (Exception e) {
                LOG.warn("Failed to write plan to fe.plan.log", e);
            }
        }
        // Write query dump to fe.dump.log.
        // Two cases:
        //   1. DumpInfo already exists (session var enable_query_dump was set): write the full dump.
        //   2. DumpInfo is absent: re-plan the SQL with dump mode enabled to generate a full dump
        //      (FE-side planning only, no BE execution) with zero upfront overhead on normal queries.
        if (Config.dump_query_on_failure) {
            try {
                DumpInfo dumpInfo = connectContext.getDumpInfo();
                if (dumpInfo == null) {
                    dumpInfo = buildVirtualDump(connectContext, originStmt, errorMsg, execPlan != null);
                }
                if (dumpInfo instanceof QueryDumpInfo) {
                    String queryId = DebugUtil.printId(connectContext.getQueryId());
                    QueryDumpLog.getQueryDump().log(QueryDumper.toJson((QueryDumpInfo) dumpInfo, queryId));
                }
            } catch (Exception e) {
                LOG.warn("Failed to write query dump to fe.dump.log", e);
            }
        }
    }

    /**
     * Regenerates a full {@link QueryDumpInfo} by re-planning the failed query with dump mode enabled.
     * This re-runs only FE-side planning (no BE execution), so table schemas, column statistics,
     * and optimizer decisions are captured exactly as during the original query.
     *
     * <p>Re-planning is only attempted when {@code execPlanSucceeded} is true — i.e. the original
     * planning succeeded but execution failed. If planning itself failed, re-planning would hit the
     * same error again, so we skip it and return a minimal dump (SQL + error message only).
     */
    private static QueryDumpInfo buildVirtualDump(ConnectContext connectContext,
                                                  OriginStatement originStmt, String errorMsg,
                                                  boolean execPlanSucceeded) {
        String sql = originStmt != null ? originStmt.originStmt : "";
        QueryDumpInfo dump = new QueryDumpInfo(connectContext);
        dump.setOriginStmt(sql);
        dump.addException(errorMsg);
        if (sql.isEmpty() || !execPlanSucceeded) {
            // Planning failed originally; skip re-plan to avoid repeating the same error.
            return dump;
        }
        DumpInfo prevDumpInfo = connectContext.getDumpInfo();
        connectContext.setDumpInfo(dump);
        try {
            List<StatementBase> stmts = SqlParser.parse(sql, connectContext.getSessionVariable());
            int idx = originStmt.idx < stmts.size() ? originStmt.idx : 0;
            StatementPlanner.plan(stmts.get(idx), connectContext);
        } catch (Exception e) {
            LOG.debug("Re-plan for dump failed, dump will be partial", e);
        } finally {
            connectContext.setDumpInfo(prevDumpInfo);
        }
        return dump;
    }

    /**
     * Returns true if the failed query should be logged to fe.plan.log / fe.dump.log.
     * Triggers when either condition is met (OR logic):
     * 1. The error type is listed in {@code plan_log_failure_types}.
     * 2. The error message contains any keyword in {@code plan_log_error_keywords}
     * (case-insensitive substring match; ignored when the keyword list is empty).
     *
     * <p>ANALYSIS_ERR is always excluded — SQL syntax/semantic mistakes are user errors
     * and should never produce a plan dump, even if a keyword accidentally matches.
     */
    private static boolean shouldLog(QueryState.ErrType errType, String errorMsg) {
        // Hard-exclude user SQL mistakes regardless of any other config.
        if (errType == QueryState.ErrType.ANALYSIS_ERR) {
            return false;
        }
        if (Arrays.asList(Config.plan_log_failure_types).contains(errType.name())) {
            return true;
        }
        String[] keywords = Config.plan_log_error_keywords;
        if (keywords.length > 0 && errorMsg != null) {
            String lower = errorMsg.toLowerCase();
            for (String kw : keywords) {
                if (!kw.isEmpty() && lower.contains(kw.toLowerCase())) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isRetryableStatus(TStatusCode statusCode) {
        return statusCode == TStatusCode.REMOTE_FILE_NOT_FOUND
                || statusCode == TStatusCode.THRIFT_RPC_ERROR
                || statusCode == TStatusCode.GLOBAL_DICT_NOT_MATCH;
    }

    // If modifications are made to the partition files of a Hive table by user,
    // such as through "insert overwrite partition", the Frontend couldn't be aware of these changes.
    // As a result, queries may use the file information cached in the FE for execution.
    // When the Backend cannot find the corresponding files, it returns a "Status::ACCESS_REMOTE_FILE_ERROR."
    // To handle this exception, we perform a retry. Before initiating the retry, we need to
    // refresh the metadata cache for the table and clear the query-level metadata cache.
    private static void handleRemoteFileNotFound(RemoteFileNotFoundException e, RetryContext context) {
        List<ScanNode> scanNodes = context.execPlan.getScanNodes();
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
                    metadata.refreshTable(hiveTable.getCatalogDBName(), hiveTable, new ArrayList<>(), true);
                    // clear query level metadata cache
                    metadata.clear();
                }
            }
        }
        if (!existExternalCatalog) {
            throw e;
        }
        Tracers.record(Tracers.Module.EXTERNAL, "HMS.RETRY", String.valueOf(context.retryTime + 1));
    }

    private static void tryTriggerRefreshDictAsync(GlobalDictNotMatchException e, RetryContext context) {
        Pair<Optional<Integer>, Optional<String>> err = e.extract();
        if (err.first.isEmpty()) {
            return;
        }
        SlotId slotId = new SlotId(err.first.get());
        for (ScanNode scanNode : context.execPlan.getScanNodes()) {
            if (scanNode.getDesc().getSlots().stream().anyMatch(x -> x.getId().equals(slotId))) {
                String columnName = scanNode.getDesc().getSlot(slotId.asInt()).getColumn().getName();
                String tableUUID = scanNode.getDesc().getTable().getUUID();
                IRelaxDictManager.getInstance().invalidTemporarily(tableUUID, columnName);
                GlobalStateMgr.getCurrentState().getConnectorTableTriggerAnalyzeMgr().
                        addDictUpdateTask(new ConnectorTableColumnKey(tableUUID, columnName), err.second);
                return;
            }
        }
    }

    private static void handleGlobalDictNotMatchException(GlobalDictNotMatchException e, RetryContext context)
            throws Exception {
        // trigger async collect dict
        tryTriggerRefreshDictAsync(e, context);

        // rerun without low cardinality optimization
        ConnectContext connectContext = context.connectContext;
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeOnLake(false);
        rebuildExecPlan(e, context);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeOnLake(true);
    }

    private static void handleRpcException(RpcException e, RetryContext context) throws Exception {
        ConnectContext connectContext = context.connectContext;
        // Log only on the first attempt to avoid duplicate entries during retries.
        // The plan is written to fe.plan.log; the inline plan is no longer written to fe.log
        // to keep it readable. If the query ultimately fails, the finally block in
        // StmtExecutor will log again with the latest rebuilt plan.
        if (context.retryTime == 0) {
            LOG.warn("Query cancelled by crash of backends or RpcException, [QueryId={}] [SQL={}]",
                    DebugUtil.printId(connectContext.getExecutionId()),
                    context.parsedStmt.getOrigStmt() == null ? "" : context.parsedStmt.getOrigStmt().originStmt,
                    e);
            logFailedQueryPlan(context.execPlan, connectContext,
                    context.parsedStmt.getOrigStmt(), e.getMessage(), QueryState.ErrType.INTERNAL_ERR);
        }
        // rebuild the exec plan in case the node is not available any more.
        rebuildExecPlan(e, context);
    }

    private static void rebuildExecPlan(Exception e, RetryContext context) throws Exception {
        try {
            context.execPlan = StatementPlanner.plan(context.parsedStmt, context.connectContext);
        } catch (Exception e1) {
            // encounter exception when re-plan, just log the new error but throw the original cause.
            if (LOG.isDebugEnabled()) {
                ConnectContext connectContext = context.connectContext;
                LOG.debug("encounter exception when retry, [QueryId={}] [SQL={}], ",
                        DebugUtil.printId(connectContext.getExecutionId()),
                        context.parsedStmt.getOrigStmt() == null ? "" :
                                context.parsedStmt.getOrigStmt().originStmt,
                        e1);
            }
            throw e;
        }
    }

    private static void handleUserException(StarRocksException e, RetryContext context) throws Exception {
        String msg = e.getMessage();
        if (context.parsedStmt instanceof QueryStatement) {
            for (String errMsg : SCHEMA_NOT_MATCH_ERROR) {
                if (msg.contains(errMsg)) {
                    rebuildExecPlan(e, context);
                    return;
                }
            }
            // if it is cancelled due to backend not alive, rebuild the plan and retry again
            if (e.getInternalErrorCode().equals(InternalErrorCode.CANCEL_NODE_NOT_ALIVE_ERR)) {
                rebuildExecPlan(e, context);
                return;
            }
        }
        throw e;
    }

    public static class RetryContext {
        private int retryTime;

        private ExecPlan execPlan;

        private ConnectContext connectContext;

        private StatementBase parsedStmt;

        public RetryContext(int retryTime, ExecPlan execPlan, ConnectContext connectContext,
                            StatementBase parsedStmt) {
            this.retryTime = retryTime;
            this.execPlan = execPlan;
            this.connectContext = connectContext;
            this.parsedStmt = parsedStmt;
        }

        public ExecPlan getExecPlan() {
            return execPlan;
        }

        public void setRetryTime(int retryTime) {
            this.retryTime = retryTime;
        }
    }
}
