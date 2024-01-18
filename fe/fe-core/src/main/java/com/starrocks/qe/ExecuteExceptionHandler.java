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
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ExecuteExceptionHandler {

    private static final Logger LOG = LogManager.getLogger(ExecuteExceptionHandler.class);

    private static final Set<String> SCHEMA_NOT_MATCH_ERROR = ImmutableSet.of("invalid field name");

    public static void handle(Exception e, RetryContext context) throws Exception {
        if (e instanceof RemoteFileNotFoundException) {
            handleRemoteFileNotFound((RemoteFileNotFoundException) e, context);
        } else if (e instanceof RpcException) {
            handleRpcException((RpcException) e, context);
        } else if (e instanceof UserException) {
            handleUserException((UserException) e, context);
        } else {
            throw e;
        }
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
                    metadata.refreshTable(hiveTable.getDbName(), hiveTable, new ArrayList<>(), true);
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

    private static void handleRpcException(RpcException e, RetryContext context) {
        // When enable_collect_query_detail_info is set to true, the plan will be recorded in the query detail,
        // and hence there is no need to log it here.
        ConnectContext connectContext = context.connectContext;
        if (context.retryTime == 0 && connectContext.getQueryDetail() == null &&
                Config.log_plan_cancelled_by_crash_be) {
            LOG.warn(
                    "Query cancelled by crash of backends or RpcException, [QueryId={}] [SQL={}] [Plan={}]",
                    DebugUtil.printId(connectContext.getExecutionId()),
                    context.parsedStmt.getOrigStmt() == null ? "" : context.parsedStmt.getOrigStmt().originStmt,
                    context.execPlan == null ? "" : context.execPlan.getExplainString(TExplainLevel.COSTS),
                    e);
        }
    }

    private static void handleUserException(UserException e, RetryContext context) throws Exception {
        String msg = e.getMessage();
        if (context.parsedStmt instanceof QueryStatement) {
            for (String errMsg : SCHEMA_NOT_MATCH_ERROR) {
                if (msg.contains(errMsg)) {
                    try {
                        ExecPlan execPlan = StatementPlanner.plan(context.parsedStmt, context.connectContext);
                        context.execPlan = execPlan;
                        return;
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
