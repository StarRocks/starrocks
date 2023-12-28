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
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.rpc.RpcException;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class ExecuteExceptionHandler {

    private static final Logger LOG = LogManager.getLogger(ExecuteExceptionHandler.class);

    private static final Set<String> SCHEMA_NOT_MATCH_ERROR = ImmutableSet.of("invalid field name");

    public static void handle(Exception e, RetryContext context) throws Exception {
        if (e instanceof RpcException) {
            handleRpcException((RpcException) e, context);
        } else if (e instanceof UserException) {
            handleUserException((UserException) e, context);
        } else {
            throw e;
        }
    }

    private static void handleRpcException(RpcException e, RetryContext context) {
        // do nothing
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
