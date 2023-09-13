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
package com.starrocks.http;

import com.starrocks.common.UserException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.StatementBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

// inherit ConnectProcessor  to record the audit log and Query Detail
public class HttpConnectProcessor extends ConnectProcessor {
    private static final Logger LOG = LogManager.getLogger(HttpConnectProcessor.class);

    public HttpConnectProcessor(ConnectContext context) {
        super(context);
    }

    @Override
    protected void handleQuery() {
        MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(
                        ((HttpConnectContext) ctx).getRemoteIP())
                .setUser(ctx.getQualifiedUser())
                .setAuthorizedUser(
                        ctx.getCurrentUserIdentity() == null ? "null" : ctx.getCurrentUserIdentity().toString())
                .setDb(ctx.getDatabase())
                .setCatalog(ctx.getCurrentCatalog());
        Tracers.register(ctx);

        StatementBase parsedStmt = ((HttpConnectContext) ctx).getStatement();
        String sql = parsedStmt.getOrigStmt().originStmt;

        addRunningQueryDetail(parsedStmt);

        executor = new StmtExecutor(ctx, parsedStmt);
        ctx.setExecutor(executor);

        ctx.setIsLastStmt(true);

        //  for http protocal, if current FE can't read, just let client talk with leader
        if (executor.isForwardToLeader()) {
            LOG.warn("non-master FE can not read, forward HTTP request to master");
            ((HttpConnectContext) ctx).setForwardToLeader(true);
            return;
        }

        try {
            executor.execute();
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError("StarRocks process failed");
        } catch (UserException e) {
            LOG.warn("Process one query failed. SQL: " + sql + ", because.", e);
            ctx.getState().setError(e.getMessage());
            // set is as ANALYSIS_ERR so that it won't be treated as a query failure.
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe StarRocks bug.
            LOG.warn("Process one query failed. SQL: " + sql + ", because unknown reason: ", e);
            ctx.getState().setError("Unexpected exception: " + e.getMessage());
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            }
        } finally {
            Tracers.close();
        }

        // audit after exec
        // replace '\n' to '\\n' to make string in one line
        // TODO(cmy): when user send multi-statement, the executor is the last statement's executor.
        // We may need to find some way to resolve this.
        if (executor != null) {
            auditAfterExec(sql, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
        } else {
            // executor can be null if we encounter analysis error.
            auditAfterExec(sql, null, null);
        }

        addFinishedQueryDetail();
    }

    @Override
    public void processOnce() throws IOException {
        // set status of query to OK.
        ctx.getState().reset();
        executor = null;

        // only handle query，so no need to dispatch
        ctx.setCommand(MysqlCommand.COM_QUERY);
        ctx.setStartTime();
        ctx.setResourceGroup(null);
        ctx.setErrorCode("");
        this.handleQuery();
        ctx.setStartTime();

        // finalize in ExecuteSqlAction, it's more convenient to send http response

        // set command as sleep,so timeCheck will close the connection
        // when client's last query is long long ago(controled by waitTimeout session variable)
        ctx.setCommand(MysqlCommand.COM_SLEEP);

    }

}
