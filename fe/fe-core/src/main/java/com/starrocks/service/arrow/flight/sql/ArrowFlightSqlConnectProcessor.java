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

import com.starrocks.common.StarRocksException;
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

// inherit ConnectProcessor to record the audit log and Query Detail
public class ArrowFlightSqlConnectProcessor extends ConnectProcessor {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlConnectProcessor.class);

    public ArrowFlightSqlConnectProcessor(ConnectContext context) {
        super(context);
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

        StatementBase parsedStmt = ((ArrowFlightSqlConnectContext) ctx).getStatement();
        String sql = parsedStmt.getOrigStmt().originStmt;

        executor = new StmtExecutor(ctx, parsedStmt);
        ctx.setExecutor(executor);
        ctx.setIsLastStmt(true);

        try {
            executor.addRunningQueryDetail(parsedStmt);
            executor.execute();
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError("StarRocks process failed");
            ctx.getState().setErrType(QueryState.ErrType.IO_ERR);
        } catch (StarRocksException e) {
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
                ctx.getState().setErrType(QueryState.ErrType.IGNORE_ERR);
            } else {
                ctx.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
            }
        } finally {
            Tracers.close();
        }

        auditAfterExec(sql, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
        executor.addFinishedQueryDetail();
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

        // Set command as sleep, so timeCheck will close the connection.
        // When client's last query is long long ago (controlled by waitTimeout session variable).
        ctx.setStartTime();
        ctx.setCommand(MysqlCommand.COM_SLEEP);
    }
}
