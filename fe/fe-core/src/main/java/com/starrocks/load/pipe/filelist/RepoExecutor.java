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

package com.starrocks.load.pipe.filelist;

import com.google.common.base.Preconditions;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Execute SQL
 */
public class RepoExecutor {

    private static final Logger LOG = LogManager.getLogger(RepoExecutor.class);

    private static class SingletonHolder {
        private static final RepoExecutor INSTANCE = new RepoExecutor();
    }

    public static RepoExecutor getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private RepoExecutor() {
    }

    public void executeDML(String sql) {
        ConnectContext prev = ConnectContext.get();
        try {
            ConnectContext context = createHttpConnectContext();
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            Preconditions.checkState(parsedStmt instanceof DmlStmt, "the statement should be dml");
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            AuditLog.getInternalAudit().info("RepoExecutor execute SQL | Query_id {} | SQL {}",
                    DebugUtil.printId(context.getQueryId()), sql);
            executor.execute();
        } catch (Exception e) {
            LOG.error("RepoExecutor execute SQL {} failed: {}", sql, e.getMessage(), e);
            throw new SemanticException(String.format("execute sql failed: %s", e.getMessage()), e);
        } finally {
            ConnectContext.remove();
            if (prev != null) {
                prev.setThreadLocalInfo();
            }
        }
    }

    public List<TResultBatch> executeDQL(String sql) {
        ConnectContext prev = ConnectContext.get();
        try {
            ConnectContext context = createConnectContext();

            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.HTTP_PROTOCAL);
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            AuditLog.getInternalAudit().info("RepoExecutor execute SQL | Query_id {} | SQL {}",
                    DebugUtil.printId(context.getQueryId()), sql);
            Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
            if (!sqlResult.second.ok()) {
                throw new SemanticException("execute sql failed with status: " + sqlResult.second.getErrorMsg());
            }
            return sqlResult.first;
        } catch (Exception e) {
            LOG.error("Repo execute SQL failed {}", sql, e);
            throw new SemanticException("execute sql failed: " + sql, e);
        } finally {
            ConnectContext.remove();
            if (prev != null) {
                prev.setThreadLocalInfo();
            }
        }
    }

    public void executeDDL(String sql) {
        try {
            ConnectContext context = createConnectContext();

            List<StatementBase> parsedStmts = SqlParser.parse(sql, context.getSessionVariable());
            for (var parsedStmt : ListUtils.emptyIfNull(parsedStmts)) {
                Analyzer.analyze(parsedStmt, context);
                GlobalStateMgr.getCurrentState().getDdlStmtExecutor().execute(parsedStmt, context);
            }
            AuditLog.getInternalAudit().info("RepoExecutor execute DDL | SQL {}", sql);
        } catch (Exception e) {
            LOG.error("execute DDL error: {}", sql, e);
            throw new RuntimeException(e);
        } finally {
            ConnectContext.remove();
        }
    }

    private static ConnectContext createConnectContext() {
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.setThreadLocalInfo();
        context.setNeedQueued(false);
        return context;
    }

    private static HttpConnectContext createHttpConnectContext() {
        HttpConnectContext context =
                (HttpConnectContext) StatisticUtils.buildConnectContext(TResultSinkType.HTTP_PROTOCAL);
        context.setThreadLocalInfo();
        context.setNeedQueued(false);
        return context;
    }

}
