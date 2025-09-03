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

import com.google.common.base.Preconditions;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.formatter.FormatOptions;
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
public class SimpleExecutor {

    private static final Logger LOG = LogManager.getLogger(SimpleExecutor.class);

    private static final SimpleExecutor DEFAULT_REPO_EXECUTOR = new SimpleExecutor("RepoExecutor",
            TResultSinkType.HTTP_PROTOCAL);

    public static SimpleExecutor getRepoExecutor() {
        return DEFAULT_REPO_EXECUTOR;
    }

    private final String name;

    private final TResultSinkType queryResultProtocol;

    public SimpleExecutor(String name, TResultSinkType queryResultProtocol) {
        this.name = name;
        this.queryResultProtocol = queryResultProtocol;
    }

    private String formatSQL(String sql, StatementBase stmt) {
        if (!Config.enable_internal_sql) {
            return "?";
        }
        if (Config.enable_sql_desensitize_in_log) {
            return AstToSQLBuilder.toSQL(stmt, FormatOptions.allEnable().setColumnSimplifyTableName(false))
                    .orElse("this is statistic desensitize sql");
        }
        return sql;
    }

    public void executeDML(String sql) {
        ConnectContext prev = ConnectContext.get();
        try {
            ConnectContext context = createConnectContext();
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            sql = formatSQL(sql, parsedStmt);
            Preconditions.checkState(parsedStmt instanceof DmlStmt, "the statement should be dml");
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            AuditLog.getInternalAudit()
                    .info("{} execute SQL | Query_id {} | DML {}", name, DebugUtil.printId(context.getQueryId()), sql);
            executor.execute();
        } catch (Exception e) {
            LOG.error(name + " execute SQL {} failed: {}", sql, e.getMessage(), e);
            throw new SemanticException(String.format(name + " execute sql failed: %s", e.getMessage()), e);
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
            return executeDQL(sql, context);
        } finally {
            ConnectContext.remove();
            if (prev != null) {
                prev.setThreadLocalInfo();
            }
        }
    }

    public List<TResultBatch> executeDQL(String sql, ConnectContext context) {
        try {
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            sql = formatSQL(sql, parsedStmt);
            ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, queryResultProtocol);
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            AuditLog.getInternalAudit()
                    .info("{} execute SQL | Query_id {} | DQL {}", name, DebugUtil.printId(context.getQueryId()), sql);
            Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
            if (!sqlResult.second.ok()) {
                throw new SemanticException(name + "execute sql failed with status: " + sqlResult.second.getErrorMsg());
            }
            return sqlResult.first;
        } catch (Exception e) {
            LOG.error(name + " execute SQL failed {}", sql, e);
            throw new SemanticException(name + "execute sql failed: " + sql, e);
        }
    }

    public void executeDDL(String sql) {
        try {
            ConnectContext context = createConnectContext();

            List<StatementBase> parsedStmts = SqlParser.parse(sql, context.getSessionVariable());
            for (var parsedStmt : ListUtils.emptyIfNull(parsedStmts)) {
                Analyzer.analyze(parsedStmt, context);
                DDLStmtExecutor.execute(parsedStmt, context);
                sql = formatSQL(sql, parsedStmts.get(0));
            }
            AuditLog.getInternalAudit().info("{} execute DDL | DDL {}", name, sql);
        } catch (Exception e) {
            LOG.error(name + "execute DDL error: {}", sql, e);
            throw new RuntimeException(e);
        } finally {
            ConnectContext.remove();
        }
    }

    public ConnectContext createConnectContext() {
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.setThreadLocalInfo();
        context.setNeedQueued(false);
        context.setStartTime();
        return context;
    }
}
