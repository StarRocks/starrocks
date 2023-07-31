//  Copyright 2021-present StarRocks, Inc. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.starrocks.load.pipe.filelist;

import com.google.common.base.Preconditions;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.StmtExecutor;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Execute SQL
 */
public class RepoExecutor {

    private static final Logger LOG = LogManager.getLogger(RepoExecutor.class);
    private static final RepoExecutor INSTANCE = new RepoExecutor();

    public static RepoExecutor getInstance() {
        return INSTANCE;
    }

    public void executeDML(String sql) {
        try {
            ConnectContext context = StatisticUtils.buildConnectContext();
            context.setThreadLocalInfo();
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            Preconditions.checkState(parsedStmt instanceof DmlStmt, "the statement should be dml");
            DmlStmt dmlStmt = (DmlStmt) parsedStmt;
            ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.HTTP_PROTOCAL);
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            executor.handleDMLStmt(execPlan, dmlStmt);
        } catch (Exception e) {
            LOG.error("Repo execute SQL failed {}", sql, e);
            throw new SemanticException("execute sql failed with exception", e);
        } finally {
            ConnectContext.remove();
        }
    }

    public List<TResultBatch> executeDQL(String sql) {
        try {
            ConnectContext context = StatisticUtils.buildConnectContext();
            context.setThreadLocalInfo();

            // TODO: use json sink protocol, instead of statistic protocol
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.HTTP_PROTOCAL);
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
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
        }
    }

    public void executeDDL(String sql) {
        try {
            ConnectContext context = StatisticUtils.buildConnectContext();
            context.setThreadLocalInfo();

            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            Analyzer.analyze(parsedStmt, context);
            DDLStmtExecutor.execute(parsedStmt, context);
        } catch (Exception e) {
            LOG.error("execute DDL error: {}", sql, e);
            throw new RuntimeException(e);
        } finally {
            ConnectContext.remove();
        }
    }

}
