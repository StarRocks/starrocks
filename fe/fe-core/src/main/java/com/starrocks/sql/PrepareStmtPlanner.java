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

package com.starrocks.sql;

import com.starrocks.authorization.SecurityPolicyRewriteRule;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptDistributionPruner;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;

import java.util.List;

public class PrepareStmtPlanner {

    public static PreparedStatementPlan plan(ExecuteStmt executeStmt, ConnectContext session,
                                             PrepareStmt executableCopy) {
        try {
            PrepareStmtContext prepareStmtContext = session.getPreparedStmt(executeStmt.getStmtName());
            if (prepareStmtContext.isCached()) {
                StatementBase cachedStmt = prepareStmtContext.bindCached(executeStmt.getParamsExpr());
                QueryStatement cachedQuery = (QueryStatement) cachedStmt;
                if (!prepareStmtContext.needReAnalyze(cachedQuery, session) && !hasPolicy(cachedQuery, session)) {
                    // A prepared plan must not outlive the caller's current privileges (SET ROLE,
                    // COM_CHANGE_USER, grants/revokes). Policy is checked separately above because
                    // it changes the plan shape rather than only authorizing it.
                    if (!session.isBypassAuthorizerCheck()) {
                        Authorizer.check(cachedStmt, session);
                    }
                    return new PreparedStatementPlan(cachedStmt,
                            rebuildCachedPlan(executeStmt, cachedQuery, prepareStmtContext.getExecPlan(), session));
                }
                prepareStmtContext.reset();
            }

            return planFresh(executeStmt, session, prepareStmtContext, executableCopy);
        } finally {
            // Release query-level connector metadata when planning is done
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
        }
    }

    private static boolean hasPolicy(QueryStatement queryStmt, ConnectContext session) {
        return SecurityPolicyRewriteRule.hasPolicy(session, queryStmt);
    }

    private static PreparedStatementPlan planFresh(ExecuteStmt executeStmt, ConnectContext session,
                                                   PrepareStmtContext prepareStmtContext,
                                                   PrepareStmt executableCopy) {
        String currentCatalog = session.getCurrentCatalog();
        String currentDatabase = session.getDatabase();
        boolean currentRelationAliasCaseInsensitive = session.isRelationAliasCaseInsensitive();
        try {
            // The original prepared AST was resolved in this namespace. Re-enter it for the fresh
            // parse/analyze so USE, sql_dialect changes, or an unqualified UDF cannot retarget the
            // statement between PREPARE and EXECUTE.
            session.getSessionVariable().setCatalog(prepareStmtContext.getPreparedCatalog());
            session.setDatabase(prepareStmtContext.getPreparedDatabase());
            session.setRelationAliasCaseInSensitive(
                    prepareStmtContext.isPreparedRelationAliasCaseInsensitive());

            PrepareStmt executableStmt = executableCopy != null
                    ? executableCopy : prepareStmtContext.instantiate(executeStmt.getParamsExpr());
            StatementBase stmt = executableStmt.getInnerStmt();
            SecurityPolicyRewriteRule.markRelationsForRewrite(stmt);
            ExecPlan execPlan = StatementPlanner.plan(stmt, session);

            // isPointQuery relies on analyzer-populated table metadata and therefore must only be
            // evaluated after normal planning. A policy rewrite changes the source to a subquery,
            // so policy-bearing plans are intentionally never cached.
            if (execPlan != null && stmt instanceof QueryStatement && ((QueryStatement) stmt).isPointQuery()) {
                prepareStmtContext.updateLastSchemaUpdateTime((QueryStatement) stmt, session);
                prepareStmtContext.cachePlan(execPlan, executableStmt);
            }
            return new PreparedStatementPlan(stmt, execPlan);
        } finally {
            // Do not use ConnectContext.setCurrentCatalog for this temporary scope: that API
            // records a persistent modified session variable which is forwarded to other FEs.
            session.getSessionVariable().setCatalog(currentCatalog);
            session.setDatabase(currentDatabase);
            session.setRelationAliasCaseInSensitive(currentRelationAliasCaseInsensitive);
        }
    }

    private static ExecPlan rebuildCachedPlan(ExecuteStmt executeStmt, QueryStatement queryStmt,
                                              ExecPlan execPlan, ConnectContext session) {
        // use cache and rebuild physical plan
        rePlan(executeStmt, execPlan.getLogicalPlan(), execPlan.getPhysicalPlan());

        TResultSinkType resultSinkType = session instanceof HttpConnectContext ? TResultSinkType.HTTP_PROTOCAL :
                TResultSinkType.MYSQL_PROTOCAL;
        resultSinkType = queryStmt.hasOutFileClause() ? TResultSinkType.FILE : resultSinkType;

        OptExpression physicalPlan = execPlan.getPhysicalPlan();
        LogicalPlan logicalPlan = execPlan.getLogicalPlan();
        ColumnRefFactory columnRefFactory = execPlan.getColumnRefFactory();
        QueryRelation query = queryStmt.getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();

        return PlanFragmentBuilder.createPhysicalPlan(
                physicalPlan, session, logicalPlan.getOutputColumn(), columnRefFactory,
                colNames,
                resultSinkType,
                !session.getSessionVariable().isSingleNodeExecPlan());
    }

    public static class PreparedStatementPlan {
        private final StatementBase statement;
        private final ExecPlan execPlan;

        public PreparedStatementPlan(StatementBase statement, ExecPlan execPlan) {
            this.statement = statement;
            this.execPlan = execPlan;
        }

        public StatementBase getStatement() {
            return statement;
        }

        public ExecPlan getExecPlan() {
            return execPlan;
        }
    }

    private static void rePlan(ExecuteStmt executeStmt,
                               LogicalPlan logicalPlan,
                               OptExpression optimizedPlan) {

        Operator operator = logicalPlan.getRoot().getInputs().get(0).getOp();
        if (operator instanceof LogicalFilterOperator) {
            ScalarOperator.updateLiteralPredicates(operator.getPredicate(), executeStmt.getParamsExpr());
        }

        rePlanOptimizedPlan(logicalPlan, optimizedPlan);
    }

    private static void rePlanOptimizedPlan(LogicalPlan logicalPlan, OptExpression optimizedPlan) {
        if (!(optimizedPlan.getOp() instanceof PhysicalOlapScanOperator)) {
            return;
        }

        ScalarOperator predicate = logicalPlan.getRoot().getInputs().get(0).getOp().getPredicate();

        // process logical scan operator
        LogicalOlapScanOperator logicalScanOperator =
                (LogicalOlapScanOperator) logicalPlan.getRoot().getInputs().get(0).getInputs().get(0)
                        .getInputs().get(0).getOp();
        LogicalOlapScanOperator logicalOlapScanOperator =
                OptOlapPartitionPruner.prunePartitions(logicalScanOperator);
        logicalOlapScanOperator
                .buildColumnFilters(predicate);

        // update optimized plan partitionIds and tabletIds with predicates
        optimizedPlan.getOp().setPredicate(predicate);
        PhysicalOlapScanOperator physicalOlapScanOperator = (PhysicalOlapScanOperator) optimizedPlan.getOp();
        physicalOlapScanOperator.setSelectedPartitionId(logicalOlapScanOperator.getSelectedPartitionId());
        List<Long> pruneTabletIds = OptDistributionPruner.pruneTabletIds(logicalOlapScanOperator,
                logicalOlapScanOperator.getSelectedPartitionId());

        physicalOlapScanOperator.setSelectedTabletId(pruneTabletIds);
    }

}
