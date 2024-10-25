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

import com.starrocks.http.HttpConnectContext;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.sql.ast.ExecuteStmt;
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

    public static ExecPlan plan(ExecuteStmt executeStmt, StatementBase stmt, ConnectContext session) {
        if (!(stmt instanceof QueryStatement)) {
            return StatementPlanner.plan(stmt, session);
        }
        QueryStatement queryStmt = (QueryStatement) stmt;
        if (!queryStmt.isPointQuery()) {
            return StatementPlanner.plan(stmt, session);
        }

        PrepareStmtContext prepareStmtContext = session.getPreparedStmt(executeStmt.getStmtName());
        if (!prepareStmtContext.isCached()) {
            return planAndCacheExecPlan(stmt, session, prepareStmtContext);
        } else {
            if (prepareStmtContext.needReAnalyze(queryStmt, session)) {
                return planAndCacheExecPlan(stmt, session, prepareStmtContext);
            } else {
                ExecPlan execPlan = prepareStmtContext.getExecPlan();

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
        }
    }

    private static ExecPlan planAndCacheExecPlan(StatementBase stmt, ConnectContext session,
                                                 PrepareStmtContext prepareStmtContext) {
        ExecPlan execPlan = StatementPlanner.plan(stmt, session);
        if (execPlan == null) {
            return null;
        }

        prepareStmtContext.setExecPlan(execPlan);
        prepareStmtContext.updateLastSchemaUpdateTime((QueryStatement) stmt, session);
        prepareStmtContext.cachePlan(execPlan);
        return execPlan;
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