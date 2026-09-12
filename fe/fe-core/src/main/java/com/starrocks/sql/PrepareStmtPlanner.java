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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.ResolvedAIFunctionDetector;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.Parameter;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptDistributionPruner;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PrepareStmtPlanner {
    private static final Logger LOG = LogManager.getLogger(PrepareStmtPlanner.class);

    public static ExecPlan plan(ExecuteStmt executeStmt, StatementBase stmt, ConnectContext session) {
        try {
            if (!(stmt instanceof QueryStatement)) {
                return StatementPlanner.plan(stmt, session);
            }
            QueryStatement queryStmt = (QueryStatement) stmt;
            if (!queryStmt.isPointQuery()) {
                return StatementPlanner.plan(stmt, session);
            }
            if (ResolvedAIFunctionDetector.contains(queryStmt)) {
                return StatementPlanner.plan(stmt, session);
            }

            PrepareStmtContext prepareStmtContext = session.getPreparedStmt(executeStmt.getStmtName());
            if (!prepareStmtContext.isCached()) {
                return planAndCacheExecPlan(executeStmt, stmt, session, prepareStmtContext);
            } else {
                if (prepareStmtContext.needReAnalyze(queryStmt, session)) {
                    return planAndCacheExecPlan(executeStmt, stmt, session, prepareStmtContext);
                } else {
                    ExecPlan execPlan = prepareStmtContext.getExecPlan();

                    // a rejected rebind leaves the cached predicate untouched; the full planning below then
                    // decides whether to replace the cached plan or drop it
                    if (!tryRebind(executeStmt, queryStmt, execPlan)) {
                        return planAndCacheExecPlan(executeStmt, stmt, session, prepareStmtContext);
                    }

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
        } finally {
            // Release query-level connector metadata when planning is done
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
        }
    }

    private static ExecPlan planAndCacheExecPlan(ExecuteStmt executeStmt, StatementBase stmt, ConnectContext session,
                                                 PrepareStmtContext prepareStmtContext) {
        ExecPlan execPlan = StatementPlanner.plan(stmt, session);
        if (execPlan == null) {
            return null;
        }

        QueryStatement queryStmt = (QueryStatement) stmt;
        if (!isRebindable(executeStmt, queryStmt, execPlan)) {
            // reset, not just skip: the needReAnalyze() call site arrives holding a plan built against the old schema
            prepareStmtContext.reset();
            return execPlan;
        }

        prepareStmtContext.setExecPlan(execPlan);
        prepareStmtContext.updateLastSchemaUpdateTime(queryStmt, session);
        prepareStmtContext.cachePlan(execPlan);
        return execPlan;
    }

    // decided once, when the plan is cached, so the hit path never meets a shape whose constants it cannot locate
    private static boolean isRebindable(ExecuteStmt executeStmt, QueryStatement queryStmt, ExecPlan execPlan) {
        LogicalFilterOperator filter = rebindableFilter(execPlan.getLogicalPlan());
        if (filter == null) {
            return rejectCaching(executeStmt, "logical plan is not a filter over an olap scan");
        }
        if (!(execPlan.getPhysicalPlan().getOp() instanceof PhysicalOlapScanOperator)) {
            return rejectCaching(executeStmt, "physical plan root is not an olap scan");
        }

        Map<String, Integer> slotByColumn = paramSlotByColumn(queryStmt);
        if (slotByColumn == null) {
            return rejectCaching(executeStmt, "where clause is not a conjunction of column = parameter");
        }

        int paramCount = executeStmt.getParamsExpr().size();
        Set<Integer> boundSlots = new HashSet<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(filter.getPredicate())) {
            ColumnRefOperator column = equalityColumn(conjunct);
            if (column == null) {
                return rejectCaching(executeStmt, "predicate is not a conjunction of column = constant");
            }
            Integer slot = slotByColumn.get(column.getName());
            if (slot == null || slot >= paramCount || !boundSlots.add(slot)) {
                return rejectCaching(executeStmt, "predicate column " + column.getName() + " has no unique parameter");
            }
        }

        // a parameter outside the predicate, in the select list say, would keep the value it had when the plan was built
        if (boundSlots.size() != paramCount) {
            return rejectCaching(executeStmt, "only " + boundSlots.size() + " of " + paramCount
                    + " parameters appear in the predicate");
        }
        return true;
    }

    // replace the cached predicate constants with this EXECUTE's parameters, located by column name, not by tree position
    private static boolean tryRebind(ExecuteStmt executeStmt, QueryStatement queryStmt, ExecPlan execPlan) {
        LogicalPlan logicalPlan = execPlan.getLogicalPlan();
        LogicalFilterOperator filter = rebindableFilter(logicalPlan);
        Map<String, Integer> slotByColumn = filter == null ? null : paramSlotByColumn(queryStmt);
        if (slotByColumn == null) {
            return rejectRebind(executeStmt, "cached plan no longer has a rebindable shape");
        }

        List<Expr> params = executeStmt.getParamsExpr();
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(filter.getPredicate());
        List<ConstantOperator> rebound = new ArrayList<>(conjuncts.size());
        for (ScalarOperator conjunct : conjuncts) {
            ColumnRefOperator column = equalityColumn(conjunct);
            Integer slot = column == null ? null : slotByColumn.get(column.getName());
            if (slot == null || slot >= params.size()) {
                return rejectRebind(executeStmt, "no parameter matches predicate column");
            }
            if (!(params.get(slot) instanceof LiteralExpr)) {
                return rejectRebind(executeStmt, "parameter " + slot + " is not a literal");
            }
            ConstantOperator adapted = rebindConstant(column, (LiteralExpr) params.get(slot));
            if (adapted == null) {
                return rejectRebind(executeStmt, "parameter " + slot + " would change the comparison for column "
                        + column.getName());
            }
            rebound.add(adapted);
        }

        // every parameter is validated before the first write, so a rejected EXECUTE never leaves the tree half bound
        for (int i = 0; i < conjuncts.size(); i++) {
            conjuncts.get(i).setChild(1, rebound.get(i));
        }
        rePlanOptimizedPlan(logicalPlan, execPlan.getPhysicalPlan());
        return true;
    }

    // run the real cast rule instead of predicting it: only a parameter it leaves the column uncast for keeps the
    // cached comparison semantics (a DOUBLE bound onto a VARCHAR key makes planning compare numerically instead)
    private static ConstantOperator rebindConstant(ColumnRefOperator column, LiteralExpr literal) {
        // same construction as SqlToScalarOperatorTranslator.visitLiteral, so the value matches what planning builds
        ConstantOperator value = literal instanceof NullLiteral
                ? ConstantOperator.createNull(literal.getType())
                : ConstantOperator.createObject(literal.getRealObjectValue(), literal.getType());

        // a rule may write through to what it is handed (ReduceCastRule setTypes a cast child in place) and this
        // reference belongs to the cached tree, so the candidate gets a copy
        ColumnRefOperator candidateColumn = (ColumnRefOperator) column.clone();
        ScalarOperator rewritten;
        try {
            rewritten = new ScalarOperatorRewriter().rewrite(
                    new BinaryPredicateOperator(BinaryType.EQ, candidateColumn, value),
                    ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        } catch (StarRocksPlannerException e) {
            // let full planning raise it, from the path that can name the statement
            return null;
        }

        if (!(rewritten instanceof BinaryPredicateOperator)
                || ((BinaryPredicateOperator) rewritten).getBinaryType() != BinaryType.EQ
                || !(rewritten.getChild(1) instanceof ConstantOperator)) {
            return null;
        }
        // identity, not equality: a cast wraps the reference in a new operator, an untouched column stays this object
        return rewritten.getChild(0) == candidateColumn ? (ConstantOperator) rewritten.getChild(1) : null;
    }

    // the exact shape rePlanOptimizedPlan walks unguarded: project -> filter -> project -> olap scan
    private static LogicalFilterOperator rebindableFilter(LogicalPlan logicalPlan) {
        OptExpression root = logicalPlan.getRoot();
        if (root.getInputs().isEmpty() || !(root.inputAt(0).getOp() instanceof LogicalFilterOperator)) {
            return null;
        }
        OptExpression filter = root.inputAt(0);
        if (filter.getInputs().isEmpty() || filter.inputAt(0).getInputs().isEmpty()
                || !(filter.inputAt(0).inputAt(0).getOp() instanceof LogicalOlapScanOperator)) {
            return null;
        }
        return (LogicalFilterOperator) filter.getOp();
    }

    private static ColumnRefOperator equalityColumn(ScalarOperator conjunct) {
        if (!(conjunct instanceof BinaryPredicateOperator)
                || ((BinaryPredicateOperator) conjunct).getBinaryType() != BinaryType.EQ) {
            return null;
        }
        if (!(conjunct.getChild(0) instanceof ColumnRefOperator) || !(conjunct.getChild(1) instanceof ConstantOperator)) {
            return null;
        }
        return (ColumnRefOperator) conjunct.getChild(0);
    }

    private static Map<String, Integer> paramSlotByColumn(QueryStatement queryStmt) {
        Map<String, Integer> slotByColumn = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Expr predicate = ((SelectRelation) queryStmt.getQueryRelation()).getPredicate();
        return collectParamSlots(predicate, slotByColumn) ? slotByColumn : null;
    }

    private static boolean collectParamSlots(Expr predicate, Map<String, Integer> slotByColumn) {
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) predicate;
            return compound.getOp() == CompoundPredicate.Operator.AND
                    && collectParamSlots(compound.getChild(0), slotByColumn)
                    && collectParamSlots(compound.getChild(1), slotByColumn);
        }
        if (!(predicate instanceof BinaryPredicate) || ((BinaryPredicate) predicate).getOp() != BinaryType.EQ) {
            return false;
        }
        Expr left = predicate.getChild(0);
        Expr right = predicate.getChild(1);
        SlotRef column = left instanceof SlotRef ? (SlotRef) left : (right instanceof SlotRef ? (SlotRef) right : null);
        Expr parameter = left instanceof SlotRef ? right : left;
        if (column == null || !(parameter instanceof Parameter)) {
            return false;
        }
        return slotByColumn.put(column.getColumnName(), ((Parameter) parameter).getSlotId()) == null;
    }

    // the repo has no plan-cache hit-rate metric, so a debug line is all a user has to explain a point query gone slow
    private static boolean rejectCaching(ExecuteStmt executeStmt, String reason) {
        LOG.debug("prepared statement {}: plan not cached for parameter rebinding, {}", executeStmt.getStmtName(), reason);
        return false;
    }

    private static boolean rejectRebind(ExecuteStmt executeStmt, String reason) {
        LOG.debug("prepared statement {}: falling back to full planning, {}", executeStmt.getStmtName(), reason);
        return false;
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
