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

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Table;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.UnionNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.ScalarOperatorToExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ShortCircuitPlanner {

    public static final long MAX_RETURN_ROWS = 1024;

    public static BaseLogicalPlanChecker createLogicalPlanChecker(OptExpression root, boolean allowFilter,
                                                                  boolean allowLimit, boolean allowProject,
                                                                  boolean allowSort, ScalarOperator predicate,
                                                                  List<String> orderByColumns, long limit) {
        if (root.getOp() instanceof LogicalOlapScanOperator) {
            return new ShortCircuitPlannerHybrid.LogicalPlanChecker(allowFilter, allowLimit, allowProject,
                    allowSort, predicate, orderByColumns, limit);
        } else {
            return new BaseLogicalPlanChecker();
        }
    }

    public static BaseExecPlanNodeBuilder createExecPlanNodeBuilder(OptExpression root,
                                                                    LogicalProjectOperator projection,
                                                                    ScalarOperator predicate,
                                                                    long limit,
                                                                    ColumnRefSet requiredOutputColumns) {
        if (root.getOp() instanceof LogicalOlapScanOperator) {
            return new ShortCircuitPlannerHybrid.ExecPlanNodeBuilder(projection, predicate, limit, requiredOutputColumns);
        } else {
            return new BaseExecPlanNodeBuilder(requiredOutputColumns);
        }
    }

    public static boolean maySupportShortCircuit(StatementBase statement) {
        if (statement instanceof QueryStatement) {
            return true;
        }

        return false;
    }

    public static boolean supportShortCircuitRead(OptExpression root, ConnectContext connectContext) {
        if (!connectContext.getSessionVariable().isEnableShortCircuit()) {
            return false;
        }
        return root.getOp().accept(new LogicalPlanChecker(), root, null);
    }

    public ExecPlan planSelect(LogicalPlan logicalPlan, QueryRelation relation, ConnectContext connectContext) {
        OptExpression root = logicalPlan.getRoot();
        ExecPlan execPlan = new ExecPlan(connectContext, relation.getColumnOutputNames(), root,
                logicalPlan.getOutputColumn());
        ColumnRefSet requiredOutputColumns = new ColumnRefSet(logicalPlan.getOutputColumn());
        PlanNode planNodeRoot =
                root.getOp().accept(new BaseExecPlanNodeBuilder(requiredOutputColumns), root, execPlan);

        List<Expr> outputExprs = logicalPlan.getOutputColumn().stream().map(variable -> ScalarOperatorToExpr
                .buildExecExpression(variable, new ScalarOperatorToExpr.FormatterContext(execPlan.getColRefToExpr()))
        ).collect(Collectors.toList());

        PlanFragment planFragment = new PlanFragment(new PlanFragmentId(0), planNodeRoot, DataPartition.UNPARTITIONED);
        planFragment.setShortCircuit(true);
        planFragment.setOutputExprs(outputExprs);
        execPlan.getFragments().add(planFragment);
        execPlan.getOutputExprs().addAll(outputExprs);
        return execPlan;
    }

    protected static boolean isRedundant(Map<ColumnRefOperator, ScalarOperator> projections) {
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projections.entrySet()) {
            if (!entry.getKey().equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static class BaseLogicalPlanChecker extends OptExpressionVisitor<Boolean, Void> {
        @Override
        public Boolean visit(OptExpression optExpression, Void context) {
            return false;
        }

        protected boolean visitChild(OptExpression optExpression, Void context) {
            if (optExpression.getInputs().size() != 1) {
                return false;
            }
            OptExpression child = optExpression.getInputs().get(0);
            return child.getOp().accept(this, child, context);
        }

    }

    /**
     * Checker for select plan:
     * 1. Pattern: Limit - Sort (TopN) - Project - Filter - Scan.
     * 2. Scan: single row store table.
     * 3. Filter: contains all keys in table primary keys.
     * 4. Limit: no offset.
     */
    public static class LogicalPlanChecker extends BaseLogicalPlanChecker {
        protected boolean allowFilter = true;
        protected boolean allowLimit = true;
        protected boolean allowProject = true;
        protected boolean allowSort = true;

        protected ScalarOperator predicate = null;
        protected List<String> orderByColumns = null;

        protected long limit = Operator.DEFAULT_LIMIT;

        public LogicalPlanChecker() {
        }

        public LogicalPlanChecker(boolean allowFilter, boolean allowLimit, boolean allowProject, boolean allowSort,
                                  ScalarOperator predicate, List<String> orderByColumns, long limit) {
            this.allowFilter = allowFilter;
            this.allowLimit = allowLimit;
            this.allowProject = allowProject;
            this.allowSort = allowSort;
            this.predicate = predicate;
            this.orderByColumns = orderByColumns;
            this.limit = limit;
        }

        @Override
        public Boolean visitLogicalFilter(OptExpression optExpression, Void context) {
            if (!allowFilter) {
                return false;
            }
            allowFilter = false;
            allowLimit = false;
            allowProject = false;
            allowSort = false;

            LogicalFilterOperator filterOp = optExpression.getOp().cast();
            predicate = filterOp.getPredicate();
            return visitChild(optExpression, context);
        }

        @Override
        public Boolean visitLogicalProject(OptExpression optExpression, Void context) {
            LogicalProjectOperator projectOp = optExpression.getOp().cast();
            if (isRedundant(projectOp.getColumnRefMap())) {
                return visitChild(optExpression, context);
            }

            if (!allowProject) {
                return false;
            }
            return visitChild(optExpression, context);
        }

        @Override
        public Boolean visitLogicalTopN(OptExpression optExpression, Void context) {
            if (!allowSort) {
                return false;
            }
            allowSort = false;
            allowLimit = false;

            LogicalTopNOperator topNOp = optExpression.getOp().cast();
            List<Ordering> orderings = topNOp.getOrderByElements();
            if (!orderings.stream().allMatch(Ordering::isAscending)) {
                return false;
            }
            orderByColumns = orderings.stream()
                    .map(Ordering::getColumnRef)
                    .map(ColumnRefOperator::getName)
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
            if (topNOp.getOffset() != Operator.DEFAULT_OFFSET) {
                return false;
            }
            if (topNOp.getLimit() != Operator.DEFAULT_LIMIT) {
                limit = topNOp.getLimit();
            }
            return visitChild(optExpression, context);
        }

        @Override
        public Boolean visitLogicalLimit(OptExpression optExpression, Void context) {
            if (!allowLimit) {
                return false;
            }
            allowLimit = false;

            LogicalLimitOperator limitOp = optExpression.getOp().cast();
            if (limitOp.getOffset() != 0) {
                return false;
            }
            limit = limitOp.getLimit();
            return visitChild(optExpression, context);
        }

        @Override
        public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
            return createLogicalPlanChecker(optExpression, allowFilter, allowLimit, allowProject,
                    allowSort, predicate, orderByColumns, limit).visitLogicalTableScan(optExpression, context);
        }

        protected static boolean isPointScan(Table table, List<String> keyColumns, List<ScalarOperator> conjuncts) {
            Map<String, PartitionColumnFilter> filters = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            filters.putAll(ColumnFilterConverter.convertColumnFilter(conjuncts, table));
            if (keyColumns == null || keyColumns.isEmpty()) {
                return false;
            }
            long cardinality = 1;
            for (String keyColumn : keyColumns) {
                if (!filters.containsKey(keyColumn)) {
                    return false;
                }
                PartitionColumnFilter filter = filters.get(keyColumn);
                if (filter.getInPredicateLiterals() != null) {
                    cardinality *= filter.getInPredicateLiterals().size();
                    if (cardinality > MAX_RETURN_ROWS) {
                        return false;
                    }
                } else if (!filter.isPoint()) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class ValuesPlanChecker extends BaseLogicalPlanChecker {
        @Override
        public Boolean visitLogicalValues(OptExpression optExpression, Void context) {
            LogicalValuesOperator valuesOp = optExpression.getOp().cast();
            return valuesOp.getRows().size() <= MAX_RETURN_ROWS;
        }

        @Override
        public Boolean visitLogicalProject(OptExpression optExpression, Void context) {
            return visitChild(optExpression, context);
        }
    }

    protected static class BaseExecPlanNodeBuilder extends OptExpressionVisitor<PlanNode, ExecPlan> {

        protected LogicalProjectOperator projection = null;

        protected ScalarOperator predicate = null;

        protected long limit = Operator.DEFAULT_LIMIT;

        protected ColumnRefSet requiredOutputColumns;

        public BaseExecPlanNodeBuilder(ColumnRefSet requiredOutputColumns) {
            this.requiredOutputColumns = requiredOutputColumns;
        }

        public BaseExecPlanNodeBuilder(LogicalProjectOperator projection, ScalarOperator predicate, long limit,
                                       ColumnRefSet requiredOutputColumns) {
            this.projection = projection;
            this.predicate = predicate;
            this.limit = limit;
            this.requiredOutputColumns = requiredOutputColumns;
        }

        @Override
        public PlanNode visit(OptExpression optExpression, ExecPlan context) {
            throw new IllegalArgumentException("plan pattern is not supported");
        }

        @Override
        public PlanNode visitLogicalValues(OptExpression optExpression, ExecPlan context) {
            LogicalValuesOperator valuesOperator = (LogicalValuesOperator) optExpression.getOp();

            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            for (ColumnRefOperator columnRefOperator : valuesOperator.getColumnRefSet()) {
                if (!requiredOutputColumns.contains(columnRefOperator)) {
                    continue;
                }

                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(columnRefOperator.getId()));
                slotDescriptor.setIsNullable(columnRefOperator.isNullable());
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setType(columnRefOperator.getType());
                context.getColRefToExpr()
                        .put(columnRefOperator, new SlotRef(columnRefOperator.toString(), slotDescriptor));
            }
            tupleDescriptor.computeMemLayout();

            if (valuesOperator.getRows().isEmpty()) {
                return addProject(new EmptySetNode(context.getNextNodeId(),
                        Lists.newArrayList(tupleDescriptor.getId())), context);
            } else {
                UnionNode unionNode = new UnionNode(context.getNextNodeId(), tupleDescriptor.getId());
                unionNode.setLimit(valuesOperator.getLimit());

                List<List<Expr>> consts = new ArrayList<>();
                for (List<ScalarOperator> row : valuesOperator.getRows()) {
                    List<Expr> exprRow = new ArrayList<>();
                    for (ScalarOperator field : row) {
                        exprRow.add(ScalarOperatorToExpr.buildExecExpression(
                                field, new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())));
                    }
                    consts.add(exprRow);
                }

                unionNode.setMaterializedConstExprLists_(consts);
                consts.forEach(unionNode::addConstExprList);
                return addProject(unionNode, context);
            }
        }

        @Override
        public PlanNode visitLogicalTableScan(OptExpression optExpression, ExecPlan context) {
            return createExecPlanNodeBuilder(optExpression, projection, predicate, limit,
                    requiredOutputColumns).visitLogicalTableScan(optExpression, context);
        }

        @Override
        public PlanNode visitLogicalProject(OptExpression optExpression, ExecPlan context) {
            LogicalProjectOperator projectOp = (LogicalProjectOperator) optExpression.getOp();
            if (projection == null) {
                Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectOp.getColumnRefMap().entrySet()) {
                    if (requiredOutputColumns.contains(entry.getKey())) {
                        resultMap.put(entry.getKey(), entry.getValue());
                    }
                }
                projection = new LogicalProjectOperator(resultMap);
            } else {
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectOp.getColumnRefMap());
                Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                    resultMap.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
                }
                projection = new LogicalProjectOperator(resultMap);
            }
            ColumnRefSet requiredColumns = new ColumnRefSet();
            projection.getColumnRefMap().values().forEach(s -> requiredColumns.union(s.getUsedColumns()));
            if (predicate != null) {
                requiredColumns.union(predicate.getUsedColumns());
            }
            requiredOutputColumns = requiredColumns;
            return visitChild(optExpression, context);
        }

        @Override
        public PlanNode visitLogicalFilter(OptExpression optExpression, ExecPlan context) {
            LogicalFilterOperator filterOp = optExpression.getOp().cast();
            predicate = filterOp.getPredicate();
            requiredOutputColumns.union(predicate.getUsedColumns());
            return visitChild(optExpression, context);
        }

        @Override
        public PlanNode visitLogicalLimit(OptExpression optExpression, ExecPlan context) {
            LogicalLimitOperator limitOp = optExpression.getOp().cast();
            limit = limitOp.getLimit();
            return visitChild(optExpression, context);
        }

        @Override
        public PlanNode visitLogicalTopN(OptExpression optExpression, ExecPlan context) {
            return visitChild(optExpression, context);
        }

        private PlanNode visitChild(OptExpression optExpression, ExecPlan context) {
            Preconditions.checkArgument(optExpression.getInputs().size() == 1);
            OptExpression child = optExpression.getInputs().get(0);
            return child.getOp().accept(this, child, context);
        }

        protected PlanNode addProject(PlanNode child, ExecPlan context) {
            if (projection == null || isRedundant(projection.getColumnRefMap())) {
                return child;
            }
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();

            Map<SlotId, Expr> projectMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                Expr expr = ScalarOperatorToExpr.buildExecExpression(entry.getValue(),
                        new ScalarOperatorToExpr.FormatterContext(
                                context.getColRefToExpr(), projection.getColumnRefMap()));

                projectMap.put(new SlotId(entry.getKey().getId()), expr);

                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setIsNullable(expr.isNullable());
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setType(expr.getType());
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }

            ProjectNode projectNode =
                    new ProjectNode(context.getNextNodeId(),
                            tupleDescriptor,
                            child,
                            projectMap,
                            ImmutableMap.of());

            projectNode.setHasNullableGenerateChild();
            return projectNode;
        }
    }
}
