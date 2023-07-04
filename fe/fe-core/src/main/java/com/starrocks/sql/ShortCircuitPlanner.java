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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.UserException;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.UnionNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rowstore.RowStoreUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import com.starrocks.thrift.TResultSinkType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;

public class ShortCircuitPlanner {

    public static final long MAX_RETURN_ROWS = 1024;

    public static boolean supportShortCircuitRead(OptExpression root, ConnectContext connectContext) {
        if (!connectContext.getSessionVariable().isEnableShortCircuit()) {
            return false;
        }
        return root.getOp().accept(new LogicalPlanChecker(), root, null);
    }

    public ExecPlan planSelect(LogicalPlan logicalPlan, QueryRelation relation, ConnectContext connectContext) {
        OptExpression root = logicalPlan.getRoot();
        ExecPlan execPlan =
                new ExecPlan(connectContext, relation.getColumnOutputNames(), root, logicalPlan.getOutputColumn());
        PlanNode planNodeRoot = root.getOp().accept(new ExecPlanNodeBuilder(), root, execPlan);

        List<Expr> outputExprs = logicalPlan.getOutputColumn().stream().map(variable -> ScalarOperatorToExpr
                .buildExecExpression(variable,
                        new ScalarOperatorToExpr.FormatterContext(execPlan.getColRefToExpr()))
        ).collect(Collectors.toList());

        PlanFragment planFragment = new PlanFragment(new PlanFragmentId(0), planNodeRoot, DataPartition.UNPARTITIONED);
        planFragment.setShortCircuit(true);
        planFragment.setOutputExprs(outputExprs);
        execPlan.getFragments().add(planFragment);
        execPlan.getOutputExprs().addAll(outputExprs);

        // mysql result sink
        planFragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);

        return execPlan;
    }

    private static Table getTargetTable(DmlStmt dmlStmt) {
        if (dmlStmt instanceof InsertStmt) {
            return ((InsertStmt) dmlStmt).getTargetTable();
        } else if (dmlStmt instanceof UpdateStmt) {
            return ((UpdateStmt) dmlStmt).getTable();
        } else if (dmlStmt instanceof DeleteStmt) {
            return ((DeleteStmt) dmlStmt).getTable();
        } else {
            throw new UnsupportedOperationException("Unsupported DML statement: " + dmlStmt.getClass().getName());
        }
    }

    /**
     * @param projections a=a not need compute
     * @return
     */
    private static boolean isRedundant(Map<ColumnRefOperator, ScalarOperator> projections) {
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projections.entrySet()) {
            if (!entry.getKey().equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasNonDeterministicFunctions(ScalarOperator expr) {
        if (expr instanceof CallOperator) {
            CallOperator callExpr = (CallOperator) expr;
            if (FunctionSet.nonDeterministicFunctions.contains(callExpr.getFnName())) {
                return true;
            }
        }
        return expr.getChildren().stream().anyMatch(ShortCircuitPlanner::hasNonDeterministicFunctions);
    }

    private abstract static class BaseLogicalPlanChecker extends OptExpressionVisitor<Boolean, Void> {

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
    private static class LogicalPlanChecker extends BaseLogicalPlanChecker {
        private boolean allowFilter = true;
        private boolean allowLimit = true;

        private boolean allowProject = true;

        private boolean allowSort = true;

        private ScalarOperator predicate = null;

        private List<String> orderByColumns = null;

        private long limit = Operator.DEFAULT_LIMIT;

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

            if (projectOp.getColumnRefMap().values().stream()
                    .anyMatch(ShortCircuitPlanner::hasNonDeterministicFunctions)) {
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
                    .map(Ordering::getColumnRef).map(ColumnRefOperator::getName).collect(Collectors.toList());
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
            LogicalScanOperator scanOp = optExpression.getOp().cast();
            Table table = scanOp.getTable();
            if (!(table instanceof OlapTable) || !((OlapTable) table).hasRowStorageType()) {
                return false;
            }

            for (Column column : table.getFullSchema()) {
                if (IDictManager.getInstance().hasGlobalDict(table.getId(), column.getName())) {
                    return false;
                }
            }

            List<String> keyColumns = ((OlapTable) table).getKeyColumns().stream().map(Column::getName).collect(
                    Collectors.toList());
            List<String> sortingColumns = keyColumns;
            return checkPredicate(table, keyColumns, sortingColumns, predicate, limit)
                    && checkOrderBy(sortingColumns, orderByColumns);
        }

        private static boolean checkPredicate(Table table, List<String> keyColumns, List<String> sortingColumns,
                                              ScalarOperator predicate, long limit) {
            List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
            return isPointScan(table, keyColumns, conjuncts);
        }

        private static boolean isPointScan(Table table, List<String> keyColumns, List<ScalarOperator> conjuncts) {
            Map<String, PartitionColumnFilter> filters = ColumnFilterConverter.convertColumnFilter(conjuncts, table);
            if (keyColumns == null || keyColumns.isEmpty()) {
                return false;
            }
            if (!filters.keySet().equals(new HashSet<>(keyColumns))) {
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

        /**
         * // TODO (no supported)
         * As row store is ordering, the query is short circuit query if filter is prefix scan.
         * eg, `select * from usertable where ycsb_key >= 'user995139035672819231' limit 1;`
         */
        private static boolean isPrefixRangeScan(
                List<ScalarOperator> conjuncts, long limit, List<String> sortingColumns) {
            if (limit == Operator.DEFAULT_LIMIT || limit > MAX_RETURN_ROWS
                    || sortingColumns == null || sortingColumns.isEmpty()) {
                return false;
            }
            return RowStoreUtils.isPrefixRangeScan(sortingColumns, conjuncts);
        }

        private boolean checkOrderBy(List<String> sortingColumns, List<String> orderByColumns) {
            if (orderByColumns == null || orderByColumns.isEmpty()) {
                return true;
            }
            if (sortingColumns.size() < orderByColumns.size()) {
                return false;
            }
            return sortingColumns.subList(0, orderByColumns.size()).equals(orderByColumns);
        }
    }

    private static class ExecPlanNodeBuilder extends OptExpressionVisitor<PlanNode, ExecPlan> {

        private final List<LogicalProjectOperator> projectOperators = new ArrayList<>();

        private ScalarOperator predicate = null;

        private long limit = Operator.DEFAULT_LIMIT;

        @Override
        public PlanNode visit(OptExpression optExpression, ExecPlan context) {
            throw new IllegalArgumentException("plan pattern is not supported");
        }

        @Override
        public PlanNode visitLogicalValues(OptExpression optExpression, ExecPlan context) {
            LogicalValuesOperator valuesOperator = (LogicalValuesOperator) optExpression.getOp();

            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            for (ColumnRefOperator columnRefOperator : valuesOperator.getColumnRefSet()) {
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
            LogicalScanOperator scan = optExpression.getOp().cast();

            if (scan instanceof LogicalOlapScanOperator) {
                Table referenceTable = scan.getTable();
                context.getDescTbl().addReferencedTable(referenceTable);
                TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
                tupleDescriptor.setTable(referenceTable);

                for (Map.Entry<ColumnRefOperator, Column> entry : scan.getColRefToColumnMetaMap().entrySet()) {
                    SlotDescriptor slotDescriptor =
                            context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                    slotDescriptor.setColumn(entry.getValue());
                    slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                    slotDescriptor.setIsMaterialized(true);
                    context.getColRefToExpr()
                            .put(entry.getKey(), new SlotRef(entry.getKey().getName(), slotDescriptor));
                }
                tupleDescriptor.computeMemLayout();

                //set tablet
                Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), context.getConnectContext());
                analyzer.setDescTbl(context.getDescTbl());
                OlapScanNode scanNode =
                        new OlapScanNode(context.getNextNodeId(), tupleDescriptor, "COLUMN_WITH_ROW");
                scanNode.selectBestRollupByRollupSelector();
                context.getScanNodes().add(scanNode);
                try {
                    LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();
                    List<Long> selectedPartitionIds = olapScanOperator.getTable().getPartitions().stream()
                            .filter(Partition::hasData).map(Partition::getId).collect(
                                    Collectors.toList());
                    LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
                    builder.withOperator(olapScanOperator)
                            .setSelectedPartitionId(selectedPartitionIds).setPredicate(predicate);
                    olapScanOperator.buildColumnFilters(predicate);
                    olapScanOperator = builder.build();
                    scanNode.setColumnFilters(olapScanOperator.getColumnFilters());
                    scanNode.setSelectedPartitionIds(selectedPartitionIds);
                    scanNode.finalizeStats(analyzer);
                } catch (UserException e) {
                    throw new StarRocksPlannerException(
                            "Build Exec FileScanNode fail, scan info is invalid," + e.getMessage(),
                            INTERNAL_ERROR);
                }

                // set predicate
                if (predicate != null) {
                    List<ScalarOperator> predicates = Utils.extractConjuncts(predicate);
                    ScalarOperatorToExpr.FormatterContext formatterContext =
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
                    for (ScalarOperator predicate : predicates) {
                        scanNode.getConjuncts()
                                .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
                    }
                }

                // set limit
                scanNode.setLimit(limit);
                scanNode.computePointScanRangeLocations();
                return addProject(scanNode, context);
            }
            return visit(optExpression, context);
        }

        @Override
        public PlanNode visitLogicalProject(OptExpression optExpression, ExecPlan context) {
            LogicalProjectOperator projectOp = (LogicalProjectOperator) optExpression.getOp();
            if (!isRedundant(projectOp.getColumnRefMap())) {
                projectOperators.add(projectOp);
            }
            return visitChild(optExpression, context);
        }

        @Override
        public PlanNode visitLogicalFilter(OptExpression optExpression, ExecPlan context) {
            LogicalFilterOperator filterOp = optExpression.getOp().cast();
            predicate = filterOp.getPredicate();
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

        private PlanNode addProject(PlanNode child, ExecPlan context) {
            if (projectOperators.isEmpty()) {
                return child;
            }
            LogicalProjectOperator projectOp =
                    projectOperators.stream().reduce(ExecPlanNodeBuilder::mergeProject).get();

            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();

            Map<SlotId, Expr> projectMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectOp.getColumnRefMap().entrySet()) {
                Expr expr = ScalarOperatorToExpr.buildExecExpression(entry.getValue(),
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr(),
                                projectOp.getColumnRefMap()));

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

        private static LogicalProjectOperator mergeProject(
                LogicalProjectOperator parent, LogicalProjectOperator child) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(child.getColumnRefMap());
            Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : parent.getColumnRefMap().entrySet()) {
                resultMap.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
            }
            return new LogicalProjectOperator(resultMap);
        }
    }
}
