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

import com.starrocks.catalog.Table;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ShortCircuitPlanner {

    public static final long MAX_RETURN_ROWS = 2048;

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

    public static OptExpression checkSupportShortCircuitRead(OptExpression root, ConnectContext connectContext) {
        if (!connectContext.getSessionVariable().isEnableShortCircuit()) {
            root.setShortCircuit(false);
            return root;
        }
        boolean supportShortCircuit = root.getOp().accept(new LogicalPlanChecker(), root, null);
        root.setShortCircuit(supportShortCircuit);
        return root;
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
     * 1. Pattern: Project - Filter - Scan.
     * Filter only support pk IN/EQUAL/AND/OR
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
            this.allowProject = allowProject;
            this.predicate = predicate;
            this.orderByColumns = orderByColumns;
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
        public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
            return createLogicalPlanChecker(optExpression, allowFilter, allowLimit, allowProject,
                    allowSort, predicate, orderByColumns, limit).visitLogicalTableScan(optExpression, context);
        }

        protected static boolean isPointScan(Table table, List<String> keyColumns, List<ScalarOperator> conjuncts) {
            Map<String, PartitionColumnFilter> filters = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            filters.putAll(ColumnFilterConverter.convertColumnFilter(conjuncts, table));
            Set<String> boolKeyColumns = new HashSet<>();
            for (ScalarOperator conjunct : conjuncts) {
                if (conjunct instanceof ColumnRefOperator) {
                    boolKeyColumns.add(((ColumnRefOperator) conjunct).getName());
                }
            }
            if (keyColumns == null || keyColumns.isEmpty()) {
                return false;
            }
            long cardinality = 1;
            for (String keyColumn : keyColumns) {
                if (!filters.containsKey(keyColumn) && !boolKeyColumns.contains(keyColumn)) {
                    return false;
                }
                if (filters.containsKey(keyColumn)) {
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
            }
            return true;
        }
    }

}
