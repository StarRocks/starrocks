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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PhysicalDistributionAggOptRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable sv = ConnectContext.get().getSessionVariable();
        if (sv.isEnableQueryCache()) {
            return root;
        }
        if (sv.isEnableSortAggregate()) {
            root.getOp().accept(new UseSortAGGRule(), root, null);
            return root;
        }

        if (sv.isEnableSpill() && sv.getSpillMode().equals("force")) {
            return root;
        }

        // per bucket optimize will be replaced with group execution.
        // remove me in 4.0
        if (sv.isEnablePerBucketComputeOptimize()) {
            root.getOp().accept(new UsePerBucketOptimizeRule(sv.isEnablePartitionBucketOptimize()), root, null);
            return root;
        }
        return root;
    }

    private static class NoopVisitor extends OptExpressionVisitor<Void, Void> {
        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression opt : optExpression.getInputs()) {
                opt.getOp().accept(this, opt, context);
            }
            return null;
        }
    }

    private static class UsePerBucketOptimizeRule extends NoopVisitor {
        private final boolean enablePartitionBucketOptimize;
        private boolean hasColocateRequirement = false;

        UsePerBucketOptimizeRule(boolean enablePartitionBucketOptimize) {
            this.enablePartitionBucketOptimize = enablePartitionBucketOptimize;
        }

        @Override
        public Void visitPhysicalHashJoin(OptExpression optExpression, Void context) {
            hasColocateRequirement = true;
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            if (optExpression.getInputs().get(0).getOp().getOpType() != OperatorType.PHYSICAL_OLAP_SCAN) {
                return visit(optExpression, context);
            }

            PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getInputs().get(0).getOp();
            PhysicalHashAggregateOperator agg = (PhysicalHashAggregateOperator) optExpression.getOp();

            // Now we only support one-stage AGG for per-bucket optimize
            if (!agg.getType().isGlobal() || agg.getGroupBys().isEmpty()) {
                return null;
            }
            agg.setUsePerBucketOptmize(true);
            scan.setNeedOutputChunkByBucket(true);

            if (!hasColocateRequirement && enablePartitionBucketOptimize) {
                OlapTable olapTable = ((OlapTable) scan.getTable());
                Set<Column> partitionColumns = Sets.newHashSet(olapTable.getPartitionInfo()
                        .getPartitionColumns(olapTable.getIdToColumn()));
                List<ColumnRefOperator> groupBys = agg.getGroupBys();
                for (ColumnRefOperator groupBy : groupBys) {
                    Column column = scan.getColRefToColumnMetaMap().get(groupBy);
                    if (column != null) {
                        partitionColumns.remove(column);
                    }
                }
                if (partitionColumns.isEmpty()) {
                    agg.setWithoutColocateRequirement(true);
                    scan.setWithoutColocateRequirement(true);
                }
            }

            return null;
        }
    }

    private static class UseSortAGGRule extends NoopVisitor {
        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            if (optExpression.getInputs().get(0).getOp().getOpType() != OperatorType.PHYSICAL_OLAP_SCAN) {
                return visit(optExpression, context);
            }

            PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getInputs().get(0).getOp();
            PhysicalHashAggregateOperator agg = (PhysicalHashAggregateOperator) optExpression.getOp();

            // Now we only support one-stage AGG
            // TODO: support multi-stage AGG
            if (!agg.getType().isGlobal() || agg.getGroupBys().isEmpty()) {
                return null;
            }

            // the same key in multi partition are not in the same tablet
            if (scan.getSelectedPartitionId().size() > 1) {
                return null;
            }

            // Check if GROUP BY keys are prefix of sort key
            if (!isGroupByPrefixOfSortKey(scan, agg)) {
                return null;
            }

            agg.setUseSortAgg(true);
            scan.setNeedSortedByKeyPerTablet(true);

            return null;
        }

        /**
         * Check if GROUP BY keys form a prefix of the table's sort key
         * This method handles equality predicates and monotonic expressions
         */
        private boolean isGroupByPrefixOfSortKey(PhysicalOlapScanOperator scan, PhysicalHashAggregateOperator agg) {
            OlapTable olapTable = (OlapTable) scan.getTable();
            List<Integer> sortKeyIdxes = olapTable.getIndexMetaByIndexId(scan.getSelectedIndexId()).getSortKeyIdxes();

            // Get target columns (sort key or key columns)
            List<Column> sortedColumns;
            if (sortKeyIdxes != null && !sortKeyIdxes.isEmpty()) {
                // Use explicit sort key columns
                sortedColumns = new ArrayList<>();
                List<Column> schema = olapTable.getSchemaByIndexId(scan.getSelectedIndexId());
                for (Integer idx : sortKeyIdxes) {
                    if (idx < schema.size()) {
                        Column column = schema.get(idx);
                        if (!column.isGeneratedColumn()) {
                            sortedColumns.add(schema.get(idx));
                        } else {
                            Optional<List<Column>> columns = resolveEncodeSortKeys(column, schema, olapTable);
                            if (columns.isEmpty()) {
                                return false;
                            }
                            sortedColumns.addAll(columns.get());
                        }
                    }
                }
            } else {
                // Fall back to key columns
                sortedColumns = new ArrayList<>();
                for (Column column : olapTable.getSchemaByIndexId(scan.getSelectedIndexId())) {
                    if (column.isKey()) {
                        sortedColumns.add(column);
                    }
                }
            }

            return checkGroupByAgainstTargetColumns(scan, agg, sortedColumns);
        }

        // ORDER BY (encode_sort_key(k1, k2, k3))
        private Optional<List<Column>> resolveEncodeSortKeys(Column column, List<Column> schema, OlapTable olapTable) {
            Expr gen = column.getGeneratedColumnExpr(schema);
            List<Column> res = Lists.newArrayList();
            if (gen instanceof FunctionCallExpr funcCall &&
                    funcCall.getFnName().getFunction().equalsIgnoreCase(FunctionSet.ENCODE_SORT_KEY)) {
                List<Expr> encodeColumns = funcCall.getParams().exprs();
                for (Expr expr : encodeColumns) {
                    if (expr instanceof SlotRef slotRef) {
                        String colName = slotRef.getColumnName();
                        Column col = olapTable.getColumn(colName);
                        if (col != null) {
                            res.add(col);
                        } else {
                            return Optional.empty();
                        }
                    } else if (expr instanceof FunctionCallExpr) {
                        // JSON expression like: get_json_int(data, 'k1')
                        // The extracted column is data.k1
                        Column col = JsonPathRewriteRule.resolveJsonExpr(expr, olapTable);
                        if (col == null) {
                            break;
                        }
                        res.add(col);
                    } else {
                        return Optional.empty();
                    }
                }
            }
            if (res.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(res);
        }

        /**
         * Check if GROUP BY keys and equality predicates cover the sort key prefix
         * This method traverses the sort key and checks each key against GROUP BY and equality predicates
         */
        private boolean checkGroupByAgainstTargetColumns(PhysicalOlapScanOperator scan,
                                                         PhysicalHashAggregateOperator agg,
                                                         List<Column> targetColumns) {
            List<ColumnRefOperator> groupBys = agg.getGroupBys();
            ScalarOperator predicate = scan.getPredicate();

            // Extract equality predicates for quick lookup
            Set<Column> equalityPredicateColumns = new HashSet<>();
            if (predicate != null) {
                equalityPredicateColumns =
                        extractEqualityPredicateColumns(predicate).stream()
                                .map(x -> scan.getColRefToColumnMetaMap().get(x))
                                .collect(Collectors.toUnmodifiableSet());

            }

            // Create a set of GROUP BY columns for quick lookup
            Map<Column, Column> groupByColumns = Maps.newHashMap();
            for (ColumnRefOperator ref : groupBys) {
                if (!extractGroupByColumns(scan, ref, groupByColumns)) {
                    return false;
                }
            }

            // Check if this sort key column is covered by either:
            // 1. An equality predicate, OR
            // 2. A GROUP BY key
            for (Column sortKeyColumn : targetColumns) {
                // Check equality predicates
                boolean isCovered = false;
                if (equalityPredicateColumns.contains(sortKeyColumn)) {
                    isCovered = true;
                }

                // If not covered by equality predicate, check GROUP BY keys
                Column column = groupByColumns.get(sortKeyColumn);
                if (column != null && column.getType().equals(sortKeyColumn.getType())) {
                    isCovered = true;
                    groupByColumns.remove(sortKeyColumn);
                }

                // If this sort key column is not covered, we can't use sort aggregation
                if (!isCovered) {
                    break;
                }
            }

            return groupByColumns.isEmpty();
        }

        private static boolean extractGroupByColumns(PhysicalOlapScanOperator scan, ColumnRefOperator ref,
                                                     Map<Column, Column> groupByColumns) {
            boolean resolved = false;
            if (scan.getColRefToColumnMetaMap().containsKey(ref)) {
                Column col = scan.getColRefToColumnMetaMap().get(ref);
                groupByColumns.put(col, col);
                resolved = true;
            } else {
                ScalarOperator resolvedRef = scan.getProjection().resolveColumnRef(ref);
                if (resolvedRef instanceof ColumnRefOperator) {
                    Column col = scan.getColRefToColumnMetaMap().get(resolvedRef);
                    if (col.getType().equals(ref.getType())) {
                        groupByColumns.put(col, col);
                        resolved = true;
                    }
                }
            }

            return resolved;
        }

        /**
         * Extract column references from equality predicates
         */
        private Set<ColumnRefOperator> extractEqualityPredicateColumns(ScalarOperator predicate) {
            Set<ColumnRefOperator> columns = new HashSet<>();
            List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
            for (ScalarOperator op : CollectionUtils.emptyIfNull(conjuncts)) {
                if (op instanceof BinaryPredicateOperator binary) {
                    if (binary.getBinaryType().isEquivalence()) {
                        // Extract column references from equality predicates
                        if (binary.getChild(0).isColumnRef() && binary.getChild(1).isConstantRef()) {
                            columns.add((ColumnRefOperator) binary.getChild(0));
                        }
                        if (binary.getChild(1).isColumnRef() && binary.getChild(0).isConstantRef()) {
                            columns.add((ColumnRefOperator) binary.getChild(1));
                        }
                    }
                }
            }
            return columns;
        }

    }
}
