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

import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

            // Check if scan has equality predicates that don't interfere with sort key
            if (!canApplySortAggWithPredicates(scan, agg)) {
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
            List<Column> targetColumns;
            if (sortKeyIdxes != null && !sortKeyIdxes.isEmpty()) {
                // Use explicit sort key columns
                targetColumns = new ArrayList<>();
                List<Column> schema = olapTable.getSchemaByIndexId(scan.getSelectedIndexId());
                for (Integer idx : sortKeyIdxes) {
                    if (idx < schema.size()) {
                        targetColumns.add(schema.get(idx));
                    }
                }
            } else {
                // Fall back to key columns
                targetColumns = new ArrayList<>();
                for (Column column : olapTable.getSchemaByIndexId(scan.getSelectedIndexId())) {
                    if (column.isKey()) {
                        targetColumns.add(column);
                    }
                }
            }

            return checkGroupByAgainstTargetColumns(scan, agg, targetColumns);
        }

        /**
         * Check if GROUP BY keys and equality predicates cover the sort key prefix
         * This method traverses the sort key and checks each key against GROUP BY and equality predicates
         */
        private boolean checkGroupByAgainstTargetColumns(PhysicalOlapScanOperator scan, PhysicalHashAggregateOperator agg, 
                                                       List<Column> targetColumns) {
            List<ColumnRefOperator> groupBys = agg.getGroupBys();
            ScalarOperator predicate = scan.getPredicate();
            
            // Extract equality predicates for quick lookup
            Set<ColumnRefOperator> equalityPredicateColumns = new HashSet<>();
            if (predicate != null) {
                equalityPredicateColumns = extractEqualityPredicateColumns(predicate);
            }
            
            // Create a set of GROUP BY columns for quick lookup
            Set<ColumnRefOperator> groupBySet = new HashSet<>(groupBys);
            
            // Traverse each sort key column
            for (int i = 0; i < targetColumns.size(); i++) {
                Column sortKeyColumn = targetColumns.get(i);
                
                // Check if this sort key column is covered by either:
                // 1. An equality predicate, OR
                // 2. A GROUP BY key
                
                boolean isCovered = false;
                
                // Check equality predicates
                for (ColumnRefOperator eqCol : equalityPredicateColumns) {
                    Column mappedColumn = scan.getColRefToColumnMetaMap().get(eqCol);
                    if (mappedColumn != null && mappedColumn.equals(sortKeyColumn)) {
                        isCovered = true;
                        break;
                    }
                }
                
                // If not covered by equality predicate, check GROUP BY keys
                if (!isCovered) {
                    for (ColumnRefOperator groupBy : groupBys) {
                        // Case 1: Simple column reference
                        if (groupBy.isColumnRef()) {
                            Column mappedColumn = scan.getColRefToColumnMetaMap().get(groupBy);
                            if (mappedColumn != null && mappedColumn.equals(sortKeyColumn)) {
                                isCovered = true;
                                break;
                            }
                        }
                        // Case 2: Monotonic expression like encode_sort_key(a, b, c)
                        else if (isMonotonicExpressionCoveringColumn(groupBy, sortKeyColumn, i, scan)) {
                            isCovered = true;
                            break;
                        }
                    }
                }
                
                // If this sort key column is not covered, we can't use sort aggregation
                if (!isCovered) {
                    return false;
                }
            }
            
            return true;
        }

        /**
         * Check if the monotonic expression covers the specified sort key column
         * For example: encode_sort_key(a, b, c) covers column 'b' at position 1
         */
        private boolean isMonotonicExpressionCoveringColumn(ScalarOperator expr, Column targetColumn, int targetIndex, 
                                                          PhysicalOlapScanOperator scan) {
            if (!(expr instanceof CallOperator)) {
                return false;
            }
            
            CallOperator callOp = (CallOperator) expr;
            String functionName = callOp.getFnName();
            
            // Check if it's encode_sort_key function
            if (!"encode_sort_key".equals(functionName)) {
                return false;
            }
            
            List<ScalarOperator> arguments = callOp.getArguments();
            
            // Check if the target index is within the range of arguments
            if (targetIndex >= arguments.size()) {
                return false;
            }
            
            // Check if the argument at the target index maps to the target column
            ScalarOperator arg = arguments.get(targetIndex);
            if (!arg.isColumnRef()) {
                return false;
            }
            
            // Check if the column reference maps to the target column
            ColumnRefOperator colRef = (ColumnRefOperator) arg;
            Column mappedColumn = scan.getColRefToColumnMetaMap().get(colRef);
            return mappedColumn != null && mappedColumn.equals(targetColumn);
        }

        /**
         * Check if scan has equality predicates that don't interfere with sort aggregation
         * This is a simplified check - the main logic for handling equality predicates
         * is now in checkGroupByAgainstTargetColumns method
         */
        private boolean canApplySortAggWithPredicates(PhysicalOlapScanOperator scan, PhysicalHashAggregateOperator agg) {
            // For now, we allow all equality predicates
            // The detailed handling of equality predicates on GROUP BY columns
            // is done in checkGroupByAgainstTargetColumns method
            return true;
        }

        /**
         * Extract column references from equality predicates
         */
        private Set<ColumnRefOperator> extractEqualityPredicateColumns(ScalarOperator predicate) {
            Set<ColumnRefOperator> columns = new HashSet<>();
            extractEqualityPredicateColumnsHelper(predicate, columns);
            return columns;
        }

        private void extractEqualityPredicateColumnsHelper(ScalarOperator predicate, Set<ColumnRefOperator> columns) {
            if (predicate instanceof CompoundPredicateOperator) {
                CompoundPredicateOperator compound = (CompoundPredicateOperator) predicate;
                if (compound.isAnd()) {
                    // For AND predicates, we can extract columns from both sides
                    for (ScalarOperator child : compound.getChildren()) {
                        extractEqualityPredicateColumnsHelper(child, columns);
                    }
                }
                // For OR predicates, we need to be more careful, but for now we'll be conservative
            } else if (predicate instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
                if (binary.getBinaryType().isEquivalence()) {
                    // Extract column references from equality predicates
                    if (binary.getChild(0).isColumnRef()) {
                        columns.add((ColumnRefOperator) binary.getChild(0));
                    }
                    if (binary.getChild(1).isColumnRef()) {
                        columns.add((ColumnRefOperator) binary.getChild(1));
                    }
                }
            }
        }
    }

}
