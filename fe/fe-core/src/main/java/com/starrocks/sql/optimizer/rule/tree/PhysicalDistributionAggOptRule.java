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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
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

            for (ColumnRefOperator groupBy : agg.getGroupBys()) {
                if (!scan.getColRefToColumnMetaMap().containsKey(groupBy)) {
                    return null;
                }

                if (!scan.getColRefToColumnMetaMap().get(groupBy).isKey()) {
                    return null;
                }
            }

            List<Column> nonKeyGroupBys =
                    agg.getGroupBys().stream().map(s -> scan.getColRefToColumnMetaMap().get(s)).collect(
                            Collectors.toList());

            for (Column column : ((OlapTable) scan.getTable()).getSchemaByIndexId(scan.getSelectedIndexId())) {
                if (!nonKeyGroupBys.contains(column)) {
                    break;
                }
                nonKeyGroupBys.remove(column);
            }

            if (nonKeyGroupBys.isEmpty()) {
                agg.setUseSortAgg(true);
                scan.setNeedSortedByKeyPerTablet(true);
            }

            return null;
        }
    }

}
