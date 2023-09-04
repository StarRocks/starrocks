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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.stream.Collectors;

public class PhysicalDistributionAggOptRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (ConnectContext.get().getSessionVariable().isEnableQueryCache()) {
            return root;
        }
        if (ConnectContext.get().getSessionVariable().isEnableSortAggregate()) {
            root.getOp().accept(new UseSortAGGRule(), root, null);
            return root;
        }
        if (ConnectContext.get().getSessionVariable().isEnablePerBucketComputeOptimize()) {
            root.getOp().accept(new UsePerBucketOptimizeRule(), root, null);
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
