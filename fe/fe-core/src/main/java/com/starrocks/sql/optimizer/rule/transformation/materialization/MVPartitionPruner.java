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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptDistributionPruner;
import com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;

import java.util.List;

public class MVPartitionPruner {
    private final OptimizerContext optimizerContext;
    private final MvRewriteContext mvRewriteContext;

    public MVPartitionPruner(OptimizerContext optimizerContext, MvRewriteContext mvRewriteContext) {
        this.optimizerContext = optimizerContext;
        this.mvRewriteContext = mvRewriteContext;
    }

    public OptExpression prunePartition(OptExpression queryExpression) {
        return queryExpression.getOp().accept(new MVPartitionPrunerVisitor(), queryExpression, null);
    }

    private class MVPartitionPrunerVisitor extends OptExpressionVisitor<OptExpression, Void> {
        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOperator = optExpression.getOp().cast();

            if (scanOperator instanceof LogicalOlapScanOperator) {
                // NOTE: need clear original partition predicates before,
                // original partition predicates if cannot be rewritten may contain wrong slot refs.
                // MV   : select c1, c3, c2 from test_base_part where c2 < 2000
                // Query: select c1, c3, c2 from test_base_part where c2 < 3000 and c3 < 3000
                LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) (scanOperator);
                LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
                builder.withOperator(olapScanOperator)
                        .setPrunedPartitionPredicates(Lists.newArrayList())
                        .setSelectedPartitionId(Lists.newArrayList())
                        .setSelectedTabletId(Lists.newArrayList());

                // for mv: select c1, c3, c2 from test_base_part where c3 < 2000 and c1 = 1,
                // which c3 is partition column and c1 is distribution column.
                // we should add predicate c3 < 2000 and c1 = 1 into scan operator to do pruning
                boolean isAddMvPrunePredicate = scanOperator.getTable().isMaterializedView()
                        && scanOperator.getTable().getId() == mvRewriteContext.getMaterializationContext().getMv().getId()
                        && mvRewriteContext.getMvPruneConjunct() != null;
                if (isAddMvPrunePredicate) {
                    ScalarOperator originPredicate = scanOperator.getPredicate();
                    ScalarOperator newPredicate = Utils.compoundAnd(originPredicate, mvRewriteContext.getMvPruneConjunct());
                    builder.setPredicate(newPredicate);
                }
                LogicalOlapScanOperator copiedOlapScanOperator = builder.build();

                // prune partition
                final LogicalOlapScanOperator prunedOlapScanOperator =
                        OptOlapPartitionPruner.prunePartitions(copiedOlapScanOperator);

                // prune distribution key
                copiedOlapScanOperator.buildColumnFilters(prunedOlapScanOperator.getPredicate());
                List<Long> selectedTabletIds = OptDistributionPruner.pruneTabletIds(copiedOlapScanOperator,
                        prunedOlapScanOperator.getSelectedPartitionId());

                ScalarOperator scanPredicate = prunedOlapScanOperator.getPredicate();
                if (isAddMvPrunePredicate) {
                    List<ScalarOperator> originConjuncts = Utils.extractConjuncts(scanOperator.getPredicate());
                    List<ScalarOperator> pruneConjuncts = Utils.extractConjuncts(mvRewriteContext.getMvPruneConjunct());
                    pruneConjuncts.removeAll(originConjuncts);
                    List<ScalarOperator> currentConjuncts = Utils.extractConjuncts(prunedOlapScanOperator.getPredicate());
                    currentConjuncts.removeAll(pruneConjuncts);
                    scanPredicate = Utils.compoundAnd(currentConjuncts);
                }

                LogicalOlapScanOperator.Builder rewrittenBuilder = new LogicalOlapScanOperator.Builder();
                scanOperator = rewrittenBuilder.withOperator(prunedOlapScanOperator)
                        .setPredicate(MvUtils.canonizePredicate(scanPredicate))
                        .setSelectedTabletId(selectedTabletIds)
                        .build();
            } else if (scanOperator instanceof LogicalHiveScanOperator ||
                    scanOperator instanceof LogicalHudiScanOperator ||
                    scanOperator instanceof LogicalIcebergScanOperator ||
                    scanOperator instanceof LogicalDeltaLakeScanOperator ||
                    scanOperator instanceof LogicalFileScanOperator ||
                    scanOperator instanceof LogicalEsScanOperator) {
                Operator.Builder builder = OperatorBuilderFactory.build(scanOperator);
                LogicalScanOperator copiedScanOperator =
                        (LogicalScanOperator) builder.withOperator(scanOperator).build();
                scanOperator = OptExternalPartitionPruner.prunePartitions(optimizerContext,
                        copiedScanOperator);
            }
            return OptExpression.create(scanOperator);
        }

        public OptExpression visit(OptExpression optExpression, Void context) {
            List<OptExpression> children = Lists.newArrayList();
            for (int i = 0; i < optExpression.arity(); ++i) {
                children.add(optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), null));
            }
            return OptExpression.create(optExpression.getOp(), children);
        }
    }
}
